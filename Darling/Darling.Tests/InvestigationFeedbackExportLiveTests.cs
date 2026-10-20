/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The export of investigation cases that have feedback (#6236) on a live store: it holds exactly the cases with feedback inside the
/// filter (time range, server, verdict), each with the alert, the brief, the report and the recorded verdict and note; collected text
/// stays masked as the report reads mask it; the CLI verb writes valid JSON lines; and the web download answers the role that may
/// record feedback and refuses the role that may not.
/// </summary>
public sealed class InvestigationFeedbackExportLiveTests
{
    private const int OtherServerId = 7_591_302;
    private const string OtherServerName = "LcExportOther";
    private const string Secret = "hunter2-secret";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* A reported request on (serverId, family) opened hoursAgo hours ago: a brief that holds the secret in a finding, a report that holds it
       in its root cause. */
    private static async Task<Guid> CaseAsync(
        NpgsqlDataSource owner, InvestigationRequestStore store, int serverId, string serverName, string family, double hoursAgo, CancellationToken ct)
    {
        var alert = new InvestigationAlert(1, serverId, serverName, family, "Cpu", DateTime.UtcNow, new[] { serverId + ":" + family }, null,
            50, 200, LeaseMinutes: 15, ExpiryMinutes: 240);
        var requestId = (await store.OpenOrJoinAsync(alert, ct)).RequestId!.Value;
        var claim = await store.ClaimAsync(requestId, "agent-a", ct);
        var brief = new JsonObject
        {
            ["brief_format"] = 1,
            ["data_detail"] = "full",
            ["findings"] = new JsonArray(new JsonObject { ["category"] = family, ["story"] = $"blocked on WHERE name = '{Secret}'" }),
        }.ToJsonString();
        Assert.True(await store.SetBriefAsync(requestId, claim.ClaimId!.Value, 1, brief, ct));
        Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
            requestId, claim.ClaimId!.Value, "agent-a", 1, "real", $"slow WHERE name = '{Secret}'", true, "summary",
            "[{\"tool\":\"get_blocking\"}]"), ct));
        await using var command = owner.CreateCommand($"UPDATE config.investigation_requests SET opened_at = now() - make_interval(mins => {(int)(hoursAgo * 60)}) WHERE request_id = '{requestId}'");
        await command.ExecuteNonQueryAsync(ct);
        return requestId;
    }

    private static Task RecordAsync(InvestigationFeedbackStore feedback, Guid request, string verdict, string? comment, CancellationToken ct) =>
        feedback.RecordAsync(new InvestigationFeedbackSubmission(request, verdict, null, null, comment, "dana"), ct);

    private static async Task<InvestigationFeedbackCases> ReadAsync(
        InvestigationRequestStore store, DateTime? since, DateTime? until, int? server, string? verdict, CancellationToken ct) =>
        await store.ReadFeedbackCasesAsync(since, until, server, verdict, InvestigationRequestStore.FeedbackExportLimit, ct);

    [Fact]
    public async Task TheExport_HoldsExactlyTheCasesWithFeedbackInsideTheFilter_EachWithAlertBriefReportAndVerdict()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, OtherServerId, OtherServerName, ct);
        }

        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var wrong = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 2, ct);
        var right = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "resources", 30, ct);
        var other = await CaseAsync(owner, store, OtherServerId, OtherServerName, "blocking", 5, ct);
        var unlabelled = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "availability", 3, ct);
        await RecordAsync(feedback, wrong, "wrong", "the head blocker was a backup", ct);
        await RecordAsync(feedback, right, "right", null, ct);
        await RecordAsync(feedback, other, "partly", "close", ct);

        /* No filter: the three cases with feedback, newest request first; the one with no feedback is not exported. */
        var all = await ReadAsync(store, null, null, null, null, ct);
        Assert.Equal(new[] { wrong, other, right }, all.Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.DoesNotContain(unlabelled, all.Cases.Select(c => c.Request.RequestId));
        Assert.False(all.Truncated);

        /* A time range, a server and a verdict each narrow it; they combine. */
        var since = DateTime.UtcNow.AddHours(-10);
        Assert.Equal(new[] { wrong, other }, (await ReadAsync(store, since, null, null, null, ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Equal(new[] { right }, (await ReadAsync(store, null, since, null, null, ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Equal(new[] { other }, (await ReadAsync(store, null, null, OtherServerId, null, ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Equal(new[] { wrong }, (await ReadAsync(store, null, null, null, "wrong", ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Empty((await ReadAsync(store, since, null, null, "right", ct)).Cases);
        Assert.Empty((await ReadAsync(store, null, null, OtherServerId, "wrong", ct)).Cases);

        /* Each line: the alert, the brief the agent was given, the report it wrote, the verdict and the note a person recorded. */
        var line = JsonNode.Parse(InvestigationFeedbackExport.Line(all.Cases[0], McpDataDetail.Full, null))!;
        Assert.Equal(wrong.ToString(), (string)line["request_id"]!);
        Assert.Equal(InvestigationFeedbackLiveTests.ServerName, (string)line["alert"]!["server_name"]!);
        Assert.Equal("blocking", (string)line["alert"]!["family"]!);
        Assert.Equal("blocking", (string)line["brief"]!["findings"]![0]!["category"]!);
        Assert.Equal("real", (string)line["report"]!["verdict"]!);
        Assert.Equal("agent-a", (string)line["report"]!["agent_id"]!);
        var recorded = Assert.Single(line["feedback"]!.AsArray())!;
        Assert.Equal("wrong", (string)recorded["verdict"]!);
        Assert.Equal("the head blocker was a backup", (string)recorded["comment"]!);
        Assert.Equal("dana", (string)recorded["author"]!);
        Assert.Null(line["report"]!["feedback"]);

        /* The cap says so. */
        var capped = await store.ReadFeedbackCasesAsync(null, null, null, null, 2, ct);
        Assert.Equal(2, capped.Cases.Count);
        Assert.True(capped.Truncated);
    }

    [Fact]
    public async Task CollectedText_StaysMasked_InTheExport_AsTheReportReadMasksIt()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct);
        await RecordAsync(feedback, request, "wrong", $"it was WHERE name = '{Secret}' all along", ct);

        var (full, _) = await InvestigationFeedbackExport.ReadLinesAsync(store, null, null, null, null, McpDataDetail.Full, null, ct);
        Assert.Contains(Secret, Assert.Single(full), StringComparison.Ordinal);

        var (masked, _) = await InvestigationFeedbackExport.ReadLinesAsync(store, null, null, null, null, McpDataDetail.MaskLiterals, null, ct);
        var maskedLine = Assert.Single(masked);
        Assert.DoesNotContain(Secret, maskedLine, StringComparison.Ordinal);
        var node = JsonNode.Parse(maskedLine)!;
        Assert.Contains("WHERE name = ", (string)node["report"]!["root_cause"]!, StringComparison.Ordinal);
        Assert.Contains("WHERE name = ", (string)node["brief"]!["findings"]![0]!["story"]!, StringComparison.Ordinal);
        Assert.Contains("WHERE name = ", (string)node["feedback"]![0]!["comment"]!, StringComparison.Ordinal);
        Assert.Equal("maskLiterals", (string)node["brief"]!["data_detail"]!);

        var (withheld, _) = await InvestigationFeedbackExport.ReadLinesAsync(store, null, null, null, null, McpDataDetail.MetadataOnly, null, ct);
        var withheldLine = Assert.Single(withheld);
        Assert.DoesNotContain(Secret, withheldLine, StringComparison.Ordinal);
        Assert.DoesNotContain("dana", withheldLine, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheCliVerb_WritesValidJsonLines_OneCasePerLine_AndRefusesToReplaceAFileWithoutForce()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var first = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 2, ct);
        var second = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "resources", 4, ct);
        await RecordAsync(feedback, first, "wrong", "line one\nand a second line, with \"quotes\"", ct);
        await RecordAsync(feedback, second, "right", null, ct);

        var file = Path.Combine(Path.GetTempPath(), "darling-export-" + Guid.NewGuid().ToString("N") + ".jsonl");
        try
        {
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs(
                [file, "--verdict", "wrong", "--server", InvestigationFeedbackLiveTests.ServerName, "--since", "2020-01-01"], out var wrongOnly, out _));
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);
            Assert.Equal(0, await DarlingCliCommands.ExportInvestigationsAsync(owner, wrongOnly!, McpDataDetail.Full, output, error, ct));
            var lines = File.ReadAllLines(file);
            var one = Assert.Single(lines);
            Assert.Equal(first.ToString(), (string)JsonNode.Parse(one)!["request_id"]!);
            Assert.Equal("line one\nand a second line, with \"quotes\"", (string)JsonNode.Parse(one)!["feedback"]![0]!["comment"]!);
            Assert.Contains("Wrote 1 investigation case", output.ToString(), StringComparison.Ordinal);

            /* A file that is there is not replaced without --force. */
            using var refusedOutput = new StringWriter(CultureInfo.InvariantCulture);
            using var refusedError = new StringWriter(CultureInfo.InvariantCulture);
            Assert.Equal(1, await DarlingCliCommands.ExportInvestigationsAsync([file], refusedOutput, refusedError, ct));
            Assert.Contains("--force", refusedError.ToString(), StringComparison.Ordinal);
            Assert.Single(File.ReadAllLines(file));

            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file, "--force"], out var forced, out _));
            Assert.Equal(0, await DarlingCliCommands.ExportInvestigationsAsync(owner, forced!, McpDataDetail.Full, output, error, ct));
            var both = File.ReadAllLines(file);
            Assert.Equal(2, both.Length);
            Assert.All(both, l => Assert.Equal(System.Text.Json.JsonValueKind.Object, System.Text.Json.JsonDocument.Parse(l).RootElement.ValueKind));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Theory]
    [InlineData("--verdict", "great")]
    [InlineData("--since", "not-a-date")]
    [InlineData("--bogus", "x")]
    public void TheCliVerb_RefusesAnArgumentItCannotUse_InPlainWords(string flag, string value)
    {
        Assert.False(DarlingCliCommands.TryParseExportInvestigationsArgs(["out.jsonl", flag, value], out var parsed, out var error));
        Assert.Null(parsed);
        Assert.False(string.IsNullOrWhiteSpace(error));
        Assert.False(DarlingCliCommands.TryParseExportInvestigationsArgs([], out _, out _));
        Assert.False(DarlingCliCommands.TryParseExportInvestigationsArgs(["a.jsonl", "b.jsonl"], out _, out _));
        Assert.False(DarlingCliCommands.TryParseExportInvestigationsArgs(["a.jsonl", "--since", "2026-10-02", "--until", "2026-10-01"], out _, out _));
        Assert.True(DarlingCliCommands.IsKnownVerb("--export-investigations"));
    }

    [Fact]
    public async Task TheWebDownload_ServesTheCases_ToTheAdminRole_AndRefusesTheViewerRole_With403()
    {
        await InvestigationFeedbackLiveTests.WithWebRoutesAsync("admin", async (owner, request, Send, ct) =>
        {
            /* No case has feedback yet: an empty download, not an error. */
            var empty = await Send("GET", "/api/investigation-export?verdict=wrong", null, null);
            Assert.Equal(200, empty.Status);
            Assert.Null(empty.Body);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(
                new InvestigationFeedbackSubmission(request, "wrong", null, null, "web note", "dana"), ct);
            var served = await Send("GET", "/api/investigation-export", null, null);
            Assert.Equal(200, served.Status);
            Assert.Equal(request.ToString(), (string)served.Body!["request_id"]!);
            Assert.Equal("web note", (string)served.Body["feedback"]![0]!["comment"]!);
            Assert.Equal(400, (await Send("GET", "/api/investigation-export?verdict=great", null, null)).Status);
            Assert.Equal(400, (await Send("GET", "/api/investigation-export?since=never", null, null)).Status);
        });

        await InvestigationFeedbackLiveTests.WithWebRoutesAsync("viewer", async (owner, request, Send, ct) =>
        {
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(
                new InvestigationFeedbackSubmission(request, "wrong", null, null, "owner note", "dana"), ct);
            var refused = await Send("GET", "/api/investigation-export", null, null);
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationExportRefusedMessage, (string)refused.Body!["error"]!);
            Assert.Null(refused.Body["request_id"]);
        });
    }
}

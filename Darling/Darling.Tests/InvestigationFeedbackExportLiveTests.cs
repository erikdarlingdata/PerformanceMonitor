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
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The export of investigation cases that have feedback (#6236) on a live store: it holds exactly the cases with feedback inside the
/// filter (time range, server, verdict), each with the alert, the brief, the report and the recorded verdict and note; collected text
/// stays masked as the report reads mask it and a credential statement is withheld at every level; the command line and the web
/// download write the same bytes; only a signed-in admin account may download; and the file is written without following links,
/// without replacing an existing file unless asked, and is gone when the export fails.
/// </summary>
public sealed class InvestigationFeedbackExportLiveTests
{
    private const int OtherServerId = 7_591_302;
    private const string OtherServerName = "LcExportOther";
    private const string Secret = "zebra-42-literal";
    private const string Credential = "pw-Zq93-NotAWord";
    private static readonly string CredentialStatement = $"CREATE LOGIN exporter WITH PASSWORD = '{Credential}'";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingWebSeat AdminSeat = new("pat@example.test", CanEdit: true);
    private static readonly DarlingWebSeat ViewerSeat = new("vic@example.test", CanEdit: false);

    /* A reported request on (serverId, family) opened hoursAgo hours ago: a brief that holds the secret in a finding, a report that holds it
       in its root cause. With credential set, the brief, the root cause and queries_run each also hold a credential statement. */
    internal static async Task<Guid> CaseAsync(
        NpgsqlDataSource owner, InvestigationRequestStore store, int serverId, string serverName, string family, double hoursAgo, CancellationToken ct,
        string? credential = null, string? dedup = null, Action<JsonObject>? shapeBrief = null)
    {
        var alert = new InvestigationAlert(1, serverId, serverName, family, "Cpu", DateTime.UtcNow, new[] { dedup ?? serverId + ":" + family }, null,
            50, 200, LeaseMinutes: 15, ExpiryMinutes: 240);
        var requestId = (await store.OpenOrJoinAsync(alert, ct)).RequestId!.Value;
        var claim = await store.ClaimAsync(requestId, "agent-a", ct);
        var story = $"blocked on WHERE name = '{Secret}'" + (credential is null ? "" : $" after {credential}");
        var brief = new JsonObject
        {
            ["brief_format"] = 1,
            ["data_detail"] = "full",
            ["findings"] = new JsonArray(new JsonObject { ["category"] = family, ["story"] = story }),
        };
        shapeBrief?.Invoke(brief);
        Assert.True(await store.SetBriefAsync(requestId, claim.ClaimId!.Value, 1, brief.ToJsonString(), ct));
        var queries = credential is null ? "[{\"tool\":\"get_blocking\"}]" : new JsonArray(credential).ToJsonString();
        Assert.Equal(InvestigationSubmitOutcome.Reported, await store.SubmitAsync(new InvestigationReportSubmission(
            requestId, claim.ClaimId!.Value, "agent-a", 1, "real", $"slow WHERE name = '{Secret}'" + (credential is null ? "" : $" because {credential}"), true, "summary",
            queries), ct));
        /* The case's grouping window ended 30 minutes after it opened, as a real one hoursAgo hours old did. Left open, a later case of the
           same server, family and metric would join this reported request quietly (#6778) instead of opening its own. */
        await using var command = owner.CreateCommand($"UPDATE config.investigation_requests SET opened_at = now() - make_interval(mins => {(int)(hoursAgo * 60)}), window_until = now() - make_interval(mins => {(int)(hoursAgo * 60)}) + interval '30 minutes' WHERE request_id = '{requestId}'");
        await command.ExecuteNonQueryAsync(ct);
        return requestId;
    }

    private static Task RecordAsync(InvestigationFeedbackStore feedback, Guid request, string verdict, string? comment, CancellationToken ct) =>
        feedback.RecordAsync(new InvestigationFeedbackSubmission(request, verdict, null, null, comment, "dana"), ct);

    private static async Task<(List<InvestigationFeedbackCase> Cases, bool Truncated)> ReadAsync(
        InvestigationRequestStore store, DateTime? since, DateTime? until, int? server, string? verdict, CancellationToken ct,
        int limit = InvestigationRequestStore.FeedbackExportLimit)
    {
        var cases = new List<InvestigationFeedbackCase>();
        var truncated = await store.ReadFeedbackCasesAsync(since, until, server, verdict, limit, c =>
        {
            cases.Add(c);
            return Task.CompletedTask;
        }, ct);
        return (cases, truncated);
    }

    private static async Task<string[]> ExportLinesAsync(
        InvestigationRequestStore store, McpDataDetail mode, CancellationToken ct)
    {
        using var bytes = new MemoryStream();
        var outcome = await InvestigationFeedbackExport.WriteAsync(store, null, null, null, null, mode, null, bytes, ct);
        Assert.False(outcome.Refused);
        return Encoding.UTF8.GetString(bytes.ToArray()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
    }

    private static string TempFile() => Path.Combine(Path.GetTempPath(), "darling-export-" + Guid.NewGuid().ToString("N") + ".jsonl");

    private static string[] PartialsBeside(string file) =>
        Directory.GetFiles(Path.GetDirectoryName(file)!, "." + Path.GetFileName(file) + ".*.partial");

    [Fact]
    public async Task TheExport_HoldsExactlyTheCasesWithFeedbackInsideTheFilter_EachWithAlertBriefReportAndVerdict()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
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
        var (all, truncated) = await ReadAsync(store, null, null, null, null, ct);
        Assert.Equal(new[] { wrong, other, right }, all.Select(c => c.Request.RequestId).ToArray());
        Assert.DoesNotContain(unlabelled, all.Select(c => c.Request.RequestId));
        Assert.False(truncated);

        /* A time range, a server and a verdict each narrow it; they combine. */
        var since = DateTime.UtcNow.AddHours(-10);
        Assert.Equal(new[] { wrong, other }, (await ReadAsync(store, since, null, null, null, ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Equal(new[] { right }, (await ReadAsync(store, null, since, null, null, ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Equal(new[] { other }, (await ReadAsync(store, null, null, OtherServerId, null, ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Equal(new[] { wrong }, (await ReadAsync(store, null, null, null, "wrong", ct)).Cases.Select(c => c.Request.RequestId).ToArray());
        Assert.Empty((await ReadAsync(store, since, null, null, "right", ct)).Cases);
        Assert.Empty((await ReadAsync(store, null, null, OtherServerId, "wrong", ct)).Cases);

        /* Each line: the alert, the brief the agent was given, the report it wrote, the verdict and the note a person recorded. */
        var line = JsonNode.Parse(InvestigationFeedbackExport.Line(all[0], McpDataDetail.Full, null)!)!;
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
        var (capped, cappedTruncated) = await ReadAsync(store, null, null, null, null, ct, limit: 2);
        Assert.Equal(2, capped.Count);
        Assert.True(cappedTruncated);
    }

    [Fact]
    public async Task TheRead_PagesThroughALargeWindow_EveryCaseOnce_InOrder_AndTheCapSpansPages()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var count = InvestigationRequestStore.FeedbackExportPageSize * 2 + 7;
        for (var i = 0; i < count; i++)
        {
            var id = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct, dedup: "page-" + i);
            await RecordAsync(feedback, id, "right", null, ct);
        }

        /* Every request opened in the same instant: the page cursor has to break the tie on the request id, or a page boundary drops or repeats one. */
        await using (var command = owner.CreateCommand("UPDATE config.investigation_requests SET opened_at = date_trunc('second', now()) - interval '1 hour'"))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        var expected = new List<Guid>();
        await using (var command = owner.CreateCommand("SELECT request_id FROM config.investigation_requests ORDER BY opened_at DESC, request_id"))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                expected.Add(reader.GetGuid(0));
        }

        var (all, truncated) = await ReadAsync(store, null, null, null, null, ct);
        Assert.Equal(expected, all.Select(c => c.Request.RequestId).ToList());
        Assert.False(truncated);

        var (capped, cappedTruncated) = await ReadAsync(store, null, null, null, null, ct, limit: InvestigationRequestStore.FeedbackExportPageSize + 11);
        Assert.Equal(expected.Take(InvestigationRequestStore.FeedbackExportPageSize + 11).ToList(), capped.Select(c => c.Request.RequestId).ToList());
        Assert.True(cappedTruncated);
        var (exact, exactTruncated) = await ReadAsync(store, null, null, null, null, ct, limit: count);
        Assert.Equal(count, exact.Count);
        Assert.False(exactTruncated);
    }

    [Fact]
    public async Task CollectedText_StaysMasked_InTheExport_AsTheReportReadMasksIt()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct);
        await RecordAsync(feedback, request, "wrong", $"it was WHERE name = '{Secret}' all along", ct);

        var full = await ExportLinesAsync(store, McpDataDetail.Full, ct);
        Assert.Contains(Secret, Assert.Single(full), StringComparison.Ordinal);

        var masked = await ExportLinesAsync(store, McpDataDetail.MaskLiterals, ct);
        var maskedLine = Assert.Single(masked);
        Assert.DoesNotContain(Secret, maskedLine, StringComparison.Ordinal);
        var node = JsonNode.Parse(maskedLine)!;
        Assert.Contains("WHERE name = ", (string)node["report"]!["root_cause"]!, StringComparison.Ordinal);
        Assert.Contains("WHERE name = ", (string)node["brief"]!["findings"]![0]!["story"]!, StringComparison.Ordinal);
        Assert.Contains("WHERE name = ", (string)node["feedback"]![0]!["comment"]!, StringComparison.Ordinal);
        Assert.Equal("maskLiterals", (string)node["brief"]!["data_detail"]!);

        var withheld = await ExportLinesAsync(store, McpDataDetail.MetadataOnly, ct);
        var withheldLine = Assert.Single(withheld);
        Assert.DoesNotContain(Secret, withheldLine, StringComparison.Ordinal);
        Assert.DoesNotContain("dana", withheldLine, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("full")]
    [InlineData("maskLiterals")]
    [InlineData("metadataOnly")]
    public async Task ACredentialStatement_InAReportACommentAndTheBrief_IsWithheldFromTheCliFile_AtEveryLevel(string level)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var mode = McpDataDetailModes.Resolve(level);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct, credential: CredentialStatement);
        await RecordAsync(feedback, request, "wrong", $"it was the job that runs {CredentialStatement}", ct);

        var file = TempFile();
        try
        {
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file], out var args, out _));
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);
            Assert.Equal(0, await DarlingCliCommands.ExportInvestigationsAsync(owner, args!, mode, output, error, ct));
            var text = File.ReadAllText(file);
            Assert.Contains(request.ToString(), text, StringComparison.Ordinal);
            Assert.DoesNotContain(Credential, text, StringComparison.Ordinal);
            Assert.DoesNotContain("exporter", text, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task TheCliVerb_WritesValidJsonLines_OneCasePerLine_AndRefusesToReplaceAFileWithoutForce()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var first = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 2, ct);
        var second = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "resources", 4, ct);
        await RecordAsync(feedback, first, "wrong", "line one\nand a second line, with \"quotes\"", ct);
        await RecordAsync(feedback, second, "right", null, ct);

        var file = TempFile();
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
            Assert.Contains(Path.GetFullPath(file), output.ToString(), StringComparison.Ordinal);
            Assert.Contains(InvestigationFeedbackLiveTests.ServerName, output.ToString(), StringComparison.Ordinal);
            Assert.Empty(PartialsBeside(file));

            /* The file is readable by the current user only. */
            if (OperatingSystem.IsWindows())
            {
                Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(file));
            }
            else
            {
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(file));
            }

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
            Assert.Empty(PartialsBeside(file));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task TheCliVerb_RefusesAFolderTarget_AndAnAmbiguousOrPartialServerName_AndWritesNothing()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, OtherServerId, "LcAmbiguous", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, OtherServerId + 1, "lcambiguous", ct);
        }

        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct);
        await RecordAsync(feedback, request, "wrong", null, ct);

        var folder = Path.Combine(Path.GetTempPath(), "darling-export-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        var file = TempFile();
        try
        {
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([folder, "--force"], out var asFolder, out _));
            Assert.Equal(1, await DarlingCliCommands.ExportInvestigationsAsync(owner, asFolder!, McpDataDetail.Full, output, error, ct));
            Assert.Contains("folder", error.ToString(), StringComparison.Ordinal);
            Assert.Empty(Directory.GetFileSystemEntries(folder));
            Assert.Empty(Directory.GetFiles(Path.GetTempPath(), "." + Path.GetFileName(folder) + ".*.partial"));

            /* The same name in two cases is ambiguous: the refusal names both, and nothing is written. */
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file, "--server", "lcambiguous"], out var ambiguous, out _));
            using var ambiguousError = new StringWriter(CultureInfo.InvariantCulture);
            Assert.Equal(1, await DarlingCliCommands.ExportInvestigationsAsync(owner, ambiguous!, McpDataDetail.Full, output, ambiguousError, ct));
            Assert.Contains("LcAmbiguous", ambiguousError.ToString(), StringComparison.Ordinal);
            Assert.Contains("lcambiguous", ambiguousError.ToString(), StringComparison.Ordinal);

            /* A part of a name is not a match. */
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file, "--server", "LcAmbig"], out var partial, out _));
            using var partialError = new StringWriter(CultureInfo.InvariantCulture);
            Assert.Equal(1, await DarlingCliCommands.ExportInvestigationsAsync(owner, partial!, McpDataDetail.Full, output, partialError, ct));
            Assert.Contains("No registered server", partialError.ToString(), StringComparison.Ordinal);
            Assert.False(File.Exists(file));
            Assert.Empty(PartialsBeside(file));
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
            File.Delete(file);
        }
    }

    [Fact]
    public async Task AFileThatAppearsAtTheTarget_WhileTheStoreIsRead_IsNotReplacedWithoutForce_AndNoPartialFileIsLeft()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct);
        await RecordAsync(feedback, request, "wrong", null, ct);

        var file = TempFile();
        try
        {
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file], out var args, out _));
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);

            /* A held table lock keeps the read waiting, so the file below appears during it. */
            await using var holder = await owner.OpenConnectionAsync(ct);
            await using var transaction = await holder.BeginTransactionAsync(ct);
            await using (var lockCommand = new NpgsqlCommand("LOCK TABLE config.investigation_feedback IN ACCESS EXCLUSIVE MODE", holder, transaction))
            {
                await lockCommand.ExecuteNonQueryAsync(ct);
            }

            var export = DarlingCliCommands.ExportInvestigationsAsync(owner, args!, McpDataDetail.Full, output, error, ct);
            await WaitForAsync(() => PartialsBeside(file).Length == 1, ct);
            await File.WriteAllTextAsync(file, "someone else's file", ct);
            await transaction.RollbackAsync(ct);

            Assert.NotEqual(0, await export);
            Assert.Equal("someone else's file", await File.ReadAllTextAsync(file, ct));
            Assert.Empty(PartialsBeside(file));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public async Task AnExportThatFailsMidway_LeavesNoFileAndNoPartialFile()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var request = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct);
        await RecordAsync(feedback, request, "wrong", null, ct);

        var file = TempFile();
        try
        {
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file], out var args, out _));
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);
            using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);

            await using var holder = await owner.OpenConnectionAsync(ct);
            await using var transaction = await holder.BeginTransactionAsync(ct);
            await using (var lockCommand = new NpgsqlCommand("LOCK TABLE config.investigation_feedback IN ACCESS EXCLUSIVE MODE", holder, transaction))
            {
                await lockCommand.ExecuteNonQueryAsync(ct);
            }

            var export = DarlingCliCommands.ExportInvestigationsAsync(owner, args!, McpDataDetail.Full, output, error, cancel.Token);
            await WaitForAsync(() => PartialsBeside(file).Length == 1, ct);
            await cancel.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => export);
            await transaction.RollbackAsync(ct);

            Assert.False(File.Exists(file));
            Assert.Empty(PartialsBeside(file));
        }
        finally
        {
            File.Delete(file);
        }
    }

    private static async Task WaitForAsync(Func<bool> condition, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "the export did not start in time");
            await Task.Delay(10, ct);
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

    private static (int Status, string Text) Decode((int Status, byte[] Raw) answer) => (answer.Status, Encoding.UTF8.GetString(answer.Raw));

    [Fact]
    public async Task TheWebDownload_ServesTheCases_ToASignedInAdmin_WithAnAdminStoreLogin_AndRefusesBadFilters()
    {
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("admin", AdminSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            /* No case has feedback yet: an empty download, not an error. */
            var empty = Decode(await Send("GET", "/api/investigation-export?verdict=wrong", null, null));
            Assert.Equal(200, empty.Status);
            Assert.Equal("", empty.Text);
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(
                new InvestigationFeedbackSubmission(request, "wrong", null, null, "web note", "dana"), ct);
            var served = Decode(await Send("GET", "/api/investigation-export", null, null));
            Assert.Equal(200, served.Status);
            var line = JsonNode.Parse(served.Text.TrimEnd('\n'))!;
            Assert.Equal(request.ToString(), (string)line["request_id"]!);
            Assert.Equal("web note", (string)line["feedback"]![0]!["comment"]!);
            Assert.Equal(400, Decode(await Send("GET", "/api/investigation-export?verdict=great", null, null)).Status);
            Assert.Equal(400, Decode(await Send("GET", "/api/investigation-export?since=never", null, null)).Status);
            Assert.Equal(400, Decode(await Send("GET", "/api/investigation-export?since=2026-10-02&until=2026-10-01", null, null)).Status);
            var partial = Decode(await Send("GET", "/api/investigation-export?server=" + InvestigationFeedbackLiveTests.ServerName[..6], null, null));
            Assert.Equal(400, partial.Status);
            var exact = await Send("GET", "/api/investigation-export?server=" + InvestigationFeedbackLiveTests.ServerName.ToLowerInvariant(), null, null);
            Assert.Equal(200, exact.Status);
            Assert.Contains(request.ToString(), Encoding.UTF8.GetString(exact.Raw), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task TheWebDownload_RefusesAViewerAccount_ADashboardWithNoSignIn_AndAViewerStoreLogin_With403()
    {
        async Task Recorded(NpgsqlDataSource owner, Guid request, CancellationToken ct) =>
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(
                new InvestigationFeedbackSubmission(request, "wrong", null, null, "owner note", "dana"), ct);

        /* A viewer account, with a store login that could record. */
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("admin", ViewerSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            await Recorded(owner, request, ct);
            var refused = Decode(await Send("GET", "/api/investigation-export", null, null));
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationExportSeatMessage, (string)JsonNode.Parse(refused.Text)!["error"]!);
            Assert.DoesNotContain(request.ToString(), refused.Text, StringComparison.Ordinal);
        });

        /* A dashboard with no sign-in configured stores no account at all. */
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("admin", null, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            await Recorded(owner, request, ct);
            var refused = Decode(await Send("GET", "/api/investigation-export", null, null));
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationExportSeatMessage, (string)JsonNode.Parse(refused.Text)!["error"]!);
            Assert.DoesNotContain(request.ToString(), refused.Text, StringComparison.Ordinal);
        });

        /* A signed-in admin whose dashboard store login cannot record feedback. */
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("viewer", AdminSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            await Recorded(owner, request, ct);
            var refused = Decode(await Send("GET", "/api/investigation-export", null, null));
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationExportRefusedMessage(false), (string)JsonNode.Parse(refused.Text)!["error"]!);
            Assert.DoesNotContain(request.ToString(), refused.Text, StringComparison.Ordinal);
        });
    }

    /* #6600: the managed store's shape. The web reads as the viewer role and the cases are read on it; the access probe (can this login record
       feedback) runs on the arm for a signed-in admin, so the download is served. */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_IsServedTheDownload_BecauseTheProbeRunsOnTheArm()
    {
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("viewer", AdminSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(request, "wrong", null, null, "owner note", "dana"), ct);
            var served = Decode(await Send("GET", "/api/investigation-export", null, null));
            Assert.Equal(200, served.Status);
            Assert.Equal(request.ToString(), (string)JsonNode.Parse(served.Text.TrimEnd('\n'))!["request_id"]!);
        }, arm: AdminArmShape.Admin, managedStore: true);
    }

    /* #6600: an OIDC viewer seat never opens the arm (its login is refused here, so opening it would fail): the download is refused as today. */
    [Fact]
    public async Task OnTheManagedStore_AViewerSeat_NeverOpensTheArm_AndIsRefusedAsToday()
    {
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("viewer", ViewerSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(request, "wrong", null, null, "owner note", "dana"), ct);
            var refused = Decode(await Send("GET", "/api/investigation-export", null, null));
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationExportSeatMessage, (string)JsonNode.Parse(refused.Text)!["error"]!);
            Assert.DoesNotContain(request.ToString(), refused.Text, StringComparison.Ordinal);
        }, arm: AdminArmShape.Refused, managedStore: true);
    }

    /* #6600: with the switch off (no arm on a managed store) a signed-in admin is refused as today, and the 403 names the switch. */
    [Fact]
    public async Task OnTheManagedStore_WithTheSwitchOff_TheSignedInAdminGets403_NamingTheSwitch()
    {
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("viewer", AdminSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(request, "wrong", null, null, "owner note", "dana"), ct);
            var refused = Decode(await Send("GET", "/api/investigation-export", null, null));
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.InvestigationExportRefusedMessage(true, adminWritesOff: true), (string)JsonNode.Parse(refused.Text)!["error"]!);
            Assert.DoesNotContain(request.ToString(), refused.Text, StringComparison.Ordinal);
        }, managedStore: true);
    }

    [Theory]
    [InlineData("full")]
    [InlineData("metadataOnly")]
    public async Task TheCliFile_AndTheWebDownload_AreByteIdentical_ForTheSameRowsAndSettings(string level)
    {
        var mode = McpDataDetailModes.Resolve(level);
        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("admin", AdminSeat, mode, null, async (owner, request, Send, ct) =>
        {
            var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
            var feedback = new InvestigationFeedbackStore(owner, 30);
            var second = await CaseAsync(owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "resources", 1, ct, credential: CredentialStatement);
            await RecordAsync(feedback, request, "wrong", $"seen WHERE name = '{Secret}' by host-a", ct);
            await RecordAsync(feedback, second, "partly", $"it ran {CredentialStatement}", ct);
            await feedback.RecordAsync(new InvestigationFeedbackSubmission(second, "right", null, null, "second opinion", "erin"), ct);

            var web = await Send("GET", "/api/investigation-export", null, null);
            Assert.Equal(200, web.Status);

            var file = TempFile();
            try
            {
                Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file], out var args, out _));
                using var output = new StringWriter(CultureInfo.InvariantCulture);
                using var error = new StringWriter(CultureInfo.InvariantCulture);
                Assert.Equal(0, await DarlingCliCommands.ExportInvestigationsAsync(owner, args!, mode, output, error, ct));
                var cli = await File.ReadAllBytesAsync(file, ct);
                Assert.Equal(2, Encoding.UTF8.GetString(cli).Split('\n', StringSplitOptions.RemoveEmptyEntries).Length);
                Assert.Equal(cli, web.Raw);
                Assert.DoesNotContain(Credential, Encoding.UTF8.GetString(web.Raw), StringComparison.Ordinal);
            }
            finally
            {
                File.Delete(file);
            }
        });
    }
}

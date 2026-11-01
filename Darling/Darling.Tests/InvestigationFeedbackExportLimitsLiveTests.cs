/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres (the shared web harness), so the class is
   deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The limits of the investigation export (#6236) on a live store: one web export runs at a time and a second answers busy; the
/// read-admission slot is held for the store read and released before the file is sent; the route is heavy from its first call; no
/// work file remains after a send, a refusal, a failure or a client that goes away; Ctrl+C on the command line deletes its partial
/// file; and a case a live practice replays is in neither the export nor another case's stored brief while the practice is live.
/// </summary>
public sealed class InvestigationFeedbackExportLimitsLiveTests
{
    private const string Route = DarlingWebEndpoints.InvestigationExportRoute;
    private const string Credential = "pw-Zq93-NotAWord";
    private static readonly DarlingWebSeat AdminSeat = new("pat@example.test", CanEdit: true);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string NewFolder() => Path.Combine(Path.GetTempPath(), "darling-export-limits-" + Guid.NewGuid().ToString("N"));

    private static string[] Left(string folder) => Directory.Exists(folder) ? Directory.GetFiles(folder) : [];

    [Fact]
    public async Task TheRoute_IsHeavyFromItsFirstCall_AndTakesASlotWithNoHistory()
    {
        var admission = new ReadAdmission();
        admission.Configure(1);
        Assert.True(admission.IsHeavy(ReadSurface.Web, Route, ReadWindowBand.None, out _));
        Assert.False(admission.IsHeavy(ReadSurface.Web, "get_wait_stats", ReadWindowBand.None, out _));

        using var ticket = await admission.AdmitAsync(ReadSurface.Web, Route, ReadWindowBand.None, TestContext.Current.CancellationToken);
        Assert.Null(ticket.Refusal);
        Assert.Equal(1, admission.Stats().Running);
    }

    [Fact]
    public async Task ASecondExport_WhileOneRuns_AnswersBusy_TheSlotIsReleasedBeforeTheSend_AndNothingIsLeftBehind()
    {
        var folder = NewFolder();
        var admission = new ReadAdmission();
        admission.Configure(1);
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var runningAtSend = -1;
        var gate = new InvestigationExportGate(folder)
        {
            BeforeSend = async () =>
            {
                runningAtSend = admission.Stats().Running;
                reached.TrySetResult();
                await release.Task;
            },
        };

        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("admin", AdminSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            await new InvestigationFeedbackStore(owner, 30).RecordAsync(
                new InvestigationFeedbackSubmission(request, "wrong", null, null, "web note", "dana"), ct);

            var first = Send("GET", "/api/investigation-export", null, null);
            await reached.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

            /* The slot covered the store read only: the first export is now sending, and holds none. */
            Assert.Equal(0, runningAtSend);
            using (var other = await admission.AdmitAsync(ReadSurface.Web, "get_wait_stats", ReadWindowBand.SevenDays, ct))
            {
                Assert.Null(other.Refusal);
                Assert.Equal(1, admission.Stats().Running);
            }

            /* One export at a time: the second answers busy, and creates no file. */
            var filesWhileFirstSends = Left(folder).Length;
            var second = await Send("GET", "/api/investigation-export", null, null);
            Assert.Equal(429, second.Status);
            Assert.Contains("already running", Encoding.UTF8.GetString(second.Raw), StringComparison.Ordinal);
            Assert.Equal(filesWhileFirstSends, Left(folder).Length);

            release.SetResult();
            var served = await first;
            Assert.Equal(200, served.Status);
            Assert.Contains(request.ToString(), Encoding.UTF8.GetString(served.Raw), StringComparison.Ordinal);

            /* The gate is free again once the first is done. */
            gate.BeforeSend = null;
            Assert.Equal(200, (await Send("GET", "/api/investigation-export", null, null)).Status);
            Assert.Empty(Left(folder));
        }, gate, admission);

        if (Directory.Exists(folder))
            Directory.Delete(folder, recursive: true);
    }

    [Fact]
    public async Task NoWorkFileRemains_AfterASend_AFailureAClientDisconnect_AndARefusal()
    {
        var folder = NewFolder();
        var gate = new InvestigationExportGate(folder);
        IHttpRequestLifetimeFeature? lifetime = null;

        await InvestigationFeedbackLiveTests.WithRawWebRoutesAsync("admin", AdminSeat, McpDataDetail.Full, null, async (owner, request, Send, ct) =>
        {
            var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
            var feedback = new InvestigationFeedbackStore(owner, 30);
            await feedback.RecordAsync(new InvestigationFeedbackSubmission(request, "wrong", null, null, "web note", "dana"), ct);

            /* A send. */
            Assert.Equal(200, (await Send("GET", "/api/investigation-export", null, null)).Status);
            Assert.Empty(Left(folder));

            /* A failure after the file exists. */
            gate.BeforeSend = () => throw new IOException("the connection went bad");
            Assert.NotEqual(200, (await Send("GET", "/api/investigation-export", null, null)).Status);
            Assert.Empty(Left(folder));

            /* A client that goes away while the file is being sent. */
            gate.BeforeSend = () =>
            {
                lifetime!.Abort();
                return Task.CompletedTask;
            };
            try
            {
                await Send("GET", "/api/investigation-export", null, null);
            }
            catch (OperationCanceledException)
            {
                /* The aborted request ends in a cancellation, which is the point. */
            }

            /* The client sees the abort before the server's handler has unwound and closed its delete-on-close handle, so the file goes
               a moment after the client call returns (#6317: slower under the TimescaleDB store). It must still go: wait for it, bounded. */
            var goneBy = DateTime.UtcNow.AddSeconds(15);
            while (Left(folder).Length > 0 && DateTime.UtcNow < goneBy)
            {
                await Task.Delay(50, ct);
            }

            Assert.Empty(Left(folder));

            /* A refusal: a case the credential-statement check cannot pass refuses the whole download. */
            gate.BeforeSend = null;
            var risky = await InvestigationFeedbackExportLiveTests.CaseAsync(
                owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "resources", 2, ct,
                credential: $"CREATE LOGIN exporter WITH PASSWORD = '{Credential}'", dedup: "risky");
            await feedback.RecordAsync(new InvestigationFeedbackSubmission(risky, "wrong", null, null, "note", "dana"), ct);
            var refused = await Send("GET", "/api/investigation-export", null, null);
            Assert.DoesNotContain(Credential, Encoding.UTF8.GetString(refused.Raw), StringComparison.Ordinal);
            Assert.Empty(Left(folder));

            /* And the gate was given back each time: the next export is not told busy. */
            Assert.NotEqual(429, refused.Status);
        }, gate, null, http => lifetime = http.Features.Get<IHttpRequestLifetimeFeature>());

        if (Directory.Exists(folder))
            Directory.Delete(folder, recursive: true);
    }

    [Fact]
    public async Task CtrlC_OnTheCliVerb_CancelsTheExport_AndDeletesThePartialFile()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var request = await InvestigationFeedbackExportLiveTests.CaseAsync(
            owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct);
        await new InvestigationFeedbackStore(owner, 30).RecordAsync(new InvestigationFeedbackSubmission(request, "wrong", null, null, null, "dana"), ct);

        var file = Path.Combine(Path.GetTempPath(), "darling-export-" + Guid.NewGuid().ToString("N") + ".jsonl");
        try
        {
            Assert.True(DarlingCliCommands.TryParseExportInvestigationsArgs([file], out var args, out _));
            using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var error = new StringWriter(CultureInfo.InvariantCulture);

            await using var holder = await owner.OpenConnectionAsync(ct);
            await using var transaction = await holder.BeginTransactionAsync(ct);
            await using (var lockCommand = new NpgsqlCommand("LOCK TABLE config.investigation_feedback IN ACCESS EXCLUSIVE MODE", holder, transaction))
            {
                await lockCommand.ExecuteNonQueryAsync(ct);
            }

            string[] Partials() => Directory.GetFiles(Path.GetDirectoryName(file)!, "." + Path.GetFileName(file) + ".*.partial");

            /* The token the verb is given is never cancelled: only the key press stops it. */
            var export = DarlingCliCommands.ExportInvestigationsAsync(owner, args!, McpDataDetail.Full, output, error, ct);
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (Partials().Length == 0)
            {
                Assert.True(DateTime.UtcNow < deadline, "The export never created its partial file.");
                await Task.Delay(50, ct);
            }

            InvestigationExportCancelKey.Press();
            Assert.Equal(DarlingCliCommands.InvestigationExportCancelledExitCode, await export);
            await transaction.RollbackAsync(ct);

            Assert.False(File.Exists(file));
            Assert.Empty(Partials());
            Assert.Contains("Cancelled", error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }

    private static Action<JsonObject> BriefListing(Guid replayedCase, Guid unrelatedCase) => brief =>
    {
        brief["earlier_reports"] = new JsonArray(
            new JsonObject { ["request_id"] = replayedCase.ToString(), ["verdict"] = "real", ["root_cause"] = "ROOT-OF-THE-REPLAYED-CASE" },
            new JsonObject { ["request_id"] = unrelatedCase.ToString(), ["verdict"] = "real", ["root_cause"] = "ROOT-OF-ANOTHER-CASE" });
        brief["prior_links"] = new JsonArray(
            new JsonObject { ["request_id"] = replayedCase.ToString(), ["kind"] = "jira", ["url"] = "https://tracker.example.invalid/REPLAYED-1" },
            new JsonObject { ["request_id"] = unrelatedCase.ToString(), ["kind"] = "jira", ["url"] = "https://tracker.example.invalid/OTHER-2" });
    };

    [Fact]
    public async Task ACaseALivePracticeReplays_IsInNeitherTheExportNorAnotherCasesStoredBrief_WhileLive_AndIsBackWhenItEnds()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the investigation export live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = await InvestigationFeedbackLiveTests.MigratedAsync(scratch, ct);
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var feedback = new InvestigationFeedbackStore(owner, 30);
        var unrelated = Guid.NewGuid();

        var replayed = await InvestigationFeedbackExportLiveTests.CaseAsync(
            owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 5, ct, dedup: "replayed-case");
        var later = await InvestigationFeedbackExportLiveTests.CaseAsync(
            owner, store, InvestigationFeedbackLiveTests.ServerId, InvestigationFeedbackLiveTests.ServerName, "blocking", 1, ct,
            dedup: "later-case", shapeBrief: BriefListing(replayed, unrelated));
        await feedback.RecordAsync(new InvestigationFeedbackSubmission(replayed, "wrong", null, null, "judged", "dana"), ct);
        await feedback.RecordAsync(new InvestigationFeedbackSubmission(later, "right", null, null, "judged too", "dana"), ct);

        async Task<(string[] Lines, string LaterBrief, string LaterRead)> ReadBothAsync(bool hide)
        {
            using var bytes = new MemoryStream();
            var outcome = await InvestigationFeedbackExport.WriteAsync(store, null, null, null, null, McpDataDetail.Full, null, bytes, ct);
            Assert.False(outcome.Refused);
            var lines = Encoding.UTF8.GetString(bytes.ToArray()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
            var laterLine = lines.SingleOrDefault(l => l.Contains(later.ToString(), StringComparison.Ordinal)) ?? "";
            var read = await DarlingMcpInvestigationTools.ReportsAsync(owner, McpDataDetail.Full, null, later.ToString(), 24, ct, hideLivePracticeAnswers: hide);
            return (lines, laterLine, read);
        }

        /* No practice yet: both cases, and the stored brief of the later case lists the earlier one. */
        var before = await ReadBothAsync(hide: true);
        Assert.Equal(2, before.Lines.Length);
        Assert.Contains("ROOT-OF-THE-REPLAYED-CASE", before.LaterBrief, StringComparison.Ordinal);
        Assert.Contains("ROOT-OF-THE-REPLAYED-CASE", before.LaterRead, StringComparison.Ordinal);

        /* A practice replays the first case. */
        var practice = (await new InvestigationPracticeStore(owner, 30, () => DateTime.UtcNow).OpenAsync(replayed, "person:lee", ct)).RequestId!.Value;
        var live = await ReadBothAsync(hide: true);
        var onlyLater = Assert.Single(live.Lines);
        Assert.Equal(later.ToString(), (string)JsonNode.Parse(onlyLater)!["request_id"]!);
        foreach (var text in new[] { live.LaterBrief, live.LaterRead })
        {
            Assert.DoesNotContain("ROOT-OF-THE-REPLAYED-CASE", text, StringComparison.Ordinal);
            Assert.DoesNotContain("REPLAYED-1", text, StringComparison.Ordinal);
            Assert.DoesNotContain(replayed.ToString(), text, StringComparison.Ordinal);
            /* What the practice does not replay stays. */
            Assert.Contains("ROOT-OF-ANOTHER-CASE", text, StringComparison.Ordinal);
            Assert.Contains("OTHER-2", text, StringComparison.Ordinal);
        }

        /* An operator's read (no hiding) still sees the stored brief as it was. */
        Assert.Contains("ROOT-OF-THE-REPLAYED-CASE", (await ReadBothAsync(hide: false)).LaterRead, StringComparison.Ordinal);

        /* The practice ends (its claims run out): the case is back in the export and in the later case's brief. */
        await using (var end = owner.CreateCommand($"UPDATE config.investigation_requests SET state = 'open', claim_count = 3 WHERE request_id = '{practice}'"))
        {
            await end.ExecuteNonQueryAsync(ct);
        }

        var after = await ReadBothAsync(hide: true);
        Assert.Equal(2, after.Lines.Length);
        Assert.Contains("ROOT-OF-THE-REPLAYED-CASE", after.LaterBrief, StringComparison.Ordinal);
        Assert.Contains("ROOT-OF-THE-REPLAYED-CASE", after.LaterRead, StringComparison.Ordinal);
    }

    [Fact]
    public void TheReplayFilter_LeavesABriefAloneWhenNothingIsReplayed_AndWithholdsOneItCannotRead()
    {
        var brief = new JsonObject { ["earlier_reports"] = new JsonArray(new JsonObject { ["request_id"] = Guid.NewGuid().ToString() }) }.ToJsonString();
        Assert.Same(brief, InvestigationReplayFilter.WithoutReplayed(brief, new System.Collections.Generic.HashSet<Guid>()));
        Assert.Null(InvestigationReplayFilter.WithoutReplayed("not json", new System.Collections.Generic.HashSet<Guid> { Guid.NewGuid() }));
        Assert.Null(InvestigationReplayFilter.WithoutReplayed(null, new System.Collections.Generic.HashSet<Guid> { Guid.NewGuid() }));
    }
}

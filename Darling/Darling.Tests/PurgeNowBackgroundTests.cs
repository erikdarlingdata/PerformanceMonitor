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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4825: <c>purge_now</c> used to run the whole purge inline on the command loop, unpaced. Every other
/// command waited behind it, and its unpaced deletes could stall collection on every server. It now starts the
/// purge in the SAME slot the daily purge uses (<c>_purgeTask</c>), paced, and answers at once with "started";
/// the totals and each raw table's decision go to the collection log.
///
/// <para>Everything here runs without a store: <see cref="DarlingWorker.TryStartPurgeNow"/> is driven with a
/// blocking <see cref="TaskCompletionSource"/> standing in for the purge (the same way
/// <see cref="ScheduledPurgeLaunchTests"/> drives the daily launch), the background body is driven against a
/// null data source so its store work fails fast and lands on the same fail-soft arms a real outage would, and
/// the two run records are checked through their pure builders and an injected writer.</para>
/// </summary>
public sealed class PurgeNowBackgroundTests
{
    private const string ManualLabel = "Manual purge (purge_now)";

    private static DarlingWorker MakeWorker(out CapturingTestLogger log)
    {
        var captured = new CapturingTestLogger();
        log = captured;
        return new DarlingWorker(
            new WorkerLogger(captured),
            Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());
    }

    private static DarlingWorker MakeWorker() => MakeWorker(out _);

    private static JsonElement Reply(CommandOutcome outcome)
    {
        Assert.NotNull(outcome.ResultJson);
        return JsonDocument.Parse(outcome.ResultJson!).RootElement;
    }

    /* ---- TryStartPurgeNow ------------------------------------------------------------------------------ */

    [Fact]
    public void TryStartPurgeNow_WhenIdle_StartsThePurge_AndReportsStarted()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        var called = false;

        var outcome = worker.TryStartPurgeNow(
            _ =>
            {
                called = true;
                return gate.Task;
            },
            customRetentionDays: null, CancellationToken.None);

        Assert.True(called);
        Assert.True(outcome.Success);
        var reply = Reply(outcome);
        Assert.True(reply.GetProperty("success").GetBoolean());
        Assert.True(reply.GetProperty("started").GetBoolean());
        Assert.False(reply.TryGetProperty("alreadyRunning", out _));
        Assert.Equal(JsonValueKind.Null, reply.GetProperty("customRetentionDays").ValueKind);

        gate.SetResult();
    }

    [Fact]
    public void TryStartPurgeNow_EchoesTheCustomHorizon()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();

        var outcome = worker.TryStartPurgeNow(_ => gate.Task, customRetentionDays: 30, CancellationToken.None);

        Assert.Equal(30, Reply(outcome).GetProperty("customRetentionDays").GetInt32());

        gate.SetResult();
    }

    [Fact]
    public void TryStartPurgeNow_StartedReply_CarriesTheServicesClockAsStartedAtUtc()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();

        var before = DateTime.UtcNow;
        var outcome = worker.TryStartPurgeNow(_ => gate.Task, null, CancellationToken.None);
        var after = DateTime.UtcNow;

        /* #4825: the viewer uses this as the lower bound when it looks for the run's records, so the comparison
           is between the service's clock and the service's clock (collection_time is written from the same
           DateTime.UtcNow, as naive UTC): the "o" form with seven fraction digits and no offset. */
        var stamp = Reply(outcome).GetProperty("startedAtUtc").GetString();
        Assert.NotNull(stamp);
        Assert.Matches(@"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{7}$", stamp);
        var parsed = DateTime.ParseExact(stamp, "o", CultureInfo.InvariantCulture, DateTimeStyles.None);
        Assert.Equal(DateTimeKind.Unspecified, parsed.Kind);
        Assert.InRange(parsed, before, after);

        gate.SetResult();
    }

    [Fact]
    public void TryStartPurgeNow_StartedAtUtc_IsTakenBeforeThePurgeStarts()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        DateTime? launchedAt = null;

        var outcome = worker.TryStartPurgeNow(
            _ =>
            {
                launchedAt = DateTime.UtcNow;
                return gate.Task;
            },
            null, CancellationToken.None);

        /* Every record the run writes is stamped after it started, so a stamp taken any later than the launch
           could sit above the first record and hide it from the viewer's read. */
        Assert.NotNull(launchedAt);
        var stamp = DateTime.ParseExact(
            Reply(outcome).GetProperty("startedAtUtc").GetString()!, "o", CultureInfo.InvariantCulture, DateTimeStyles.None);
        Assert.True(stamp <= launchedAt.Value, $"the stamp {stamp:o} is later than the launch {launchedAt.Value:o}");

        gate.SetResult();
    }

    [Fact]
    public void TryStartPurgeNow_AlreadyRunningReply_CarriesNoStartedAtUtc()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(Reply(worker.TryStartPurgeNow(_ => gate.Task, null, CancellationToken.None)).GetProperty("started").GetBoolean());

        var second = Reply(worker.TryStartPurgeNow(_ => Task.CompletedTask, null, CancellationToken.None));

        /* Nothing started, so there is no run of this caller's to look for: a stamp would send the viewer to
           watch the earlier run's records as if they were its own. */
        Assert.True(second.GetProperty("alreadyRunning").GetBoolean());
        Assert.False(second.TryGetProperty("startedAtUtc", out _));

        gate.SetResult();
    }

    [Fact]
    public async Task TryStartPurgeNow_ReturnsAtOnce_NeverBlockingOnThePurge()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        var ct = TestContext.Current.CancellationToken;

        var call = Task.Run(() => worker.TryStartPurgeNow(_ => gate.Task, null, CancellationToken.None), ct);
        var finished = await Task.WhenAny(call, Task.Delay(TimeSpan.FromSeconds(10), ct));

        /* The gate is never signalled until the assertion below: had the call awaited the purge, it would not
           have finished inside the ten seconds. */
        Assert.Same(call, finished);
        Assert.True(Reply(await call).GetProperty("started").GetBoolean());

        gate.SetResult();
    }

    [Fact]
    public void TryStartPurgeNow_WhileTheDailyPurgeRuns_StartsNothing_AndReportsAlreadyRunning()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(worker.TryStartScheduledPurge(DateTime.UtcNow, _ => gate.Task, CancellationToken.None));

        var manualCalled = false;
        var outcome = worker.TryStartPurgeNow(
            _ =>
            {
                manualCalled = true;
                return Task.CompletedTask;
            },
            customRetentionDays: 7, CancellationToken.None);

        Assert.False(manualCalled);
        Assert.True(outcome.Success);
        var reply = Reply(outcome);
        Assert.True(reply.GetProperty("success").GetBoolean());
        Assert.False(reply.GetProperty("started").GetBoolean());
        Assert.True(reply.GetProperty("alreadyRunning").GetBoolean());
        Assert.Equal(7, reply.GetProperty("customRetentionDays").GetInt32());

        gate.SetResult();
    }

    [Fact]
    public void TryStartPurgeNow_WhileAManualPurgeRuns_StartsNothing_AndReportsAlreadyRunning()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(Reply(worker.TryStartPurgeNow(_ => gate.Task, null, CancellationToken.None)).GetProperty("started").GetBoolean());

        var secondCalled = false;
        var second = worker.TryStartPurgeNow(
            _ =>
            {
                secondCalled = true;
                return Task.CompletedTask;
            },
            null, CancellationToken.None);

        Assert.False(secondCalled);
        Assert.False(Reply(second).GetProperty("started").GetBoolean());
        Assert.True(Reply(second).GetProperty("alreadyRunning").GetBoolean());

        gate.SetResult();
    }

    [Fact]
    public async Task TryStartPurgeNow_StartsAgain_OnceThePreviousPurgeHasCompleted()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(Reply(worker.TryStartPurgeNow(_ => gate.Task, null, CancellationToken.None)).GetProperty("started").GetBoolean());

        gate.SetResult();
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken);

        var secondCalled = false;
        var second = worker.TryStartPurgeNow(
            _ =>
            {
                secondCalled = true;
                return Task.CompletedTask;
            },
            null, CancellationToken.None);

        Assert.True(secondCalled);
        Assert.True(Reply(second).GetProperty("started").GetBoolean());
    }

    [Fact]
    public async Task TryStartPurgeNow_AFailingPurge_IsTrackedNotThrown_AndFreesTheSlot()
    {
        var worker = MakeWorker(out var log);

        var outcome = worker.TryStartPurgeNow(
            _ => throw new InvalidOperationException("purge boom"), null, CancellationToken.None);

        Assert.True(Reply(outcome).GetProperty("started").GetBoolean());
        Assert.Contains(log.Lines, line => line.StartsWith("Error:", StringComparison.Ordinal));

        await Task.Delay(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken);
        var again = worker.TryStartPurgeNow(_ => Task.CompletedTask, null, CancellationToken.None);
        Assert.True(Reply(again).GetProperty("started").GetBoolean());
    }

    [Fact]
    public void TryStartPurgeNow_HandsThePurgeTheStoppingToken_NotAPerCommandOne()
    {
        var worker = MakeWorker();
        using var stopping = new CancellationTokenSource();
        var received = CancellationToken.None;
        var gate = new TaskCompletionSource();

        worker.TryStartPurgeNow(
            token =>
            {
                received = token;
                return gate.Task;
            },
            null, stopping.Token);

        Assert.Equal(stopping.Token, received);
        Assert.False(received.IsCancellationRequested);
        stopping.Cancel();
        Assert.True(received.IsCancellationRequested);

        gate.SetResult();
    }

    [Fact]
    public void TryStartPurgeNow_DoesNotConsumeTheDailySchedule()
    {
        var worker = MakeWorker();

        /* The daily purge has never run (its stamp starts at MinValue). A manual purge that finishes must not
           make it look as though the day's purge already happened. */
        worker.TryStartPurgeNow(_ => Task.CompletedTask, null, CancellationToken.None);
        Assert.True(worker.TryStartScheduledPurge(DateTime.UtcNow, _ => Task.CompletedTask, CancellationToken.None));
    }

    [Fact]
    public async Task TryStartPurgeNow_DoesNotMoveTheDailyStamp_ThatALaunchSet()
    {
        var worker = MakeWorker();
        var start = DateTime.UtcNow;
        Assert.True(worker.TryStartScheduledPurge(start, _ => Task.CompletedTask, CancellationToken.None));
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken);

        Assert.True(Reply(worker.TryStartPurgeNow(_ => Task.CompletedTask, null, CancellationToken.None)).GetProperty("started").GetBoolean());
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken);

        /* 24h after the daily launch is still when the next daily purge is due: not earlier, not a day after the
           manual one. */
        Assert.False(worker.TryStartScheduledPurge(start.AddHours(23), _ => Task.CompletedTask, CancellationToken.None));
        Assert.True(worker.TryStartScheduledPurge(start.AddHours(25), _ => Task.CompletedTask, CancellationToken.None));
    }

    [Fact]
    public async Task TheDailyPurge_SkipsWhileAManualPurgeRuns_AndTriesAgainOnTheNextTick()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(Reply(worker.TryStartPurgeNow(_ => gate.Task, null, CancellationToken.None)).GetProperty("started").GetBoolean());

        var dailyCalled = false;
        var skipped = worker.TryStartScheduledPurge(
            DateTime.UtcNow,
            _ =>
            {
                dailyCalled = true;
                return Task.CompletedTask;
            },
            CancellationToken.None);
        Assert.False(skipped);
        Assert.False(dailyCalled);

        gate.SetResult();
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken);

        /* The skip left the daily stamp alone, so the very next tick launches it. */
        Assert.True(worker.TryStartScheduledPurge(DateTime.UtcNow, _ => Task.CompletedTask, CancellationToken.None));
    }

    [Fact]
    public async Task ADailyAndAManualStartAtOnce_NeverRunTwoPurges()
    {
        /* The launch loop and the command loop are different threads, both checking and then setting the one
           slot. Many rounds, each releasing both starts together, so a check-then-set race between them has
           room to show. */
        var ct = TestContext.Current.CancellationToken;
        var gate = new TaskCompletionSource();

        for (var round = 0; round < 300; round++)
        {
            var worker = MakeWorker();
            var launches = 0;
            using var together = new Barrier(2);

            var manual = Task.Run(
                () =>
                {
                    together.SignalAndWait(ct);
                    worker.TryStartPurgeNow(_ => { Interlocked.Increment(ref launches); return gate.Task; }, null, CancellationToken.None);
                },
                ct);
            var daily = Task.Run(
                () =>
                {
                    together.SignalAndWait(ct);
                    worker.TryStartScheduledPurge(DateTime.UtcNow, _ => { Interlocked.Increment(ref launches); return gate.Task; }, CancellationToken.None);
                },
                ct);
            await Task.WhenAll(manual, daily);

            Assert.Equal(1, launches);
        }

        gate.SetResult();
    }

    /* ---- the background body --------------------------------------------------------------------------- */

    [Fact]
    public async Task TheBackgroundPurge_PacesItsWal_AndNamesItselfManualInTheLog()
    {
        var worker = MakeWorker(out var log);
        var ct = TestContext.Current.CancellationToken;

        await worker.RunPurgeNowBackgroundAsync(
            postgres: null!, timescaleAvailable: false, new DarlingConfig(), customRetentionDays: 30, ct,
            writeRawRunRecord: (_, _, _, _) => Task.CompletedTask);

        /* The pacer reads the checkpoint settings once per run, and says so: only a paced purge builds one. */
        Assert.Single(log.Lines, line => line.Contains("WAL pacing", StringComparison.Ordinal));
        Assert.Contains(
            log.Lines,
            line => line.StartsWith("Information:", StringComparison.Ordinal)
                && line.Contains("Manual purge (purge_now, custom retention 30 day(s))", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheBackgroundPurge_CutShortByAServiceStop_LogsOneWarning_NamingTheRun_AndTheDailyPurge()
    {
        var worker = MakeWorker(out var log);
        using var stop = new CancellationTokenSource();
        await stop.CancelAsync();

        /* The service is stopping: the token is cancelled, and the last write the run makes reports it. */
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => worker.RunPurgeNowBackgroundAsync(
            postgres: null!, timescaleAvailable: true, new DarlingConfig(), customRetentionDays: 30, stop.Token,
            writeRawRunRecord: (_, _, _, token) =>
            {
                token.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }));

        var warning = Assert.Single(
            log.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal) && line.Contains("cut short", StringComparison.Ordinal));
        Assert.Contains("Manual purge (purge_now, custom retention 30 day(s))", warning, StringComparison.Ordinal);
        Assert.Contains("The next daily purge uses the configured retention horizons", warning, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheBackgroundPurge_WhoseRawStepFailsForAnotherReason_IsNotReportedAsCutShort()
    {
        var worker = MakeWorker(out var log);

        await worker.RunPurgeNowBackgroundAsync(
            postgres: null!, timescaleAvailable: true, new DarlingConfig(), customRetentionDays: null,
            TestContext.Current.CancellationToken,
            writeRawRunRecord: (_, _, _, _) => Task.CompletedTask);

        Assert.DoesNotContain(log.Lines, line => line.Contains("cut short", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheBackgroundPurge_OnPlainPostgres_WritesNoRawTableRecord()
    {
        var worker = MakeWorker();
        var written = new List<string>();

        await worker.RunPurgeNowBackgroundAsync(
            postgres: null!, timescaleAvailable: false, new DarlingConfig(), customRetentionDays: null,
            TestContext.Current.CancellationToken,
            writeRawRunRecord: (status, _, message, _) =>
            {
                written.Add($"{status} {message}");
                return Task.CompletedTask;
            });

        /* Plain PostgreSQL has no gate and no rollups: the sweep's DELETE fallback already purged raw there, so
           there is nothing for a raw-table record to say. */
        Assert.Empty(written);
    }

    [Fact]
    public async Task TheBackgroundPurge_WhenTheRawStepFails_RecordsTheGateErrorLine_AtWarning()
    {
        var worker = MakeWorker(out var log);
        var written = new List<(string Status, long DurationMs, string Message)>();

        /* A null data source cannot open the raw step's connection: the same fail-soft arm a store that drops
           mid-purge lands on. The sweep above still ran and is reported; the raw step contributes one
           gate_error line rather than failing the whole purge. */
        await worker.RunPurgeNowBackgroundAsync(
            postgres: null!, timescaleAvailable: true, new DarlingConfig(), customRetentionDays: 30,
            TestContext.Current.CancellationToken,
            writeRawRunRecord: (status, durationMs, message, _) =>
            {
                written.Add((status, durationMs, message));
                return Task.CompletedTask;
            });

        var record = Assert.Single(written);
        Assert.Equal("WARNING", record.Status);
        var lines = record.Message.Split('\n');
        Assert.Contains("Manual purge (purge_now, custom retention 30 day(s))", lines[0], StringComparison.Ordinal);
        Assert.Single(lines, line => line.StartsWith("(all three raw tables): gate_error - ", StringComparison.Ordinal));

        /* And the same line at Information, so the service log carries it too. */
        Assert.Contains(
            log.Lines,
            line => line.StartsWith("Information:", StringComparison.Ordinal)
                && line.Contains("(all three raw tables)", StringComparison.Ordinal)
                && line.Contains("gate_error", StringComparison.Ordinal));
    }

    /* ---- the raw-table run record ---------------------------------------------------------------------- */

    [Fact]
    public void RawRunRecord_HasOneLinePerRelation_WithItsOutcomeAndNote()
    {
        var entries = new List<DarlingWorker.RawPurgeNowEntry>
        {
            new("query_stats", "ran", "The gated purge ran this pass."),
            new("wait_stats", "not_covered", "Held - the rollup does not yet cover the oldest rows."),
            new("file_io_stats", "no_chunks", "Nothing to purge - no chunks exist yet."),
        };

        var (status, message) = DarlingWorker.BuildRawPurgeNowRunRecord(ManualLabel, entries);

        Assert.Equal("SUCCESS", status);
        var lines = message.Split('\n');
        Assert.Equal(4, lines.Length);
        Assert.Contains(ManualLabel, lines[0], StringComparison.Ordinal);
        Assert.Equal("query_stats: ran - The gated purge ran this pass.", lines[1]);
        Assert.Equal("wait_stats: not_covered - Held - the rollup does not yet cover the oldest rows.", lines[2]);
        Assert.Equal("file_io_stats: no_chunks - Nothing to purge - no chunks exist yet.", lines[3]);
    }

    [Theory]
    [InlineData("gate_error")]
    [InlineData("run_failed")]
    public void RawRunRecord_IsAWarning_WhenARelationFailed(string failedOutcome)
    {
        var entries = new List<DarlingWorker.RawPurgeNowEntry>
        {
            new("query_stats", "ran", "The gated purge ran this pass."),
            new("wait_stats", failedOutcome, "It failed."),
        };

        var (status, message) = DarlingWorker.BuildRawPurgeNowRunRecord(ManualLabel, entries);

        Assert.Equal("WARNING", status);
        Assert.Contains($"wait_stats: {failedOutcome} - It failed.", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("hole")]
    [InlineData("epoch_stale")]
    [InlineData("gate_unknown")]
    [InlineData("not_covered")]
    public void RawRunRecord_IsSuccess_WhenTheGateHeldARelationOnPurpose(string heldOutcome)
    {
        /* Holding a relation is the gate doing its job, not a failure to warn about. */
        var entries = new List<DarlingWorker.RawPurgeNowEntry> { new("query_stats", heldOutcome, "Held.") };

        Assert.Equal("SUCCESS", DarlingWorker.BuildRawPurgeNowRunRecord(ManualLabel, entries).Status);
    }

    /* ---- the sweep's run record ------------------------------------------------------------------------ */

    [Fact]
    public void ManualPurgeLabel_NamesTheCommand_AndTheCustomHorizonWhenSet()
    {
        Assert.Equal("Manual purge (purge_now)", DarlingRetention.BuildManualPurgeLabel(null));
        Assert.Equal("Manual purge (purge_now, custom retention 30 day(s))", DarlingRetention.BuildManualPurgeLabel(30));
    }

    [Fact]
    public void SweepRunRecord_WithALabel_SaysItWasAManualPurge_AndKeepsTheStatus()
    {
        var label = DarlingRetention.BuildManualPurgeLabel(14);

        var (status, message) = DarlingRetention.BuildRunRecordSummary(
            tablesPurged: 33, totalRowsDeleted: 1200, totalChunksDropped: 42, tablesFailed: 0,
            paced: true, walBytes: 52_428_800, pacedSeconds: 12, runLabel: label);

        Assert.Equal("SUCCESS", status);
        Assert.StartsWith("Manual purge (purge_now, custom retention 14 day(s)): Purged 33 table(s)", message, StringComparison.Ordinal);
        Assert.Contains("WAL written 50 MB", message, StringComparison.Ordinal);
    }

    [Fact]
    public void SweepRunRecord_WithoutALabel_IsTheDailyPurgesTextUnchanged()
    {
        var (_, message) = DarlingRetention.BuildRunRecordSummary(
            tablesPurged: 33, totalRowsDeleted: 1200, totalChunksDropped: 42, tablesFailed: 0);

        Assert.StartsWith("Purged 33 table(s)", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Manual", message, StringComparison.Ordinal);
    }

    /* ---- the viewer's reading of the reply ------------------------------------------------------------- */

    [Fact]
    public void Viewer_ShowsStarted_ForTheBackgroundReply()
    {
        var text = ViewerServerTab.FormatPurgeSummary(
            "{\"success\":true,\"started\":true,\"customRetentionDays\":null}", out var finished);

        Assert.Contains("Purge started", text, StringComparison.Ordinal);
        Assert.Contains("background", text, StringComparison.Ordinal);
        Assert.Contains("paced", text, StringComparison.Ordinal);
        Assert.False(finished);
    }

    [Fact]
    public void Viewer_ShowsThatAPurgeIsAlreadyRunning()
    {
        var text = ViewerServerTab.FormatPurgeSummary(
            "{\"success\":true,\"started\":false,\"alreadyRunning\":true,\"customRetentionDays\":null}", out var finished);

        Assert.Contains("already running", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Purge started", text, StringComparison.Ordinal);
        Assert.False(finished);
    }

    [Fact]
    public void Viewer_StillFormatsTheTotalsAnOlderServiceReturns()
    {
        var text = ViewerServerTab.FormatPurgeSummary(
            "{\"success\":true,\"tablesPurged\":3,\"rowsPurged\":42,\"rowsDeleted\":40,\"chunksDropped\":2}", out var finished);

        Assert.Equal("Purged 42 row(s)/chunk(s) across 3 table(s)", text);
        Assert.True(finished);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not json")]
    public void Viewer_DegradesToPurgeComplete_WhenTheReplyIsMissingOrUnreadable(string? json)
    {
        Assert.Equal("Purge complete", ViewerServerTab.FormatPurgeSummary(json, out var finished));
        Assert.True(finished);
    }

    private sealed class WorkerLogger(CapturingTestLogger inner) : ILogger<DarlingWorker>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => inner.Log(logLevel, eventId, state, exception, formatter);
    }
}

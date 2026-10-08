/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5592: the retention drain gives way to collection. Every test runs on a fake signal, a fake clock and a delay
/// that only moves that clock, so nothing sleeps. The three rules each have a test that fails without its part: the
/// wait while collection is behind, the pause scaled to the batch, and the wall budget.
/// </summary>
public sealed class RetentionCollectionYieldTests
{
    private sealed class FakeTime
    {
        public double Now;
        public readonly List<string> Events = new();

        public double Clock() => Now;

        /// <summary>A 5 s delay is a re-check wait ("w"); anything else is a post-batch pause ("p").</summary>
        public Task Delay(TimeSpan wait, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Events.Add(Math.Abs(wait.TotalSeconds - RetentionCollectionYield.RecheckSeconds) < 1e-9 ? "w" : "p");
            Now += wait.TotalSeconds;
            return Task.CompletedTask;
        }
    }

    private sealed class ScriptedPressure : ICollectionPressure
    {
        private readonly Queue<string?> _script;
        private readonly string? _afterScript;

        public ScriptedPressure(string? afterScript, params string?[] script)
        {
            _script = new Queue<string?>(script);
            _afterScript = afterScript;
        }

        public int Reads { get; private set; }

        public string? BehindReason()
        {
            Reads++;
            return _script.Count > 0 ? _script.Dequeue() : _afterScript;
        }
    }

    /// <summary>A table of expired rows: each batch takes up to <c>Cap</c> of them and costs <c>BatchSeconds</c> of fake time.</summary>
    private sealed class FakeTable
    {
        private readonly FakeTime _time;
        private readonly Queue<double>? _durations;

        public FakeTable(FakeTime time, int rows, int cap = 100, double batchSeconds = 1, double[]? durations = null)
        {
            _time = time;
            Rows = rows;
            Cap = cap;
            BatchSeconds = batchSeconds;
            _durations = durations is null ? null : new Queue<double>(durations);
        }

        public int Rows { get; private set; }
        public int Cap { get; }
        public double BatchSeconds { get; }
        public int Batches { get; private set; }

        public Task<(int Deleted, int Cap, long WalBytes)> Execute(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Batches++;
            _time.Events.Add("b");
            _time.Now += _durations is { Count: > 0 } ? _durations.Dequeue() : BatchSeconds;
            var deleted = Math.Min(Cap, Rows);
            Rows -= deleted;
            return Task.FromResult((deleted, Cap, 0L));
        }
    }

    private static RetentionCollectionYield GateOn(
        FakeTime time, ICollectionPressure pressure, double budgetSeconds = 1_800, double pauseFactor = RetentionCollectionYield.PauseFactor) =>
        new(pressure, TimeSpan.FromSeconds(budgetSeconds), logger: null, time.Delay, time.Clock, pauseFactor);

    /// <summary>A WAL pacer that never waits and writes no WAL, carrying the gate the way a real paced pass does.</summary>
    private static RetentionWalPacer PacerCarrying(RetentionCollectionYield? gate) =>
        new(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask) { Yield = gate };

    private static Task<int> Drain(FakeTable table, RetentionWalPacer? pacer, CancellationToken cancellationToken) =>
        DarlingRetention.DrainBatchesAsync(table.Execute, pacer, cancellationToken);

    /* ---- rule 1: before each batch, wait while collection is behind ---- */

    [Fact]
    public async Task Drain_WaitsWhileCollectionIsBehind_ThenRunsTheBatch()
    {
        var time = new FakeTime();
        /* The first three reads say behind (the check before the batch, then two re-checks), the fourth says caught up. */
        var pressure = new ScriptedPressure(null, "behind", "behind", "behind");
        var gate = GateOn(time, pressure);
        var table = new FakeTable(time, rows: 50);

        var deleted = await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);

        Assert.Equal(50, deleted);
        Assert.Equal(new[] { "w", "w", "w", "b", "p" }, time.Events);
        Assert.Equal(15, gate.TotalWaitSeconds);
        Assert.Equal(4, pressure.Reads);
    }

    [Fact]
    public async Task Drain_ChecksTheSignalBeforeEveryBatch_NotOnlyTheFirst()
    {
        var time = new FakeTime();
        /* Batch 1 waits once, batch 2 twice, batch 3 once. */
        var pressure = new ScriptedPressure(null, "x", null, "x", "x", null, "x", null);
        var gate = GateOn(time, pressure);
        var table = new FakeTable(time, rows: 250);

        var deleted = await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);

        Assert.Equal(250, deleted);
        Assert.Equal(3, table.Batches);
        Assert.Equal(new[] { "w", "b", "p", "w", "w", "b", "p", "w", "b", "p" }, time.Events);
        Assert.Equal(20, gate.TotalWaitSeconds);
    }

    [Fact]
    public async Task Drain_NeverReadsTheSignalWhenCollectionIsKeepingUp()
    {
        var time = new FakeTime();
        var pressure = new ScriptedPressure(null);
        var table = new FakeTable(time, rows: 250);

        await Drain(table, PacerCarrying(GateOn(time, pressure)), TestContext.Current.CancellationToken);

        Assert.DoesNotContain("w", time.Events);
        Assert.Equal(3, pressure.Reads);
    }

    [Fact]
    public async Task Drain_AFleetThatStaysBehindStillGetsOneBatchPerMaxWait()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure("behind"), budgetSeconds: 100_000);
        var table = new FakeTable(time, rows: 50);

        var deleted = await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);

        Assert.Equal(50, deleted);
        Assert.Equal(RetentionCollectionYield.MaxWaitSeconds, gate.TotalWaitSeconds);
        Assert.Equal(60, time.Events.Count(e => e == "w"));
        Assert.Equal(1, table.Batches);
    }

    /* ---- rule 2: after each batch, pause for the batch's own run time ---- */

    [Fact]
    public async Task Drain_PausesForTheBatchsOwnRunTime()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null));
        var table = new FakeTable(time, rows: 250, durations: new[] { 2.0, 8.0, 30.0 });

        await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);

        /* 2 + 8 + 30 s of batches, and a pause equal to each: the drain ran half the time. */
        Assert.Equal(40, gate.TotalPauseSeconds);
        Assert.Equal(80, time.Now);
        Assert.Equal(new[] { "b", "p", "b", "p", "b", "p" }, time.Events);
    }

    [Fact]
    public async Task Drain_ThePauseScalesWithTheFactor()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null), pauseFactor: 0.5);
        var table = new FakeTable(time, rows: 200, durations: new[] { 4.0, 10.0 });

        await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);

        /* 4 s and 10 s batches, then the empty third batch that proves the table is drained (1 s): half of 15. */
        Assert.Equal(7.5, gate.TotalPauseSeconds);
    }

    [Fact]
    public async Task Drain_AMinutesLongBatchDoesNotStallThePassForAsLong()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null));
        var table = new FakeTable(time, rows: 50, durations: new[] { 200.0 });

        await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);

        Assert.Equal(RetentionCollectionYield.MaxPauseSeconds, gate.TotalPauseSeconds);
    }

    [Fact]
    public async Task Drain_ThePauseIsBeforeTheWalWait_SoTheTwoOverlap()
    {
        /* The WAL pacer refills by elapsed time, so a pause taken before its wait is time its bucket already refilled:
           the order has to be batch, pause, WAL wait. A 4 MiB/s pacer, a 100 MiB batch: the bucket (40 MiB) is 60 MiB
           in debt, a 15 s wait. */
        var time = new FakeTime();
        var pacer = new RetentionWalPacer(
            rateBytesPerSecond: 4 * 1_048_576,
            delay: (wait, _) => { time.Events.Add("wal"); time.Now += wait.TotalSeconds; return Task.CompletedTask; },
            secondsClock: time.Clock)
        {
            Yield = GateOn(time, new ScriptedPressure(null)),
        };

        var deleted = await DarlingRetention.DrainBatchesAsync(
            ct =>
            {
                time.Events.Add("b");
                time.Now += 3;
                return Task.FromResult((10, 100, 100L * 1_048_576));
            },
            pacer,
            TestContext.Current.CancellationToken);

        Assert.Equal(10, deleted);
        Assert.Equal(new[] { "b", "p", "wal" }, time.Events);
        Assert.Equal(15, pacer.TotalWaitSeconds);
    }

    /* ---- rule 3: a wall budget per pass ---- */

    [Fact]
    public async Task Drain_AStoppedPassLeavesTheRowsAndTheNextPassFinishesThem()
    {
        var time = new FakeTime();
        var table = new FakeTable(time, rows: 1_000, cap: 100, batchSeconds: 10);

        /* Pass 1: 50 s budget; each batch is 10 s plus a 10 s pause, so batches start at 0, 20 and 40 s. */
        var gate1 = GateOn(time, new ScriptedPressure(null), budgetSeconds: 50);
        var first = await Drain(table, PacerCarrying(gate1), TestContext.Current.CancellationToken);
        gate1.FinishTable("t", first);

        Assert.Equal(300, first);
        Assert.Equal(700, table.Rows);
        Assert.True(gate1.StoppedOnBudget);
        Assert.Equal("t", gate1.FirstStoppedTable);
        Assert.Equal(0, gate1.TablesNotReached);

        /* Pass 2: a fresh gate, the same table: the rest. */
        var gate2 = GateOn(time, new ScriptedPressure(null), budgetSeconds: 100_000);
        var second = await Drain(table, PacerCarrying(gate2), TestContext.Current.CancellationToken);
        gate2.FinishTable("t", second);

        Assert.Equal(700, second);
        Assert.Equal(0, table.Rows);
        Assert.False(gate2.StoppedOnBudget);
    }

    [Fact]
    public async Task Drain_TimeSpentWaitingCountsAgainstTheBudget()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure("behind"), budgetSeconds: 60);
        var table = new FakeTable(time, rows: 500);

        var deleted = await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);
        gate.FinishTable("t", deleted);

        Assert.Equal(0, deleted);
        Assert.Equal(0, table.Batches);
        Assert.Equal(60, gate.TotalWaitSeconds);
        Assert.True(gate.StoppedOnBudget);
        Assert.Equal(1, gate.TablesNotReached);
    }

    [Fact]
    public async Task Drain_ASpentBudgetSkipsThePauseSoThePassEndsAtOnce()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null), budgetSeconds: 10);
        var table = new FakeTable(time, rows: 1_000, batchSeconds: 10);

        await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "b" }, time.Events);
        Assert.Equal(0, gate.TotalPauseSeconds);
    }

    [Fact]
    public void SkipTableOnBudget_StartsNothingOnceTheBudgetIsSpent_AndCountsTheTable()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null), budgetSeconds: 30);

        Assert.False(gate.SkipTableOnBudget("early"));
        Assert.False(gate.StoppedOnBudget);

        time.Now = 30;
        Assert.True(gate.SkipTableOnBudget("first-skipped"));
        Assert.True(gate.SkipTableOnBudget("second-skipped"));

        Assert.True(gate.StoppedOnBudget);
        Assert.Equal("first-skipped", gate.FirstStoppedTable);
        Assert.Equal(2, gate.TablesNotReached);
    }

    [Fact]
    public void FinishTable_ATableStoppedAfterDeletingRowsIsNotCountedAsNotReached()
    {
        var gate = GateOn(new FakeTime(), new ScriptedPressure(null));

        gate.FinishTable("untouched", 0);
        Assert.False(gate.StoppedOnBudget);

        gate.NoteBudgetStop();
        gate.FinishTable("part-way", 5_000);

        Assert.True(gate.StoppedOnBudget);
        Assert.Equal("part-way", gate.FirstStoppedTable);
        Assert.Equal(0, gate.TablesNotReached);
    }

    /* ---- cancellation ---- */

    [Fact]
    public async Task Drain_CancellationDuringAWaitEndsPromptly()
    {
        /* The real delay and clock: the wait is 5 s long, and the cancel must end it well inside that. */
        var gate = new RetentionCollectionYield(new ScriptedPressure("behind"), TimeSpan.FromMinutes(30));
        var batches = 0;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);

        var drain = DarlingRetention.DrainBatchesAsync(
            ct => { batches++; return Task.FromResult((0, 100, 0L)); }, PacerCarrying(gate), cts.Token);
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));

        var finished = await Task.WhenAny(drain, Task.Delay(TimeSpan.FromSeconds(4), TestContext.Current.CancellationToken));
        Assert.Same(drain, finished);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => drain);
        Assert.Equal(0, batches);
    }

    [Fact]
    public async Task Drain_CancellationBeforeTheFirstBatchRunsNoBatch()
    {
        var time = new FakeTime();
        var table = new FakeTable(time, rows: 500);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Drain(table, PacerCarrying(GateOn(time, new ScriptedPressure(null))), cts.Token));

        Assert.Equal(0, table.Batches);
    }

    /* ---- unpaced callers keep today's behavior ---- */

    [Fact]
    public async Task Drain_ACallerWithoutAGateRunsBatchesBackToBack()
    {
        var time = new FakeTime();
        var table = new FakeTable(time, rows: 250);

        var withPacerNoGate = await Drain(table, PacerCarrying(null), TestContext.Current.CancellationToken);

        Assert.Equal(250, withPacerNoGate);
        Assert.Equal(new[] { "b", "b", "b" }, time.Events);

        var unpacedTable = new FakeTable(time, rows: 250);
        var unpaced = await DarlingRetention.DrainBatchesAsync(
            async ct => { var (d, c, _) = await unpacedTable.Execute(ct); return (d, c); },
            TestContext.Current.CancellationToken);

        Assert.Equal(250, unpaced);
        Assert.DoesNotContain("p", time.Events);
        Assert.DoesNotContain("w", time.Events);
    }

    [Fact]
    public void Describe_IsNullWhenThePassNeitherWaitedNorStopped_AndTheRunRecordTextIsUnchanged()
    {
        var gate = GateOn(new FakeTime(), new ScriptedPressure(null));
        Assert.Null(gate.Describe());

        var plain = DarlingRetention.BuildRunRecordSummary(89, 18_007_201, 0, 0, paced: true, walBytes: 9_230L * 1_048_576, pacedSeconds: 596);
        var withNoNote = DarlingRetention.BuildRunRecordSummary(
            89, 18_007_201, 0, 0, paced: true, walBytes: 9_230L * 1_048_576, pacedSeconds: 596, yieldNote: null);
        Assert.Equal(plain, withNoNote);
    }

    [Fact]
    public async Task Describe_AStoppedPassSaysSo_InTheRunRecordWithoutBecomingAWarning()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null), budgetSeconds: 50);
        var table = new FakeTable(time, rows: 1_000, batchSeconds: 10);
        gate.FinishTable("collect.query_store_interval_wide_legacy", await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken));

        var note = gate.Describe();
        Assert.NotNull(note);
        Assert.Contains("paused 20 s between batches", note, StringComparison.Ordinal);
        Assert.Contains("stopped at its 1-minute time budget in collect.query_store_interval_wide_legacy", note, StringComparison.Ordinal);
        Assert.Contains("the next pass continues from the rows left", note, StringComparison.Ordinal);

        var (status, message) = DarlingRetention.BuildRunRecordSummary(12, 300, 0, 0, paced: true, yieldNote: note);
        Assert.Equal("SUCCESS", status);
        Assert.EndsWith(note!, message, StringComparison.Ordinal);
    }

    /* ---- the real signal ---- */

    [Fact]
    public void Pressure_CollectionIsSettlingForTheFirstTenMinutesAfterStart()
    {
        var start = new DateTime(2026, 10, 8, 19, 14, 57, DateTimeKind.Utc);
        var now = start.AddSeconds(5);
        var pressure = new CollectionPressure(stats: null, () => now, start);

        Assert.Contains("still settling", pressure.BehindReason(), StringComparison.Ordinal);

        now = start + CollectionPressure.SettleWindow - TimeSpan.FromSeconds(1);
        Assert.NotNull(pressure.BehindReason());

        now = start + CollectionPressure.SettleWindow;
        Assert.Null(pressure.BehindReason());
        Assert.Equal(TimeSpan.FromMinutes(10), CollectionPressure.SettleWindow);
    }

    [Fact]
    public void Pressure_ASkippedSlotInTheLastFiveMinutesIsBehind_AndAgesOut()
    {
        var start = new DateTime(2026, 10, 8, 10, 0, 0, DateTimeKind.Utc);
        var now = start.AddHours(1);
        var stats = new FleetGateStats(() => now);
        var pressure = new CollectionPressure(stats, () => now, start);
        Assert.Null(pressure.BehindReason());

        stats.RecordSlot(skipped: 1);
        Assert.Contains("skipped", pressure.BehindReason(), StringComparison.Ordinal);

        now = now.AddMinutes(4);
        Assert.NotNull(pressure.BehindReason());

        now = now.AddMinutes(1);
        Assert.Null(pressure.BehindReason());

        /* A slot that ran on time is not a skip. */
        stats.RecordSlot(skipped: 0);
        Assert.Null(pressure.BehindReason());

        /* The held-slot path the self-alert also counts (#5479). */
        stats.RecordSkippedSlots(3);
        Assert.NotNull(pressure.BehindReason());
    }

    [Fact]
    public void FleetGateStats_SkippedInLastMinutes_ReadsTheSameBucketsTheSelfAlertReads()
    {
        var now = new DateTime(2026, 10, 8, 12, 0, 30, DateTimeKind.Utc);
        var stats = new FleetGateStats(() => now);
        stats.RecordSlot(2);
        now = now.AddMinutes(7);
        stats.RecordSlot(5);
        stats.RecordSlot(0);

        Assert.Equal(5, stats.SkippedInLastMinutes(5));
        Assert.Equal(7, stats.SkippedInLastMinutes(8));
        Assert.Equal(stats.Snapshot().Skipped, stats.SkippedInLastMinutes(FleetGateStats.WindowMinutes));
        Assert.Equal(stats.Snapshot().Skipped, stats.SkippedInLastMinutes(10_000));
        Assert.Equal(0, stats.SkippedInLastMinutes(0));
        Assert.Equal(0, stats.SkippedInLastMinutes(-3));
    }

    [Fact]
    public void Pressure_ABodyRunningHalfAMinuteOrLongerIsBehind()
    {
        var start = new DateTime(2026, 10, 8, 10, 0, 0, DateTimeKind.Utc);
        var now = start.AddHours(1);
        var pressure = new CollectionPressure(stats: null, () => now, start);
        Assert.Null(pressure.BehindReason());

        long ticks = 0;
        pressure.SetBodySource(() => ticks);
        Assert.Null(pressure.BehindReason());

        ticks = now.AddSeconds(-29).Ticks;
        Assert.Null(pressure.BehindReason());

        ticks = now.AddSeconds(-30).Ticks;
        Assert.Contains("collection body has been running for 30 s", pressure.BehindReason(), StringComparison.Ordinal);

        ticks = now.AddSeconds(-95).Ticks;
        Assert.NotNull(pressure.BehindReason());
    }

    [Fact]
    public void Pressure_TheBodyLimitIsHalfTheShortestCadenceAndHalfTheWatchdogsHangRule()
    {
        var shortestSeconds = CollectorScheduleDefaults.All.Values.Where(e => e.FrequencyMinutes > 0).Min(e => e.FrequencyMinutes) * 60;

        Assert.Equal(TimeSpan.FromSeconds(shortestSeconds * 0.5), CollectionPressure.BodyRunLimit);
        /* The hang warning fires at one cadence of execution; the drain gives way at half of it. */
        Assert.Equal(DarlingWorker.SweepWatchdogSeconds, shortestSeconds);
        Assert.Equal(TimeSpan.FromSeconds(DarlingWorker.SweepWatchdogSeconds * CollectionPressure.BodyRunShare), CollectionPressure.BodyRunLimit);
    }

    [Fact]
    public void Worker_OldestRunningBodyTicks_CountsOnlyBodiesThatAreInFlightAndHoldAPermit()
    {
        var worker = MakeWorker();
        var never = new TaskCompletionSource();
        ServerLoopStateFor(1, out var running, never.Task, runStarted: 5_000);
        ServerLoopStateFor(2, out var older, never.Task, runStarted: 2_000);
        ServerLoopStateFor(3, out var queued, never.Task, runStarted: 0);
        ServerLoopStateFor(4, out var finished, Task.CompletedTask, runStarted: 1_000);
        ServerLoopStateFor(5, out var neverLaunched, null, runStarted: 0);

        Assert.Equal(0, worker.OldestRunningBodyTicks());

        worker.SetBodySnapshotForTest(new[] { running, queued, finished, neverLaunched });
        Assert.Equal(5_000, worker.OldestRunningBodyTicks());

        worker.SetBodySnapshotForTest(new[] { running, older, queued, finished, neverLaunched });
        Assert.Equal(2_000, worker.OldestRunningBodyTicks());

        worker.SetBodySnapshotForTest(new[] { queued, finished, neverLaunched });
        Assert.Equal(0, worker.OldestRunningBodyTicks());
    }

    [Fact]
    public void ABudgetStopIsNormalControlFlow_TheWarmUpAfterTheDrainStillRuns()
    {
        /* #5585's warm-up runs for the interval tables whose purge deleted rows, and a pass stopped part-way through a
           table did delete rows. The stop must therefore fall through to the run record and the warm-up: no return
           and no throw between the budget's log line and the warm-up call. */
        var source = ReadSource("DarlingRetention.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        var stopLog = source.IndexOf("if (yieldGate is { StoppedOnBudget: true })", StringComparison.Ordinal);
        var warmUp = source.IndexOf("QueryStoreIntervalFloorWarmUp.RunAfterDrainAsync(", StringComparison.Ordinal);
        Assert.True(stopLog > 0 && warmUp > stopLog, "the budget's log line must come before the warm-up call");

        var between = source[stopLog..warmUp];
        Assert.DoesNotContain("return ", between, StringComparison.Ordinal);
        Assert.DoesNotContain("throw ", between, StringComparison.Ordinal);
        Assert.DoesNotContain("throw;", between, StringComparison.Ordinal);
        Assert.Contains("LogRetentionRunAsync", between, StringComparison.Ordinal);
    }

    [Fact]
    public void Worker_BothPurgeCallersPassTheSignal()
    {
        var source = ReadWorkerSource().Replace("\r\n", "\n", StringComparison.Ordinal);

        /* The daily sweep (RunScheduledPurgeAsync) and the on-demand purge_now (RunPurgeNowBackgroundAsync). */
        Assert.Equal(2, CountOf(source, "collectionPressure: _collectionPressure"));
        Assert.Contains("paceWal: true,\n            collectionPressure: _collectionPressure", source, StringComparison.Ordinal);
        Assert.Contains("runLabel: runLabel,\n                collectionPressure: _collectionPressure", source, StringComparison.Ordinal);
        Assert.Contains("Volatile.Write(ref _bodySnapshot, sweepTargets);", source, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static DarlingWorker MakeWorker() =>
        new(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingWorker>.Instance,
            Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());

    private static void ServerLoopStateFor(int id, out DarlingWorker.ServerLoopState state, Task? inFlight, long runStarted)
    {
        var host = $"yield-test-{id}";
        state = new DarlingWorker.ServerLoopState
        {
            Config = new MonitoredServer { Name = host, Host = host },
            InFlightSweep = inFlight,
            RunStartedTicks = runStarted,
        };
    }

    private static string ReadWorkerSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "") =>
        ReadSource("DarlingWorker.cs", thisFile);

    private static string ReadSource(string fileName, [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", fileName);
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative}");
    }
}

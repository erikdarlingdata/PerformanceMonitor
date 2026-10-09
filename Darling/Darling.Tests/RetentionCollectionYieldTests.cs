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
using PerformanceMonitor.Darling.Service;
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

    /// <summary>One table's purge the way <c>PurgeOneAsync</c> does it: the entry check, the drain, then the table's record.</summary>
    private static async Task<int> PurgeLikeTheSweep(RetentionCollectionYield gate, FakeTable table, string name, CancellationToken cancellationToken)
    {
        if (gate.SkipTableOnBudget(name))
        {
            return 0;
        }

        var drained = await Drain(table, PacerCarrying(gate), cancellationToken);
        gate.FinishTable(name, drained);
        return drained;
    }

    [Fact]
    public async Task Pass_UnderASignalThatAlwaysReadsBehind_StillGivesEveryTableItsBatches_AndTheWaitsStopAtHalfTheBudget()
    {
        /* #5595 H1: 40 tables of one batch each, a signal that never clears, the real 30-minute budget. Uncapped, each
           table's first batch waits 300 s and the pass starts 6 tables. Capped, the waits stop at 900 s (half the budget)
           and the rest of the pass keeps only the pauses. */
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure("behind"));
        var tables = Enumerable.Range(0, 40).Select(_ => new FakeTable(time, rows: 50)).ToList();

        for (var i = 0; i < tables.Count; i++)
        {
            await PurgeLikeTheSweep(gate, tables[i], "t" + i, TestContext.Current.CancellationToken);
        }

        Assert.All(tables, t => Assert.Equal(1, t.Batches));
        Assert.All(tables, t => Assert.Equal(0, t.Rows));
        Assert.False(gate.StoppedOnBudget);
        Assert.Equal(0, gate.TablesNotReached);
        Assert.Equal(900, gate.TotalWaitSeconds);
        Assert.Equal(180, time.Events.Count(e => e == "w"));
        Assert.True(gate.WaitsCapped);

        /* After the cap there is no wait at all, only batch and pause. */
        var lastWait = time.Events.FindLastIndex(e => e == "w");
        Assert.All(time.Events.Skip(lastWait + 1), e => Assert.NotEqual("w", e));
        Assert.Equal(40, gate.TotalPauseSeconds);
    }

    [Fact]
    public async Task Pass_TheCapCutsAWaitShort_WhenTheTotalReachesHalfTheBudgetInTheMiddleOfOne()
    {
        /* A 700 s budget caps the waits at 350 s: the first table waits its 300 s, the second only 50 s of its 300, and
           the third none. */
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure("behind"), budgetSeconds: 700);
        var tables = Enumerable.Range(0, 3).Select(_ => new FakeTable(time, rows: 50)).ToList();

        for (var i = 0; i < tables.Count; i++)
        {
            await PurgeLikeTheSweep(gate, tables[i], "t" + i, TestContext.Current.CancellationToken);
        }

        Assert.All(tables, t => Assert.Equal(1, t.Batches));
        Assert.Equal(350, gate.WaitCapSeconds);
        Assert.Equal(350, gate.TotalWaitSeconds);
        Assert.Equal(70, time.Events.Count(e => e == "w"));
    }

    [Fact]
    public async Task Pass_SaysOncePerPassThatItStoppedWaiting()
    {
        var time = new FakeTime();
        var log = new CapturingTestLogger();
        var gate = new RetentionCollectionYield(
            new ScriptedPressure("behind"), TimeSpan.FromSeconds(1_800), log, time.Delay, time.Clock, RetentionCollectionYield.PauseFactor);

        for (var i = 0; i < 10; i++)
        {
            await PurgeLikeTheSweep(gate, new FakeTable(time, rows: 50), "t" + i, TestContext.Current.CancellationToken);
        }

        Assert.Equal(1, log.Lines.Count(l => l.Contains("stops waiting and keeps only the pause", StringComparison.Ordinal)));
        var note = gate.Describe();
        Assert.NotNull(note);
        Assert.Contains("stopped waiting at 15 minutes, half the time budget", note, StringComparison.Ordinal);
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
        /* The WAL pacer refills by elapsed time, so a pause taken before its wait is time its bucket already refilled.
           A 4 MiB/s pacer whose bucket (40 MiB) is already spent, then a 3 s batch that writes 24 MiB: the batch alone
           leaves a 12 MiB debt after its own 3 s of refill, a 3 s wait. With the 3 s pause before the wait the refill is
           24 MiB, so the debt is gone and the pacer waits 0 s: the pause absorbed the wait instead of adding to it. */
        const long MiB = 1_048_576;
        async Task<(double WalWait, List<string> Events)> Run(bool withPause)
        {
            var time = new FakeTime();
            var pacer = new RetentionWalPacer(
                rateBytesPerSecond: 4 * MiB,
                delay: (wait, _) => { time.Events.Add("wal"); time.Now += wait.TotalSeconds; return Task.CompletedTask; },
                secondsClock: time.Clock);
            if (withPause)
            {
                pacer.Yield = GateOn(time, new ScriptedPressure(null));
            }

            await pacer.AfterBatchAsync(40 * MiB, TestContext.Current.CancellationToken);
            var spentWait = pacer.TotalWaitSeconds;
            var deleted = await DarlingRetention.DrainBatchesAsync(
                ct =>
                {
                    time.Events.Add("b");
                    time.Now += 3;
                    return Task.FromResult((10, 100, 24 * MiB));
                },
                pacer,
                TestContext.Current.CancellationToken);

            Assert.Equal(10, deleted);
            return (pacer.TotalWaitSeconds - spentWait, time.Events);
        }

        var without = await Run(withPause: false);
        Assert.Equal(3, without.WalWait);
        Assert.Equal(new[] { "b", "wal" }, without.Events);

        var with = await Run(withPause: true);
        Assert.Equal(0, with.WalWait);
        Assert.Equal(new[] { "b", "p" }, with.Events);
    }

    [Fact]
    public async Task Drain_ABatchThatThrowsStillGetsItsPause_ThenTheErrorReachesTheCaller()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null));

        await Assert.ThrowsAsync<InvalidOperationException>(() => DarlingRetention.DrainBatchesAsync(
            ct => { time.Events.Add("b"); time.Now += 3; throw new InvalidOperationException("statement timeout"); },
            PacerCarrying(gate),
            TestContext.Current.CancellationToken));

        Assert.Equal(new[] { "b", "p" }, time.Events);
        Assert.Equal(3, gate.TotalPauseSeconds);
    }

    [Fact]
    public async Task Drain_CancellationDuringThePauseEndsTheDrain()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var batches = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingRetention.DrainBatchesAsync(
            ct => { batches++; time.Events.Add("b"); time.Now += 3; cts.Cancel(); return Task.FromResult((100, 100, 0L)); },
            PacerCarrying(gate),
            cts.Token));

        /* The pause's delay saw the cancelled token: no pause was taken and no second batch ran. */
        Assert.Equal(1, batches);
        Assert.Equal(new[] { "b" }, time.Events);
        Assert.Equal(0, gate.TotalPauseSeconds);
    }

    [Fact]
    public async Task Drain_ABatchThatThrowsOnACancelledTokenGetsNoPause()
    {
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null));
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingRetention.DrainBatchesAsync(
            ct => { cts.Cancel(); ct.ThrowIfCancellationRequested(); return Task.FromResult((0, 100, 0L)); },
            PacerCarrying(gate),
            cts.Token));

        Assert.Equal(0, gate.TotalPauseSeconds);
        Assert.Empty(time.Events);
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
        /* A 60 s budget: the wait before the first batch is capped at half of it (30 s), then each batch is 10 s plus a
           10 s pause. Without the wait three batches would start (at 0, 20 and 40 s); with it the second starts at 50 s
           and the third never does. */
        var gate = GateOn(time, new ScriptedPressure("behind"), budgetSeconds: 60);
        var table = new FakeTable(time, rows: 1_000, cap: 100, batchSeconds: 10);

        var deleted = await Drain(table, PacerCarrying(gate), TestContext.Current.CancellationToken);
        gate.FinishTable("t", deleted);

        Assert.Equal(200, deleted);
        Assert.Equal(2, table.Batches);
        Assert.Equal(30, gate.TotalWaitSeconds);
        Assert.True(gate.StoppedOnBudget);
        Assert.Equal(0, gate.TablesNotReached);
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
        Assert.Equal(2, gate.TablesLeft);
    }

    [Fact]
    public async Task Describe_AStopAtATablesEntryReadsBefore_NotIn_AndAStopInsideATableReadsIn()
    {
        /* #5595 L1: the budget runs out during table A's last batch, A ends normally, and B is the first table the pass
           never started. The record must not say the pass stopped "in" B. */
        var time = new FakeTime();
        var gate = GateOn(time, new ScriptedPressure(null), budgetSeconds: 20);
        await PurgeLikeTheSweep(gate, new FakeTable(time, rows: 50, batchSeconds: 25), "table_a", TestContext.Current.CancellationToken);
        await PurgeLikeTheSweep(gate, new FakeTable(time, rows: 50), "table_b", TestContext.Current.CancellationToken);
        await PurgeLikeTheSweep(gate, new FakeTable(time, rows: 50), "table_c", TestContext.Current.CancellationToken);

        Assert.Equal("before table_b", gate.StoppedPlace);
        Assert.Equal(2, gate.TablesNotReached);
        Assert.Equal(2, gate.TablesLeft);
        Assert.Contains("time budget before table_b, with 2 table(s) not reached", gate.Describe(), StringComparison.Ordinal);

        /* Stopped part-way through a table: "in". */
        var time2 = new FakeTime();
        var gate2 = GateOn(time2, new ScriptedPressure(null), budgetSeconds: 20);
        await PurgeLikeTheSweep(gate2, new FakeTable(time2, rows: 1_000, batchSeconds: 25), "table_x", TestContext.Current.CancellationToken);
        Assert.Equal("in table_x", gate2.StoppedPlace);
        Assert.Equal(1, gate2.TablesLeft); /* #5592: stopped part-way still has rows left, though it is not "not reached" */
        Assert.Equal(0, gate2.TablesNotReached);
        Assert.Contains("time budget in table_x", gate2.Describe(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Describe_APassThatOnlyPausedReadsAsBefore_AndAPassThatWaitedSaysSo()
    {
        /* #5595 L2: the pause after a batch happens on every paced pass; it is not pressure and does not write the sentence. */
        var time = new FakeTime();
        var calm = GateOn(time, new ScriptedPressure(null));
        await PurgeLikeTheSweep(calm, new FakeTable(time, rows: 250, batchSeconds: 2), "t", TestContext.Current.CancellationToken);
        Assert.True(calm.TotalPauseSeconds > 0);
        Assert.Null(calm.Describe());
        Assert.Equal(0, calm.TablesLeft);

        var time2 = new FakeTime();
        var pushed = GateOn(time2, new ScriptedPressure(null, "behind", "behind"));
        await PurgeLikeTheSweep(pushed, new FakeTable(time2, rows: 50), "t", TestContext.Current.CancellationToken);
        Assert.Contains("yielded to collection: waited 10 s while it was behind, paused 1 s between batches", pushed.Describe(), StringComparison.Ordinal);
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
    public void Pressure_CollectionIsSettlingForTheFirstTenMinutesAfterCollectionStarts()
    {
        double seconds = 100_000;
        var pressure = new CollectionPressure(stats: null, () => seconds);
        pressure.MarkCollectionStarted();

        seconds += 5;
        Assert.Contains("still settling", pressure.BehindReason(), StringComparison.Ordinal);

        seconds = 100_000 + CollectionPressure.SettleWindow.TotalSeconds - 1;
        Assert.NotNull(pressure.BehindReason());

        seconds = 100_000 + CollectionPressure.SettleWindow.TotalSeconds;
        Assert.Null(pressure.BehindReason());
        Assert.Equal(TimeSpan.FromMinutes(10), CollectionPressure.SettleWindow);

        /* The first mark wins: a second call does not restart the window. */
        pressure.MarkCollectionStarted();
        Assert.Null(pressure.BehindReason());
    }

    [Fact]
    public void Pressure_TheSettleWindowStartsAtTheMark_NotWhenThePressureWasBuilt()
    {
        /* #5595 M2: the worker is built, then the store retries and migrations take 25 minutes, then the loop starts
           collecting. The window runs from the loop's start, not the construction. Until then it reads settling. */
        double seconds = 0;
        var pressure = new CollectionPressure(stats: null, () => seconds);

        seconds = 1_500;
        Assert.Contains("has not started", pressure.BehindReason(), StringComparison.Ordinal);

        pressure.MarkCollectionStarted();
        seconds = 1_500 + 599;
        Assert.Contains("still settling", pressure.BehindReason(), StringComparison.Ordinal);

        seconds = 1_500 + 600;
        Assert.Null(pressure.BehindReason());
    }

    [Fact]
    public void Pressure_TheSettleClockIsMonotonic_AndTheWorkerMarksTheStartWhereTheLoopBegins()
    {
        /* No wall clock in the pressure read: a backward step of the clock must not hold the drain, so it carries no DateTime. */
        var pressureSource = ReadSource("RetentionCollectionYield.cs");
        Assert.DoesNotContain("DateTime", pressureSource, StringComparison.Ordinal);

        var worker = ReadWorkerSource().Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Equal(1, CountOf(worker, "_collectionPressure.MarkCollectionStarted();"));
        Assert.Equal(1, CountOf(worker, "new CollectionPressure(_fleetGateStats);"));
        var loopStarted = worker.IndexOf("PerformanceMonitor Darling collection loop started", StringComparison.Ordinal);
        var mark = worker.IndexOf("_collectionPressure.MarkCollectionStarted();", StringComparison.Ordinal);
        var loop = worker.IndexOf("while (!stoppingToken.IsCancellationRequested)", mark, StringComparison.Ordinal);
        Assert.True(loopStarted > 0 && mark > loopStarted && loop > mark, "the mark must sit between the loop's start log and the sweep loop");
        Assert.DoesNotContain("while (", worker[mark..loop], StringComparison.Ordinal);
    }

    /// <summary>The start of the fake hour: well past the settle window, on a half-minute so a minute never straddles a read.</summary>
    private static readonly DateTime HourStart = new(2026, 10, 8, 10, 0, 30, DateTimeKind.Utc);

    /// <summary>A pressure read whose collection started two hours ago, so the settle window is long over.</summary>
    private static CollectionPressure Settled(FleetGateStats? stats)
    {
        var seconds = 0.0;
        var pressure = new CollectionPressure(stats, () => seconds);
        pressure.MarkCollectionStarted();
        seconds = 7_200;
        return pressure;
    }

    /// <summary>
    /// Plays one hour a minute at a time and re-checks the signal once a minute (the drain re-checks every 5 s, so
    /// this is a coarser read of the same buckets). <paramref name="runPerMinute"/> and <paramref name="skippedPerMinute"/>
    /// give each minute's slots; returns how many of the 60 re-checks read "behind".
    /// </summary>
    private static int PlayHour(Func<int, int> runPerMinute, Func<int, int> skippedPerMinute, out FleetGateSnapshot hour)
    {
        var now = HourStart;
        var stats = new FleetGateStats(() => now);
        var pressure = Settled(stats);
        var behind = 0;
        for (var minute = 0; minute < 60; minute++)
        {
            now = HourStart.AddMinutes(minute);
            var skipped = skippedPerMinute(minute);
            for (var i = 0; i < runPerMinute(minute); i++)
            {
                /* The first slot of the minute carries that minute's skips, as a late run does. */
                stats.RecordSlot(i == 0 ? skipped : 0);
            }

            if (pressure.BehindReason() is not null)
            {
                behind++;
            }
        }

        hour = stats.Snapshot();
        return behind;
    }

    [Fact]
    public void Pressure_ANormalHourWithThreeHangEpisodes_NeverReadsBehind()
    {
        /* The field's normal day: about 3 "skipping relaunch" episodes an hour, each stepping over a few slots, about
           1,250 slots due an hour (21 a minute). One episode alone never reaches the 5-slot minimum, and the hour
           stays well under the alert's 5%. The old rule (any skipped slot in 5 minutes) read behind for 15 of these 60 re-checks. */
        var episodes = new Dictionary<int, int> { [8] = 3, [31] = 4, [52] = 3 };
        var behind = PlayHour(_ => 21, m => episodes.GetValueOrDefault(m), out var hour);

        Assert.Equal(0, behind);
        Assert.Equal(10, hour.Skipped);
        Assert.InRange(hour.Due, 1_200, 1_300);
        Assert.False(new DarlingSelfAlertEvaluator.FleetGateReport(hour.Run, hour.Skipped, 0, TimeSpan.Zero, TimeSpan.Zero, 1, HourStart).IsBehind);
    }

    [Fact]
    public void Pressure_ANormalHourWhereTwoEpisodesLandTogether_ReadsBehindOnlyWhileBothAreInTheWindow()
    {
        /* Episodes at minutes 5 and 12 (3 slots each) put 6 skipped slots, 2.8% of about 216 due, in one 10-minute
           window from minute 12 until the first one ages out at minute 15: 3 of 60 re-checks, not a fifth of them. */
        var episodes = new Dictionary<int, int> { [5] = 3, [12] = 3, [30] = 3, [45] = 3 };
        var behind = PlayHour(_ => 21, m => episodes.GetValueOrDefault(m), out _);

        Assert.Equal(3, behind);
    }

    [Fact]
    public void Pressure_TheIncidentHour_ReadsBehindThroughout()
    {
        /* The pre-fix drain hour: 97 of 1,252 due slots skipped (7.7%). 2 skipped a minute for 37 minutes, then 1. */
        var behind = PlayHour(m => m < 15 ? 20 : 19, m => m < 37 ? 2 : 1, out var hour);

        Assert.Equal(97, hour.Skipped);
        Assert.Equal(1_252, hour.Due);
        Assert.True(hour.SkippedPercent > 7.5);
        Assert.True(new DarlingSelfAlertEvaluator.FleetGateReport(hour.Run, hour.Skipped, 0, TimeSpan.Zero, TimeSpan.Zero, 1, HourStart).IsBehind);
        /* Behind from the third minute on (the 5-slot minimum is reached after three minutes of skips at 2 a minute). */
        Assert.True(behind >= 57, $"the incident hour read behind in only {behind} of 60 re-checks");
    }

    [Theory]
    [InlineData(195, 5, true)]    /* 5 of 200 = 2.5% exactly: counts */
    [InlineData(196, 5, false)]   /* 5 of 201 = 2.49%: under the share */
    [InlineData(194, 6, true)]    /* 6 of 200 = 3% */
    [InlineData(203, 5, false)]   /* 5 of 208 = 2.4%: at the field's rate the share, not the minimum, decides */
    [InlineData(202, 6, true)]    /* 6 of 208 = 2.88% */
    [InlineData(18, 2, false)]    /* 2 of 20 = 10%: a big share of a thin window, but under the 3-slot minimum */
    [InlineData(17, 3, true)]     /* 3 of 20 = 15%: at the minimum */
    [InlineData(16, 4, true)]     /* 4 of 20 = 20% */
    [InlineData(0, 0, false)]     /* nothing due */
    public void IsBehind_NeedsTheMinimumCountAndTheShare(long run, long skipped, bool expected)
    {
        Assert.Equal(expected, CollectionPressure.IsBehind(run, skipped));
    }

    [Fact]
    public void Pressure_ASustainedRateJustUnderTheMinimum_NeverHoldsTheDrain_AndStaysUnderTheAlert()
    {
        /* A thin fleet (2 slots a minute, 20 in a window) with (minimum - 1) = 2 skipped slots landing in each of the
           hour's six 10-minute windows: never behind, and the hour's total (12) is under the alert's minimum. One more
           slot in a window holds the drain. */
        var justUnder = PlayHour(_ => 2, minute => minute % 10 == 0 ? (int)(CollectionPressure.SkipMinCount - 1) : 0, out var hour);
        Assert.Equal(0, justUnder);
        Assert.Equal(6 * (CollectionPressure.SkipMinCount - 1), hour.Skipped);
        Assert.True(hour.Skipped < DarlingSelfAlertEvaluator.FleetGateBehindMinSkipped);

        var atMinimum = PlayHour(_ => 2, minute => minute % 10 == 0 ? (int)CollectionPressure.SkipMinCount : 0, out _);
        Assert.True(atMinimum > 0);
    }

    [Fact]
    public void Pressure_TheSkippedSlotsAgeOutOfTheWindowAfterTenMinutes()
    {
        var now = HourStart;
        var stats = new FleetGateStats(() => now);
        var pressure = Settled(stats);
        for (var i = 0; i < 200; i++)
        {
            stats.RecordSlot(0);
        }

        /* The held-slot path the self-alert also counts (#5479). */
        stats.RecordSkippedSlots(6);
        var reason = pressure.BehindReason();
        Assert.NotNull(reason);
        Assert.Contains("6 of 206 collector slots were skipped in the last 10 minutes", reason, StringComparison.Ordinal);

        now = now.AddMinutes(9);
        Assert.NotNull(pressure.BehindReason());

        now = now.AddMinutes(1);
        Assert.Null(pressure.BehindReason());
    }

    [Fact]
    public void Pressure_AWorkerBuiltWithoutStatsReadsNoSkips()
    {
        Assert.Null(Settled(stats: null).BehindReason());
    }

    [Fact]
    public void Pressure_TheConstantsAreTheDocumentedOnes()
    {
        Assert.Equal(10, CollectionPressure.SkipWindowMinutes);
        Assert.Equal(3, CollectionPressure.SkipMinCount);
        /* Half the alert's 5%: the drain backs off before the hour reaches the alert. */
        Assert.Equal(25, CollectionPressure.SkipSharePerMille);
        Assert.Equal(DarlingSelfAlertEvaluator.FleetGateBehindPercent * 10 / 2, CollectionPressure.SkipSharePerMille);
        /* #5595 M1: the alert's 60 buckets are six disjoint windows. A drain held just under the minimum lets
           (minimum - 1) slots skip per window, plus one slot of slop between reads: that hour must stay under the alert's
           minimum, and the next minimum up must not (so 3 is the largest). */
        var windowsPerHour = 60 / CollectionPressure.SkipWindowMinutes;
        Assert.Equal(6, windowsPerHour);
        Assert.True(
            (CollectionPressure.SkipMinCount - 1 + 1) * windowsPerHour < DarlingSelfAlertEvaluator.FleetGateBehindMinSkipped,
            "the worst hour a drain held just under the minimum can sustain must stay under the alert's minimum");
        Assert.True(
            (CollectionPressure.SkipMinCount + 1) * windowsPerHour >= DarlingSelfAlertEvaluator.FleetGateBehindMinSkipped,
            "the minimum is the largest that stays under the alert's");
        Assert.Equal(0.5, RetentionCollectionYield.WaitBudgetFraction);
        /* One wait is half the window: a burst can hold the signal for the whole window, but a batch goes every 5 minutes. */
        Assert.Equal(300, RetentionCollectionYield.MaxWaitSeconds);
        Assert.Equal(CollectionPressure.SkipWindowMinutes * 60 / 2, RetentionCollectionYield.MaxWaitSeconds);
    }

    [Fact]
    public void FleetGateStats_SlotsInLastMinutes_ReadsTheSameBucketsTheSelfAlertReads()
    {
        var now = new DateTime(2026, 10, 8, 12, 0, 30, DateTimeKind.Utc);
        var stats = new FleetGateStats(() => now);
        stats.RecordSlot(2);
        now = now.AddMinutes(7);
        stats.RecordSlot(5);
        stats.RecordSlot(0);

        Assert.Equal((2L, 5L), stats.SlotsInLastMinutes(5));
        Assert.Equal((3L, 7L), stats.SlotsInLastMinutes(8));
        var all = stats.Snapshot();
        Assert.Equal((all.Run, all.Skipped), stats.SlotsInLastMinutes(FleetGateStats.WindowMinutes));
        Assert.Equal((all.Run, all.Skipped), stats.SlotsInLastMinutes(10_000));
        Assert.Equal((0L, 0L), stats.SlotsInLastMinutes(0));
        Assert.Equal((0L, 0L), stats.SlotsInLastMinutes(-3));
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

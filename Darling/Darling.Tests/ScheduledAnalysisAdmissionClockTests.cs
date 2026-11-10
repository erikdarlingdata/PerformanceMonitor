/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8539: the admission of a due scheduled analysis pass, asked at fixed times. The due half of the step takes its clock as a
/// parameter, so every ask here names its own instant: a stamp ahead of the clock, the give-way limit to the millisecond,
/// the wait a body must reach before a pass gives way to it, an interval shortened while a stamp written under the longer
/// one is still standing, and a pass that stays refused for longer than an interval. The passes are stand-ins the test
/// holds open and ends, and a waiting body is stated with the instant it started waiting.
/// </summary>
public sealed class ScheduledAnalysisAdmissionClockTests
{
    private static readonly DateTime T0 = new(2026, 11, 10, 14, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Tick = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>The stand-in for the pass: records each start, and keeps the pass open until the test ends it.</summary>
    private sealed class HeldPasses
    {
        private readonly object _lock = new();
        private readonly List<string> _started = new();
        private readonly List<TaskCompletionSource> _open = new();

        internal IReadOnlyList<string> Started
        {
            get
            {
                lock (_lock)
                {
                    return _started.ToArray();
                }
            }
        }

        internal Task Run(DarlingWorker.ServerLoopState server, CancellationToken token)
        {
            var open = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_lock)
            {
                _started.Add(server.Config.Name);
                _open.Add(open);
            }

            return open.Task;
        }

        internal void EndAll()
        {
            TaskCompletionSource[] open;
            lock (_lock)
            {
                open = _open.ToArray();
                _open.Clear();
            }

            foreach (var pass in open)
            {
                pass.SetResult();
            }
        }
    }

    private static (DarlingWorker Worker, HeldPasses Passes, CapturingTestLogger Log) NewWorker()
    {
        var log = new CapturingTestLogger();
        var worker = SelfAlertFleetPassTests.MakeWorker(log);
        var passes = new HeldPasses();
        worker.ScheduledAnalysisOverride = passes.Run;
        return (worker, passes, log);
    }

    private static DarlingWorker.ServerLoopState Server(string name, DateTime stampUtc)
    {
        var server = SelfAlertFleetPassTests.MakeServer(name, connected: true);
        server.NextAnalysisDue = stampUtc;
        return server;
    }

    /// <summary>One ask at <paramref name="nowUtc"/>: the due half of the step, on a fleet of <paramref name="slots"/> with
    /// an analysis interval of <paramref name="intervalMinutes"/>.</summary>
    private static Task Ask(DarlingWorker worker, DarlingWorker.ServerLoopState server, DateTime nowUtc, int slots = 4, int intervalMinutes = 30) =>
        worker.RunDueScheduledAnalysisAsync(
            server, null!, null!, new DarlingConfig { MaxConcurrentSweeps = slots }, intervalMinutes, nowUtc, Ct);

    /// <summary>A body that has been waiting for a fleet slot since <paramref name="sinceUtc"/>, as the body states it
    /// when it parks on the gate. The answer ends the wait through <c>SlotWaiters.Leave</c>.</summary>
    private static LinkedListNode<DateTime> BodyWaitingSince(DarlingWorker worker, DateTime sinceUtc) =>
        worker.SlotWaiters.Enter(sinceUtc);

    /// <summary>The longest wait is in milliseconds, and -1 stands for no body waiting.</summary>
    [Theory]
    [InlineData(60_000, 0, true)]
    [InlineData(60_000, 299_999, true)]
    [InlineData(60_000, 300_000, false)]
    [InlineData(60_000, 300_001, false)]
    [InlineData(190_000, 60_000, true)]
    [InlineData(30_001, 60_000, true)]
    [InlineData(30_000, 60_000, true)]
    [InlineData(29_999, 60_000, false)]
    [InlineData(15_000, 60_000, false)]
    [InlineData(0, 60_000, false)]
    [InlineData(-1, 0, false)]
    [InlineData(-1, 299_999, false)]
    public void GivesWay_OnlyToABodyThatHasWaitedTheFloor_AndForLessThanFiveMinutesPastDue(int longestWaitMs, int millisecondsPastDue, bool givesWay)
    {
        Assert.Equal(TimeSpan.FromMinutes(5), ScheduledAnalysisAdmission.YieldLimit);
        TimeSpan? longestWait = longestWaitMs < 0 ? null : TimeSpan.FromMilliseconds(longestWaitMs);
        Assert.Equal(
            givesWay,
            ScheduledAnalysisAdmission.GivesWay(longestWait, T0, T0.AddMilliseconds(millisecondsPastDue)));
    }

    [Fact]
    public void TheWaitAPassGivesWayTo_IsTheWaitACollectionCycleRowNamesAsACause()
    {
        var floor = TimeSpan.FromMilliseconds(DarlingCycleSkip.CauseFloorMs);

        Assert.True(ScheduledAnalysisAdmission.GivesWay(floor, T0, T0.AddMinutes(1)));
        Assert.False(ScheduledAnalysisAdmission.GivesWay(floor - TimeSpan.FromMilliseconds(1), T0, T0.AddMinutes(1)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1_000)]
    [InlineData(14_999)]
    [InlineData(29_999)]
    public void ALaunchQueueYoungerThanTheFloor_DoesNotHoldADuePassBack(int queuedForMs)
    {
        var (worker, passes, _) = NewWorker();

        try
        {
            /* 42 servers on 6 slots: a tick launches 42 bodies, 6 take the slots and 36 park behind them. */
            for (var i = 0; i < 36; i++)
            {
                BodyWaitingSince(worker, T0);
            }

            var server = Server("due", T0.AddMinutes(-1));
            _ = Ask(worker, server, T0.AddMilliseconds(queuedForMs), slots: 6);

            Assert.Equal(new[] { "due" }, passes.Started);
            Assert.Equal(0, server.AnalysisGaveWay);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public void ASteadyFleetLargerThanItsGate_NeverGivesWay_ToTheLaunchQueueOfEachTick()
    {
        var log = new CapturingTestLogger();
        var worker = SelfAlertFleetPassTests.MakeWorker(log);
        var started = new List<string>();
        worker.ScheduledAnalysisOverride = (server, _) =>
        {
            started.Add(server.Config.Name);
            return Task.CompletedTask;
        };

        /* Twenty ticks, five minutes, of 42 servers on 6 slots. Every tick parks 36 bodies behind the first 6. A server
           near the front of the launch order ends its body 3 seconds in, with the tail still parked and its pass due at
           that tick. The queue drains before the next tick. */
        var gaveWay = 0;
        for (var tick = 0; tick < 20; tick++)
        {
            var tickUtc = T0 + (tick * Tick);
            var parked = Enumerable.Range(0, 36).Select(_ => BodyWaitingSince(worker, tickUtc)).ToArray();

            var server = Server("front-" + tick, tickUtc);
            Assert.True(Ask(worker, server, tickUtc.AddSeconds(3), slots: 6).IsCompletedSuccessfully);
            gaveWay += server.AnalysisGaveWay;

            foreach (var body in parked)
            {
                worker.SlotWaiters.Leave(body);
            }
        }

        Assert.Equal(Enumerable.Range(0, 20).Select(tick => "front-" + tick), started);
        Assert.Equal(0, gaveWay);
        Assert.DoesNotContain(log.Lines, l => l.Contains("giving way", StringComparison.Ordinal));
    }

    [Fact]
    public void ARestartHerdsQueue_HoldsADuePassBack_FromTheFloorOn_ForFiveMinutesPastItsDueTime()
    {
        var (worker, passes, log) = NewWorker();

        try
        {
            /* After a restart the bodies queue and stay queued: 36 of them wait from T0 on, and none gets a slot. */
            for (var i = 0; i < 36; i++)
            {
                BodyWaitingSince(worker, T0);
            }

            /* A first pass that comes due 40 seconds in. It asks every 15 seconds from T0 + 45 s. */
            var server = Server("herd", T0.AddSeconds(40));
            var asks = 0;
            for (var now = T0.AddSeconds(45); now <= T0.AddMinutes(7) && passes.Started.Count == 0; now += Tick)
            {
                asks++;
                _ = Ask(worker, server, now, slots: 6);
            }

            /* Twenty asks give way (T0 + 0:45 to T0 + 5:30), and the twenty-first, at T0 + 5:45, starts. */
            Assert.Equal(new[] { "herd" }, passes.Started);
            Assert.Equal(21, asks);
            var line = Assert.Single(log.Lines, l => l.Contains("Scheduled analysis starts", StringComparison.Ordinal));
            Assert.Contains("20 giving way to waiting collection", line, StringComparison.Ordinal);
        }
        finally
        {
            passes.EndAll();
        }
    }

    /// <summary>Every instant is in milliseconds after T0. The pass has been refused since T0 and the interval is 30
    /// minutes. A last report of -1 is one never made.</summary>
    [Theory]
    [InlineData(600_000, -1, false)]
    [InlineData(1_800_000, -1, false)]
    [InlineData(1_800_001, -1, true)]
    [InlineData(2_700_000, 1_800_001, false)]
    [InlineData(3_600_000, 1_800_001, false)]
    [InlineData(3_600_001, 1_800_001, true)]
    [InlineData(3_600_002, 1_800_001, true)]
    [InlineData(2_400_000, 3_000_000, true)]
    [InlineData(1_800_000, 3_000_000, false)]
    [InlineData(-60_000, -1, false)]
    public void WarnsLate_FromMoreThanOneIntervalRefused_OncePerInterval_AndALastReportAheadOfTheClockCountsAsOld(
        int nowMs, int lastWarnedMs, bool warns)
    {
        var lastWarnedUtc = lastWarnedMs < 0 ? DateTime.MinValue : T0.AddMilliseconds(lastWarnedMs);

        Assert.Equal(
            warns,
            ScheduledAnalysisAdmission.WarnsLate(T0, T0.AddMilliseconds(nowMs), TimeSpan.FromMinutes(30), lastWarnedUtc));
    }

    [Fact]
    public void TheLimitIsFiveMinutes_ToTheMillisecond_ThroughTheStep()
    {
        var (worker, passes, _) = NewWorker();
        BodyWaitingSince(worker, T0.AddMinutes(-1));
        var server = Server("due", T0);

        try
        {
            var before = Ask(worker, server, T0.AddMilliseconds(299_999));
            Assert.Empty(passes.Started);
            Assert.True(before.IsCompletedSuccessfully);

            _ = Ask(worker, server, T0.AddMinutes(5));
            Assert.Equal(new[] { "due" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public void AStampAheadOfTheClock_CountsItsFiveMinutesFromTheBodyThatFirstFindsItDue()
    {
        var (worker, passes, _) = NewWorker();
        BodyWaitingSince(worker, T0.AddMinutes(-1));

        /* Two intervals ahead: a clock that stepped back. It is due at once, and it has no due time of its own. */
        var server = Server("ahead", T0.AddMinutes(60));

        try
        {
            var first = Ask(worker, server, T0);
            Assert.Empty(passes.Started);
            Assert.True(first.IsCompletedSuccessfully);
            Assert.Equal(T0, server.NextAnalysisDue);

            var late = Ask(worker, server, T0.AddMilliseconds(299_999));
            Assert.Empty(passes.Started);
            Assert.True(late.IsCompletedSuccessfully);
            Assert.Equal(T0, server.NextAnalysisDue);

            _ = Ask(worker, server, T0.AddMinutes(5));
            Assert.Equal(new[] { "ahead" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public void AnIntervalShortenedMidRun_StartsThePassFiveMinutesLater_NotWhenTheOldStampComesRound()
    {
        var (worker, passes, _) = NewWorker();
        BodyWaitingSince(worker, T0.AddMinutes(-1));

        /* The pass ran ten minutes ago under a 360-minute interval, so its stamp is 350 minutes ahead. The interval is now
           30 minutes, and bodies are queueing at every ask. */
        var server = Server("shortened", T0.AddMinutes(350));

        try
        {
            var asks = 0;
            for (var now = T0; now <= T0.AddMinutes(6) && passes.Started.Count == 0; now += Tick)
            {
                asks++;
                _ = Ask(worker, server, now, intervalMinutes: 30);
            }

            Assert.Equal(new[] { "shortened" }, passes.Started);

            /* Twenty asks give way (T0 to T0 + 4:45), and the twenty-first, at T0 + 5:00, starts. */
            Assert.Equal(21, asks);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public async Task TheTwoRefusals_AreCountedApart_AndTheLineThatStartsThePassWordsEach()
    {
        var (worker, passes, log) = NewWorker();

        try
        {
            /* Two fleet slots, so one place for analysis, and another server's pass holds it. */
            var holder = Ask(worker, Server("holder", T0), T0, slots: 2);
            Assert.Equal(new[] { "holder" }, passes.Started);

            var server = Server("refused", T0);
            foreach (var seconds in new[] { 15, 30, 45 })
            {
                Assert.True(Ask(worker, server, T0.AddSeconds(seconds), slots: 2).IsCompletedSuccessfully);
            }

            /* Then a body queues for a slot, at T0 + 20 s. By T0 + 60 s it has waited past the floor, and the pass gives
               way to it twice. */
            var waiting = BodyWaitingSince(worker, T0.AddSeconds(20));
            foreach (var seconds in new[] { 60, 75 })
            {
                Assert.True(Ask(worker, server, T0.AddSeconds(seconds), slots: 2).IsCompletedSuccessfully);
            }

            Assert.Equal(new[] { "holder" }, passes.Started);

            /* The holder ends and the body gets its slot: the next ask starts the pass. */
            passes.EndAll();
            await holder.WaitAsync(Patience, Ct);
            worker.SlotWaiters.Leave(waiting);
            _ = Ask(worker, server, T0.AddSeconds(90), slots: 2);

            Assert.Equal(new[] { "holder", "refused" }, passes.Started);
            var line = Assert.Single(log.Lines, l => l.Contains("[refused]", StringComparison.Ordinal) && l.Contains("Scheduled analysis starts", StringComparison.Ordinal));
            Assert.Contains("2 giving way to waiting collection", line, StringComparison.Ordinal);
            Assert.Contains("3 because the analysis share is full", line, StringComparison.Ordinal);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public void ADuePassMoreThanOneIntervalPastDue_WarnsOncePerInterval_WithBothCountsAndTheShare()
    {
        var (worker, passes, log) = NewWorker();

        try
        {
            _ = Ask(worker, Server("holder", T0), T0, slots: 2);
            Assert.Equal(new[] { "holder" }, passes.Started);

            /* Refused by the share from its due time on, every 15 seconds. Nothing is said for the first interval. */
            var server = Server("starved", T0);
            for (var now = T0; now <= T0.AddMinutes(30); now += Tick)
            {
                Assert.True(Ask(worker, server, now, slots: 2).IsCompletedSuccessfully);
            }

            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));

            /* One tick past the interval the pass is more than an interval late: one warning, with what refused it. */
            _ = Ask(worker, server, T0.AddMinutes(30) + Tick, slots: 2);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            var warning = log.Lines.Single(l => l.Contains("has not started", StringComparison.Ordinal));
            Assert.Contains("[starved]", warning, StringComparison.Ordinal);
            Assert.Contains("30 minutes past due", warning, StringComparison.Ordinal);
            Assert.Contains("0 giving way to waiting collection", warning, StringComparison.Ordinal);
            Assert.Contains("122 because the analysis share is full (1 of 2 fleet slots)", warning, StringComparison.Ordinal);

            /* It keeps being refused. The next warning is one interval after the first, not on every ask. */
            for (var now = T0.AddMinutes(30) + Tick + Tick; now < T0.AddMinutes(60) + Tick; now += Tick)
            {
                _ = Ask(worker, server, now, slots: 2);
            }

            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            _ = Ask(worker, server, T0.AddMinutes(60) + Tick, slots: 2);
            Assert.Equal(2, log.CountAtLevel(LogLevel.Warning));
            Assert.Equal(new[] { "holder" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public void AStampNeverSet_ThatTheShareRefuses_GetsADueTime_SoItsWaitIsCountedToo()
    {
        var (worker, passes, log) = NewWorker();

        try
        {
            _ = Ask(worker, Server("holder", T0), T0, slots: 2);
            var server = SelfAlertFleetPassTests.MakeServer("never-stamped", connected: true);
            Assert.Equal(DateTime.MinValue, server.NextAnalysisDue);

            Assert.True(Ask(worker, server, T0, slots: 2).IsCompletedSuccessfully);

            /* Past the give-way limit already, as a stamp never set is, and counted from this body. */
            Assert.Equal(T0 - ScheduledAnalysisAdmission.YieldLimit, server.NextAnalysisDue);
            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));

            /* The warning counts from that first refusal, at T0, not from the due time it was given. */
            _ = Ask(worker, server, T0.AddMinutes(25) + Tick, slots: 2);
            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));

            _ = Ask(worker, server, T0.AddMinutes(30) + Tick, slots: 2);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            Assert.Contains("35 minutes past due", log.Lines.Single(l => l.Contains("has not started", StringComparison.Ordinal)), StringComparison.Ordinal);
            Assert.Equal(new[] { "holder" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public void AStampTwoIntervalsOld_ThatTheShareRefuses_WarnsOnceItHasBeenRefusedForAnInterval_NotOnTheFirstRefusal()
    {
        var (worker, passes, log) = NewWorker();

        try
        {
            _ = Ask(worker, Server("holder", T0), T0, slots: 2);
            Assert.Equal(new[] { "holder" }, passes.Started);

            /* Nothing asked for an hour (collection paused, or analysis turned off and back on), so the stamp is two
               intervals old at the first ask. The share refuses it, and one refusal is not a pass that is being starved. */
            var server = Server("resumed", T0.AddMinutes(-60));
            Assert.True(Ask(worker, server, T0, slots: 2).IsCompletedSuccessfully);
            Assert.Equal(1, server.AnalysisShareFull);
            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));

            for (var now = T0 + Tick; now <= T0.AddMinutes(30); now += Tick)
            {
                _ = Ask(worker, server, now, slots: 2);
            }

            Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));

            /* Still refused one interval after the first refusal: one warning, with the minutes since the due time. */
            _ = Ask(worker, server, T0.AddMinutes(30) + Tick, slots: 2);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            var warning = log.Lines.Single(l => l.Contains("has not started", StringComparison.Ordinal));
            Assert.Contains("[resumed]", warning, StringComparison.Ordinal);
            Assert.Contains("90 minutes past due", warning, StringComparison.Ordinal);
            Assert.Contains("122 because the analysis share is full (1 of 2 fleet slots)", warning, StringComparison.Ordinal);
            Assert.Equal(new[] { "holder" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public void APassThatStarts_IsDueAgainOneIntervalAfterTheAskThatStartedIt()
    {
        var (worker, passes, _) = NewWorker();

        try
        {
            var server = Server("due", T0.AddMinutes(-2));
            _ = Ask(worker, server, T0, intervalMinutes: 30);

            Assert.Equal(new[] { "due" }, passes.Started);
            Assert.Equal(T0.AddMinutes(30), server.NextAnalysisDue);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public async Task ACallReusesAPassesCandidates_WhileTheNextPassGivesWayAndWaitsForAPlace_AndReadsLiveAfterThat()
    {
        var (worker, passes, _) = NewWorker();
        var config = new DarlingConfig { MaxConcurrentSweeps = 4 };
        var interval = TimeSpan.FromMinutes(config.Analysis.IntervalMinutes);

        try
        {
            /* The step sets the shared reuse window from the cadence on every body, whether or not a pass is due. This
               server's pass is not: its stamp is ten minutes ahead of the wall clock the step reads. */
            await worker.RunScheduledAnalysisIfDueAsync(Server("not-due", DateTime.UtcNow.AddMinutes(10)), null!, null!, config, Ct);
            Assert.Empty(passes.Started);

            /* A cache of this test's own with that window, filled by a pass at T0. The next pass is due at T0 plus the
               interval. */
            var cache = new PgFactCollector.PlanRegressionCandidateCache { RefreshAfter = PgFactCollector.PlanRegressionCandidateCache.Shared.RefreshAfter };
            cache.Store("store", 1, "s", T0, Array.Empty<PgFactCollector.PlanRegressionCandidate>());
            var nextDue = T0 + interval;
            bool Reused(DateTime atUtc) => cache.TryGet("store", 1, "s", atUtc, atUtc) is not null;

            Assert.True(Reused(nextDue - TimeSpan.FromMilliseconds(1)), "inside one cadence");

            /* The next pass gives way to collection for up to the limit, and the entry is still served. */
            var gaveWayUntil = nextDue + ScheduledAnalysisAdmission.YieldLimit;
            Assert.True(Reused(gaveWayUntil - TimeSpan.FromMilliseconds(1)), "while the pass that refills it gives way");

            /* It then waits for a place in a full share, which a running pass frees within its budget. */
            var placeFreedBy = gaveWayUntil + DarlingWorker.AnalysisTimeout;
            Assert.True(Reused(placeFreedBy - TimeSpan.FromMilliseconds(1)), "while the pass that refills it waits for a place");

            /* The window is still a limit: past it the call reads live. */
            Assert.False(Reused(placeFreedBy), "past the longest deferral the window covers");
        }
        finally
        {
            passes.EndAll();
        }
    }
}

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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8539: the admission of a due scheduled analysis pass, asked at fixed times. The due half of the step takes its clock as a
/// parameter, so every ask here names its own instant: a stamp ahead of the clock, the give-way limit to the millisecond,
/// an interval shortened while a stamp written under the longer one is still standing, and a pass that stays refused for
/// longer than an interval. The passes are stand-ins the test holds open and ends, and the waiting body is a real body
/// parked on a full fleet gate.
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

    /// <summary>A real body waiting for a fleet slot: the gate has no free permit, so the body parks on it before this
    /// returns. Its server is one a removal retired while it queued.</summary>
    private static Task StartBodyWaitingForASlot(DarlingWorker worker, SemaphoreSlim fullGate, CancellationToken token)
    {
        var waiting = SelfAlertFleetPassTests.MakeServer("waiting");
        waiting.Retired = true;
        var process = typeof(DarlingWorker).GetMethod("ProcessServerSweepAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var body = (Task)process.Invoke(worker, new object?[] { waiting, null, null, null, null, new DarlingConfig(), fullGate, token })!;
        Assert.False(body.IsCompleted, "the body must be parked on the full gate");
        return body;
    }

    [Theory]
    [InlineData(1, 0, true)]
    [InlineData(1, 299_999, true)]
    [InlineData(1, 300_000, false)]
    [InlineData(1, 300_001, false)]
    [InlineData(3, 60_000, true)]
    [InlineData(0, 0, false)]
    [InlineData(0, 299_999, false)]
    public void GivesWay_OnlyWhileABodyWaits_AndForLessThanFiveMinutesPastDue(int bodiesWaiting, int millisecondsPastDue, bool givesWay)
    {
        Assert.Equal(TimeSpan.FromMinutes(5), ScheduledAnalysisAdmission.YieldLimit);
        Assert.Equal(
            givesWay,
            ScheduledAnalysisAdmission.GivesWay(bodiesWaiting, T0, T0.AddMilliseconds(millisecondsPastDue)));
    }

    [Fact]
    public async Task TheLimitIsFiveMinutes_ToTheMillisecond_ThroughTheStep()
    {
        var (worker, passes, _) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        _ = StartBodyWaitingForASlot(worker, gate, stop.Token);
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
            await stop.CancelAsync();
        }
    }

    [Fact]
    public async Task AStampAheadOfTheClock_CountsItsFiveMinutesFromTheBodyThatFirstFindsItDue()
    {
        var (worker, passes, _) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        _ = StartBodyWaitingForASlot(worker, gate, stop.Token);

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
            await stop.CancelAsync();
        }
    }

    [Fact]
    public async Task AnIntervalShortenedMidRun_StartsThePassFiveMinutesLater_NotWhenTheOldStampComesRound()
    {
        var (worker, passes, _) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        _ = StartBodyWaitingForASlot(worker, gate, stop.Token);

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
            await stop.CancelAsync();
        }
    }

    [Fact]
    public async Task TheTwoRefusals_AreCountedApart_AndTheLineThatStartsThePassWordsEach()
    {
        var (worker, passes, log) = NewWorker();
        using var gate = new SemaphoreSlim(0, 2);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);

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

            /* Then a body queues for a slot, and the pass gives way to it twice. */
            var waiting = StartBodyWaitingForASlot(worker, gate, stop.Token);
            foreach (var seconds in new[] { 60, 75 })
            {
                Assert.True(Ask(worker, server, T0.AddSeconds(seconds), slots: 2).IsCompletedSuccessfully);
            }

            Assert.Equal(new[] { "holder" }, passes.Started);

            /* The holder ends and the body gets its slot: the next ask starts the pass. */
            passes.EndAll();
            await holder.WaitAsync(Patience, Ct);
            gate.Release();
            await waiting.WaitAsync(Patience, Ct);
            _ = Ask(worker, server, T0.AddSeconds(90), slots: 2);

            Assert.Equal(new[] { "holder", "refused" }, passes.Started);
            var line = Assert.Single(log.Lines, l => l.Contains("[refused]", StringComparison.Ordinal) && l.Contains("Scheduled analysis starts", StringComparison.Ordinal));
            Assert.Contains("2 giving way to waiting collection", line, StringComparison.Ordinal);
            Assert.Contains("3 because the analysis share is full", line, StringComparison.Ordinal);
        }
        finally
        {
            passes.EndAll();
            await stop.CancelAsync();
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

            _ = Ask(worker, server, T0.AddMinutes(25) + Tick, slots: 2);
            Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
            Assert.Equal(new[] { "holder" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }
}

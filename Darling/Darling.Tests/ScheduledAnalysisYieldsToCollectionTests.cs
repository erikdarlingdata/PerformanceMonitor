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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8539: a scheduled analysis pass runs inside its server's fleet collection slot and holds it for its whole length, and
/// after a service restart every server's first pass comes due within a few minutes. These tests drive the body's analysis
/// step with a stand-in pass that stays open until the test ends it, faults it or cancels it, and a real body parked on a
/// full fleet gate. Each ask names its own instant, a minute after the body parked or just before it, and the due stamps
/// sit minutes either side of the limit, so nothing here depends on timing.
/// </summary>
public sealed class ScheduledAnalysisYieldsToCollectionTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>The stand-in for the pass: records each start, and keeps the pass open until the test ends it.</summary>
    private sealed class HeldPasses
    {
        private readonly object _lock = new();
        private readonly List<string> _started = new();
        private readonly Dictionary<string, TaskCompletionSource> _open = new(StringComparer.Ordinal);

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
                _open[server.Config.Name] = open;
            }

            /* A pass whose token is cancelled ends as cancelled, as the real pass does on a shutdown. */
            token.Register(() => open.TrySetCanceled(token));
            return open.Task;
        }

        internal void Fault(string name, Exception error)
        {
            TaskCompletionSource open;
            lock (_lock)
            {
                open = _open[name];
                _open.Remove(name);
            }

            open.SetException(error);
        }

        internal void End(string name)
        {
            TaskCompletionSource open;
            lock (_lock)
            {
                open = _open[name];
                _open.Remove(name);
            }

            open.TrySetResult();
        }

        internal void EndAll()
        {
            TaskCompletionSource[] open;
            lock (_lock)
            {
                open = _open.Values.ToArray();
                _open.Clear();
            }

            foreach (var pass in open)
            {
                pass.TrySetResult();
            }
        }
    }

    private static (DarlingWorker Worker, HeldPasses Passes) NewWorker()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var passes = new HeldPasses();
        worker.ScheduledAnalysisOverride = passes.Run;
        return (worker, passes);
    }

    /// <summary>A connected server whose scheduled analysis pass came due <paramref name="minutesAgo"/> minutes ago.</summary>
    private static DarlingWorker.ServerLoopState DueServer(string name, double minutesAgo = 1) =>
        DueServer(name, DateTime.UtcNow.AddMinutes(-minutesAgo));

    /// <summary>A connected server whose scheduled analysis pass came due at <paramref name="dueUtc"/>.</summary>
    private static DarlingWorker.ServerLoopState DueServer(string name, DateTime dueUtc)
    {
        var server = SelfAlertFleetPassTests.MakeServer(name, connected: true);
        server.NextAnalysisDue = dueUtc;
        return server;
    }

    private static DarlingConfig Fleet(int slots) => new() { MaxConcurrentSweeps = slots };

    /// <summary>The body's analysis step for <paramref name="server"/>. It returns once the pass has started (the stand-in
    /// keeps it open) or has not started at all, so the caller reads which it was without waiting.</summary>
    private static Task Step(DarlingWorker worker, DarlingWorker.ServerLoopState server, int slots) =>
        worker.RunScheduledAnalysisIfDueAsync(server, null!, null!, Fleet(slots), Ct);

    /// <summary>The due half of the step, asked at <paramref name="nowUtc"/> with a 30-minute analysis interval.</summary>
    private static Task Ask(DarlingWorker worker, DarlingWorker.ServerLoopState server, DateTime nowUtc, int slots = 4, CancellationToken? token = null) =>
        worker.RunDueScheduledAnalysisAsync(server, null!, null!, Fleet(slots), 30, nowUtc, token ?? Ct);

    /// <summary>An instant a minute after now. A body parked before this is read has waited more than the floor at it.</summary>
    private static DateTime AMinuteFromNow() => DateTime.UtcNow.AddMinutes(1);

    /// <summary>
    /// A real body that is waiting for a fleet slot: the gate has no free permit, so the body parks on it before this
    /// returns. The server is one a removal retired while it queued, so once it gets a slot it does nothing and gives the
    /// slot back.
    /// </summary>
    private static Task StartBodyWaitingForASlot(DarlingWorker worker, SemaphoreSlim fullGate, CancellationToken token)
    {
        Assert.Equal(0, fullGate.CurrentCount);
        var waiting = SelfAlertFleetPassTests.MakeServer("waiting");
        waiting.Retired = true;
        var process = typeof(DarlingWorker).GetMethod("ProcessServerSweepAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var body = (Task)process.Invoke(worker, new object?[] { waiting, null, null, null, null, new DarlingConfig(), fullGate, token })!;
        Assert.False(body.IsCompleted, "the body must be parked on the full gate");
        return body;
    }

    [Fact]
    public async Task ADuePass_DoesNotStart_WhileACollectionBodyHasWaitedPastTheFloorForAFleetSlot_AndStartsOnceItHasOne()
    {
        var (worker, passes) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var waitingBody = StartBodyWaitingForASlot(worker, gate, stop.Token);
        var ask = AMinuteFromNow();
        var server = DueServer("due", ask.AddMinutes(-1));
        var dueStamp = server.NextAnalysisDue;

        try
        {
            var yielded = Ask(worker, server, ask);

            Assert.Empty(passes.Started);
            Assert.True(yielded.IsCompletedSuccessfully, "a pass that gives way ends the step at once");
            Assert.Equal(dueStamp, server.NextAnalysisDue);

            /* The body gets its slot, finds its server retired and gives the slot back: nothing is waiting any more. */
            gate.Release();
            await waitingBody.WaitAsync(Patience, Ct);

            var started = Ask(worker, server, ask.AddSeconds(15));

            Assert.Equal(new[] { "due" }, passes.Started);
            Assert.False(started.IsCompleted, "the pass is open, so the step is still inside it");

            /* A pass that starts is due again one interval after the ask that started it. */
            Assert.Equal(ask.AddSeconds(15).AddMinutes(30), server.NextAnalysisDue);

            passes.End("due");
            await started.WaitAsync(Patience, Ct);
        }
        finally
        {
            passes.EndAll();
            await stop.CancelAsync();
        }
    }

    [Fact]
    public async Task ADuePass_Starts_WhileABodyThatHasWaitedNoTimeIsParkedOnTheGate_AsOnEveryTickOfAFleetLargerThanItsGate()
    {
        var (worker, passes) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        /* The ask's instant is read before the body parks, so at that instant the body has waited no time: the launch
           queue of one sweep tick, which drains inside the tick and costs no collection slot. */
        var ask = DateTime.UtcNow;
        _ = StartBodyWaitingForASlot(worker, gate, stop.Token);
        Assert.Equal(1, worker.SlotWaiters.Count);
        var server = DueServer("due", ask.AddMinutes(-1));

        try
        {
            var started = Ask(worker, server, ask);

            Assert.Equal(new[] { "due" }, passes.Started);
            Assert.False(started.IsCompleted, "the pass is open, so the step is still inside it");
            Assert.Equal(0, server.AnalysisGaveWay);
        }
        finally
        {
            passes.EndAll();
            await stop.CancelAsync();
        }
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(3, 1)]
    [InlineData(4, 2)]
    [InlineData(6, 3)]
    [InlineData(8, 4)]
    [InlineData(16, 8)]
    public async Task ScheduledPasses_HoldHalfTheFleetSlots_OrTheOnlySlotOfAGateOneWide_AndTheNextOneStartsWhenOneEnds(int slots, int share)
    {
        var (worker, passes) = NewWorker();
        var steps = new List<Task>();

        try
        {
            /* Every server in the fleet is due at once, as after a restart, and no body is waiting for a slot. */
            var servers = Enumerable.Range(0, slots + 1).Select(i => DueServer("server-" + i)).ToArray();
            foreach (var server in servers)
            {
                steps.Add(Step(worker, server, slots));
            }

            Assert.Equal(servers.Take(share).Select(s => s.Config.Name), passes.Started);

            /* The ones that did not start are still due: each asks again on its next body. */
            var heldBack = servers[share];
            Assert.True(steps[share].IsCompletedSuccessfully);
            Assert.True(heldBack.NextAnalysisDue < DateTime.UtcNow, "a pass that did not start keeps its due stamp");

            /* While the share is full the next body's ask is refused as well. */
            var refused = Step(worker, heldBack, slots);
            Assert.Equal(share, passes.Started.Count);
            Assert.True(refused.IsCompletedSuccessfully);

            /* One pass ends, and its place goes to the next server that asks. */
            passes.End(servers[0].Config.Name);
            await steps[0].WaitAsync(Patience, Ct);
            steps.Add(Step(worker, heldBack, slots));

            Assert.Equal(share + 1, passes.Started.Count);
            Assert.Equal(heldBack.Config.Name, passes.Started[share]);
        }
        finally
        {
            passes.EndAll();
        }

        await Task.WhenAll(steps).WaitAsync(Patience, Ct);
    }

    [Theory]
    [InlineData(0.5, false)]
    [InlineData(4, false)]
    [InlineData(6, true)]
    [InlineData(90, true)]
    public async Task ADuePass_GivesWayToAWaitingBody_ForFiveMinutesPastItsDueTime_ThenStarts(double minutesPastDue, bool starts)
    {
        var (worker, passes) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        _ = StartBodyWaitingForASlot(worker, gate, stop.Token);
        var ask = AMinuteFromNow();
        var server = DueServer("due", ask.AddMinutes(-minutesPastDue));

        try
        {
            var step = Ask(worker, server, ask);

            Assert.Equal(starts ? new[] { "due" } : Array.Empty<string>(), passes.Started);
            Assert.Equal(!starts, step.IsCompleted);
        }
        finally
        {
            passes.EndAll();
            await stop.CancelAsync();
        }
    }

    [Fact]
    public async Task APassPastTheFiveMinutes_StillStaysWithinTheShare_WhileABodyWaits()
    {
        var (worker, passes) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        _ = StartBodyWaitingForASlot(worker, gate, stop.Token);

        try
        {
            /* Four slots, so two places. Three servers are all long overdue while collection is still queueing. */
            var ask = AMinuteFromNow();
            var servers = Enumerable.Range(0, 3).Select(i => DueServer("overdue-" + i, ask.AddMinutes(-20))).ToArray();
            var steps = servers.Select(server => Ask(worker, server, ask)).ToArray();

            Assert.Equal(new[] { "overdue-0", "overdue-1" }, passes.Started);
            Assert.True(steps[2].IsCompletedSuccessfully);
        }
        finally
        {
            passes.EndAll();
            await stop.CancelAsync();
        }
    }

    [Fact]
    public async Task ABodyThatStopsWaiting_NoLongerHoldsPassesBack()
    {
        var (worker, passes) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var waitingBody = StartBodyWaitingForASlot(worker, gate, stop.Token);
        var ask = AMinuteFromNow();
        var server = DueServer("due", ask.AddMinutes(-1));

        try
        {
            var yielded = Ask(worker, server, ask);
            Assert.Empty(passes.Started);
            Assert.True(yielded.IsCompletedSuccessfully);

            /* The service stops the body while it is still queued: it leaves the queue without ever taking a slot. */
            await stop.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitingBody.WaitAsync(Patience, Ct));
            Assert.Equal(0, worker.SlotWaiters.Count);

            var started = Ask(worker, server, ask.AddSeconds(15));

            Assert.Equal(new[] { "due" }, passes.Started);
            passes.End("due");
            await started.WaitAsync(Patience, Ct);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public async Task AServerWhoseStampWasNeverSet_StartsAtOnce_AsItDoesWhenAnalysisIsTurnedBackOn()
    {
        var (worker, passes) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        _ = StartBodyWaitingForASlot(worker, gate, stop.Token);
        var server = SelfAlertFleetPassTests.MakeServer("never-stamped", connected: true);
        Assert.Equal(DateTime.MinValue, server.NextAnalysisDue);

        try
        {
            var step = Ask(worker, server, AMinuteFromNow());

            Assert.Equal(new[] { "never-stamped" }, passes.Started);
            passes.End("never-stamped");
            await step.WaitAsync(Patience, Ct);
        }
        finally
        {
            passes.EndAll();
            await stop.CancelAsync();
        }
    }

    [Fact]
    public async Task APassThatFaults_GivesItsPlaceBack_SoTheNextServerStarts()
    {
        var (worker, passes) = NewWorker();

        try
        {
            /* Two slots, so one place, and the holder has it. */
            var holderStep = Step(worker, DueServer("holder"), slots: 2);
            var next = DueServer("next");
            await Step(worker, next, slots: 2);
            Assert.Equal(new[] { "holder" }, passes.Started);

            passes.Fault("holder", new InvalidOperationException("the pass faulted"));
            await Assert.ThrowsAsync<InvalidOperationException>(() => holderStep.WaitAsync(Patience, Ct));

            _ = Step(worker, next, slots: 2);
            Assert.Equal(new[] { "holder", "next" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }

    [Fact]
    public async Task APassThatIsCancelled_GivesItsPlaceBack_SoTheNextServerStarts()
    {
        var (worker, passes) = NewWorker();
        using var holderStop = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        try
        {
            /* Two slots, so one place, and the holder has it. Its step runs under a token of its own. */
            var ask = DateTime.UtcNow;
            var holderStep = Ask(worker, DueServer("holder", ask.AddMinutes(-1)), ask, slots: 2, token: holderStop.Token);
            var next = DueServer("next", ask.AddMinutes(-1));
            await Ask(worker, next, ask, slots: 2);
            Assert.Equal(new[] { "holder" }, passes.Started);

            await holderStop.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => holderStep.WaitAsync(Patience, Ct));

            _ = Ask(worker, next, ask.AddSeconds(15), slots: 2);
            Assert.Equal(new[] { "holder", "next" }, passes.Started);
        }
        finally
        {
            passes.EndAll();
        }
    }
}

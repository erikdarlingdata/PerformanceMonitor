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
/// #8539: a scheduled analysis pass runs inside its server's fleet collection slot and holds it for its whole length. After
/// a service restart every server's first pass comes due within a few minutes, and on a store whose pass is long those
/// passes took most of the slots while collection bodies waited minutes for one and stepped over their one-minute slots.
/// These tests drive the body's analysis step with a stand-in pass that stays open until the test ends it, a real body
/// parked on a full fleet gate, and due stamps minutes either side of the limit, so nothing here depends on timing.
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

            return open.Task;
        }

        internal void End(string name)
        {
            TaskCompletionSource open;
            lock (_lock)
            {
                open = _open[name];
                _open.Remove(name);
            }

            open.SetResult();
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
                pass.SetResult();
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
    private static DarlingWorker.ServerLoopState DueServer(string name, double minutesAgo = 1)
    {
        var server = SelfAlertFleetPassTests.MakeServer(name, connected: true);
        server.NextAnalysisDue = DateTime.UtcNow.AddMinutes(-minutesAgo);
        return server;
    }

    private static DarlingConfig Fleet(int slots) => new() { MaxConcurrentSweeps = slots };

    /// <summary>The body's analysis step for <paramref name="server"/>. It returns once the pass has started (the stand-in
    /// keeps it open) or has not started at all, so the caller reads which it was without waiting.</summary>
    private static Task Step(DarlingWorker worker, DarlingWorker.ServerLoopState server, int slots) =>
        worker.RunScheduledAnalysisIfDueAsync(server, null!, null!, Fleet(slots), Ct);

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
    public async Task ADuePass_DoesNotStart_WhileACollectionBodyWaitsForAFleetSlot_AndStartsOnceItHasOne()
    {
        var (worker, passes) = NewWorker();
        using var gate = new SemaphoreSlim(0, 4);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var waitingBody = StartBodyWaitingForASlot(worker, gate, stop.Token);
        var server = DueServer("due");
        var dueStamp = server.NextAnalysisDue;

        try
        {
            var yielded = Step(worker, server, slots: 4);

            Assert.Empty(passes.Started);
            Assert.True(yielded.IsCompletedSuccessfully, "a pass that gives way ends the step at once");
            Assert.Equal(dueStamp, server.NextAnalysisDue);

            /* The body gets its slot, finds its server retired and gives the slot back: nothing is waiting any more. */
            gate.Release();
            await waitingBody.WaitAsync(Patience, Ct);

            var started = Step(worker, server, slots: 4);

            Assert.Equal(new[] { "due" }, passes.Started);
            Assert.False(started.IsCompleted, "the pass is open, so the step is still inside it");
            Assert.True(server.NextAnalysisDue > DateTime.UtcNow, "a pass that starts moves its stamp one interval on");

            passes.End("due");
            await started.WaitAsync(Patience, Ct);
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
    public async Task ScheduledPasses_HoldAtMostHalfTheFleetSlots_AndTheNextOneStartsWhenOneEnds(int slots, int share)
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
        var server = DueServer("due", minutesPastDue);

        try
        {
            var step = Step(worker, server, slots: 4);

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
            var servers = Enumerable.Range(0, 3).Select(i => DueServer("overdue-" + i, minutesAgo: 20)).ToArray();
            var steps = servers.Select(server => Step(worker, server, slots: 4)).ToArray();

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
        var server = DueServer("due");

        try
        {
            var yielded = Step(worker, server, slots: 4);
            Assert.Empty(passes.Started);
            Assert.True(yielded.IsCompletedSuccessfully);

            /* The service stops the body while it is still queued: it leaves the queue without ever taking a slot. */
            await stop.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waitingBody.WaitAsync(Patience, Ct));

            var started = Step(worker, server, slots: 4);

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
            var step = Step(worker, server, slots: 4);

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
}

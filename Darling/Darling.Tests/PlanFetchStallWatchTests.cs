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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9535: the stall probe of the plan fetch phase. The trigger wait and the commands are tasks the test completes
/// itself, so "still running at the trigger" is set, never timed.
/// </summary>
public sealed class PlanFetchStallWatchTests
{
    private static readonly StallProbeObservation s_reading = new(640, 20, -1, -1);

    private sealed class Probe
    {
        public List<TaskCompletionSource> Delays { get; } = [];

        public List<TimeSpan> DelaySpans { get; } = [];

        public List<StallProbeObservation> Fired { get; } = [];

        public TaskCompletionSource FiredOnce { get; } = new();

        public Exception? FireFailure { get; init; }

        public PlanFetchStallWatch Watch() => new(
            observation =>
            {
                Fired.Add(observation);
                FiredOnce.TrySetResult();
                if (FireFailure is not null)
                {
                    throw FireFailure;
                }

                return Task.CompletedTask;
            },
            (span, _) =>
            {
                DelaySpans.Add(span);
                var delay = new TaskCompletionSource();
                Delays.Add(delay);
                return delay.Task;
            });
    }

    [Fact]
    public async Task ACommandStillRunningAtTheTrigger_FiresOneProbe_WithThePhasesReading()
    {
        var probe = new Probe();
        var watch = probe.Watch();
        var command = new TaskCompletionSource<int>();

        var watched = watch.WatchAsync(() => command.Task, () => s_reading, CancellationToken.None);

        /* Half the fetch's lock timeout, so a command that waits on a lock is sampled inside the wait. */
        Assert.Equal(TimeSpan.FromMilliseconds(500), PlanFetchStallWatch.Trigger);
        Assert.Equal(PlanFetchStallWatch.Trigger, Assert.Single(probe.DelaySpans));
        Assert.Empty(probe.Fired);

        probe.Delays[0].SetResult();
        await probe.FiredOnce.Task;

        Assert.Equal(s_reading, Assert.Single(probe.Fired));
        Assert.Equal(1, watch.Fires);
        Assert.False(watched.IsCompleted);

        command.SetResult(7);
        Assert.Equal(7, await watched);
    }

    [Fact]
    public async Task ACommandThatEndsBeforeTheTrigger_FiresNothing()
    {
        var probe = new Probe();
        var watch = probe.Watch();

        Assert.Equal(7, await watch.WatchAsync(() => Task.FromResult(7), () => s_reading, CancellationToken.None));

        /* The trigger wait ending after the command is over reads a finished command. */
        probe.Delays[0].SetResult();

        Assert.Empty(probe.Fired);
        Assert.Equal(0, watch.Fires);
    }

    [Fact]
    public async Task OnlyTheFirstStalledCommandOfARun_IsSampled()
    {
        var probe = new Probe();
        var watch = probe.Watch();
        var first = new TaskCompletionSource<int>();
        var second = new TaskCompletionSource<int>();

        var firstWatched = watch.WatchAsync(() => first.Task, () => s_reading, CancellationToken.None);
        probe.Delays[0].SetResult();
        await probe.FiredOnce.Task;
        first.SetResult(1);
        await firstWatched;

        var secondWatched = watch.WatchAsync(() => second.Task, () => s_reading, CancellationToken.None);
        second.SetResult(2);

        Assert.Equal(2, await secondWatched);
        Assert.Single(probe.Fired);
        Assert.Single(probe.Delays);
        Assert.Equal(1, watch.Fires);
    }

    [Fact]
    public async Task ACommandsFailure_ReachesTheCaller_AndAProbeThatFailsToStartDoesNot()
    {
        var probe = new Probe { FireFailure = new InvalidOperationException("no probe") };
        var watch = probe.Watch();
        var command = new TaskCompletionSource<int>();

        var watched = watch.WatchAsync(() => command.Task, () => s_reading, CancellationToken.None);
        probe.Delays[0].SetResult();
        await probe.FiredOnce.Task;

        command.SetException(new TimeoutException("lock"));

        await Assert.ThrowsAsync<TimeoutException>(() => watched);
        Assert.Single(probe.Fired);
    }

    private sealed class LockTimeout : Exception
    {
    }

    [Fact]
    public async Task TheBoundedFetch_RunsItsCommandsUnderTheWatch_AndTheProbeCarriesThePlansAnsweredByThen()
    {
        var probe = new Probe();
        var watch = probe.Watch();
        var stalled = new TaskCompletionSource<Dictionary<int, string>>();
        var commands = new List<int[]>();
        var applied = new Dictionary<int, string>();
        var clock = TimeSpan.Zero;

        Task<Dictionary<int, string>> Fetch(IReadOnlyList<int> keys, CancellationToken cancellationToken)
        {
            commands.Add(keys.ToArray());
            clock += TimeSpan.FromMilliseconds(30);
            if (commands.Count == 2)
            {
                /* The second chunk holds the plan that waits on a lock. */
                return stalled.Task;
            }

            if (keys.Contains(27))
            {
                throw new LockTimeout();
            }

            return Task.FromResult(Enumerable.Range(0, keys.Count).ToDictionary(ord => ord, ord => "plan-" + keys[ord]));
        }

        var run = BoundedPlanFetch.RunAsync(
            Enumerable.Range(0, 45).ToList(),
            Fetch,
            (ord, plan) => applied.Add(ord, plan),
            ex => ex is LockTimeout,
            TimeSpan.FromSeconds(5),
            () => clock,
            new PlanFetchLaterKeys<int>(150),
            key => key,
            CancellationToken.None,
            watch);

        /* Chunk 0 answered, chunk 1 is still running: its trigger wait is the second one asked for. */
        Assert.Equal(2, commands.Count);
        Assert.Equal(2, probe.Delays.Count);
        probe.Delays[1].SetResult();
        await probe.FiredOnce.Task;

        var reading = Assert.Single(probe.Fired);
        Assert.Equal(60, reading.ElapsedMs);
        Assert.Equal(20, reading.RowsRead);
        Assert.Equal(-1, reading.BytesRead);
        Assert.Equal(-1, reading.LastReadMs);

        stalled.SetException(new LockTimeout());
        var outcome = await run;

        /* The single-key retries and the last chunk run after the one probe of the run, so none is watched. */
        Assert.Equal(23, commands.Count);
        Assert.Equal(2, probe.Delays.Count);
        Assert.Single(probe.Fired);
        Assert.Equal(44, outcome.Answered);
        Assert.Equal(1, outcome.Skipped);
        Assert.DoesNotContain(27, applied.Keys);
    }
}

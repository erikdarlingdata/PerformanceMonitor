using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;
using Xunit;
using Loop = PerformanceMonitorLite.Services.CollectionBackgroundService.HousekeepingLoop;
using Step = PerformanceMonitorLite.Services.CollectionBackgroundService.HousekeepingStep;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5731: housekeeping (archival, retention, the purges, the Query Store backfill, analysis) runs on its own loops
/// instead of as awaited steps of the collection loop, so a long step no longer makes the collection loop miss its
/// next slots. The loops are driven through <see cref="CollectionBackgroundService.StartHousekeepingLoops"/> with a fake
/// delay and no real waiting; the collection side is the real <see cref="CollectionBackgroundService.WaitForNextCycleAsync"/>
/// on a fake clock.
/// </summary>
public sealed class HousekeepingLoopTests
{
    private static readonly DateTime T0 = new(2026, 10, 12, 10, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(1);

    /// <summary>A delay that returns when the test releases it, so the test decides when each wait ends.</summary>
    private sealed class ManualClock
    {
        private TaskCompletionSource _tick = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _waits;
        public int Waits => Volatile.Read(ref _waits);

        public Task Delay(TimeSpan wait, CancellationToken token)
        {
            var tick = _tick;
            Interlocked.Increment(ref _waits);
            return tick.Task.WaitAsync(token);
        }

        public void Release()
        {
            var tick = _tick;
            _tick = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            tick.SetResult();
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            await Task.Delay(1, timeout.Token);
        }
    }

    private static Task Done => Task.CompletedTask;

    private static Step Instant(string name, Action onRun) => new(name, _ => { onRun(); return Task.CompletedTask; });

    [Fact]
    public async Task SlowMaintenanceStep_DoesNotDelayTheNextCollection()
    {
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        var clock = new ManualClock();
        using var stop = new CancellationTokenSource();
        var loops = new[]
        {
            new Loop("maintenance", [new Step("archival", async _ => { Interlocked.Increment(ref entered); await blocked.Task; })]),
        };

        var tasks = CollectionBackgroundService.StartHousekeepingLoops(
            loops, Interval, () => true, Done, clock.Delay, (_, _, _) => { }, stop.Token);

        await WaitUntilAsync(() => clock.Waits == 1);
        clock.Release();
        await WaitUntilAsync(() => Volatile.Read(ref entered) == 1); /* the archive is now running and will not finish */

        /* Five collection slots on the real wait, with a fake clock: each one arrives on time while the step is unresolved. */
        var now = T0;
        var cycleStart = T0;
        for (var slot = 1; slot <= 5; slot++)
        {
            var previous = cycleStart;
            cycleStart = await CollectionBackgroundService.WaitForNextCycleAsync(
                cycleStart, Interval, () => now, (wait, _) => { now += wait; return Task.CompletedTask; }, CancellationToken.None);

            Assert.Equal(0, CollectionCycleSkip.CountSkippedSlots(previous, cycleStart, Interval));
            Assert.Equal(T0 + (slot * Interval), cycleStart);
            Assert.False(blocked.Task.IsCompleted);
        }

        Assert.Equal(1, Volatile.Read(ref entered)); /* and the housekeeping loop did not start a second pass behind it */
        blocked.SetResult();
        await stop.CancelAsync();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TheLoops_RunTheirStepsInOrder_AndNoLoopTicksBeforeTheFirstRound()
    {
        var order = new List<string>();
        var firstRound = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new ManualClock();
        using var stop = new CancellationTokenSource();
        var loops = new[]
        {
            new Loop("maintenance",
            [
                Instant("archival", () => { lock (order) order.Add("archival"); }),
                Instant("retention", () => { lock (order) order.Add("retention"); }),
                Instant("findings_cleanup", () => { lock (order) order.Add("findings_cleanup"); }),
                Instant("dismissed_alerts_cleanup", () => { lock (order) order.Add("dismissed_alerts_cleanup"); }),
            ]),
        };

        var tasks = CollectionBackgroundService.StartHousekeepingLoops(
            loops, Interval, () => true, firstRound.Task, clock.Delay, (_, _, _) => { }, stop.Token);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Equal(0, clock.Waits); /* not even the first wait has started */
        Assert.Empty(order);

        firstRound.SetResult();
        await WaitUntilAsync(() => clock.Waits == 1);
        Assert.Empty(order);
        clock.Release();
        await WaitUntilAsync(() => { lock (order) return order.Count == 4; });
        Assert.Equal(["archival", "retention", "findings_cleanup", "dismissed_alerts_cleanup"], order);

        await stop.CancelAsync();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Cancelling_EndsEveryLoop_EvenOneStillWaitingForTheFirstRound()
    {
        var clock = new ManualClock();
        using var stop = new CancellationTokenSource();
        var neverStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loops = new[]
        {
            new Loop("maintenance", [Instant("archival", () => { })]),
            new Loop("query_store_backfill", [Instant("query_store_backfill", () => { })]),
            new Loop("analysis", [Instant("analysis", () => { })]),
        };

        var waiting = CollectionBackgroundService.StartHousekeepingLoops(
            loops, Interval, () => true, neverStarted.Task, clock.Delay, (_, _, _) => { }, stop.Token);
        var ticking = CollectionBackgroundService.StartHousekeepingLoops(
            loops, Interval, () => true, Done, clock.Delay, (_, _, _) => { }, stop.Token);
        Assert.Equal(3, waiting.Length);
        await WaitUntilAsync(() => clock.Waits == 3);

        await stop.CancelAsync();
        await Task.WhenAll(waiting.Concat(ticking)).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.All(waiting.Concat(ticking), t => Assert.True(t.IsCompletedSuccessfully));
    }

    [Fact]
    public async Task AStepThatThrows_CostsOnlyItself_AndAGatedTickRunsNothing()
    {
        var order = new List<string>();
        var faults = new List<string>();
        var allowed = false;
        var clock = new ManualClock();
        using var stop = new CancellationTokenSource();
        var loops = new[]
        {
            new Loop("maintenance",
            [
                new Step("archival", _ => throw new InvalidOperationException("boom")),
                Instant("retention", () => { lock (order) order.Add("retention"); }),
            ]),
        };

        var tasks = CollectionBackgroundService.StartHousekeepingLoops(
            loops, Interval, () => Volatile.Read(ref allowed), Done, clock.Delay,
            (loop, step, ex) => { lock (faults) faults.Add($"{loop}/{step}/{ex.Message}"); }, stop.Token);

        await WaitUntilAsync(() => clock.Waits == 1);
        clock.Release(); /* paused or database down: the gate refuses the tick */
        await WaitUntilAsync(() => clock.Waits == 2);
        Assert.Empty(order);
        Assert.Empty(faults);

        Volatile.Write(ref allowed, true);
        clock.Release();
        await WaitUntilAsync(() => { lock (order) return order.Count == 1; });
        Assert.Equal(["maintenance/archival/boom"], faults); /* the later step still ran, and the loop is waiting again */
        await WaitUntilAsync(() => clock.Waits == 3);

        await stop.CancelAsync();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }

    private static string LoopSource([CallerFilePath] string thisFile = "")
        => File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Services", "CollectionBackgroundService.cs")));

    [Fact]
    public void TheRealLoops_HoldTheSixSteps_InTheRulesOrder_AndAreStartedBeforeTheCollectionLoopAndAwaitedAtShutdown()
    {
        var source = LoopSource();
        var build = source[source.IndexOf("IReadOnlyList<HousekeepingLoop> BuildHousekeepingLoops()", StringComparison.Ordinal)..];
        build = build[..build.IndexOf("];", StringComparison.Ordinal)];

        var last = -1;
        foreach (var call in new[] { "RunArchivalIfDueAsync(token)", "RunRetentionIfDueAsync()", "RunFindingsCleanupIfDueAsync()",
                     "RunDismissedAlertsCleanupIfDueAsync()", "RunQueryStoreBackfillIfDueAsync(token)", "RunAnalysisIfDueAsync(token)" })
        {
            var at = build.IndexOf(call, StringComparison.Ordinal);
            Assert.True(at > last, $"{call} is missing or out of order in BuildHousekeepingLoops");
            last = at;
        }

        /* The first three share the maintenance loop (archival before retention: they delete and merge the same parquet files). */
        Assert.True(build.IndexOf("new HousekeepingLoop(\"maintenance\"", StringComparison.Ordinal)
                    < build.IndexOf("RunArchivalIfDueAsync(token)", StringComparison.Ordinal));
        Assert.True(build.IndexOf("RunDismissedAlertsCleanupIfDueAsync()", StringComparison.Ordinal)
                    < build.IndexOf("new HousekeepingLoop(\"query_store_backfill\"", StringComparison.Ordinal));
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(build, @"new HousekeepingLoop\(").Count);

        var execute = source[source.IndexOf("protected override async Task ExecuteAsync", StringComparison.Ordinal)..];
        var started = execute.IndexOf("var housekeepingLoops = StartHousekeepingLoops(", StringComparison.Ordinal);
        var cycle = execute.IndexOf("while (!stoppingToken.IsCancellationRequested)", StringComparison.Ordinal);
        Assert.True(started > 0 && cycle > started, "the housekeeping loops must be started before the collection loop");
        Assert.Contains("firstRound.TrySetResult();", execute);
        Assert.Contains("await Task.WhenAll(housekeepingLoops);", execute);
        Assert.Contains("TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)", execute);
        Assert.Contains("!IsPaused && !_collectorService.LocalDatabaseIsDown()", execute[started..cycle]);

        /* No housekeeping call is awaited from ExecuteAsync itself: only from the loops' step lambdas. */
        Assert.DoesNotContain("IfDueAsync(", execute[..execute.IndexOf("Task.WhenAll(housekeepingLoops)", StringComparison.Ordinal)]);
    }
}

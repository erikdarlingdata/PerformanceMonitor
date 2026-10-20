/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6186: a collector the monitored server refuses on every run (a permission refusal no grant to the monitoring user can fix,
/// such as tempdb_stats for a contained user on Azure SQL Database) used to run on its normal schedule and write a PERMISSIONS
/// row every interval, with no end. It now backs off while the same refusal repeats, and goes back to the normal interval on
/// its first success or a changed answer. Nothing here needs a store or a monitored server.
/// </summary>
public sealed class RefusedCollectorBackoffTests
{
    private const string Collector = "tempdb_stats";
    private const string Denied = "The server refused the read: the user has no access to tempdb.";

    private static readonly TimeSpan OneMinute = TimeSpan.FromMinutes(1);

    [Fact]
    public void TheWaitDoublesPastTheFreeRefusals_AndStopsAtAnHourForAOneMinuteCollector()
    {
        Assert.Equal(OneMinute, RefusedCollectorBackoff.DelayAfter(1, OneMinute));
        Assert.Equal(OneMinute, RefusedCollectorBackoff.DelayAfter(RefusedCollectorBackoff.FreeRefusals, OneMinute));
        Assert.Equal(TimeSpan.FromMinutes(2), RefusedCollectorBackoff.DelayAfter(RefusedCollectorBackoff.FreeRefusals + 1, OneMinute));
        Assert.Equal(TimeSpan.FromMinutes(4), RefusedCollectorBackoff.DelayAfter(RefusedCollectorBackoff.FreeRefusals + 2, OneMinute));
        Assert.Equal(RefusedCollectorBackoff.MaxDelay, RefusedCollectorBackoff.DelayAfter(50, OneMinute));
        Assert.Equal(RefusedCollectorBackoff.MaxDelay, RefusedCollectorBackoff.DelayAfter(int.MaxValue, OneMinute));
    }

    [Fact]
    public void ABackoffNeverMakesACollectorRunMoreOftenThanItsOwnInterval()
    {
        var daily = TimeSpan.FromHours(24);
        Assert.Equal(daily, RefusedCollectorBackoff.DelayAfter(40, daily));

        var backoff = new RefusedCollectorBackoff();
        var now = DateTime.UtcNow;
        for (var run = 0; run < 10; run++)
        {
            backoff.Record(Collector, refused: true, Denied, now, daily);
        }

        /* The ordinary schedule already spaces a daily collector's tries: nothing is stamped that could push it past its slot. */
        Assert.False(backoff.IsBackedOff(Collector, now.AddHours(1), daily));
    }

    [Fact]
    public void ARefusalThatRepeats_BacksOff_AndTheFirstSuccessRestoresTheNormalInterval()
    {
        var backoff = new RefusedCollectorBackoff();
        var now = DateTime.UtcNow;
        for (var run = 1; run <= RefusedCollectorBackoff.FreeRefusals; run++)
        {
            backoff.Record(Collector, refused: true, Denied, now, OneMinute);
            Assert.False(backoff.IsBackedOff(Collector, now.AddSeconds(30), OneMinute), $"refusal {run} still runs at the normal interval");
        }

        backoff.Record(Collector, refused: true, Denied, now, OneMinute);
        Assert.True(backoff.IsBackedOff(Collector, now.AddSeconds(90), OneMinute));
        Assert.False(backoff.IsBackedOff(Collector, now.AddMinutes(3), OneMinute), "never stopped for good: it is tried again after the wait");

        backoff.Record(Collector, refused: false, null, now.AddMinutes(3), OneMinute);
        Assert.Equal(0, backoff.Streak(Collector));
        Assert.False(backoff.IsBackedOff(Collector, now.AddMinutes(3), OneMinute));
    }

    [Fact]
    public void ADifferentAnswerRestartsTheStreak()
    {
        var backoff = new RefusedCollectorBackoff();
        var now = DateTime.UtcNow;
        for (var run = 0; run < 6; run++)
        {
            backoff.Record(Collector, refused: true, Denied, now, OneMinute);
        }

        Assert.True(backoff.IsBackedOff(Collector, now.AddSeconds(30), OneMinute));

        backoff.Record(Collector, refused: true, "A different refusal.", now.AddHours(1), OneMinute);
        Assert.Equal(1, backoff.Streak(Collector));
        Assert.False(backoff.IsBackedOff(Collector, now.AddHours(1), OneMinute));
    }

    [Fact]
    public void AStampFarAheadOfTheClock_AClockStepBackwards_CountsAsDue()
    {
        var backoff = new RefusedCollectorBackoff();
        var now = DateTime.UtcNow;
        for (var run = 0; run < 12; run++)
        {
            backoff.Record(Collector, refused: true, Denied, now, OneMinute);
        }

        Assert.True(backoff.IsBackedOff(Collector, now, OneMinute));
        Assert.False(backoff.IsBackedOff(Collector, now.AddHours(-5), OneMinute));
    }

    private static (DarlingWorker Worker, DarlingWorker.ServerLoopState Server, int[] Runs, string?[] Answer) Rig(int serverId)
    {
        var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
        var server = DailyRunKit.MakeServer(serverId, Collector);
        var runs = new int[1];
        var answer = new string?[] { Denied };
        worker.RunOneBodyOverride = (_, _, collector, _) =>
        {
            if (collector == Collector)
            {
                runs[0]++;
            }

            return Task.FromResult(1);
        };
        worker.RunOneRefusalOverride = collector => collector == Collector ? answer[0] : null;
        return (worker, server, runs, answer);
    }

    private static async Task PassAsync(DarlingWorker worker, DarlingWorker.ServerLoopState server)
    {
        /* One scheduler pass with the collector due, as it is every interval. */
        server.NextDue[Collector] = DateTime.UtcNow.AddMinutes(-5);
        await worker.RunDueCollectorsAsync(server, null!, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ACollectorRefusedRunAfterRun_RunsLessOften_AndTheFirstSuccessRestoresTheInterval()
    {
        var (worker, server, runs, answer) = Rig(7601);
        const int passes = 12;

        for (var pass = 0; pass < passes; pass++)
        {
            await PassAsync(worker, server);
        }

        /* Each run is one collection_log row: the refusal writes the free runs and the first backed-off one, not one per pass. */
        Assert.Equal(RefusedCollectorBackoff.FreeRefusals + 1, runs[0]);
        Assert.True(runs[0] < passes);

        /* The wait passes: it is tried again, once, and waits again for longer. */
        server.RefusedBackoff.SetNotBeforeForTests(Collector, DateTime.UtcNow.AddSeconds(-1));
        await PassAsync(worker, server);
        await PassAsync(worker, server);
        Assert.Equal(RefusedCollectorBackoff.FreeRefusals + 2, runs[0]);

        /* The grant arrives: the next try succeeds, and every pass after it runs the collector again. */
        server.RefusedBackoff.SetNotBeforeForTests(Collector, DateTime.UtcNow.AddSeconds(-1));
        answer[0] = null;
        await PassAsync(worker, server);
        Assert.Equal(0, server.RefusedBackoff.Streak(Collector));

        var before = runs[0];
        for (var pass = 0; pass < 4; pass++)
        {
            await PassAsync(worker, server);
        }

        Assert.Equal(before + 4, runs[0]);
    }

    [Fact]
    public async Task ABackedOffCollector_PicksUpAChangedAnswerAtItsNextTry()
    {
        var (worker, server, runs, answer) = Rig(7602);
        for (var pass = 0; pass < 8; pass++)
        {
            await PassAsync(worker, server);
        }

        Assert.Equal(RefusedCollectorBackoff.FreeRefusals + 1, runs[0]);

        answer[0] = "Now refused for another reason.";
        server.RefusedBackoff.SetNotBeforeForTests(Collector, DateTime.UtcNow.AddSeconds(-1));
        await PassAsync(worker, server);
        Assert.Equal(1, server.RefusedBackoff.Streak(Collector));

        var before = runs[0];
        await PassAsync(worker, server);
        await PassAsync(worker, server);
        Assert.Equal(before + 2, runs[0]);
    }

    [Fact]
    public void ACollectorBackedOffForHours_StillBandsAsNoPermissions_NotStoppedOrStale()
    {
        /* Seven days of an hourly refusal: 168 runs, every one a PERMISSIONS run, the newest under an hour old. The runs are
           rarer than the collector's 1-minute interval, and the band is still the setup step. */
        var band = CollectorHealthClassifier.Classify(
            totalRuns: 168, successCount: 0, errorCount: 0, permissionDeniedCount: 168, extensionMissingCount: 0,
            abandonedCount: 0, hoursSinceLastSuccess: 999, hoursSinceLastRun: 0.9, frequencyMinutes: 1,
            latestRunNote: null, trailingSuccessRuns: 0);

        Assert.Equal(CollectorHealthClassifier.NoPermissions, band);
    }
}

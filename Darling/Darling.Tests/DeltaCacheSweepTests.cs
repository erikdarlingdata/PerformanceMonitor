/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7649: the delta calculator's baseline cache used to keep every key it ever saw, so a query_stats group that gets
/// new plan handles on every pass (2,000 candidate rows a run, eight groups per key) grew until the service ran out
/// of memory. A group is now swept once per pass of entries older than its longest gap, measured from the pass's
/// collection time. These pin the halves of that: the cache stays bounded, every delta through
/// <c>CalculateDeltaWithInterval</c> is unchanged, and a series-age call that finds its stale baseline swept gets the
/// first-sighting rescue (current, pass gap) where the discarded baseline would have given (0, 0).
/// </summary>
public sealed class DeltaCacheSweepTests
{
    private const int ServerId = 7;
    private const string Group = "query_stats_exec";
    private const int Gap = CollectorDeltaCalculator.DefaultMaxGapSeconds;
    private static readonly DateTime T0 = new(2026, 11, 5, 8, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The documented update and add rules, written out independently of the calculator: no baseline or a
    /// baseline older than the gap is (0, 0), a lower value is (0, 0), anything else is the difference over the real
    /// interval.</summary>
    private sealed class Model
    {
        private readonly Dictionary<string, (long Value, DateTime Time)> _baselines = new();

        public (long Delta, int Interval) Next(string key, long value, DateTime time, int gap)
        {
            (long, int) result = (0, 0);
            if (_baselines.TryGetValue(key, out var previous)
                && (time - previous.Time).TotalSeconds <= gap
                && value >= previous.Value)
            {
                result = (value - previous.Value, (int)(time - previous.Time).TotalSeconds);
            }

            _baselines[key] = (value, time);
            return result;
        }

        /// <summary>How many keys a call at <paramref name="time"/> could still subtract from.</summary>
        public int KeysWithin(DateTime time, int gap)
        {
            var count = 0;
            foreach (var baseline in _baselines.Values)
            {
                if ((time - baseline.Time).TotalSeconds <= gap)
                {
                    count++;
                }
            }

            return count;
        }
    }

    /// <summary>Seeds baselines the way a host restores them from its store, without going through a delta call.</summary>
    private sealed class SeedableCalculator : CollectorDeltaCalculator
    {
        public void SeedBaseline(int serverId, string group, string key, long value, DateTime time)
            => Seed(serverId, group, key, value, time);
    }

    [Fact]
    public void ManyDistinctKeysOverManyPasses_LeaveOnlyTheKeysSeenInsideTheGap_AndChangeNoDelta()
    {
        const int passes = 40;
        const int hot = 20;
        const int churnPerPass = 200;
        const int passSeconds = 600;

        var calc = new CollectorDeltaCalculator();
        var model = new Model();
        var counter = 0L;

        for (var pass = 0; pass < passes; pass++)
        {
            var time = T0.AddSeconds(pass * passSeconds);
            counter += 10;

            var keys = new List<string>();
            for (var i = 0; i < hot; i++)
            {
                keys.Add($"hot-{i}");
            }

            for (var i = 0; i < churnPerPass; i++)
            {
                keys.Add($"churn-{pass}-{i}");
            }

            /* Keys from earlier passes that come back: 3 passes back is inside the gap (a real delta), 12 passes
               back is past it (the gap policy discards that baseline: (0, 0)). */
            if (pass >= 12)
            {
                keys.Add($"churn-{pass - 3}-0");
                keys.Add($"churn-{pass - 12}-0");
            }

            foreach (var key in keys)
            {
                var expected = model.Next(key, counter, time, Gap);
                var delta = calc.CalculateDeltaWithInterval(ServerId, Group, key, counter, out var interval,
                    collectionTime: time, maxGapSeconds: Gap);

                Assert.True(expected == (delta, interval),
                    $"pass {pass} key {key}: expected {expected}, got ({delta}, {interval})");
            }
        }

        /* The last pass sweeps entries older than 3,600 s before it writes, so what is left is the keys the gap policy
           would still subtract from: the hot keys, the churn keys of the passes inside the gap, and the few older ones
           a later pass brought back. Dev's code keeps every key ever seen: 20 + 200 * 40 + the returners. */
        var last = T0.AddSeconds((passes - 1) * passSeconds);
        var held = calc.PeekBaselines(ServerId, Group).Count;
        Assert.Equal(model.KeysWithin(last, Gap), held);
        Assert.True(held < hot + churnPerPass * 8, $"{held} entries held after {passes} passes");
    }

    /// <summary>Runs one churn key per pass at 600 s steps from <see cref="T0"/> up to and including pass
    /// <paramref name="lastPass"/>, each through <c>CalculateDeltaWithSeriesAge</c>, so the group grows every pass and a
    /// sweep is due on each.</summary>
    private static void RunChurnPasses(CollectorDeltaCalculator calc, int firstPass, int lastPass)
    {
        for (var pass = firstPass; pass <= lastPass; pass++)
        {
            calc.CalculateDeltaWithSeriesAge(ServerId, Group, $"churn-{pass}", 10, seriesAgeSeconds: null, out _,
                collectionTime: T0.AddSeconds(pass * 600), maxGapSeconds: Gap);
        }
    }

    [Fact]
    public void ABaselineOlderThanTheGap_ThatComesBackWithAYoungSeriesAge_IsCreditedItsWholeCountAfterASweep()
    {
        var calc = new CollectorDeltaCalculator();

        /* A statement-level recompile keeps the plan handle, so the key is the same. Its baseline (1) was written at
           T0 and the key then fell out of the candidate list for an hour and more. */
        calc.CalculateDeltaWithSeriesAge(ServerId, Group, "stale", 1, seriesAgeSeconds: null, out _,
            collectionTime: T0, maxGapSeconds: Gap);
        RunChurnPasses(calc, 1, 7);

        /* Pass 7 is T0 + 4,200 s: past the 3,600 s gap from the baseline, and 600 s after the previous pass. The
           statement recompiled 300 s ago and has run 5 times since, so its counter did not go down. The first call of
           the pass sweeps the stale baseline, so this call takes the first-sighting rescue and credits what ran since
           the compile. Without the sweep the stale arm of the update branch reports (0, 0). */
        var delta = calc.CalculateDeltaWithSeriesAge(ServerId, Group, "stale", 5, seriesAgeSeconds: 300,
            out var interval, collectionTime: T0.AddSeconds(7 * 600), maxGapSeconds: Gap);

        Assert.Equal((5L, 600), (delta, interval));
    }

    [Fact]
    public void ABaselineInsideTheGap_IsLeftAloneByTheSweep_AndGetsItsOrdinaryDeltaWithASeriesAge()
    {
        var calc = new CollectorDeltaCalculator();

        calc.CalculateDeltaWithSeriesAge(ServerId, Group, "fresh", 10, seriesAgeSeconds: null, out _,
            collectionTime: T0, maxGapSeconds: Gap);
        RunChurnPasses(calc, 1, 5);
        /* Seen again at T0 + 3,000 s, inside the gap: this baseline must survive every later sweep. */
        calc.CalculateDeltaWithSeriesAge(ServerId, Group, "fresh", 12, seriesAgeSeconds: null, out _,
            collectionTime: T0.AddSeconds(5 * 600), maxGapSeconds: Gap);
        RunChurnPasses(calc, 6, 7);

        /* T0 + 4,200 s: the sweep ran first and dropped the churn keys older than the gap, but "fresh" is 1,200 s old.
           The series age (50,000 s) is older than the pass gap, so only a surviving baseline can give a delta here. */
        var delta = calc.CalculateDeltaWithSeriesAge(ServerId, Group, "fresh", 17, seriesAgeSeconds: 50_000,
            out var interval, collectionTime: T0.AddSeconds(7 * 600), maxGapSeconds: Gap);

        Assert.Equal((5L, 1200), (delta, interval));
    }

    [Fact]
    public void AGroupWithACallerThatPassesNoLimit_IsNeverSweptByAge()
    {
        var calc = new CollectorDeltaCalculator();

        for (var pass = 0; pass < 30; pass++)
        {
            var time = T0.AddSeconds(pass * 600);
            for (var i = 0; i < 50; i++)
            {
                /* maxGapSeconds left at its default of 0: a baseline of any age is still read. */
                calc.CalculateDelta(ServerId, Group, $"k-{pass}-{i}", 100, time);
            }
        }

        Assert.Equal(30 * 50, calc.PeekBaselines(ServerId, Group).Count);
    }

    [Fact]
    public void OneNoLimitCallerAmongLimitedOnes_StopsTheSweepForItsGroup_ButNotForAnotherGroup()
    {
        var calc = new CollectorDeltaCalculator();

        for (var pass = 0; pass < 30; pass++)
        {
            var time = T0.AddSeconds(pass * 600);
            for (var i = 0; i < 50; i++)
            {
                calc.CalculateDeltaWithInterval(ServerId, "limited", $"k-{pass}-{i}", 100, out _,
                    collectionTime: time, maxGapSeconds: Gap);
                calc.CalculateDeltaWithInterval(ServerId, "mixed", $"k-{pass}-{i}", 100, out _,
                    collectionTime: time, maxGapSeconds: i == 0 && pass == 3 ? 0 : Gap);
            }
        }

        Assert.Equal(7 * 50, calc.PeekBaselines(ServerId, "limited").Count);
        /* Passes 0 to 3 were swept-free (pass 3 declared the no-limit caller before it could sweep), and after it
           nothing is removed: every key of all 30 passes is still there. */
        Assert.Equal(30 * 50, calc.PeekBaselines(ServerId, "mixed").Count);
    }

    [Fact]
    public void ACaptureThenASweepThenARestore_PutsBackWhatTheFailedRunChanged()
    {
        var calc = new SeedableCalculator();
        var older = T0;
        var recent = T0.AddSeconds(7200 - 600);
        var run = T0.AddSeconds(7200);

        /* Before the run, as a restart seed leaves them (no call has swept yet): "stale" and "gone" were last seen two
           hours ago, "recent" ten minutes ago. */
        calc.SeedBaseline(ServerId, Group, "stale", 100, older);
        calc.SeedBaseline(ServerId, Group, "gone", 100, older);
        calc.SeedBaseline(ServerId, Group, "recent", 100, recent);

        /* The run captures the keys it is about to touch, then advances them. Its first call sweeps the group, which
           drops "stale" (captured) and "gone" (not captured, and dead); the run then writes "stale" back as a new
           first sighting and adds "fresh". */
        var snapshot = calc.CaptureBaselines(ServerId, new[] { Group }, new[] { "stale", "recent", "fresh" }, run);
        Assert.NotNull(snapshot);
        calc.CalculateDeltaWithInterval(ServerId, Group, "recent", 150, out _, collectionTime: run, maxGapSeconds: Gap);
        calc.CalculateDeltaWithInterval(ServerId, Group, "stale", 130, out _, collectionTime: run, maxGapSeconds: Gap);
        calc.CalculateDeltaWithInterval(ServerId, Group, "fresh", 5, out _, collectionTime: run, maxGapSeconds: Gap);
        Assert.False(calc.PeekBaselines(ServerId, Group).ContainsKey("gone"));

        snapshot!.Restore();

        var after = calc.PeekBaselines(ServerId, Group);
        Assert.Equal(100, after["stale"]);   /* back, with its two-hour-old time */
        Assert.Equal(100, after["recent"]);
        Assert.False(after.ContainsKey("fresh"));
        Assert.False(after.ContainsKey("gone")); /* dead before the run and not the run's to restore */

        /* The retry's deltas are what they would have been without the failed run or the sweep. */
        var retry = run.AddSeconds(600);
        var staleDelta = calc.CalculateDeltaWithInterval(ServerId, Group, "stale", 130, out var staleInterval,
            collectionTime: retry, maxGapSeconds: Gap);
        var recentDelta = calc.CalculateDeltaWithInterval(ServerId, Group, "recent", 150, out var recentInterval,
            collectionTime: retry, maxGapSeconds: Gap);
        Assert.Equal((0L, 0), (staleDelta, staleInterval));
        Assert.Equal((50L, 600 + 600), (recentDelta, recentInterval));
    }
}

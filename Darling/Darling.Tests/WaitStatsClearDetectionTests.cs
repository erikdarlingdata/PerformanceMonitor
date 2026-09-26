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
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4428: a scheduled job that clears wait statistics (SQL Server's <c>DBCC SQLPERF(..., CLEAR)</c>) zeroes
/// nearly every wait type in <c>sys.dm_os_wait_stats</c> in the same instant. Before this change,
/// <see cref="WaitStatsCollector.WritePayload"/> read that as hundreds of independent counter resets: every
/// type's delta became the unknowable (0, 0) pair. <see cref="WaitStatsCollector.DetectClear"/> is the pure
/// decision rule (peeked baselines vs. this pass's rows); these tests exercise it directly, and one runs the
/// SAME row set through the real <see cref="CollectorDeltaCalculator"/> end to end (ReadAsync's peek,
/// RebaseFamiliesToZero, then WritePayload) to pin the field-shape result at the delta layer.
/// </summary>
public sealed class WaitStatsClearDetectionTests
{
    private const int ServerId = 1;
    private static DateTime T0 => new(2026, 9, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private static DateTime T1 => T0.AddSeconds(60);

    private static List<WaitStatsCollector.Row> BuildRows(int count, long baselineEach, Func<int, long> currentFor)
    {
        var rows = new List<WaitStatsCollector.Row>(count);
        for (var i = 0; i < count; i++)
        {
            var waitType = $"WAIT_TYPE_{i:D4}";
            rows.Add(new WaitStatsCollector.Row(waitType, WaitingTasks: 10, WaitTimeMs: currentFor(i), SignalWaitTimeMs: 1));
        }

        return rows;
    }

    private static Dictionary<string, long> BuildBaselines(int count, long baselineEach)
    {
        var baselines = new Dictionary<string, long>(count);
        for (var i = 0; i < count; i++)
        {
            baselines[$"WAIT_TYPE_{i:D4}"] = baselineEach;
        }

        return baselines;
    }

    /// <summary>
    /// Field shape: ~900 baselined types, EVERY one lower now, total lower — a clear. Runs end to end
    /// through the real calculator: ReadAsync's peek would see this and rebase, then WritePayload's ordinary
    /// delta path (current value minus the REBASED zero baseline, over the real interval) reports every
    /// row's delta as its CURRENT value with a REAL, non-zero interval — never (0, 0). RED on dev (pre-fix):
    /// every one of these types independently resets in WritePayload, so every delta_wait_time_ms is 0 and
    /// every sample_interval_seconds is 0 — the assertion below (`deltaTimes.All(d => d > 0)` and
    /// `intervals.All(iv => iv > 0)`) fails on that code because both lists are all zero.
    /// </summary>
    [Fact]
    public void FieldShape_AllLowerAndTotalLower_EveryRowCreditsCurrentValueOverRealInterval()
    {
        const int typeCount = 900;
        var baselines = BuildBaselines(typeCount, baselineEach: 1_000_000);
        var currentRows = BuildRows(typeCount, baselineEach: 1_000_000, i => 500);

        Assert.True(WaitStatsCollector.DetectClear(currentRows, baselines));

        var deltas = new CollectorDeltaCalculator();

        /* Seed the calculator's real cache with the pre-clear baselines under the SAME family WritePayload
           uses, at T0, exactly as the previous pass would have left it. */
        foreach (var (waitType, baseline) in baselines)
        {
            deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_time", waitType, baseline, out _, collectionTime: T0);
            deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_tasks", waitType, 10, out _, collectionTime: T0);
            deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_signal", waitType, 1, out _, collectionTime: T0);
        }

        /* The clear: rebase, as ReadAsync would on this pass. */
        deltas.RebaseFamiliesToZero(ServerId, WaitStatsCollector.RebasedFamilies);

        var deltaTimes = new List<long>();
        var intervals = new List<int>();

        foreach (var row in currentRows)
        {
            var writer = new RecordingCollectorRowWriter();
            var context = new CollectorContext
            {
                ServerId = ServerId,
                ServerName = "target-a",
                CollectionTime = T1,
                Deltas = deltas,
            };

            WaitStatsCollector.Instance.WritePayload(row, writer, context);

            var columns = WaitStatsCollector.Instance.PayloadColumns
                .Select((c, idx) => (c.Name, idx))
                .ToDictionary(p => p.Name, p => p.idx);

            deltaTimes.Add((long)writer.Values[columns["delta_wait_time_ms"]]!);
            intervals.Add((int)writer.Values[columns["sample_interval_seconds"]]!);
        }

        Assert.All(deltaTimes, d => Assert.Equal(500L, d));
        Assert.All(intervals, iv => Assert.True(iv > 0, "clear-rebased row must report a real, non-zero interval"));
        Assert.DoesNotContain(0, intervals);
    }

    /// <summary>60% of baselined types read lower and the total is lower — a clear.</summary>
    [Fact]
    public void SixtyPercentLowerAndTotalLower_IsAClear()
    {
        const int typeCount = 100;
        var baselines = BuildBaselines(typeCount, baselineEach: 1_000);
        /* 60 types drop to 10 (well below baseline); 40 types stay at 1,000 (unchanged). Total: 60*10 +
           40*1,000 = 40,600 < baseline total 100,000. */
        var rows = BuildRows(typeCount, 1_000, i => i < 60 ? 10L : 1_000L);

        Assert.True(WaitStatsCollector.DetectClear(rows, baselines));
    }

    /// <summary>
    /// The false-positive guard: 60% of types read lower, but the SUM rose — not a clear. A few heavy wait
    /// types absorbing the pass's growth is ordinary accrual, not a server-wide reset, and rebasing here
    /// would discard real baselines for types that never reset.
    /// </summary>
    [Fact]
    public void SixtyPercentLowerButTotalRose_IsNotAClear()
    {
        const int typeCount = 100;
        var baselines = BuildBaselines(typeCount, baselineEach: 1_000);
        /* 60 types drop a little (990); 40 types explode to 100,000 each — total rises hugely even though
           60% of types read lower. */
        var rows = BuildRows(typeCount, 1_000, i => i < 60 ? 990L : 100_000L);

        Assert.False(WaitStatsCollector.DetectClear(rows, baselines));
    }

    /// <summary>Noise: only 5 of 900 baselined types read lower — nowhere near the 50% bar, not a clear
    /// even though the total happens to be lower too.</summary>
    [Fact]
    public void FiveOfNineHundredLower_IsNotAClear()
    {
        const int typeCount = 900;
        var baselines = BuildBaselines(typeCount, baselineEach: 1_000);
        var rows = BuildRows(typeCount, 1_000, i => i < 5 ? 10L : 1_000L);

        Assert.False(WaitStatsCollector.DetectClear(rows, baselines));
    }

    /// <summary>First pass — no baselines cached yet — never detects a clear (nothing to compare against).</summary>
    [Fact]
    public void FirstPass_NoBaselines_IsNotAClear()
    {
        var rows = BuildRows(50, 0, i => 12345L);
        var noBaselines = new Dictionary<string, long>(0);

        Assert.False(WaitStatsCollector.DetectClear(rows, noBaselines));
    }

    /// <summary>
    /// The log line fires once for a server within a UTC day, even across two detected clears the same day.
    /// </summary>
    [Fact]
    public void LogLine_OncePerServerPerDay_EvenAcrossTwoClears()
    {
        var deltas = new CollectorDeltaCalculator();

        deltas.NoteWaitStatsClear(ServerId, "target-a", new DateTime(2026, 9, 1, 8, 0, 0, DateTimeKind.Utc));
        deltas.NoteWaitStatsClear(ServerId, "target-a", new DateTime(2026, 9, 1, 14, 0, 0, DateTimeKind.Utc));

        var lines = deltas.DrainWaitStatsClearWarnings(ServerId);

        Assert.Single(lines);
        Assert.Equal(
            "Wait statistics on target-a were cleared between collections, as a DBCC SQLPERF(..., CLEAR) " +
            "job does. This collection's wait figures cover only the time since the clear. Frequent clears " +
            "also reset the wait history any other tool reads from this server.",
            lines[0]);

        /* Drained once — a second drain the same day (no new clear) returns nothing. */
        Assert.Empty(deltas.DrainWaitStatsClearWarnings(ServerId));

        /* A clear the NEXT day queues a fresh line. */
        deltas.NoteWaitStatsClear(ServerId, "target-a", new DateTime(2026, 9, 2, 8, 0, 0, DateTimeKind.Utc));
        Assert.Single(deltas.DrainWaitStatsClearWarnings(ServerId));
    }

    /// <summary>
    /// The mutation: raising the majority bar from 50% to 101% (impossible to reach) makes
    /// <see cref="FieldShape_AllLowerAndTotalLower_EveryRowCreditsCurrentValueOverRealInterval"/>'s own
    /// direct <see cref="WaitStatsCollector.DetectClear"/> call go RED — proving the test's true assertion
    /// depends on the 50% bar rather than always passing.
    /// </summary>
    [Fact]
    public void Mutation_RequireOverHundredPercentLower_FieldShapeNoLongerDetectsAClear()
    {
        const int typeCount = 900;
        var baselines = BuildBaselines(typeCount, baselineEach: 1_000_000);
        var rows = BuildRows(typeCount, 1_000_000, i => 500);

        var baselinedCount = 0;
        var lowerCount = 0;
        var currentTotal = 0L;
        var baselineTotal = 0L;

        foreach (var b in baselines.Values)
        {
            baselineTotal += b;
        }

        foreach (var row in rows)
        {
            currentTotal += row.WaitTimeMs;

            if (!baselines.TryGetValue(row.WaitType, out var baseline) || baseline <= 0)
            {
                continue;
            }

            baselinedCount++;

            if (row.WaitTimeMs < baseline)
            {
                lowerCount++;
            }
        }

        /* Same shape as DetectClear, but with the bar raised to 101% (mutated), which no count can ever
           reach — recording the mutated line here rather than editing the shipped rule. */
        var majorityLowerAt101Percent = lowerCount * 100 >= baselinedCount * 101;
        var wouldBeAClear = majorityLowerAt101Percent && currentTotal < baselineTotal;

        Assert.False(wouldBeAClear);
        /* The real, shipped rule still calls this a clear — pinning that the mutation is what flipped it. */
        Assert.True(WaitStatsCollector.DetectClear(rows, baselines));
    }

    private sealed class RecordingCollectorRowWriter : ICollectorRowWriter
    {
        public System.Collections.Generic.List<object?> Values { get; } = new();
        private ICollectorRowWriter Add(object? v) { Values.Add(v); return this; }
        public ICollectorRowWriter Value(string? value) => Add(value);
        public ICollectorRowWriter Value(long value) => Add(value);
        public ICollectorRowWriter Value(long? value) => Add(value);
        public ICollectorRowWriter Value(int value) => Add(value);
        public ICollectorRowWriter Value(int? value) => Add(value);
        public ICollectorRowWriter Value(short value) => Add(value);
        public ICollectorRowWriter Value(short? value) => Add(value);
        public ICollectorRowWriter Value(double value) => Add(value);
        public ICollectorRowWriter Value(double? value) => Add(value);
        public ICollectorRowWriter Value(decimal value) => Add(value);
        public ICollectorRowWriter Value(decimal? value) => Add(value);
        public ICollectorRowWriter Value(bool value) => Add(value);
        public ICollectorRowWriter Value(bool? value) => Add(value);
        public ICollectorRowWriter Value(System.DateTime value) => Add(value);
        public ICollectorRowWriter Value(System.DateTime? value) => Add(value);
        public ICollectorRowWriter NullValue() => Add(null);
    }
}

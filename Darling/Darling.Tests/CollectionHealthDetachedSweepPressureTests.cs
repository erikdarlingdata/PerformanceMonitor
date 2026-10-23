/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4999 (part of #4938): get_collection_health's sweep-pressure roll-up compares the collectors' single-run
/// costs with the one 60-second body their server's sequential pass has to fit in, so it may count only the
/// collectors that run IN that pass. A collector that runs detached does not: it runs beside the pass, and a
/// long p95 on it said BODY_OVERRUN for a body that never waited on it (index_object_stats, once a day, naming
/// itself as the collector that owns the body).
///
/// <para>The decision is <see cref="DarlingWorker.RunsDetached"/>, which the worker's dispatch also asks, so the
/// two cannot disagree about what runs in the pass. The source pins below are what keep it that way: a second,
/// hand-copied version of the test in either file is a drift the behavioural cases cannot see.</para>
/// </summary>
public sealed class CollectionHealthDetachedSweepPressureTests
{
    private const string Daily = "index_object_stats";
    private const string OneMinute = "wait_stats";

    /// <summary>One run costs more than the whole 60,000 ms body, so a collector counted in the body reads BODY_OVERRUN.</summary>
    private const double LongP95Ms = 90_000;

    private static CollectorHealth Row(string name, double avgMs, double p95Ms) =>
        new() { CollectorName = name, AvgDurationMs = avgMs, P95DurationMs = p95Ms };

    private static SweepPressure Rollup(params CollectorHealth[] rows) =>
        SweepPressureClassifier.Compute(DarlingMcpDataTools.SweepBodyCollectors(rows));

    [Fact]
    public void ADailyCollectorWithALongP95_IsNotChargedToTheSweepBody()
    {
        var rows = new[]
        {
            Row(Daily, avgMs: 40_000, p95Ms: LongP95Ms),
            Row(OneMinute, avgMs: 50, p95Ms: 100),
        };

        /* The fixture is a real overrun when the daily collector IS counted: this is what the tool said before. */
        var counted = SweepPressureClassifier.Compute(
            rows.Select(r => (r.CollectorName, r.AvgDurationMs, r.P95DurationMs, r.FrequencyMinutes)));
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, counted.PeakCycleRisk);
        Assert.Equal(Daily, counted.PeakCollectorName);

        var pressure = Rollup(rows);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Equal(OneMinute, pressure.PeakCollectorName);
    }

    [Fact]
    public void AOneMinuteCollectorWithTheSameP95_StillReadsBodyOverrun()
    {
        Assert.Equal(1, CollectorScheduleDefaults.All[OneMinute].FrequencyMinutes);

        var pressure = Rollup(
            Row(OneMinute, avgMs: 40_000, p95Ms: LongP95Ms),
            Row(Daily, avgMs: 50, p95Ms: 100));

        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, pressure.PeakCycleRisk);
        Assert.Equal(OneMinute, pressure.PeakCollectorName);
    }

    /// <summary>
    /// The three that were detached before the daily ones (by name, on a five-minute cadence) and one of each
    /// kind of daily collector: a catalog 1440-minute one, and an on-load one whose catalog 0 is its daily
    /// recapture. None of them runs in the pass, so none is charged to it.
    /// </summary>
    [Theory]
    [InlineData("query_store")]
    [InlineData("plan_correction")]
    [InlineData("pg_wait_sampling")]
    [InlineData("index_object_stats")]
    [InlineData("pg_index_bloat")]
    [InlineData("server_config")]
    public void ACollectorThatRunsDetached_IsLeftOutOfTheRollUp(string name)
    {
        Assert.True(CollectorScheduleDefaults.All.ContainsKey(name), $"'{name}' is not in the catalog");

        var pressure = Rollup(Row(name, avgMs: 40_000, p95Ms: LongP95Ms));

        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Null(pressure.PeakCollectorName);
        Assert.Equal(0, pressure.BusyMsPerMinute);
    }

    /// <summary>
    /// Over the whole catalog, from the catalog's own cadences rather than from the predicate under test: a row
    /// is left out of the roll-up exactly when its collector is detached by name or runs at a daily cadence or
    /// slower, and kept otherwise.
    /// </summary>
    [Fact]
    public void TheRollUpKeepsExactlyTheCollectorsThatRunInThePass()
    {
        foreach (var (name, schedule) in CollectorScheduleDefaults.All)
        {
            var byName = name is "query_store" or "plan_correction" or "pg_wait_sampling";
            var effective = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes);
            var expectedInPass = !byName && effective < 1440;

            var kept = DarlingMcpDataTools.SweepBodyRows(new[] { Row(name, 1, 1) }).Any();

            Assert.True(
                expectedInPass == kept,
                $"{name} (every {effective} min, detached by name: {byName}): in the pass = {expectedInPass}, kept in the roll-up = {kept}");
        }
    }
}

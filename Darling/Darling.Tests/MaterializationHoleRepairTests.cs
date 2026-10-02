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
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 (Q10, item 9's last clause): the materialization-hole repair —
/// <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> — pinned in its pure parts here and proven
/// on the outage shape itself in <see cref="MaterializationHoleRepairLiveTests"/>.
///
/// <para><b>The shape the pins hold.</b> The targets are every registered aggregate in dependency order with
/// the source, time column, width and CREATE the scan reads; the scan SQL is two index probes per bucket with
/// the aggregate's own filter on the source side; contiguous hole buckets fold into one refresh each; the cap is
/// one policy window per aggregate per start, oldest first, splitting a straddling range exactly; the horizon
/// is the source's own retention. Each is a function of the registry or of its arguments and nothing else, so
/// each is walked without a store.</para>
/// </summary>
public sealed class MaterializationHoleRepairTests
{
    private static readonly DateTime Hour = new(2026, 9, 18, 10, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void Targets_AreEveryRegisteredAggregate_InDependencyOrder_WithTheirOwnCreate()
    {
        var targets = TimescaleSupport.MaterializationHoleTargets;
        var registered = TimescaleSupport.HourlyAggregates.Concat(TimescaleSupport.DailyAggregates).Concat(TimescaleSupport.BaselineAggregates).ToArray();

        Assert.Equal(registered.Length, targets.Count);
        // #3653 LC: 26 (A6 lane LB's count) minus the six the freeze took out of HourlyAggregates/DailyAggregates
        // (they stay in TimescaleSupport.RollupViews and FrozenRollupAggregates, but nothing repairs them now).
        Assert.Equal(20, targets.Count);
        Assert.Equal(registered.Select(a => a.View).OrderBy(v => v, StringComparer.Ordinal), targets.Select(t => t.View).OrderBy(v => v, StringComparer.Ordinal));

        /* The rollups come first in the backfill's dependency order — every raw-sourced rollup before the
           rollups that read it — and the baselines follow; each carries its own CREATE. */
        Assert.Equal(RollupBackfill.Targets.Select(t => t.View), targets.Take(RollupBackfill.Targets.Length).Select(t => t.View));
        Assert.Equal(TimescaleSupport.BaselineAggregates.Select(a => a.View), targets.Skip(RollupBackfill.Targets.Length).Select(t => t.View));
        foreach (var target in targets)
        {
            Assert.Equal(registered.Single(a => a.View == target.View).CreateSql, target.CreateSql);
            Assert.Contains($"FROM collect.{target.Source}", target.CreateSql, StringComparison.Ordinal);
            Assert.True(target.BucketWidth == TimescaleSupport.HourlyBucket || target.BucketWidth == TimescaleSupport.DailyBucket);
            Assert.Equal(target.Source.EndsWith("_hourly", StringComparison.Ordinal) || target.Source.EndsWith("_daily", StringComparison.Ordinal) ? "bucket" : "collection_time", target.SourceTimeColumn);
        }

        /* A daily is scanned after the hourly it reads, so its scan sees the rows the hourly's repair wrote.
           #3653 LC: SupersededHourlyRollups' own Legacy/DependentDaily pair is frozen out of `targets` entirely
           now (there is nothing left to order), so this re-pins the live analog — SupersededDailyRollups' own
           SuccessorHourly/SuccessorDaily pair, derived rather than typed, the same two relations
           RollupBackfill.Targets still orders by depth. */
        foreach (var (_, successorDaily, successorHourly) in TimescaleSupport.SupersededDailyRollups)
        {
            Assert.True(
                targets.ToList().FindIndex(t => t.View == successorHourly) < targets.ToList().FindIndex(t => t.View == successorDaily),
                $"{successorDaily} must be scanned after {successorHourly}");
        }
    }

    /// <summary>#3653 LC: the repair walk must never see a frozen view — refreshing one is the one thing the
    /// freeze forbids (see <see cref="TimescaleSupport.FrozenRollupAggregates"/>'s remarks). A regression that
    /// re-adds one of the six to <see cref="RollupBackfill.Targets"/> (and so to
    /// <see cref="TimescaleSupport.MaterializationHoleTargets"/>) fails here rather than at a live refresh.</summary>
    [Fact]
    public void MaterializationHoleTargets_HoldsNoFrozenRollupAggregate()
    {
        Assert.DoesNotContain(
            TimescaleSupport.MaterializationHoleTargets,
            target => TimescaleSupport.IsFrozenRollupAggregate(target.View));
    }

    [Fact]
    public void TheCap_IsOnePolicyWindowInBuckets_PerGrain()
    {
        Assert.Equal(24, TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.HourlyBucket));
        Assert.Equal(3, TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.DailyBucket));
        Assert.Equal((int)(TimescaleSupport.HourlyRefreshStartSpan.Ticks / TimescaleSupport.HourlyBucket.Ticks), TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.HourlyBucket));
        Assert.Equal((int)(TimescaleSupport.DailyRefreshStartSpan.Ticks / TimescaleSupport.DailyBucket.Ticks), TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.DailyBucket));
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.MaterializationHoleRepairCapBuckets(TimeSpan.Zero));
    }

    [Fact]
    public void TheScanHorizon_IsTheSourcesOwnRetention_AndTheBaselineTierForCollectorPurgedSources()
    {
        Assert.Equal(TimescaleSupport.RawRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("query_stats"));
        Assert.Equal(TimescaleSupport.RawRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("procedure_stats"));
        Assert.Equal(TimescaleSupport.RawRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("query_store_stats"));
        Assert.Equal(TimescaleSupport.HourlyRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor(TimescaleSupport.QueryStatsHourlyView));
        Assert.Equal(TimescaleSupport.IntervalRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor(TimescaleSupport.QueryStoreStatsIntervalHourlyView));
        Assert.Equal(TimescaleSupport.IntervalDailyRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor(TimescaleSupport.QueryStoreStatsIntervalDailyView));
        Assert.Equal(TimescaleSupport.BaselineRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("wait_stats"));
        Assert.Equal(TimescaleSupport.BaselineRetentionSpan, TimescaleSupport.MaterializationHoleScanSpanFor("perfmon_stats"));

        /* Every target's source resolves — a new tier with an unknown horizon string throws by design. */
        foreach (var target in TimescaleSupport.MaterializationHoleTargets)
        {
            Assert.True(TimescaleSupport.MaterializationHoleScanSpanFor(target.Source) > TimeSpan.Zero);
        }
    }

    [Fact]
    public void TheSourceFilter_IsTheAggregatesOwnWhere_Verbatim_OrEmpty()
    {
        Assert.Equal(string.Empty, TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsHourlySql));
        Assert.Equal("sample_interval_seconds IS DISTINCT FROM 0", TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsIntervalHourlySql));
        Assert.Equal("delta_worker_time IS NOT NULL AND sample_interval_seconds IS DISTINCT FROM 0", TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsDbIntervalHourlySql));
        Assert.Equal("delta_worker_time IS NOT NULL", TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreateQueryStatsDbHourlySql));
        Assert.Equal(
            "counter_name = 'Batch Requests/sec' AND delta_cntr_value >= 0 AND sample_interval_seconds IS DISTINCT FROM 0",
            TimescaleSupport.MaterializationHoleSourceFilterFor(TimescaleSupport.CreatePerfmonIntervalBaselineSql));
        Assert.Throws<ArgumentNullException>(() => TimescaleSupport.MaterializationHoleSourceFilterFor(null!));

        /* And the scan SQL carries it on the SOURCE probe only, with both probes present and the width bound
           once as $3. The candidate buckets are the fenced subquery c (#3933, MaterializationHoleScanShapeSqlTests
           pins the fences), so the source probe reads c.bucket. */
        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == TimescaleSupport.QueryStatsIntervalHourlyView);
        var sql = TimescaleSupport.MaterializationHoleScanSql(target, ("_timescaledb_internal", "_materialized_hypertable_42"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("generate_series($1::timestamp, $2::timestamp, $3::interval)", sql, StringComparison.Ordinal);
        Assert.Contains("NOT EXISTS (SELECT 1 FROM \"_timescaledb_internal\".\"_materialized_hypertable_42\" AS m WHERE m.bucket = b.bucket OFFSET 0)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_stats AS s", sql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time >= c.bucket", sql, StringComparison.Ordinal);
        Assert.Contains("s.collection_time < c.bucket + $3::interval", sql, StringComparison.Ordinal);
        Assert.Contains("AND   sample_interval_seconds IS DISTINCT FROM 0\n    OFFSET 0)", sql, StringComparison.Ordinal);
        Assert.EndsWith("ORDER BY c.bucket", sql.TrimEnd(), StringComparison.Ordinal);

        /* #3653 LC froze query_stats_daily out of MaterializationHoleTargets; its unfiltered, hierarchical shape
           lives on in its live successor daily (SuccessorDailyOf, derived rather than typed), which is
           hierarchical from query_stats_interval_hourly with no WHERE of its own either. */
        var successorDaily = TimescaleSupport.SuccessorDailyOf(TimescaleSupport.QueryStatsIntervalHourlyView)
            ?? throw new InvalidOperationException(
                $"{TimescaleSupport.QueryStatsIntervalHourlyView} must be in {nameof(TimescaleSupport.SupersededDailyRollups)}.");
        var unfiltered = TimescaleSupport.MaterializationHoleScanSql(
            TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == successorDaily), ("s", "m"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_stats_interval_hourly AS s", unfiltered, StringComparison.Ordinal);
        Assert.Contains("s.bucket >= c.bucket", unfiltered, StringComparison.Ordinal);
        /* No filter: the source probe's fence follows straight after the width bound. */
        Assert.Contains("s.bucket < c.bucket + $3::interval\n    OFFSET 0)", unfiltered, StringComparison.Ordinal);
    }

    [Fact]
    public void ContiguousBuckets_FoldIntoOneRefresh_AndACoveredBucketBetweenTwoHolesKeepsThemApart()
    {
        var width = TimescaleSupport.HourlyBucket;

        Assert.Empty(TimescaleSupport.MergeContiguousBuckets(Array.Empty<DateTime>(), width));

        var one = TimescaleSupport.MergeContiguousBuckets(new[] { Hour }, width);
        Assert.Equal(new[] { (Hour, Hour.AddHours(1)) }, one);

        /* Unordered input, two runs: 10-12 (three buckets) and 14 alone; 13 is covered and stays covered. */
        var two = TimescaleSupport.MergeContiguousBuckets(new[] { Hour.AddHours(2), Hour, Hour.AddHours(4), Hour.AddHours(1) }, width);
        Assert.Equal(new[] { (Hour, Hour.AddHours(3)), (Hour.AddHours(4), Hour.AddHours(5)) }, two);

        /* Daily width folds days. */
        var days = TimescaleSupport.MergeContiguousBuckets(new[] { Hour.Date, Hour.Date.AddDays(1) }, TimescaleSupport.DailyBucket);
        Assert.Equal(new[] { (Hour.Date, Hour.Date.AddDays(2)) }, days);

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.MergeContiguousBuckets(new[] { Hour }, TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => TimescaleSupport.MergeContiguousBuckets(null!, width));
    }

    [Fact]
    public void TheCap_TakesTheOldestFirst_SplitsAStraddlingRangeExactly_AndDefersTheRest()
    {
        var width = TimescaleSupport.HourlyBucket;
        var ranges = new List<(DateTime Start, DateTime End)>
        {
            (Hour.AddHours(30), Hour.AddHours(40)),   /* newest, 10 buckets */
            (Hour, Hour.AddHours(20)),                /* oldest, 20 buckets */
            (Hour.AddHours(22), Hour.AddHours(28)),   /* middle, 6 buckets */
        };

        var (repair, deferred) = TimescaleSupport.CapMaterializationHoleRepairs(ranges, 24, width);

        /* Oldest 20 whole, then 4 of the middle's 6 — split at the cap — the other 2 and the newest deferred. */
        Assert.Equal(new[] { (Hour, Hour.AddHours(20)), (Hour.AddHours(22), Hour.AddHours(26)) }, repair);
        Assert.Equal(new[] { (Hour.AddHours(26), Hour.AddHours(28)), (Hour.AddHours(30), Hour.AddHours(40)) }, deferred);
        Assert.Equal(24, repair.Sum(r => (int)((r.End - r.Start).Ticks / width.Ticks)));

        /* Under the cap: everything repaired, nothing deferred. Exactly at it: the same. */
        var small = new List<(DateTime Start, DateTime End)> { (Hour, Hour.AddHours(2)) };
        Assert.Equal(small, TimescaleSupport.CapMaterializationHoleRepairs(small, 24, width).Repair);
        Assert.Empty(TimescaleSupport.CapMaterializationHoleRepairs(small, 24, width).Deferred);
        Assert.Empty(TimescaleSupport.CapMaterializationHoleRepairs(new List<(DateTime, DateTime)> { (Hour, Hour.AddHours(24)) }, 24, width).Deferred);

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.CapMaterializationHoleRepairs(small, 0, width));
        Assert.Throws<ArgumentNullException>(() => TimescaleSupport.CapMaterializationHoleRepairs(null!, 24, width));
    }

    /// <summary>
    /// #4186 round-3 H1: the seam window's own direction. Mirrors
    /// <see cref="TheCap_TakesTheOldestFirst_SplitsAStraddlingRangeExactly_AndDefersTheRest"/> exactly, with
    /// <c>newestFirst: true</c> — same three ranges, same cap, but the NEWEST 24 buckets are kept and a
    /// straddling range splits at its OLDER edge instead of its newer one, so the kept portion stays adjacent
    /// to whatever sits above it (the successor's already-materialized span in the real caller).
    /// </summary>
    [Fact]
    public void TheCap_NewestFirst_TakesTheNewestFirst_SplitsAStraddlingRangeAtItsOlderEdge_AndDefersTheRest()
    {
        var width = TimescaleSupport.HourlyBucket;
        var ranges = new List<(DateTime Start, DateTime End)>
        {
            (Hour.AddHours(30), Hour.AddHours(40)),   /* newest, 10 buckets */
            (Hour, Hour.AddHours(20)),                /* oldest, 20 buckets */
            (Hour.AddHours(22), Hour.AddHours(28)),   /* middle, 6 buckets */
        };

        var (repair, deferred) = TimescaleSupport.CapMaterializationHoleRepairs(ranges, 24, width, newestFirst: true);

        /* Newest 10 whole, then the middle's 6 whole (16 spent), then 8 of the oldest's 20 — split at its NEWER
           edge, since newestFirst keeps the portion adjacent to what is already above it (24 = 10 + 6 + 8) —
           the older remaining 12 hours of the oldest range deferred. */
        Assert.Equal(new[] { (Hour.AddHours(30), Hour.AddHours(40)), (Hour.AddHours(22), Hour.AddHours(28)), (Hour.AddHours(12), Hour.AddHours(20)) }, repair);
        Assert.Equal(new[] { (Hour, Hour.AddHours(12)) }, deferred);
        Assert.Equal(24, repair.Sum(r => (int)((r.End - r.Start).Ticks / width.Ticks)));

        /* Under the cap and exactly at it: everything repaired, nothing deferred, same as oldest-first. */
        var small = new List<(DateTime Start, DateTime End)> { (Hour, Hour.AddHours(2)) };
        Assert.Equal(small, TimescaleSupport.CapMaterializationHoleRepairs(small, 24, width, newestFirst: true).Repair);
        Assert.Empty(TimescaleSupport.CapMaterializationHoleRepairs(small, 24, width, newestFirst: true).Deferred);
    }

    [Fact]
    public void AlignDown_LandsOnABucketBoundary_ForBothWidths()
    {
        var instant = new DateTime(2026, 9, 18, 10, 47, 13, DateTimeKind.Unspecified);
        Assert.Equal(Hour, TimescaleSupport.AlignDown(instant, TimescaleSupport.HourlyBucket));
        Assert.Equal(Hour.Date, TimescaleSupport.AlignDown(instant, TimescaleSupport.DailyBucket));
        Assert.Equal(Hour, TimescaleSupport.AlignDown(Hour, TimescaleSupport.HourlyBucket));
        Assert.Equal(DateTimeKind.Unspecified, TimescaleSupport.AlignDown(instant, TimescaleSupport.HourlyBucket).Kind);
        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.AlignDown(instant, TimeSpan.Zero));
    }

    [Fact]
    public void IsRawSourced_IsTrueOnlyForTheThreeRawTierCoverageRelations()
    {
        foreach (var (relation, _, _) in TimescaleSupport.RawTierCoverage)
        {
            Assert.True(TimescaleSupport.IsRawSourced(relation));
        }

        Assert.False(TimescaleSupport.IsRawSourced("query_store_stats_hourly"));
        Assert.False(TimescaleSupport.IsRawSourced("not_a_real_relation"));
    }

    /// <summary>
    /// #4299: a range this same pass's cap deferred inside the drop window counts as a hole for the
    /// trigger's gate, even though a fresh scan alone (with no deferred rows fed in) would not have found it —
    /// the gate's whole point is that "the repair finished" is not the same claim as "nothing is left", and a
    /// deferred range is exactly the gap between those two claims. Pure inputs: this exercises
    /// <see cref="TimescaleSupport.HoleFreeThroughAsync"/>'s overlap check directly by asserting the SAME
    /// half-open-interval logic the gate must apply — a range fully outside [dropFrom, dropTo) does not block,
    /// one that overlaps it at all does, matching the seam/ordinary deferred lists the repair pass returns.
    /// </summary>
    [Fact]
    public void DeferredRangeOverlappingDropWindow_WouldBlockTheGate_ByHalfOpenIntervalOverlap()
    {
        var dropFrom = Hour;
        var dropTo = Hour.AddHours(24);

        bool Overlaps(DateTime start, DateTime end) => start < dropTo && end > dropFrom;

        /* Deferred range fully inside the drop window: blocks. */
        Assert.True(Overlaps(Hour.AddHours(5), Hour.AddHours(6)));
        /* Deferred range straddling the drop window's edge: blocks. */
        Assert.True(Overlaps(Hour.AddHours(-2), Hour.AddHours(1)));
        /* Deferred range strictly before the drop window: does not block. */
        Assert.False(Overlaps(Hour.AddHours(-10), Hour));
        /* Deferred range strictly after the drop window: does not block. */
        Assert.False(Overlaps(dropTo, dropTo.AddHours(5)));
    }

    [Fact]
    public void ScanWindows_NoSeam_GivesTheOrdinaryWindowOnly()
    {
        var width = TimescaleSupport.HourlyBucket;

        /* floor newer than the horizon: the ordinary window starts at floor. */
        var floor = Hour.AddHours(50);
        var horizon = Hour;
        var ceiling = Hour.AddHours(80);
        Assert.Equal(
            new[] { (floor, ceiling) },
            TimescaleSupport.MaterializationHoleScanWindows(floor, ceiling, horizon, seamFloor: floor, width));

        /* floor older than the horizon (the ordinary, pre-#4186 clamp): the window starts at the horizon. */
        var oldFloor = Hour;
        var laterHorizon = Hour.AddHours(20);
        Assert.Equal(
            new[] { (laterHorizon, ceiling) },
            TimescaleSupport.MaterializationHoleScanWindows(oldFloor, ceiling, laterHorizon, seamFloor: oldFloor, width));
    }

    [Fact]
    public void ScanWindows_SeamAboveTheHorizon_GivesTwoWindows_SeamThenOrdinary()
    {
        var width = TimescaleSupport.HourlyBucket;
        var floor = Hour.AddHours(10);
        var horizon = Hour.AddHours(2);
        var seamFloor = Hour.AddHours(4);
        var ceiling = Hour.AddHours(50);

        var windows = TimescaleSupport.MaterializationHoleScanWindows(floor, ceiling, horizon, seamFloor, width);

        Assert.Equal(new[] { (seamFloor, floor.AddHours(-1)), (floor, ceiling) }, windows);
        Assert.True(windows[0].From >= horizon, "the seam sits above the horizon in this case — a sanity check, not the regression this pins");
    }

    [Fact]
    public void ScanWindows_SeamBelowTheHorizon_GivesTwoWindows_TheSeamUnclamped()
    {
        /* #4186 follow-up: a store stopped more than the raw span (4 days = 96h here) before its successor's
           first start. The seam predates the horizon entirely — the exact shape the clamp used to swallow. */
        var width = TimescaleSupport.HourlyBucket;
        var horizon = Hour.AddHours(96);
        var floor = Hour.AddHours(150);
        var seamFloor = Hour.AddHours(10);
        var ceiling = Hour.AddHours(200);

        var windows = TimescaleSupport.MaterializationHoleScanWindows(floor, ceiling, horizon, seamFloor, width);

        Assert.Equal(new[] { (seamFloor, floor.AddHours(-1)), (floor, ceiling) }, windows);

        /* The regression itself: the seam window's From reaches below the horizon rather than clamping to it —
           the pre-fix formula (from = max(seamFloor, horizon)) would have reported Hour.AddHours(96) here. */
        Assert.True(windows[0].From < horizon);
        Assert.Equal(seamFloor, windows[0].From);
    }

    [Fact]
    public void ScanWindows_TheOrdinaryWindow_NeverStartsBelowTheHorizon()
    {
        var width = TimescaleSupport.HourlyBucket;
        var ceiling = Hour.AddHours(500);

        foreach (var (floor, horizon, seamFloor) in new[]
        {
            (Hour.AddHours(50), Hour, Hour.AddHours(50)),               /* no seam, floor above horizon */
            (Hour, Hour.AddHours(20), Hour),                            /* no seam, floor below horizon */
            (Hour.AddHours(10), Hour.AddHours(2), Hour.AddHours(4)),    /* seam above horizon */
            (Hour.AddHours(150), Hour.AddHours(96), Hour.AddHours(10)), /* seam below horizon */
        })
        {
            var windows = TimescaleSupport.MaterializationHoleScanWindows(floor, ceiling, horizon, seamFloor, width);
            var ordinary = windows[^1];
            Assert.True(ordinary.From >= horizon);
            Assert.Equal(ordinary.From, floor > horizon ? floor : horizon);
        }
    }

    [Fact]
    public void ScanWindows_EdgeCases_OneBucketSeam_NoWindowsWhenNothingToScan_AndTheWidthGuard()
    {
        var width = TimescaleSupport.HourlyBucket;

        /* A seam exactly one bucket wide still yields a valid, single-bucket window. */
        var floor = Hour.AddHours(5);
        var oneBucketSeam = floor.AddHours(-1);
        var windows = TimescaleSupport.MaterializationHoleScanWindows(floor, Hour.AddHours(50), Hour, oneBucketSeam, width);
        Assert.Equal(new[] { (oneBucketSeam, oneBucketSeam), (floor, Hour.AddHours(50)) }, windows);

        /* No seam and the whole span is older than the horizon: nothing to scan. */
        Assert.Empty(TimescaleSupport.MaterializationHoleScanWindows(Hour, Hour.AddHours(5), Hour.AddHours(20), seamFloor: Hour, width));

        Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.MaterializationHoleScanWindows(Hour, Hour, Hour, Hour, TimeSpan.Zero));
    }

    /// <summary>
    /// #4301 (H2): a legacy-interior gap — a hole BELOW the legacy's last bucket, inside its own frozen span,
    /// from an outage that predates this store ever taking the freeze — gets a THIRD window, distinct from
    /// both the seam window and the ordinary window, emitted FIRST (oldest of the three). Superseded by the
    /// #4301 ruling ("fill the successor CONTIGUOUSLY DOWNWARD"): the walk no longer takes a separate
    /// legacy-interior window at all — the seam window's own lower bound moved to raw's filtered floor, so a
    /// hole below the legacy's last bucket is repaired by the SAME newest-first descent as everything else
    /// below the successor's floor. <c>MaterializationHoleScanWindows</c> dropped the
    /// <c>legacyInteriorFrom</c>/<c>legacyInteriorTo</c> parameters this test exercised; removed with them.
    /// </summary>

    /// <summary>
    /// #4301 (H2, filter parity): <see cref="TimescaleSupport.LegacySuccessorHoleExistsSql"/> (the gate's
    /// <c>EXISTS</c> wrapper) is the shared hole definition <see cref="TimescaleSupport.RetentionArmSafetySql"/>
    /// uses. This pins its shape: a well-formed <c>EXISTS(...)</c> wrapping the <c>generate_series(...)</c>
    /// buckets clause, fenced with <c>OFFSET 0</c> for the #3933 reason its own doc states.
    /// </summary>
    [Fact]
    public void LegacySuccessorHoleExistsSql_WrapsTheBucketsClauseInExists()
    {
        const string relation = "query_stats";
        const string sourceTimeColumn = "collection_time";
        const string sourceFilter = "sample_interval_seconds IS DISTINCT FROM 0";
        const string legacy = "query_stats_hourly";
        const string successor = "query_stats_interval_hourly";
        const string fromExpr = "$1::timestamp";
        const string toExpr = "$2::timestamp";
        const string bucketWidthLiteral = "$3::interval";

        var existsSql = TimescaleSupport.LegacySuccessorHoleExistsSql(
            relation, sourceTimeColumn, sourceFilter, legacy, successor, fromExpr, toExpr, bucketWidthLiteral);

        Assert.StartsWith("EXISTS (", existsSql, StringComparison.Ordinal);
        Assert.EndsWith(")", existsSql, StringComparison.Ordinal);
        Assert.Contains("generate_series(", existsSql, StringComparison.Ordinal);
        Assert.Contains("OFFSET 0", existsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The start path: launched (not awaited) right after the ensure, on its own connection, inside the
    /// TimescaleDB block, before the compression and retention ensures; drained at shutdown beside the baseline
    /// backfill. Source-order pins, the RetiredBaselineAggregateTests shape.
    /// </summary>
    [Fact]
    public void Worker_LaunchesTheRepair_AfterTheEnsure_BeforeTheRetentionReArm_AndDrainsIt()
    {
        var worker = ReadWorkerSource();

        /* #3817 re-anchored the two ensures either side of this launch: they now live in the shared
           convergence list both cadences walk, so the start path names SEGMENTS here instead of calls. The
           property is the same one and is the reason the list is walked in two slices at all — the repair is
           launched between the segment that ends with the aggregate ensure and the segment that begins with
           the dedup/compression pair, which is exactly what "after the ensure, before compression" meant. The
           retention ensure did NOT move (it has its own hourly tenant), so that anchor is unchanged. */
        /* The SEGMENT CALLS, not the stage names — the enum members and the list's per-step tags spell those
           tokens far earlier in the file, so a bare-name anchor would measure the declaration. */
        var ensureAt = worker.IndexOf("StoreObjectConvergenceStage.Timescale, startupConvergence, stoppingToken);", StringComparison.Ordinal);
        var launchAt = worker.IndexOf("holeRepair = RunMaterializationHoleRepairAsync(postgres, stoppingToken);", StringComparison.Ordinal);
        var compressionAt = worker.IndexOf("StoreObjectConvergenceStage.TimescaleAfterRepairLaunch, startupConvergence, stoppingToken);", StringComparison.Ordinal);
        var retentionAt = worker.IndexOf("TimescaleSupport.EnsureRetentionPoliciesAsync(", StringComparison.Ordinal);
        var plainModeAt = worker.IndexOf("continuing in plain-PostgreSQL mode", StringComparison.Ordinal);
        var drainAt = worker.IndexOf("await holeRepair;", StringComparison.Ordinal);
        var stoppedAt = worker.IndexOf("collection loop stopped", StringComparison.Ordinal);

        Assert.True(ensureAt > 0 && launchAt > 0 && compressionAt > 0 && retentionAt > 0 && plainModeAt > 0 && drainAt > 0 && stoppedAt > 0);
        Assert.True(ensureAt < launchAt, "the repair must be launched after the segment whose last step is the ensure that creates every aggregate");
        Assert.True(launchAt < compressionAt && launchAt < retentionAt, "the repair is launched before the compression and retention ensures");
        Assert.True(launchAt < plainModeAt, "the repair is launched inside the TimescaleDB block");
        Assert.True(drainAt < stoppedAt, "the repair is drained before the loop reports stopped");

        /* Launched, not awaited; its own connection; the pass itself is called once, in the runner. */
        Assert.DoesNotContain("await RunMaterializationHoleRepairAsync(", worker, StringComparison.Ordinal);
        Assert.Contains("await TimescaleSupport.RepairMaterializationHolesAsync(connection, _logger, DateTime.UtcNow, stoppingToken);", worker, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(worker, "TimescaleSupport.RepairMaterializationHolesAsync("));
    }

    /// <summary>
    /// #3756: the ONE summary line per start is the worker's and it is UNCONDITIONAL. The lie this pins
    /// against: the pass's first cut wrote a summary only when it had repaired, deferred or failed something
    /// (Debug otherwise), so on the first store to carry it a zero-hole start, a start whose scan threw before
    /// its first probe and a start that never reached the scan all left the same absence. Source-order pins,
    /// the shape of the launch pin above: the runner takes the returned tally, and the very next statement is
    /// the INFORMATION line — no <c>if</c> between them — naming every count the tally carries; the
    /// cancellation arm writes its own, different line and rethrows; and the pass writes NO summary of its own
    /// any more, so a start cannot get two. The tally the line reports is pinned live in
    /// <see cref="MaterializationHoleRepairLiveTests"/> on a zero-hole pass and on a straddled one.
    /// </summary>
    [Fact]
    public void Worker_WritesTheSummaryLine_Unconditionally_AndThePassWritesNoneOfItsOwn()
    {
        var worker = ReadWorkerSource();

        var runnerAt = worker.IndexOf("private async Task RunMaterializationHoleRepairAsync(", StringComparison.Ordinal);
        Assert.True(runnerAt > 0, "the runner must still exist under its name");
        var runner = worker.Substring(runnerAt, worker.IndexOf("private void ReconcileSweepGate(", runnerAt, StringComparison.Ordinal) - runnerAt);

        var callAt = runner.IndexOf("var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, _logger, DateTime.UtcNow, stoppingToken);", StringComparison.Ordinal);
        var lineAt = runner.IndexOf("_logger.LogInformation(", StringComparison.Ordinal);
        var templateAt = runner.IndexOf("\"TimescaleDB: materialization hole scan (#3653 Q10) — {Scanned} aggregate(s) walked", StringComparison.Ordinal);
        Assert.True(callAt > 0, "the runner must take the returned tally, not discard it");
        Assert.True(lineAt > callAt, "the summary line follows the call");
        Assert.True(templateAt > lineAt, "the first INFORMATION line after the call is the summary");

        /* UNCONDITIONAL: nothing between the call and the line decides whether to write it. A bare `if (`
           anywhere in that span is the regression this pin exists for. */
        var between = runner.Substring(callAt, lineAt - callAt);
        Assert.DoesNotContain("if (", between, StringComparison.Ordinal);
        Assert.DoesNotContain("return;", between, StringComparison.Ordinal);

        /* Every count the tally carries is named, and the elapsed is the pass's own. */
        var template = runner.Substring(templateAt, runner.IndexOf("\",", templateAt, StringComparison.Ordinal) - templateAt);
        foreach (var placeholder in new[] { "{Scanned}", "{Skipped}", "{HolesFound}", "{BucketsFound}", "{HolesRepaired}", "{BucketsRepaired}", "{Forced}", "{HolesDeferred}", "{BucketsDeferred}", "{Remaining}", "{Failures}", "{ElapsedMs}" })
        {
            Assert.Contains(placeholder, template, StringComparison.Ordinal);
        }

        Assert.Contains("deferred past the cap", template, StringComparison.Ordinal);
        Assert.Contains("isolated failure(s)", template, StringComparison.Ordinal);
        Assert.Contains("summary.Elapsed.TotalMilliseconds", runner, StringComparison.Ordinal);

        /* The third absence: a scan cut short by shutdown writes its own, DIFFERENT line and rethrows so the
           drain still sees the cancellation it was written for. */
        var cancelAt = runner.IndexOf("catch (OperationCanceledException)", StringComparison.Ordinal);
        Assert.True(cancelAt > lineAt, "the cancellation arm follows the summary line");
        var cancelArm = runner.Substring(cancelAt, runner.IndexOf("catch (Exception ex)", cancelAt, StringComparison.Ordinal) - cancelAt);
        Assert.Contains("was cancelled before it could report", cancelArm, StringComparison.Ordinal);
        Assert.Contains("throw;", cancelArm, StringComparison.Ordinal);

        /* And the connection-open / outside-isolation failure keeps its WARNING, so all three outcomes of a
           launched scan differ from one another and from the absence a never-launched scan leaves. */
        Assert.Contains("Materialization-hole repair could not run", runner, StringComparison.Ordinal);

        /* The pass itself no longer writes a summary: one owner, one line per start. The per-hole, per-deferral
           and per-failure lines stay — they are the detail the summary counts. */
        var storage = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "TimescaleSupport.MaterializationHoles.cs");
        Assert.DoesNotContain("aggregate(s) scanned", storage, StringComparison.Ordinal);
        Assert.DoesNotContain("no holes.", storage, StringComparison.Ordinal);
        Assert.Contains("passClock.Elapsed", storage, StringComparison.Ordinal);
        /* #4186 round-3 H1: found is now the seam and ordinary windows' ranges/buckets summed, since the two
           are scanned and capped separately (the seam newest-first, the ordinary oldest-first) rather than
           merged into one list before counting. */
        Assert.Contains("holesFound += seamRanges.Count + ordinaryRanges.Count;", storage, StringComparison.Ordinal);
        Assert.Contains("bucketsFound += seamHoles.Count + ordinaryHoles.Count;", storage, StringComparison.Ordinal);
        Assert.Contains("had {Buckets} bucket(s) in [{Start}, {End})", storage, StringComparison.Ordinal);
        Assert.Contains("left for a later run", storage, StringComparison.Ordinal);
        Assert.Contains("could not scan or repair {View} this run", storage, StringComparison.Ordinal);
    }

    /// <summary>The tally's shape is the line's shape: every count the worker names is a member, and the
    /// straddle arithmetic the record's summary states holds on a hand-built instance — so a reader of the
    /// line can be told, from a pin rather than prose, how found relates to repaired and deferred.</summary>
    [Fact]
    public void TheTally_CarriesEveryCountTheLineNames_AndStatesTheStraddleArithmetic()
    {
        /* One 26-bucket hole under a 24-bucket cap: found once, repaired 24, deferred 2 — the buckets add up,
           the ranges do not (the split counts on both sides). */
        var straddled = new TimescaleSupport.MaterializationHoleRepairSummary(
            AggregatesScanned: 5, AggregatesSkipped: 18, HolesFound: 1, BucketsFound: 26, HolesRepaired: 1, BucketsRepaired: 24,
            HolesDeferred: 1, BucketsDeferred: 2, HolesRemaining: 0, Failures: 0, HolesForced: 0, Elapsed: TimeSpan.FromMilliseconds(1234));

        Assert.Equal(straddled.BucketsFound, straddled.BucketsRepaired + straddled.BucketsDeferred);
        Assert.Equal(straddled.HolesFound + 1, straddled.HolesRepaired + straddled.HolesDeferred);
        Assert.Equal(1234, (long)straddled.Elapsed.TotalMilliseconds);

        /* Zero-hole: every count zero except the walk itself, and the elapsed still carries the cost of proving
           it — the shape the worker's line reports on the start the issue was filed about. */
        var zero = straddled with { HolesFound = 0, BucketsFound = 0, HolesRepaired = 0, BucketsRepaired = 0, HolesDeferred = 0, BucketsDeferred = 0 };
        Assert.Equal(0, zero.BucketsFound);
        Assert.Equal(zero.BucketsFound, zero.BucketsRepaired + zero.BucketsDeferred);
        Assert.Equal(5, zero.AggregatesScanned);
        Assert.True(zero.Elapsed > TimeSpan.Zero);
    }

    /// <summary>
    /// #4300, pure pin for <see cref="TimescaleSupport.ChainedDailyRange"/>: which part, if any, of a
    /// just-repaired successor-hourly seam range the dependent successor daily should be chased over.
    /// </summary>
    [Fact]
    public void ChainedDailyRange_ChasesOnlyTheDaysOlderThanTheDailysOwnWindow_AlignedOutAndCapped()
    {
        var windowStart = new DateTime(2026, 9, 20, 0, 0, 0, DateTimeKind.Unspecified);

        /* A repaired range entirely OLDER than the daily's own window: the aligned whole-day range, in full,
           uncapped. */
        var older = TimescaleSupport.ChainedDailyRange(
            new DateTime(2026, 9, 10, 3, 0, 0, DateTimeKind.Unspecified),
            new DateTime(2026, 9, 15, 7, 0, 0, DateTimeKind.Unspecified),
            windowStart, capDays: 30);
        Assert.NotNull(older);
        Assert.Equal(new DateTime(2026, 9, 10, 0, 0, 0, DateTimeKind.Unspecified), older!.Value.Start);
        Assert.Equal(new DateTime(2026, 9, 16, 0, 0, 0, DateTimeKind.Unspecified), older.Value.End);

        /* A repaired range entirely INSIDE the daily's own window: nothing to chase — the daily policy was
           always going to reach these days on its own ordinary schedule. */
        var inside = TimescaleSupport.ChainedDailyRange(
            new DateTime(2026, 9, 21, 1, 0, 0, DateTimeKind.Unspecified),
            new DateTime(2026, 9, 22, 5, 0, 0, DateTimeKind.Unspecified),
            windowStart, capDays: 30);
        Assert.Null(inside);

        /* A repaired range STRADDLING the boundary: only the OLDER part, aligned out to whole days. */
        var straddling = TimescaleSupport.ChainedDailyRange(
            new DateTime(2026, 9, 18, 12, 0, 0, DateTimeKind.Unspecified),
            new DateTime(2026, 9, 21, 6, 0, 0, DateTimeKind.Unspecified),
            windowStart, capDays: 30);
        Assert.NotNull(straddling);
        Assert.Equal(new DateTime(2026, 9, 18, 0, 0, 0, DateTimeKind.Unspecified), straddling!.Value.Start);
        Assert.Equal(windowStart, straddling.Value.End);

        /* The cap truncates to the NEWEST N days of the clipped span — fill contiguously downward, the same
           direction as the seam walk itself — leaving the OLDER remainder for a later pass. */
        var wide = TimescaleSupport.ChainedDailyRange(
            new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified),
            new DateTime(2026, 9, 19, 0, 0, 0, DateTimeKind.Unspecified),
            windowStart, capDays: 3);
        Assert.NotNull(wide);
        Assert.Equal(new DateTime(2026, 9, 16, 0, 0, 0, DateTimeKind.Unspecified), wide!.Value.Start);
        Assert.Equal(new DateTime(2026, 9, 19, 0, 0, 0, DateTimeKind.Unspecified), wide.Value.End);

        /* The catch-up caller passes (hourlyFloor, dailyFloor): when the daily already reaches back at least
           as far as the hourly floor, there is nothing older left to chase — the common case where the daily
           is already caught up and the retry should be a no-op, not a whole-window re-chase. */
        var alreadyCaughtUp = TimescaleSupport.ChainedDailyRange(
            new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified),
            new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified),
            windowStart, capDays: 30);
        Assert.Null(alreadyCaughtUp);

        var dailyAlreadyOlder = TimescaleSupport.ChainedDailyRange(
            new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified),
            new DateTime(2026, 9, 10, 0, 0, 0, DateTimeKind.Unspecified),
            windowStart, capDays: 30);
        Assert.Null(dailyAlreadyOlder);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>The worker's source, raw — every anchor here sits on one line. Through <see cref="RepoFile"/>
    /// rather than a private root walk (#3756 retired this file's own; RepoFileAdoptionTests says why one
    /// authority).</summary>
    private static string ReadWorkerSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
}

/// <summary>
/// The outage shape, planted for real on TimescaleDB and repaired with ONE targeted refresh (#3653, Q10).
///
/// <para><b>The sequence is the fleet's, step for step.</b> Collections land; a policy refresh materializes up
/// to its window's end; two more hours of collections land ABOVE the invalidation threshold that refresh set
/// (so they are never logged as invalidations — the mechanism, not a simulation of it); the service goes down
/// for hours; it comes back, collections resume, and the first refresh after resume covers only its own
/// trailing window. The aggregate then holds the pre-outage buckets and the post-outage ones and NOTHING for
/// the two-hour tail, while raw holds the tail's rows — measured here as the defect before it is repaired.
/// The repair finds exactly that tail, refreshes exactly its bounds, and the tail reads. The outage hours
/// themselves, which have no rows, are never treated as holes; the buckets below the aggregate's floor are
/// never touched. A second pass finds nothing.</para>
///
/// <para><b>Into a COMPRESSED chunk.</b> The materialization's chunk holding the hole is compressed before the
/// repair, because on a store the hole is typically past <c>compress_after</c> by the time a restart finds it,
/// and a refresh that could not write into a compressed materialization would make the repair a no-op exactly
/// where it is needed.</para>
///
/// <para><b>Both refresh paths, each on the shape that needs it.</b> The outage tail closes on the PLAIN refresh
/// (2.28.1 records the region a refresh skipped past as invalid when it advances the threshold — measured here,
/// after the first cut of the pass assumed the opposite and went straight to <c>force</c>). A hole with no
/// invalidation behind it — rows deleted from the materialization hypertable directly, the shape a refresh cut
/// short after consuming its entries leaves — does NOT close on the plain refresh and DOES on the forced one,
/// and the pass says which path it took.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class MaterializationHoleRepairLiveTests
{
    private const int ServerId = -936537;
    private const string ServerName = "hole-repair-e2e";

    [Fact]
    public async Task PreOutageTail_ReadsEmptyAfterResume_AndOneTargetedForcedRefreshClosesIt_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live materialization-hole repair test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live hole-repair test needs TimescaleDB: a materialization hole exists only in a materialization.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* #3653 LC: query_stats_hourly itself is now frozen — the repair walk never touches it, so it cannot
           carry this proof any more. query_stats_interval_hourly is its live successor, raw-sourced from the
           same collect.query_stats this test's own inserts target, so every plain (non-restart) row below is
           admitted the same way the legacy used to admit it. */
        var view = TimescaleSupport.QueryStatsIntervalHourlyView;
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);

        /* Twelve hours, all inside the raw horizon: H0-H2 collected and refreshed before the outage, H3-H4 the
           pre-outage TAIL (collected, never refreshed), H5-H8 the outage (nothing collected), H9-H10 collected
           after resume and refreshed by the first post-resume window. H11 is the still-filling current hour. */
        var h0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-1), DateTimeKind.Unspecified);
        DateTime H(int n) => h0.AddHours(n);

        await InsertHoursAsync(connection, new[] { 0, 1, 2 }, H, ct);
        await RefreshAsync(connection, view, H(0), H(3), ct);

        /* The tail: inserted ABOVE the threshold the refresh just set, so TimescaleDB does not log it. */
        await InsertHoursAsync(connection, new[] { 3, 4 }, H, ct);

        /* The outage: H5-H8 empty. Resume: H9, H10 collected; the first refresh after resume covers only its
           own trailing window. */
        await InsertHoursAsync(connection, new[] { 9, 10 }, H, ct);
        await RefreshAsync(connection, view, H(9), H(11), ct);

        /* THE DEFECT, MEASURED: raw holds H3 and H4, the aggregate holds nothing for them, and the aggregate's
           floor (H0) says the window is covered. */
        Assert.Equal(new[] { H(0), H(1), H(2), H(9), H(10) }, await MaterializedBucketsAsync(connection, view, ct));
        Assert.Equal(2, await RawHoursAsync(connection, H(3), H(5), ct));
        Assert.Equal(0, await RawHoursAsync(connection, H(5), H(9), ct));

        /* The scan alone, before the repair: exactly the tail — not the outage hours, not the live edge. */
        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == view);
        Assert.Equal(new[] { H(3), H(4) }, await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));

        /* Into a compressed chunk: compression enabled on every materialization the product's own way (#3581's
           ensure), then the chunk(s) holding the tail compressed by hand — the nightly policy would do it two
           days on, which is exactly when a restart typically finds the hole. */
        await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);
        await using (var compress = new NpgsqlCommand(
            $"SELECT count(compress_chunk(c)) FROM show_chunks('{materialization.Value.Schema}.{materialization.Value.Name}') AS c", connection))
        {
            var compressed = (long)(await compress.ExecuteScalarAsync(ct))!;
            Assert.True(compressed >= 1, "the materialization must have at least one chunk to compress for this leg to mean anything");
        }

        /* THE REPAIR: one forced refresh over [H3, H5) and nothing else. */
        var log = new CapturingTestLogger();
        var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, log, DateTime.UtcNow, ct);

        Assert.Equal(1, summary.HolesRepaired);
        Assert.Equal(2, summary.BucketsRepaired);
        Assert.Equal(0, summary.HolesDeferred);
        Assert.Equal(0, summary.HolesRemaining);
        Assert.Equal(0, summary.Failures);
        Assert.True(summary.AggregatesScanned >= 1);

        /* #3756: the tally the worker's one summary line reports, on a pass that found something — found is
           what the scan saw before the cap, and buckets found is exactly what was repaired plus deferred. */
        Assert.Equal(1, summary.HolesFound);
        Assert.Equal(2, summary.BucketsFound);
        Assert.Equal(summary.BucketsFound, summary.BucketsRepaired + summary.BucketsDeferred);
        Assert.True(summary.Elapsed > TimeSpan.Zero, "the pass times itself");

        /* THE OUTAGE SHAPE CLOSES ON THE PLAIN REFRESH — the engine logged the skipped region as invalid when
           the post-resume refresh advanced the threshold past it. Pinned, because the first cut of the pass
           assumed the opposite; if a TimescaleDB ever stops doing this the forced path still closes it and this
           assertion is what says the premise moved. */
        Assert.Equal(0, summary.HolesForced);
        Assert.Contains($"{view} had 2 bucket(s) in [{H(3):O}, {H(5):O})", log.Joined, StringComparison.Ordinal);
        Assert.Contains("one refresh over exactly those bounds closed it", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("a plain refresh left it standing", log.Joined, StringComparison.Ordinal);
        /* #3756: the pass writes NO summary of its own any more — the worker's unconditional line is the one
           owner — so the forced count is read off the tally above, not off a line. */
        Assert.DoesNotContain("needed the forced refresh", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("aggregate(s) scanned", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("still shows", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("left for a later run", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("could not scan or repair", log.Joined, StringComparison.Ordinal);

        /* The tail reads; the outage hours are still (correctly) absent; the floor did not move. */
        Assert.Equal(new[] { H(0), H(1), H(2), H(3), H(4), H(9), H(10) }, await MaterializedBucketsAsync(connection, view, ct));
        Assert.Empty(await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));

        /* A second pass finds nothing to do — and (#3756) STILL RETURNS THE WALK: this is the zero-hole tally
           the worker's line reports on the start the issue was filed about. The aggregates were probed (scanned
           is not zero), nothing was found, nothing repaired, nothing deferred, nothing failed, and the elapsed
           carries the cost of proving it. A pass that fell silent here would read the same as one that never
           ran; a pass that returns this cannot. */
        var quietLog = new CapturingTestLogger();
        var again = await TimescaleSupport.RepairMaterializationHolesAsync(connection, quietLog, DateTime.UtcNow, ct);
        Assert.Equal(0, again.HolesRepaired);
        Assert.Equal(0, again.Failures);
        Assert.Equal(0, again.HolesFound);
        Assert.Equal(0, again.BucketsFound);
        Assert.Equal(0, again.BucketsRepaired);
        Assert.Equal(0, again.HolesDeferred);
        Assert.Equal(0, again.BucketsDeferred);
        Assert.Equal(0, again.HolesRemaining);
        Assert.Equal(0, again.HolesForced);
        Assert.True(again.AggregatesScanned >= 1, "a zero-hole pass still walked the aggregates");
        Assert.Equal(TimescaleSupport.MaterializationHoleTargets.Count, again.AggregatesScanned + again.AggregatesSkipped);
        Assert.True(again.Elapsed > TimeSpan.Zero, "a zero-hole pass still took time to prove it");
        Assert.DoesNotContain("Information:", quietLog.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("Warning:", quietLog.Joined, StringComparison.Ordinal);

        /* THE HOLE WITH NO INVALIDATION BEHIND IT: H1's rows deleted from the materialization hypertable directly
           (a DELETE on the materialization logs nothing; it is the shape a refresh cut short after consuming its
           entries leaves). The plain refresh finds nothing to do and the hole stands; the pass measures that,
           escalates to the forced refresh, and the hole closes — the one case force is for, on the real engine. */
        await using (var lose = new NpgsqlCommand(
            $"DELETE FROM {materialization.Value.Schema}.{materialization.Value.Name} WHERE bucket = $1", connection))
        {
            lose.Parameters.AddWithValue(H(1));
            Assert.True(await lose.ExecuteNonQueryAsync(ct) >= 1, "the materialization must have held H1 for this leg to delete");
        }

        Assert.Equal(new[] { H(1) }, await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));
        await RefreshAsync(connection, view, H(1), H(2), ct);
        Assert.Equal(new[] { H(1) }, await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));

        var forcedLog = new CapturingTestLogger();
        var forcedSummary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, forcedLog, DateTime.UtcNow, ct);
        Assert.Equal(1, forcedSummary.HolesRepaired);
        Assert.Equal(1, forcedSummary.HolesForced);
        Assert.Equal(0, forcedSummary.HolesRemaining);
        Assert.Equal(0, forcedSummary.Failures);
        Assert.Contains($"{view} had 1 bucket(s) in [{H(1):O}, {H(2):O})", forcedLog.Joined, StringComparison.Ordinal);
        Assert.Contains("a plain refresh left it standing (no invalidation behind it) and one forced refresh over exactly those bounds closed it", forcedLog.Joined, StringComparison.Ordinal);
        Assert.Empty(await ScanAsync(connection, target, materialization.Value, H(0), H(10), ct));
        Assert.Equal(new[] { H(0), H(1), H(2), H(3), H(4), H(9), H(10) }, await MaterializedBucketsAsync(connection, view, ct));

        /* THE CONTROL on the source filter (#3653 LC): pre-freeze this planted a restart-only row and
           contrasted the legacy (admits it) against the successor (excludes it, scan and all) — two live
           relations reading the same collect.query_stats. The legacy no longer repairs at all, so only the
           successor's own half still runs live (MaterializationHoleScanShapeTests/MaterializationHoleRepairTests'
           pure pins still hold the CreateSql contrast). What remains provable here: an hour holding ONLY a
           restart row is neither materialized NOR reported as a hole — the scan applies view's own filter, so
           an uncovered hour the filter would reject is not a false positive. */
        await using (var restartOnly = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (99, $1, $2, $3, 'HoleDb', '0xHOLEHASH', '0xHOLEHANDLE', 0, 0, 0, 0)", connection))
        {
            restartOnly.Parameters.AddWithValue(H(12).AddMinutes(5));
            restartOnly.Parameters.AddWithValue(ServerId);
            restartOnly.Parameters.AddWithValue(ServerName);
            await restartOnly.ExecuteNonQueryAsync(ct);
        }

        await InsertHoursAsync(connection, new[] { 14 }, H, ct);
        await RefreshAsync(connection, view, H(14), H(15), ct);

        var restartHoles = await ScanAsync(connection, target, materialization.Value, H(0), H(14), ct);
        Assert.DoesNotContain(H(12), restartHoles);
        Assert.DoesNotContain(H(12), await MaterializedBucketsAsync(connection, view, ct));
    }

    /// <summary>
    /// #3756's second named pin: the tally on a pass that repaired one hole and deferred one. ONE contiguous
    /// 26-bucket hole under the hourly cap of 24 — the straddle <see cref="TimescaleSupport.CapMaterializationHoleRepairs"/>
    /// splits — so found, repaired and deferred are all non-zero on the same pass and the arithmetic the
    /// record states is measured rather than argued: buckets found is repaired plus deferred exactly, and the
    /// range counts exceed found by one because the split reports on both sides. The next pass finds the two
    /// deferred buckets as the one remaining hole and repairs them; the pass after that is the zero-hole walk.
    ///
    /// <para>Planted entirely below the hourly refresh policy's window (<c>now - 1 day</c>) so a background
    /// policy run landing mid-test cannot repair part of the hole and move the counts, and entirely above the raw
    /// retention horizon so the scan reaches every bucket of it.</para>
    /// </summary>
    [Fact]
    public async Task OneHoleRepairedAndOneDeferred_TheTallyCarriesFoundRepairedAndDeferredExactly_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live materialization-hole tally test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live hole-tally test needs TimescaleDB: a materialization hole exists only in a materialization.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* #3653 LC: query_stats_hourly is frozen out of the repair walk; query_stats_interval_hourly is its live
           successor and, like the legacy, admits every plain (non-restart) row this test plants. */
        var view = TimescaleSupport.QueryStatsIntervalHourlyView;
        var cap = TimescaleSupport.MaterializationHoleRepairCapBuckets(TimescaleSupport.HourlyBucket);
        Assert.Equal(24, cap);

        /* H0 materialized (the floor), H1..H26 collected and never refreshed (26 buckets of hole, cap + 2),
           H27 collected and refreshed (the ceiling). H28 = now - 32 h sits below the policy window; H0 = now - 60 h
           sits well inside the 4-day raw horizon. */
        var h0 = TimescaleSupport.AlignDown(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified), TimescaleSupport.HourlyBucket).AddHours(-60);
        DateTime H(int n) => h0.AddHours(n);
        Assert.True(H(28) < DateTime.UtcNow - TimescaleSupport.HourlyRefreshStartSpan, "the whole plant must sit below the hourly policy window");
        Assert.True(H(0) > DateTime.UtcNow - TimescaleSupport.RawRetentionSpan, "the whole plant must sit inside the raw horizon");

        await InsertHoursAsync(connection, new[] { 0 }, H, ct);
        await RefreshAsync(connection, view, H(0), H(1), ct);
        await InsertHoursAsync(connection, Enumerable.Range(1, 26).ToArray(), H, ct);
        await InsertHoursAsync(connection, new[] { 27 }, H, ct);
        await RefreshAsync(connection, view, H(27), H(28), ct);

        var target = TimescaleSupport.MaterializationHoleTargets.Single(t => t.View == view);
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);
        Assert.Equal(new[] { H(0), H(27) }, await MaterializedBucketsAsync(connection, view, ct));
        Assert.Equal(Enumerable.Range(1, 26).Select(H).ToArray(), await ScanAsync(connection, target, materialization.Value, H(0), H(27), ct));

        /* PASS ONE: one hole found (26 buckets), 24 repaired, 2 deferred past the cap — on the same pass. */
        var log = new CapturingTestLogger();
        var first = await TimescaleSupport.RepairMaterializationHolesAsync(connection, log, DateTime.UtcNow, ct);

        Assert.Equal(0, first.Failures);
        Assert.Equal(1, first.HolesFound);
        Assert.Equal(26, first.BucketsFound);
        Assert.Equal(1, first.HolesRepaired);
        Assert.Equal(24, first.BucketsRepaired);
        Assert.Equal(1, first.HolesDeferred);
        Assert.Equal(2, first.BucketsDeferred);
        Assert.Equal(0, first.HolesRemaining);
        Assert.Equal(first.BucketsFound, first.BucketsRepaired + first.BucketsDeferred);
        Assert.Equal(first.HolesFound + 1, first.HolesRepaired + first.HolesDeferred);
        Assert.True(first.AggregatesScanned >= 1);
        Assert.True(first.Elapsed > TimeSpan.Zero);

        /* The detail lines the tally counts: the repair over exactly [H1, H25) and the deferral of exactly
           [H25, H27), named with its bounds and the cap; no summary line from the pass itself. */
        Assert.Contains($"{view} had 24 bucket(s) in [{H(1):O}, {H(25):O})", log.Joined, StringComparison.Ordinal);
        Assert.Contains($"{view} has a further 2 bucket(s) of hole in [{H(25):O}, {H(27):O}) left for a later run", log.Joined, StringComparison.Ordinal);
        Assert.Contains("this run's cap for it is 24 bucket(s)", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("aggregate(s) scanned", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("could not scan or repair", log.Joined, StringComparison.Ordinal);
        Assert.Equal(Enumerable.Range(0, 25).Concat(new[] { 27 }).Select(H).ToArray(), await MaterializedBucketsAsync(connection, view, ct));
        Assert.Equal(new[] { H(25), H(26) }, await ScanAsync(connection, target, materialization.Value, H(0), H(27), ct));

        /* PASS TWO: the deferred remainder is the one hole now — found, repaired whole, nothing deferred. */
        var second = await TimescaleSupport.RepairMaterializationHolesAsync(connection, new CapturingTestLogger(), DateTime.UtcNow, ct);
        Assert.Equal(0, second.Failures);
        Assert.Equal(1, second.HolesFound);
        Assert.Equal(2, second.BucketsFound);
        Assert.Equal(1, second.HolesRepaired);
        Assert.Equal(2, second.BucketsRepaired);
        Assert.Equal(0, second.HolesDeferred);
        Assert.Equal(0, second.BucketsDeferred);
        Assert.Equal(0, second.HolesRemaining);
        Assert.Equal(Enumerable.Range(0, 28).Select(H).ToArray(), await MaterializedBucketsAsync(connection, view, ct));

        /* PASS THREE: the zero-hole walk, with the walk still in the tally. */
        var third = await TimescaleSupport.RepairMaterializationHolesAsync(connection, new CapturingTestLogger(), DateTime.UtcNow, ct);
        Assert.Equal(0, third.HolesFound);
        Assert.Equal(0, third.BucketsFound);
        Assert.Equal(0, third.HolesRepaired);
        Assert.Equal(0, third.HolesDeferred);
        Assert.Equal(0, third.Failures);
        Assert.True(third.AggregatesScanned >= 1);
        Assert.True(third.Elapsed > TimeSpan.Zero);
    }

    private static async Task InsertHoursAsync(NpgsqlConnection connection, int[] hours, Func<int, DateTime> at, CancellationToken ct)
    {
        foreach (var hour in hours)
        {
            for (var minute = 0; minute < 60; minute += 20)
            {
                await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'HoleDb', '0xHOLEHASH', '0xHOLEHANDLE', 1000, 1000, 10, 1200)", connection);
                insert.Parameters.AddWithValue((long)(hour * 100 + minute));
                insert.Parameters.AddWithValue(at(hour).AddMinutes(minute));
                insert.Parameters.AddWithValue(ServerId);
                insert.Parameters.AddWithValue(ServerName);
                await insert.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        /* The plain, unforced form a policy runs — the CALL cannot be inside a transaction. */
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DateTime[]> MaterializedBucketsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var buckets = new List<DateTime>();
        await using var read = new NpgsqlCommand($"SELECT DISTINCT bucket FROM collect.{view} WHERE server_id = $1 ORDER BY bucket", connection);
        read.Parameters.AddWithValue(ServerId);
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            buckets.Add(reader.GetDateTime(0));
        }

        return buckets.ToArray();
    }

    private static async Task<long> RawHoursAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand(
            "SELECT count(DISTINCT time_bucket('1 hour', collection_time)) FROM collect.query_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
        count.Parameters.AddWithValue(ServerId);
        count.Parameters.AddWithValue(from);
        count.Parameters.AddWithValue(to);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime[]> ScanAsync(
        NpgsqlConnection connection, TimescaleSupport.MaterializationHoleTarget target, (string Schema, string Name) materialization,
        DateTime from, DateTime to, CancellationToken ct)
    {
        var holes = new List<DateTime>();
        await using var scan = new NpgsqlCommand(TimescaleSupport.MaterializationHoleScanSql(target, materialization), connection);
        scan.Parameters.AddWithValue(from);
        scan.Parameters.AddWithValue(to);
        scan.Parameters.AddWithValue(target.BucketWidth);
        await using var reader = await scan.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            holes.Add(reader.GetDateTime(0));
        }

        return holes.ToArray();
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for #1841: the Query Store aggregate reads must count a re-collected
/// runtime-stats interval ONCE, at its latest cumulative values.
///
/// <para>query_store_stats rows are CUMULATIVE per-interval snapshots. The collector is incremental on
/// last_execution_time, so the OPEN interval is re-fetched every cycle and stored again with a growing
/// execution_count. Live evidence on issue #1841: the same (server, database, query_id, plan_id) appeared
/// up to 496 times inside ONE hour bucket. Every read that SUMs raw rows counted that interval's work 496
/// times.</para>
///
/// <para>The seed reproduces both live shapes at once: interval A is collected four times with a
/// FLAT execution_count of 1 (the 496x shape — one interval, many collections), and interval B is
/// collected three times with a GROWING execution_count (10 -> 25 -> 40, the cumulative shape). The
/// assertions are written against the true totals, so they fail loudly against an un-deduped read.</para>
/// </summary>
public sealed class QueryStoreDedupReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8841;
    private const string Db = "DedupDb";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreDedupReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /* Every seeded collection lands inside ONE date_trunc('hour') bucket so the slicer assertion is about
       dedup and not about bucket boundaries, and the whole bucket sits comfortably inside hoursBack: 24.
       hoursBack (not fromDate/toDate) on purpose: GetTimeRange applies ServerTimeHelper.UtcOffsetMinutes
       to an explicit range, which would make these fixed timestamps depend on the machine's server-time
       offset. Floored to the hour, then to whole seconds by construction — DuckDB TIMESTAMP is
       microsecond-resolution, so raw DateTime ticks would not survive the round trip. */
    private static readonly DateTime BucketStart = HourFloor(DateTime.UtcNow.AddHours(-3));

    private static DateTime HourFloor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerHour)), DateTimeKind.Unspecified);

    /* Interval identity: a runtime-stats interval has a stable first_execution_time. */
    private static readonly DateTime FirstExecA = BucketStart.AddMinutes(1);
    private static readonly DateTime FirstExecB = BucketStart.AddMinutes(2);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /// <param name="intervalId">
    /// #1841 tier 2's real interval identity. NULL seeds a LEGACY row — one collected before the collector
    /// stored the identity — which is what the mixed-window arms have to keep handling.
    /// </param>
    /// <param name="intervalStart">
    /// When the interval STARTED (UTC). NULL alongside a NULL id is the legacy shape; the reads fall back
    /// to collection_time placement for exactly these rows.
    /// </param>
    private async Task SeedAsync(
        DateTime collectionTime,
        long queryId,
        long planId,
        DateTime? firstExecutionTime,
        long executionCount,
        long avgCpuUs,
        long avgDurationUs,
        long avgReads,
        string queryHash,
        long? intervalId = null,
        DateTime? intervalStart = null,
        long avgWrites = 0,
        long avgPhysicalReads = 0)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "DedupSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = planId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)firstExecutionTime ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"SELECT {queryId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = executionCount });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgCpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgReads });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgWrites });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgPhysicalReads });
        cmd.Parameters.Add(new DuckDBParameter { Value = $"0xPLAN{planId}" });
        cmd.Parameters.Add(new DuckDBParameter { Value = false });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)intervalId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)intervalStart ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Interval A — the 496x shape: ONE interval, execution_count never moves off 1, collected four times.
    /// True work: 1 execution, 1,000us CPU, 2,000us duration, 7 reads. An un-deduped SUM reports 4x that.
    /// Interval B — the cumulative shape: execution_count and the averages both grow as the interval
    /// accumulates. True work is the LAST snapshot only: 40 x 300us CPU, 40 x 7,000us duration, 40 x 5 reads.
    /// An un-deduped SUM reports 10x100 + 25x200 + 40x300 = 18,000us of CPU against a true 12,000us.
    /// </summary>
    /* Both intervals START in BucketStart's hour and are collected inside it, so the bucket assertions
       below are about DEDUP and not about placement — the placement fix has its own test, where the two
       hours deliberately disagree. Seeded WITH the tier-2 identity so the reads exercise the real key;
       the legacy (identity-NULL) generation has its own coverage. */
    private const long IntervalIdA = 9101;
    private const long IntervalIdB = 9102;

    private async Task SeedBothIntervalShapesAsync()
    {
        foreach (var minute in new[] { 5, 10, 15, 20 })
        {
            await SeedAsync(BucketStart.AddMinutes(minute), queryId: 1, planId: 11, FirstExecA,
                executionCount: 1, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 7, queryHash: "0xHASH_A",
                intervalId: IntervalIdA, intervalStart: BucketStart);
        }

        var growth = new (int Minute, long Execs, long Cpu, long Dur)[]
        {
            (5, 10L, 100L, 5_000L),
            (10, 25L, 200L, 6_000L),
            (15, 40L, 300L, 7_000L),
        };
        foreach (var g in growth)
        {
            await SeedAsync(BucketStart.AddMinutes(g.Minute), queryId: 2, planId: 22, FirstExecB,
                executionCount: g.Execs, avgCpuUs: g.Cpu, avgDurationUs: g.Dur, avgReads: 5, queryHash: "0xHASH_B",
                intervalId: IntervalIdB, intervalStart: BucketStart);
        }
    }

    /* True deduped totals for the single hour bucket, in the units the reads return. The un-deduped
       figures beside them are what the pre-#1841 queries returned for this same seed (4 collections of
       interval A, plus B's 10x, 25x and 40x snapshots all summed).
       CPU:      (1 x 1,000 + 40 x 300) / 1000                                   = 13 ms   (un-deduped: 22 ms)
       Duration: (1 x 2,000 + 40 x 7,000) / 1000                                 = 282 ms  (un-deduped: 488 ms)
       Reads:     1 x 7 + 40 x 5                                                 = 207     (un-deduped: 403) */
    private const double TrueBucketCpuMs = 13.0;
    private const double TrueBucketDurationMs = 282.0;
    private const double TrueBucketReads = 207.0;

    [Fact]
    public async Task SlicerBucket_CountsARecollectedIntervalOnce_AtItsLatestValues()
    {
        await SeedBothIntervalShapesAsync();

        var buckets = await new LocalDataService(_duckDb).GetQueryStoreSlicerDataAsync(ServerId, hoursBack: 24);

        var bucket = Assert.Single(buckets);
        Assert.Equal(2, bucket.SessionCount); /* COUNT(DISTINCT query_id) — unaffected, guards the seed */
        Assert.Equal(TrueBucketCpuMs, bucket.TotalCpu, precision: 6);
        Assert.Equal(TrueBucketDurationMs, bucket.TotalElapsed, precision: 6);
        Assert.Equal(TrueBucketReads, bucket.TotalReads, precision: 6);
    }

    /// <summary>
    /// #3530: the slicer's reader mapped BOTH read fields to ordinal 4 and never read ordinal 6, so the
    /// SELECT's total_physical_reads was computed and dropped on the floor. The seed's three I/O columns
    /// carry values no other column can reproduce — equal fixture values are exactly how the slip stayed
    /// invisible, so distinct-per-column is the point of this test, not a nicety.
    /// </summary>
    [Fact]
    public async Task SlicerBucket_MapsWritesAndPhysicalReads_ToTheirOwnColumns()
    {
        await SeedAsync(BucketStart.AddMinutes(5), queryId: 7, planId: 77, FirstExecA,
            executionCount: 10, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 11, queryHash: "0xIOMAP",
            intervalId: 9301, intervalStart: BucketStart, avgWrites: 3, avgPhysicalReads: 5);

        var service = new LocalDataService(_duckDb);
        var bucket = Assert.Single(await service.GetQueryStoreSlicerDataAsync(ServerId, hoursBack: 24));

        /* 10 executions x the averages: logical 110, writes 30, physical 50 — all pairwise distinct.
           TotalReads and TotalLogicalReads are deliberate aliases of the LOGICAL aggregate (ordinal 4),
           the same shape the query-stats slicer maps; physical rides its own column at ordinal 6. */
        Assert.Equal(110.0, bucket.TotalReads, precision: 6);
        Assert.Equal(110.0, bucket.TotalLogicalReads, precision: 6);
        Assert.Equal(30.0, bucket.TotalWrites, precision: 6);
        Assert.Equal(50.0, bucket.TotalPhysicalReads, precision: 6);

        /* #3547's data half: the slicer OVERLAY reads the same rows through the timeline, whose SELECT
           carried no physical column at all — so a physical-sorted chart had nothing honest to draw.
           Same distinct values, so a logical/physical swap on either side goes red here. (The overlay's
           metric->field switch itself is WPF code-behind, untestable here like its sibling arms.) */
        var point = Assert.Single(await service.GetQueryStoreItemTimelineAsync(ServerId, Db, queryId: 7, planId: 77, hoursBack: 24));
        Assert.Equal(110.0, point.Reads, precision: 6);
        Assert.Equal(50.0, point.PhysicalReads, precision: 6);
    }

    [Fact]
    public async Task TopQueries_ReportTheLatestCumulativeExecutionCount_NotTheSumOfSnapshots()
    {
        await SeedBothIntervalShapesAsync();

        var rows = await new LocalDataService(_duckDb).GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24);

        var a = Assert.Single(rows, r => r.QueryId == 1);
        var b = Assert.Single(rows, r => r.QueryId == 2);

        /* Four collections of one 1-execution interval is ONE execution, not four. */
        Assert.Equal(1L, a.TotalExecutions);
        /* 10 -> 25 -> 40 is one interval that reached 40, not 75 executions. */
        Assert.Equal(40L, b.TotalExecutions);

        /* The averages must come from the LATEST snapshot of the interval, not be an avg-of-avgs across
           re-collections (which would give B (5000+6000+7000)/3 = 6.0 ms). */
        Assert.Equal(2.0, a.AvgDurationMs, precision: 6);
        Assert.Equal(7.0, b.AvgDurationMs, precision: 6);
        Assert.Equal(0.3, b.AvgCpuTimeMs, precision: 6);
    }

    [Fact]
    public async Task Comparison_WeightsEachIntervalOnce_InBothWindows()
    {
        await SeedBothIntervalShapesAsync();

        /* The comparison takes explicit UTC ranges rather than hoursBack. Both windows cover the same
           seeded bucket, so a correct read reports identical current and baseline numbers — any dedup
           asymmetry between the two arms would show up as a spurious delta. */
        var start = BucketStart.AddMinutes(-1);
        var end = BucketStart.AddMinutes(59);

        var rows = await new LocalDataService(_duckDb)
            .GetQueryStoreComparisonAsync(ServerId, start, end, start, end);

        var b = Assert.Single(rows, r => r.QueryHash == "0xHASH_B");
        Assert.Equal(40L, b.ExecutionCount);
        Assert.Equal(40L, b.BaselineExecutionCount);
        Assert.Equal(7.0, b.AvgDurationMs, precision: 6);
        Assert.Equal(0.3, b.AvgCpuMs, precision: 6);

        var a = Assert.Single(rows, r => r.QueryHash == "0xHASH_A");
        Assert.Equal(1L, a.ExecutionCount);
        Assert.Equal(2.0, a.AvgDurationMs, precision: 6);
    }

    [Fact]
    public async Task DurationTrend_PlacesEachIntervalsWorkAtTheHourItRan_DedupedOnce()
    {
        /* DELIBERATE FLIP of the #1845 pin that lived here (#1841 tier 2). That pin recorded the one
           Query Store aggregate left un-deduped, and its reasoning was sound but rested on a premise that
           was FALSE: that placing work at the interval's own clock would trade a magnitude bug for a
           timezone bug, because the interval clock is the monitored server's LOCAL wall time. It is not.
           sys.query_store_runtime_stats_interval.start_time and first_execution_time are both
           datetimeoffset, verified on a live server reading +00:00 while the host sat at UTC-4, and the
           collector normalizes through DateTimeOffset.UtcDateTime before storing either way. So the
           interval clock shares an axis with collection_time and the honest fix was available all along
           once the identity was collected.

           Two intervals, one hour apart, EACH collected twice and straddling the hour boundary — the
           shape that made the old query overstate. Interval 1 ran in hour 0 and was collected at 0:50 and
           1:10; interval 2 ran in hour 1 and was collected at 1:50 and 2:10.

           Un-deduped at collection_time (the pre-tier-2 read) that is FOUR points carrying every
           cumulative restatement. Deduped and placed at the interval start it is TWO points, each holding
           its interval's true final total, at the hour that work actually ran. */
        var h0 = BucketStart;
        var h1 = BucketStart.AddHours(1);

        await SeedAsync(h0.AddMinutes(50), queryId: 1, planId: 11, FirstExecA,
            executionCount: 10, avgCpuUs: 100, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xT1",
            intervalId: 7001, intervalStart: h0);
        await SeedAsync(h1.AddMinutes(10), queryId: 1, planId: 11, FirstExecA,
            executionCount: 40, avgCpuUs: 300, avgDurationUs: 7_000, avgReads: 0, queryHash: "0xT1",
            intervalId: 7001, intervalStart: h0);

        await SeedAsync(h1.AddMinutes(50), queryId: 2, planId: 22, FirstExecB,
            executionCount: 3, avgCpuUs: 100, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xT2",
            intervalId: 7002, intervalStart: h1);
        await SeedAsync(h1.AddHours(1).AddMinutes(10), queryId: 2, planId: 22, FirstExecB,
            executionCount: 9, avgCpuUs: 200, avgDurationUs: 2_000, avgReads: 0, queryHash: "0xT2",
            intervalId: 7002, intervalStart: h1);

        var points = await new LocalDataService(_duckDb).GetQueryStoreDurationTrendAsync(ServerId, hoursBack: 24);

        Assert.Equal(2, points.Count);
        Assert.Equal(h0, points[0].CollectionTime);
        Assert.Equal(h1, points[1].CollectionTime);

        /* The first point has no predecessor, so its rate is UNKNOWABLE — null, not the 0 the query_stats and
           procedure_stats trends (and this one) used to fabricate (#3541 A12). The point itself is kept:
           the interval ran, only its rate is undefined. */
        Assert.False(points[0].HasRate);
        Assert.Null(points[0].Value);
        Assert.Null(points[0].ExecutionsPerSecond);

        /* Interval 2's FINAL snapshot only: 9 executions x 2,000us = 18 ms of work, over the 3,600s
           between interval starts. Un-deduped this point would also carry interval 2's earlier 3x1,000us
           restatement AND interval 1's rows that were collected in this hour. */
        Assert.Equal(18.0 / 3600.0, points[1].Value!.Value, precision: 9);
    }

    [Fact]
    public async Task DurationTrend_LegacyRowsKeepThePreTier2Treatment_AndTheArmsDoNotOverlap()
    {
        /* The mixed-window boundary (#1841 tier 2), pinned so it can neither lie nor crash. Rows with no
           interval identity cannot be placed at an interval start — nothing can reconstruct one — so they
           keep the pre-tier-2 behavior exactly: un-deduped, one point per collection_time. The arms split
           on interval_start_time_utc IS NULL, which partitions the rows with NO overlap and NO gap, so a
           window spanning the upgrade counts every row exactly once.

           Two legacy collections of one interval (the cumulative shape) plus one identified interval an
           hour later. Legacy contributes its TWO restatement points; the identified interval contributes
           ONE, at its start. If the arms overlapped, a legacy row would appear twice; if they had a gap,
           one would vanish. */
        var h0 = BucketStart;
        var h1 = BucketStart.AddHours(1);

        await SeedAsync(h0.AddMinutes(10), queryId: 1, planId: 11, FirstExecA,
            executionCount: 10, avgCpuUs: 100, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xL1");
        await SeedAsync(h0.AddMinutes(20), queryId: 1, planId: 11, FirstExecA,
            executionCount: 25, avgCpuUs: 200, avgDurationUs: 2_000, avgReads: 0, queryHash: "0xL1");

        await SeedAsync(h1.AddMinutes(10), queryId: 2, planId: 22, FirstExecB,
            executionCount: 4, avgCpuUs: 100, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xN1",
            intervalId: 7101, intervalStart: h1);

        var points = await new LocalDataService(_duckDb).GetQueryStoreDurationTrendAsync(ServerId, hoursBack: 24);

        Assert.Equal(3, points.Count);
        Assert.Equal(h0.AddMinutes(10), points[0].CollectionTime);   /* legacy, at collection_time */
        Assert.Equal(h0.AddMinutes(20), points[1].CollectionTime);   /* legacy, still restated */
        Assert.Equal(h1, points[2].CollectionTime);                  /* identified, at its interval START */
    }

    [Fact]
    public async Task SlicerBucket_PlacesAnIntervalInTheHourItRan_NotTheHourItWasCollected()
    {
        /* The one-bucket placement lag (#1841 tier 2). Query Store's default interval is 60 minutes and
           the closing fetch lands in the cycle AFTER the interval ends, so an interval that ran in hour 0
           was reliably drawn in hour 1. Here the disagreement is explicit: the interval STARTED at hour 0
           and every one of its collections landed in hour 1.

           Pre-tier-2 this produced a single bucket at h1. Now it produces a single bucket at h0, because
           interval_start_time_utc is the interval's own start converted to UTC at collection — the same
           clock as collection_time, so this is a placement fix and not a timezone trade. */
        var h0 = BucketStart;
        var h1 = BucketStart.AddHours(1);

        await SeedAsync(h1.AddMinutes(5), queryId: 1, planId: 11, FirstExecA,
            executionCount: 3, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 6, queryHash: "0xP1",
            intervalId: 7201, intervalStart: h0);
        await SeedAsync(h1.AddMinutes(10), queryId: 1, planId: 11, FirstExecA,
            executionCount: 5, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 6, queryHash: "0xP1",
            intervalId: 7201, intervalStart: h0);

        var buckets = await new LocalDataService(_duckDb).GetQueryStoreSlicerDataAsync(ServerId, hoursBack: 24);

        var bucket = Assert.Single(buckets);
        Assert.Equal(h0, bucket.BucketTime);
        /* And still deduped: the LATEST snapshot only, 5 executions x 1,000us = 5 ms, not 8 x. */
        Assert.Equal(5.0, bucket.TotalCpu, precision: 6);
    }

    /// <summary>
    /// WATCHED (mutation): put the window filter back on collection_time and this goes red.
    ///
    /// <para>#1892's left edge. Once the bars were keyed on the interval's start (#1841 tier 2) but the window
    /// was still filtered on collection_time, the two stopped agreeing: an interval that STARTED before the
    /// requested range but whose closing fetch landed inside it passed the filter and then drew a bar dated
    /// before the range began. A "last 24 hours" chart grew a bar to the left of 24 hours ago.</para>
    ///
    /// <para>The seeded row is deliberately well clear of the edge in both directions -- the interval starts
    /// 26 hours back, the collection lands 22 hours back -- so the window moving by however long the test
    /// takes to run cannot decide the outcome.</para>
    /// </summary>
    [Fact]
    public async Task Slicer_DropsAnIntervalThatStartedBeforeTheWindow_EvenWhenItWasCollectedInsideIt()
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var startedBeforeWindow = HourFloor(now.AddHours(-26));
        var collectedInsideWindow = now.AddHours(-22);

        await SeedAsync(collectedInsideWindow, queryId: 90, planId: 990, FirstExecA,
            executionCount: 4, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 6, queryHash: "0xEDGE",
            intervalId: 9001, intervalStart: startedBeforeWindow);

        /* One row in the window on the old filter, none on the new one. */
        var buckets = await new LocalDataService(_duckDb).GetQueryStoreSlicerDataAsync(ServerId, hoursBack: 24);

        Assert.DoesNotContain(buckets, b => b.BucketTime == startedBeforeWindow);
        Assert.Empty(buckets);
    }

    /// <summary>
    /// WATCHED (mutation): restore the collection_time ceiling and this goes red.
    ///
    /// <para>#1892's right edge, and the half that COSTS data rather than adding it. A window's final interval
    /// is still open when the window ends, so its closing fetch happens afterwards -- and a
    /// <c>collection_time &lt;= end</c> filter therefore dropped the newest bar entirely. That is the
    /// collection lag #1841 set out to remove, reappearing one layer down as a missing bar instead of a
    /// misplaced one.</para>
    ///
    /// <para>The collection is seeded AFTER the window's end, which for a "last N hours" window means a
    /// timestamp in the future. That is a stand-in, stated plainly: the shape occurs for real whenever the
    /// requested range ends in the past, which is every historical window the date pickers produce. Using
    /// hoursBack keeps the test off <c>ServerTimeHelper.UtcOffsetMinutes</c>, a process-wide mutable static
    /// that an explicit range would drag in.</para>
    /// </summary>
    [Fact]
    public async Task Slicer_KeepsAnIntervalThatStartedInsideTheWindow_ThoughItsClosingFetchLandedAfterIt()
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var startedInsideWindow = HourFloor(now.AddHours(-2));
        var collectedAfterWindow = now.AddHours(2);

        await SeedAsync(collectedAfterWindow, queryId: 91, planId: 991, FirstExecB,
            executionCount: 6, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 6, queryHash: "0xEDGE2",
            intervalId: 9002, intervalStart: startedInsideWindow);

        var buckets = await new LocalDataService(_duckDb).GetQueryStoreSlicerDataAsync(ServerId, hoursBack: 24);

        var bucket = Assert.Single(buckets);
        Assert.Equal(startedInsideWindow, bucket.BucketTime);
        Assert.Equal(6.0, bucket.TotalCpu, precision: 6);
    }

    /// <summary>
    /// WATCHED (mutation): the duration trend carries the identical mismatch, and fixing only the slicer
    /// leaves the two charts on the same screen disagreeing about which interval is in the window.
    /// </summary>
    [Fact]
    public async Task DurationTrend_DropsAnIntervalThatStartedBeforeTheWindow_LikeTheSlicerDoes()
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var startedBeforeWindow = HourFloor(now.AddHours(-26));

        await SeedAsync(now.AddHours(-22), queryId: 92, planId: 992, FirstExecA,
            executionCount: 4, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 6, queryHash: "0xEDGE3",
            intervalId: 9003, intervalStart: startedBeforeWindow);

        var points = await new LocalDataService(_duckDb).GetQueryStoreDurationTrendAsync(ServerId, hoursBack: 24);

        Assert.DoesNotContain(points, p => p.CollectionTime == startedBeforeWindow);
        Assert.Empty(points);
    }

    [Fact]
    public async Task DedupKeysOnTheRealIntervalId_WhenFirstExecutionTimeCannotTellIntervalsApart()
    {
        /* What the real identity buys over the tier-1 proxy. first_execution_time is NULL here — Query
           Store leaves it unset on rows the engine never attributed a first execution to — so under
           tier 1's key these two DISTINCT intervals collapsed into one and the read UNDER-counted, which
           is the failure mode dedup is supposed to prevent, not cause. Keyed on runtime_stats_interval_id
           they stay two intervals: 6 + 5 = 11 executions. */
        await SeedAsync(BucketStart.AddMinutes(5), queryId: 3, planId: 33, firstExecutionTime: null,
            executionCount: 4, avgCpuUs: 1_000, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xNULLFE",
            intervalId: 7301, intervalStart: BucketStart);
        await SeedAsync(BucketStart.AddMinutes(10), queryId: 3, planId: 33, firstExecutionTime: null,
            executionCount: 6, avgCpuUs: 1_000, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xNULLFE",
            intervalId: 7301, intervalStart: BucketStart);
        await SeedAsync(BucketStart.AddMinutes(15), queryId: 3, planId: 33, firstExecutionTime: null,
            executionCount: 5, avgCpuUs: 1_000, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xNULLFE",
            intervalId: 7302, intervalStart: BucketStart);

        var rows = await new LocalDataService(_duckDb).GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24);

        var row = Assert.Single(rows);
        Assert.Equal(11L, row.TotalExecutions);
    }

    [Fact]
    public async Task DedupIsScopedPerInterval_SoASecondIntervalOfTheSameQueryStillCounts()
    {
        /* The dedup key is (database, query_id, plan_id, first_execution_time). A NEW interval of the same
           query and plan is a distinct unit of work and must survive — a dedup keyed only on the query
           would silently drop it and under-count instead. */
        await SeedAsync(BucketStart.AddMinutes(5), queryId: 3, planId: 33, FirstExecA,
            executionCount: 4, avgCpuUs: 1_000, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xHASH_C");
        await SeedAsync(BucketStart.AddMinutes(10), queryId: 3, planId: 33, FirstExecA,
            executionCount: 6, avgCpuUs: 1_000, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xHASH_C");
        await SeedAsync(BucketStart.AddMinutes(15), queryId: 3, planId: 33, FirstExecB,
            executionCount: 5, avgCpuUs: 1_000, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xHASH_C");

        var rows = await new LocalDataService(_duckDb).GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24);

        /* Interval one closed at 6, interval two at 5 — 11 executions, not 15 (un-deduped) and not 6
           (over-deduped to one row per query/plan). */
        var row = Assert.Single(rows);
        Assert.Equal(11L, row.TotalExecutions);
    }

    /// <summary>
    /// #1921 (Erik's option 1): the slicer OVERLAY places its points at the hour the work RAN, matching the
    /// bars it is drawn over, not at the hour the collector observed it.
    ///
    /// <para>The seed makes the two clocks disagree on purpose — the interval STARTED in hour 0 and every one
    /// of its collections landed in hour 1 — because that disagreement is the only thing that can show the
    /// move. Before #1921 the overlay plotted at collection_time while #1841 had already moved the bars to
    /// the interval start, so a point sat up to one Query Store interval to the RIGHT of the bar describing
    /// the very same work, and the file's own stated invariant (the overlay agrees with the bars) had
    /// silently stopped holding on its placement half.</para>
    ///
    /// <para>The assertion pins BOTH halves against the bar computed from the same rows: same x, same value.
    /// The dedup half matters as much as the placement half here — the overlay used to plot every history row,
    /// so this interval drew a rising staircase of restatements (3 then 5 executions) rather than one point at
    /// its final 5.</para>
    /// </summary>
    [Fact]
    public async Task Overlay_PlacesPointsWhereTheWorkRan_AgreeingWithTheBarsItIsDrawnOver()
    {
        var h0 = BucketStart;
        var h1 = BucketStart.AddHours(1);

        await SeedAsync(h1.AddMinutes(5), queryId: 1, planId: 11, FirstExecA,
            executionCount: 3, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 6, queryHash: "0xOVL",
            intervalId: 7301, intervalStart: h0);
        await SeedAsync(h1.AddMinutes(10), queryId: 1, planId: 11, FirstExecA,
            executionCount: 5, avgCpuUs: 1_000, avgDurationUs: 2_000, avgReads: 6, queryHash: "0xOVL",
            intervalId: 7301, intervalStart: h0);

        var service = new LocalDataService(_duckDb);
        var timeline = await service.GetQueryStoreItemTimelineAsync(ServerId, Db, queryId: 1, planId: 11, hoursBack: 24);

        var point = Assert.Single(timeline);
        Assert.Equal(h0, point.PointTime);
        Assert.NotEqual(h1.AddMinutes(10), point.PointTime);

        /* The interval's FINAL snapshot, not a staircase: 5 x 1,000us CPU = 5 ms, 5 x 2,000us = 10 ms. */
        Assert.Equal(5.0, point.CpuMs, precision: 6);
        Assert.Equal(10.0, point.ElapsedMs, precision: 6);
        Assert.Equal(30.0, point.Reads, precision: 6);

        /* And it agrees with the bar drawn from the same rows — which is the invariant, stated as an
           assertion rather than a comment. */
        var bucket = Assert.Single(await service.GetQueryStoreSlicerDataAsync(ServerId, hoursBack: 24));
        Assert.Equal(bucket.BucketTime, point.PointTime);
        Assert.Equal(bucket.TotalCpu, point.CpuMs, precision: 6);
    }

    /// <summary>
    /// Source-containment guard, the Lite counterpart of the Darling Viewer's SQL-constant theory. Lite
    /// builds its Query Store SQL inline (no exposed constants to pin), so a FIFTH aggregate read added to
    /// this file could ship un-deduped and silently reintroduce #1841. Every read of v_query_store_stats in
    /// LocalDataService.QueryStore.cs must therefore be accounted for: either it carries a dedup CTE, or it
    /// is one of the two reads deliberately left raw, each of which says so at the source.
    /// </summary>
    [Fact]
    public void EveryQueryStoreAggregateInTheFile_CarriesADedupCte()
    {
        var source = File.ReadAllText(SourcePath("Lite", "Services", "LocalDataService.QueryStore.cs"));

        /* The six aggregate reads: slicer, top queries, the comparison's two windows, the duration trend's
           identified arm (#1841 tier 2), and the slicer OVERLAY (#1921). Counting occurrences rather than
           matching order keeps this from breaking on a harmless reshuffle.

           The overlay is the newest and the reason this count moved: before #1921 it reused the history
           grid's raw per-collection read, so it carried no dedup at all — which is exactly the "a read in
           this file ships un-deduped" case this guard exists to catch, arriving from the direction of a read
           being ADDED rather than one being edited. */
        var partitions = Regex.Matches(
            source,
            @"PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role").Count;
        var rankFilters = Regex.Matches(source, @"(?:WHERE|AND)\s+(?:qs\.)?rn = 1").Count;

        Assert.Equal(6, partitions);
        /* Eight, not six: the comparison's two deduped CTEs are each consumed TWICE — once to pick the
           top 100 hashes and once by the value aggregate — and an rn filter missing from either consumer
           would let the un-deduped rows straight back into the numbers. */
        Assert.Equal(8, rankFilters);

        /* Every dedup orders by collection_time FIRST — "latest" is never decided by execution_count, which
           can sit still across a hundred re-collections of the same interval.

           execution_count is the #1907 tie-break and must follow it at every site, never replace it. It only
           decides rows that tie on collection_time, which after #1907 no newly collected row can do: Query
           Store's flushed and in-memory slices of one interval used to be stored as two rows sharing this
           whole partition AND collection_time, and the survivor was whichever the engine emitted first. The
           collector now combines them, so this clause is inert on new rows and exists for the ones already
           stored, which cannot be rewritten (#1912). Re-pinned from #1845/#1853, deliberately: the previous
           form asserted the ORDER BY ended at collection_time. */
        Assert.Equal(partitions, Regex.Matches(source, @"ORDER BY collection_time DESC, execution_count DESC\s*\n\s*\) AS rn").Count);
        Assert.DoesNotContain("ORDER BY execution_count", source, StringComparison.Ordinal);

        /* No dedup may key on the tier-1 proxy ALONE any more: the real interval id has to be in every
           partition, or a row whose first_execution_time is NULL collapses with every other such interval
           of the same plan and the read silently UNDER-counts. */
        Assert.DoesNotContain("PARTITION BY database_name, query_id, plan_id, first_execution_time", source, StringComparison.Ordinal);

        /* The history drilldown's exclusion must keep explaining itself, so it never reads as an oversight. */
        Assert.Contains("Deliberately NOT deduped per interval (#1841)", source, StringComparison.Ordinal);

        /* The duration trend is no longer an exclusion — it is the two-armed fix. Both arms must be
           present and the split must stay total: identified rows placed at the interval start, legacy
           rows at collection_time, partitioned on IS NULL / IS NOT NULL so nothing is counted twice and
           nothing is dropped. */
        Assert.DoesNotContain("KNOWN OVERSTATEMENT", source, StringComparison.Ordinal);
        Assert.Contains("AND   interval_start_time_utc IS NOT NULL", source, StringComparison.Ordinal);
        Assert.Contains("AND   interval_start_time_utc IS NULL", source, StringComparison.Ordinal);
        Assert.Contains("interval_start_time_utc AS point_time", source, StringComparison.Ordinal);
        Assert.Contains("collection_time AS point_time", source, StringComparison.Ordinal);

        /* Both bucketed reads place work at the interval start when there is one — the slicer's
           one-bucket lag fix. COALESCE, not a bare column, so legacy rows keep collection_time. */
        Assert.Equal(2, Regex.Matches(source, @"date_trunc\('hour', COALESCE\(interval_start_time_utc, collection_time\)\)").Count);
    }

    /// <summary>Walks up from the test binary to the repo root so the pin works from any run directory.</summary>
    private static string SourcePath(params string[] parts)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, "could not locate the repository root from " + AppContext.BaseDirectory);
        return Path.Combine([dir!, .. parts]);
    }

    [Fact]
    public async Task DedupKeepsTheLatestRow_EvenWhenTheCumulativeCountDoesNotGrow()
    {
        /* Guards the ORDER BY: "latest" must be decided by collection_time, not by execution_count.
           Interval A's execution_count never moves, so a MAX(execution_count) style dedup would be
           satisfied by the FIRST row and would silently keep the stalest averages. */
        await SeedAsync(BucketStart.AddMinutes(5), queryId: 4, planId: 44, FirstExecA,
            executionCount: 2, avgCpuUs: 1_000, avgDurationUs: 1_000, avgReads: 0, queryHash: "0xHASH_D");
        await SeedAsync(BucketStart.AddMinutes(10), queryId: 4, planId: 44, FirstExecA,
            executionCount: 2, avgCpuUs: 9_000, avgDurationUs: 9_000, avgReads: 0, queryHash: "0xHASH_D");

        var rows = await new LocalDataService(_duckDb).GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24);

        var row = Assert.Single(rows);
        Assert.Equal(2L, row.TotalExecutions);
        Assert.Equal(9.0, row.AvgDurationMs, precision: 6);
    }

    /// <summary>
    /// #1907: two rows that tie on the ENTIRE dedup key AND collection_time — the flushed and the
    /// still-in-memory slice of one Query Store interval, as every pre-#1907 build stored them — must
    /// resolve to the same row every time, and to the FLUSHED one.
    ///
    /// <para>The collector now combines the slices before storing, so this shape cannot be collected any
    /// more; it is pinned because the rows already in the store still carry it and cannot be rewritten.
    /// Neither survivor is the interval's truth (125 here) — the truth is the SUM, which no read-side rule
    /// can express (#1912) — so the contract this pins is DETERMINISM plus closest-available, not
    /// correctness.</para>
    ///
    /// <para>Both insertion orders are seeded, as two independent queries, precisely so the test cannot
    /// pass by accident. Without the tie-break the survivor is whatever the engine happens to emit first,
    /// which for a heap scan tracks insertion order — so one of the two arms returns the sliver and the
    /// test goes red. Asserting only one order would let an engine that happens to favour the earlier row
    /// keep this green with the fix reverted.</para>
    /// </summary>
    [Fact]
    public async Task TiedSlicesOfOneInterval_ResolveDeterministicallyToTheFlushedSlice()
    {
        var tied = BucketStart.AddMinutes(30);
        const long IntervalIdTied = 9107;

        /* Query 5: the SLIVER is inserted first, the flushed slice second. */
        await SeedAsync(tied, queryId: 5, planId: 55, FirstExecA,
            executionCount: 25, avgCpuUs: 200, avgDurationUs: 2_000, avgReads: 1, queryHash: "0xHASH_E",
            intervalId: IntervalIdTied, intervalStart: BucketStart);
        await SeedAsync(tied, queryId: 5, planId: 55, FirstExecA,
            executionCount: 100, avgCpuUs: 800, avgDurationUs: 8_000, avgReads: 4, queryHash: "0xHASH_E",
            intervalId: IntervalIdTied, intervalStart: BucketStart);

        /* Query 6: the same pair, inserted the other way round. */
        await SeedAsync(tied, queryId: 6, planId: 66, FirstExecA,
            executionCount: 100, avgCpuUs: 800, avgDurationUs: 8_000, avgReads: 4, queryHash: "0xHASH_F",
            intervalId: IntervalIdTied, intervalStart: BucketStart);
        await SeedAsync(tied, queryId: 6, planId: 66, FirstExecA,
            executionCount: 25, avgCpuUs: 200, avgDurationUs: 2_000, avgReads: 1, queryHash: "0xHASH_F",
            intervalId: IntervalIdTied, intervalStart: BucketStart);

        var rows = await new LocalDataService(_duckDb).GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24);

        Assert.Equal(2, rows.Count);
        foreach (var r in rows)
        {
            /* The flushed slice's numbers, both times, whichever order they were stored in. */
            Assert.Equal(100L, r.TotalExecutions);
            Assert.Equal(8.0, r.AvgDurationMs, precision: 6);
        }
    }
}

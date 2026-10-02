/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3540, the read half of the keystone, proven END TO END against a real DuckDB through the real
/// <see cref="LocalDataService"/> reads: a row the collector stored with <c>sample_interval_seconds = 0</c> — the
/// shared calculator's "no delta knowable" marker (first sighting, counter reset, a gap past the measured
/// 3600 s policy; in practice, a restart) — is NOT a point. It used to be a confident 0.00 ms/sec or a
/// "0.00 ms" latency at exactly the moment nothing was knowable.
///
/// <para>Three states per row, and every read here treats them distinctly: a MEASURED interval divides the
/// delta and wins over the LAG derivation; the marker (0) yields no point; and NULL — every row collected
/// before Lite schema v60, whose interval was never recorded — falls back to the LAG over collection_time
/// those reads always used, so history keeps rendering exactly as it did. Darling's twin of these claims is
/// pinned against live Postgres in the <c>Viewer*LivePostgresTests</c> (gated on <c>DARLING_TEST_PG</c>).</para>
///
/// <para>These run against the store rather than against SQL text because the contract is the ROW that
/// comes back, not the shape of the string — a pin on <c>NULLIF</c> would pass with the reader still mapping
/// NULL to 0 in C#, which is exactly the fabrication being removed.</para>
/// </summary>
public sealed class DeltaFamilyUnknowableRowReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -3540;
    private const string ServerName = "delta-interval-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public DeltaFamilyUnknowableRowReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// The single-type wait trend: four collections five minutes apart. t1/t2 are pre-v60 rows (NULL
    /// interval) — t1 has no prior and is not a point (the fabricated first point), t2 divides by the LAG's
    /// 300 s. t3 is the marker and must be absent. t4 stores a measured 120 s beside a 1200 ms delta, and
    /// the stored interval wins over the LAG (which would say 300 s).
    /// </summary>
    [Fact]
    public async Task WaitTrend_DropsTheUnknowableRow_PrefersTheStoredInterval_KeepsPreV60History()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);
        var t4 = t3.AddMinutes(5);

        await SeedWaitAsync(t1, "CXPACKET", deltaMs: 100, deltaSignal: 10, deltaTasks: 5, interval: null);
        await SeedWaitAsync(t2, "CXPACKET", deltaMs: 600, deltaSignal: 60, deltaTasks: 3, interval: null);
        await SeedWaitAsync(t3, "CXPACKET", deltaMs: 0, deltaSignal: 0, deltaTasks: 0, interval: 0);
        await SeedWaitAsync(t4, "CXPACKET", deltaMs: 1200, deltaSignal: 120, deltaTasks: 4, interval: 120);

        var points = await _dataService.GetWaitStatsTrendAsync(ServerId, "CXPACKET", hoursBack: 3);

        Assert.Equal(new[] { t2, t4 }, points.Select(p => p.CollectionTime).ToArray());

        /* t2 (pre-v60): 600 / 300 = 2.0 ms/sec; signal 0.2; avg 600 / 3 = 200. */
        Assert.Equal(2.0, points[0].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(0.2, points[0].SignalWaitTimeMsPerSecond, precision: 6);
        Assert.Equal(200.0, points[0].AvgMsPerWait, precision: 6);

        /* t4: the STORED 120 s — 1200 / 120 = 10.0, not the LAG's 1200 / 300 = 4.0. */
        Assert.Equal(10.0, points[1].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(1.0, points[1].SignalWaitTimeMsPerSecond, precision: 6);
        Assert.Equal(300.0, points[1].AvgMsPerWait, precision: 6);

        /* The batched-by-type sibling reads the same rows through its own SQL and must agree. */
        var byType = await _dataService.GetWaitStatsTrendsByTypesAsync(ServerId, new List<string> { "CXPACKET" }, hoursBack: 3);
        Assert.Equal(new[] { t2, t4 }, byType["CXPACKET"].Select(p => p.CollectionTime).ToArray());
        Assert.Equal(10.0, byType["CXPACKET"][1].WaitTimeMsPerSecond, precision: 6);
    }

    /// <summary>
    /// The all-types total (the Overview wait lane): the collection's interval is the MAX over its rows, so a
    /// wait type first seen in an otherwise steady pass (its own row stores 0) does not blank the collection —
    /// it adds 0 to the sum — while a restart collection, where EVERY row stores 0, is absent.
    /// </summary>
    [Fact]
    public async Task TotalWaitTrend_TakesMaxStoredIntervalPerCollection_DropsAnAllUnknowableCollection()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(1);
        var t3 = t2.AddMinutes(1);
        var t4 = t3.AddMinutes(1);

        /* Pre-v60 pair: t1 is the window's first row (no point); t2 = (60 + 120) / LAG 60 s = 3.0. */
        await SeedWaitAsync(t1, "WAIT_A", 100, 0, 1, interval: null);
        await SeedWaitAsync(t1, "WAIT_B", 200, 0, 1, interval: null);
        await SeedWaitAsync(t2, "WAIT_A", 60, 0, 1, interval: null);
        await SeedWaitAsync(t2, "WAIT_B", 120, 0, 1, interval: null);
        /* Restart: every row unknowable. */
        await SeedWaitAsync(t3, "WAIT_A", 0, 0, 0, interval: 0);
        await SeedWaitAsync(t3, "WAIT_B", 0, 0, 0, interval: 0);
        /* Steady pass with a newly seen wait type: MAX = 30 wins over the LAG's 60; (90 + 0) / 30 = 3.0. */
        await SeedWaitAsync(t4, "WAIT_A", 90, 0, 1, interval: 30);
        await SeedWaitAsync(t4, "WAIT_C", 0, 0, 0, interval: 0);

        var points = await _dataService.GetTotalWaitTrendAsync(ServerId, hoursBack: 3);

        Assert.Equal(new[] { t2, t4 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(3.0, points[0].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(3.0, points[1].WaitTimeMsPerSecond, precision: 6);
    }

    /// <summary>
    /// The lock-wait trend (Blocking tab) shares the idiom: pre-v60 rows keep the LAG, the marker is absent.
    /// </summary>
    [Fact]
    public async Task LockWaitTrend_DropsTheUnknowableRow()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(1);
        var t3 = t2.AddMinutes(1);

        await SeedWaitAsync(t1, "LCK_M_S", 3000, 0, 1, interval: null);
        await SeedWaitAsync(t2, "LCK_M_S", 6000, 0, 1, interval: null);
        await SeedWaitAsync(t3, "LCK_M_S", 0, 0, 0, interval: 0);

        var points = await _dataService.GetLockWaitTrendAsync(ServerId, hoursBack: 3);

        var point = Assert.Single(points);
        Assert.Equal(t2, point.CollectionTime);
        Assert.Equal(100.0, point.WaitTimeMsPerSecond, precision: 6);
    }

    /// <summary>
    /// The file I/O latency trend: a restart's (0 stall, 0 reads) row used to render as "0.00 ms" for the file
    /// — a point at the bottom of the chart claiming instant storage. With the marker stored it is absent; a
    /// pre-v60 row with 0 reads still reads 0 (the pre-existing idle-file convention, not this lane's).
    /// </summary>
    [Fact]
    public async Task FileIoLatencyTrend_DropsTheUnknowableRow_KeepsPreV60Rows()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(1);
        var t3 = t2.AddMinutes(1);

        await SeedFileIoAsync(t1, reads: 5, writes: 0, stallRead: 50, stallWrite: 0, interval: null);
        await SeedFileIoAsync(t2, reads: 0, writes: 0, stallRead: 0, stallWrite: 0, interval: 0);
        await SeedFileIoAsync(t3, reads: 10, writes: 4, stallRead: 200, stallWrite: 20, interval: 60);

        var points = await _dataService.GetFileIoLatencyTrendAsync(ServerId, hoursBack: 3);

        Assert.Equal(new[] { t1, t3 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(10.0, points[0].AvgReadLatencyMs, precision: 6);
        Assert.Equal(20.0, points[1].AvgReadLatencyMs, precision: 6);
        Assert.Equal(5.0, points[1].AvgWriteLatencyMs, precision: 6);

        /* The tempdb file read is the same shape over database_name = 'tempdb'. */
        await SeedFileIoAsync(t2, reads: 0, writes: 0, stallRead: 0, stallWrite: 0, interval: 0, database: "tempdb", file: "tempdev");
        await SeedFileIoAsync(t3, reads: 8, writes: 8, stallRead: 80, stallWrite: 40, interval: 60, database: "tempdb", file: "tempdev");
        var tempdb = await _dataService.GetTempDbFileIoTrendAsync(ServerId, hoursBack: 3);
        var tempdbPoint = Assert.Single(tempdb);
        Assert.Equal(t3, tempdbPoint.CollectionTime);
        Assert.Equal(10.0, tempdbPoint.AvgReadLatencyMs, precision: 6);
    }

    /// <summary>
    /// The latest-snapshot file I/O read (get_file_io_stats): the row is returned — the caller sees the deltas
    /// and the interval — but its latencies are null on the marker, a number on a measured or pre-v60 row.
    /// </summary>
    [Fact]
    public async Task LatestFileIoStats_ReportsNullLatency_OnTheUnknowableRow()
    {
        var t = Truncate(DateTime.UtcNow.AddMinutes(-5));

        await SeedFileIoAsync(t, reads: 0, writes: 0, stallRead: 0, stallWrite: 0, interval: 0, database: "AppDb", file: "AppDb_data");
        await SeedFileIoAsync(t, reads: 10, writes: 2, stallRead: 100, stallWrite: 10, interval: 60, database: "AppDb", file: "AppDb_log");
        await SeedFileIoAsync(t, reads: 4, writes: 0, stallRead: 20, stallWrite: 0, interval: null, database: "OldDb", file: "OldDb_data");

        var rows = await _dataService.GetLatestFileIoStatsAsync(ServerId);
        var byFile = rows.ToDictionary(r => r.FileName);

        Assert.True(byFile["AppDb_data"].IsUnknowable);
        Assert.Null(byFile["AppDb_data"].AvgReadLatencyMs);
        Assert.Null(byFile["AppDb_data"].AvgWriteLatencyMs);
        Assert.Equal(0, byFile["AppDb_data"].SampleIntervalSeconds);

        Assert.Equal(10.0, byFile["AppDb_log"].AvgReadLatencyMs);
        Assert.Equal(5.0, byFile["AppDb_log"].AvgWriteLatencyMs);
        Assert.Equal(60, byFile["AppDb_log"].SampleIntervalSeconds);

        /* Pre-v60: NULL interval is not the marker; the row keeps its stall/op reading. */
        Assert.False(byFile["OldDb_data"].IsUnknowable);
        Assert.Null(byFile["OldDb_data"].SampleIntervalSeconds);
        Assert.Equal(5.0, byFile["OldDb_data"].AvgReadLatencyMs);
    }

    /// <summary>The latch and spinlock trends share the idiom with their own partition columns.</summary>
    [Fact]
    public async Task LatchAndSpinlockTrends_DropTheUnknowableRow_PreferTheStoredInterval()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);
        var t4 = t3.AddMinutes(5);

        await SeedLatchAsync(t1, deltaWait: 100, interval: null);
        await SeedLatchAsync(t2, deltaWait: 300, interval: null);
        await SeedLatchAsync(t3, deltaWait: 0, interval: 0);
        await SeedLatchAsync(t4, deltaWait: 600, interval: 120);

        var latch = await _dataService.GetLatchStatsTrendAsync(ServerId, hoursBack: 3);
        Assert.Equal(new[] { t2, t4 }, latch.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(1.0, latch[0].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(5.0, latch[1].WaitTimeMsPerSecond, precision: 6);

        await SeedSpinlockAsync(t1, deltaCollisions: 30, interval: null);
        await SeedSpinlockAsync(t2, deltaCollisions: 600, interval: null);
        await SeedSpinlockAsync(t3, deltaCollisions: 0, interval: 0);
        await SeedSpinlockAsync(t4, deltaCollisions: 1200, interval: 120);

        var spin = await _dataService.GetSpinlockStatsTrendAsync(ServerId, hoursBack: 3);
        Assert.Equal(new[] { t2, t4 }, spin.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(2.0, spin[0].CollisionsPerSecond, precision: 6);
        Assert.Equal(10.0, spin[1].CollisionsPerSecond, precision: 6);
    }

    /// <summary>
    /// #3653 A7: the latch and spinlock SNAPSHOT grids, which showed the (0, 0) restart row as "Δ 0" beside
    /// real deltas. Three contenders at one collection: the marker (interval 0) — its deltas come back null so
    /// the grid renders "—" and the Interval cell says why; a measured row (120 s) — deltas as stored, the
    /// interval as a number; a pre-v60 row (NULL) — deltas as stored, "not stored" in the Interval cell, never
    /// mistaken for the marker. The marker is seeded with a non-zero <c>delta_waiting_requests_count</c> (the
    /// helper's 1) so the pin proves the INTERVAL decides, not a zero delta. The MCP twin that shares the row
    /// (<c>get_latch_stats</c>) is pinned in <c>McpPageContractTests</c>.
    /// </summary>
    [Fact]
    public async Task LatchAndSpinlockSnapshots_NullTheDeltasOnTheMarkerRow_KeepMeasuredAndPreV60Deltas()
    {
        var t = Truncate(DateTime.UtcNow.AddMinutes(-2));

        await SeedLatchAsync(t, deltaWait: 0, interval: 0, latchClass: "BUFFER");
        await SeedLatchAsync(t, deltaWait: 600, interval: 120, latchClass: "LOG_MANAGER");
        await SeedLatchAsync(t, deltaWait: 300, interval: null, latchClass: "ACCESS_METHODS_DATASET_PARENT");

        var latches = (await _dataService.GetLatchStatsSnapshotAsync(ServerId, hoursBack: 1)).ToDictionary(r => r.LatchClass);
        Assert.Equal(3, latches.Count);

        var marker = latches["BUFFER"];
        Assert.Equal(0, marker.SampleIntervalSeconds);
        Assert.True(marker.IsUnknowable);
        Assert.Null(marker.DeltaWaitTimeMs);
        Assert.Null(marker.DeltaWaitingRequestsCount);
        Assert.Equal("restart / first sample", marker.IntervalDisplay);

        var measured = latches["LOG_MANAGER"];
        Assert.Equal(120, measured.SampleIntervalSeconds);
        Assert.False(measured.IsUnknowable);
        Assert.Equal(600L, measured.DeltaWaitTimeMs);
        Assert.Equal(1L, measured.DeltaWaitingRequestsCount);
        Assert.Equal("120", measured.IntervalDisplay);

        var preV60 = latches["ACCESS_METHODS_DATASET_PARENT"];
        Assert.Null(preV60.SampleIntervalSeconds);
        Assert.False(preV60.IsUnknowable);
        Assert.Equal(300L, preV60.DeltaWaitTimeMs);
        Assert.Equal("not stored", preV60.IntervalDisplay);

        await SeedSpinlockAsync(t, deltaCollisions: 0, interval: 0, spinlockName: "LOCK_HASH");
        await SeedSpinlockAsync(t, deltaCollisions: 1200, interval: 120, spinlockName: "SOS_CACHESTORE");
        await SeedSpinlockAsync(t, deltaCollisions: 30, interval: null, spinlockName: "XDESMGR");

        var spinlocks = (await _dataService.GetSpinlockStatsSnapshotAsync(ServerId, hoursBack: 1)).ToDictionary(r => r.SpinlockName);
        Assert.Equal(3, spinlocks.Count);

        Assert.True(spinlocks["LOCK_HASH"].IsUnknowable);
        Assert.Null(spinlocks["LOCK_HASH"].DeltaCollisions);
        Assert.Null(spinlocks["LOCK_HASH"].DeltaSpins);
        Assert.Equal("restart / first sample", spinlocks["LOCK_HASH"].IntervalDisplay);

        Assert.Equal(1200L, spinlocks["SOS_CACHESTORE"].DeltaCollisions);
        Assert.Equal(0L, spinlocks["SOS_CACHESTORE"].DeltaSpins);   // a measured zero stays a zero
        Assert.Equal("120", spinlocks["SOS_CACHESTORE"].IntervalDisplay);

        Assert.Equal(30L, spinlocks["XDESMGR"].DeltaCollisions);
        Assert.Null(spinlocks["XDESMGR"].SampleIntervalSeconds);
        Assert.Equal("not stored", spinlocks["XDESMGR"].IntervalDisplay);
    }

    /// <summary>
    /// #3540 (v61): the procedure duration trend, the read that LAG-divided procedure_stats' fabricated
    /// zero into a confident 0.00 ms/sec. Four collections five minutes apart: t1/t2 are pre-v61 collections
    /// (NULL interval) — t1 has no prior and is UNRATED (a point with null rates, #3541 A12: kept rather than
    /// dropped so a lone collection is never an empty series and the MCP payload's effective_start is the
    /// first collection the store held), t2 divides by the LAG's 300 s. t3 is a restart: every row stores 0,
    /// so MAX is 0 and the collection is likewise unrated — never 0.00 ms/sec. t4 is a steady pass with a plan
    /// the TOP (150) just readmitted (its row stores 0 beside a 0 delta) beside a measured row (120 s), so MAX
    /// is 120 — the stored interval wins over the LAG's 300 — and the readmitted plan adds nothing to the sums.
    /// </summary>
    [Fact]
    public async Task ProcedureDurationTrend_LeavesTheUnknowableCollectionUnrated_PrefersTheStoredInterval_KeepsPreV61History()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);
        var t4 = t3.AddMinutes(5);

        await SeedProcedureAsync(t1, "usp_A", deltaExecutions: 5, deltaElapsedUs: 100_000, interval: null);
        await SeedProcedureAsync(t2, "usp_A", deltaExecutions: 30, deltaElapsedUs: 600_000, interval: null);
        await SeedProcedureAsync(t3, "usp_A", deltaExecutions: 0, deltaElapsedUs: 0, interval: 0);
        await SeedProcedureAsync(t4, "usp_A", deltaExecutions: 24, deltaElapsedUs: 1_200_000, interval: 120);
        await SeedProcedureAsync(t4, "usp_New", deltaExecutions: 0, deltaElapsedUs: 0, interval: 0);

        var points = await _dataService.GetProcedureDurationTrendAsync(ServerId, hoursBack: 3);

        Assert.Equal(new[] { t1, t2, t3, t4 }, points.Select(p => p.CollectionTime).ToArray());

        /* t1 (no prior) and t3 (restart marker): present, unrated — null, never 0. */
        Assert.False(points[0].HasRate);
        Assert.Null(points[0].Value);
        Assert.Null(points[0].ExecutionsPerSecond);
        Assert.False(points[2].HasRate);
        Assert.Null(points[2].Value);
        Assert.Null(points[2].ExecutionCount);

        /* t2 (pre-v61): 600 ms / 300 s = 2.0 ms/sec; 30 / 300 = 0.1 executions/sec. */
        Assert.Equal(2.0, points[1].Value!.Value, precision: 6);
        Assert.Equal(0.1, points[1].ExecutionsPerSecond!.Value, precision: 6);

        /* t4: the STORED 120 s — 1200 / 120 = 10.0, not the LAG's 1200 / 300 = 4.0; 24 / 120 = 0.2. */
        Assert.Equal(10.0, points[3].Value!.Value, precision: 6);
        Assert.Equal(0.2, points[3].ExecutionsPerSecond!.Value, precision: 6);
    }

    /// <summary>
    /// #3653 (A11): the query duration trend — the read that LAG-recomputed an interval <c>query_stats</c> has
    /// stored from its first schema, and so divided a restart row's fabricated 0 delta by the real elapsed
    /// seconds into a confident 0.00 ms/sec. The procedure fixture's four collections, on query_stats: t1/t2
    /// pre-v61 (NULL interval) — t1 has no prior and is UNRATED, t2 divides by the LAG's 300 s; t3 a restart
    /// (every row 0) — unrated, where the LAG-only read said 0.00; t4 a steady pass with a readmitted plan (0)
    /// beside a measured 120 s row — the STORED 120 wins over the LAG's 300, and the readmitted plan adds
    /// nothing to the sums. Same rows, same expectations as the procedure test, on purpose: the two reads are
    /// one idiom now.
    /// </summary>
    [Fact]
    public async Task QueryDurationTrend_LeavesTheUnknowableCollectionUnrated_PrefersTheStoredInterval_KeepsPreV61History()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);
        var t4 = t3.AddMinutes(5);

        await SeedQueryStatAsync(t1, "0xA", deltaExecutions: 5, deltaElapsedUs: 100_000, interval: null);
        await SeedQueryStatAsync(t2, "0xA", deltaExecutions: 30, deltaElapsedUs: 600_000, interval: null);
        await SeedQueryStatAsync(t3, "0xA", deltaExecutions: 0, deltaElapsedUs: 0, interval: 0);
        await SeedQueryStatAsync(t4, "0xA", deltaExecutions: 24, deltaElapsedUs: 1_200_000, interval: 120);
        await SeedQueryStatAsync(t4, "0xNEW", deltaExecutions: 0, deltaElapsedUs: 0, interval: 0);

        var points = await _dataService.GetQueryDurationTrendAsync(ServerId, hoursBack: 3);

        Assert.Equal(new[] { t1, t2, t3, t4 }, points.Select(p => p.CollectionTime).ToArray());

        /* t1 (no prior) and t3 (restart marker): present, unrated — null, never 0. */
        Assert.False(points[0].HasRate);
        Assert.Null(points[0].Value);
        Assert.Null(points[0].ExecutionsPerSecond);
        Assert.False(points[2].HasRate);
        Assert.Null(points[2].Value);
        Assert.Null(points[2].ExecutionCount);

        /* t2 (pre-v61): 600 ms / 300 s = 2.0 ms/sec; 30 / 300 = 0.1 executions/sec. */
        Assert.Equal(2.0, points[1].Value!.Value, precision: 6);
        Assert.Equal(0.1, points[1].ExecutionsPerSecond!.Value, precision: 6);

        /* t4: the STORED 120 s — 1200 / 120 = 10.0, not the LAG's 1200 / 300 = 4.0; 24 / 120 = 0.2. */
        Assert.Equal(10.0, points[3].Value!.Value, precision: 6);
        Assert.Equal(0.2, points[3].ExecutionsPerSecond!.Value, precision: 6);
    }

    /// <summary>
    /// #3653 (A11): the execution-count trend is the duration trend's executions column on its own and reads
    /// the interval the same way. Same fixture: t1 unrated (no prior), t2 the LAG's 300 s (30 / 300 = 0.1),
    /// t3 the restart marker — unrated, where the LAG-only read published 0.00 executions/sec — and t4 the
    /// stored 120 s (24 / 120 = 0.2, not the LAG's 24 / 300 = 0.08).
    /// </summary>
    [Fact]
    public async Task ExecutionCountTrend_LeavesTheUnknowableCollectionUnrated_PrefersTheStoredInterval_KeepsPreV61History()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);
        var t4 = t3.AddMinutes(5);

        await SeedQueryStatAsync(t1, "0xE", 5, 100_000, interval: null);
        await SeedQueryStatAsync(t2, "0xE", 30, 600_000, interval: null);
        await SeedQueryStatAsync(t3, "0xE", 0, 0, interval: 0);
        await SeedQueryStatAsync(t4, "0xE", 24, 1_200_000, interval: 120);
        await SeedQueryStatAsync(t4, "0xNEW", 0, 0, interval: 0);

        var points = await _dataService.GetExecutionCountTrendAsync(ServerId, hoursBack: 3);

        Assert.Equal(new[] { t1, t2, t3, t4 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Null(points[0].Value);
        Assert.Equal(0.1, points[1].Value!.Value, precision: 6);
        Assert.Null(points[2].Value);
        Assert.Equal(0.2, points[3].Value!.Value, precision: 6);
    }

    /// <summary>
    /// #3540 (v61): the procedure history grid's "Interval (sec)" column shows the row's STORED interval where
    /// it has one — the marker's 0 INCLUDED, which is what the query-stats history has always shown for an
    /// unknowable row (a displayed interval is not a rate, so 0 is honest here where it would be a lie in a
    /// division) — and the LAG-derived gap for a pre-v61 row that never recorded one.
    /// </summary>
    [Fact]
    public async Task ProcedureHistory_ShowsTheStoredIntervalIncludingTheMarker_DerivesOnlyForPreV61Rows()
    {
        var t1 = Truncate(DateTime.UtcNow.AddHours(-2));
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);
        var t4 = t3.AddMinutes(5);

        await SeedProcedureAsync(t1, "usp_H", 5, 100_000, interval: null);
        await SeedProcedureAsync(t2, "usp_H", 30, 600_000, interval: null);
        await SeedProcedureAsync(t3, "usp_H", 0, 0, interval: 0);
        await SeedProcedureAsync(t4, "usp_H", 24, 1_200_000, interval: 120);

        var rows = await _dataService.GetProcedureStatsHistoryAsync(ServerId, "AppDb", "dbo", "usp_H", hoursBack: 3);

        Assert.Equal(new[] { t1, t2, t3, t4 }, rows.Select(r => r.CollectionTime).ToArray());
        Assert.Null(rows[0].SampleIntervalSeconds);      /* pre-v61, no prior: nothing to derive from */
        Assert.Equal(300, rows[1].SampleIntervalSeconds); /* pre-v61: the LAG gap */
        Assert.Equal(0, rows[2].SampleIntervalSeconds);   /* the marker, shown as the 0 it is */
        Assert.Equal(120, rows[3].SampleIntervalSeconds); /* stored, not the LAG's 300 */
    }

    /// <summary>
    /// #3540 (v61): the resource-semaphore snapshot carries the stored interval and <c>IsUnknowable</c> is
    /// true ONLY for the marker — never for a pre-v61 NULL, which is "never recorded" rather than
    /// "unknowable". Three semaphores at the latest collection: a marker, a measured row, a pre-v61 row.
    /// </summary>
    [Fact]
    public async Task ResourceSemaphoreSnapshot_CarriesTheStoredInterval_UnknowableOnlyForTheMarker()
    {
        var t = Truncate(DateTime.UtcNow.AddMinutes(-2));

        await SeedMemoryGrantAsync(t, poolId: 1, interval: 0);
        await SeedMemoryGrantAsync(t, poolId: 2, interval: 120);
        await SeedMemoryGrantAsync(t, poolId: 3, interval: null);

        var rows = await _dataService.GetResourceSemaphoreSnapshotAsync(ServerId, hoursBack: 1);
        var byPool = rows.ToDictionary(r => r.PoolId);
        Assert.Equal(3, byPool.Count);

        Assert.Equal(0, byPool[1].SampleIntervalSeconds);
        Assert.True(byPool[1].IsUnknowable);
        Assert.Equal(120, byPool[2].SampleIntervalSeconds);
        Assert.False(byPool[2].IsUnknowable);
        Assert.Null(byPool[3].SampleIntervalSeconds);
        Assert.False(byPool[3].IsUnknowable);
    }

    /* ---- seeding ---------------------------------------------------------------------------------------- */

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private static object IntervalValue(int? interval) => interval.HasValue ? interval.Value : DBNull.Value;

    private async Task SeedWaitAsync(DateTime at, string waitType, long deltaMs, long deltaSignal, long deltaTasks, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $8, $9)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, waitType, deltaTasks, deltaMs, deltaSignal, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedFileIoAsync(DateTime at, long reads, long writes, long stallRead, long stallWrite, int? interval,
        string database = "AppDb", string file = "AppDb_data")
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
             sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, $6, 'ROWS', '', 100, $7, $8, 0, 0, $9, $10, $11)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, database, file, reads, writes, stallRead, stallWrite, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureAsync(DateTime at, string objectName, long deltaExecutions, long deltaElapsedUs, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO procedure_stats
            (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
             execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes,
             delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, 'AppDb', 'dbo', $5, 'PROCEDURE', 0, 0, 0, 0, 0, 0, $6, 0, $7, $8)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, objectName, deltaExecutions, deltaElapsedUs, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStatAsync(DateTime at, string queryHash, long deltaExecutions, long deltaElapsedUs, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_stats
            (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
             execution_count, total_worker_time, total_elapsed_time,
             delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, 'AppDb', $5, '0xPLAN', '0xSQL', '0xPLANH', 0, 0, 0, $6, 0, $7, $8)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, queryHash, deltaExecutions, deltaElapsedUs, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedMemoryGrantAsync(DateTime at, int poolId, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_grant_stats
            (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id,
             target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb,
             grantee_count, waiter_count, timeout_error_count, forced_grant_count,
             timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, 0, $5, 100, 200, 90, 80, 10, 8, 3, 1, 5, 2, 0, 0, $6)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, poolId, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedLatchAsync(DateTime at, long deltaWait, int? interval, string latchClass = "BUFFER")
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO latch_stats
            (collection_id, collection_time, server_id, server_name, latch_class,
             waiting_requests_count, wait_time_ms, max_wait_time_ms,
             delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $7, 0, 0, 0, 1, $5, 0, $6)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, deltaWait, IntervalValue(interval), latchClass })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedSpinlockAsync(DateTime at, long deltaCollisions, int? interval, string spinlockName = "LOCK_HASH")
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO spinlock_stats
            (collection_id, collection_time, server_id, server_name, spinlock_name,
             collisions, spins, spins_per_collision, sleep_time, backoffs,
             delta_collisions, delta_spins, delta_sleep_time, delta_backoffs, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $7, 0, 0, 0, 0, 0, $5, 0, 0, 0, $6)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, deltaCollisions, IntervalValue(interval), spinlockName })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}

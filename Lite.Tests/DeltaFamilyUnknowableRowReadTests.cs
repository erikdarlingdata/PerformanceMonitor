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

    private async Task SeedLatchAsync(DateTime at, long deltaWait, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO latch_stats
            (collection_id, collection_time, server_id, server_name, latch_class,
             waiting_requests_count, wait_time_ms, max_wait_time_ms,
             delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, 'BUFFER', 0, 0, 0, 1, $5, 0, $6)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, deltaWait, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedSpinlockAsync(DateTime at, long deltaCollisions, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO spinlock_stats
            (collection_id, collection_time, server_id, server_name, spinlock_name,
             collisions, spins, spins_per_collision, sleep_time, backoffs,
             delta_collisions, delta_spins, delta_sleep_time, delta_backoffs, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, 'LOCK_HASH', 0, 0, 0, 0, 0, $5, 0, 0, 0, $6)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, deltaCollisions, IntervalValue(interval) })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}

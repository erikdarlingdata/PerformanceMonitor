/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4234: the three Overview correlated-lane reads — <see cref="LocalDataService.GetCpuUtilizationAsync"/>,
/// <see cref="LocalDataService.GetTotalWaitTrendAsync"/>, <see cref="LocalDataService.GetMemoryTrendAsync"/> —
/// now bucket in DuckDB to <see cref="TrendBudget.Chart"/>'s point budget instead of shipping one row per
/// collection. Four claims, proved end to end against a real DuckDB through <see cref="LocalDataService"/>:
/// each statement carries a bucket width; a 7-day window stays within budget where the raw row count alone
/// would not; a window narrow enough that every bucket holds exactly one physical collection renders
/// byte-identical to the pre-#4234 per-collection read; and the wait lane still drops an isolated unrated
/// collection rather than rendering it as a 0.
/// </summary>
public sealed class OverviewLaneBucketingTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4234;
    private const string ServerName = "overview-lane-bucket-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public OverviewLaneBucketingTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>Source check (no live DuckDB needed): each bucketed statement's text names <c>time_bucket</c>,
    /// so a future edit that quietly reverts one read to per-collection SQL fails here first.</summary>
    [Fact]
    public void BucketedTrendSql_CarriesABucketWidth()
    {
        Assert.Contains("time_bucket", LocalDataService.CpuUtilizationTrendSql);
        Assert.Contains("time_bucket", LocalDataService.MemoryTrendSql);
        Assert.Contains("time_bucket", LocalDataService.TotalWaitTrendSql(string.Empty));
    }

    /// <summary>
    /// 2,016 collections (5-minute cadence over 7 days) per lane — more than <see cref="TrendBuckets.ChartPointBudget"/>
    /// (1,500), so an unbucketed read would have blown the budget. All three lanes stay at or under it.
    /// </summary>
    [Fact]
    public async Task SevenDayWindow_StaysWithinChartBudget_ForAllThreeLanes()
    {
        var windowStart = Truncate(DateTime.UtcNow).AddDays(-7).AddHours(1);
        const int days = 7;
        const int cadenceMinutes = 5;
        const int perDay = 1440 / cadenceMinutes;
        Assert.True(days * perDay > TrendBuckets.ChartPointBudget, "the fixture must seed more raw rows than the budget to prove bucketing matters");

        await BulkSeedCpuAsync(windowStart, days, perDay, cadenceMinutes);
        await BulkSeedMemoryAsync(windowStart, days, perDay, cadenceMinutes);
        await BulkSeedWaitAsync(windowStart, days, perDay, cadenceMinutes);

        var cpu = await _dataService.GetCpuUtilizationAsync(ServerId, hoursBack: 24 * 7, utcOffsetMinutes: 0);
        var memory = await _dataService.GetMemoryTrendAsync(ServerId, hoursBack: 24 * 7);
        var wait = await _dataService.GetTotalWaitTrendAsync(ServerId, hoursBack: 24 * 7);

        Assert.True(cpu.Count > 0 && cpu.Count <= TrendBudget.Chart.AutoPoints, $"CPU returned {cpu.Count} points");
        Assert.True(memory.Count > 0 && memory.Count <= TrendBudget.Chart.AutoPoints, $"Memory returned {memory.Count} points");
        Assert.True(wait.Count > 0 && wait.Count <= TrendBudget.Chart.AutoPoints, $"Wait returned {wait.Count} points");
    }

    /// <summary>
    /// Three collections five minutes apart, seeded at :37 seconds past the minute (off the bucket grid) so
    /// the fixture cannot pass by accident. A 3-hour window auto-sizes to a 1-minute bucket
    /// (<c>TrendBuckets.AutoMinutes</c>'s narrowest rung), and a 5-minute gap never shares a 1-minute bucket,
    /// so every bucket holds exactly one physical collection and each lane must stamp its point at that
    /// collection's OWN clock and value — not the bucket grid line — reproducing the pre-#4234 per-collection
    /// read exactly.
    /// </summary>
    [Fact]
    public async Task SingletonBuckets_MatchThePerCollectionReadExactly_ForAllThreeLanes()
    {
        var anchor = DateTime.UtcNow.AddHours(-2);
        var t1 = new DateTime(anchor.Year, anchor.Month, anchor.Day, anchor.Hour, anchor.Minute, 37, DateTimeKind.Unspecified);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedCpuAsync(t1, sqlCpu: 40, otherCpu: 5);
        await SeedCpuAsync(t2, sqlCpu: 55, otherCpu: 6);
        await SeedCpuAsync(t3, sqlCpu: 70, otherCpu: 7);

        await SeedMemoryAsync(t1, total: 40000, target: 39000, buffer: 30000, plan: 4000);
        await SeedMemoryAsync(t2, total: 41000, target: 39000, buffer: 31000, plan: 4100);
        await SeedMemoryAsync(t3, total: 42000, target: 39000, buffer: 32000, plan: 4200);

        await SeedWaitAsync(t1, "CXPACKET", deltaMs: 3000, deltaSignal: 300, deltaTasks: 10, interval: 300);
        await SeedWaitAsync(t2, "CXPACKET", deltaMs: 1500, deltaSignal: 150, deltaTasks: 5, interval: 300);
        await SeedWaitAsync(t3, "CXPACKET", deltaMs: 6000, deltaSignal: 600, deltaTasks: 20, interval: 300);

        var cpu = await _dataService.GetCpuUtilizationAsync(ServerId, hoursBack: 3, utcOffsetMinutes: 0);
        Assert.Equal(new[] { t1, t2, t3 }, cpu.Select(r => r.SampleTime).ToArray());
        Assert.Equal(new[] { 40, 55, 70 }, cpu.Select(r => r.SqlServerCpu).ToArray());
        Assert.Equal(new[] { 5, 6, 7 }, cpu.Select(r => r.OtherProcessCpu).ToArray());

        var memory = await _dataService.GetMemoryTrendAsync(ServerId, hoursBack: 3);
        Assert.Equal(new[] { t1, t2, t3 }, memory.Select(r => r.CollectionTime).ToArray());
        Assert.Equal(new[] { 40000.0, 41000.0, 42000.0 }, memory.Select(r => r.TotalServerMemoryMb).ToArray());
        Assert.Equal(new[] { 30000.0, 31000.0, 32000.0 }, memory.Select(r => r.BufferPoolMb).ToArray());

        var wait = await _dataService.GetTotalWaitTrendAsync(ServerId, hoursBack: 3);
        Assert.Equal(new[] { t1, t2, t3 }, wait.Select(r => r.CollectionTime).ToArray());
        /* 3000/300=10.0, 1500/300=5.0, 6000/300=20.0 */
        Assert.Equal(new[] { 10.0, 5.0, 20.0 }, wait.Select(r => r.WaitTimeMsPerSecond).ToArray());
    }

    /// <summary>
    /// #3540 still holds post-bucketing: a collection where every wait type stored a 0 interval (a restart) is
    /// unrated, and when it sits alone in its own bucket (isolated by 5 minutes from its neighbors, same as the
    /// singleton test above) the bucket has no rated row at all — <c>HAVING COUNT(rated_seconds) &gt; 0</c> drops
    /// it, so it stays ABSENT rather than rendering as a 0.00 ms/sec point.
    /// </summary>
    [Fact]
    public async Task TotalWaitTrend_Bucketed_DropsAnIsolatedUnratedCollection()
    {
        var anchor = DateTime.UtcNow.AddHours(-2);
        var t1 = new DateTime(anchor.Year, anchor.Month, anchor.Day, anchor.Hour, anchor.Minute, 37, DateTimeKind.Unspecified);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedWaitAsync(t1, "CXPACKET", deltaMs: 3000, deltaSignal: 300, deltaTasks: 10, interval: 300);
        await SeedWaitAsync(t2, "CXPACKET", deltaMs: 0, deltaSignal: 0, deltaTasks: 0, interval: 0);
        await SeedWaitAsync(t3, "CXPACKET", deltaMs: 1500, deltaSignal: 150, deltaTasks: 5, interval: 300);

        var points = await _dataService.GetTotalWaitTrendAsync(ServerId, hoursBack: 3);

        Assert.Equal(new[] { t1, t3 }, points.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(10.0, points[0].WaitTimeMsPerSecond, precision: 6);
        Assert.Equal(5.0, points[1].WaitTimeMsPerSecond, precision: 6);
    }

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

    private async Task SeedCpuAsync(DateTime sampleTime, int sqlCpu, int otherCpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1, $2, $3, $4, $5, $6, $7)";
        foreach (var v in new object[] { _nextId--, DateTime.UtcNow, ServerId, ServerName, sampleTime, sqlCpu, otherCpu })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedMemoryAsync(DateTime collectionTime, double total, double target, double buffer, double plan)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_stats
            (collection_id, collection_time, server_id, server_name,
             total_physical_memory_mb, available_physical_memory_mb,
             target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, plan_cache_mb)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        foreach (var v in new object[] { _nextId--, collectionTime, ServerId, ServerName, 65536.0, 8192.0, target, total, buffer, plan })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync();
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

    /// <summary>Bulk-inserts <paramref name="days"/> * <paramref name="perDay"/> CPU rows in ONE statement
    /// (a C# loop of thousands of round trips would dominate the test's time) at
    /// <paramref name="cadenceMinutes"/> apart, starting at <paramref name="windowStart"/>.</summary>
    private async Task BulkSeedCpuAsync(DateTime windowStart, int days, int perDay, int cadenceMinutes)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT -900000000 - (d * {perDay} + i),
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' + (d * INTERVAL 1 DAY) + (i * INTERVAL {cadenceMinutes} MINUTE),
       {ServerId}, '{ServerName}',
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' + (d * INTERVAL 1 DAY) + (i * INTERVAL {cadenceMinutes} MINUTE),
       50, 10
FROM generate_series(0, {days - 1}) AS t1(d)
CROSS JOIN generate_series(0, {perDay - 1}) AS t2(i)";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task BulkSeedMemoryAsync(DateTime windowStart, int days, int perDay, int cadenceMinutes)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, plan_cache_mb)
SELECT -800000000 - (d * {perDay} + i),
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' + (d * INTERVAL 1 DAY) + (i * INTERVAL {cadenceMinutes} MINUTE),
       {ServerId}, '{ServerName}',
       65536.0, 8192.0, 39000.0, 40000.0, 30000.0, 4000.0
FROM generate_series(0, {days - 1}) AS t1(d)
CROSS JOIN generate_series(0, {perDay - 1}) AS t2(i)";
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task BulkSeedWaitAsync(DateTime windowStart, int days, int perDay, int cadenceMinutes)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT -700000000 - (d * {perDay} + i),
       TIMESTAMP '{windowStart:yyyy-MM-dd HH:mm:ss}' + (d * INTERVAL 1 DAY) + (i * INTERVAL {cadenceMinutes} MINUTE),
       {ServerId}, '{ServerName}', 'CXPACKET',
       0, 0, 0,
       10, 3000, 300, {cadenceMinutes * 60}
FROM generate_series(0, {days - 1}) AS t1(d)
CROSS JOIN generate_series(0, {perDay - 1}) AS t2(i)";
        await cmd.ExecuteNonQueryAsync();
    }
}

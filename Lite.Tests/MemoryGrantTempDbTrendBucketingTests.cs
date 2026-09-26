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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4349's own required tests for Lite's three bucketed reads (memory-grant overlay, Memory Grants
/// chart, TempDB usage), mirroring <see cref="LiteMemoryFileIoTrendBucketingLiveTests"/>'s shape: the
/// 7-day row-budget cap, the singleton raw-timestamp/no-averaging pin, and the merged-bucket
/// summed-not-mean pin for the grant chart's true deltas. Source checks for the bucket width /
/// first_collection_time / collection_count columns already existed before this PR
/// (<c>LiteMemoryFileIoTrendBucketWidthSqlTests</c>'s siblings do not cover these three statements, so
/// none needed updating here) — this file is the live-DuckDB half the PR body calls out as missing.
/// </summary>
[Collection("server-time-helper")]
public sealed class MemoryGrantTempDbTrendBucketingLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4349301;
    private const string ServerName = "memtempdb-bucketing-lite-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public MemoryGrantTempDbTrendBucketingLiveTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _savedUtcOffsetMinutes = ServerTimeHelper.UtcOffsetMinutes;
        ServerTimeHelper.UtcOffsetMinutes = 0;
    }

    public void Dispose()
    {
        ServerTimeHelper.UtcOffsetMinutes = _savedUtcOffsetMinutes;
        _seedConn?.Dispose();
    }

    [Fact]
    public async Task MemoryGrantTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 10, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedGrantAsync(start, end, poolId: 1);

        var trend = await _dataService.GetMemoryGrantTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints, $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
    }

    [Fact]
    public async Task MemoryGrantTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged()
    {
        var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedGrantAsync(t1, poolId: 1, grantedMb: 100, timeoutDelta: 0, forcedDelta: 0, interval: 60);
        await SeedGrantAsync(t2, poolId: 1, grantedMb: 200, timeoutDelta: 0, forcedDelta: 0, interval: 60);
        await SeedGrantAsync(t3, poolId: 1, grantedMb: 300, timeoutDelta: 0, forcedDelta: 0, interval: 60);

        var trend = await _dataService.GetMemoryGrantTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t3.AddMinutes(1));

        Assert.Equal(new[] { t1, t2, t3 }, trend.Select(p => p.CollectionTime).ToArray());
        Assert.Equal(new[] { 100.0, 200.0, 300.0 }, trend.Select(p => p.TotalGrantedMb).ToArray());
    }

    /// <summary>The merged-bucket guard: two collections of the same pool inside ONE bucket must SUM their
    /// timeout/forced deltas (7 and 3, not a mean); a separate, unrated (#3540) collection alone in its
    /// own bucket has its deltas nulled out of the sum (0, not a confident non-zero) while its sizing
    /// gauge still counts.</summary>
    [Fact]
    public async Task MemoryGrantChart_MergedBucket_SumsDeltas_UnratedCollectionNulledNotDropped()
    {
        var minute = new DateTime(2026, 3, 10, 11, 5, 0);
        var t1 = minute.AddSeconds(5);
        var t2 = minute.AddSeconds(45);

        await SeedGrantAsync(t1, poolId: 1, grantedMb: 100, timeoutDelta: 3, forcedDelta: 1, interval: 60);
        await SeedGrantAsync(t2, poolId: 1, grantedMb: 200, timeoutDelta: 4, forcedDelta: 2, interval: 60);

        var mergedRows = await _dataService.GetMemoryGrantChartDataAsync(ServerId, fromDate: minute.AddMinutes(-1), toDate: minute.AddMinutes(1));
        var merged = Assert.Single(mergedRows);
        Assert.Equal(150.0, merged.GrantedMemoryMb, precision: 3);
        Assert.Equal(7L, merged.TimeoutErrorCountDelta);
        Assert.Equal(3L, merged.ForcedGrantCountDelta);

        var unratedTime = new DateTime(2026, 3, 10, 12, 0, 0);
        await SeedGrantAsync(unratedTime, poolId: 1, grantedMb: 50, timeoutDelta: 99, forcedDelta: 99, interval: 0);

        var unratedRows = await _dataService.GetMemoryGrantChartDataAsync(ServerId, fromDate: unratedTime.AddMinutes(-1), toDate: unratedTime.AddMinutes(1));
        var unratedRow = Assert.Single(unratedRows);
        Assert.Equal(50.0, unratedRow.GrantedMemoryMb, precision: 3);
        Assert.Equal(0L, unratedRow.TimeoutErrorCountDelta);
        Assert.Equal(0L, unratedRow.ForcedGrantCountDelta);
    }

    [Fact]
    public async Task TempDbTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 11, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedTempDbAsync(start, end);

        var trend = await _dataService.GetTempDbTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints, $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
    }

    /// <summary>Ruling item 3 plus the top-session "last raw collection" rule: at a budget-covers-everything
    /// window, every point stamps its own raw collection time, and each collection's own
    /// top_session_id/mb pair rides through unchanged (nothing to merge, so arg_max just returns the one
    /// row it has).</summary>
    [Fact]
    public async Task TempDbTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndTopSessionUnchanged()
    {
        var t1 = new DateTime(2026, 3, 11, 9, 0, 37);
        var t2 = t1.AddMinutes(5);

        await SeedTempDbAsync(t1, totalMb: 100, topSessionId: 11, topSessionMb: 5.5);
        await SeedTempDbAsync(t2, totalMb: 200, topSessionId: 22, topSessionMb: 15.5);

        var trend = await _dataService.GetTempDbTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t2.AddMinutes(1));

        Assert.Equal(new[] { t1, t2 }, trend.Select(s => s.CollectionTime).ToArray());
        Assert.Equal(new[] { 100.0, 200.0 }, trend.Select(s => s.TotalReservedMb).ToArray());
        Assert.Equal(new[] { 11, 22 }, trend.Select(s => s.TopSessionId).ToArray());
        Assert.Equal(new[] { 5.5, 15.5 }, trend.Select(s => s.TopSessionTempDbMb).ToArray());
    }

    /* ---- seeding ---------------------------------------------------------------------------------------- */

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedGrantAsync(DateTime at, int poolId, double grantedMb, long timeoutDelta, long forcedDelta, int interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_grant_stats
            (collection_id, collection_time, server_id, server_name, pool_id,
             available_memory_mb, granted_memory_mb, used_memory_mb,
             grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, 900, $6, 80, 1, 0, $7, $8, $9)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, poolId, grantedMb, timeoutDelta, forcedDelta, interval })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task BulkSeedGrantAsync(DateTime start, DateTime end, int poolId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO memory_grant_stats
            (collection_id, collection_time, server_id, server_name, pool_id,
             available_memory_mb, granted_memory_mb, used_memory_mb,
             grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, $4, 900, 100, 80, 1, 0, 0, 0, 60
            FROM generate_series(CAST($5 AS TIMESTAMP), CAST($6 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, poolId, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }

    private async Task SeedTempDbAsync(DateTime at, double totalMb, int topSessionId, double topSessionMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO tempdb_stats
            (collection_id, collection_time, server_id, server_name,
             user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb,
             total_reserved_mb, unallocated_mb, total_sessions_using_tempdb,
             top_session_id, top_session_tempdb_mb)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 10, 3, $9, $10)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, totalMb / 2, totalMb / 4, totalMb / 4, totalMb, topSessionId, topSessionMb })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task BulkSeedTempDbAsync(DateTime start, DateTime end)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO tempdb_stats
            (collection_id, collection_time, server_id, server_name,
             user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb,
             total_reserved_mb, unallocated_mb, total_sessions_using_tempdb,
             top_session_id, top_session_tempdb_mb)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, 50, 25, 25, 100, 10, 3, 1, 5
            FROM generate_series(CAST($4 AS TIMESTAMP), CAST($5 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }
}

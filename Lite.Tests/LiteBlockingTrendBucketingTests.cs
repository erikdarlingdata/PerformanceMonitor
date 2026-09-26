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
/// #4349's Lite-twin tests for the blocking trio's wide-window bucketing (lock waits, waiting-task
/// duration, blocked-session count), mirroring <see cref="LiteMemoryFileIoTrendBucketWidthSqlTests"/>
/// (#4340) for the source checks and <see cref="LiteMemoryFileIoTrendBucketingLiveTests"/> for the
/// live-DuckDB budget cap / point equality / singleton-stamping tests. No live SQL Server or Postgres
/// needed — DuckDB is the embedded store itself.
/// </summary>
public sealed class LiteBlockingTrendBucketWidthSqlTests
{
    [Fact]
    public void LockWaitTrendSql_CarriesABucketWidth_AndKeepsThe3540RatedCteNoDeltaRule()
    {
        var sql = LocalDataService.LockWaitTrendSql;

        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, sql, StringComparison.Ordinal);

        /* #3540: an unrated row is NULLed into the rated CTE, not filtered from the FROM clause. */
        Assert.Contains("CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN delta_wait_time_ms END AS rated_wait_ms", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN interval_seconds END AS rated_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("HAVING COUNT(rated_seconds) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(rated_wait_ms) / SUM(rated_seconds) AS wait_time_ms_per_second", sql, StringComparison.Ordinal);
    }
}

/// <summary>The ruling's remaining tests (the 7-day row-budget cap, point equality when the budget covers
/// every collection, and the merged-bucket average-not-sum pin) against a real embedded DuckDB. Mirrors
/// <see cref="LiteMemoryFileIoTrendBucketingLiveTests"/> for the blocking trio the #4234/#4340 lanes left
/// untouched.</summary>
[Collection("server-time-helper")]
public sealed class LiteBlockingTrendBucketingLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4349301;
    private const string ServerName = "blocking-bucketing-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public LiteBlockingTrendBucketingLiveTests(SharedDuckDbFixture fixture)
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
    public async Task LockWaitTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 10, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedWaitStatAsync(start, end, "LCK_M_S");

        var rows = await _dataService.GetLockWaitTrendAsync(ServerId, fromDate: start, toDate: end);

        var budget = TrendBudget.Chart.AutoPoints;
        Assert.True(rows.Count > 0 && rows.Count <= budget, $"lock wait: {rows.Count} rows over a budget of {budget}");
        Assert.True(rows.Count < 10_079, "the unbucketed read would have returned nearly one row per minute");
    }

    [Fact]
    public async Task LockWaitTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged()
    {
        var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
        var t2 = t1.AddMinutes(5);
        var t3 = t2.AddMinutes(5);

        await SeedWaitStatAsync(t1, "LCK_M_S", deltaWaitTimeMs: 0, intervalSeconds: 0);
        await SeedWaitStatAsync(t2, "LCK_M_S", deltaWaitTimeMs: 6000, intervalSeconds: 60);
        await SeedWaitStatAsync(t3, "LCK_M_S", deltaWaitTimeMs: 12000, intervalSeconds: 60);

        var rows = await _dataService.GetLockWaitTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t3.AddMinutes(1));

        /* t1 is unrated (dropped by HAVING); t2/t3 survive at their own raw collection times. */
        Assert.Equal(new[] { t2, t3 }, rows.Select(r => r.CollectionTime).ToArray());
        Assert.Equal(new[] { 100.0, 200.0 }, rows.Select(r => r.WaitTimeMsPerSecond).ToArray());
    }

    [Fact]
    public async Task WaitingTaskTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 11, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedWaitingTaskAsync(start, end, blockingSessionId: 0);

        var rows = await _dataService.GetWaitingTaskTrendAsync(ServerId, fromDate: start, toDate: end);

        var budget = TrendBudget.Chart.AutoPoints;
        Assert.True(rows.Count > 0 && rows.Count <= budget, $"waiting task: {rows.Count} rows over a budget of {budget}");
        Assert.True(rows.Count < 10_080, "the unbucketed read would have returned one row per minute");
    }

    [Fact]
    public async Task WaitingTaskTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged()
    {
        var t1 = new DateTime(2026, 3, 11, 9, 0, 37);
        var t2 = t1.AddMinutes(5);

        await SeedWaitingTaskAsync(t1, "LCK_M_X", waitDurationMs: 100, blockingSessionId: 60, databaseName: "AppDb");
        await SeedWaitingTaskAsync(t2, "LCK_M_X", waitDurationMs: 200, blockingSessionId: 60, databaseName: "AppDb");

        var rows = await _dataService.GetWaitingTaskTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t2.AddMinutes(1));

        Assert.Equal(new[] { t1, t2 }, rows.Select(r => r.CollectionTime).ToArray());
        Assert.Equal(new[] { 100L, 200L }, rows.Select(r => r.TotalWaitMs).ToArray());
    }

    [Fact]
    public async Task BlockedSessionTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 12, 0, 0, 0);
        var start = end.AddDays(-7);
        await BulkSeedWaitingTaskAsync(start, end, blockingSessionId: 90);

        var rows = await _dataService.GetBlockedSessionTrendAsync(ServerId, fromDate: start, toDate: end);

        var budget = TrendBudget.Chart.AutoPoints;
        Assert.True(rows.Count > 0 && rows.Count <= budget, $"blocked session: {rows.Count} rows over a budget of {budget}");
        Assert.True(rows.Count < 10_080, "the unbucketed read would have returned one row per minute");
    }

    [Fact]
    public async Task BlockedSessionTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged()
    {
        var t1 = new DateTime(2026, 3, 12, 9, 0, 37);
        var t2 = t1.AddMinutes(5);

        /* t1: two blocked rows in AppDb. t2: one blocked row in AppDb. */
        await SeedWaitingTaskAsync(t1, "LCK_M_X", waitDurationMs: 100, blockingSessionId: 90, databaseName: "AppDb");
        await SeedWaitingTaskAsync(t1, "LCK_M_X", waitDurationMs: 100, blockingSessionId: 90, databaseName: "AppDb");
        await SeedWaitingTaskAsync(t2, "LCK_M_X", waitDurationMs: 100, blockingSessionId: 91, databaseName: "AppDb");

        var rows = await _dataService.GetBlockedSessionTrendAsync(ServerId, fromDate: t1.AddMinutes(-1), toDate: t2.AddMinutes(1));

        Assert.Equal(new[] { t1, t2 }, rows.Select(r => r.CollectionTime).ToArray());
        Assert.Equal(new[] { 2, 1 }, rows.Select(r => r.BlockedCount).ToArray());
    }

    /// <summary>The defect this lane found and fixed: two blocked-session collections merged into one
    /// bucket must AVERAGE the per-collection counts, never SUM them (which would double the count purely
    /// because the wide bucket merged two snapshots, with no more blocking having happened). Every
    /// collection in this fixture holds exactly 4 blocked rows, so a correct read reports 4 for every
    /// bucket regardless of how many collections a bucket merges; a SUM-based regression reports a
    /// multiple of 4 in any bucket that merges more than one.</summary>
    [Fact]
    public async Task BlockedSessionTrend_MergedBucket_AveragesThePerSnapshotCount_NeverSums()
    {
        var end = new DateTime(2026, 3, 13, 0, 0, 0);
        var start = end.AddDays(-7);

        await BulkSeedBlockedTaskAtIntervalAsync(start, end, intervalMinutes: 5, blockedRowsPerCollection: 4);

        var rows = await _dataService.GetBlockedSessionTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.Equal(4, r.BlockedCount));
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

    private async Task SeedWaitStatAsync(DateTime at, string waitType, long deltaWaitTimeMs, int? intervalSeconds)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, $5, 1, $6, $7)";
        foreach (var v in new object?[] { _nextId--, at, ServerId, ServerName, waitType, deltaWaitTimeMs, intervalSeconds })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One rated LCK_M_S collection per minute (60s stored interval), across the whole window.</summary>
    private async Task BulkSeedWaitStatAsync(DateTime start, DateTime end, string waitType)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, sample_interval_seconds)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, $4, 1, 6000, 60
            FROM generate_series(CAST($5 AS TIMESTAMP), CAST($6 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, waitType, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }

    private async Task SeedWaitingTaskAsync(DateTime at, string waitType, long waitDurationMs, int blockingSessionId, string databaseName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO waiting_tasks
            (collection_id, collection_time, server_id, server_name,
             session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, 55, waitType, waitDurationMs, blockingSessionId, "keylock", databaseName })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One row per minute across the whole window, blocking_session_id fixed per call
    /// (0 = unblocked for the waiting-task duration budget-cap test, non-zero for the blocked-session-count
    /// twin).</summary>
    private async Task BulkSeedWaitingTaskAsync(DateTime start, DateTime end, int blockingSessionId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO waiting_tasks
            (collection_id, collection_time, server_id, server_name,
             session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, 71, 'LCK_M_X', 100, $4, 'keylock', 'AppDb'
            FROM generate_series(CAST($5 AS TIMESTAMP), CAST($6 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, blockingSessionId, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }

    /// <summary>A fixed number of blocked rows in EVERY collection, spaced <paramref
    /// name="intervalMinutes"/> apart, across the whole window — the merged-bucket average test's seed.</summary>
    private async Task BulkSeedBlockedTaskAtIntervalAsync(DateTime start, DateTime end, int intervalMinutes, int blockedRowsPerCollection)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO waiting_tasks
            (collection_id, collection_time, server_id, server_name,
             session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t, s), t, $2, $3, 70 + s, 'LCK_M_X', 100, 90 + s, 'keylock', 'AppDb'
            FROM generate_series(CAST($4 AS TIMESTAMP), CAST($5 AS TIMESTAMP), ($6 || ' minutes')::INTERVAL) AS g(t)
            CROSS JOIN generate_series(1, $7) AS r(s)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, start, end, intervalMinutes, blockedRowsPerCollection })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= ((long)(end - start).TotalMinutes / intervalMinutes + 2) * blockedRowsPerCollection;
    }
}

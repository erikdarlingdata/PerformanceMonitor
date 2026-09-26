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
/// #4234/#4349: the Lite (DuckDB) twin of <c>Darling.Tests.ViewerCpuSessionsPlanCacheTrendBucketWidthSqlTests</c>
/// — the ruling's required source checks for <see cref="LocalDataService.GetCpuSchedulerTrendAsync"/>,
/// <see cref="LocalDataService.GetSessionStatsAsync"/> and <see cref="LocalDataService.GetPlanCacheTrendAsync"/>,
/// mirroring <c>LiteMemoryFileIoTrendBucketWidthSqlTests</c> (#4340). No live SQL Server or Postgres needed —
/// DuckDB is the embedded store itself.
/// <para>Proven once by hand against the pre-#4349 text: reverting the three SQL statements below to their
/// per-collection SELECTs (no <c>rated</c>/<c>agg</c> CTE, no <c>time_bucket</c>, no width parameter, no
/// <c>first_collection_time</c>/<c>collection_count</c> columns) fails every source check here directly.</para>
/// </summary>
public sealed class LiteCpuSessionsPlanCacheTrendBucketWidthSqlTests
{
    [Fact]
    public void CpuSchedulerTrendSql_CarriesABucketWidth_AndProjectsSingletonDetectionColumns()
    {
        var sql = LocalDataService.CpuSchedulerTrendSql;
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(COALESCE(total_runnable_tasks_count, 0))", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SessionStatsTrendSql_CarriesABucketWidth_AveragesGauges_AndTakesNewestForAttribution()
    {
        var sql = LocalDataService.SessionStatsTrendSql;
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);

        foreach (var column in new[]
        {
            "total_sessions", "running_sessions", "sleeping_sessions", "background_sessions",
            "dormant_sessions", "idle_sessions_over_30min", "sessions_waiting_for_memory",
            "databases_with_connections",
        })
        {
            Assert.Contains($"AVG(COALESCE({column}, 0))", sql, StringComparison.Ordinal);
        }

        /* DuckDB's QUALIFY ROW_NUMBER(), newest collection first, in place of PG's DISTINCT ON. */
        Assert.Contains("QUALIFY ROW_NUMBER() OVER (PARTITION BY bucket_start ORDER BY collection_time DESC) = 1", sql, StringComparison.Ordinal);
        Assert.Contains("top_application_name", sql, StringComparison.Ordinal);
        Assert.Contains("top_host_name", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCacheTrendSql_CarriesABucketWidth_AndProjectsSingletonDetectionColumns()
    {
        var sql = LocalDataService.PlanCacheTrendSql;
        Assert.Contains("time_bucket(to_minutes(CAST($4 AS INTEGER))", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBuckets.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(single_use_mb)", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(multi_use_mb)", sql, StringComparison.Ordinal);
    }
}

/// <summary>
/// The ruling's remaining tests against a real embedded DuckDB: the 7-day row-budget cap and point
/// equality (gauges averaged per bucket; a singleton bucket stamped at its own raw collection time;
/// Session Stats' attribution columns taking the bucket's newest collection per claude-desktop's ruling).
/// Mirrors <c>LiteMemoryFileIoTrendBucketingLiveTests</c> (#4340).
/// <para>fromDate/toDate is server-local (GetTimeRange converts it back to UTC through
/// ServerTimeHelper.UtcOffsetMinutes, a process-wide mutable static) — every fromDate/toDate this class
/// passes is meant as an exact UTC instant, so the offset is pinned to 0 for the class's lifetime. xUnit
/// runs test classes in parallel, so this joins the collection the other offset-touching classes use
/// rather than racing them.</para>
/// </summary>
[Collection("server-time-helper")]
public sealed class LiteCpuSessionsPlanCacheTrendBucketingLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4349301;
    private const string ServerName = "cpu-session-plancache-bucketing-e2e";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public LiteCpuSessionsPlanCacheTrendBucketingLiveTests(SharedDuckDbFixture fixture)
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
    public async Task CpuSchedulerTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 10, 0, 0, 0);
        var start = end.AddDays(-7);

        await BulkSeedCpuAsync(start, end);

        var trend = await _dataService.GetCpuSchedulerTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints,
            $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
    }

    [Fact]
    public async Task CpuSchedulerTrend_MergedBucket_AveragesGauges()
    {
        var end = new DateTime(2026, 3, 11, 0, 0, 0);
        var start = end.AddDays(-8);
        var t1 = start.AddHours(1);
        var t2 = t1.AddSeconds(60);

        await SeedCpuAsync(t1, runnable: 4, blocked: 2, queued: 10);
        await SeedCpuAsync(t2, runnable: 6, blocked: 4, queued: 20);

        var trend = await _dataService.GetCpuSchedulerTrendAsync(ServerId, fromDate: start, toDate: end);

        var point = Assert.Single(trend);
        Assert.Equal(5, point.RunnableTasks);
        Assert.Equal(3, point.BlockedTasks);
        Assert.Equal(15, point.QueuedRequests);
    }

    [Fact]
    public async Task SessionStatsTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 12, 0, 0, 0);
        var start = end.AddDays(-7);

        await BulkSeedSessionAsync(start, end);

        var trend = await _dataService.GetSessionStatsAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints,
            $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
    }

    /// <summary>claude-desktop's ruling addition: a bucket containing two collections carries the NEWER
    /// collection's app and host, not any blended or oldest answer. The gauges still average.</summary>
    [Fact]
    public async Task SessionStatsTrend_MergedBucket_AveragesGauges_AttributionColumnsTakeTheNewestCollection()
    {
        var end = new DateTime(2026, 3, 13, 0, 0, 0);
        var start = end.AddDays(-8);
        var t1 = start.AddHours(1);
        var t2 = t1.AddSeconds(60);

        await SeedSessionAsync(t1, total: 100, running: 5, topApp: "OlderApp", topAppConns: 40, topHost: "OlderHost", topHostConns: 30);
        await SeedSessionAsync(t2, total: 120, running: 9, topApp: "NewerApp", topAppConns: 55, topHost: "NewerHost", topHostConns: 45);

        var trend = await _dataService.GetSessionStatsAsync(ServerId, fromDate: start, toDate: end);

        var point = Assert.Single(trend);
        Assert.Equal(110, point.TotalSessions);
        Assert.Equal(7, point.RunningSessions);
        Assert.Equal("NewerApp", point.TopApplicationName);
        Assert.Equal(55, point.TopApplicationConnections);
        Assert.Equal("NewerHost", point.TopHostName);
        Assert.Equal(45, point.TopHostConnections);
    }

    [Fact]
    public async Task PlanCacheTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        var end = new DateTime(2026, 3, 14, 0, 0, 0);
        var start = end.AddDays(-7);

        await BulkSeedPlanCacheAsync(start, end);

        var trend = await _dataService.GetPlanCacheTrendAsync(ServerId, fromDate: start, toDate: end);

        Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints,
            $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
    }

    [Fact]
    public async Task PlanCacheTrend_MergedBucket_AveragesThePerCollectionSummedMb()
    {
        var end = new DateTime(2026, 3, 15, 0, 0, 0);
        var start = end.AddDays(-8);
        var t1 = start.AddHours(1);
        var t2 = t1.AddSeconds(60);

        await SeedPlanCacheAsync(t1, singleMb: 40, multiMb: 100);
        await SeedPlanCacheAsync(t2, singleMb: 60, multiMb: 200);

        var trend = await _dataService.GetPlanCacheTrendAsync(ServerId, fromDate: start, toDate: end);

        var point = Assert.Single(trend);
        Assert.Equal(50.0, point.SingleUseSizeMb, precision: 3);
        Assert.Equal(150.0, point.MultiUseSizeMb, precision: 3);
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

    private async Task SeedCpuAsync(DateTime at, int runnable, int blocked, int queued)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_scheduler_stats
            (collection_id, collection_time, server_id, server_name,
             max_workers_count, scheduler_count, cpu_count,
             total_runnable_tasks_count, total_work_queue_count, total_current_workers_count,
             avg_runnable_tasks_count, total_active_request_count, total_queued_request_count,
             total_blocked_task_count, total_active_parallel_thread_count,
             runnable_request_count, total_request_count, runnable_percent,
             worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning,
             queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb,
             system_memory_state_desc, physical_memory_pressure_warning,
             total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
            VALUES ($1, $2, $3, $4, 512, 8, 8, $5, 0, 128, 0.5, 2, $6, $7, 0, 3, 20, 12.5,
                    false, false, false, false, 67108864, 8388608, 'ok', false, 2, 2, 0, false)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, runnable, queued, blocked })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One row a minute across the whole window (10,080 rows for a 7-day window) — the
    /// budget-cap test's bulk seed.</summary>
    private async Task BulkSeedCpuAsync(DateTime start, DateTime end)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_scheduler_stats
            (collection_id, collection_time, server_id, server_name,
             max_workers_count, scheduler_count, cpu_count,
             total_runnable_tasks_count, total_work_queue_count, total_current_workers_count,
             avg_runnable_tasks_count, total_active_request_count, total_queued_request_count,
             total_blocked_task_count, total_active_parallel_thread_count,
             runnable_request_count, total_request_count, runnable_percent,
             worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning,
             queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb,
             system_memory_state_desc, physical_memory_pressure_warning,
             total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3,
                   512, 8, 8, 3, 0, 128, 0.5, 2, 5, 1, 0, 3, 20, 12.5,
                   false, false, false, false, 67108864, 8388608, 'ok', false, 2, 2, 0, false
            FROM generate_series(CAST($4 AS TIMESTAMP), CAST($5 AS TIMESTAMP), INTERVAL 1 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)(end - start).TotalMinutes + 2;
    }

    private async Task SeedSessionAsync(DateTime at, int total, int running, string topApp, int topAppConns, string topHost, int topHostConns)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO session_summary_stats
            (collection_id, collection_time, server_id, server_name,
             total_sessions, running_sessions, sleeping_sessions, background_sessions, dormant_sessions,
             idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
             top_application_name, top_application_connections, top_host_name, top_host_connections)
            VALUES ($1, $2, $3, $4, $5, $6, 0, 0, 0, 0, 0, 1, $7, $8, $9, $10)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, total, running, topApp, topAppConns, topHost, topHostConns })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One row every 5 minutes across the whole window (2,016 rows for a 7-day window) — the
    /// budget-cap test's bulk seed.</summary>
    private async Task BulkSeedSessionAsync(DateTime start, DateTime end)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO session_summary_stats
            (collection_id, collection_time, server_id, server_name,
             total_sessions, running_sessions, sleeping_sessions, background_sessions, dormant_sessions,
             idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
             top_application_name, top_application_connections, top_host_name, top_host_connections)
            SELECT $1 - ROW_NUMBER() OVER (ORDER BY t), t, $2, $3,
                   100, 5, 90, 3, 2, 10, 1, 7, 'App', 40, 'Host', 30
            FROM generate_series(CAST($4 AS TIMESTAMP), CAST($5 AS TIMESTAMP), INTERVAL 5 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= (long)((end - start).TotalMinutes / 5) + 2;
    }

    private async Task SeedPlanCacheAsync(DateTime at, int singleMb, int multiMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO plan_cache_stats
            (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
             total_plans, total_size_mb, single_use_plans, single_use_size_mb,
             multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
            VALUES ($1, $2, $3, $4, 'Compiled Plan', 'Adhoc', 10, $5 + $6, 5, $5, 5, $6, 1.5, 64, $2)";
        foreach (var v in new object[] { _nextId--, at, ServerId, ServerName, singleMb, multiMb })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One collection every 5 minutes across the whole window, two objtype groups each — the
    /// budget-cap test's bulk seed (2,016 collections over a 7-day window).</summary>
    private async Task BulkSeedPlanCacheAsync(DateTime start, DateTime end)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO plan_cache_stats
            (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
             total_plans, total_size_mb, single_use_plans, single_use_size_mb,
             multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
            SELECT $1 - 2 * ROW_NUMBER() OVER (ORDER BY t) + 1, t, $2, $3, 'Compiled Plan', 'Proc',
                   10, 140, 1, 40, 4, 100, 1.5, 64, t
            FROM generate_series(CAST($4 AS TIMESTAMP), CAST($5 AS TIMESTAMP), INTERVAL 5 MINUTE) AS s(t)
            UNION ALL
            SELECT $1 - 2 * ROW_NUMBER() OVER (ORDER BY t), t, $2, $3, 'Compiled Plan', 'Adhoc',
                   30, 210, 25, 10, 5, 200, 1.5, 64, t
            FROM generate_series(CAST($4 AS TIMESTAMP), CAST($5 AS TIMESTAMP), INTERVAL 5 MINUTE) AS s(t)";
        foreach (var v in new object[] { _nextId, ServerId, ServerName, start, end })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
        _nextId -= 2 * ((long)((end - start).TotalMinutes / 5) + 2);
    }
}

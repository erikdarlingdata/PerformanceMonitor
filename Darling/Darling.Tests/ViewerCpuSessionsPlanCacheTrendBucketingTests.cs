/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4234/#4349: the bucketing tests for the CPU scheduler, Session Stats and Plan Cache trend reads —
/// mirroring <see cref="ViewerCpuTempDbSqlTests"/> (#4353) and Lite's
/// <c>LiteMemoryFileIoTrendBucketWidthSqlTests</c>/<c>LiteMemoryFileIoTrendBucketingLiveTests</c> (#4340).
/// Each source check would fail against the pre-#4349 per-collection SELECTs (no <c>date_bin</c>, no
/// width parameter, no <c>first_collection_time</c>/<c>collection_count</c> columns) — proven once by
/// hand against origin/dev's copy of these three statements before this PR bucketed them. The live class
/// pins the 7-day row-budget cap and the point-equality rules the PR body promises: gauges averaged per
/// bucket, a singleton bucket stamped at its own raw collection time, and — per claude-desktop's ruling —
/// Session Stats' per-snapshot attribution columns (app/host name + counts) carry the bucket's NEWEST
/// collection rather than any blended answer.
/// </summary>
public sealed class ViewerCpuSessionsPlanCacheTrendBucketWidthSqlTests
{
    [Fact]
    public void CpuSchedulerTrendSql_CarriesABucketWidth_AndProjectsSingletonDetectionColumns()
    {
        var sql = ViewerDataService.CpuSchedulerTrendSql;
        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(COALESCE(total_runnable_tasks_count, 0))", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(COALESCE(total_blocked_task_count, 0))", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(COALESCE(total_queued_request_count, 0))", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SessionStatsSql_CarriesABucketWidth_AveragesGauges_AndTakesNewestForAttribution()
    {
        var sql = ViewerDataService.SessionStatsSql;
        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);

        /* The eight status/count gauges are averaged per bucket. */
        foreach (var column in new[]
        {
            "total_sessions", "running_sessions", "sleeping_sessions", "background_sessions",
            "dormant_sessions", "idle_sessions_over_30min", "sessions_waiting_for_memory",
            "databases_with_connections",
        })
        {
            Assert.Contains($"AVG(COALESCE({column}, 0))", sql, StringComparison.Ordinal);
        }

        /* The four per-snapshot attribution columns cannot be averaged: DISTINCT ON, newest collection
           first, per claude-desktop's ruling. */
        Assert.Contains("DISTINCT ON (bucket_start)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY bucket_start, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("top_application_name", sql, StringComparison.Ordinal);
        Assert.Contains("top_application_connections", sql, StringComparison.Ordinal);
        Assert.Contains("top_host_name", sql, StringComparison.Ordinal);
        Assert.Contains("top_host_connections", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCacheTrendSql_CarriesABucketWidth_AndProjectsSingletonDetectionColumns()
    {
        var sql = ViewerDataService.PlanCacheTrendSql;
        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(date_bin(", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(single_use_mb)", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(multi_use_mb)", sql, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips: the 7-day row-budget cap (bulk-seeded at each collector's real
/// cadence, matching the issue's own measured row counts — 10,080 for CPU scheduler's 1-minute cadence,
/// 2,016 for Session Stats/Plan Cache's 5-minute cadence) and point equality (a bucket's averaged value vs
/// the hand-computed mean of its collections; a singleton bucket stamped at its own raw collection time;
/// Session Stats' attribution columns carrying the newer of two collections in one bucket). Shares the
/// serialized "live-postgres" collection; uses negative sentinel server_ids and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerCpuSessionsPlanCacheTrendBucketingLiveTests
{
    private const int CpuServerId = -943491;
    private const string CpuServerName = "viewer-cpu-scheduler-bucketing-e2e";

    private const int SessionServerId = -943492;
    private const string SessionServerName = "viewer-session-stats-bucketing-e2e";

    private const int PlanCacheServerId = -943493;
    private const string PlanCacheServerName = "viewer-plan-cache-bucketing-e2e";

    [Fact]
    public async Task CpuSchedulerTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU-scheduler bucketing test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteCpuAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = TruncateToSeconds(DateTime.UtcNow.AddDays(-30));
            var start = end.AddDays(-7);

            /* One row a minute for 7 days = 10,080 rows — the issue's own measured pre-bucketing count. */
            await BulkSeedCpuAsync(connection, start, end);

            var trend = await viewer.GetCpuSchedulerTrendAsync(CpuServerId, start, end);

            Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints,
                $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteCpuAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task CpuSchedulerTrend_MergedBucket_AveragesGauges_SingletonBucket_StampsRawTime_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live CPU-scheduler bucketing test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteCpuAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* A window wide enough (8 days) that TrendBuckets.AutoMinutes picks a bucket wider than the
               60 seconds between t1/t2 below, so both collections land in the SAME bucket. */
            var end = TruncateToSeconds(DateTime.UtcNow.AddDays(-60));
            var start = end.AddDays(-8);
            var t1 = start.AddHours(1);
            var t2 = t1.AddSeconds(60);

            await InsertCpuAsync(connection, 1, t1, runnable: 4, blocked: 2, queued: 10);
            await InsertCpuAsync(connection, 2, t2, runnable: 6, blocked: 4, queued: 20);

            var trend = await viewer.GetCpuSchedulerTrendAsync(CpuServerId, start, end);

            var point = Assert.Single(trend);
            /* Gauges averaged per bucket: (4+6)/2 = 5, (2+4)/2 = 3, (10+20)/2 = 15. */
            Assert.Equal(5, point.RunnableTasks);
            Assert.Equal(3, point.BlockedTasks);
            Assert.Equal(15, point.QueuedRequests);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteCpuAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task SessionStatsTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Session-Stats bucketing test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteSessionAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = TruncateToSeconds(DateTime.UtcNow.AddDays(-30));
            var start = end.AddDays(-7);

            /* One row every 5 minutes for 7 days = 2,016 rows — the issue's own measured pre-bucketing count. */
            await BulkSeedSessionAsync(connection, start, end);

            var trend = await viewer.GetSessionStatsAsync(SessionServerId, start, end);

            Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints,
                $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSessionAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>claude-desktop's ruling addition: a bucket containing two collections carries the NEWER
    /// collection's app and host, not any blended or oldest answer. The gauges still average.</summary>
    [Fact]
    public async Task SessionStatsTrend_MergedBucket_AveragesGauges_AttributionColumnsTakeTheNewestCollection_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Session-Stats bucketing test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteSessionAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = TruncateToSeconds(DateTime.UtcNow.AddDays(-60));
            var start = end.AddDays(-8);
            var t1 = start.AddHours(1);
            var t2 = t1.AddSeconds(60);

            await InsertSessionAsync(connection, 1, t1, total: 100, running: 5,
                topApp: "OlderApp", topAppConns: 40, topHost: "OlderHost", topHostConns: 30);
            await InsertSessionAsync(connection, 2, t2, total: 120, running: 9,
                topApp: "NewerApp", topAppConns: 55, topHost: "NewerHost", topHostConns: 45);

            var trend = await viewer.GetSessionStatsAsync(SessionServerId, start, end);

            var point = Assert.Single(trend);
            /* Gauges averaged: (100+120)/2 = 110, (5+9)/2 = 7. */
            Assert.Equal(110, point.TotalSessions);
            Assert.Equal(7, point.RunningSessions);
            /* Attribution columns take the newer (t2) collection's values, not a blend of the two. */
            Assert.Equal("NewerApp", point.TopApplicationName);
            Assert.Equal(55, point.TopApplicationConnections);
            Assert.Equal("NewerHost", point.TopHostName);
            Assert.Equal(45, point.TopHostConnections);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSessionAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task PlanCacheTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Plan-Cache bucketing test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeletePlanCacheAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = TruncateToSeconds(DateTime.UtcNow.AddDays(-30));
            var start = end.AddDays(-7);

            /* One collection every 5 minutes for 7 days, two objtype groups each = 4,032 rows in the
               table, 2,016 collections — the issue's own measured pre-bucketing point count. */
            await BulkSeedPlanCacheAsync(connection, start, end);

            var trend = await viewer.GetPlanCacheTrendAsync(PlanCacheServerId, start, end);

            Assert.True(trend.Count > 0 && trend.Count <= TrendBudget.Chart.AutoPoints,
                $"{trend.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeletePlanCacheAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task PlanCacheTrend_MergedBucket_AveragesThePerCollectionSummedMb_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Plan-Cache bucketing test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeletePlanCacheAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = TruncateToSeconds(DateTime.UtcNow.AddDays(-60));
            var start = end.AddDays(-8);
            var t1 = start.AddHours(1);
            var t2 = t1.AddSeconds(60);

            /* Collection 1: single-use 40, multi-use 100 (one group). Collection 2: single-use 60,
               multi-use 200 (one group). Per-collection sums: 40/100 and 60/200; bucket average: 50/150. */
            await InsertPlanCacheAsync(connection, 1, t1, singleMb: 40, multiMb: 100);
            await InsertPlanCacheAsync(connection, 2, t2, singleMb: 60, multiMb: 200);

            var trend = await viewer.GetPlanCacheTrendAsync(PlanCacheServerId, start, end);

            var point = Assert.Single(trend);
            Assert.Equal(50.0, point.SingleUseSizeMb, precision: 3);
            Assert.Equal(150.0, point.MultiUseSizeMb, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeletePlanCacheAsync(cleanup, cleanupCt));
        }
    }

    /* ---- CPU scheduler seeding ---------------------------------------------------------------------- */

    private static async Task InsertCpuAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc, int runnable, int blocked, int queued)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_scheduler_stats
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
        false, false, false, false, 67108864, 8388608, 'ok', false, 2, 2, 0, false)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(CpuServerId);
        command.Parameters.AddWithValue(CpuServerName);
        command.Parameters.AddWithValue(runnable);
        command.Parameters.AddWithValue(queued);
        command.Parameters.AddWithValue(blocked);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One row a minute across the whole window (10,080 rows for a 7-day window), through
    /// <c>generate_series</c> in a single INSERT — the budget-cap test's bulk seed.</summary>
    private static async Task BulkSeedCpuAsync(NpgsqlConnection connection, DateTime start, DateTime end)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_scheduler_stats
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
SELECT
    -ROW_NUMBER() OVER (ORDER BY t), t, $1, $2,
    512, 8, 8, 3, 0, 128, 0.5, 2, 5, 1, 0, 3, 20, 12.5,
    false, false, false, false, 67108864, 8388608, 'ok', false, 2, 2, 0, false
FROM generate_series($3::timestamp, $4::timestamp, INTERVAL '1 minute') AS s(t)", connection);
        command.Parameters.AddWithValue(CpuServerId);
        command.Parameters.AddWithValue(CpuServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteCpuAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM cpu_scheduler_stats WHERE server_id = {CpuServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ---- Session Stats seeding ---------------------------------------------------------------------- */

    private static async Task InsertSessionAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc,
        int total, int running, string topApp, int topAppConns, string topHost, int topHostConns)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO session_summary_stats
    (collection_id, collection_time, server_id, server_name,
     total_sessions, running_sessions, sleeping_sessions, background_sessions, dormant_sessions,
     idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
     top_application_name, top_application_connections, top_host_name, top_host_connections)
VALUES ($1, $2, $3, $4, $5, $6, 0, 0, 0, 0, 0, 1, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(SessionServerId);
        command.Parameters.AddWithValue(SessionServerName);
        command.Parameters.AddWithValue(total);
        command.Parameters.AddWithValue(running);
        command.Parameters.AddWithValue(topApp);
        command.Parameters.AddWithValue(topAppConns);
        command.Parameters.AddWithValue(topHost);
        command.Parameters.AddWithValue(topHostConns);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One row every 5 minutes across the whole window (2,016 rows for a 7-day window) — the
    /// budget-cap test's bulk seed.</summary>
    private static async Task BulkSeedSessionAsync(NpgsqlConnection connection, DateTime start, DateTime end)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO session_summary_stats
    (collection_id, collection_time, server_id, server_name,
     total_sessions, running_sessions, sleeping_sessions, background_sessions, dormant_sessions,
     idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
     top_application_name, top_application_connections, top_host_name, top_host_connections)
SELECT
    -ROW_NUMBER() OVER (ORDER BY t), t, $1, $2,
    100, 5, 90, 3, 2, 10, 1, 7, 'App', 40, 'Host', 30
FROM generate_series($3::timestamp, $4::timestamp, INTERVAL '5 minutes') AS s(t)", connection);
        command.Parameters.AddWithValue(SessionServerId);
        command.Parameters.AddWithValue(SessionServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteSessionAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM session_summary_stats WHERE server_id = {SessionServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ---- Plan Cache seeding -------------------------------------------------------------------------- */

    private static async Task InsertPlanCacheAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc, int singleMb, int multiMb)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO plan_cache_stats
    (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
     total_plans, total_size_mb, single_use_plans, single_use_size_mb,
     multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES ($1, $2, $3, $4, 'Compiled Plan', 'Adhoc', 10, $5 + $6, 5, $5, 5, $6, 1.5, 64, $2)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(PlanCacheServerId);
        command.Parameters.AddWithValue(PlanCacheServerName);
        command.Parameters.AddWithValue(singleMb);
        command.Parameters.AddWithValue(multiMb);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One collection every 5 minutes across the whole window, two objtype groups each — the
    /// budget-cap test's bulk seed (2,016 collections over a 7-day window).</summary>
    private static async Task BulkSeedPlanCacheAsync(NpgsqlConnection connection, DateTime start, DateTime end)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO plan_cache_stats
    (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
     total_plans, total_size_mb, single_use_plans, single_use_size_mb,
     multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
SELECT
    -(2 * ROW_NUMBER() OVER (ORDER BY t) - 1), t, $1, $2, 'Compiled Plan', 'Proc',
    10, 140, 1, 40, 4, 100, 1.5, 64, t
FROM generate_series($3::timestamp, $4::timestamp, INTERVAL '5 minutes') AS s(t)
UNION ALL
SELECT
    -(2 * ROW_NUMBER() OVER (ORDER BY t)), t, $1, $2, 'Compiled Plan', 'Adhoc',
    30, 210, 25, 10, 5, 200, 1.5, 64, t
FROM generate_series($3::timestamp, $4::timestamp, INTERVAL '5 minutes') AS s(t)", connection);
        command.Parameters.AddWithValue(PlanCacheServerId);
        command.Parameters.AddWithValue(PlanCacheServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeletePlanCacheAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM plan_cache_stats WHERE server_id = {PlanCacheServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}

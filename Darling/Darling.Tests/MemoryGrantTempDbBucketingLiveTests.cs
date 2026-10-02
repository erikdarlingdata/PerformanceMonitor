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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4349's own required tests for the three reads it bucketed (memory-grant overlay, Memory Grants
/// chart, TempDB usage), mirroring <c>ViewerCpuTempDbLivePostgresTests</c>'s budget-cap/singleton-pin
/// shape and <c>LiteMemoryFileIoTrendBucketingLiveTests</c>'s merged-bucket shape. Source checks for
/// these three statements already existed in <c>ViewerMemoryTests.cs</c> before this PR (they pinned the
/// pre-bucket per-collection shape); this file adds the live-Postgres row-budget cap, the singleton
/// raw-timestamp pin, and the grant chart's merged-bucket summed-delta pin the PR body calls out as
/// missing. Shares the "live-postgres" collection; negative sentinel server_ids; cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class MemoryGrantTempDbBucketingLiveTests
{
    private const int GrantBudgetServerId = -934901;
    private const string ServerName = "memtempdb-bucketing-e2e";
    private const int GrantSingletonServerId = -934902;
    private const int GrantChartMergedServerId = -934903;
    private const int TempDbBudgetServerId = -934904;
    private const int TempDbSingletonServerId = -934905;

    /// <summary>7-day, one-collection-per-minute window (10,080 rows unbucketed) must come back capped to
    /// <see cref="TrendBudget.Chart"/>'s single-series budget. Against the pre-#4349 per-collection read
    /// this assertion fails outright (10,080 &gt; budget).</summary>
    [Fact]
    public async Task MemoryGrantTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-grant-trend budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_grant_stats", GrantBudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            await BulkSeedGrantAsync(connection, GrantBudgetServerId, start, end, poolId: 1);

            var trend = await viewer.GetMemoryGrantTrendAsync(GrantBudgetServerId, start, end);

            var budget = TrendBudget.Chart.AutoPoints;
            Assert.True(trend.Count > 0 && trend.Count <= budget, $"{trend.Count} rows over a budget of {budget}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_grant_stats", GrantBudgetServerId, cleanupCt));
        }
    }

    /// <summary>Ruling item 3: when the budget covers every collection, every point is stamped at its own
    /// raw collection time, seeded off the minute grid (:37s) so a fix that floors to the bucket grid
    /// instead loses the seconds and fails this.</summary>
    [Fact]
    public async Task MemoryGrantTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsUnchanged_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-grant singleton-pin test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_grant_stats", GrantSingletonServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertGrantAsync(connection, GrantSingletonServerId, t1, poolId: 1, grantedMb: 100m);
            await InsertGrantAsync(connection, GrantSingletonServerId, t2, poolId: 1, grantedMb: 200m);
            await InsertGrantAsync(connection, GrantSingletonServerId, t3, poolId: 1, grantedMb: 300m);

            var trend = await viewer.GetMemoryGrantTrendAsync(GrantSingletonServerId, t1.AddMinutes(-1), t3.AddMinutes(1));

            Assert.Equal(new[] { t1.Ticks, t2.Ticks, t3.Ticks }, trend.Select(p => p.CollectionTime.Ticks));
            Assert.Equal(new[] { 100.0, 200.0, 300.0 }, trend.Select(p => p.TotalGrantedMb));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_grant_stats", GrantSingletonServerId, cleanupCt));
        }
    }

    /// <summary>The merged-bucket guard for <c>MemoryGrantChartDataSql</c>'s two true deltas: two
    /// collections of the same pool inside ONE bucket must SUM their timeout/forced deltas, and an
    /// unrated collection (sample_interval_seconds = 0, #3540) nulls out of the sum rather than
    /// contributing a confident 0 — but the bucket's sizing gauges (averaged) still see it, proving the
    /// null-not-drop treatment the SQL's comment claims.</summary>
    [Fact]
    public async Task MemoryGrantChart_MergedBucket_SumsDeltas_UnratedCollectionNulledNotDropped_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-grant-chart merged-bucket test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_grant_stats", GrantChartMergedServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var minute = new DateTime(2026, 3, 10, 11, 5, 0);
            var t1 = minute.AddSeconds(5);
            var t2 = minute.AddSeconds(45);

            /* Both collections rated (interval > 0): deltas sum 3+4=7 timeouts, 1+2=3 forced. Sizing MB
               averages 100 and 200 -> 150. */
            await InsertGrantFullAsync(connection, GrantChartMergedServerId, t1, poolId: 1,
                availMb: 900m, grantedMb: 100m, usedMb: 80m, grantee: 1, waiter: 0,
                timeoutDelta: 3, forcedDelta: 1, intervalSeconds: 60);
            await InsertGrantFullAsync(connection, GrantChartMergedServerId, t2, poolId: 1,
                availMb: 800m, grantedMb: 200m, usedMb: 150m, grantee: 2, waiter: 0,
                timeoutDelta: 4, forcedDelta: 2, intervalSeconds: 60);

            var withinBucketRows = await viewer.GetMemoryGrantChartDataAsync(
                GrantChartMergedServerId, minute.AddMinutes(-1), minute.AddMinutes(1));
            var merged = Assert.Single(withinBucketRows);
            Assert.Equal(150.0, merged.GrantedMemoryMb, precision: 3);
            Assert.Equal(7L, merged.TimeoutErrorCountDelta);
            Assert.Equal(3L, merged.ForcedGrantCountDelta);

            /* A third, unrated (#3540) collection alone in its own later bucket: its deltas must be
               ABSENT from the sum (nulled, not a confident 0), but its sizing gauge still counts. */
            await DeleteRowsAsync(connection, "memory_grant_stats", GrantChartMergedServerId, TestContext.Current.CancellationToken);
            var unratedTime = new DateTime(2026, 3, 10, 12, 0, 0);
            await InsertGrantFullAsync(connection, GrantChartMergedServerId, unratedTime, poolId: 1,
                availMb: 500m, grantedMb: 50m, usedMb: 40m, grantee: 1, waiter: 0,
                timeoutDelta: 99, forcedDelta: 99, intervalSeconds: 0);

            var unratedRows = await viewer.GetMemoryGrantChartDataAsync(
                GrantChartMergedServerId, unratedTime.AddMinutes(-1), unratedTime.AddMinutes(1));
            var unratedRow = Assert.Single(unratedRows);
            Assert.Equal(50.0, unratedRow.GrantedMemoryMb, precision: 3);   // sizing gauge unaffected
            Assert.Equal(0L, unratedRow.TimeoutErrorCountDelta);            // SUM of a NULL is NULL -> 0 here
            Assert.Equal(0L, unratedRow.ForcedGrantCountDelta);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_grant_stats", GrantChartMergedServerId, cleanupCt));
        }
    }

    /// <summary>7-day one-per-minute tempdb_stats window must come back capped to the chart budget,
    /// exactly like the CPU/file-I/O twins.</summary>
    [Fact]
    public async Task TempDbTrend_SevenDayWindow_ReturnsAtMostBudgetRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb-usage budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "tempdb_stats", TempDbBudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            await BulkSeedTempDbAsync(connection, TempDbBudgetServerId, start, end);

            var trend = await viewer.GetTempDbTrendAsync(TempDbBudgetServerId, start, end);

            var budget = TrendBudget.Chart.AutoPoints;
            Assert.True(trend.Count > 0 && trend.Count <= budget, $"{trend.Count} rows over a budget of {budget}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "tempdb_stats", TempDbBudgetServerId, cleanupCt));
        }
    }

    /// <summary>Ruling item 3, plus the top-session "last raw collection, not an average" rule: at a
    /// budget-covers-everything window, every point stamps its own raw collection time (off-grid :37s),
    /// and the LAST collection's top_session_id/mb pair rides through the singleton case exactly as its
    /// own raw value (proving the array_agg ORDER BY DESC pick isn't accidentally averaging or
    /// re-ordering when there's nothing to merge).</summary>
    [Fact]
    public async Task TempDbTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndTopSessionUnchanged_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live tempdb-usage singleton-pin test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "tempdb_stats", TempDbSingletonServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);

            await InsertTempDbAsync(connection, TempDbSingletonServerId, t1, totalMb: 100m, topSessionId: 11, topSessionMb: 5.5m);
            await InsertTempDbAsync(connection, TempDbSingletonServerId, t2, totalMb: 200m, topSessionId: 22, topSessionMb: 15.5m);

            var trend = await viewer.GetTempDbTrendAsync(TempDbSingletonServerId, t1.AddMinutes(-1), t2.AddMinutes(1));

            Assert.Equal(new[] { t1.Ticks, t2.Ticks }, trend.Select(s => s.CollectionTime.Ticks));
            Assert.Equal(new[] { 100.0, 200.0 }, trend.Select(s => s.TotalReservedMb));
            Assert.Equal(new[] { 11, 22 }, trend.Select(s => s.TopSessionId));
            Assert.Equal(new[] { 5.5, 15.5 }, trend.Select(s => s.TopSessionTempDbMb));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "tempdb_stats", TempDbSingletonServerId, cleanupCt));
        }
    }

    private static async Task InsertGrantAsync(NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, int poolId, decimal grantedMb) =>
        await InsertGrantFullAsync(connection, serverId, collectionTimeUtc, poolId, availMb: 900m, grantedMb: grantedMb, usedMb: 0m, grantee: 0, waiter: 0, timeoutDelta: 0, forcedDelta: 0, intervalSeconds: 60);

    private static async Task InsertGrantFullAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, int poolId,
        decimal availMb, decimal grantedMb, decimal usedMb, int grantee, int waiter,
        long timeoutDelta, long forcedDelta, int intervalSeconds)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, pool_id,
     available_memory_mb, granted_memory_mb, used_memory_mb,
     grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(poolId);
        command.Parameters.AddWithValue(availMb);
        command.Parameters.AddWithValue(grantedMb);
        command.Parameters.AddWithValue(usedMb);
        command.Parameters.AddWithValue(grantee);
        command.Parameters.AddWithValue(waiter);
        command.Parameters.AddWithValue(timeoutDelta);
        command.Parameters.AddWithValue(forcedDelta);
        command.Parameters.AddWithValue(intervalSeconds);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task BulkSeedGrantAsync(NpgsqlConnection connection, int serverId, DateTime start, DateTime end, int poolId)
    {
        var baseId = CollectionIdGenerator.Next() * 1_000_000L;
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, pool_id,
     available_memory_mb, granted_memory_mb, used_memory_mb,
     grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
SELECT $1 + row_number() OVER (), g, $2, $3, $4, 900, 100, 80, 1, 0, 0, 0, 60
FROM generate_series($5::timestamp, $6::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(baseId);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(poolId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertTempDbAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, decimal totalMb, int topSessionId, decimal topSessionMb)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO tempdb_stats
    (collection_id, collection_time, server_id, server_name,
     user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb,
     total_reserved_mb, unallocated_mb, total_sessions_using_tempdb,
     top_session_id, top_session_tempdb_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(totalMb / 2);
        command.Parameters.AddWithValue(totalMb / 4);
        command.Parameters.AddWithValue(totalMb / 4);
        command.Parameters.AddWithValue(totalMb);
        command.Parameters.AddWithValue(10m);
        command.Parameters.AddWithValue(3L);
        command.Parameters.AddWithValue(topSessionId);
        command.Parameters.AddWithValue(topSessionMb);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task BulkSeedTempDbAsync(NpgsqlConnection connection, int serverId, DateTime start, DateTime end)
    {
        var baseId = CollectionIdGenerator.Next() * 1_000_000L;
        using var command = new NpgsqlCommand(@"
INSERT INTO tempdb_stats
    (collection_id, collection_time, server_id, server_name,
     user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb,
     total_reserved_mb, unallocated_mb, total_sessions_using_tempdb,
     top_session_id, top_session_tempdb_mb)
SELECT $1 + row_number() OVER (), g, $2, $3, 50, 25, 25, 100, 10, 3, 1, 5
FROM generate_series($4::timestamp, $5::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(baseId);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

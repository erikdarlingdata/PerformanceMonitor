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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7676: the memory-grant collector now keeps the small-query semaphore (resource_semaphore_id 1) beside the regular one (id 0).
/// The per-semaphore reads show both rows; the per-pool reads still sum a pool's semaphores into one row, so the pool panels add up
/// to the two semaphore rows and show a pool's small-query waiters.
/// </summary>
[Collection("live-postgres")]
public sealed class ResourceSemaphoreSmallQueryRowLiveTests
{
    private const string ServerName = "darling-small-query-semaphore";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ASmallQuerySemaphoreRow_ShowsInThePerSemaphoreReads_AndThePerPoolReadsSumBoth()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live small-query semaphore test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            /* One pool, one collection: the regular semaphore (3 waiters, 100 MB granted) and the small-query semaphore (5 waiters,
               7 MB granted, no max target: the DMV returns NULL and the collector stores that NULL). */
            foreach (var (semaphore, target, maxTarget, available, granted, used, grantees, waiters) in new[]
            {
                ((short)0, 8000m, (decimal?)12000m, 50m, 100m, 40m, 2, 3),
                ((short)1, 700m, (decimal?)null, 20m, 7m, 6m, 1, 5),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, semaphore, 1, target, (object?)maxTarget ?? DBNull.Value, target, available, granted, used, grantees, waiters, 0L, 0L, 0L, 0L, 60);
            }

            var start = t.AddMinutes(-5);
            var end = t.AddMinutes(5);

            /* Per semaphore: both rows, regular first within the pool. */
            var perSemaphore = await DarlingResourceSemaphoreReader.GetLatestAsync(postgres, ServerId, start, end, 30, ct);
            Assert.Equal(new short[] { 0, 1 }, perSemaphore.Select(r => r.ResourceSemaphoreId).ToArray());
            var perSemaphoreWindow = await DarlingResourceSemaphoreReader.GetWindowAsync(postgres, ServerId, start, end, 30, ct);
            Assert.Equal(new short?[] { 0, 1 }, perSemaphoreWindow.Select(r => r.ResourceSemaphoreId).ToArray());

            /* get_resource_semaphore: the small-query semaphore has no max target, so the stored NULL reaches the tool as null, not a 0 (#7676). */
            using (var doc = System.Text.Json.JsonDocument.Parse(await DarlingMcpMemoryGrantTools.GetResourceSemaphore(postgres, ServerName)))
            {
                var byId = doc.RootElement.GetProperty("grants").EnumerateArray()
                    .ToDictionary(g => g.GetProperty("resource_semaphore_id").GetInt32());
                Assert.Equal(12000d, byId[0].GetProperty("max_target_memory_mb").GetDouble());
                Assert.Equal(System.Text.Json.JsonValueKind.Null, byId[1].GetProperty("max_target_memory_mb").ValueKind);
            }

            /* Per pool: the two semaphores summed into one row. */
            var perPool = await DarlingMemoryGrantReader.GetMemoryGrantsLatestAsync(postgres, ServerId, start, end, ct);
            var pool = Assert.Single(perPool);
            Assert.Equal(107d, pool.GrantedMemoryMb);
            Assert.Equal(70d, pool.AvailableMemoryMb);
            Assert.Equal(8, pool.WaiterCount);
            Assert.Equal(3, pool.GranteeCount);
            var perPoolWindow = await DarlingMemoryGrantReader.GetMemoryGrantsWindowAsync(postgres, ServerId, start, end, ct);
            var poolWindow = Assert.Single(perPoolWindow);
            Assert.Equal(8, poolWindow.PeakWaiterCount);
            Assert.Equal(107d, poolWindow.PeakGrantedMemoryMb);
            Assert.Equal(70d, poolWindow.MinAvailableMemoryMb);

            /* The per-collection granted-memory trend sums them too. */
            await using (var trend = postgres.CreateCommand(TrendBucketSql.MemoryGrantPerCollectionSql))
            {
                StoreReadParameters.AddWindow(trend, ServerId, start, end);
                await using var reader = await trend.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(107d, reader.GetDouble(1));
                Assert.False(await reader.ReadAsync(ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #7676: every read of the grant pressure that is the regular semaphore's own figure must ignore the small-query row. One pool, one
    /// collection: id 0 (3 waiters, 100 MB granted, 4 timeouts, 2 forced) and id 1 (5 waiters, 7 MB granted, 9 timeouts, 8 forced).
    /// Each read below must report id 0's numbers alone, so deleting its <c>resource_semaphore_id</c> filter changes what it returns.
    /// </summary>
    [Fact]
    public async Task EveryRegularSemaphoreRead_IgnoresTheSmallQuerySemaphoreRow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live small-query semaphore test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            foreach (var (semaphore, granted, waiters, timeouts, forced) in new[]
            {
                ((short)0, 100m, 3, 4L, 2L),
                ((short)1, 7m, 5, 9L, 8L),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,1,8000,12000,8000,50,$6,$6,2,$7,0,0,$8,$9,60)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, semaphore, granted, waiters, timeouts, forced);
            }

            var start = DateTime.SpecifyKind(t.AddMinutes(-5), DateTimeKind.Unspecified);
            var end = DateTime.SpecifyKind(t.AddMinutes(5), DateTimeKind.Unspecified);

            /* The fleet card's memory pressure. */
            await using (var fleet = postgres.CreateCommand(DarlingFleetReader.FleetMemoryPressureSql))
            {
                await using var reader = await fleet.ExecuteReaderAsync(ct);
                var found = false;
                while (await reader.ReadAsync(ct))
                {
                    if (reader.GetInt32(0) != ServerId) continue;
                    found = true;
                    Assert.Equal(3L, reader.GetInt64(1));
                    Assert.Equal(4L, reader.GetInt64(2));
                    Assert.Equal(2L, reader.GetInt64(3));
                    Assert.Equal(100d, reader.GetDouble(4));
                }
                Assert.True(found);
            }

            /* The Viewer's server summary memory pressure. */
            await using (var summary = postgres.CreateCommand(PerformanceMonitor.Darling.Viewer.ViewerDataService.ServerSummaryMemoryPressureSql))
            {
                summary.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
                await using var reader = await summary.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(3L, reader.GetInt64(0));
                Assert.Equal(4L, reader.GetInt64(1));
                Assert.Equal(2L, reader.GetInt64(2));
                Assert.Equal(100d, reader.GetDouble(3));
            }

            /* The analysis fact read. */
            await using (var fact = postgres.CreateCommand(PerformanceMonitor.Darling.Analysis.PgFactCollector.MemoryGrantSql))
            {
                fact.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
                fact.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
                fact.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
                await using var reader = await fact.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(3L, Convert.ToInt64(reader.GetValue(0)));
                Assert.Equal(3d, Convert.ToDouble(reader.GetValue(1)));
                Assert.Equal(4L, Convert.ToInt64(reader.GetValue(3)));
                Assert.Equal(2L, Convert.ToInt64(reader.GetValue(4)));
            }

            /* The analysis drill-down's pending grants: one row, the regular semaphore's. */
            await using (var pending = postgres.CreateCommand(PerformanceMonitor.Darling.Analysis.PgDrillDownCollector.PendingGrantsSql))
            {
                pending.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
                pending.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start });
                pending.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end });
                await using var reader = await pending.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(3, reader.GetInt32(7));
                Assert.False(await reader.ReadAsync(ct));
            }

            /* The FinOps memory-grant efficiency, per day. */
            var efficiency = Assert.Single(await DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(postgres, ServerId, 24, 30, ct));
            Assert.Equal(100m, efficiency.AvgGrantedMb);
            Assert.Equal(100m, efficiency.PeakGrantedMb);
            Assert.Equal(3L, efficiency.TotalWaiters);
            Assert.Equal(4L, efficiency.TimeoutErrors);
            Assert.Equal(2L, efficiency.ForcedGrants);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM memory_grant_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

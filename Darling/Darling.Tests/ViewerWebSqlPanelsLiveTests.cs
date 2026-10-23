/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database and every row it plants goes through DeleteRowsAsync. */

/// <summary>
/// #6388: the Viewer's Resource Semaphore Pressure, Resource Semaphore, Plan Cache by Type and Sessions by Application panels return the
/// rows the web's tools return for the same server and window. The store holds an older and a newer snapshot of each collector, so a
/// read that took the wrong snapshot, or dropped the window, would differ from the tool's answer.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerWebSqlPanelsLiveTests
{
    private const int ServerId = -6388001;
    private const int EmptyServerId = -6388002;
    private const string ServerName = "viewer-web-sql-panels";
    private const string EmptyServerName = "viewer-web-sql-panels-empty";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheViewersFourPanels_ReturnTheRowsTheWebToolsReturn_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer web-panel test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, EmptyServerId, EmptyServerName, ct);
            var newer = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            var older = newer.AddMinutes(-8);

            /* Two pools, two snapshots: the older one has the storm (5 waiters), the newer one 1. The latest read must show the newer, the window
               the older's peak. */
            foreach (var (at, waiters, granted) in new[] { (older, 5, 3000m), (newer, 1, 2000m) })
            {
                foreach (var pool in new[] { 1, 2 })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
                        CollectionIdGenerator.Next(), at, ServerId, ServerName, (short)0, pool, 8000m, 12000m, 8000m, 6000m, granted, 1500m, 3, waiters + pool, 4L, 2L, 1L, 2L, 120);
                }
            }

            /* Plan cache: the older snapshot has a group the newer lacks. */
            foreach (var (at, cacheType, objType, total, single) in new[]
            {
                (older, "Compiled Plan", "Trigger", 50, 49), (older, "Compiled Plan", "Adhoc", 900, 800),
                (newer, "Compiled Plan", "Adhoc", 800, 700), (newer, "Compiled Plan", "Proc", 200, 10),
            })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO plan_cache_stats (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype, total_plans, total_size_mb, single_use_plans, single_use_size_mb, multi_use_plans, multi_use_size_mb, avg_use_count, avg_size_kb, oldest_plan_create_time)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, cacheType, objType, total, total / 2, single, single / 2, total - single, 100, 3.5m, 64, at);
            }

            /* Sessions: the older snapshot has an app the newer lacks. */
            foreach (var (at, app, connections) in new[] { (older, "OldApp", 99L), (newer, "SQLCMD", 12L), (newer, "SSMS", 30L) })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count, running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_logical_reads)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, app, connections, 3, 9, 0, 4200L, 99999L);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var windowEnd = DateTime.UtcNow.AddMinutes(1);
            var windowStart = windowEnd.AddHours(-24);

            /* Resource Semaphore + Resource Semaphore Pressure vs get_resource_semaphore. */
            using (var doc = JsonDocument.Parse(await DarlingMcpMemoryGrantTools.GetResourceSemaphore(postgres, ServerName, 24, cancellationToken: ct)))
            {
                var webGrants = doc.RootElement.GetProperty("grants").EnumerateArray().ToList();
                var latest = await viewer.GetResourceSemaphoreLatestAsync(ServerId, windowStart, windowEnd, ct);
                Assert.Equal(2, latest.Count);
                Assert.Equal(webGrants.Count, latest.Count);
                for (var i = 0; i < webGrants.Count; i++)
                {
                    Assert.Equal(webGrants[i].GetProperty("pool_id").GetInt32(), latest[i].PoolId);
                    Assert.Equal(webGrants[i].GetProperty("resource_semaphore_id").GetInt32(), latest[i].ResourceSemaphoreId);
                    Assert.Equal(webGrants[i].GetProperty("granted_memory_mb").GetDouble(), latest[i].GrantedMemoryMb);
                    Assert.Equal(webGrants[i].GetProperty("waiter_count").GetInt32(), latest[i].WaiterCount);
                    Assert.Equal(webGrants[i].GetProperty("max_target_memory_mb").GetDouble(), latest[i].MaxTargetMemoryMb);
                }

                Assert.All(latest, r => Assert.Equal(newer, r.CollectionTime));

                var webWindow = doc.RootElement.GetProperty("window").EnumerateArray().ToList();
                var window = await viewer.GetResourceSemaphoreWindowAsync(ServerId, windowStart, windowEnd, ct);
                Assert.Equal(2, window.Count);
                Assert.Equal(webWindow.Count, window.Count);
                for (var i = 0; i < webWindow.Count; i++)
                {
                    Assert.Equal(webWindow[i].GetProperty("pool_id").GetInt32(), window[i].PoolId);
                    Assert.Equal(webWindow[i].GetProperty("snapshots_in_window").GetInt64(), window[i].SnapshotsInWindow);
                    Assert.Equal(webWindow[i].GetProperty("peak_waiter_count").GetInt64(), window[i].PeakWaiterCount);
                    Assert.Equal(webWindow[i].GetProperty("peak_granted_memory_mb").GetDouble(), Math.Round(window[i].PeakGrantedMemoryMb, 2));
                    Assert.Equal(webWindow[i].GetProperty("timeout_errors_in_window").GetInt64(), window[i].TimeoutErrorsInWindow);
                    Assert.Equal(webWindow[i].GetProperty("forced_grants_in_window").GetInt64(), window[i].ForcedGrantsInWindow);
                }

                /* The plant means what the comments say: the window saw both snapshots and the older one's storm. */
                Assert.All(window, r => Assert.Equal(2, r.SnapshotsInWindow));
                Assert.Equal(new long[] { 6, 7 }, window.Select(r => r.PeakWaiterCount));
                Assert.Equal(new[] { older, older }, window.Select(r => r.PeakWaitersAt));
            }

            /* Plan Cache by Type vs get_plan_cache_bloat's cache_types. */
            using (var doc = JsonDocument.Parse(await DarlingMcpPlanCacheSchedulerTools.GetPlanCacheBloat(postgres, ServerName, 24, cancellationToken: ct)))
            {
                var web = doc.RootElement.GetProperty("cache_types").EnumerateArray().ToList();
                var rows = await viewer.GetPlanCacheByTypeAsync(ServerId, windowStart, windowEnd, ct);
                Assert.Equal(new[] { "Adhoc", "Proc" }, rows.Select(r => r.ObjType));
                Assert.Equal(web.Count, rows.Count);
                for (var i = 0; i < web.Count; i++)
                {
                    Assert.Equal(web[i].GetProperty("cache_type").GetString(), rows[i].CacheObjType);
                    Assert.Equal(web[i].GetProperty("object_type").GetString(), rows[i].ObjType);
                    Assert.Equal(web[i].GetProperty("total_plans").GetInt32(), rows[i].TotalPlans);
                    Assert.Equal(web[i].GetProperty("single_use_plans").GetInt32(), rows[i].SingleUsePlans);
                    Assert.Equal(web[i].GetProperty("multi_use_size_mb").GetInt32(), rows[i].MultiUseSizeMb);
                    Assert.Equal(web[i].GetProperty("avg_use_count").GetDecimal(), rows[i].AvgUseCount);
                }
            }

            /* Sessions by Application vs get_session_stats' applications. */
            using (var doc = JsonDocument.Parse(await DarlingMcpSessionTools.GetSessionStats(postgres, ServerName, ct)))
            {
                var web = doc.RootElement.GetProperty("applications").EnumerateArray().ToList();
                var rows = await viewer.GetSessionsByApplicationAsync(ServerId, ct);
                Assert.Equal(new[] { "SSMS", "SQLCMD" }, rows.Select(r => r.ProgramName));
                Assert.Equal(web.Count, rows.Count);
                for (var i = 0; i < web.Count; i++)
                {
                    Assert.Equal(web[i].GetProperty("program_name").GetString(), rows[i].ProgramName);
                    Assert.Equal(web[i].GetProperty("connections").GetInt64(), rows[i].ConnectionCount);
                    Assert.Equal(web[i].GetProperty("running").GetInt32(), rows[i].RunningCount);
                    Assert.Equal(web[i].GetProperty("total_cpu_time_ms").GetInt64(), rows[i].TotalCpuTimeMs);
                    Assert.Equal(web[i].GetProperty("total_logical_reads").GetInt64(), rows[i].TotalLogicalReads);
                }
            }

            /* A server that never collected any of them: every read is empty, not an error. */
            Assert.Empty(await viewer.GetResourceSemaphoreLatestAsync(EmptyServerId, windowStart, windowEnd, ct));
            Assert.Empty(await viewer.GetResourceSemaphoreWindowAsync(EmptyServerId, windowStart, windowEnd, ct));
            Assert.Empty(await viewer.GetPlanCacheByTypeAsync(EmptyServerId, windowStart, windowEnd, ct));
            Assert.Empty(await viewer.GetSessionsByApplicationAsync(EmptyServerId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = new[] { ServerId, EmptyServerId };
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM memory_grant_stats WHERE server_id = ANY($1)", ids);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM plan_cache_stats WHERE server_id = ANY($1)", ids);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM session_stats WHERE server_id = ANY($1)", ids);
    }
}

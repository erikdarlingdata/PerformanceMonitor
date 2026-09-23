/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3936 regression, Lite's twin of Darling's <c>DarlingMcpPlanCacheSchedulerToolsLivePostgresTests
/// .GetCpuSchedulerPressure_TwoSnapshotsShareOneCollectionTime_ReadsTheNewerByCollectionId</c>: two
/// cpu_scheduler_stats snapshots planted under ONE collection_time (a run-overlap or clock-resolution
/// collision — reproduced live on DARLING01, see the issue) must still read deterministically as the
/// NEWER of the two. Lite's <c>GetCpuSchedulerSnapshotAsync</c> is also the read behind its
/// <c>get_cpu_scheduler_pressure</c> MCP tool (McpPlanCacheSchedulerTools), so this pin covers both
/// surfaces the way <see cref="LocalDataService.GetCpuSchedulerSnapshotAsync"/>'s own doc comment says.
/// </summary>
public sealed class CpuSchedulerSnapshotCollisionTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 42;
    private const string ServerName = "test-server";
    private readonly DuckDbInitializer _duckDb;

    public CpuSchedulerSnapshotCollisionTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>
    /// The lower collection_id row (NORMAL) is inserted AFTER the higher one (CRITICAL) specifically so a
    /// physical/insertion-order coincidence cannot make this pass without the collection_id DESC tiebreak
    /// actually doing the work — reverting the tiebreak in GetCpuSchedulerSnapshotAsync's SQL turns this
    /// CRITICAL-shaped assertion into the NORMAL-shaped row's data.
    /// </summary>
    [Fact]
    public async Task GetCpuSchedulerSnapshotAsync_TwoSnapshotsShareOneCollectionTime_ReadsTheNewerByCollectionId()
    {
        var tiedCollectionTime = DateTime.UtcNow.AddMinutes(-2);

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        const string insertSql = @"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count,
     total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count,
     total_active_request_count, total_queued_request_count, total_blocked_task_count,
     total_active_parallel_thread_count, runnable_request_count, total_request_count, runnable_percent,
     worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning,
     total_physical_memory_kb, available_physical_memory_kb, system_memory_state_desc,
     physical_memory_pressure_warning, total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)";

        /* The OLDER snapshot (lower collection_id) under the SAME collection_time, inserted FIRST (i.e.
           physically first) so a reader that trusted insertion/scan order instead of collection_id would
           land on THIS one: no pressure at all. DuckDB's own tie-break for a plain
           "ORDER BY collection_time DESC LIMIT 1" over two equal-timestamp rows returns the FIRST-inserted
           row on this engine (measured empirically — the opposite of Postgres's behavior for the same
           shape in Darling's twin test), which is exactly what makes this ordering the one that catches a
           reverted tiebreak. */
        await InsertAsync(connection, insertSql,
            9001L, tiedCollectionTime, ServerId, ServerName, 512, 8, 8, 5, 0L, 10, 0.5m, 0, 0, 0, 0L,
            0, 0, 0.0m, false, false, false, false, 65536000L, 60000000L, "Available physical memory is high",
            false, 1, 1, 0, false);

        /* The NEWER snapshot (higher collection_id), inserted SECOND: runnable tasks 60 -> worker
           utilization and the runnable-queue heuristics both read as under real pressure. */
        await InsertAsync(connection, insertSql,
            9002L, tiedCollectionTime, ServerId, ServerName, 512, 8, 8, 60, 5L, 480, 7.5m, 40, 12, 2, 20L,
            30, 40, 75.0m, true, true, false, true, 65536000L, 3276800L, "Available physical memory is low",
            true, 1, 1, 0, false);

        var dataService = new LocalDataService(_duckDb);
        var snapshot = await dataService.GetCpuSchedulerSnapshotAsync(ServerId, hoursBack: 24);

        Assert.NotNull(snapshot);
        Assert.Equal(60, snapshot!.TotalRunnableTasksCount);
        Assert.Equal(480, snapshot.TotalCurrentWorkersCount);
        Assert.True(snapshot.WorkerThreadExhaustionWarning);
        Assert.True(snapshot.PhysicalMemoryPressureWarning);
    }

    private static async Task InsertAsync(DuckDBConnection connection, string sql, params object?[] values)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}

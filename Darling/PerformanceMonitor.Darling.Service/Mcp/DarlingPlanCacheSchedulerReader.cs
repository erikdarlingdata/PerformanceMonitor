/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the plan-cache + CPU-scheduler snapshot MCP tools
/// (<see cref="DarlingMcpPlanCacheSchedulerTools"/>) — the SAME data the Dashboard's <c>get_plan_cache_bloat</c>
/// / <c>get_cpu_scheduler_pressure</c> and the viewer's <c>ViewerDataService.PlanCache</c> /
/// <c>ViewerDataService.CpuScheduler</c> read. Both are point-in-time snapshot collectors (one/many rows per
/// collection, no deltas), so each read is the LATEST snapshot: plan-cache = every (cacheobjtype, objtype)
/// group at the newest collection in the window; cpu-scheduler = the single newest row for the server. STORED
/// reads, no live monitored-server hit, on the <c>v_plan_cache_stats</c> / <c>v_cpu_scheduler_stats</c> views.
///
/// <para>
/// The Dashboard's <c>bloat_level</c> (plan cache) and <c>pressure_level</c> / <c>recommendation</c> (CPU
/// scheduler) are NOT stored columns — they are the <c>report.plan_cache_bloat</c> / <c>report.cpu_scheduler_pressure</c>
/// view's CASE derivations (#1410), the same client-side classification the viewer's
/// <c>ClassifyPlanCacheBloat</c> reproduces. Both are reproduced verbatim here as pure static helpers so the
/// tools serve the full result shape. Every SQL string is a public const so Darling.Tests can pin the dialect
/// + columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingPlanCacheSchedulerReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One (cacheobjtype, objtype) group at the latest plan-cache snapshot.</summary>
    public sealed record PlanCacheGroupRow(
        DateTime CollectionTime, string CacheObjType, string ObjType, int TotalPlans, int TotalSizeMb,
        int SingleUsePlans, int SingleUseSizeMb, int MultiUsePlans, int MultiUseSizeMb, decimal AvgUseCount);

    /// <summary>The latest CPU-scheduler snapshot — the columns get_cpu_scheduler_pressure surfaces plus the
    /// pressure-level classification inputs.</summary>
    public sealed record CpuSchedulerRow(
        DateTime CollectionTime, int SchedulerCount, int TotalRunnableTasksCount, decimal AvgRunnableTasksCount,
        int TotalCurrentWorkersCount, int MaxWorkersCount, decimal? RunnablePercent, int TotalQueuedRequestCount,
        int TotalActiveRequestCount, bool WorkerThreadExhaustionWarning, bool RunnableTasksWarning,
        bool BlockedTasksWarning, bool QueuedRequestsWarning, bool PhysicalMemoryPressureWarning);

    /* ─────────────────────────── plan cache (latest snapshot, all groups) ─────────────────────────── */

    /// <summary>
    /// Every (cacheobjtype, objtype) group at the latest collection in the window — the Dashboard's
    /// <c>get_plan_cache_bloat</c> composition, ordered by cache size. The group cardinality is small
    /// (cacheobjtype × objtype), so the read is uncapped and the tool sums it for the exact summary strip.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string PlanCacheBloatSql = """
        WITH latest AS
        (
            SELECT MAX(collection_time) AS mx
            FROM v_plan_cache_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            collection_time,
            cacheobjtype,
            objtype,
            total_plans,
            total_size_mb,
            single_use_plans,
            single_use_size_mb,
            multi_use_plans,
            multi_use_size_mb,
            avg_use_count
        FROM v_plan_cache_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        ORDER BY total_size_mb DESC, total_plans DESC
        """;

    public static async Task<List<PlanCacheGroupRow>> GetPlanCacheBloatAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<PlanCacheGroupRow>();
        await using var command = postgres.CreateCommand(PlanCacheBloatSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PlanCacheGroupRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
                reader.IsDBNull(9) ? 0m : reader.GetDecimal(9)));
        }

        return rows;
    }

    /// <summary>
    /// The plan-cache bloat classification — <c>report.plan_cache_bloat</c> / the viewer's
    /// <c>ClassifyPlanCacheBloat</c> (#1410), reproduced. The ratio is single-use plans / total plans:
    /// &gt; 50% CRITICAL, &gt; 30% HIGH, &gt; 20% MEDIUM, else NORMAL; the recommendation flips at &gt; 20%.
    /// An empty cache (totalPlans == 0) is NORMAL.
    /// </summary>
    public static (string Level, string Recommendation) ClassifyPlanCacheBloat(long totalPlans, long singleUsePlans)
    {
        double singleUsePercent = totalPlans > 0 ? singleUsePlans * 100.0 / totalPlans : 0.0;

        var level =
            singleUsePercent > 50 ? "CRITICAL" :
            singleUsePercent > 30 ? "HIGH" :
            singleUsePercent > 20 ? "MEDIUM" :
            "NORMAL";

        var recommendation = singleUsePercent > 20
            ? "Check for unparameterized queries / Consider Forced Parameterization"
            : "Plan cache composition is healthy";

        return (level, recommendation);
    }

    /* ─────────────────────────── cpu scheduler (latest snapshot) ─────────────────────────── */

    /// <summary>
    /// The single most recent CPU-scheduler snapshot for the server — the Dashboard's
    /// <c>get_cpu_scheduler_pressure</c> point-in-time read (the Dashboard tool takes no window, reading
    /// <c>report.cpu_scheduler_pressure</c>'s TOP 1). $1 server_id.
    /// </summary>
    public const string CpuSchedulerPressureSql = """
        SELECT
            collection_time,
            scheduler_count,
            total_runnable_tasks_count,
            avg_runnable_tasks_count,
            total_current_workers_count,
            max_workers_count,
            runnable_percent,
            total_queued_request_count,
            total_active_request_count,
            worker_thread_exhaustion_warning,
            runnable_tasks_warning,
            blocked_tasks_warning,
            queued_requests_warning,
            physical_memory_pressure_warning
        FROM v_cpu_scheduler_stats
        WHERE server_id = $1
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    public static async Task<CpuSchedulerRow?> GetCpuSchedulerPressureAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(CpuSchedulerPressureSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new CpuSchedulerRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            reader.IsDBNull(3) ? 0m : reader.GetDecimal(3),
            reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
            reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            reader.IsDBNull(6) ? null : reader.GetDecimal(6),
            reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
            reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
            !reader.IsDBNull(9) && reader.GetBoolean(9),
            !reader.IsDBNull(10) && reader.GetBoolean(10),
            !reader.IsDBNull(11) && reader.GetBoolean(11),
            !reader.IsDBNull(12) && reader.GetBoolean(12),
            !reader.IsDBNull(13) && reader.GetBoolean(13));
    }

    /// <summary>
    /// The CPU-scheduler pressure classification — <c>report.cpu_scheduler_pressure</c>'s
    /// <c>pressure_level</c> + <c>recommendation</c> CASE (#1410), reproduced verbatim. Runnable-task queue
    /// bands (&gt; 50 CRITICAL / &gt; 20 HIGH / &gt; 10 MEDIUM) come first, then a worker-utilization &gt; 90%
    /// escalation, then the collector's warning flags as a fallback.
    /// </summary>
    public static (string Level, string Recommendation) ClassifyCpuSchedulerPressure(
        int totalRunnableTasks, int totalCurrentWorkers, int maxWorkers, int totalQueuedRequests,
        bool workerThreadExhaustionWarning, bool runnableTasksWarning, bool queuedRequestsWarning)
    {
        double workerUtilization = maxWorkers > 0 ? totalCurrentWorkers * 100.0 / maxWorkers : 0.0;

        var level =
            totalRunnableTasks > 50 ? "CRITICAL - High runnable task queue" :
            totalRunnableTasks > 20 ? "HIGH - Moderate runnable task queue" :
            totalRunnableTasks > 10 ? "MEDIUM - Some runnable tasks queued" :
            workerUtilization > 90 ? "HIGH - Worker thread exhaustion" :
            workerThreadExhaustionWarning ? "CRITICAL - Worker thread exhaustion warning" :
            runnableTasksWarning ? "HIGH - Runnable tasks warning" :
            queuedRequestsWarning ? "MEDIUM - Queued requests warning" :
            "NORMAL";

        var recommendation =
            totalRunnableTasks > 20 ? "CPU pressure detected - check for CPU-intensive queries, consider adding CPU cores" :
            workerThreadExhaustionWarning ? "Worker thread exhaustion - check max worker threads setting" :
            totalQueuedRequests > 0 ? "Requests queued for execution - CPU or worker thread pressure" :
            "No CPU scheduler pressure detected";

        return (level, recommendation);
    }
}

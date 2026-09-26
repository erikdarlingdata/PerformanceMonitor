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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One point on the CPU-scheduler pressure trend: the runnable / blocked / queued task
/// counts at a collection instant. A point-in-time snapshot collector (one row per collection), so
/// these plot directly with no delta math.</summary>
public sealed record CpuSchedulerTrendPoint(
    DateTime CollectionTime,
    int RunnableTasks,
    int BlockedTasks,
    int QueuedRequests) : ICpuSchedulerTrendPoint;

/// <summary>
/// The most recent CPU-scheduler snapshot in the window — every column the cpu_scheduler_stats
/// collector captures (scheduler / worker / NUMA / OS-memory pressure), feeding the CPU Scheduler
/// sub-tab's latest-snapshot metric grid and its warning highlights.
/// </summary>
public sealed record CpuSchedulerSnapshot(
    DateTime CollectionTime,
    int MaxWorkersCount,
    int SchedulerCount,
    int CpuCount,
    int TotalRunnableTasksCount,
    long TotalWorkQueueCount,
    int TotalCurrentWorkersCount,
    decimal AvgRunnableTasksCount,
    int TotalActiveRequestCount,
    int TotalQueuedRequestCount,
    int TotalBlockedTaskCount,
    long TotalActiveParallelThreadCount,
    int? RunnableRequestCount,
    int? TotalRequestCount,
    decimal? RunnablePercent,
    bool WorkerThreadExhaustionWarning,
    bool RunnableTasksWarning,
    bool BlockedTasksWarning,
    bool QueuedRequestsWarning,
    long TotalPhysicalMemoryKb,
    long AvailablePhysicalMemoryKb,
    string? SystemMemoryStateDesc,
    bool PhysicalMemoryPressureWarning,
    int TotalNodeCount,
    int NodesOnlineCount,
    int OfflineCpuCount,
    bool OfflineCpuWarning) : ICpuSchedulerSnapshot
{
    /* The two averaged/percentage columns come off Postgres NUMERIC as decimal; the shared
       ICpuSchedulerSnapshot surfaces them as double (Lite's native type). They feed only the metric grid's
       F2 display strings in CpuSchedulerMetrics.BuildMetrics — never the pressure banding — so this widening
       cast is display-only and not observably different for these small values. */
    double ICpuSchedulerSnapshot.AvgRunnableTasksCount => (double)AvgRunnableTasksCount;

    double? ICpuSchedulerSnapshot.RunnablePercent =>
        RunnablePercent.HasValue ? (double)RunnablePercent.Value : (double?)null;
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The CPU Scheduler pressure trend read: the runnable / blocked / queued task counts per collection
    /// over the window, from the <c>v_cpu_scheduler_stats</c> passthrough view. A point-in-time collector
    /// (single row per collection), so the three counts plot directly — no delta normalization.
    /// $1 server_id, $2 window start, $3 window end (all naive UTC), $4 the bucket width in minutes.
    ///
    /// <para>#3936: <c>collection_id</c> is a secondary sort inside the raw CTE, not a filter — a
    /// same-instant collision (rare, see
    /// <see cref="PerformanceMonitor.Collectors.CollectionTimeClock"/>) still plots both real snapshots
    /// into the same bucket, just in a deterministic left-to-right order rather than whatever physical
    /// row order the plan happens to return them in.</para>
    ///
    /// <para>#4234: BUCKETED. All three counts are gauges (ruling item 2, point-in-time snapshot columns,
    /// no delta math): a bucket's value is the plain average of its collections. 10,080 rows over 7 days
    /// at the collector's 1-minute cadence, with no cap, is the issue's own measured number.
    /// <c>first_collection_time</c>/<c>collection_count</c> ride along so the C# reader can stamp a bucket
    /// that merged nothing at its one collection's own raw time (ruling item 3), matching
    /// <see cref="ViewerDataService.GetCpuUtilizationAsync"/>.</para>
    /// </summary>
    public static readonly string CpuSchedulerTrendSql = $"""
        WITH raw AS
        (
            SELECT
                collection_time,
                collection_id,
                total_runnable_tasks_count,
                total_blocked_task_count,
                total_queued_request_count
            FROM v_cpu_scheduler_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(COALESCE(total_runnable_tasks_count, 0)) AS total_runnable_tasks_count,
            AVG(COALESCE(total_blocked_task_count, 0)) AS total_blocked_task_count,
            AVG(COALESCE(total_queued_request_count, 0)) AS total_queued_request_count,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM raw
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>
    /// The CPU Scheduler latest-snapshot read: the single most recent cpu_scheduler_stats row in the
    /// window (all pressure columns), for the metric grid. $1 server_id, $2 start, $3 end (naive UTC).
    ///
    /// <para>#3936: the tiebreak is <c>collection_id DESC</c>, not a second <c>collection_time</c>. A
    /// run-overlap or clock-resolution collision can store two DIFFERENT snapshots under one
    /// <c>collection_time</c> — <c>collection_id</c> is the per-process monotonic counter every row already
    /// carries, so it orders two same-instant rows the same way on every read instead of a bare
    /// <c>LIMIT 1</c> returning either one depending on physical row order.</para>
    /// </summary>
    public const string CpuSchedulerSnapshotSql = """
        SELECT
            collection_time,
            max_workers_count,
            scheduler_count,
            cpu_count,
            total_runnable_tasks_count,
            total_work_queue_count,
            total_current_workers_count,
            avg_runnable_tasks_count,
            total_active_request_count,
            total_queued_request_count,
            total_blocked_task_count,
            total_active_parallel_thread_count,
            runnable_request_count,
            total_request_count,
            runnable_percent,
            worker_thread_exhaustion_warning,
            runnable_tasks_warning,
            blocked_tasks_warning,
            queued_requests_warning,
            total_physical_memory_kb,
            available_physical_memory_kb,
            system_memory_state_desc,
            physical_memory_pressure_warning,
            total_node_count,
            nodes_online_count,
            offline_cpu_count,
            offline_cpu_warning
        FROM v_cpu_scheduler_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY collection_time DESC, collection_id DESC
        LIMIT 1
        """;

    /// <summary>
    /// The scheduler pressure trend (runnable/blocked/queued counts) over the window, bucketed to
    /// <see cref="TrendBudget.Chart"/>'s point budget (#4234). A bucket holding exactly one physical
    /// collection is stamped at that collection's own raw time rather than the bucket grid when EVERY
    /// bucket this call returned is such a singleton (ruling item 3).
    /// </summary>
    public async Task<List<CpuSchedulerTrendPoint>> GetCpuSchedulerTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(CpuSchedulerTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, int Runnable, int Blocked, int Queued)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(5) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(4),
                reader.IsDBNull(1) ? 0 : (int)Math.Round(reader.GetDouble(1)),
                reader.IsDBNull(2) ? 0 : (int)Math.Round(reader.GetDouble(2)),
                reader.IsDBNull(3) ? 0 : (int)Math.Round(reader.GetDouble(3))));
        }

        var result = new List<CpuSchedulerTrendPoint>(rows.Count);
        foreach (var row in rows)
        {
            result.Add(new CpuSchedulerTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.Runnable,
                row.Blocked,
                row.Queued));
        }

        return result;
    }

    /// <summary>The most recent CPU-scheduler snapshot in the window, or null when none was collected.</summary>
    public async Task<CpuSchedulerSnapshot?> GetCpuSchedulerSnapshotAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(CpuSchedulerSnapshotSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new CpuSchedulerSnapshot(
            CollectionTime: reader.GetDateTime(0),
            MaxWorkersCount: reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            SchedulerCount: reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            CpuCount: reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
            TotalRunnableTasksCount: reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
            TotalWorkQueueCount: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
            TotalCurrentWorkersCount: reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
            AvgRunnableTasksCount: reader.IsDBNull(7) ? 0m : reader.GetDecimal(7),
            TotalActiveRequestCount: reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
            TotalQueuedRequestCount: reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
            TotalBlockedTaskCount: reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
            TotalActiveParallelThreadCount: reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
            RunnableRequestCount: reader.IsDBNull(12) ? null : reader.GetInt32(12),
            TotalRequestCount: reader.IsDBNull(13) ? null : reader.GetInt32(13),
            RunnablePercent: reader.IsDBNull(14) ? null : reader.GetDecimal(14),
            WorkerThreadExhaustionWarning: !reader.IsDBNull(15) && reader.GetBoolean(15),
            RunnableTasksWarning: !reader.IsDBNull(16) && reader.GetBoolean(16),
            BlockedTasksWarning: !reader.IsDBNull(17) && reader.GetBoolean(17),
            QueuedRequestsWarning: !reader.IsDBNull(18) && reader.GetBoolean(18),
            TotalPhysicalMemoryKb: reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
            AvailablePhysicalMemoryKb: reader.IsDBNull(20) ? 0 : reader.GetInt64(20),
            SystemMemoryStateDesc: reader.IsDBNull(21) ? null : reader.GetString(21),
            PhysicalMemoryPressureWarning: !reader.IsDBNull(22) && reader.GetBoolean(22),
            TotalNodeCount: reader.IsDBNull(23) ? 0 : reader.GetInt32(23),
            NodesOnlineCount: reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
            OfflineCpuCount: reader.IsDBNull(25) ? 0 : reader.GetInt32(25),
            OfflineCpuWarning: !reader.IsDBNull(26) && reader.GetBoolean(26));
    }
}

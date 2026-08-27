/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /* The CPU Scheduler readers — the Lite (DuckDB) port of the Darling viewer's
       ViewerDataService.CpuScheduler reads. cpu_scheduler_stats is a POINT-IN-TIME snapshot collector
       (one aggregate row per collection), NOT a cumulative-counter delta table, so the trend's three
       task counts plot directly with no delta/LAG normalization. Reads run against the
       v_cpu_scheduler_stats archive view (base table UNION the archived parquet), matching every other
       Lite trend reader. collection_time is stored in UTC, so the window uses GetTimeRange (UTC). */

    /// <summary>
    /// The CPU Scheduler pressure trend: the runnable / blocked / queued task counts per collection over
    /// the window, plotted directly (point-in-time collector, no delta math).
    /// </summary>
    public async Task<List<CpuSchedulerTrendPoint>> GetCpuSchedulerTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetCpuSchedulerTrendAsync", "v_cpu_scheduler_stats pressure trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    total_runnable_tasks_count,
    total_blocked_task_count,
    total_queued_request_count
FROM v_cpu_scheduler_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<CpuSchedulerTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CpuSchedulerTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                RunnableTasks = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                BlockedTasks = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                QueuedRequests = reader.IsDBNull(3) ? 0 : reader.GetInt32(3)
            });
        }

        return items;
    }

    /// <summary>
    /// The CPU Scheduler latest-snapshot: the single most recent cpu_scheduler_stats row in the window
    /// (every scheduler / worker / NUMA / OS-memory pressure column + the collector's CASE-computed
    /// warning flags), feeding the metric grid. Returns null when the window holds no snapshot.
    /// </summary>
    public async Task<CpuSchedulerSnapshot?> GetCpuSchedulerSnapshotAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetCpuSchedulerSnapshotAsync", "v_cpu_scheduler_stats latest snapshot");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
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
ORDER BY collection_time DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return new CpuSchedulerSnapshot
        {
            CollectionTime = reader.GetDateTime(0),
            MaxWorkersCount = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            SchedulerCount = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            CpuCount = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
            TotalRunnableTasksCount = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
            TotalWorkQueueCount = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
            TotalCurrentWorkersCount = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
            AvgRunnableTasksCount = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
            TotalActiveRequestCount = reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
            TotalQueuedRequestCount = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
            TotalBlockedTaskCount = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
            TotalActiveParallelThreadCount = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
            RunnableRequestCount = reader.IsDBNull(12) ? null : reader.GetInt32(12),
            TotalRequestCount = reader.IsDBNull(13) ? null : reader.GetInt32(13),
            RunnablePercent = reader.IsDBNull(14) ? null : ToDouble(reader.GetValue(14)),
            WorkerThreadExhaustionWarning = !reader.IsDBNull(15) && reader.GetBoolean(15),
            RunnableTasksWarning = !reader.IsDBNull(16) && reader.GetBoolean(16),
            BlockedTasksWarning = !reader.IsDBNull(17) && reader.GetBoolean(17),
            QueuedRequestsWarning = !reader.IsDBNull(18) && reader.GetBoolean(18),
            TotalPhysicalMemoryKb = reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
            AvailablePhysicalMemoryKb = reader.IsDBNull(20) ? 0 : reader.GetInt64(20),
            SystemMemoryStateDesc = reader.IsDBNull(21) ? null : reader.GetString(21),
            PhysicalMemoryPressureWarning = !reader.IsDBNull(22) && reader.GetBoolean(22),
            TotalNodeCount = reader.IsDBNull(23) ? 0 : reader.GetInt32(23),
            NodesOnlineCount = reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
            OfflineCpuCount = reader.IsDBNull(25) ? 0 : reader.GetInt32(25),
            OfflineCpuWarning = !reader.IsDBNull(26) && reader.GetBoolean(26)
        };
    }
}

/// <summary>One point on the CPU-scheduler pressure trend: the runnable / blocked / queued task counts
/// at a collection instant. A point-in-time snapshot collector (one row per collection), so these plot
/// directly with no delta math.</summary>
public class CpuSchedulerTrendPoint : ICpuSchedulerTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public int RunnableTasks { get; set; }
    public int BlockedTasks { get; set; }
    public int QueuedRequests { get; set; }
}

/// <summary>The most recent CPU-scheduler snapshot in the window — every column the cpu_scheduler_stats
/// collector captures (scheduler / worker / NUMA / OS-memory pressure), feeding the CPU Scheduler tab's
/// latest-snapshot metric grid and its warning highlights. Implements <see cref="ICpuSchedulerSnapshot"/> so
/// the shared <see cref="CpuSchedulerMetrics"/> projection reads it directly (its double / double? averaged
/// columns satisfy the interface as-is).</summary>
public class CpuSchedulerSnapshot : ICpuSchedulerSnapshot
{
    public DateTime CollectionTime { get; set; }
    public int MaxWorkersCount { get; set; }
    public int SchedulerCount { get; set; }
    public int CpuCount { get; set; }
    public int TotalRunnableTasksCount { get; set; }
    public long TotalWorkQueueCount { get; set; }
    public int TotalCurrentWorkersCount { get; set; }
    public double AvgRunnableTasksCount { get; set; }
    public int TotalActiveRequestCount { get; set; }
    public int TotalQueuedRequestCount { get; set; }
    public int TotalBlockedTaskCount { get; set; }
    public long TotalActiveParallelThreadCount { get; set; }
    public int? RunnableRequestCount { get; set; }
    public int? TotalRequestCount { get; set; }
    public double? RunnablePercent { get; set; }
    public bool WorkerThreadExhaustionWarning { get; set; }
    public bool RunnableTasksWarning { get; set; }
    public bool BlockedTasksWarning { get; set; }
    public bool QueuedRequestsWarning { get; set; }
    public long TotalPhysicalMemoryKb { get; set; }
    public long AvailablePhysicalMemoryKb { get; set; }
    public string? SystemMemoryStateDesc { get; set; }
    public bool PhysicalMemoryPressureWarning { get; set; }
    public int TotalNodeCount { get; set; }
    public int NodesOnlineCount { get; set; }
    public int OfflineCpuCount { get; set; }
    public bool OfflineCpuWarning { get; set; }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// CPU scheduler + workload-group + NUMA + OS-memory pressure snapshot (the Dashboard-parity port
/// of install/17_collect_cpu_scheduler_stats.sql). A POINT-IN-TIME snapshot, not a cumulative-counter
/// delta collector: it reads the current worker/runnable/blocked/queued task counts, worker-thread
/// exhaustion, NUMA online/offline, and OS memory pressure directly — no delta groups (mirrors
/// <see cref="MemoryStatsCollector"/> / <see cref="SessionStatsCollector"/>). The query aggregates
/// sys.dm_os_schedulers (VISIBLE ONLINE) with single-row CROSS JOINs to sys.dm_os_sys_info,
/// sys.dm_os_sys_memory, a sys.dm_os_nodes NUMA rollup, and a
/// sys.dm_resource_governor_workload_groups rollup, plus an OUTER APPLY over sys.dm_exec_requests —
/// collapsing to exactly one row. The pressure warnings (worker-thread exhaustion, runnable/blocked/
/// queued, physical-memory, offline-CPU) are the Dashboard proc's own CASE expressions, kept in SQL
/// so the stored columns are value-identical to the Dashboard table; they are CONVERT(bit, ...) so
/// the reader gets a clean boolean. The Dashboard proc's collection_log / critical_issues logging
/// and RAISERROR debug output are the collector framework's job and are intentionally dropped.
///
/// <para>Does NOT apply to Azure SQL Database (edition 5): sys.dm_os_sys_memory is not exposed there
/// (MS Learn "Applies to" omits Azure SQL DB — only SQL Server + Managed Instance), so the query
/// would fail outright; and sys.dm_exec_requests is limited to the current connection on Azure SQL DB
/// (VIEW SERVER STATE cannot be granted), which would make the runnable/total request counts
/// meaningless even if the memory DMV existed. Both are documented, verified gaps — not a scope-down.
/// Gated exactly like <see cref="MemoryPressureEventsCollector"/>. Azure SQL Managed Instance
/// (edition 8) exposes all of these DMVs, so it collects there (AppliesTo = !IsAzureSqlDb).</para>
/// </summary>
public sealed class CpuSchedulerStatsCollector : CollectorDefinitionBase<CpuSchedulerStatsCollector.Row>
{
    public static CpuSchedulerStatsCollector Instance { get; } = new();

    private CpuSchedulerStatsCollector()
    {
    }

    public readonly record struct Row(
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
        bool OfflineCpuWarning);

    /* Ported verbatim from install/17_collect_cpu_scheduler_stats.sql's SELECT. The only additions
       over the proc body are explicit output-type CONVERTs the proc relied on the INSERT to do
       implicitly (COUNT_BIG -> integer for total_request_count/total_node_count; the 1/0 CASE
       results -> bit for the warnings; AVG(...) -> decimal(38,2)) so the positional reader gets the
       exact storage types. Values stored are identical to the Dashboard table. */
    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    max_workers_count = osi.max_workers_count,
    scheduler_count = osi.scheduler_count,
    cpu_count = osi.cpu_count,
    total_runnable_tasks_count = SUM(dos.runnable_tasks_count),
    total_work_queue_count = SUM(dos.work_queue_count),
    total_current_workers_count = SUM(dos.current_workers_count),
    avg_runnable_tasks_count = CONVERT(decimal(38,2), AVG(CONVERT(decimal(38,2), dos.runnable_tasks_count))),
    total_active_request_count = MAX(wg.active_request_count),
    total_queued_request_count = MAX(wg.queued_request_count),
    total_blocked_task_count = MAX(wg.blocked_task_count),
    total_active_parallel_thread_count = MAX(wg.active_parallel_thread_count),
    runnable_request_count = r.runnable_count,
    total_request_count = CONVERT(integer, r.total_count),
    runnable_percent = r.runnable_pct,
    worker_thread_exhaustion_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN SUM(dos.current_workers_count) >= (osi.max_workers_count * 0.90)
                THEN 1
                ELSE 0
            END
        ),
    runnable_tasks_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN SUM(dos.runnable_tasks_count) >= osi.cpu_count
                THEN 1
                ELSE 0
            END
        ),
    blocked_tasks_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN MAX(wg.blocked_task_count) >= 10
                THEN 1
                ELSE 0
            END
        ),
    queued_requests_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN MAX(wg.queued_request_count) >= osi.cpu_count
                THEN 1
                ELSE 0
            END
        ),
    total_physical_memory_kb = osm.total_physical_memory_kb,
    available_physical_memory_kb = osm.available_physical_memory_kb,
    system_memory_state_desc = osm.system_memory_state_desc,
    physical_memory_pressure_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN osm.available_physical_memory_kb < (osm.total_physical_memory_kb * 0.10)
                THEN 1
                ELSE 0
            END
        ),
    total_node_count = CONVERT(integer, nodes.total_nodes),
    nodes_online_count = nodes.online_nodes,
    offline_cpu_count = nodes.offline_cpus,
    offline_cpu_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN
                (
                    SELECT COUNT_BIG(*)
                    FROM sys.dm_os_schedulers AS dos_offline
                    WHERE dos_offline.is_online = 0
                ) > 0
                THEN 1
                ELSE 0
            END
        )
FROM sys.dm_os_schedulers AS dos
CROSS JOIN sys.dm_os_sys_info AS osi
CROSS JOIN sys.dm_os_sys_memory AS osm
CROSS JOIN
(
    SELECT
        total_nodes = COUNT_BIG(*),
        online_nodes =
            SUM
            (
                CASE
                    WHEN n.node_state_desc = N'ONLINE'
                    THEN 1
                    ELSE 0
                END
            ),
        offline_cpus =
            SUM
            (
                CASE
                    WHEN n.node_state_desc <> N'ONLINE'
                    THEN 1
                    ELSE 0
                END
            )
    FROM sys.dm_os_nodes AS n
    WHERE n.node_id <> 32767 /*Exclude DAC node*/
) AS nodes
CROSS JOIN
(
    SELECT
        active_request_count = SUM(wg.active_request_count),
        queued_request_count = SUM(wg.queued_request_count),
        blocked_task_count = SUM(wg.blocked_task_count),
        active_parallel_thread_count = SUM(wg.active_parallel_thread_count)
    FROM sys.dm_resource_governor_workload_groups AS wg
) AS wg
OUTER APPLY
(
    SELECT
        total_count = COUNT_BIG(*),
        runnable_count =
            SUM
            (
                CASE
                    WHEN der.status = N'runnable'
                    THEN 1
                    ELSE 0
                END
            ),
        runnable_pct =
            CONVERT
            (
                decimal(38,2),
                (
                    SUM
                    (
                        CASE
                            WHEN der.status = N'runnable'
                            THEN 1.0
                            ELSE 0.0
                        END
                    ) /
                    NULLIF(CONVERT(decimal(38,2), COUNT_BIG(*)), 0)
                ) * 100.0
            )
    FROM sys.dm_exec_requests AS der
    WHERE der.session_id > 50
    AND   der.session_id <> @@SPID
    AND   der.status NOT IN (N'background', N'sleeping')
) AS r
WHERE dos.status = N'VISIBLE ONLINE'
GROUP BY
    osi.max_workers_count,
    osi.scheduler_count,
    osi.cpu_count,
    r.runnable_count,
    r.total_count,
    r.runnable_pct,
    osm.total_physical_memory_kb,
    osm.available_physical_memory_kb,
    osm.system_memory_state_desc,
    nodes.total_nodes,
    nodes.online_nodes,
    nodes.offline_cpus
OPTION(RECOMPILE);";

    public override string Name => "cpu_scheduler_stats";

    public override string TargetTable => "cpu_scheduler_stats";

    public override string? WatermarkColumn => null;

    /// <summary>
    /// Skips Azure SQL DB: sys.dm_os_sys_memory is not exposed there and sys.dm_exec_requests is
    /// limited to the current connection (both MS Learn-verified). Collects on SQL Server and Azure
    /// SQL Managed Instance, which expose the full DMV set. Mirrors MemoryPressureEventsCollector.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("max_workers_count", CollectorColumnType.Integer),
        new CollectorColumn("scheduler_count", CollectorColumnType.Integer),
        new CollectorColumn("cpu_count", CollectorColumnType.Integer),
        new CollectorColumn("total_runnable_tasks_count", CollectorColumnType.Integer),
        new CollectorColumn("total_work_queue_count", CollectorColumnType.BigInt),
        new CollectorColumn("total_current_workers_count", CollectorColumnType.Integer),
        new CollectorColumn("avg_runnable_tasks_count", CollectorColumnType.Decimal, 38, 2),
        new CollectorColumn("total_active_request_count", CollectorColumnType.Integer),
        new CollectorColumn("total_queued_request_count", CollectorColumnType.Integer),
        new CollectorColumn("total_blocked_task_count", CollectorColumnType.Integer),
        new CollectorColumn("total_active_parallel_thread_count", CollectorColumnType.BigInt),
        new CollectorColumn("runnable_request_count", CollectorColumnType.Integer),
        new CollectorColumn("total_request_count", CollectorColumnType.Integer),
        new CollectorColumn("runnable_percent", CollectorColumnType.Decimal, 38, 2),
        new CollectorColumn("worker_thread_exhaustion_warning", CollectorColumnType.Boolean),
        new CollectorColumn("runnable_tasks_warning", CollectorColumnType.Boolean),
        new CollectorColumn("blocked_tasks_warning", CollectorColumnType.Boolean),
        new CollectorColumn("queued_requests_warning", CollectorColumnType.Boolean),
        new CollectorColumn("total_physical_memory_kb", CollectorColumnType.BigInt),
        new CollectorColumn("available_physical_memory_kb", CollectorColumnType.BigInt),
        new CollectorColumn("system_memory_state_desc", CollectorColumnType.Varchar),
        new CollectorColumn("physical_memory_pressure_warning", CollectorColumnType.Boolean),
        new CollectorColumn("total_node_count", CollectorColumnType.Integer),
        new CollectorColumn("nodes_online_count", CollectorColumnType.Integer),
        new CollectorColumn("offline_cpu_count", CollectorColumnType.Integer),
        new CollectorColumn("offline_cpu_warning", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                MaxWorkersCount: reader.GetInt32(0),
                SchedulerCount: reader.GetInt32(1),
                CpuCount: reader.GetInt32(2),
                TotalRunnableTasksCount: reader.GetInt32(3),
                TotalWorkQueueCount: reader.GetInt64(4),
                TotalCurrentWorkersCount: reader.GetInt32(5),
                AvgRunnableTasksCount: reader.GetDecimal(6),
                TotalActiveRequestCount: reader.GetInt32(7),
                TotalQueuedRequestCount: reader.GetInt32(8),
                TotalBlockedTaskCount: reader.GetInt32(9),
                TotalActiveParallelThreadCount: reader.GetInt64(10),
                /* r is an OUTER APPLY over an aggregate: total_count is COUNT_BIG (0 when no
                   qualifying requests) but runnable_count/runnable_pct are SUM/ratio over an empty
                   set = NULL — nullable, matching the Dashboard columns. */
                RunnableRequestCount: reader.IsDBNull(11) ? null : reader.GetInt32(11),
                TotalRequestCount: reader.IsDBNull(12) ? null : reader.GetInt32(12),
                RunnablePercent: reader.IsDBNull(13) ? null : reader.GetDecimal(13),
                WorkerThreadExhaustionWarning: reader.GetBoolean(14),
                RunnableTasksWarning: reader.GetBoolean(15),
                BlockedTasksWarning: reader.GetBoolean(16),
                QueuedRequestsWarning: reader.GetBoolean(17),
                TotalPhysicalMemoryKb: reader.GetInt64(18),
                AvailablePhysicalMemoryKb: reader.GetInt64(19),
                SystemMemoryStateDesc: reader.IsDBNull(20) ? null : reader.GetString(20),
                PhysicalMemoryPressureWarning: reader.GetBoolean(21),
                TotalNodeCount: reader.GetInt32(22),
                NodesOnlineCount: reader.GetInt32(23),
                OfflineCpuCount: reader.GetInt32(24),
                OfflineCpuWarning: reader.GetBoolean(25)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.MaxWorkersCount)                  /* max_workers_count INTEGER */
            .Value(row.SchedulerCount)                   /* scheduler_count INTEGER */
            .Value(row.CpuCount)                         /* cpu_count INTEGER */
            .Value(row.TotalRunnableTasksCount)          /* total_runnable_tasks_count INTEGER */
            .Value(row.TotalWorkQueueCount)              /* total_work_queue_count BIGINT */
            .Value(row.TotalCurrentWorkersCount)         /* total_current_workers_count INTEGER */
            .Value(row.AvgRunnableTasksCount)            /* avg_runnable_tasks_count DECIMAL(38,2) */
            .Value(row.TotalActiveRequestCount)          /* total_active_request_count INTEGER */
            .Value(row.TotalQueuedRequestCount)          /* total_queued_request_count INTEGER */
            .Value(row.TotalBlockedTaskCount)            /* total_blocked_task_count INTEGER */
            .Value(row.TotalActiveParallelThreadCount)   /* total_active_parallel_thread_count BIGINT */
            .Value(row.RunnableRequestCount)             /* runnable_request_count INTEGER (nullable) */
            .Value(row.TotalRequestCount)                /* total_request_count INTEGER (nullable) */
            .Value(row.RunnablePercent)                  /* runnable_percent DECIMAL(38,2) (nullable) */
            .Value(row.WorkerThreadExhaustionWarning)    /* worker_thread_exhaustion_warning BOOLEAN */
            .Value(row.RunnableTasksWarning)             /* runnable_tasks_warning BOOLEAN */
            .Value(row.BlockedTasksWarning)              /* blocked_tasks_warning BOOLEAN */
            .Value(row.QueuedRequestsWarning)            /* queued_requests_warning BOOLEAN */
            .Value(row.TotalPhysicalMemoryKb)            /* total_physical_memory_kb BIGINT */
            .Value(row.AvailablePhysicalMemoryKb)        /* available_physical_memory_kb BIGINT */
            .Value(row.SystemMemoryStateDesc)            /* system_memory_state_desc VARCHAR (nullable) */
            .Value(row.PhysicalMemoryPressureWarning)    /* physical_memory_pressure_warning BOOLEAN */
            .Value(row.TotalNodeCount)                   /* total_node_count INTEGER */
            .Value(row.NodesOnlineCount)                 /* nodes_online_count INTEGER */
            .Value(row.OfflineCpuCount)                  /* offline_cpu_count INTEGER */
            .Value(row.OfflineCpuWarning);               /* offline_cpu_warning BOOLEAN */
    }
}

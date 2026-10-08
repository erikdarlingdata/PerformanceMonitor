/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the cross-SKU parity contract of the shared cpu_scheduler_stats collector definition (the
/// Dashboard-parity port of install/17_collect_cpu_scheduler_stats.sql): verbatim query text, the
/// Azure SQL DB skip (sys.dm_os_sys_memory / connection-scoped sys.dm_exec_requests), payload column
/// order, ReadAsync mapping (including the nullable request columns), and the point-in-time no-delta
/// WritePayload. An intentional change to any of these must consciously update these tests.
/// </summary>
public sealed class CpuSchedulerStatsCollectorDefinitionTests
{
    private const string ExpectedQuery = @"
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

    [Fact]
    public void Query_IsTheVerbatimParityContract()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());
        Assert.Equal(ExpectedQuery, CpuSchedulerStatsCollector.Instance.BuildQuery(context).Text);
        Assert.Empty(CpuSchedulerStatsCollector.Instance.BuildQuery(context).Parameters);
        Assert.Null(CpuSchedulerStatsCollector.Instance.WatermarkColumn);
        Assert.Equal("cpu_scheduler_stats", CpuSchedulerStatsCollector.Instance.Name);
        Assert.Equal("cpu_scheduler_stats", CpuSchedulerStatsCollector.Instance.TargetTable);
    }

    [Fact]
    public void AppliesTo_SkipsAzureSqlDb_ButCollectsOnPremAndManagedInstance()
    {
        /* sys.dm_os_sys_memory is not exposed on Azure SQL DB and sys.dm_exec_requests is
           connection-scoped there — skip the cycle, mirroring memory_pressure_events. */
        Assert.False(CpuSchedulerStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(CpuSchedulerStatsCollector.Instance.AppliesTo(new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.True(CpuSchedulerStatsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    [Fact]
    public void PayloadColumns_AreInAppendOrder()
    {
        var names = CpuSchedulerStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(
            new[]
            {
                "max_workers_count",
                "scheduler_count",
                "cpu_count",
                "total_runnable_tasks_count",
                "total_work_queue_count",
                "total_current_workers_count",
                "avg_runnable_tasks_count",
                "total_active_request_count",
                "total_queued_request_count",
                "total_blocked_task_count",
                "total_active_parallel_thread_count",
                "runnable_request_count",
                "total_request_count",
                "runnable_percent",
                "worker_thread_exhaustion_warning",
                "runnable_tasks_warning",
                "blocked_tasks_warning",
                "queued_requests_warning",
                "total_physical_memory_kb",
                "available_physical_memory_kb",
                "system_memory_state_desc",
                "physical_memory_pressure_warning",
                "total_node_count",
                "nodes_online_count",
                "offline_cpu_count",
                "offline_cpu_warning",
            },
            names);

        /* The two decimal(38,2) averages/percent carry explicit precision so Darling's numeric(38,2)
           matches the Dashboard column; the four warning flags are booleans. */
        Assert.Equal(CollectorColumnType.Decimal, CpuSchedulerStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "avg_runnable_tasks_count").Type);
        Assert.Equal(38, CpuSchedulerStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "runnable_percent").Precision);
        Assert.Equal(CollectorColumnType.Boolean, CpuSchedulerStatsCollector.Instance.PayloadColumns.Single(c => c.Name == "offline_cpu_warning").Type);
    }

    [Fact]
    public async Task ReadAsync_MapsColumns_IncludingNullableRequestColumns()
    {
        using var reader = new FakeCollectorDataReader(
            /* A healthy row: request columns populated, no warnings. */
            new object[]
            {
                512, 8, 8, 3, 0L, 40, 1.50m, 5, 0, 0, 0L, 2, 10, 20.00m,
                false, false, false, false, 16777216L, 8388608L, "Available physical memory is high",
                false, 1, 1, 0, false,
            },
            /* No qualifying requests: runnable_count / runnable_pct come back NULL (SUM/ratio over an
               empty set), and a warning is set. */
            new object[]
            {
                512, 8, 8, 9, 0L, 60, 9.00m, 0, 0, 0, 0L, DBNull.Value, DBNull.Value, DBNull.Value,
                true, true, false, false, 16777216L, 512000L, "Physical memory low",
                true, 2, 1, 1, true,
            });

        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await CpuSchedulerStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(
            new CpuSchedulerStatsCollector.Row(512, 8, 8, 3, 0, 40, 1.50m, 5, 0, 0, 0, 2, 10, 20.00m,
                false, false, false, false, 16777216, 8388608, "Available physical memory is high",
                false, 1, 1, 0, false),
            rows[0]);
        Assert.Equal(
            new CpuSchedulerStatsCollector.Row(512, 8, 8, 9, 0, 60, 9.00m, 0, 0, 0, 0, null, null, null,
                true, true, false, false, 16777216, 512000, "Physical memory low",
                true, 2, 1, 1, true),
            rows[1]);
    }

    [Fact]
    public void WritePayload_EmitsPayloadOrder_PointInTime_NoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);
        var writer = new RecordingCollectorRowWriter();
        var row = new CpuSchedulerStatsCollector.Row(512, 8, 8, 3, 0, 40, 1.50m, 5, 0, 0, 0, 2, 10, 20.00m,
            false, false, false, false, 16777216, 8388608, "Available physical memory is high",
            false, 1, 1, 0, false);

        CpuSchedulerStatsCollector.Instance.WritePayload(row, writer, context);

        Assert.Equal(
            new object?[]
            {
                512, 8, 8, 3, 0L, 40, 1.50m, 5, 0, 0, 0L, 2, 10, 20.00m,
                false, false, false, false, 16777216L, 8388608L, "Available physical memory is high",
                false, 1, 1, 0, false,
            },
            writer.Values);

        /* Point-in-time snapshot — no delta calculator interaction at all. */
        Assert.Empty(deltas.Calls);
    }
}

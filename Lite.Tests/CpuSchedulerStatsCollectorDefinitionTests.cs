/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
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
    total_runnable_tasks_count = sched.total_runnable_tasks_count,
    total_work_queue_count = sched.total_work_queue_count,
    total_current_workers_count = sched.total_current_workers_count,
    avg_runnable_tasks_count = sched.avg_runnable_tasks_count,
    total_active_request_count = wg.active_request_count,
    total_queued_request_count = wg.queued_request_count,
    total_blocked_task_count = wg.blocked_task_count,
    total_active_parallel_thread_count = wg.active_parallel_thread_count,
    runnable_request_count = r.runnable_count,
    total_request_count = CONVERT(integer, r.total_count),
    runnable_percent = r.runnable_pct,
    worker_thread_exhaustion_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN sched.total_current_workers_count >= (osi.max_workers_count * 0.90)
                THEN 1
                ELSE 0
            END
        ),
    runnable_tasks_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN sched.total_runnable_tasks_count >= osi.cpu_count
                THEN 1
                ELSE 0
            END
        ),
    blocked_tasks_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN wg.blocked_task_count >= 10
                THEN 1
                ELSE 0
            END
        ),
    queued_requests_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN wg.queued_request_count >= osi.cpu_count
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
                AND  osm.system_high_memory_signal_state = 0 /*The engine says available memory is high: a buffer pool that uses the RAM is not pressure*/
                THEN 1
                ELSE 0
            END
        ),
    total_node_count = CONVERT(integer, nodes.total_nodes),
    nodes_online_count = nodes.online_nodes,
    offline_cpu_count = CONVERT(integer, offline_cpus.offline_cpu_count),
    offline_cpu_warning =
        CONVERT
        (
            bit,
            CASE
                WHEN offline_cpus.offline_cpu_count > 0
                THEN 1
                ELSE 0
            END
        )
FROM
(
    /*The scheduler aggregate stands alone with no grouping, so it is exactly one row whatever the plan does with the
      single-row pieces cross joined below. It used to share one grouping with them (#5710): a piece read again per
      scheduler row could return a moving value, and the schedulers fell into several groups, one stored row each.*/
    SELECT
        total_runnable_tasks_count = SUM(dos.runnable_tasks_count),
        total_work_queue_count = SUM(dos.work_queue_count),
        total_current_workers_count = SUM(dos.current_workers_count),
        avg_runnable_tasks_count = CONVERT(decimal(38,2), AVG(CONVERT(decimal(38,2), dos.runnable_tasks_count)))
    FROM sys.dm_os_schedulers AS dos
    WHERE dos.status = N'VISIBLE ONLINE'
    HAVING COUNT_BIG(*) > 0 /*no VISIBLE ONLINE scheduler writes no row (as before #5710), not one all-NULL row*/
) AS sched
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
                    WHEN n.node_state_desc NOT LIKE N'OFFLINE%'
                    THEN 1
                    ELSE 0
                END
            )
    FROM sys.dm_os_nodes AS n
    WHERE n.node_state_desc NOT LIKE N'%DAC%' /*Exclude the DAC node by its state, not by node_id (#5656)*/
) AS nodes
CROSS JOIN
(
    /*Offline CPUs are user schedulers SQL Server has taken offline; the warning flag reads this same count*/
    SELECT
        offline_cpu_count = COUNT_BIG(*)
    FROM sys.dm_os_schedulers AS dos_offline
    WHERE dos_offline.status = N'VISIBLE OFFLINE'
) AS offline_cpus
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

    /* #5710: a collection must be ONE row. The scheduler aggregate used to share one GROUP BY with columns of the
       single-row DMV pieces (sys.dm_os_sys_memory, sys.dm_exec_requests, ...). The plan read some of those pieces
       once per scheduler row, so a value that moved between reads split the schedulers into several groups and
       the collector stored several rows per collection. Now the aggregate is its own derived table with no GROUP
       BY (an aggregate without GROUP BY always returns exactly one row), cross joined only to single-row pieces,
       and the outer query has no GROUP BY at all. */
    [Fact]
    public void Query_AggregatesSchedulersInADerivedTableWithNoGroupBy_SoACollectionIsOneRow()
    {
        var sql = CpuSchedulerStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(new RecordingCollectorDeltaCalculator())).Text;

        /* No GROUP BY anywhere: the only aggregates are scalar ones (the scheduler derived table, node, offline-CPU,
           workload-group and request pieces), each of which returns exactly one row by itself. */
        Assert.DoesNotContain("GROUP BY", sql);

        /* The scheduler aggregate is one derived table, aliased and read by the select list under its own name,
           and the VISIBLE ONLINE filter sits inside it (the outer query has no WHERE of its own). */
        var derivedStart = sql.IndexOf("FROM\r\n(", StringComparison.Ordinal);
        if (derivedStart < 0)
        {
            derivedStart = sql.IndexOf("FROM\n(", StringComparison.Ordinal);
        }

        Assert.True(derivedStart > 0, "the FROM clause must open with the scheduler derived table");
        var derivedEnd = sql.IndexOf(") AS sched", derivedStart, StringComparison.Ordinal);
        Assert.True(derivedEnd > derivedStart, "the scheduler derived table must be aliased sched");
        var derived = sql[derivedStart..derivedEnd];
        Assert.Contains("SUM(dos.runnable_tasks_count)", derived);
        Assert.Contains("AVG(CONVERT(decimal(38,2), dos.runnable_tasks_count))", derived);
        Assert.Contains("FROM sys.dm_os_schedulers AS dos", derived);
        Assert.Contains("WHERE dos.status = N'VISIBLE ONLINE'", derived);

        /* #5712 review: a scalar aggregate over zero rows is ONE all-NULL row, and ReadAsync's GetInt32(3) throws on it.
           HAVING COUNT_BIG(*) > 0 (still no GROUP BY) makes an instance with no VISIBLE ONLINE scheduler write no row,
           as before the derived table. */
        Assert.Contains("HAVING COUNT_BIG(*) > 0", derived);
        Assert.True(
            derived.IndexOf("WHERE dos.status", StringComparison.Ordinal) < derived.IndexOf("HAVING COUNT_BIG(*) > 0", StringComparison.Ordinal),
            "HAVING follows the VISIBLE ONLINE filter inside the derived table");

        /* Outside the derived table sys.dm_os_schedulers is read only for the offline-CPU count, and no column
           of a single-row piece is aggregated again. */
        var outside = sql[..derivedStart] + sql[derivedEnd..];
        Assert.DoesNotContain("dos.", outside.Replace("dos_offline.", string.Empty));
        Assert.DoesNotContain("SUM(dos", outside);
        Assert.DoesNotContain("MAX(wg.", sql);
        Assert.DoesNotContain("\nWHERE dos.status", sql);
        Assert.Contains("total_runnable_tasks_count = sched.total_runnable_tasks_count", sql);
        Assert.Contains("avg_runnable_tasks_count = sched.avg_runnable_tasks_count", sql);
    }

    /* #5656: the shared collector SQL and the Dashboard's install/17 script both counted the DAC node as
       an offline node (it was filtered by node_id, but the DAC node's id is 64), matched "online" with an
       exact N'ONLINE' (so ONLINE DAC, IDLE and the combined states read as offline), and reported nodes as
       "offline CPUs" while the warning flag counted schedulers. Pin both copies so they cannot drift back. */
    [Theory]
    [InlineData("collector")]
    [InlineData("install")]
    public void NodeAndOfflineCpuCounts_UseStateRulesAndOneSchedulerPredicate(string copy)
    {
        var sql = copy == "collector"
            ? CpuSchedulerStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(new RecordingCollectorDeltaCalculator())).Text
            : CollectionSelect(File.ReadAllText(FindRepoFile(Path.Combine("install", "17_collect_cpu_scheduler_stats.sql"))));

        /* The DAC node is left out by its state, never by a node_id; "online" is every state except OFFLINE. */
        Assert.Contains("n.node_state_desc NOT LIKE N'%DAC%'", sql);
        Assert.Contains("n.node_state_desc NOT LIKE N'OFFLINE%'", sql);
        Assert.DoesNotContain("32767", sql);
        Assert.DoesNotContain("node_id <>", sql);
        Assert.DoesNotContain("node_state_desc = N'ONLINE'", sql);
        Assert.DoesNotContain("node_state_desc <> N'ONLINE'", sql);

        /* Offline CPUs are counted from the user schedulers SQL Server took offline, once, and the warning
           flag reads that same count (no second is_online = 0 subquery that can disagree with it). */
        Assert.Equal(1, CountOccurrences(sql, "= N'VISIBLE OFFLINE'"));
        Assert.DoesNotContain("is_online", sql);
        Assert.Contains("offline_cpu_count = CONVERT(integer, offline_cpus.offline_cpu_count)", sql);
        Assert.Contains("WHEN offline_cpus.offline_cpu_count > 0", sql);
        Assert.DoesNotContain("nodes.offline_cpus", sql);
    }

    [Fact]
    public void InstallScript_DebugMessage_NamesOfflineCpusNotNodes()
    {
        var sql = File.ReadAllText(FindRepoFile(Path.Combine("install", "17_collect_cpu_scheduler_stats.sql")));
        Assert.Contains("Offline CPUs detected: %d offline CPUs", sql);
        Assert.DoesNotContain("offline nodes", sql);
    }

    /* The collection_log row is what an operator reads back after the fact, and the count no longer counts nodes. */
    [Fact]
    public void InstallScript_CollectionLogText_NamesOfflineCpusNotNodes()
    {
        var sql = File.ReadAllText(FindRepoFile(Path.Combine("install", "17_collect_cpu_scheduler_stats.sql")));
        Assert.Contains("N'CRITICAL: Offline CPUs detected'", sql);
        Assert.DoesNotContain("in NUMA nodes", sql);
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

    /* The install proc's collection SELECT, from its first select-list column to its OPTION(RECOMPILE). The
       whole-file DoesNotContain checks (32767, is_online) belong to this slice, not to the rest of the proc. Both
       anchors are asserted, so a rename cannot leave an empty slice that passes every DoesNotContain. */
    private static string CollectionSelect(string installScript)
    {
        const string startAnchor = "max_workers_count = osi.max_workers_count";
        const string endAnchor = "OPTION(RECOMPILE);";
        var start = installScript.IndexOf(startAnchor, StringComparison.Ordinal);
        Assert.True(start >= 0, $"install/17 no longer contains the collection SELECT start anchor: {startAnchor}");
        var end = installScript.IndexOf(endAnchor, start, StringComparison.Ordinal);
        Assert.True(end > start, $"install/17 no longer contains {endAnchor} after the collection SELECT start");
        return installScript.Substring(start, end + endAnchor.Length - start);
    }

    private static int CountOccurrences(string text, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}

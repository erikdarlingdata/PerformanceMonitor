/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the shape of the shared per-collection combine every cpu_scheduler_stats reader uses (#5710): a collection
/// written before the collector fix can be split across several rows that share one <c>collection_time</c>, and the
/// readers fold them back into one. The live arms (Darling's PostgreSQL store, Lite's DuckDB file) run the text; these
/// pins hold its rule where no database is needed.
/// </summary>
public sealed class CpuSchedulerCollectionSqlTests
{
    [Fact]
    public void CombinedColumns_NameEveryColumnTheCollectorStores_UnderItsStoredName()
    {
        foreach (var column in CpuSchedulerStatsCollector.Instance.PayloadColumns)
        {
            Assert.Contains($" AS {column.Name}", CpuSchedulerCollectionSql.CombinedColumns, StringComparison.Ordinal);
        }

        Assert.StartsWith("collection_time,", CpuSchedulerCollectionSql.CombinedColumns.TrimStart(), StringComparison.Ordinal);
    }

    [Fact]
    public void CombinedColumns_FollowTheRule_SumTheCounts_DivideTheAverage_MaxTheSingleRowPieces()
    {
        var sql = CpuSchedulerCollectionSql.CombinedColumns;

        Assert.Contains("CAST(SUM(total_runnable_tasks_count) AS integer) AS total_runnable_tasks_count", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(total_current_workers_count) AS integer) AS total_current_workers_count", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(total_work_queue_count) AS bigint) AS total_work_queue_count", sql, StringComparison.Ordinal);

        /* A one-row collection keeps its stored average; a split one is the summed total over scheduler_count. */
        Assert.Contains("WHEN COUNT(*) = 1", sql, StringComparison.Ordinal);
        Assert.Contains("THEN MAX(avg_runnable_tasks_count)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(total_runnable_tasks_count) AS DECIMAL(38,2)) / NULLIF(", sql, StringComparison.Ordinal);

        /* #5712 L5: the split average is rounded in the SQL, so PostgreSQL (numeric) and DuckDB (double) agree on a half cent. */
        Assert.Contains("CAST(ROUND(CAST(SUM(total_runnable_tasks_count) AS DECIMAL(38,2)) /", sql, StringComparison.Ordinal);
        Assert.Contains(", 2) AS DECIMAL(38,2))", sql, StringComparison.Ordinal);

        /* The divisor is the ONLINE scheduler count: the configured count less the stored offline count. */
        Assert.Contains("GREATEST(MAX(scheduler_count) - COALESCE(MAX(offline_cpu_count), 0), 0)", sql, StringComparison.Ordinal);

        /* The workload-group, memory, request and node columns are single-row pieces: one deterministic value. */
        foreach (var column in new[]
        {
            "max_workers_count", "scheduler_count", "cpu_count", "total_active_request_count", "total_queued_request_count",
            "total_blocked_task_count", "total_active_parallel_thread_count", "runnable_request_count", "total_request_count",
            "runnable_percent", "total_physical_memory_kb", "available_physical_memory_kb", "system_memory_state_desc",
            "total_node_count", "nodes_online_count", "offline_cpu_count",
        })
        {
            Assert.Contains($"MAX({column}) AS {column}", sql, StringComparison.Ordinal);
        }

        /* The two flags built from the summed counts are recomputed for a split collection. */
        Assert.Contains("SUM(total_current_workers_count) >= MAX(max_workers_count) * 0.90", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(total_runnable_tasks_count) >= MAX(cpu_count)", sql, StringComparison.Ordinal);

        /* #5712 L1: the memory flag rises as available memory FALLS, so OR-ing it would follow MIN(available) while the
           read shows MAX(available). It is recomputed from the two shown values, the collector's own rule (below 10 %). */
        Assert.Contains("ELSE MAX(available_physical_memory_kb) < MAX(total_physical_memory_kb) * 0.10", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("BOOL_OR(physical_memory_pressure_warning) AS", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerCollection_GroupsByCollectionTime_AndNewestOnlyFindsTheNewestTimeFirst()
    {
        const string filter = "server_id = $1 AND collection_time >= $2 AND collection_time <= $3";

        var all = CpuSchedulerCollectionSql.PerCollection("v_cpu_scheduler_stats", filter, newestOnly: false);
        Assert.Contains("FROM v_cpu_scheduler_stats", all, StringComparison.Ordinal);
        Assert.Contains("WHERE " + filter, all, StringComparison.Ordinal);
        /* #5712 L4: grouped by server too, so a filter that reaches several servers never merges two of them. */
        Assert.Contains("GROUP BY server_id, collection_time", all, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(collection_time)", all, StringComparison.Ordinal);

        var newest = CpuSchedulerCollectionSql.PerCollection("v_cpu_scheduler_stats", filter, newestOnly: true);
        Assert.Contains("GROUP BY server_id, collection_time", newest, StringComparison.Ordinal);
        Assert.Equal(2, newest.Split(filter).Length - 1);
        Assert.Contains("MAX(collection_time)", newest, StringComparison.Ordinal);
        Assert.Equal(2, newest.Split("FROM v_cpu_scheduler_stats").Length - 1);

        /* Plain SQL both stores run: no PostgreSQL-only casts, no DuckDB-only functions. */
        Assert.DoesNotContain("::", newest, StringComparison.Ordinal);
        Assert.DoesNotContain("DISTINCT ON", newest, StringComparison.Ordinal);
        Assert.False(newest.Contains("LIMIT", StringComparison.Ordinal), "the combine selects its rows by the newest time, not a LIMIT");
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The one statement text that turns the stored <c>cpu_scheduler_stats</c> rows of ONE collection back into ONE row
/// (#5710). A collection should be a single row, but stores written before the collector was fixed hold
/// collections split across several rows that share a <c>collection_time</c>: each row covers only some of the
/// schedulers (4 schedulers stored as 3 + 1, 1 + 2 + 1, even 1 + 1 + 1 + 1), every row still says
/// <c>scheduler_count</c> = 4. A read that takes one row per collection then shows part of the server. Old rows stay
/// as they are (no data rewrite), so every reader combines them with this text. It is plain SQL that PostgreSQL
/// (Darling) and DuckDB (Lite) both run.
///
/// <para><b>The rule.</b> <c>total_runnable_tasks_count</c>, <c>total_work_queue_count</c> and
/// <c>total_current_workers_count</c> are SUMs over the pieces. <c>avg_runnable_tasks_count</c> is the stored
/// average when the collection is one row, and otherwise the summed total over the online scheduler count
/// (the per-scheduler average of the whole server, rounded to 2 places so both engines round a half value alike; the
/// divisor is explained below). Every other column comes from a single-row DMV piece the old
/// collector read once per scheduler row, so its value can differ between the pieces of one collection: the read
/// takes the MAX, one deterministic value per collection. The two warning flags that are computed from the summed
/// counts (<c>worker_thread_exhaustion_warning</c>, <c>runnable_tasks_warning</c>) are recomputed from the combined
/// counts when a collection was split (a flag stored on a partial row says nothing about the whole server), and so is
/// <c>physical_memory_pressure_warning</c>: it rises as available memory FALLS, so OR-ing it would follow the MIN of
/// <c>available_physical_memory_kb</c> while the read shows the MAX. It is recomputed from the two values shown, by the
/// collector's own rule (available below 10 % of total). The other flags (<c>blocked_tasks_warning</c>,
/// <c>queued_requests_warning</c>, <c>offline_cpu_warning</c>) are OR-ed: each rises with the count it comes from, so
/// the OR follows the MAX that the read shows.</para>
///
/// <para><b>One server per group.</b> <see cref="PerCollection"/> groups by <c>server_id</c> as well as
/// <c>collection_time</c>, so a filter that reaches several servers can never add two servers' rows that share a
/// timestamp into one.</para>
///
/// <para><b>The split average's divisor.</b> The collector's own average runs over VISIBLE ONLINE schedulers only.
/// The stored <c>scheduler_count</c> is <c>sys.dm_os_sys_info.scheduler_count</c>, which Microsoft documents as
/// "the number of user schedulers configured in the SQL Server process": a configured scheduler that an affinity mask
/// or an edition core limit has taken offline is still configured, so the count can include VISIBLE OFFLINE
/// schedulers (the docs do not say whether offline ones are left out, so the read assumes they are counted).
/// <c>offline_cpu_count</c> stores the VISIBLE OFFLINE count, so the divisor is the configured
/// count minus the offline count, the best online count the stored row offers (floored at zero, a missing offline
/// count read as 0). On a server with no offline schedulers it equals <c>scheduler_count</c>. If a server reports
/// <c>scheduler_count</c> as online-only, an old split collection on a server with offline schedulers would read high
/// by that subtraction; rows written since the fix are one row and keep the collector's own stored average.</para>
/// </summary>
public static class CpuSchedulerCollectionSql
{
    /// <summary>
    /// The select list of the per-collection combine, one output column per stored column the readers use, under the
    /// stored names and types (integer, bigint, DECIMAL(38,2), boolean, text), plus <c>collection_time</c> (the group
    /// key) and <c>collection_id</c> (the newest id of the collection).
    /// </summary>
    public const string CombinedColumns = """
        collection_time,
        MAX(collection_id) AS collection_id,
        MAX(max_workers_count) AS max_workers_count,
        MAX(scheduler_count) AS scheduler_count,
        MAX(cpu_count) AS cpu_count,
        CAST(SUM(total_runnable_tasks_count) AS integer) AS total_runnable_tasks_count,
        CAST(SUM(total_work_queue_count) AS bigint) AS total_work_queue_count,
        CAST(SUM(total_current_workers_count) AS integer) AS total_current_workers_count,
        CASE
            WHEN COUNT(*) = 1
            THEN MAX(avg_runnable_tasks_count)
            ELSE CAST(ROUND(CAST(SUM(total_runnable_tasks_count) AS DECIMAL(38,2)) / NULLIF(GREATEST(MAX(scheduler_count) - COALESCE(MAX(offline_cpu_count), 0), 0), 0), 2) AS DECIMAL(38,2))
        END AS avg_runnable_tasks_count,
        MAX(total_active_request_count) AS total_active_request_count,
        MAX(total_queued_request_count) AS total_queued_request_count,
        MAX(total_blocked_task_count) AS total_blocked_task_count,
        MAX(total_active_parallel_thread_count) AS total_active_parallel_thread_count,
        MAX(runnable_request_count) AS runnable_request_count,
        MAX(total_request_count) AS total_request_count,
        MAX(runnable_percent) AS runnable_percent,
        CASE
            WHEN COUNT(*) = 1
            THEN BOOL_OR(worker_thread_exhaustion_warning)
            ELSE SUM(total_current_workers_count) >= MAX(max_workers_count) * 0.90
        END AS worker_thread_exhaustion_warning,
        CASE
            WHEN COUNT(*) = 1
            THEN BOOL_OR(runnable_tasks_warning)
            ELSE SUM(total_runnable_tasks_count) >= MAX(cpu_count)
        END AS runnable_tasks_warning,
        BOOL_OR(blocked_tasks_warning) AS blocked_tasks_warning,
        BOOL_OR(queued_requests_warning) AS queued_requests_warning,
        MAX(total_physical_memory_kb) AS total_physical_memory_kb,
        MAX(available_physical_memory_kb) AS available_physical_memory_kb,
        MAX(system_memory_state_desc) AS system_memory_state_desc,
        CASE
            WHEN COUNT(*) = 1
            THEN BOOL_OR(physical_memory_pressure_warning)
            ELSE MAX(available_physical_memory_kb) < MAX(total_physical_memory_kb) * 0.10
        END AS physical_memory_pressure_warning,
        MAX(total_node_count) AS total_node_count,
        MAX(nodes_online_count) AS nodes_online_count,
        MAX(offline_cpu_count) AS offline_cpu_count,
        BOOL_OR(offline_cpu_warning) AS offline_cpu_warning
        """;

    /// <summary>
    /// A statement that returns one row per <c>collection_time</c> from <paramref name="table"/> (the
    /// <c>v_cpu_scheduler_stats</c> view, or the <c>cpu_scheduler_stats</c> table), over the rows
    /// <paramref name="rowFilter"/> keeps (the server and window predicate, written against the bare column names).
    /// <paramref name="newestOnly"/> keeps just the newest collection that filter reaches: the rows of that one
    /// <c>collection_time</c> are looked up by the newest time first, so a long window does not group every row it
    /// holds to answer "what is the latest snapshot". Use it as a derived table or a CTE body.
    /// </summary>
    public static string PerCollection(string table, string rowFilter, bool newestOnly)
    {
        var newest = newestOnly
            ? $"""

              AND   collection_time =
              (
                  SELECT
                      MAX(collection_time)
                  FROM {table}
                  WHERE {rowFilter}
              )
              """
            : string.Empty;

        return $"""
            SELECT
                server_id,
                {CombinedColumns}
            FROM {table}
            WHERE {rowFilter}{newest}
            GROUP BY server_id, collection_time
            """;
    }
}

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
/// average when the collection is one row, and otherwise the summed total over scheduler_count
/// (the per-scheduler average of the whole server, rounded to 2 places so both engines round a half value alike; the
/// divisor is explained below). Every other column comes from a single-row DMV piece the old
/// collector read once per scheduler row, so its value can differ between the pieces of one collection: the read
/// takes the MAX, or, for the request and memory groups that must agree with each other, one stored row's values (below). The two warning flags that are computed from the summed
/// counts (<c>worker_thread_exhaustion_warning</c>, <c>runnable_tasks_warning</c>) are recomputed from the combined
/// counts when a collection was split (a flag stored on a partial row says nothing about the whole server), and so is
/// <c>physical_memory_pressure_warning</c>: it is recomputed from the available and total memory shown (one row's pair),
/// by the collector's own rule (available below 10 % of total, and not while the engine's high-memory signal is set; the signal is read
/// from the shown row's <c>system_memory_state_desc</c>, the only form the store keeps). The other flags (<c>blocked_tasks_warning</c>,
/// <c>queued_requests_warning</c>, <c>offline_cpu_warning</c>) are OR-ed: each rises with the count it comes from, so
/// the OR follows the MAX that the read shows.</para>
///
/// <para><b>One server per group.</b> <see cref="PerCollection"/> groups by <c>server_id</c> as well as
/// <c>collection_time</c>, so a filter that reaches several servers can never add two servers' rows that share a
/// timestamp into one.</para>
///
/// <para><b>The split average's divisor.</b> The collector's own average runs over VISIBLE ONLINE schedulers only, and
/// the stored <c>scheduler_count</c> (<c>sys.dm_os_sys_info.scheduler_count</c>) IS that online count: measured on a
/// 4-CPU SQL Server 2019 VM, <c>ALTER SERVER CONFIGURATION SET PROCESS AFFINITY CPU = 0</c> gave cpu_count 4,
/// scheduler_count 1, VISIBLE ONLINE 1, VISIBLE OFFLINE 3 (back on AUTO: 4, 4, 4, 0). So the divisor is
/// <c>NULLIF(MAX(scheduler_count), 0)</c> and nothing is subtracted for <c>offline_cpu_count</c>; an earlier draft
/// subtracted it and produced NULL on every server that has offline schedulers.</para>
///
/// <para><b>Correlated columns come from ONE stored row.</b> <c>runnable_request_count</c>, <c>total_request_count</c>
/// and <c>runnable_percent</c> are one reading of the collector's <c>dm_exec_requests</c> piece and are shown as a triple, so
/// independent maxima could print a percent that contradicts the two counts. A split collection takes all three from the
/// row with the most requests (ties: the most runnable requests, then the newest <c>collection_id</c>). The
/// <c>sys.dm_os_sys_memory</c> group (<c>total_physical_memory_kb</c>, <c>available_physical_memory_kb</c>,
/// <c>system_memory_state_desc</c>) comes from the row with the LEAST available memory, and the pressure flag is recomputed from
/// that same row's pair. The pick is <c>(ARRAY_AGG(col ORDER BY ...))[1]</c>, which PostgreSQL and DuckDB both run.</para>
/// </summary>
public static class CpuSchedulerCollectionSql
{
    /// <summary>
    /// Orders the rows of one collection for the <c>dm_exec_requests</c> group (<c>runnable_request_count</c>,
    /// <c>total_request_count</c>, <c>runnable_percent</c>): the row with the most requests wins, then the most runnable
    /// ones, then the newest <c>collection_id</c>, so the three columns come from ONE stored row (#5710 review).
    /// <c>NULLS LAST</c> is spelled out because PostgreSQL sorts NULLs first on DESC and DuckDB last.
    /// </summary>
    private const string RequestRowOrder =
        "ORDER BY total_request_count DESC NULLS LAST, runnable_request_count DESC NULLS LAST, collection_id DESC";

    /// <summary>
    /// Orders the rows of one collection for the <c>sys.dm_os_sys_memory</c> group (<c>total_physical_memory_kb</c>,
    /// <c>available_physical_memory_kb</c>, <c>system_memory_state_desc</c>): the row with the LEAST available memory
    /// wins (the pressure reading), then the most total memory, then the newest <c>collection_id</c>.
    /// </summary>
    private const string MemoryRowOrder =
        "ORDER BY available_physical_memory_kb ASC NULLS LAST, total_physical_memory_kb DESC NULLS LAST, collection_id DESC";

    /// <summary>
    /// The <c>system_memory_state_desc</c> the engine reports while <c>sys.dm_os_sys_memory.system_high_memory_signal_state</c> is 1,
    /// as a SQL literal. The collector's pressure rule (available below 10 % of total) does not hold while that signal is set, and
    /// the store keeps the description, not the bit (no migration), so a split collection's recompute reads the signal from the
    /// description of the row it shows. A description in another wording leaves the flag as the 10 % rule alone gives it.
    /// </summary>
    private const string HighMemoryStateDesc = "'Available physical memory is high'";

    /// <summary>
    /// The select list of the per-collection combine, one output column per stored column the readers use, under the
    /// stored names and types (integer, bigint, DECIMAL(38,2), boolean, text), plus <c>collection_time</c> (the group
    /// key) and <c>collection_id</c> (the newest id of the collection).
    /// </summary>
    public const string CombinedColumns = $"""
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
            /* scheduler_count IS the online count: measured on a 4-CPU SQL Server 2019 VM, AFFINITY CPU = 0 gave cpu_count 4, scheduler_count 1, VISIBLE OFFLINE 3. */
            ELSE CAST(ROUND(CAST(SUM(total_runnable_tasks_count) AS DECIMAL(38,2)) / NULLIF(MAX(scheduler_count), 0), 2) AS DECIMAL(38,2))
        END AS avg_runnable_tasks_count,
        MAX(total_active_request_count) AS total_active_request_count,
        MAX(total_queued_request_count) AS total_queued_request_count,
        MAX(total_blocked_task_count) AS total_blocked_task_count,
        MAX(total_active_parallel_thread_count) AS total_active_parallel_thread_count,
        (ARRAY_AGG(runnable_request_count {RequestRowOrder}))[1] AS runnable_request_count,
        (ARRAY_AGG(total_request_count {RequestRowOrder}))[1] AS total_request_count,
        (ARRAY_AGG(runnable_percent {RequestRowOrder}))[1] AS runnable_percent,
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
        (ARRAY_AGG(total_physical_memory_kb {MemoryRowOrder}))[1] AS total_physical_memory_kb,
        (ARRAY_AGG(available_physical_memory_kb {MemoryRowOrder}))[1] AS available_physical_memory_kb,
        (ARRAY_AGG(system_memory_state_desc {MemoryRowOrder}))[1] AS system_memory_state_desc,
        CASE
            WHEN COUNT(*) = 1
            THEN BOOL_OR(physical_memory_pressure_warning)
            ELSE (ARRAY_AGG(available_physical_memory_kb {MemoryRowOrder}))[1] < (ARRAY_AGG(total_physical_memory_kb {MemoryRowOrder}))[1] * 0.10
                 AND (ARRAY_AGG(system_memory_state_desc {MemoryRowOrder}))[1] IS DISTINCT FROM {HighMemoryStateDesc}
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

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Services;

public sealed partial class LocalDataService
{
    /// <summary>
    /// #9195: the ranking statement of <see cref="GetQueryStoreTopQueriesAsync"/> at one candidate limit, with <c>v_query_store_stats</c>
    /// still in it (<see cref="SwapArchiveView"/> swaps the hot table in when the archive has nothing in the window). A method of
    /// its own so a test can run it under EXPLAIN ANALYZE and see what the Parquet scans read. $1 is the server, $2 and $3 the window.
    /// </summary>
    internal static string QueryStoreTopSql(string dbClause, string executionTypeClause, string moduleClause, int candidates)
    {
        return @"
WITH latest AS (
    /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
       per-Query-Store-interval snapshots, and the collector re-fetches the OPEN interval every cycle
       as its last_execution_time advances, so the SAME interval is stored repeatedly with a growing
       execution_count. SUM(execution_count) over the raw rows reports 10 + 25 + 40 for an interval that
       reached 40, and the execution-weighted averages would count a re-collected interval again at its own
       execution count. Keep the LATEST snapshot per interval. The aggregate below projects nearly every
       payload column, so they are listed in `deduped`; #5381: query_text and query_plan_text are NOT among them.
       The text is read after the ranking, for the page's rows only (below).

       runtime_stats_interval_id is the REAL interval identity (tier 2), with first_execution_time kept
       beside it as the tier-1 proxy for rows collected before it existed — see the slicer above for why
       both are in the key rather than a COALESCE of the two.

       replica_role and execution_type_desc are in the partition because the aggregate below is grouped
       (or MAXed) on them: the dedup key must be at least as fine as the read's own row identity, or
       dedup would silently drop a row the grid is supposed to show rather than de-duplicate one.

       #5381: the window runs over the NARROW columns only (the row's id and time), and the payload comes back by a
       join on those two for the winning rows. Running the window over the payload sorted every column of every row
       in the window, which is the part of this read that grows with the window, and DuckDB keeps a copy of it per
       thread. The join's build side is one narrow row per interval. */
    /* #9195: the newest snapshot per interval by a hash aggregate on the narrow columns, not ROW_NUMBER() over them: the window sorts
       every row of the window (about 4x the aggregate's time on 480,000 rows) and a server-wide read has the most rows. arg_max picks
       the greatest (collection_time, execution_count), the row the window's ORDER BY collection_time DESC, execution_count DESC put first,
       and the NULL-safe grouping is the window partition's. execution_count is nullable and DuckDB orders a NULL struct field above every
       value, so COALESCE(execution_count, -1) sorts a NULL count lowest (a count is never below 0), which keeps the window's NULLS LAST
       order on an execution_count tie (#9195). collection_time cannot be NULL here, the bounds below drop those rows. */
    SELECT
        arg_max(collection_id, (collection_time, COALESCE(execution_count, -1))) AS latest_id,
        MAX(collection_time) AS latest_time
    FROM v_query_store_stats
    WHERE server_id = $1
    /* #7776: the window is the interval's own start, the expression the slicer's bars are keyed on (#1892), so a slice's rows are
       the intervals its bars draw: collection_time trails the interval's start by up to its length, which showed an interval
       collected inside the slice but started before it (no bar) and dropped the slice's last interval, collected after its end.
       Legacy rows have no interval start and keep collection_time. The collection_time bounds are the slicer read's pruning
       bounds (see GetQueryStoreSlicerDataAsync for the reasoning), not filters. */
    AND   COALESCE(interval_start_time_utc, collection_time) >= $2
    AND   COALESCE(interval_start_time_utc, collection_time) < $3
    AND   collection_time >= $2 - INTERVAL '1 day'
    AND   collection_time <= $3 + INTERVAL '30 days'" + dbClause + executionTypeClause + @"

    GROUP BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
),
deduped AS (
    SELECT
        database_name, query_id, plan_id, query_hash, replica_role, module_name, execution_type_desc,
        runtime_stats_interval_id, first_execution_time, last_execution_time, collection_time,
        execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
        avg_physical_io_reads, avg_rowcount, min_dop, max_dop, query_plan_hash, is_forced_plan,
        plan_forcing_type, avg_clr_time_us, avg_tempdb_space_used, avg_log_bytes_used, plan_type,
        force_failure_count, last_force_failure_reason, compatibility_level, min_duration_us,
        max_duration_us, min_cpu_time_us, max_cpu_time_us, min_logical_io_reads, max_logical_io_reads,
        min_logical_io_writes, max_logical_io_writes, min_physical_io_reads, max_physical_io_reads,
        min_clr_time_us, max_clr_time_us, min_rowcount, max_rowcount, min_log_bytes_used,
        max_log_bytes_used, min_tempdb_space_used, max_tempdb_space_used, avg_query_max_used_memory,
        min_query_max_used_memory, max_query_max_used_memory, avg_num_physical_io_reads,
        min_num_physical_io_reads, max_num_physical_io_reads
    FROM v_query_store_stats
    INNER JOIN latest ON latest.latest_id = collection_id AND latest.latest_time = collection_time
    WHERE server_id = $1
    /* #7776: the same window as the ranking above; the join on (id, time) already picks the winning snapshots, so this only has to keep them. */
    AND   COALESCE(interval_start_time_utc, collection_time) >= $2
    AND   COALESCE(interval_start_time_utc, collection_time) < $3
    AND   collection_time >= $2 - INTERVAL '1 day'
    AND   collection_time <= $3 + INTERVAL '30 days'" + dbClause + executionTypeClause + @"
),
ranked AS (
    SELECT
        database_name,
        query_id,
        plan_id,
        query_hash,
        /* A GROUP BY key, not MAX(): with Query Store for secondary replicas (2022+) the primary holds
           ONE shared Query Store carrying every replica's rows, so grouping without it would average
           primary and secondary workload into a single blended row — the conflation this column exists
           to expose. Grouping splits them into one row per replica role instead. On a standalone/non-AG
           server every row shares one value (NULL, or 'Primary' on 2025), so the grouping is a no-op
           and the grid is unchanged. */
        replica_role,
        MAX(module_name) AS module_name,
        /* A GROUP BY key, like replica_role above: Query Store keeps Regular, Aborted and Exception executions
           of one plan in separate runtime-stats rows, and MAX() here showed Regular for any mixed group (it
           sorts last) while the averages blended a timeout's duration into the plan's normal cost. One row per
           outcome instead. The execution_type filter is applied in deduped, before the ROW_NUMBER: the column
           is in the partition, so filtering first cannot change which row wins. */
        execution_type_desc,
        SUM(execution_count) AS total_executions,
        SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_duration_ms,
        SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_cpu_time_ms,
        SUM(CAST(avg_logical_io_reads AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) AS avg_logical_reads,
        SUM(CAST(avg_logical_io_writes AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) AS avg_logical_writes,
        SUM(CAST(avg_physical_io_reads AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) AS avg_physical_reads,
        SUM(CAST(avg_rowcount AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) AS avg_rowcount,
        MIN(min_dop) AS min_dop,
        MAX(max_dop) AS max_dop,
        MAX(last_execution_time) AS last_execution_time,
        MAX(query_plan_hash) AS query_plan_hash,
        MAX(CASE WHEN is_forced_plan THEN TRUE ELSE FALSE END) AS is_forced_plan,
        MAX(plan_forcing_type) AS plan_forcing_type,
        MIN(first_execution_time) AS first_execution_time,
        SUM(CAST(avg_clr_time_us AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_clr_time_ms,
        SUM(CAST(avg_tempdb_space_used AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) AS avg_tempdb_space_used,
        SUM(CAST(avg_log_bytes_used AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) AS avg_log_bytes_used,
        MAX(plan_type) AS plan_type,
        MAX(force_failure_count) AS force_failure_count,
        MAX(last_force_failure_reason) AS last_force_failure_reason,
        MAX(compatibility_level) AS compatibility_level,
        MIN(CAST(min_duration_us AS DOUBLE PRECISION)) / 1000.0 AS min_duration_ms,
        MAX(CAST(max_duration_us AS DOUBLE PRECISION)) / 1000.0 AS max_duration_ms,
        MIN(CAST(min_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS min_cpu_time_ms,
        MAX(CAST(max_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS max_cpu_time_ms,
        MIN(CAST(min_logical_io_reads AS DOUBLE PRECISION)) AS min_logical_reads,
        MAX(CAST(max_logical_io_reads AS DOUBLE PRECISION)) AS max_logical_reads,
        MIN(CAST(min_logical_io_writes AS DOUBLE PRECISION)) AS min_logical_writes,
        MAX(CAST(max_logical_io_writes AS DOUBLE PRECISION)) AS max_logical_writes,
        MIN(CAST(min_physical_io_reads AS DOUBLE PRECISION)) AS min_physical_reads,
        MAX(CAST(max_physical_io_reads AS DOUBLE PRECISION)) AS max_physical_reads,
        MIN(CAST(min_clr_time_us AS DOUBLE PRECISION)) / 1000.0 AS min_clr_time_ms,
        MAX(CAST(max_clr_time_us AS DOUBLE PRECISION)) / 1000.0 AS max_clr_time_ms,
        MIN(CAST(min_rowcount AS DOUBLE PRECISION)) AS min_rowcount,
        MAX(CAST(max_rowcount AS DOUBLE PRECISION)) AS max_rowcount,
        MIN(CAST(min_log_bytes_used AS DOUBLE PRECISION)) AS min_log_bytes_used,
        MAX(CAST(max_log_bytes_used AS DOUBLE PRECISION)) AS max_log_bytes_used,
        MIN(CAST(min_tempdb_space_used AS DOUBLE PRECISION)) AS min_tempdb_space_used,
        MAX(CAST(max_tempdb_space_used AS DOUBLE PRECISION)) AS max_tempdb_space_used,
        SUM(CAST(avg_query_max_used_memory AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) * 8.0 / 1024.0 AS avg_memory_mb,
        MIN(CAST(min_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS min_memory_mb,
        MAX(CAST(max_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS max_memory_mb,
        SUM(CAST(avg_num_physical_io_reads AS DOUBLE PRECISION) * execution_count) / NULLIF(SUM(execution_count), 0) AS avg_num_physical_io_reads,
        MIN(CAST(min_num_physical_io_reads AS DOUBLE PRECISION)) AS min_num_physical_io_reads,
        MAX(CAST(max_num_physical_io_reads AS DOUBLE PRECISION)) AS max_num_physical_io_reads
    FROM deduped
    WHERE 1 = 1" + moduleClause + @"
    GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
    /* #5299 round 3 (O16): the ranking ends on the whole group key - see GetTopQueriesByCpuAsync. */
    ORDER BY SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
    LIMIT " + candidates + @"
),
/* #5381: `ranked` is read ONCE, here. Referencing it a second time (for a candidate count, or to find each candidate's
   text row) makes DuckDB materialise it and hold a second copy of its window and aggregate state, which doubled this
   read's peak memory (753 MB against the old read's 726 MB over a 7 day window at 1 GB). The count is the number of
   rows this statement returns, and each candidate's latest text row is found by a second statement over the
   candidates' keys (ReadQueryStoreLatestRowsAsync), so nothing here looks at the whole archive. */
page AS (
SELECT
    r.database_name,
    r.query_id,
    r.plan_id,
    r.query_hash,
    CAST(NULL AS VARCHAR) AS query_text,
    r.module_name,
    r.total_executions,
    r.avg_duration_ms,
    r.avg_cpu_time_ms,
    r.avg_logical_reads,
    r.avg_logical_writes,
    r.avg_physical_reads,
    r.avg_rowcount,
    r.min_dop,
    r.max_dop,
    r.last_execution_time,
    r.query_plan_hash,
    r.is_forced_plan,
    r.plan_forcing_type,
    r.execution_type_desc,
    r.first_execution_time,
    r.avg_clr_time_ms,
    r.avg_tempdb_space_used,
    r.avg_log_bytes_used,
    r.plan_type,
    r.force_failure_count,
    r.last_force_failure_reason,
    r.compatibility_level,
    r.min_duration_ms,
    r.max_duration_ms,
    r.min_cpu_time_ms,
    r.max_cpu_time_ms,
    r.min_logical_reads,
    r.max_logical_reads,
    r.min_logical_writes,
    r.max_logical_writes,
    r.min_physical_reads,
    r.max_physical_reads,
    r.min_clr_time_ms,
    r.max_clr_time_ms,
    r.min_rowcount,
    r.max_rowcount,
    r.min_log_bytes_used,
    r.max_log_bytes_used,
    r.min_tempdb_space_used,
    r.max_tempdb_space_used,
    r.avg_memory_mb,
    r.min_memory_mb,
    r.max_memory_mb,
    r.avg_num_physical_io_reads,
    r.min_num_physical_io_reads,
    r.max_num_physical_io_reads,
    r.replica_role,
    ROW_NUMBER() OVER (ORDER BY r.total_executions * r.avg_duration_ms DESC, r.database_name, r.query_id, r.plan_id, r.query_hash, r.execution_type_desc, r.replica_role) AS page_ord
FROM ranked r
)
/* #5313: a round reports how many candidates its ranking produced, so TopFill knows whether more exist.
   #5381: that count is the number of rows returned here (the page is not trimmed in SQL any more), and the WAITFOR
   filter and the page limit need the text, so they run in code after the text read, over the candidates in page_ord
   order - the same rows in the same order the old statement's WHERE and LIMIT $4 kept. */
SELECT p.*
FROM page p
ORDER BY p.page_ord";
    }
}

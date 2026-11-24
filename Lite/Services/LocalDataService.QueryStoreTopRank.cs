/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information
 */

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>The first half of the Query Store top read's ranking statement: the newest snapshot of each interval (#9195).</summary>
    internal static string QueryStoreTopRankHeadSql(string dbClause, string executionTypeClause) => @"
WITH deduped AS (
    /* LOAD-BEARING (correctness, not just perf) - #1841. query_store_stats rows are CUMULATIVE
       per-Query-Store-interval snapshots, and the collector re-fetches the OPEN interval every cycle
       as its last_execution_time advances, so the SAME interval is stored repeatedly with a growing
       execution_count. SUM(execution_count) over the raw rows reports 10 + 25 + 40 for an interval that
       reached 40, and the execution-weighted averages would count a re-collected interval again at its own
       execution count. Keep the LATEST snapshot per interval.

       runtime_stats_interval_id is the REAL interval identity (tier 2), with first_execution_time kept
       beside it as the tier-1 proxy for rows collected before it existed - see the slicer above for why
       both are in the key rather than a COALESCE of the two.

       replica_role and execution_type_desc are in the key because the aggregate below is grouped
       (or MAXed) on them: the dedup key must be at least as fine as the read's own row identity, or
       dedup would silently drop a row the grid is supposed to show rather than de-duplicate one.

       #5381: query_text and query_plan_text are NOT carried. The text is read after the ranking, for the page's rows only (below).

       #9195: ONE pass over the rows, one max() per interval key. This read used to take each interval's newest row by an arg_max on
       the narrow columns and then scan the same rows a second time, wide, and join them back to that result on (collection_id,
       collection_time): the second scan of the archive's Parquet was the largest operator of the read, with the join and the
       group-by on the dedup after it. The newest snapshot of an interval is the largest (collection_time, execution_count,
       collection_id), so max() of one struct that lists those first and the payload the ranking needs after them gives the winner
       row's own fields, and nothing is read twice. execution_count is nullable and DuckDB orders a NULL above every value, so
       COALESCE(execution_count, -1) sorts a NULL count lowest (a count is never below 0), which keeps the window's NULLS LAST order
       on a count tie. collection_time cannot be NULL here, the bounds below drop those rows. collection_id is unique per row
       (CollectionIdGenerator), so the struct picks one row: two rows of an interval with one (collection_time, execution_count)
       used to give an arbitrary winner and give the higher collection_id now, and a row held by both the hot table and the archive
       (as one can be between an archival's copy and its delete) is the same row twice and is counted once, where the join returned
       it twice. */
    SELECT
        database_name, query_id, plan_id,
        replica_role, execution_type_desc, first_execution_time,
        w.collection_time AS collection_time, w.execution_count AS execution_count, w.query_hash AS query_hash,
        w.module_name AS module_name, w.last_execution_time AS last_execution_time, w.avg_duration_us AS avg_duration_us,
        w.avg_cpu_time_us AS avg_cpu_time_us, w.avg_logical_io_reads AS avg_logical_io_reads, w.avg_logical_io_writes AS avg_logical_io_writes,
        w.avg_physical_io_reads AS avg_physical_io_reads, w.avg_rowcount AS avg_rowcount, w.min_dop AS min_dop,
        w.max_dop AS max_dop, w.query_plan_hash AS query_plan_hash, w.is_forced_plan AS is_forced_plan,
        w.plan_forcing_type AS plan_forcing_type, w.avg_clr_time_us AS avg_clr_time_us, w.avg_tempdb_space_used AS avg_tempdb_space_used,
        w.avg_log_bytes_used AS avg_log_bytes_used, w.plan_type AS plan_type, w.force_failure_count AS force_failure_count,
        w.last_force_failure_reason AS last_force_failure_reason, w.compatibility_level AS compatibility_level, w.min_duration_us AS min_duration_us,
        w.max_duration_us AS max_duration_us, w.min_cpu_time_us AS min_cpu_time_us, w.max_cpu_time_us AS max_cpu_time_us,
        w.min_logical_io_reads AS min_logical_io_reads, w.max_logical_io_reads AS max_logical_io_reads, w.min_logical_io_writes AS min_logical_io_writes,
        w.max_logical_io_writes AS max_logical_io_writes, w.min_physical_io_reads AS min_physical_io_reads, w.max_physical_io_reads AS max_physical_io_reads,
        w.min_clr_time_us AS min_clr_time_us, w.max_clr_time_us AS max_clr_time_us, w.min_rowcount AS min_rowcount,
        w.max_rowcount AS max_rowcount, w.min_log_bytes_used AS min_log_bytes_used, w.max_log_bytes_used AS max_log_bytes_used,
        w.min_tempdb_space_used AS min_tempdb_space_used, w.max_tempdb_space_used AS max_tempdb_space_used, w.avg_query_max_used_memory AS avg_query_max_used_memory,
        w.min_query_max_used_memory AS min_query_max_used_memory, w.max_query_max_used_memory AS max_query_max_used_memory, w.avg_num_physical_io_reads AS avg_num_physical_io_reads,
        w.min_num_physical_io_reads AS min_num_physical_io_reads, w.max_num_physical_io_reads AS max_num_physical_io_reads
    FROM
    (
        SELECT
            database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role,
            max(
            {
                'collection_time': collection_time,
                'execution_rank': COALESCE(execution_count, -1),
                'collection_id': collection_id,
                'execution_count': execution_count,
                'query_hash': query_hash,
                'module_name': module_name,
                'last_execution_time': last_execution_time,
                'avg_duration_us': avg_duration_us,
                'avg_cpu_time_us': avg_cpu_time_us,
                'avg_logical_io_reads': avg_logical_io_reads,
                'avg_logical_io_writes': avg_logical_io_writes,
                'avg_physical_io_reads': avg_physical_io_reads,
                'avg_rowcount': avg_rowcount,
                'min_dop': min_dop,
                'max_dop': max_dop,
                'query_plan_hash': query_plan_hash,
                'is_forced_plan': is_forced_plan,
                'plan_forcing_type': plan_forcing_type,
                'avg_clr_time_us': avg_clr_time_us,
                'avg_tempdb_space_used': avg_tempdb_space_used,
                'avg_log_bytes_used': avg_log_bytes_used,
                'plan_type': plan_type,
                'force_failure_count': force_failure_count,
                'last_force_failure_reason': last_force_failure_reason,
                'compatibility_level': compatibility_level,
                'min_duration_us': min_duration_us,
                'max_duration_us': max_duration_us,
                'min_cpu_time_us': min_cpu_time_us,
                'max_cpu_time_us': max_cpu_time_us,
                'min_logical_io_reads': min_logical_io_reads,
                'max_logical_io_reads': max_logical_io_reads,
                'min_logical_io_writes': min_logical_io_writes,
                'max_logical_io_writes': max_logical_io_writes,
                'min_physical_io_reads': min_physical_io_reads,
                'max_physical_io_reads': max_physical_io_reads,
                'min_clr_time_us': min_clr_time_us,
                'max_clr_time_us': max_clr_time_us,
                'min_rowcount': min_rowcount,
                'max_rowcount': max_rowcount,
                'min_log_bytes_used': min_log_bytes_used,
                'max_log_bytes_used': max_log_bytes_used,
                'min_tempdb_space_used': min_tempdb_space_used,
                'max_tempdb_space_used': max_tempdb_space_used,
                'avg_query_max_used_memory': avg_query_max_used_memory,
                'min_query_max_used_memory': min_query_max_used_memory,
                'max_query_max_used_memory': max_query_max_used_memory,
                'avg_num_physical_io_reads': avg_num_physical_io_reads,
                'min_num_physical_io_reads': min_num_physical_io_reads,
                'max_num_physical_io_reads': max_num_physical_io_reads
            }) AS w
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
    ) AS g
),
";

    /// <summary>The second half: the ranking over those rows, the candidate cap and the page order. It reads <c>deduped</c>.</summary>
    internal static string QueryStoreTopRankTailSql(string moduleClause, int candidates) => @"
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

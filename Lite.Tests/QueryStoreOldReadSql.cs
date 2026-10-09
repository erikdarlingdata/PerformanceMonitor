/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace Lite.Tests;

/// <summary>
/// #5381: the three Query Store reads exactly as they were before query_text was moved after the ranking, kept
/// verbatim as a test-only ORACLE. <see cref="QueryStoreTextAfterRankingTests"/> runs these beside the shipped reads
/// to prove the rewrite changes no row, and at a low memory_limit to prove the old shape runs out of memory where the
/// new one does not. Do not edit these to follow the product SQL: their whole value is that they are the old text.
/// The optional database / execution-type / module filters are left out (no filter is the case under test);
/// <c>@@CANDIDATES@@</c> stands for the candidate limit the top-queries round was built with.
/// </summary>
internal static class QueryStoreOldReadSql
{
    /// <summary>GetQueryStoreTopQueriesAsync, one round. Parameters: $1 server, $2 start, $3 end, $4 page size.</summary>
    internal const string TopQueries = """
        
WITH deduped AS (
    /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
       per-Query-Store-interval snapshots, and the collector re-fetches the OPEN interval every cycle
       as its last_execution_time advances, so the SAME interval is stored repeatedly with a growing
       execution_count. SUM(execution_count) over the raw rows reports 10 + 25 + 40 for an interval that
       reached 40, and AVG(avg_*) becomes an avg-of-avgs weighted by how many times each interval happened
       to be re-collected. Keep the LATEST snapshot per interval. SELECT * because the aggregate below
       projects nearly every payload column.

       runtime_stats_interval_id is the REAL interval identity (tier 2), with first_execution_time kept
       beside it as the tier-1 proxy for rows collected before it existed — see the slicer above for why
       both are in the key rather than a COALESCE of the two.

       replica_role and execution_type_desc are in the partition because the aggregate below is grouped
       (or MAXed) on them: the dedup key must be at least as fine as the read's own row identity, or
       dedup would silently drop a row the grid is supposed to show rather than de-duplicate one. */
    SELECT
        *,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
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
        AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS DOUBLE PRECISION)) AS avg_logical_reads,
        AVG(CAST(avg_logical_io_writes AS DOUBLE PRECISION)) AS avg_logical_writes,
        AVG(CAST(avg_physical_io_reads AS DOUBLE PRECISION)) AS avg_physical_reads,
        AVG(CAST(avg_rowcount AS DOUBLE PRECISION)) AS avg_rowcount,
        MIN(min_dop) AS min_dop,
        MAX(max_dop) AS max_dop,
        MAX(last_execution_time) AS last_execution_time,
        MAX(query_plan_hash) AS query_plan_hash,
        MAX(CASE WHEN is_forced_plan THEN TRUE ELSE FALSE END) AS is_forced_plan,
        MAX(plan_forcing_type) AS plan_forcing_type,
        MIN(first_execution_time) AS first_execution_time,
        AVG(CAST(avg_clr_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_clr_time_ms,
        AVG(CAST(avg_tempdb_space_used AS DOUBLE PRECISION)) AS avg_tempdb_space_used,
        AVG(CAST(avg_log_bytes_used AS DOUBLE PRECISION)) AS avg_log_bytes_used,
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
        AVG(CAST(avg_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS avg_memory_mb,
        MIN(CAST(min_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS min_memory_mb,
        MAX(CAST(max_query_max_used_memory AS DOUBLE PRECISION)) * 8.0 / 1024.0 AS max_memory_mb,
        AVG(CAST(avg_num_physical_io_reads AS DOUBLE PRECISION)) AS avg_num_physical_io_reads,
        MIN(CAST(min_num_physical_io_reads AS DOUBLE PRECISION)) AS min_num_physical_io_reads,
        MAX(CAST(max_num_physical_io_reads AS DOUBLE PRECISION)) AS max_num_physical_io_reads
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
    /* #5299 round 3 (O16): the ranking ends on the whole group key - see GetTopQueriesByCpuAsync. */
    ORDER BY SUM(execution_count) * AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
    LIMIT @@CANDIDATES@@
),
page AS (
SELECT
    r.database_name,
    r.query_id,
    r.plan_id,
    r.query_hash,
    t.query_text,
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
    NULL AS query_plan_text,
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
LEFT JOIN LATERAL (
    SELECT query_text
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   query_id = r.query_id
    AND   database_name = r.database_name
    AND   query_text IS NOT NULL
    /* #5299 round 2 (N3): a tie on the time breaks on the row collected last - see GetTopQueriesByCpuAsync. */
    ORDER BY collection_time DESC, collection_id DESC
    LIMIT 1
) t ON TRUE
WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
ORDER BY r.total_executions * r.avg_duration_ms DESC, r.database_name, r.query_id, r.plan_id, r.query_hash, r.execution_type_desc, r.replica_role
LIMIT $4
)
/* #5313: the count row rides beside the page so a round trimmed to nothing still reports whether more candidates exist. */
SELECT p.*, c.candidate_count
FROM (SELECT COUNT(*) AS candidate_count FROM ranked) c
LEFT JOIN page p ON TRUE
ORDER BY p.page_ord
""";

    /// <summary>GetQueryStoreComparisonAsync. Parameters: $1 server, $2 current start, $3 current end, $4 baseline start, $5 baseline end.</summary>
    internal const string Comparison = """
        
WITH deduped_current AS (
    /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
       per-Query-Store-interval snapshots and the collector re-fetches the OPEN interval every cycle,
       so the SAME interval (same first_execution_time) is stored repeatedly with a growing
       execution_count. Keep the LATEST snapshot per interval before aggregating.

       The execution-count weighting below does NOT rescue this on its own: the repeated snapshots of
       one interval carry DIFFERENT (growing) weights AND different avg_* values, so an open interval
       is weighted by the triangular sum of its own growth. That bias is stronger in the recent window
       than in the baseline window (recent windows hold more still-open intervals), which skews the
       delta this comparison exists to compute. One deduped CTE per window, so both arms are treated
       identically. */
    SELECT
        database_name,
        query_hash,
        query_text,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2 AND collection_time <= $3
),
deduped_baseline AS (
    SELECT
        database_name,
        query_hash,
        query_text,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $4 AND collection_time <= $5
),
top_current AS (
    SELECT database_name, query_hash
    FROM deduped_current
    WHERE rn = 1
    AND   execution_count > 0
    GROUP BY database_name, query_hash
    ORDER BY SUM(execution_count) DESC
    LIMIT 100
),
top_baseline AS (
    SELECT database_name, query_hash
    FROM deduped_baseline
    WHERE rn = 1
    AND   execution_count > 0
    GROUP BY database_name, query_hash
    ORDER BY SUM(execution_count) DESC
    LIMIT 100
),
top_hashes AS (
    SELECT DISTINCT database_name, query_hash
    FROM (
        SELECT * FROM top_current
        UNION ALL
        SELECT * FROM top_baseline
    ) combined
),
current_period AS (
    SELECT th.database_name, th.query_hash,
           SUM(qs.execution_count) AS exec_count,
           SUM(qs.execution_count * qs.avg_duration_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.execution_count * qs.avg_cpu_time_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.execution_count * qs.avg_logical_io_reads::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN deduped_current qs
      ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
      AND qs.database_name IS NOT DISTINCT FROM th.database_name
    WHERE qs.rn = 1
    AND   qs.execution_count > 0
    GROUP BY th.database_name, th.query_hash
),
baseline_period AS (
    SELECT th.database_name, th.query_hash,
           SUM(qs.execution_count) AS exec_count,
           SUM(qs.execution_count * qs.avg_duration_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.execution_count * qs.avg_cpu_time_us::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.execution_count * qs.avg_logical_io_reads::DOUBLE PRECISION) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN deduped_baseline qs
      ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
      AND qs.database_name IS NOT DISTINCT FROM th.database_name
    WHERE qs.rn = 1
    AND   qs.execution_count > 0
    GROUP BY th.database_name, th.query_hash
)
SELECT COALESCE(c.database_name, b.database_name) AS database_name,
       COALESCE(c.query_hash, b.query_hash) AS query_hash,
       COALESCE(c.query_text, b.query_text) AS query_text,
       c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
       b.exec_count AS baseline_exec_count,
       b.avg_duration_ms AS baseline_avg_duration_ms,
       b.avg_cpu_ms AS baseline_avg_cpu_ms,
       b.avg_reads AS baseline_avg_reads
FROM current_period c
FULL OUTER JOIN baseline_period b
  ON  c.database_name IS NOT DISTINCT FROM b.database_name
  AND c.query_hash IS NOT DISTINCT FROM b.query_hash;
""";

    /// <summary>GetQueryStoreRegressionsAsync. Parameters: $1 server, $2 start, $3 end, $4 baseline start, $5 max rows.</summary>
    internal const string Regressions = """
        
WITH deduped_baseline AS (
    /* LOAD-BEARING (correctness, not just perf) — #1841. Keep the LATEST cumulative snapshot per
       interval before aggregating; see the file header for why this read is the most exposed to it. */
    SELECT
        database_name,
        query_id,
        plan_id,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $4
    AND   collection_time < $2
),
deduped_recent AS (
    SELECT
        database_name,
        query_id,
        plan_id,
        query_text,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
        last_execution_time,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
baseline_performance AS (
    SELECT
        database_name,
        query_id,
        AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS DOUBLE PRECISION)) AS avg_logical_io_reads,
        CAST(SUM(execution_count) AS BIGINT) AS exec_count,
        CAST(COUNT(DISTINCT plan_id) AS INTEGER) AS plan_count
    FROM deduped_baseline
    WHERE rn = 1
    GROUP BY database_name, query_id
),
recent_performance AS (
    SELECT
        database_name,
        query_id,
        MAX(query_text) AS query_text_sample,
        AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS DOUBLE PRECISION)) AS avg_logical_io_reads,
        CAST(SUM(execution_count) AS BIGINT) AS exec_count,
        CAST(COUNT(DISTINCT plan_id) AS INTEGER) AS plan_count,
        MAX(last_execution_time) AS last_execution_time
    FROM deduped_recent
    WHERE rn = 1
    GROUP BY database_name, query_id
)
SELECT
    r.database_name,
    r.query_id,
    b.avg_duration_ms AS baseline_duration_ms,
    r.avg_duration_ms AS recent_duration_ms,
    (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) AS duration_regression_percent,
    b.avg_cpu_time_ms AS baseline_cpu_ms,
    r.avg_cpu_time_ms AS recent_cpu_ms,
    (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) AS cpu_regression_percent,
    b.avg_logical_io_reads AS baseline_reads,
    r.avg_logical_io_reads AS recent_reads,
    (r.avg_logical_io_reads - b.avg_logical_io_reads) * 100.0 / NULLIF(b.avg_logical_io_reads, 0) AS io_regression_percent,
    (r.avg_duration_ms - b.avg_duration_ms) * r.exec_count AS additional_duration_ms,
    b.exec_count AS baseline_exec_count,
    r.exec_count AS recent_exec_count,
    b.plan_count AS baseline_plan_count,
    r.plan_count AS recent_plan_count,
    CASE
        WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 100 THEN 'CRITICAL'
        WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 50 THEN 'HIGH'
        WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    r.query_text_sample,
    r.last_execution_time
FROM recent_performance AS r
JOIN baseline_performance AS b
  ON  b.database_name = r.database_name
  AND b.query_id = r.query_id
WHERE (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) > 25
ORDER BY additional_duration_ms DESC
LIMIT $5;
""";
}

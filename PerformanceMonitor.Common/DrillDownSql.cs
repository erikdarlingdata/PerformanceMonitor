/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The two drill-down reads behind <c>analyze_server</c>'s top-CPU list and the BAD_ACTOR detail, written once for both
/// stores (#8470). Lite's DuckDB drill-down collector and Darling's PostgreSQL one run this text as it stands over their own
/// <c>v_query_stats</c>, binding <c>$1</c>..<c>$5</c> positionally. The text is dialect-neutral: <c>ROW_NUMBER() OVER</c>,
/// <c>MAX() OVER</c>, <c>CASE</c> aggregates, <c>::BIGINT</c> and <c>::DOUBLE PRECISION</c> casts, and explicit
/// <c>NULLS LAST</c> on every DESC tie-breaker, because DuckDB and Postgres default DESC null placement differently.
///
/// <para>#3648: <c>max_dop</c> is <c>sys.dm_exec_query_stats.max_dop</c>, a per-plan high-water mark since the plan entered
/// the cache. Each read ranks a hash's rows newest-first (<c>collection_time</c>, then compile time, then CPU spent) and
/// projects the newest plan's reading as <c>max_dop</c>, with the cross-plan maximum beside it as <c>max_dop_any_plan</c>,
/// when it was last seen as <c>max_dop_any_plan_last_seen</c>, and <c>plan_count</c>. The provenance columns come last, so
/// the ordinals before them are fixed. The readers coerce a NULL <c>max_dop</c> to null, never 0, and compose
/// <c>dop_note</c> through <see cref="QueryDopProvenance"/>.</para>
/// </summary>
public static class DrillDownSql
{
    /// <summary>
    /// The <c>top_cpu_queries</c> read: the five (database, query_hash) groups that spent the most CPU in the window.
    /// <c>$1</c> server_id, <c>$2</c> and <c>$3</c> the window. #3959: ranked and cut to five without the statement text,
    /// which the final SELECT reads for the five that print as the same <c>MAX(query_text)</c> over the same rows, equality
    /// first (the (server_id, query_hash, collection_time) index serves it) and NULL-safe only for a group with a NULL key.
    /// Ordinals: 0 database_name, 1 query_hash, 2 total_cpu_us, 3 exec_count, 4 max_dop, 5 spills, 6 query_text,
    /// 7 plan_count, 8 max_dop_any_plan, 9 max_dop_any_plan_last_seen.
    /// </summary>
    public const string TopCpuQueries = @"
WITH windowed AS
(
    -- #3648: rank each hash's rows newest-first and carry the hash-wide maximum onto every row, so the
    -- outer aggregate can name WHICH reading is current and WHEN the maximum was last seen. Explicit
    -- NULLS LAST on the tie-breakers: DuckDB and Postgres default DESC null placement differently.
    SELECT database_name, query_hash, query_plan_hash, collection_time, max_dop,
           delta_worker_time, delta_execution_count, delta_spills,
           ROW_NUMBER() OVER
           (
               PARTITION BY database_name, query_hash
               ORDER BY collection_time DESC, creation_time DESC NULLS LAST, delta_worker_time DESC NULLS LAST
           ) AS newest_rn,
           MAX(max_dop) OVER (PARTITION BY database_name, query_hash) AS max_dop_any_plan
    FROM v_query_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   delta_worker_time > 0
),
top_queries AS
(
    -- #3959: ranked and cut WITHOUT the statement text. The text used to ride the window above for every
    -- row it read, to print five, and on Darling v_query_stats resolves it from the fleet's text dimension
    -- row by row. The final SELECT reads it for the five that print.
    SELECT database_name, query_hash,
           SUM(delta_worker_time)::BIGINT AS total_cpu_us,
           SUM(delta_execution_count)::BIGINT AS exec_count,
           MAX(CASE WHEN newest_rn = 1 THEN max_dop END) AS max_dop,
           SUM(delta_spills)::BIGINT AS spills,
           COUNT(DISTINCT query_plan_hash) AS plan_count,
           MAX(max_dop_any_plan) AS max_dop_any_plan,
           MAX(CASE WHEN max_dop = max_dop_any_plan THEN collection_time END) AS max_dop_any_plan_last_seen
    FROM windowed
    GROUP BY database_name, query_hash
    ORDER BY total_cpu_us DESC
    LIMIT 5
)
SELECT t.database_name, t.query_hash,
       t.total_cpu_us,
       t.exec_count,
       t.max_dop,
       t.spills,
       -- #3959: the same MAX over the same rows the single pass took it over (this group's window rows,
       -- under the window's own filter). Equality first, which the (server_id, query_hash, collection_time)
       -- index serves. GROUP BY puts a NULL key's rows in one group that equality cannot match, so only
       -- a group with a NULL key reads them NULL-safe.
       COALESCE
       (
           (
               SELECT MAX(v.query_text)
               FROM v_query_stats AS v
               WHERE v.server_id = $1 AND v.collection_time >= $2 AND v.collection_time <= $3
               AND   v.delta_worker_time > 0
               AND   v.database_name = t.database_name
               AND   v.query_hash = t.query_hash
           ),
           CASE WHEN t.database_name IS NULL OR t.query_hash IS NULL THEN
           (
               SELECT MAX(v.query_text)
               FROM v_query_stats AS v
               WHERE v.server_id = $1 AND v.collection_time >= $2 AND v.collection_time <= $3
               AND   v.delta_worker_time > 0
               AND   v.database_name IS NOT DISTINCT FROM t.database_name
               AND   v.query_hash IS NOT DISTINCT FROM t.query_hash
           )
           END
       ) AS query_text,
       t.plan_count,
       t.max_dop_any_plan,
       t.max_dop_any_plan_last_seen
FROM top_queries AS t
ORDER BY t.total_cpu_us DESC";

    /// <summary>
    /// The <c>bad_actor_query</c> read: one hash's figures over the window, unfiltered by CPU spent, so the newest-plan
    /// tie-break is what separates a stale parallel plan still in the cache from the serial one doing the work at the same
    /// <c>collection_time</c>. <c>$1</c> server_id, <c>$2</c> and <c>$3</c> the window, <c>$4</c> the hash. #8317: <c>$5</c>
    /// is the finding's database, so when one hash runs in several databases the row described is the one the fact was
    /// scored on; a finding with no database sends an empty <c>$5</c> and gets the database that spent the most CPU on the
    /// hash. Ordinals: 0 database_name, 1 query_hash, 2 query_text, 3 exec_count, 4 avg_cpu_ms, 5 avg_elapsed_ms,
    /// 6 avg_reads, 7 total_cpu_us, 8 total_reads, 9 total_spills, 10 max_dop, 11 plan_count, 12 max_dop_any_plan,
    /// 13 max_dop_any_plan_last_seen.
    /// </summary>
    public const string BadActorDetail = @"
WITH windowed AS
(
    -- #3648: see CollectTopCpuQueries' windowed CTE; same ranking, same hash-wide maximum.
    SELECT database_name, query_hash, query_plan_hash, collection_time, max_dop,
           delta_worker_time, delta_execution_count, delta_elapsed_time, delta_logical_reads, delta_spills,
           query_text,
           ROW_NUMBER() OVER
           (
               PARTITION BY database_name, query_hash
               ORDER BY collection_time DESC, creation_time DESC NULLS LAST, delta_worker_time DESC NULLS LAST
           ) AS newest_rn,
           MAX(max_dop) OVER (PARTITION BY database_name, query_hash) AS max_dop_any_plan
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_hash = $4
    AND   ($5 = '' OR database_name = $5)
)
SELECT database_name, query_hash,
       MAX(query_text) AS query_text,
       SUM(delta_execution_count)::BIGINT AS exec_count,
       CASE WHEN SUM(delta_execution_count) > 0
            THEN SUM(delta_worker_time)::DOUBLE PRECISION / SUM(delta_execution_count) / 1000.0
            ELSE 0 END AS avg_cpu_ms,
       CASE WHEN SUM(delta_execution_count) > 0
            THEN SUM(delta_elapsed_time)::DOUBLE PRECISION / SUM(delta_execution_count) / 1000.0
            ELSE 0 END AS avg_elapsed_ms,
       CASE WHEN SUM(delta_execution_count) > 0
            THEN SUM(delta_logical_reads)::DOUBLE PRECISION / SUM(delta_execution_count)
            ELSE 0 END AS avg_reads,
       SUM(delta_worker_time)::BIGINT AS total_cpu_us,
       SUM(delta_logical_reads)::BIGINT AS total_reads,
       SUM(delta_spills)::BIGINT AS total_spills,
       MAX(CASE WHEN newest_rn = 1 THEN max_dop END) AS max_dop,
       COUNT(DISTINCT query_plan_hash) AS plan_count,
       MAX(max_dop_any_plan) AS max_dop_any_plan,
       MAX(CASE WHEN max_dop = max_dop_any_plan THEN collection_time END) AS max_dop_any_plan_last_seen
FROM windowed
GROUP BY database_name, query_hash
ORDER BY total_cpu_us DESC
LIMIT 1";
}

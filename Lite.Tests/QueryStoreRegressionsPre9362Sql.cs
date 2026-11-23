/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9362: the Query Store regressions statement exactly as it stood before the coverage facts moved into it and
/// <c>recent_winner</c> became one aggregate (the statement of 3.13.0, kept verbatim). A test oracle: the equality tests
/// run it beside the shipped statement over one seeded store and require the same rows in the same order. It returns the
/// statement text only, with the same pieces the reader fills in (database clause, parameter slots, per-hour divisors).
/// </summary>
internal static class QueryStoreRegressionsPre9362Sql
{
    internal static string Build(
        string dbClause, int baselineParamIndex, string excludeClause, string baseHours, string recentHours, string baseEmpty, string limit)
    {
        return @"
WITH latest AS (
    /* LOAD-BEARING (correctness, not just perf) - #1841. Keep the LATEST cumulative snapshot per
       interval before aggregating; see the file header for why this read is the most exposed to it. Every outcome is
       kept (#6268): the Regular cost below and the Aborted and Exception counts are separate FILTERs over this set.
       #7875: ONE dedupe over the whole span, baseline start to window end. Two arms, each deduping inside its own
       collection_time range, kept an interval that has a snapshot on each side of the window start as the latest of the
       baseline AND the latest of the recent window, counting its executions in both. The interval's latest snapshot is
       now assigned to one side by its last_execution_time, the line Darling's read splits on (#6750). */
    SELECT
        collection_id,
        collection_time,
        database_name,
        query_id,
        plan_id,
        execution_type_desc,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        last_execution_time,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $" + baselineParamIndex + @"
    AND   collection_time <= $3" + dbClause + @"
),
deduped_baseline AS (
    SELECT
        database_name,
        query_id,
        plan_id,
        execution_type_desc,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        rn
    FROM latest
    WHERE rn = 1
    AND   last_execution_time >= $" + baselineParamIndex + @"
    AND   last_execution_time < $2
),
deduped_recent AS (
    SELECT
        collection_id,
        collection_time,
        database_name,
        query_id,
        plan_id,
        execution_type_desc,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        last_execution_time,
        rn
    FROM latest
    WHERE rn = 1
    AND   last_execution_time >= $2
),
baseline_performance AS (
    SELECT
        database_name,
        query_id,
        SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Regular')
            / NULLIF(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Regular'), 0) / 1000.0 AS avg_duration_ms,
        SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Regular')
            / NULLIF(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Regular'), 0) / 1000.0 AS avg_cpu_time_ms,
        CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Regular'), 0) AS BIGINT) AS exec_count,
        CAST(COUNT(DISTINCT plan_id) FILTER (WHERE execution_type_desc = 'Regular') AS INTEGER) AS plan_count,
        CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted'), 0) AS BIGINT) AS aborted_execs,
        SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Aborted') AS aborted_cpu_us,
        SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Aborted') AS aborted_dur_us,
        CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Exception'), 0) AS BIGINT) AS exception_execs,
        SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Exception') AS exception_cpu_us,
        SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Exception') AS exception_dur_us
    FROM deduped_baseline
    WHERE rn = 1
    GROUP BY database_name, query_id
),
recent_performance AS (
    SELECT
        database_name,
        query_id,
        SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Regular')
            / NULLIF(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Regular'), 0) / 1000.0 AS avg_duration_ms,
        SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Regular')
            / NULLIF(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Regular'), 0) / 1000.0 AS avg_cpu_time_ms,
        CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Regular'), 0) AS BIGINT) AS exec_count,
        CAST(COUNT(DISTINCT plan_id) FILTER (WHERE execution_type_desc = 'Regular') AS INTEGER) AS plan_count,
        CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Aborted'), 0) AS BIGINT) AS aborted_execs,
        SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Aborted') AS aborted_cpu_us,
        SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Aborted') AS aborted_dur_us,
        CAST(COALESCE(SUM(execution_count) FILTER (WHERE execution_type_desc = 'Exception'), 0) AS BIGINT) AS exception_execs,
        SUM(CAST(avg_cpu_time_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Exception') AS exception_cpu_us,
        SUM(CAST(avg_duration_us AS DOUBLE PRECISION) * execution_count) FILTER (WHERE execution_type_desc = 'Exception') AS exception_dur_us,
        COALESCE(MAX(last_execution_time) FILTER (WHERE execution_type_desc = 'Regular'), MAX(last_execution_time)) AS last_execution_time
    FROM deduped_recent
    WHERE rn = 1
    GROUP BY database_name, query_id
),
/* #5381: query_text is not carried through the windows above. The sample used to be MAX(query_text) over the
   query's kept recent rows; it is read afterwards, by key, for the rows this statement returns, from the latest kept
   row of each query: a query_id has one text for life in Query Store, so that is the same text. */
recent_winner AS (
    SELECT database_name, query_id, collection_id, collection_time
    FROM
    (
        SELECT
            database_name,
            query_id,
            collection_id,
            collection_time,
            ROW_NUMBER() OVER (PARTITION BY database_name, query_id ORDER BY collection_time DESC, collection_id DESC) AS wr
        FROM deduped_recent
        WHERE rn = 1
    ) x
    WHERE wr = 1
),
/* #6268: a query is a candidate when it has recent Regular runs, or recent Aborted or Exception runs reaching the minimum. */
candidates AS (
    SELECT database_name, query_id
    FROM recent_performance
    WHERE exec_count > 0
    OR    aborted_execs >= " + QueryStoreRegressionRules.FailureMinRecentRuns + @"
    OR    exception_execs >= " + QueryStoreRegressionRules.FailureMinRecentRuns + @"
),
joined AS (
    SELECT
        k.database_name,
        k.query_id,
        b.avg_duration_ms AS base_dur_ms,
        r.avg_duration_ms AS recent_dur_ms,
        b.avg_cpu_time_ms AS base_cpu_ms,
        r.avg_cpu_time_ms AS recent_cpu_ms,
        COALESCE(b.exec_count, 0) AS base_execs,
        r.exec_count AS recent_execs,
        COALESCE(b.plan_count, 0) AS base_plans,
        r.plan_count AS recent_plans,
        r.last_execution_time,
        COALESCE(b.aborted_execs, 0) AS base_aborted_execs,
        r.aborted_execs AS recent_aborted_execs,
        b.aborted_cpu_us AS base_aborted_cpu_us,
        r.aborted_cpu_us AS recent_aborted_cpu_us,
        b.aborted_dur_us AS base_aborted_dur_us,
        r.aborted_dur_us AS recent_aborted_dur_us,
        COALESCE(b.exception_execs, 0) AS base_exception_execs,
        r.exception_execs AS recent_exception_execs,
        b.exception_cpu_us AS base_exception_cpu_us,
        r.exception_cpu_us AS recent_exception_cpu_us,
        b.exception_dur_us AS base_exception_dur_us,
        r.exception_dur_us AS recent_exception_dur_us
    FROM candidates AS k
    JOIN recent_performance AS r ON r.database_name = k.database_name AND r.query_id = k.query_id
    LEFT JOIN baseline_performance AS b ON b.database_name = k.database_name AND b.query_id = k.query_id
),
flagged AS (
    SELECT
        j.*,
        (j.recent_dur_ms - j.base_dur_ms) * 100.0 / NULLIF(j.base_dur_ms, 0) AS duration_regression_percent,
        (j.recent_cpu_ms - j.base_cpu_ms) * 100.0 / NULLIF(j.base_cpu_ms, 0) AS cpu_regression_percent,
        " + QueryStoreRegressionRules.CostTestSql("j") + @" AS is_cost,
        COALESCE(j.recent_aborted_execs >= " + QueryStoreRegressionRules.FailureMinRecentRuns + @"
            AND (j.base_aborted_execs = 0 OR " + baseEmpty + @"
                 OR (j.recent_aborted_execs / " + recentHours + @") >= " + QueryStoreRegressionRules.FailureRateMultiple + @" * (j.base_aborted_execs / " + baseHours + @")), false) AS is_timeouts,
        COALESCE(j.recent_exception_execs >= " + QueryStoreRegressionRules.FailureMinRecentRuns + @"
            AND (j.base_exception_execs = 0 OR " + baseEmpty + @"
                 OR (j.recent_exception_execs / " + recentHours + @") >= " + QueryStoreRegressionRules.FailureRateMultiple + @" * (j.base_exception_execs / " + baseHours + @")), false) AS is_errors
    FROM joined AS j
),
qualified AS (
    SELECT
        f.*,
        /* A cost row keeps the duration bands (CRITICAL over 100%, HIGH over 50%, MEDIUM over 25%, else LOW),
           except that a cost row which added under a minute of duration over the window and under a second per run is LOW
           whatever its percent (#7592). A failure row is
           HIGH when its rate is at least FailureHighMultiple times the baseline's or the baseline had none, else MEDIUM; a row with
           both takes the higher band. A failure never reads CRITICAL. */
        CASE
            WHEN NOT f.is_cost THEN 0
            WHEN " + QueryStoreRegressionRules.GradeFloorSql("f") + @" THEN 1
            WHEN f.duration_regression_percent > 100 THEN 4
            WHEN f.duration_regression_percent > 50 THEN 3
            WHEN f.duration_regression_percent > 25 THEN 2
            ELSE 1
        END AS cost_rank,
        GREATEST(
            CASE WHEN NOT f.is_timeouts THEN 0
                 WHEN f.base_aborted_execs = 0 OR " + baseEmpty + @"
                      OR (f.recent_aborted_execs / " + recentHours + @") >= " + QueryStoreRegressionRules.FailureHighMultiple + @" * (f.base_aborted_execs / " + baseHours + @") THEN 3
                 ELSE 2 END,
            CASE WHEN NOT f.is_errors THEN 0
                 WHEN f.base_exception_execs = 0 OR " + baseEmpty + @"
                      OR (f.recent_exception_execs / " + recentHours + @") >= " + QueryStoreRegressionRules.FailureHighMultiple + @" * (f.base_exception_execs / " + baseHours + @") THEN 3
                 ELSE 2 END) AS failure_rank,
        (f.recent_dur_ms - f.base_dur_ms) * f.recent_execs AS additional_duration_ms
    FROM flagged AS f
    WHERE (f.is_cost OR f.is_timeouts OR f.is_errors)" + excludeClause + @"
),
ranked AS (
    SELECT
        q.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY q.is_cost
            ORDER BY CASE WHEN q.is_cost THEN COALESCE(q.additional_duration_ms, 0) ELSE CAST(q.recent_aborted_execs + q.recent_exception_execs AS DOUBLE PRECISION) END DESC,
                     q.database_name, q.query_id
        ) AS class_rn,
        COUNT(*) FILTER (WHERE q.is_cost) OVER () AS cost_total,
        COUNT(*) FILTER (WHERE NOT q.is_cost) OVER () AS failure_total,
        COUNT(*) OVER () AS total_count
    FROM qualified AS q
),
slots AS (
    /* Failure-only rows: their share of the cap, or more when the cost rows leave slots free. Cost rows take the rest. */
    SELECT
        GREATEST(LEAST(MAX(failure_total), " + limit + @" // " + QueryStoreRegressionRules.FailureShareDivisor + @"),
                 LEAST(MAX(failure_total), " + limit + @" - MAX(cost_total))) AS failure_slots
    FROM ranked
)
SELECT
    r.database_name,
    r.query_id,
    r.base_dur_ms AS baseline_duration_ms,
    r.recent_dur_ms AS recent_duration_ms,
    r.duration_regression_percent,
    r.base_cpu_ms AS baseline_cpu_ms,
    r.recent_cpu_ms AS recent_cpu_ms,
    r.cpu_regression_percent,
    COALESCE(r.additional_duration_ms, 0) AS additional_duration_ms,
    r.base_execs AS baseline_exec_count,
    r.recent_execs AS recent_exec_count,
    r.base_plans AS baseline_plan_count,
    r.recent_plans AS recent_plan_count,
    CASE GREATEST(r.cost_rank, r.failure_rank)
        WHEN 4 THEN 'CRITICAL'
        WHEN 3 THEN 'HIGH'
        WHEN 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    w.collection_id AS text_collection_id,
    r.last_execution_time,
    w.collection_time AS text_collection_time,
    r.is_cost,
    r.is_timeouts,
    r.is_errors,
    r.base_aborted_execs AS base_aborted_count,
    r.recent_aborted_execs AS recent_aborted_count,
    r.base_aborted_cpu_us / NULLIF(r.base_aborted_execs, 0) / 1000.0 AS base_aborted_cpu_ms,
    r.recent_aborted_cpu_us / NULLIF(r.recent_aborted_execs, 0) / 1000.0 AS recent_aborted_cpu_ms,
    r.base_aborted_dur_us / NULLIF(r.base_aborted_execs, 0) / 1000.0 AS base_aborted_dur_ms,
    r.recent_aborted_dur_us / NULLIF(r.recent_aborted_execs, 0) / 1000.0 AS recent_aborted_dur_ms,
    r.base_exception_execs AS base_exception_count,
    r.recent_exception_execs AS recent_exception_count,
    r.base_exception_cpu_us / NULLIF(r.base_exception_execs, 0) / 1000.0 AS base_exception_cpu_ms,
    r.recent_exception_cpu_us / NULLIF(r.recent_exception_execs, 0) / 1000.0 AS recent_exception_cpu_ms,
    r.base_exception_dur_us / NULLIF(r.base_exception_execs, 0) / 1000.0 AS base_exception_dur_ms,
    r.recent_exception_dur_us / NULLIF(r.recent_exception_execs, 0) / 1000.0 AS recent_exception_dur_ms,
    /* Executions per hour, so the baseline (7 days) and the recent window (hours_back) compare in one unit. */
    r.base_execs / " + baseHours + @" AS baseline_regular_per_hour,
    r.recent_execs / " + recentHours + @" AS recent_regular_per_hour,
    r.base_aborted_execs / " + baseHours + @" AS baseline_aborted_per_hour,
    r.recent_aborted_execs / " + recentHours + @" AS recent_aborted_per_hour,
    r.base_exception_execs / " + baseHours + @" AS baseline_exception_per_hour,
    r.recent_exception_execs / " + recentHours + @" AS recent_exception_per_hour,
    /* #6128: the rows that passed the flags, before the cap, in the same statement. */
    r.total_count
FROM ranked AS r
CROSS JOIN slots AS s
LEFT JOIN recent_winner AS w
  ON  w.database_name = r.database_name
  AND w.query_id = r.query_id
WHERE (r.is_cost AND r.class_rn <= " + limit + @" - s.failure_slots)
OR    (NOT r.is_cost AND r.class_rn <= s.failure_slots)
ORDER BY r.is_cost DESC,
         CASE WHEN r.is_cost THEN COALESCE(r.additional_duration_ms, 0) END DESC,
         r.recent_aborted_execs + r.recent_exception_execs DESC,
         r.database_name,
         r.query_id
LIMIT " + limit;
    }
}

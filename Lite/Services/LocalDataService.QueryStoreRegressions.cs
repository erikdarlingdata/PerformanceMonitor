/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>One Query Store regression row - the DuckDB twin of Darling's
/// <c>DarlingQueryStoreRegressionReader.RegressionRow</c>. Durations and CPU are ms (converted from the
/// stored microseconds); the percents are plain deltas. Logical reads are not reported (#5506, #6268): they are not a
/// useful metric.
/// <para>Both percents are NULLABLE (#3541 A12, contract rule 5). Each divides through
/// <c>NULLIF(baseline, 0)</c>, so a query whose baseline side is 0 has no ratio: the SQL returns NULL, and the reader used
/// to coerce it to 0, which published the most dramatic possible regression as "no change". NULL stays NULL here and the
/// tool says why. A row flagged only for timeouts or errors can have no Regular runs on a side, so it has no cost comparison
/// and both percents are NULL.</para>
/// <para>#6268: <see cref="Aborted"/> and <see cref="Exception"/> are the other two groups of runs, beside the Regular figures
/// above; <see cref="RegressionReason"/> says which of cost, timeouts and errors flagged the row.</para></summary>
public sealed class QueryStoreRegressionRow
{
    public string DatabaseName { get; set; } = "";
    public long QueryId { get; set; }
    public double BaselineDurationMs { get; set; }
    public double RecentDurationMs { get; set; }
    public double? DurationRegressionPercent { get; set; }
    public double BaselineCpuMs { get; set; }
    public double RecentCpuMs { get; set; }
    public double? CpuRegressionPercent { get; set; }
    public double AdditionalDurationMs { get; set; }
    public long BaselineExecCount { get; set; }
    public long RecentExecCount { get; set; }
    public int BaselinePlanCount { get; set; }
    public int RecentPlanCount { get; set; }
    public string Severity { get; set; } = "";
    public string QueryTextSample { get; set; } = "";
    public DateTime? LastExecutionTime { get; set; }
    /// <summary>#6128: every regression that passed the gate, before the row cap. The same value on every row of one read.</summary>
    public long TotalCount { get; set; }
    /// <summary>#6268: why the row is listed: cost, timeouts, errors, or a comma-separated combination in that order.</summary>
    public string RegressionReason { get; set; } = "";
    public double? BaselineRegularPerHour { get; set; }
    public double? RecentRegularPerHour { get; set; }
    public QueryStoreRunGroup Aborted { get; set; } = new();
    public QueryStoreRunGroup Exception { get; set; } = new();
}

/// <summary>One group of runs (Aborted or Exception, #6268) of one query: the execution count and the per-execution CPU and
/// duration (ms, weighted by execution count, null when the count is 0), and the count per hour, for the baseline and for the
/// recent window. Darling twin: <c>DarlingQueryStoreRegressionReader.RunGroup</c>.</summary>
public sealed class QueryStoreRunGroup
{
    public long BaselineCount { get; set; }
    public long RecentCount { get; set; }
    public double? BaselineCpuMs { get; set; }
    public double? RecentCpuMs { get; set; }
    public double? BaselineDurationMs { get; set; }
    public double? RecentDurationMs { get; set; }
    public double? BaselinePerHour { get; set; }
    public double? RecentPerHour { get; set; }
}

/*
 * The Query Store regressions read (#2484) - Lite's port of Darling's, which is itself the viewer's port
 * of the Dashboard's report.query_store_regressions inline TVF. Two windowed passes over the SAME
 * v_query_store_stats view: BASELINE is a fixed lookback before the window start, RECENT is the window.
 *
 * Both arms are DEDUPED first, and that is correctness rather than performance. Query Store rows are
 * CUMULATIVE per-interval snapshots and the collector re-fetches an open interval every cycle, so the same
 * interval is stored repeatedly with a growing execution_count. This read is the most exposed of any to
 * that: the baseline arm and the recent arm can have systematically different re-collection density per
 * interval - which alone moves the averages the regression percent is computed from and the 25% CPU gate,
 * manufacturing and hiding regressions for reasons that have nothing to do with the query.
 *
 * #6268 (Darling's #5506): THREE groups of runs are read per query, side by side: Regular (the cost the gate compares), Aborted
 * (timeouts, cancels) and Exception (errors). Counting only Regular runs hides what a timeout or an error does (one 30 s timeout
 * among 1,000 fast runs barely moves an average), and blending them into one average would hide it the same way. Cost is the
 * Regular runs' CPU and duration per execution, WEIGHTED by execution count (the summed cost over the summed executions, not an
 * average of interval averages), for both sides. A query is flagged for COST (Regular CPU up more than CostGatePercent,
 * with at least CostMinAddedCpuMs of added CPU over the window, #6781), for TIMEOUTS or ERRORS (that group's per-hour rate in the recent window at least FailureRateMultiple times the baseline's, or any at
 * all where the baseline had none, with at least FailureMinRecentRuns such runs in the recent window), or for several. The
 * thresholds are QueryStoreRegressionRules', shared with Darling. Logical reads are not read at all.
 *
 * Differences from Darling's, none of them in which rows the read returns: the query-text sample comes only from the fact rows
 * (Darling resolves it from collect.query_store_text, #2150, a Darling-store table Lite has no equivalent of); Lite keeps no
 * per-day totals table, so the baseline always comes from the fact rows; and an interval belongs to the baseline or the recent
 * window by the collection_time of its latest snapshot, as this read always did.
 */
public partial class LocalDataService
{
    /// <summary>
    /// The baseline's own lookback (Lite's twin of Darling's #4195 fix): a fixed 7 days ending at the recent
    /// window's start, so the comparison period stops moving with retention. Before this the baseline was
    /// every retained row before the window with NO lower bound, so its cost tracked how much history the
    /// store still held rather than the window asked for. A regression against a baseline OLDER than 7 days
    /// is no longer caught; a server retaining less than 7 days of Query Store history is unaffected - the
    /// bound never reaches further back than the unbounded read already stopped.
    /// </summary>
    public const int BaselineLookbackDays = 7;

    /// <summary>
    /// The queries whose Query Store performance got WORSE over [now - <paramref name="hoursBack"/>, now]
    /// compared with a fixed <see cref="BaselineLookbackDays"/>-day baseline ending at the window's start: Regular cost
    /// per execution up, or Aborted (timeouts) or Exception (errors) runs up (#6268). Cost rows come first, ranked by
    /// execution-count-weighted extra duration; rows listed only for timeouts or errors follow, by their recent failed runs,
    /// and take at most <c>maxRows / FailureShareDivisor</c> slots (more only when the cost rows leave room).
    /// </summary>
    public async Task<List<QueryStoreRegressionRow>> GetQueryStoreRegressionsAsync(
        int serverId, int hoursBack = 24, int maxRows = 50, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null,
        IReadOnlyList<string>? excludedDatabases = null, DateTime? dataStartUtc = null)
    {
        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var baselineStartTime = startTime.AddDays(-BaselineLookbackDays);

        /* #6750: where the server's Query Store data starts, the instant the tool's window notice reports as effective_start
           (McpQueryTools.EffectiveWindowStart over GetQueryWindowFloorAsync). The tool already probes it for that notice, so it
           passes the answer in (dataStartUtc) and the read makes no second probe. A caller that passes none gets the same probe
           here: one MIN over the rows from the baseline's start, and one more only when that row sits after the start. It is
           read before this read's own connection opens, as the probe opens one of its own. It only DIVIDES: the row filters
           below keep the baseline's start. */
        var dataStart = dataStartUtc
            ?? Mcp.McpQueryTools.EffectiveWindowStart(
                await GetQueryWindowFloorAsync(QueryWindowRelation.QueryStoreStats, serverId, baselineStartTime, endTime), baselineStartTime);

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var baselineParamIndex = 4 + dbValues.Count;
        var limitIndex = baselineParamIndex + 1;
        /* #6654: the databases the server dropped, left out of the candidates before the rank and the cap (the exclusion binds after the limit). */
        var excludeClause = BuildDbExcludeClause(excludedDatabases, "f.database_name", limitIndex + 1, out var excludeValues);

        /* The per-hour rates divide by the hours each side HOLDS (#6750), not the hours its window names: the baseline is
           [baseline start, window start) and the store holds it from the data start, the recent window [window start, window end]
           and the store holds it from the later of the two. Both are numbers computed here, written as invariant literals. A side
           that holds no hour divides by NULL, so its per-hour rate is NULL as Darling's NULLIF gives (a zero literal would depend
           on the engine's division by zero). When the store holds the whole baseline the two equal the old whole windows.
           Darling's gates count a baseline that holds no hours as empty, because there a baseline row can have runs while it holds
           none (its rows are filtered on last_execution_time). Here they need no such arm: the baseline rows are filtered on
           collection_time < $2, and the data start is at or before the first row's collection_time (GetQueryWindowFloorAsync), so
           a baseline with any row holds a positive number of hours (#6750). */
        static string Hours(TimeSpan held) => held > TimeSpan.Zero ? held.TotalHours.ToString("R", CultureInfo.InvariantCulture) : "CAST(NULL AS DOUBLE)";
        var baseHours = Hours(startTime - (dataStart > baselineStartTime ? dataStart : baselineStartTime));
        var recentHours = Hours(endTime - (dataStart > startTime ? dataStart : startTime));
        var limit = "CAST($" + limitIndex + " AS INTEGER)";

        command.CommandText = @"
WITH deduped_baseline AS (
    /* LOAD-BEARING (correctness, not just perf) - #1841. Keep the LATEST cumulative snapshot per
       interval before aggregating; see the file header for why this read is the most exposed to it. Every outcome is
       kept (#6268): the Regular cost below and the Aborted and Exception counts are separate FILTERs over this set. */
    SELECT
        database_name,
        query_id,
        plan_id,
        execution_type_desc,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $" + baselineParamIndex + @"
    AND   collection_time < $2" + dbClause + @"
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
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3" + dbClause + @"
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
            AND (j.base_aborted_execs = 0
                 OR (j.recent_aborted_execs / " + recentHours + @") >= " + QueryStoreRegressionRules.FailureRateMultiple + @" * (j.base_aborted_execs / " + baseHours + @")), false) AS is_timeouts,
        COALESCE(j.recent_exception_execs >= " + QueryStoreRegressionRules.FailureMinRecentRuns + @"
            AND (j.base_exception_execs = 0
                 OR (j.recent_exception_execs / " + recentHours + @") >= " + QueryStoreRegressionRules.FailureRateMultiple + @" * (j.base_exception_execs / " + baseHours + @")), false) AS is_errors
    FROM joined AS j
),
qualified AS (
    SELECT
        f.*,
        /* A cost row keeps the duration bands (CRITICAL over 100%, HIGH over 50%, MEDIUM over 25%, else LOW). A failure row is
           HIGH when its rate is at least FailureHighMultiple times the baseline's or the baseline had none, else MEDIUM; a row with
           both takes the higher band. A failure never reads CRITICAL. */
        CASE
            WHEN NOT f.is_cost THEN 0
            WHEN f.duration_regression_percent > 100 THEN 4
            WHEN f.duration_regression_percent > 50 THEN 3
            WHEN f.duration_regression_percent > 25 THEN 2
            ELSE 1
        END AS cost_rank,
        GREATEST(
            CASE WHEN NOT f.is_timeouts THEN 0
                 WHEN f.base_aborted_execs = 0
                      OR (f.recent_aborted_execs / " + recentHours + @") >= " + QueryStoreRegressionRules.FailureHighMultiple + @" * (f.base_aborted_execs / " + baseHours + @") THEN 3
                 ELSE 2 END,
            CASE WHEN NOT f.is_errors THEN 0
                 WHEN f.base_exception_execs = 0
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

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        command.Parameters.Add(new DuckDBParameter { Value = baselineStartTime });
        command.Parameters.Add(new DuckDBParameter { Value = maxRows });
        foreach (var excluded in excludeValues)
            command.Parameters.Add(new DuckDBParameter { Value = excluded });

        var rows = new List<QueryStoreRegressionRow>();
        var textKeys = new List<(long? Id, DateTime? Time)>();
        using (var reader = await command.ExecuteReaderAsync())
        {
        while (await reader.ReadAsync())
        {
            textKeys.Add((reader.IsDBNull(14) ? null : reader.GetInt64(14), reader.IsDBNull(16) ? null : reader.GetDateTime(16)));
            var isCost = reader.GetBoolean(17);
            var isTimeouts = reader.GetBoolean(18);
            var isErrors = reader.GetBoolean(19);
            var reasons = new List<string>(3);
            if (isCost) reasons.Add("cost");
            if (isTimeouts) reasons.Add("timeouts");
            if (isErrors) reasons.Add("errors");
            rows.Add(new QueryStoreRegressionRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                BaselineDurationMs = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                RecentDurationMs = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                DurationRegressionPercent = reader.IsDBNull(4) ? null : ToDouble(reader.GetValue(4)),
                BaselineCpuMs = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                RecentCpuMs = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                CpuRegressionPercent = reader.IsDBNull(7) ? null : ToDouble(reader.GetValue(7)),
                AdditionalDurationMs = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                BaselineExecCount = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
                RecentExecCount = reader.IsDBNull(10) ? 0 : Convert.ToInt64(reader.GetValue(10)),
                BaselinePlanCount = reader.IsDBNull(11) ? 0 : Convert.ToInt32(reader.GetValue(11)),
                RecentPlanCount = reader.IsDBNull(12) ? 0 : Convert.ToInt32(reader.GetValue(12)),
                Severity = reader.IsDBNull(13) ? "" : reader.GetString(13),
                QueryTextSample = "",
                LastExecutionTime = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                RegressionReason = string.Join(",", reasons),
                Aborted = ReadRunGroup(reader, 20, 32 + 2, 32 + 3),
                Exception = ReadRunGroup(reader, 26, 32 + 4, 32 + 5),
                BaselineRegularPerHour = reader.IsDBNull(32) ? null : ToDouble(reader.GetValue(32)),
                RecentRegularPerHour = reader.IsDBNull(33) ? null : ToDouble(reader.GetValue(33)),
                TotalCount = reader.IsDBNull(38) ? 0 : Convert.ToInt64(reader.GetValue(38)),
            });
        }
        }

        /* #5381: the text sample, read by key for the returned rows only. */
        var texts = await ReadQueryStoreTextByRowAsync(
            connection, serverId,
            textKeys.Where(k => k.Id.HasValue && k.Time.HasValue).Select(k => (k.Id!.Value, k.Time!.Value)));
        for (var i = 0; i < rows.Count; i++)
        {
            string? text = null;
            if (textKeys[i].Id.HasValue && textKeys[i].Time.HasValue)
            {
                texts.TryGetValue((textKeys[i].Id!.Value, textKeys[i].Time!.Value), out text);
                /* The latest row carried no text: the old MAX skipped NULLs, so take any text the query has in the window. */
                text ??= await ReadQueryStoreWindowTextAsync(connection, serverId, rows[i].DatabaseName, rows[i].QueryId, startTime, endTime);
            }

            rows[i].QueryTextSample = text ?? "";
        }

        return rows;
    }

    private static QueryStoreRunGroup ReadRunGroup(System.Data.Common.DbDataReader reader, int first, int baselinePerHour, int recentPerHour) => new()
    {
        BaselineCount = reader.IsDBNull(first) ? 0 : Convert.ToInt64(reader.GetValue(first)),
        RecentCount = reader.IsDBNull(first + 1) ? 0 : Convert.ToInt64(reader.GetValue(first + 1)),
        BaselineCpuMs = reader.IsDBNull(first + 2) ? null : ToDouble(reader.GetValue(first + 2)),
        RecentCpuMs = reader.IsDBNull(first + 3) ? null : ToDouble(reader.GetValue(first + 3)),
        BaselineDurationMs = reader.IsDBNull(first + 4) ? null : ToDouble(reader.GetValue(first + 4)),
        RecentDurationMs = reader.IsDBNull(first + 5) ? null : ToDouble(reader.GetValue(first + 5)),
        BaselinePerHour = reader.IsDBNull(baselinePerHour) ? null : ToDouble(reader.GetValue(baselinePerHour)),
        RecentPerHour = reader.IsDBNull(recentPerHour) ? null : ToDouble(reader.GetValue(recentPerHour)),
    };

    /// <summary>
    /// Whether this server has Query Store rows in the baseline lookback before the window, and whether it
    /// has any INSIDE it.
    /// <para>One round trip for the two facts that decide what an empty regression result means, run only
    /// on the empty path. Zero regressions is four states here, not two, and only one of them is good news.
    /// The dangerous one is a server whose entire collected history sits INSIDE the requested window: it
    /// has no BEFORE, so it can never show a regression however badly it regressed. Reads
    /// <c>v_query_store_stats</c>, the same view the read itself uses. Darling twin:
    /// <c>DarlingQueryStoreRegressionReader.RegressionCoverageSql</c>.</para>
    /// <para>#5015: <paramref name="databaseNames"/> narrows both questions to the same databases the read was
    /// asked for (null: every database), so a caller that filtered can tell "nothing regressed in that database"
    /// from "no capture of that database was ever read", which an unfiltered probe cannot.</para>
    /// </summary>
    public async Task<(bool HasBaseline, bool HasRecent)> GetQueryStoreRegressionCoverageAsync(
        int serverId, int hoursBack = 24, DateTime? asOfUtc = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var baselineStartTime = startTime.AddDays(-BaselineLookbackDays);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 5, out var dbValues);

        command.CommandText = @"
SELECT
    EXISTS (
        SELECT 1
        FROM v_query_store_stats
        WHERE server_id = $1
        AND   collection_time >= $4
        AND   collection_time < $2" + dbClause + @"
    ) AS has_baseline,
    EXISTS (
        SELECT 1
        FROM v_query_store_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3" + dbClause + @"
    ) AS has_recent";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = baselineStartTime });
        foreach (var value in dbValues)
        {
            command.Parameters.Add(new DuckDBParameter { Value = value });
        }

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }
}

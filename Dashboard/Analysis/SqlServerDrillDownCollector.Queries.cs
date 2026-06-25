using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerDrillDownCollector
{
    private async Task CollectQueriesAtSpike(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // Check if query_snapshots table exists (created dynamically by sp_WhoIsActive)
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT OBJECT_ID(N'collect.query_snapshots', N'U')";
        var tableExists = await checkCmd.ExecuteScalarAsync();
        if (tableExists == null || tableExists == DBNull.Value) return;

        // Step 1: Find when the spike occurred
        using var peakCmd = connection.CreateCommand();
        peakCmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1 collection_time, sqlserver_cpu_utilization
FROM collect.cpu_utilization_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY sqlserver_cpu_utilization DESC;";

        peakCmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        peakCmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        DateTime? peakTime = null;
        int peakCpu = 0;
        using (var peakReader = await peakCmd.ExecuteReaderAsync())
        {
            if (await peakReader.ReadAsync())
            {
                peakTime = peakReader.GetDateTime(0);
                peakCpu = peakReader.GetInt32(1);
            }
        }

        if (peakTime == null) return;

        // Step 2: Get queries active within 2 minutes of peak
        using var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    [session_id],
    [database_name],
    [status],
    DATEDIFF(MILLISECOND, 0, [CPU]) AS cpu_time_ms,
    DATEDIFF(MILLISECOND, 0, [elapsed_time]) AS total_elapsed_time_ms,
    [reads] AS logical_reads,
    [wait_info] AS wait_type,
    0 AS dop,
    0 AS parallel_worker_count,
    LEFT(CAST([sql_text] AS NVARCHAR(MAX)), 500) AS query_text
FROM collect.query_snapshots
WHERE collection_time >= @spikeStart
AND   collection_time <= @spikeEnd
AND   CAST([sql_text] AS NVARCHAR(MAX)) NOT LIKE 'WAITFOR%'
ORDER BY DATEDIFF(MILLISECOND, 0, [CPU]) DESC;";

        queryCmd.Parameters.Add(new SqlParameter("@spikeStart", peakTime.Value.AddMinutes(-2)));
        queryCmd.Parameters.Add(new SqlParameter("@spikeEnd", peakTime.Value.AddMinutes(2)));

        var items = new List<object>();
        using (var reader = await queryCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                items.Add(new
                {
                    time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                    session_id = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                    database = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    status = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    cpu_time_ms = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                    elapsed_time_ms = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                    logical_reads = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                    wait_type = reader.IsDBNull(7) ? "" : reader.GetString(7),
                    dop = reader.IsDBNull(8) ? 0 : Convert.ToInt32(reader.GetValue(8)),
                    parallel_workers = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                    query_text = reader.IsDBNull(10) ? "" : reader.GetString(10)
                });
            }
        }

        if (items.Count > 0)
        {
            finding.DrillDown!["spike_peak"] = new
            {
                time = peakTime.Value.ToString("o"),
                cpu_percent = peakCpu
            };
            finding.DrillDown!["queries_at_spike"] = items;
        }
    }

    private async Task CollectTopCpuQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    database_name,
    CONVERT(VARCHAR(18), query_hash, 1) AS query_hash,
    CAST(SUM(total_worker_time_delta) AS BIGINT) AS total_cpu_us,
    CAST(SUM(execution_count_delta) AS BIGINT) AS exec_count,
    MAX(max_dop) AS max_dop,
    CAST(SUM(total_spills) AS BIGINT) AS spills,
    LEFT(CAST(DECOMPRESS(MAX(query_text)) AS NVARCHAR(MAX)), 500) AS query_text
FROM collect.query_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   total_worker_time_delta > 0
GROUP BY database_name, query_hash
ORDER BY CAST(SUM(total_worker_time_delta) AS BIGINT) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_hash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                total_cpu_ms = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)) / 1000.0,
                execution_count = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                max_dop = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                spills = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                query_text = reader.IsDBNull(6) ? "" : reader.GetString(6)
            });
        }

        if (items.Count > 0 && !finding.DrillDown!.ContainsKey("top_cpu_queries"))
            finding.DrillDown!["top_cpu_queries"] = items;
    }

    /// <summary>
    /// The plan-cache anomaly detector (§2): one row per offending <c>query_hash</c> whose
    /// CURRENT per-exec CPU has jumped to an abnormal multiple of its OWN trailing baseline,
    /// and which is a material CPU contributor in the window. This is the enrichment the
    /// (PR-B) "Clear cached plan (advanced)" affordance reads. PR-A emits the drill-down but
    /// does NOT register the handler / emit the affordance — dead-code-safe display only.
    ///
    /// <para>
    /// §2a ROW-LEVEL exclusion (the round-2 correctness fix): the delta framework
    /// (install/05_delta_framework.sql:218-307) assigns <c>delta = full cumulative raw
    /// total</c> on TWO arms — first-collection-of-a-plan_handle (the <c>pc.collection_id
    /// IS NULL</c> arm, :233-235) and the first-post-restart row (the
    /// <c>server_start_time &gt;= pc.collection_time</c> arm, :235-236). Both inject false
    /// anomalies on exactly this feature's target population. We exclude BOTH, per row, in
    /// BOTH the current and baseline windows: a row counts as a REAL prior-delta row only
    /// when an earlier collection exists for the same (sql_handle, offsets, plan_handle)
    /// AND that earlier collection_time is &gt; this row's server_start_time (i.e. the
    /// delta interval started at a real prior collection, not at compile/restart). The
    /// per-exec math (M-3) and the materiality CPU sum use ONLY these real-delta rows.
    /// </para>
    /// </summary>
    private async Task CollectAbnormalCpuPlans(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys)
    {
        // The anomaly threshold (sibling of PLAN_REGRESSION's regression_factor) and the
        // materiality floor (a query must contribute at least this much CPU in the window
        // for clearing to be worth offering). Conservative defaults — start ~3x.
        const double AnomalyThreshold = 3.0;
        const double MaterialCpuMsFloor = 1000.0;   // 1s of CPU in the window

        // Co-fired-fact awareness (drives the §5 disclosure steer): whether this CPU
        // finding's story crossed PLAN_REGRESSION / PARAMETER_SENSITIVITY. The analysis
        // already holds the story path — no extra SQL.
        var planRegressionCoFired = pathKeys.Contains("PLAN_REGRESSION");
        var parameterSensitivityCoFired = pathKeys.Contains("PARAMETER_SENSITIVITY");

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        // The baseline window is the @baselineDays preceding the current window (NOT
        // overlapping it). The current window is [@startTime, @endTime].
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE @baselineStart datetime2(7) = DATEADD(DAY, -@baselineDays, @startTime);

/*
Real-delta rows only (§2a row-level exclusion): a row is a genuine inter-collection
delta when an EARLIER collection exists for the same (sql_handle, offsets, plan_handle)
whose collection_time is BOTH < this row's collection_time (it is a prior) AND
> this row's server_start_time (so the delta interval did not start at compile/restart).
This drops the first-collection-of-a-plan_handle row and the first-post-restart row in
one predicate, by row, without using sample_interval_seconds (R2-MIN-A).
*/
WITH
    real_delta AS
(
    SELECT
        qs.query_hash,
        qs.database_name,
        qs.collection_time,
        qs.total_worker_time_delta,
        qs.execution_count_delta,
        qs.plan_handle,
        qs.query_text
    FROM collect.query_stats AS qs
    WHERE qs.query_hash IS NOT NULL
    AND   qs.collection_time >= @baselineStart
    AND   qs.collection_time <= @endTime
    AND   EXISTS
          (
              SELECT 1
              FROM collect.query_stats AS prior
              WHERE prior.sql_handle = qs.sql_handle
              AND   prior.statement_start_offset = qs.statement_start_offset
              AND   prior.statement_end_offset = qs.statement_end_offset
              AND   prior.plan_handle = qs.plan_handle
              AND   prior.collection_time < qs.collection_time
              AND   prior.collection_time > qs.server_start_time
          )
),
    windowed AS
(
    SELECT
        rd.query_hash,
        /* current-window per-exec CPU (ms): SUM(worker)/SUM(execs) on real-delta rows */
        current_worker_ms =
            CAST(SUM(CASE WHEN rd.collection_time >= @startTime THEN rd.total_worker_time_delta ELSE 0 END) AS float) / 1000.0,
        current_execs =
            SUM(CASE WHEN rd.collection_time >= @startTime THEN rd.execution_count_delta ELSE 0 END),
        /* baseline-window per-exec CPU (ms): the preceding window, same exclusion */
        baseline_worker_ms =
            CAST(SUM(CASE WHEN rd.collection_time < @startTime THEN rd.total_worker_time_delta ELSE 0 END) AS float) / 1000.0,
        baseline_execs =
            SUM(CASE WHEN rd.collection_time < @startTime THEN rd.execution_count_delta ELSE 0 END),
        /* window CPU contribution (ms), §2a exclusion applied (R2-MIN-B) */
        current_total_cpu_ms =
            CAST(SUM(CASE WHEN rd.collection_time >= @startTime THEN rd.total_worker_time_delta ELSE 0 END) AS float) / 1000.0
    FROM real_delta AS rd
    GROUP BY rd.query_hash
),
    /*
    LOW-1 fix: the window's TOTAL query CPU (ms) over the SAME §2a real-delta rows in the
    current window, so each query's cpu_percent is its real share of query CPU over the
    window (NOT the hardcoded 0 that understated risk in PR-A). Using the same exclusion
    keeps the numerator and denominator consistent (a query can't show a share computed on
    contaminated raw-total CPU it won't actually clear, R2-MIN-B).
    */
    window_total AS
(
    SELECT
        total_cpu_ms =
            CAST(SUM(CASE WHEN rd.collection_time >= @startTime THEN rd.total_worker_time_delta ELSE 0 END) AS float) / 1000.0
    FROM real_delta AS rd
)
SELECT TOP 5
    query_hash = CONVERT(VARCHAR(18), w.query_hash, 1),
    database_name =
    (
        SELECT TOP (1) rd2.database_name
        FROM real_delta AS rd2
        WHERE rd2.query_hash = w.query_hash
        ORDER BY rd2.collection_time DESC
    ),
    current_cpu_per_exec_ms = w.current_worker_ms / NULLIF(w.current_execs, 0),
    baseline_cpu_per_exec_ms = w.baseline_worker_ms / NULLIF(w.baseline_execs, 0),
    anomaly_ratio =
        (w.current_worker_ms / NULLIF(w.current_execs, 0)) /
        NULLIF(w.baseline_worker_ms / NULLIF(w.baseline_execs, 0), 0),
    execution_count = w.current_execs,
    total_cpu_ms = w.current_total_cpu_ms,
    /* LOW-1: real share of the window's total query CPU (rounded int %), 0 when the window
       total is non-positive (degenerate). Display-only — carried into the disclosure. */
    cpu_percent =
        CONVERT(int, ROUND(100.0 * w.current_total_cpu_ms / NULLIF(wt.total_cpu_ms, 0), 0)),
    latest_plan_handle =
    (
        SELECT TOP (1) CONVERT(VARCHAR(130), rd3.plan_handle, 1)
        FROM real_delta AS rd3
        WHERE rd3.query_hash = w.query_hash
        AND   rd3.plan_handle IS NOT NULL
        ORDER BY rd3.collection_time DESC
    ),
    query_text =
    (
        SELECT TOP (1) LEFT(CAST(DECOMPRESS(rd4.query_text) AS NVARCHAR(MAX)), 500)
        FROM real_delta AS rd4
        WHERE rd4.query_hash = w.query_hash
        ORDER BY rd4.collection_time DESC
    )
FROM windowed AS w
CROSS JOIN window_total AS wt
WHERE w.current_execs > 0
AND   w.baseline_execs > 0
AND   w.baseline_worker_ms > 0
AND   w.current_total_cpu_ms >= @materialFloor
/* the anomaly gate: current per-exec >= T x baseline per-exec */
AND   (w.current_worker_ms / NULLIF(w.current_execs, 0)) >=
      @threshold * (w.baseline_worker_ms / NULLIF(w.baseline_execs, 0))
ORDER BY w.current_total_cpu_ms DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));
        cmd.Parameters.Add(new SqlParameter("@baselineDays", 7));
        cmd.Parameters.Add(new SqlParameter("@threshold", AnomalyThreshold));
        cmd.Parameters.Add(new SqlParameter("@materialFloor", MaterialCpuMsFloor));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                query_hash = reader.IsDBNull(0) ? "" : reader.GetString(0),
                database = reader.IsDBNull(1) ? "" : reader.GetString(1),
                current_cpu_per_exec_ms = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                baseline_cpu_per_exec_ms = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                anomaly_ratio = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4)),
                execution_count = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                total_cpu_ms = reader.IsDBNull(6) ? 0.0 : Convert.ToDouble(reader.GetValue(6)),
                // LOW-1 fix: the REAL window CPU share (was hardcoded 0 in PR-A, which made
                // the disclosure render "responsible for 0% of CPU" and understate the risk).
                cpu_percent = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
                latest_plan_handle = reader.IsDBNull(8) ? "" : reader.GetString(8),
                query_text = reader.IsDBNull(9) ? "" : reader.GetString(9),
                // §2b co-fired flags (drive the §5 disclosure steer); display-only.
                plan_regression_cofired = planRegressionCoFired,
                parameter_sensitivity_cofired = parameterSensitivityCoFired
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["abnormal_cpu_plans"] = items;
    }

    private async Task CollectTopSpillingQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    database_name,
    CONVERT(VARCHAR(18), query_hash, 1) AS query_hash,
    CAST(SUM(total_spills) AS BIGINT) AS total_spills,
    CAST(SUM(execution_count_delta) AS BIGINT) AS exec_count,
    LEFT(CAST(DECOMPRESS(MAX(query_text)) AS NVARCHAR(MAX)), 500) AS query_text
FROM collect.query_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   total_spills > 0
GROUP BY database_name, query_hash
ORDER BY CAST(SUM(total_spills) AS BIGINT) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_hash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                total_spills = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
                execution_count = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                query_text = reader.IsDBNull(4) ? "" : reader.GetString(4)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_spilling_queries"] = items;
    }

    /// <summary>
    /// Top parameter-sensitive plans behind a PARAMETER_SENSITIVITY finding.
    /// Re-runs the detector for the top 5 offenders.
    /// </summary>
    private async Task CollectParameterSensitiveQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH latest AS
(
    SELECT
        database_name,
        query_hash,
        query_plan_hash,
        execution_count,
        creation_time,
        min_worker_time,
        max_worker_time,
        min_grant_kb,
        max_grant_kb,
        min_spills,
        max_spills,
        query_text,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_hash, query_plan_hash
            ORDER BY collection_time DESC
        ) AS rn
    FROM collect.query_stats
    WHERE collection_time >= @startTime
    AND   collection_time <= @endTime
    AND   execution_count_delta > 0
)
SELECT
    database_name,
    CONVERT(varchar(18), query_hash, 1) AS query_hash,
    CONVERT(varchar(18), query_plan_hash, 1) AS query_plan_hash,
    execution_count,
    min_worker_time,
    max_worker_time,
    CAST(max_worker_time AS float) / NULLIF(min_worker_time, 0) AS worker_ratio,
    CAST(max_grant_kb AS float) / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence,
    LEFT(CAST(DECOMPRESS(query_text) AS NVARCHAR(MAX)), 500) AS query_text
FROM latest
WHERE rn = 1
AND   min_worker_time >= 10000
AND   max_worker_time >= 250000
AND   execution_count >= 20
AND   creation_time <= @startTime
AND   CAST(max_worker_time AS float) / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC
OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_hash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                query_plan_hash = reader.IsDBNull(2) ? "" : reader.GetString(2),
                execution_count = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                min_worker_time_us = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                max_worker_time_us = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                worker_ratio = reader.IsDBNull(6) ? 0.0 : Convert.ToDouble(reader.GetValue(6)),
                grant_ratio = reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7)),
                spills_on_some_inputs = !reader.IsDBNull(8) && Convert.ToInt32(reader.GetValue(8)) == 1,
                query_text = reader.IsDBNull(9) ? "" : reader.GetString(9)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["parameter_sensitive_queries"] = items;
    }

    /// <summary>
    /// Top regressed queries behind a PLAN_REGRESSION finding.
    /// Uses the same 14-day server_last_execution_time comparison window as the detector
    /// (NOT the standard analysis window) so the days-old "best plan" baseline is present.
    /// </summary>
    private async Task CollectRegressedQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH deduped AS
(
    SELECT
        database_name,
        query_id,
        plan_id,
        query_plan_hash,
        count_executions,
        avg_cpu_time,
        avg_duration,
        server_last_execution_time,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, server_first_execution_time
            ORDER BY collection_time DESC
        ) AS rn
    FROM collect.query_store_data
    WHERE execution_type_desc = N'Regular'
    AND   server_last_execution_time >= @windowStart
),
plan_agg AS
(
    -- query_plan_hash is invariant within a plan_id, so include it in the GROUP BY
    -- (MS Learn's MAX page does not list binary/varbinary in the accepted types).
    SELECT
        database_name,
        query_id,
        plan_id,
        query_plan_hash,
        SUM(count_executions) AS execs,
        CASE WHEN SUM(count_executions) > 0
             THEN SUM(avg_cpu_time * count_executions) / NULLIF(SUM(count_executions), 0)
             ELSE 0 END AS cpu_per_exec,
        CASE WHEN SUM(count_executions) > 0
             THEN SUM(avg_duration * count_executions) / NULLIF(SUM(count_executions), 0)
             ELSE 0 END AS dur_per_exec,
        MAX(server_last_execution_time) AS last_exec
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id, query_plan_hash
),
plan_dedup AS
(
    -- MAX(plan_id) carries the most recently observed plan_id in the hash partition
    -- forward — newer plans are less likely evicted by Query Store retention.
    -- Functionally any plan_id sharing the hash forces the same execution shape.
    SELECT
        database_name,
        query_id,
        query_plan_hash,
        MAX(plan_id) AS plan_id,
        SUM(execs) AS execs,
        CASE WHEN SUM(execs) > 0
             THEN SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0)
             ELSE 0 END AS cpu_per_exec,
        CASE WHEN SUM(execs) > 0
             THEN SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0)
             ELSE 0 END AS dur_per_exec,
        MAX(last_exec) AS last_exec
    FROM plan_agg
    GROUP BY database_name, query_id, query_plan_hash
    HAVING SUM(execs) >= 25
),
ranked AS
(
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id ORDER BY last_exec DESC) AS recency,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id ORDER BY cpu_per_exec ASC) AS cheapness
    FROM plan_dedup
),
compared AS
(
    SELECT
        l.database_name,
        l.query_id,
        l.query_plan_hash AS latest_plan_hash,
        l.cpu_per_exec AS latest_cpu,
        l.dur_per_exec AS latest_dur,
        b.query_plan_hash AS best_plan_hash,
        b.plan_id AS best_plan_id,
        b.cpu_per_exec AS best_cpu,
        b.dur_per_exec AS best_dur,
        (SELECT MAX(v)
         FROM (VALUES
             (CAST(l.cpu_per_exec AS float) / NULLIF(b.cpu_per_exec, 0)),
             (CAST(l.dur_per_exec AS float) / NULLIF(b.dur_per_exec, 0))
         ) AS x(v)) AS regression_factor
    FROM ranked AS l
    JOIN ranked AS b
      ON  b.database_name = l.database_name
      AND b.query_id = l.query_id
      AND b.cheapness = 1
    WHERE l.recency = 1
    AND   l.query_plan_hash <> b.query_plan_hash
)
SELECT
    c.database_name,
    c.query_id,
    CONVERT(varchar(18), c.latest_plan_hash, 1) AS latest_plan_hash,
    c.latest_cpu,
    c.latest_dur,
    CONVERT(varchar(18), c.best_plan_hash, 1) AS best_plan_hash,
    c.best_plan_id,
    c.best_cpu,
    c.best_dur,
    c.regression_factor,
    -- query_sql_text is varbinary(max); fetch it via APPLY (MAX() on varbinary(max) is invalid).
    LEFT(CAST(DECOMPRESS(qt.query_sql_text) AS NVARCHAR(MAX)), 500) AS query_text
FROM compared AS c
OUTER APPLY
(
    SELECT TOP (1) qs.query_sql_text
    FROM collect.query_store_data AS qs
    WHERE qs.database_name = c.database_name
    AND   qs.query_id = c.query_id
    AND   qs.query_plan_hash = c.latest_plan_hash
    AND   qs.server_last_execution_time >= @windowStart
    ORDER BY qs.server_last_execution_time DESC
) AS qt
WHERE c.regression_factor >= 2
ORDER BY c.regression_factor DESC
OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY";

        cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart.AddDays(-14)));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_id = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1)),
                latest_plan_hash = reader.IsDBNull(2) ? "" : reader.GetString(2),
                latest_cpu_per_exec_us = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                latest_duration_per_exec_us = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4)),
                best_plan_hash = reader.IsDBNull(5) ? "" : reader.GetString(5),
                best_plan_id = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                best_cpu_per_exec_us = reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7)),
                best_duration_per_exec_us = reader.IsDBNull(8) ? 0.0 : Convert.ToDouble(reader.GetValue(8)),
                regression_factor = reader.IsDBNull(9) ? 0.0 : Convert.ToDouble(reader.GetValue(9)),
                query_text = reader.IsDBNull(10) ? "" : reader.GetString(10)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["regressed_queries"] = items;
    }

    private async Task CollectBadActorDetail(AnalysisFinding finding, AnalysisContext context)
    {
        // Extract query_hash from the fact key (BAD_ACTOR_0x...)
        var queryHash = finding.RootFactKey.Replace("BAD_ACTOR_", "");
        if (string.IsNullOrEmpty(queryHash)) return;

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    database_name,
    CONVERT(VARCHAR(18), query_hash, 1) AS query_hash,
    LEFT(CAST(DECOMPRESS(MAX(query_text)) AS NVARCHAR(MAX)), 500) AS query_text,
    CAST(SUM(execution_count_delta) AS BIGINT) AS exec_count,
    CASE WHEN SUM(execution_count_delta) > 0
         THEN CAST(SUM(total_worker_time_delta) AS FLOAT) / SUM(execution_count_delta) / 1000.0
         ELSE 0 END AS avg_cpu_ms,
    CASE WHEN SUM(execution_count_delta) > 0
         THEN CAST(SUM(total_elapsed_time_delta) AS FLOAT) / SUM(execution_count_delta) / 1000.0
         ELSE 0 END AS avg_elapsed_ms,
    CASE WHEN SUM(execution_count_delta) > 0
         THEN CAST(SUM(total_logical_reads_delta) AS FLOAT) / SUM(execution_count_delta)
         ELSE 0 END AS avg_reads,
    CAST(SUM(total_worker_time_delta) AS BIGINT) AS total_cpu_us,
    CAST(SUM(total_logical_reads_delta) AS BIGINT) AS total_reads,
    CAST(SUM(total_spills) AS BIGINT) AS total_spills,
    MAX(max_dop) AS max_dop
FROM collect.query_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   query_hash = CONVERT(BINARY(8), @queryHash, 1)
GROUP BY database_name, query_hash;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));
        cmd.Parameters.Add(new SqlParameter("@queryHash", queryHash));

        using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            finding.DrillDown!["bad_actor_query"] = new
            {
                database = reader.IsDBNull(0) ? "" : reader.GetString(0),
                query_hash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                query_text = reader.IsDBNull(2) ? "" : reader.GetString(2),
                execution_count = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                avg_cpu_ms = reader.IsDBNull(4) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(4)), 2),
                avg_elapsed_ms = reader.IsDBNull(5) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(5)), 2),
                avg_reads = reader.IsDBNull(6) ? 0.0 : Math.Round(Convert.ToDouble(reader.GetValue(6)), 0),
                total_cpu_ms = reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7)) / 1000.0,
                total_reads = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                total_spills = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                max_dop = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10))
            };
        }
    }

    private async Task CollectPendingGrants(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    target_memory_mb, total_memory_mb, available_memory_mb,
    granted_memory_mb, used_memory_mb,
    grantee_count, waiter_count,
    timeout_error_count_delta, forced_grant_count_delta
FROM collect.memory_grant_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   waiter_count > 0
ORDER BY waiter_count DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                target_memory_mb = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                total_memory_mb = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                available_memory_mb = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                granted_memory_mb = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4)),
                used_memory_mb = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5)),
                grantee_count = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                waiter_count = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                timeout_errors = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                forced_grants = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["pending_grants"] = items;
    }
}

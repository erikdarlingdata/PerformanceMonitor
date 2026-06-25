using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerFactCollector
{
    /// <summary>
    /// Collects query-level aggregate facts from query_stats.
    /// Focuses on spills (memory grant misestimates) and high-parallelism queries.
    /// </summary>
    private async Task CollectQueryStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    SUM(total_spills) AS total_spills,
    COUNT(CASE WHEN max_dop > 8 THEN 1 END) AS high_dop_queries,
    COUNT(CASE WHEN total_spills > 0 THEN 1 END) AS spilling_queries,
    SUM(execution_count_delta) AS total_executions,
    SUM(total_worker_time_delta) AS total_cpu_time_us
FROM collect.query_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   execution_count_delta > 0";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var totalSpills = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            var highDopQueries = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var spillingQueries = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var totalExecutions = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var totalCpuTimeUs = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

            if (totalSpills > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "QUERY_SPILLS",
                    Value = totalSpills,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["total_spills"] = totalSpills,
                        ["spilling_query_count"] = spillingQueries,
                        ["total_executions"] = totalExecutions
                    }
                });
            }

            if (highDopQueries > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "QUERY_HIGH_DOP",
                    Value = highDopQueries,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["high_dop_query_count"] = highDopQueries,
                        ["total_cpu_time_us"] = totalCpuTimeUs,
                        ["total_executions"] = totalExecutions
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectQueryStatsFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Detects parameter-sensitive cached plans: a single query_plan_hash whose
    /// per-execution worker time varies wildly — one plan serving very different
    /// parameter values. Emits one aggregate PARAMETER_SENSITIVITY fact.
    /// Note min_*/max_* are cumulative over the plan's cached lifetime, so the
    /// finding means "this plan, active now, has a history of widely varying cost".
    /// </summary>
    private async Task CollectParameterSensitivityFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH latest AS
(
    SELECT
        query_hash,
        query_plan_hash,
        database_name,
        execution_count,
        creation_time,
        min_worker_time,
        max_worker_time,
        min_grant_kb,
        max_grant_kb,
        min_spills,
        max_spills,
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
    min_worker_time,
    max_worker_time,
    CAST(max_worker_time AS float) / NULLIF(min_worker_time, 0) AS worker_ratio,
    CAST(max_grant_kb AS float) / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence
FROM latest
WHERE rn = 1
AND   min_worker_time >= 10000
AND   max_worker_time >= 250000
AND   execution_count >= 20
AND   creation_time <= @startTime
AND   CAST(max_worker_time AS float) / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC
OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            var offenderCount = 0;
            var worstRatio = 0.0;
            var worstMinWorker = 0L;
            var worstMaxWorker = 0L;
            var worstGrantRatio = 0.0;
            var worstSpillDivergence = 0;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                // Rows arrive ordered by worker_ratio DESC — the first row is the worst offender.
                if (offenderCount == 0)
                {
                    worstMinWorker = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
                    worstMaxWorker = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
                    worstRatio = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
                    worstGrantRatio = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                    worstSpillDivergence = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4));
                }
                offenderCount++;
            }

            if (offenderCount == 0) return;

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "PARAMETER_SENSITIVITY",
                Value = worstRatio,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["offender_count"] = offenderCount,
                    ["worst_ratio"] = worstRatio,
                    ["worst_min_worker_us"] = worstMinWorker,
                    ["worst_max_worker_us"] = worstMaxWorker,
                    ["worst_grant_ratio"] = worstGrantRatio,
                    ["grant_divergence"] = worstGrantRatio >= 5 ? 1 : 0,
                    ["spill_divergence"] = worstSpillDivergence
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectParameterSensitivityFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Detects plan regressions: a query whose currently-active plan has per-execution
    /// cost >= 2x the best plan that query is known to perform well with. Emits one
    /// aggregate PLAN_REGRESSION fact. Sourced from Query Store (collect.query_store_data);
    /// no fact when Query Store is not enabled on the monitored databases.
    /// Unlike other collectors this windows on server_last_execution_time (14-day
    /// comparison window), NOT collection_time.
    /// </summary>
    private async Task CollectPlanRegressionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH deduped AS
(
    -- Collapse incremental re-collections of the same open runtime-stats interval:
    -- keep only the latest collection_time row per logical interval.
    SELECT
        database_name,
        query_id,
        plan_id,
        query_plan_hash,
        count_executions,
        avg_cpu_time,
        avg_duration,
        server_last_execution_time,
        is_forced_plan,
        force_failure_count,
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
    -- Execution-weighted per-exec cost per plan_id. query_plan_hash is invariant
    -- within a plan_id, so include it in the GROUP BY rather than aggregating it
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
        MAX(server_last_execution_time) AS last_exec,
        MAX(CAST(is_forced_plan AS tinyint)) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id, query_plan_hash
),
plan_dedup AS
(
    -- Collapse plan_ids that share a query_plan_hash (a recompile can produce an
    -- identical plan under a new plan_id); keep only plans with enough executions.
    SELECT
        database_name,
        query_id,
        query_plan_hash,
        SUM(execs) AS execs,
        CASE WHEN SUM(execs) > 0
             THEN SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0)
             ELSE 0 END AS cpu_per_exec,
        CASE WHEN SUM(execs) > 0
             THEN SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0)
             ELSE 0 END AS dur_per_exec,
        MAX(last_exec) AS last_exec,
        MAX(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count
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
    -- Latest active plan vs the best-performing plan for the same query.
    SELECT
        l.query_id,
        l.cpu_per_exec AS latest_cpu,
        l.dur_per_exec AS latest_dur,
        l.is_forced_plan AS latest_is_forced,
        l.force_failure_count AS force_failure_count,
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
    query_id,
    latest_cpu,
    latest_dur,
    latest_is_forced,
    force_failure_count,
    best_cpu,
    best_dur,
    regression_factor
FROM compared
WHERE regression_factor >= 2
ORDER BY regression_factor DESC
OFFSET 0 ROWS FETCH NEXT 20 ROWS ONLY";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart.AddDays(-14)));

            var offenderCount = 0;
            var worstFactor = 0.0;
            var worstQueryId = 0L;
            var worstLatestCpu = 0.0;
            var worstBestCpu = 0.0;
            var worstDimension = 1;
            var worstLatestForced = 0;
            var worstForceFailures = 0L;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                // Rows arrive ordered by regression_factor DESC — the first row is the worst offender.
                if (offenderCount == 0)
                {
                    worstQueryId = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
                    var latestCpu = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
                    var latestDur = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
                    worstLatestForced = (!reader.IsDBNull(3) && Convert.ToInt32(reader.GetValue(3)) > 0) ? 1 : 0;
                    worstForceFailures = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
                    var bestCpu = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));
                    var bestDur = reader.IsDBNull(6) ? 0.0 : Convert.ToDouble(reader.GetValue(6));
                    worstFactor = reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7));

                    worstLatestCpu = latestCpu;
                    worstBestCpu = bestCpu;
                    var cpuRatio = bestCpu > 0 ? latestCpu / bestCpu : 0.0;
                    var durRatio = bestDur > 0 ? latestDur / bestDur : 0.0;
                    worstDimension = cpuRatio >= durRatio ? 1 : 2; // 1 = cpu, 2 = duration
                }
                offenderCount++;
            }

            if (offenderCount == 0) return;

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "PLAN_REGRESSION",
                Value = worstFactor,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["offender_count"] = offenderCount,
                    ["worst_regression_factor"] = worstFactor,
                    ["worst_query_id"] = worstQueryId,
                    ["latest_cpu_per_exec_us"] = worstLatestCpu,
                    ["best_cpu_per_exec_us"] = worstBestCpu,
                    ["regressed_dimension"] = worstDimension,
                    ["latest_is_forced"] = worstLatestForced,
                    ["force_failure_count"] = worstForceFailures
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectPlanRegressionFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// Collects procedure stats: top procedure by delta CPU time in the period.
    /// </summary>
    private async Task CollectProcedureStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    COUNT(DISTINCT object_name) AS distinct_procs,
    SUM(execution_count_delta) AS total_executions,
    SUM(total_worker_time_delta) AS total_cpu_time_us,
    SUM(total_elapsed_time_delta) AS total_elapsed_time_us,
    SUM(total_logical_reads_delta) AS total_logical_reads
FROM collect.procedure_stats
WHERE collection_time >= @startTime
AND   collection_time <= @endTime
AND   execution_count_delta > 0";

            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var distinctProcs = reader.IsDBNull(0) ? 0L : Convert.ToInt64(reader.GetValue(0));
            var totalExecs = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var totalCpuUs = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var totalElapsedUs = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
            var totalReads = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

            if (totalExecs == 0) return;

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "PROCEDURE_STATS",
                Value = totalCpuUs,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["distinct_procedures"] = distinctProcs,
                    ["total_executions"] = totalExecs,
                    ["total_cpu_time_us"] = totalCpuUs,
                    ["total_elapsed_time_us"] = totalElapsedUs,
                    ["total_logical_reads"] = totalReads,
                    ["avg_cpu_per_exec_us"] = totalExecs > 0 ? (double)totalCpuUs / totalExecs : 0
                }
            });
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectProcedureStatsFactsAsync failed", ex);
        }
    }

    /// <summary>
    /// WS4: plan-XML advisories. Parses the already-collected query plans of the top queries by
    /// cost (no live fetch, no DMV) with the shared ShowPlanParser/PlanAnalyzer and emits two
    /// advise-only facts — MISSING_INDEX (Value = distinct suggested indexes) and PLAN_WARNING
    /// (Value = actionable warnings). The specific indexes/warnings ride in the finding drill-down
    /// (SqlServerDrillDownCollector); Fact.Metadata is numeric only.
    /// </summary>
    private async Task CollectPlanAdvisoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var planXmls = new List<string>();

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (10)
    plan_xml = CAST(DECOMPRESS(qs.query_plan_text) AS nvarchar(max))
FROM collect.query_stats AS qs
WHERE qs.collection_time >= @startTime
AND   qs.collection_time <= @endTime
AND   qs.query_plan_text IS NOT NULL
ORDER BY
    qs.total_worker_time DESC;";
                cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
                cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    if (!reader.IsDBNull(0))
                        planXmls.Add(reader.GetString(0));
                }
            }

            if (planXmls.Count == 0)
                return;

            var summary = PlanAdvisoryAggregator.Summarize(planXmls);

            if (summary.MissingIndexCount > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "MISSING_INDEX",
                    Value = summary.MissingIndexCount,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["index_count"] = summary.MissingIndexCount,
                        ["max_impact"] = summary.MaxImpact
                    }
                });
            }

            if (summary.WarningCount > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "PLAN_WARNING",
                    Value = summary.WarningCount,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["warning_count"] = summary.WarningCount,
                        ["critical_count"] = summary.CriticalCount
                    }
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerFactCollector.CollectPlanAdvisoryFactsAsync failed", ex);
        }
    }
}

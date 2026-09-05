using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

public partial class DuckDbFactCollector
{
    /// <summary>
    /// Collects query-level aggregate facts from query_stats.
    /// Focuses on spills (memory grant misestimates) and high-parallelism queries.
    /// </summary>
    private async Task CollectQueryStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    SUM(delta_spills) AS total_spills,
    COUNT(CASE WHEN max_dop > 8 THEN 1 END) AS high_dop_queries,
    COUNT(CASE WHEN delta_spills > 0 THEN 1 END) AS spilling_queries,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_time_us
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalSpills = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var highDopQueries = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var spillingQueries = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalExecutions = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalCpuTimeUs = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
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
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH svr AS
(
    -- creation_time is the MONITORED SERVER's local wall clock -- QueryStatsCollector ships the
    -- dm_exec_query_stats value verbatim -- while the window bound is naive UTC off DateTime.UtcNow.
    -- De-skewing the column by the collected offset is what lets the compiled-before-the-window test
    -- compare a single frame. Untranslated, a negative offset admits exactly the in-window plans this
    -- predicate exists to exclude, and a positive one discards plans that legitimately predate the
    -- window; either way a wide min/max worker-time spread stops being evidence of parameter
    -- sensitivity and becomes an artefact of plan age. COALESCE to 0 because server_properties is an
    -- on-load collector, so an absent offset is the state every server passes through on its first
    -- cycle -- refusing the read there would pre-empt the two answers that outrank any window. The
    -- CTE returns exactly one row, so no plan is lost to it.
    SELECT COALESCE
    (
        (
            SELECT utc_offset_minutes
            FROM v_server_properties
            WHERE server_id = $1
            AND   utc_offset_minutes IS NOT NULL
            ORDER BY collection_time DESC
            LIMIT 1
        ),
        0
    ) AS offset_minutes
),
latest AS
(
    SELECT
        query_hash,
        query_plan_hash,
        database_name,
        execution_count,
        creation_time - svr.offset_minutes * INTERVAL '1' MINUTE AS creation_time_utc,
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
    FROM v_query_stats, svr
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   delta_execution_count > 0
)
SELECT
    min_worker_time,
    max_worker_time,
    max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) AS worker_ratio,
    max_grant_kb::DOUBLE PRECISION / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence
FROM latest
WHERE rn = 1
AND   min_worker_time >= 10000
AND   max_worker_time >= 250000
AND   execution_count >= 20
AND   creation_time_utc <= $2
AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC
LIMIT 20";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            var offenderCount = 0;
            var worstRatio = 0.0;
            var worstMinWorker = 0L;
            var worstMaxWorker = 0L;
            var worstGrantRatio = 0.0;
            var worstSpillDivergence = 0;

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                // Rows arrive ordered by worker_ratio DESC — the first row is the worst offender.
                if (offenderCount == 0)
                {
                    worstMinWorker = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
                    worstMaxWorker = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Detects plan regressions: a query whose currently-active plan has per-execution
    /// cost >= 2x the best plan that query is known to perform well with. Emits one
    /// aggregate PLAN_REGRESSION fact. Sourced from Query Store (v_query_store_stats);
    /// no fact when Query Store is not enabled on the monitored databases.
    /// Unlike other collectors this windows on last_execution_time (14-day comparison
    /// window), NOT collection_time — see plan note.
    /// </summary>
    private async Task CollectPlanRegressionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH deduped AS
(
    -- Collapse incremental re-collections of the same open runtime-stats interval:
    -- keep only the latest collection_time row per logical interval.
    --
    -- #1850: replica_role is part of the interval's identity, not a passenger.
    -- sys.query_store_runtime_stats is keyed by (plan_id, interval, execution_type, replica_group), and
    -- on a SQL Server 2022+ AG with Query Store for secondary replicas enabled the primary holds ONE
    -- shared Query Store carrying every replica's rows. Two rows differing only in replica_role are
    -- distinct legitimate work, so a partition without it does not de-duplicate — it DISCARDS one
    -- replica's row at the rn = 1 filter. That is an under-count, which is strictly worse than the
    -- double-count this CTE exists to fix: a double-count is visible in the number, a dropped row is
    -- silent. Same reasoning, same key as the read side (#1845).
    -- execution_type_desc is correctly absent: the WHERE pins it to 'Regular', so it is constant here.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        query_plan_hash,
        execution_count,
        avg_cpu_time_us,
        avg_duration_us,
        last_execution_time,
        is_forced_plan,
        force_failure_count,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   execution_type_desc = 'Regular'
    AND   last_execution_time >= $2
),
plan_agg AS
(
    -- Execution-weighted per-exec cost per plan_id, per replica. Keeping replica_role in the grain all
    -- the way down is what makes the wider dedup key an improvement rather than a blend: were it dropped
    -- here, both replicas' rows would survive the dedup and then be summed into one number, and primary
    -- and secondary workload would be indistinguishable in the output.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        any_value(query_plan_hash) AS query_plan_hash,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        MAX(last_execution_time) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id, replica_role
),
plan_dedup AS
(
    -- Collapse plan_ids that share a query_plan_hash (a recompile can produce an
    -- identical plan under a new plan_id); keep only plans with enough executions.
    SELECT
        database_name,
        query_id,
        replica_role,
        query_plan_hash,
        SUM(execs) AS execs,
        SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0) AS cpu_per_exec,
        SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0) AS dur_per_exec,
        MAX(last_exec) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count
    FROM plan_agg
    GROUP BY database_name, query_id, replica_role, query_plan_hash
    HAVING SUM(execs) >= 25
),
ranked AS
(
    -- Rank WITHIN a replica: a regression means this replica's current plan is worse than the best plan
    -- this replica has run, never a cross-replica comparison of two different workloads.
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY last_exec DESC) AS recency,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY cpu_per_exec ASC) AS cheapness
    FROM plan_dedup
),
compared AS
(
    -- Latest active plan vs the best-performing plan for the same query on the same replica.
    -- IS NOT DISTINCT FROM, never =: replica_role is NULL on every standalone server, every non-AG
    -- server and everything below SQL Server 2022, and NULL = NULL is UNKNOWN — an equi-join here would
    -- match nothing and silently disable plan-regression detection for the overwhelming majority of
    -- installs. The NULL-safe operator groups those rows the same way the PARTITION BYs above do.
    SELECT
        l.query_id,
        l.cpu_per_exec AS latest_cpu,
        l.dur_per_exec AS latest_dur,
        l.is_forced_plan AS latest_is_forced,
        l.force_failure_count AS force_failure_count,
        b.cpu_per_exec AS best_cpu,
        b.dur_per_exec AS best_dur,
        -- #2138: CPU is the PRIMARY signal — duration alone is confounded by blocking, IO waits, and
        -- machine contention that no plan choice caused, so it must not fire a plan-regression verdict
        -- by itself. A CPU regression scores at its own ratio; a duration-dominant one fires only when
        -- EXTREME (>= 4x) AND corroborated by at least mild CPU worsening (>= 1.25x), scored at half
        -- the duration ratio so it competes honestly with CPU-detected rows. NULL when neither path
        -- fires — the >= 2 gate below drops it.
        CASE
            WHEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 2
                THEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0)
            WHEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) >= 4
             AND l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 1.25
                THEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) / 2
        END AS regression_factor,
        -- The resource-expenditure half of the importance gate (#2138): total CPU the LATEST plan burned
        -- over the window. The exec-count floor above only counts; this weighs.
        l.execs * l.cpu_per_exec AS latest_total_cpu_us
    FROM ranked AS l
    JOIN ranked AS b
      ON  b.database_name = l.database_name
      AND b.query_id = l.query_id
      AND b.replica_role IS NOT DISTINCT FROM l.replica_role
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
-- 10 CPU-seconds across the window: a NOISE floor, not an importance ranking — it exists to exclude
-- near-zero-cost queries whose ratios are all sampling jitter; magnitude ranking stays with
-- regression_factor and the scorer.
AND   latest_total_cpu_us >= 10000000
ORDER BY regression_factor DESC
LIMIT 20";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart.AddDays(-14) });

            var offenderCount = 0;
            var worstFactor = 0.0;
            var worstQueryId = 0L;
            var worstLatestCpu = 0.0;
            var worstBestCpu = 0.0;
            var worstDimension = 1;
            var worstLatestForced = 0;
            var worstForceFailures = 0L;

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                // Rows arrive ordered by regression_factor DESC — the first row is the worst offender.
                if (offenderCount == 0)
                {
                    worstQueryId = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
                    var latestCpu = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
                    worstLatestForced = (!reader.IsDBNull(3) && Convert.ToBoolean(reader.GetValue(3))) ? 1 : 0;
                    worstForceFailures = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
                    var bestCpu = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));
                    worstFactor = reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7));

                    worstLatestCpu = latestCpu;
                    worstBestCpu = bestCpu;
                    // Which CASE branch fired, not which raw ratio is larger (review catch on #2138):
                    // CPU has PRECEDENCE in the scoring, so a row with cpu 2.5x and duration 10x is a
                    // CPU-detected regression at 2.5 — comparing magnitudes would mislabel it duration.
                    var cpuRatio = bestCpu > 0 ? latestCpu / bestCpu : 0.0;
                    worstDimension = cpuRatio >= 2 ? 1 : 2; // 1 = cpu, 2 = duration
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects procedure stats: top procedure by delta CPU time in the period.
    /// </summary>
    private async Task CollectProcedureStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    COUNT(DISTINCT object_name) AS distinct_procs,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_time_us,
    SUM(delta_elapsed_time) AS total_elapsed_time_us,
    SUM(delta_logical_reads) AS total_logical_reads
FROM v_procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var distinctProcs = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var totalExecs = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalCpuUs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalElapsedUs = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalReads = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// WS4: plan-XML advisories. Parses the already-collected query plans of the top queries by
    /// cost with the shared ShowPlanParser/PlanAnalyzer and emits two advise-only facts —
    /// MISSING_INDEX (Value = distinct suggested indexes) and PLAN_WARNING (Value = actionable
    /// warnings). The specifics ride in the finding drill-down (DrillDownCollector); Fact.Metadata
    /// is numeric only. Mirrors the Dashboard SqlServerFactCollector against query_stats.query_plan_xml.
    /// </summary>
    private async Task CollectPlanAdvisoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var planXmls = new List<string>();

            using (var readLock = _duckDb.AcquireReadLock(context.CancellationToken))
            using (var connection = _duckDb.CreateConnection())
            {
                await connection.OpenAsync(context.CancellationToken);

                using var command = connection.CreateCommand();
                command.CommandText = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_plan_xml IS NOT NULL
ORDER BY delta_worker_time DESC
LIMIT 10";
                command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
                command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

                using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (!reader.IsDBNull(0))
                        planXmls.Add(reader.GetString(0));
                }
            }

            // Read lock released above; parse off the lock (CPU-only, no DB).
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            // query_stats / plan parse may be unavailable — skip, the advisory is best-effort.
            // An abandonment is NOT swallowed here (#2443).
            ReportCollectionFailure(ex, context);
        }
    }

}

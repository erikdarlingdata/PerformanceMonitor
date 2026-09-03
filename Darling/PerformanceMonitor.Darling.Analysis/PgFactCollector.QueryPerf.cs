/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /* #2705: max_dop on v_query_stats is sys.dm_exec_query_stats' lifetime-max for the plan's time in
       cache (QueryStatExtremes.cs's doctrine — same semantics as max/min_cpu_ms), so a plan compiled
       before 'max degree of parallelism' was lowered keeps reporting the old higher DOP until it is
       evicted or recompiled. get_top_queries_by_cpu's tool description has always carried this caveat
       for a human reader; this fact collector had no equivalent guard and fed the raw lifetime value
       into a QUERY_HIGH_DOP finding at full confidence. current_maxdop applies the same provable-tell
       reasoning QueryStatExtremes uses for CPU/elapsed: a max_dop reading that EXCEEDS what the
       server's current maxdop setting can produce right now is impossible under today's configuration,
       so it provably predates whatever change set that configuration and must not be counted. A
       current_maxdop of 0 (unlimited) or unknown (no server_config row yet) makes no configuration
       impossible, so the count is unchanged in both of those cases. */
    public const string QueryStatsSql = @"
WITH current_maxdop AS
(
    SELECT value_in_use
    FROM server_config
    WHERE server_id = $1
    AND   configuration_name = 'max degree of parallelism'
    ORDER BY capture_time DESC
    LIMIT 1
)
SELECT
    SUM(v.delta_spills) AS total_spills,
    COUNT(CASE WHEN v.max_dop > 8
                AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use)
               THEN 1 END) AS high_dop_queries,
    COUNT(CASE WHEN v.delta_spills > 0 THEN 1 END) AS spilling_queries,
    SUM(v.delta_execution_count) AS total_executions,
    SUM(v.delta_worker_time) AS total_cpu_time_us
FROM v_query_stats AS v
LEFT JOIN current_maxdop AS m ON true
WHERE v.server_id = $1
AND   v.collection_time >= $2
AND   v.collection_time <= $3
AND   v.delta_execution_count > 0";

    /// <summary>
    /// Collects query-level aggregate facts from query_stats.
    /// Focuses on spills (memory grant misestimates) and high-parallelism queries.
    /// </summary>
    private async Task CollectQueryStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(QueryStatsSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string ParameterSensitivitySql = @"
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
    FROM v_query_stats
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
AND   creation_time <= $2
AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC
LIMIT 20";

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
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(ParameterSensitivitySql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>The PLAN_REGRESSION comparison window, in days — how far back a query's "best known" plan
    /// may have been observed.</summary>
    internal const int PlanRegressionWindowDays = 14;

    /// <summary>
    /// How far BELOW the comparison window the chunk-exclusion bound sits (#2387). `last_execution_time`
    /// is the monitored server's clock and `collection_time` is the store's; a monitored server running
    /// ahead can report an execution time later than the collection that carried it. A day absorbs any
    /// plausible drift while still excluding all but ~15 days of a store that may hold months.
    /// </summary>
    internal const int PlanRegressionSkewMarginDays = 1;

    /* PG port: any_value() below is standard SQL:2023, in Postgres since 16 — the product's
       minimum supported PG is 17, so it stays verbatim (DuckDB and PG agree on its semantics:
       an arbitrary non-null value from the group). */
    public const string PlanRegressionSql = @"
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
    -- #2387: the SEMANTIC window is last_execution_time above; this is a REDUNDANT bound on the
    -- partitioning column so TimescaleDB can exclude chunks. Without it this reads the server's whole
    -- history every analysis cycle, decompressing whatever is compressed, per server -- cost scaling with
    -- STORE SIZE rather than with the configured window, which is why no VM size fixes it. Redundant to
    -- the ANSWER because a row cannot be collected before the execution it reports, so
    -- last_execution_time >= X already implies collection_time >= X. $3 carries a skew margin below X
    -- because last_execution_time comes from the MONITORED server's clock and collection_time from the
    -- store's: a monitored server running fast would otherwise have its newest rows excluded here, which
    -- would be a silent under-count and a worse bug than the one this fixes. Do not delete as dead
    -- weight -- it is doing all the pruning.
    AND   collection_time >= $3
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

    /// <summary>
    /// Detects plan regressions: a query whose currently-active plan has per-execution
    /// cost >= 2x the best plan that query is known to perform well with. Emits one
    /// aggregate PLAN_REGRESSION fact. Sourced from Query Store (v_query_store_stats);
    /// no fact when Query Store is not enabled on the monitored databases.
    /// Windows on last_execution_time (the 14-day comparison window) because a plan regression is about
    /// when the query last RAN, not when we happened to collect it. It ALSO carries a redundant
    /// collection_time bound so TimescaleDB can exclude chunks — see the note in the SQL (#2387).
    /// </summary>
    private async Task CollectPlanRegressionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PlanRegressionSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart.AddDays(-PlanRegressionWindowDays)));

            /* #2387: the chunk-exclusion bound, one CLOCK-SKEW MARGIN below the comparison window. Bound as
               its own parameter rather than written as "$2 - INTERVAL '1 day'" so the planner compares
               against a bare parameter, which is the form runtime chunk exclusion handles most reliably. */
            cmd.Parameters.AddWithValue(AsNaive(
                context.TimeRangeStart.AddDays(-(PlanRegressionWindowDays + PlanRegressionSkewMarginDays))));

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string ProcedureStatsSql = @"
SELECT
    COUNT(DISTINCT object_name) AS distinct_procs,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_time_us,
    SUM(delta_elapsed_time) AS total_elapsed_time_us,
    SUM(delta_logical_reads) AS total_logical_reads
FROM procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0";

    /// <summary>
    /// Collects procedure stats: top procedure by delta CPU time in the period.
    /// </summary>
    private async Task CollectProcedureStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(ProcedureStatsSql, connection);
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string PlanAdvisorySql = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_plan_xml IS NOT NULL
ORDER BY delta_worker_time DESC
LIMIT 10";

    /// <summary>
    /// WS4: plan-XML advisories. Parses the already-collected query plans of the top queries by
    /// cost with the shared ShowPlanParser/PlanAnalyzer and emits two advise-only facts —
    /// MISSING_INDEX (Value = distinct suggested indexes) and PLAN_WARNING (Value = actionable
    /// warnings). The specifics ride in the finding drill-down (DrillDownCollector); Fact.Metadata
    /// is numeric only. Mirrors the Lite/Dashboard fact collectors against query_stats.query_plan_xml.
    /// </summary>
    private async Task CollectPlanAdvisoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var planXmls = new List<string>();

            /* PG port: Lite scopes the connection in a block to release its DuckDB read lock
               before the CPU-only parse; the scoping is kept so the connection closes before
               the parse, even though PG holds no lock. */
            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var command = new NpgsqlCommand(PlanAdvisorySql, connection);
                command.Parameters.AddWithValue(context.ServerId);
                command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (!reader.IsDBNull(0))
                        planXmls.Add(reader.GetString(0));
                }
            }

            // Connection released above; parse off the store (CPU-only, no DB).
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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            // query_stats / plan parse may be unavailable — skip, the advisory is best-effort.
            // An abandonment is NOT swallowed here (#2443).
            ReportCollectionFailure(ex, context);
        }
    }

}

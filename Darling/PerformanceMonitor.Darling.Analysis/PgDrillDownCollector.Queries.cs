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

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgDrillDownCollector
{
    /// <summary>
    /// Per-command timeout for every drill-down query. These run inside the shared DarlingWorker sweep
    /// loop whose watchdog warns at 60s ("collection body has not completed after 60s" -> stale-status
    /// flapping), so no single drill-down query may run unbounded: even an optimized one must fail fast on
    /// a cold cache / index bloat / a much busier server rather than eat the whole budget. Npgsql defaults
    /// to 30s already, but pinning it here makes the bound explicit and independent of any global default.
    /// </summary>
    private const int DrillDownCommandTimeoutSeconds = 30;

    public const string SpikePeakSql = @"
SELECT collection_time, sqlserver_cpu_utilization
FROM v_cpu_utilization_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
ORDER BY sqlserver_cpu_utilization DESC
LIMIT 1";

    public const string QueriesAtSpikeSql = @"
SELECT collection_time, session_id, database_name, status,
       cpu_time_ms, total_elapsed_time_ms, logical_reads,
       wait_type, dop, parallel_worker_count,
       LEFT(query_text, 500) AS query_text
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_text NOT LIKE 'WAITFOR%'
ORDER BY cpu_time_ms DESC
LIMIT 5";

    private async Task CollectQueriesAtSpike(AnalysisFinding finding, AnalysisContext context)
    {
        // Find the peak CPU time, then get queries active within 2 minutes of it
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        // Step 1: Find when the spike occurred
        using var peakCmd = new NpgsqlCommand(SpikePeakSql, connection);
        peakCmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        peakCmd.Parameters.AddWithValue(context.ServerId);
        peakCmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        peakCmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        DateTime? peakTime = null;
        int peakCpu = 0;
        using (var peakReader = await peakCmd.ExecuteReaderAsync(context.CancellationToken))
        {
            if (await peakReader.ReadAsync(context.CancellationToken))
            {
                peakTime = peakReader.GetDateTime(0);
                peakCpu = peakReader.GetInt32(1);
            }
        }

        if (peakTime == null) return;

        // Step 2: Get queries active within 2 minutes of peak
        using var queryCmd = new NpgsqlCommand(QueriesAtSpikeSql, connection);
        queryCmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        queryCmd.Parameters.AddWithValue(context.ServerId);
        queryCmd.Parameters.AddWithValue(AsNaive(peakTime.Value.AddMinutes(-2)));
        queryCmd.Parameters.AddWithValue(AsNaive(peakTime.Value.AddMinutes(2)));

        var items = new List<object>();
        using (var reader = await queryCmd.ExecuteReaderAsync(context.CancellationToken))
        {
            while (await reader.ReadAsync(context.CancellationToken))
            {
                items.Add(new
                {
                    time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                    session_id = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                    database = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    status = reader.IsDBNull(3) ? "" : reader.GetString(3),
                    cpu_time_ms = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                    elapsed_time_ms = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                    logical_reads = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
                    wait_type = reader.IsDBNull(7) ? "" : reader.GetString(7),
                    dop = reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
                    parallel_workers = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
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

    public const string TopCpuQueriesSql = @"
SELECT database_name, query_hash,
       SUM(delta_worker_time)::BIGINT AS total_cpu_us,
       SUM(delta_execution_count)::BIGINT AS exec_count,
       MAX(max_dop) AS max_dop,
       SUM(delta_spills)::BIGINT AS spills,
       LEFT(MAX(query_text), 500) AS query_text
FROM v_query_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   delta_worker_time > 0
GROUP BY database_name, query_hash
ORDER BY total_cpu_us DESC
LIMIT 5";

    private async Task CollectTopCpuQueries(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(TopCpuQueriesSql, connection);
        cmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
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

    public const string TopSpillingQueriesSql = @"
SELECT database_name, query_hash,
       SUM(delta_spills)::BIGINT AS total_spills,
       SUM(delta_execution_count)::BIGINT AS exec_count,
       LEFT(MAX(query_text), 500) AS query_text
FROM v_query_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   delta_spills > 0
GROUP BY database_name, query_hash
ORDER BY total_spills DESC
LIMIT 5";

    private async Task CollectTopSpillingQueries(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(TopSpillingQueriesSql, connection);
        cmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
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

    public const string ParameterSensitiveSql = @"
WITH svr AS
(
    -- Detector A's creation_time de-skew, same shape and same reason: creation_time is the monitored
    -- server's local wall clock, the window bound is naive UTC, and 0 covers a server whose
    -- server_properties has not been collected yet. The CTE returns exactly one row, so nothing is lost.
    SELECT COALESCE
    (
        (
            SELECT sp.utc_offset_minutes
            FROM server_properties AS sp
            WHERE sp.server_id = $1
            AND   sp.utc_offset_minutes IS NOT NULL
            ORDER BY sp.collection_time DESC
            LIMIT 1
        ),
        0
    ) AS offset_minutes
),
latest AS
(
    SELECT
        database_name,
        query_hash,
        query_plan_hash,
        execution_count,
        creation_time - make_interval(mins => svr.offset_minutes) AS creation_time_utc,
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
    FROM v_query_stats, svr
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   delta_execution_count > 0
)
SELECT
    database_name,
    query_hash,
    query_plan_hash,
    execution_count,
    min_worker_time,
    max_worker_time,
    max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) AS worker_ratio,
    max_grant_kb::DOUBLE PRECISION / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence,
    LEFT(query_text, 500) AS query_text
FROM latest
WHERE rn = 1
AND   min_worker_time >= 10000
AND   max_worker_time >= 250000
AND   execution_count >= 20
AND   creation_time_utc <= $2
AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC
LIMIT 5";

    /// <summary>
    /// Top parameter-sensitive plans behind a PARAMETER_SENSITIVITY finding.
    /// Re-runs Detector A's detection (standard analysis window) for the top 5 offenders.
    /// </summary>
    private async Task CollectParameterSensitiveQueries(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(ParameterSensitiveSql, connection);
        cmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
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

    public const string RegressedQueriesSql = @"
WITH svr AS
(
    -- Detector A's creation_time de-skew, same shape and same reason: creation_time is the monitored
    -- server's local wall clock, the window bound is naive UTC, and 0 covers a server whose
    -- server_properties has not been collected yet. The CTE returns exactly one row, so nothing is lost.
    SELECT COALESCE
    (
        (
            SELECT sp.utc_offset_minutes
            FROM server_properties AS sp
            WHERE sp.server_id = $1
            AND   sp.utc_offset_minutes IS NOT NULL
            ORDER BY sp.collection_time DESC
            LIMIT 1
        ),
        0
    ) AS offset_minutes
),
psp_signature AS
(
    -- #2138 gap 3: the PARAMETER_SENSITIVITY detector's EXACT firing signature (same floors, same
    -- ratio, same analysis window) reduced to the (database, query_hash) set it would report. Using
    -- the detector's own thresholds is what keeps the flag honest: a query flagged here IS one the
    -- detector counts when it fires, never a looser lookalike. Grant/spill divergence stay metadata
    -- on the PSP side — they do not fire the detector alone, so they do not fire this flag alone.
    SELECT DISTINCT
        database_name,
        query_hash
    FROM
    (
        SELECT
            database_name,
            query_hash,
            query_plan_hash,
            execution_count,
            creation_time - make_interval(mins => svr.offset_minutes) AS creation_time_utc,
            min_worker_time,
            max_worker_time,
            ROW_NUMBER() OVER
            (
                PARTITION BY database_name, query_hash, query_plan_hash
                ORDER BY collection_time DESC
            ) AS rn
        FROM v_query_stats, svr
        WHERE server_id = $1
        AND   collection_time >= $3
        AND   collection_time <= $4
        AND   delta_execution_count > 0
    ) AS latest_cache
    WHERE rn = 1
    AND   min_worker_time >= 10000
    AND   max_worker_time >= 250000
    AND   execution_count >= 20
    AND   creation_time_utc <= $3
    AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10
),
deduped AS
(
    -- LOAD-BEARING (correctness, not just perf): query_store_stats rows are CUMULATIVE per-Query-Store-
    -- interval snapshots. The QueryStoreCollector is incremental and re-fetches the OPEN interval every
    -- cycle as its last_execution_time advances (WHERE last_execution_time > watermark), so the SAME
    -- interval (same first_execution_time) is collected repeatedly with a GROWING execution_count. Keep
    -- only the LATEST snapshot per interval; summing the accumulating snapshots would multiply-count
    -- executions and corrupt the execution_count-weighted cpu/duration averages.
    --
    -- #1850: replica_role is part of the interval's identity, not a passenger.
    -- sys.query_store_runtime_stats is keyed by (plan_id, interval, execution_type, replica_group), and
    -- on a SQL Server 2022+ AG with Query Store for secondary replicas enabled the primary holds ONE
    -- shared Query Store carrying every replica's rows. Two rows differing only in replica_role are
    -- distinct legitimate work, so a partition without it does not de-duplicate — it DISCARDS one
    -- replica's row at the rn = 1 filter. That is an under-count, which is strictly worse than the
    -- double-count this CTE exists to fix: a double-count is visible in the number, a dropped row is
    -- silent. Same reasoning, same key as the read side (#1845). It is carried through the grouping and
    -- out into the drill-down row below, so the operator sees WHICH replica regressed.
    -- execution_type_desc is correctly absent: the WHERE pins it to 'Regular', so it is constant here.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        query_plan_hash,
        query_hash,
        execution_count,
        avg_cpu_time_us,
        avg_duration_us,
        last_execution_time,
        query_text,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   execution_type_desc = 'Regular'
    AND   last_execution_time >= $2
    -- Chunk-exclusion bound. collection_time >= last_execution_time ALWAYS (a row is collected AFTER the
    -- interval's last execution), so last_execution_time >= $2 provably implies this; the 1-day slack is
    -- safety. query_store_stats is a hypertable partitioned on collection_time, so this lets TimescaleDB
    -- exclude whole chunks instead of decompress-then-filter as retained history grows.
    AND   collection_time >= $2 - interval '1 day'
),
plan_dedup AS
(
    -- Aggregate the DISTINCT intervals (rn = 1) per (query_id, plan hash). Collapses the old
    -- plan_agg(per plan_id) + plan_dedup(per hash) two-stage into one: a weighted-avg of weighted-avgs
    -- equals the direct execution_count-weighted avg, and MAX(plan_id) is unchanged. MAX(plan_id) carries
    -- the most recently observed plan_id in the hash forward (newer plans are less likely evicted by
    -- Query Store retention; any plan_id sharing the hash forces the same execution shape).
    SELECT
        database_name,
        query_id,
        replica_role,
        query_plan_hash,
        MAX(plan_id) AS plan_id,
        any_value(query_hash) AS query_hash,
        any_value(query_text) AS query_text,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        MAX(last_execution_time) AS last_exec
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, replica_role, query_plan_hash
    HAVING SUM(execution_count) >= 25
),
latest AS
(
    -- The most recently executed plan per query. DISTINCT ON instead of the old self-referential rank,
    -- so plan_dedup is materialized ONCE rather than the whole pipeline running twice (once per side).
    -- Per REPLICA as well as per query: a regression means this replica's current plan is worse than the
    -- best plan this replica has run, never a cross-replica comparison of two different workloads.
    -- DISTINCT ON groups NULL replica_role rows together (it is a grouping, not an equality test), so
    -- non-AG servers behave exactly as before.
    SELECT DISTINCT ON (database_name, query_id, replica_role) *
    FROM plan_dedup
    ORDER BY database_name, query_id, replica_role, last_exec DESC
),
cheapest AS
(
    -- The cheapest (best) plan per query by cpu-per-exec, on the same replica.
    SELECT DISTINCT ON (database_name, query_id, replica_role) *
    FROM plan_dedup
    ORDER BY database_name, query_id, replica_role, cpu_per_exec ASC
),
scored AS
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
        -- #2138: the SAME CPU-primary scoring as the PLAN_REGRESSION fact (PgFactCollector.QueryPerf.cs,
        -- where the rationale lives). The drill-down must agree with the fact that displays it: under the
        -- old GREATEST a duration-only regression could appear here that the fact never counted.
        CASE
            WHEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 2
                THEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0)
            WHEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) >= 4
             AND l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 1.25
                THEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) / 2
        END AS regression_factor,
        -- #2150: text lives in collect.query_store_text now, keyed on (server, database, query_id) — the
        -- grain `latest` is already at — so it resolves with the keyed join below rather than being carried
        -- up through any_value(). l.query_text stays as the fallback: it is where text lived before the
        -- cutover, so history collected earlier still shows a statement instead of an empty drill-down.
        LEFT(COALESCE(x.query_sql_text, l.query_text), 500) AS query_text,
        l.replica_role,
        l.execs * l.cpu_per_exec AS latest_total_cpu_us,
        -- #2138 gap 3: does this regressed query ALSO carry the parameter-sensitivity signature in the
        -- plan cache? Keyed on (database, query_hash) — the hash bridges Query Store and the cache.
        -- Steers the force-plan remediation's caution text; the future bot never auto-forces on true.
        EXISTS
        (
            SELECT 1
            FROM psp_signature AS p
            WHERE p.database_name = l.database_name
            AND   p.query_hash = l.query_hash
        ) AS parameter_sensitivity_cofired
    FROM latest AS l
    JOIN cheapest AS b
      ON  b.database_name = l.database_name
      AND b.query_id = l.query_id
      -- IS NOT DISTINCT FROM, never = (and never USING, which is an equi-join): replica_role is NULL on
      -- every standalone server, every non-AG server and everything below SQL Server 2022, and NULL = NULL
      -- is UNKNOWN — matching on it with = would join nothing and silently empty this drill-down for the
      -- overwhelming majority of installs. The NULL-safe operator groups those rows as DISTINCT ON does.
      AND b.replica_role IS NOT DISTINCT FROM l.replica_role
    -- #2150 text resolution (see the projection). LEFT, so a query whose text has not been fetched yet
    -- still reports its regression; one row per key by primary key, so no fan-out and none of the
    -- aggregates above are affected.
    LEFT JOIN query_store_text AS x
      ON  x.server_id = $1
      AND x.database_name = l.database_name
      AND x.query_id = l.query_id
    WHERE l.query_plan_hash <> b.query_plan_hash
)
SELECT
    database_name,
    query_id,
    latest_plan_hash,
    latest_cpu,
    latest_dur,
    best_plan_hash,
    best_plan_id,
    best_cpu,
    best_dur,
    regression_factor,
    query_text,
    replica_role,
    parameter_sensitivity_cofired
FROM scored
WHERE regression_factor >= 2
AND   latest_total_cpu_us >= 10000000
ORDER BY regression_factor DESC
LIMIT 5";

    /// <summary>
    /// Top regressed queries behind a PLAN_REGRESSION finding.
    /// Re-runs Detector B's detection for the top 5 offenders. Uses the same 14-day
    /// last_execution_time comparison window as the detector — NOT the standard analysis
    /// window — so the days-old "best plan" baseline is present.
    /// </summary>
    private async Task CollectRegressedQueries(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(RegressedQueriesSql, connection);
        cmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart.AddDays(-14)));
        /* $3/$4: the STANDARD analysis window for the psp_signature CTE — deliberately not the 14-day
           comparison window above, so the flag matches what the PARAMETER_SENSITIVITY detector itself
           would report for this run. */
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
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
                query_text = reader.IsDBNull(10) ? "" : reader.GetString(10),
                /* #1850: which replica this regression was measured on. NULL — rendered as "" — on every
                   standalone/non-AG/pre-2022 server, which is the overwhelming majority; it is only
                   populated on an AG primary with Query Store for secondary replicas enabled, where two
                   rows for the same query are now legitimately distinct rather than one silently dropped.
                   Appended after the older columns so the existing reader ordinals are untouched. */
                replica_role = reader.IsDBNull(11) ? "" : reader.GetString(11),
                /* #2138 gap 3: the plan-cache PSP signature co-fired for this query's hash. Steers the
                   force-plan caution text; the future bot never auto-forces a flagged target. */
                parameter_sensitivity_cofired = !reader.IsDBNull(12) && reader.GetBoolean(12)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["regressed_queries"] = items;
    }

    public const string BadActorDetailSql = @"
SELECT database_name, query_hash,
       LEFT(MAX(query_text), 500) AS query_text,
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
       MAX(max_dop) AS max_dop
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_hash = $4
GROUP BY database_name, query_hash";

    private async Task CollectBadActorDetail(AnalysisFinding finding, AnalysisContext context)
    {
        // Extract query_hash from the fact key (BAD_ACTOR_0x...)
        var queryHash = finding.RootFactKey.Replace("BAD_ACTOR_", "");
        if (string.IsNullOrEmpty(queryHash)) return;

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(BadActorDetailSql, connection);
        cmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(queryHash);

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (await reader.ReadAsync(context.CancellationToken))
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

    public const string PendingGrantsSql = @"
SELECT collection_time,
       target_memory_mb, total_memory_mb, available_memory_mb,
       granted_memory_mb, used_memory_mb,
       grantee_count, waiter_count,
       timeout_error_count_delta, forced_grant_count_delta
FROM v_memory_grant_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   waiter_count > 0
ORDER BY waiter_count DESC
LIMIT 5";

    private async Task CollectPendingGrants(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(PendingGrantsSql, connection);
        cmd.CommandTimeout = DrillDownCommandTimeoutSeconds;
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
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

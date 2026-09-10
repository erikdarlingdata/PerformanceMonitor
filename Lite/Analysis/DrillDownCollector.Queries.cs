using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Analysis;

public partial class DrillDownCollector
{
    private async Task CollectQueriesAtSpike(AnalysisFinding finding, AnalysisContext context)
    {
        // Find the peak CPU time, then get queries active within 2 minutes of it
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        // Step 1: Find when the spike occurred
        using var peakCmd = connection.CreateCommand();
        peakCmd.CommandText = @"
SELECT collection_time, sqlserver_cpu_utilization
FROM v_cpu_utilization_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
ORDER BY sqlserver_cpu_utilization DESC
LIMIT 1";

        peakCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        peakCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        peakCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
        using var queryCmd = connection.CreateCommand();
        queryCmd.CommandText = @"
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

        queryCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        queryCmd.Parameters.Add(new DuckDBParameter { Value = peakTime.Value.AddMinutes(-2) });
        queryCmd.Parameters.Add(new DuckDBParameter { Value = peakTime.Value.AddMinutes(2) });

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

    private async Task CollectTopCpuQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
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

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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

    private async Task CollectTopSpillingQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
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

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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

    /// <summary>
    /// Top parameter-sensitive plans behind a PARAMETER_SENSITIVITY finding.
    /// Re-runs Detector A's detection (standard analysis window) for the top 5 offenders.
    /// </summary>
    private async Task CollectParameterSensitiveQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
WITH svr AS
(
    -- Detector A's creation_time de-skew, same shape and same reason: creation_time is the monitored
    -- server's local wall clock, the window bound is naive UTC, and 0 covers a server whose
    -- server_properties has not been collected yet. The CTE returns exactly one row, so nothing is lost.
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
        database_name,
        query_hash,
        query_plan_hash,
        execution_count,
        creation_time - svr.offset_minutes * INTERVAL '1' MINUTE AS creation_time_utc,
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

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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

    /// <summary>
    /// Top regressed queries behind a PLAN_REGRESSION finding.
    /// Re-runs Detector B's detection for the top 5 offenders. Uses the same 14-day
    /// last_execution_time comparison window as the detector — NOT the standard analysis
    /// window — so the days-old "best plan" baseline is present.
    /// </summary>
    private async Task CollectRegressedQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
WITH svr AS
(
    -- Detector A's creation_time de-skew, same shape and same reason: creation_time is the monitored
    -- server's local wall clock, the window bound is naive UTC, and 0 covers a server whose
    -- server_properties has not been collected yet. The CTE returns exactly one row, so nothing is lost.
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
            creation_time - svr.offset_minutes * INTERVAL '1' MINUTE AS creation_time_utc,
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
),
plan_agg AS
(
    -- Per-exec cost per plan_id, per replica. Keeping replica_role in the grain all the way down is what
    -- makes the wider dedup key an improvement rather than a blend: were it dropped here, both replicas'
    -- rows would survive the dedup and then be summed into one number, and primary and secondary
    -- workload would be indistinguishable in the output.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        any_value(query_plan_hash) AS query_plan_hash,
        any_value(query_hash) AS query_hash,
        any_value(query_text) AS query_text,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        MAX(last_execution_time) AS last_exec
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id, replica_role
),
plan_dedup AS
(
    -- MAX(plan_id) carries the most recently observed plan_id in the hash partition
    -- forward — newer plans are less likely evicted by Query Store retention.
    -- Functionally any plan_id sharing the hash forces the same execution shape.
    SELECT
        database_name,
        query_id,
        replica_role,
        query_plan_hash,
        MAX(plan_id) AS plan_id,
        any_value(query_hash) AS query_hash,
        any_value(query_text) AS query_text,
        SUM(execs) AS execs,
        SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0) AS cpu_per_exec,
        SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0) AS dur_per_exec,
        MAX(last_exec) AS last_exec
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
    -- IS NOT DISTINCT FROM, never =: replica_role is NULL on every standalone server, every non-AG
    -- server and everything below SQL Server 2022, and NULL = NULL is UNKNOWN — an equi-join here would
    -- match nothing and silently empty this drill-down for the overwhelming majority of installs. The
    -- NULL-safe operator groups those rows the same way the PARTITION BYs above do.
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
        l.query_text,
        -- #2138: the SAME CPU-primary scoring as the PLAN_REGRESSION fact (DuckDbFactCollector.QueryPerf.cs,
        -- where the rationale lives). The drill-down must agree with the fact that displays it: under the
        -- old GREATEST a duration-only regression could appear here that the fact never counted.
        CASE
            WHEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 2
                THEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0)
            WHEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) >= 4
             AND l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 1.25
                THEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) / 2
        END AS regression_factor,
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
    LEFT(query_text, 500) AS query_text,
    replica_role,
    parameter_sensitivity_cofired
FROM compared
WHERE regression_factor >= 2
AND   latest_total_cpu_us >= 10000000
ORDER BY regression_factor DESC
LIMIT 5";

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart.AddDays(-14) });
        /* $3/$4: the STANDARD analysis window for the psp_signature CTE — deliberately not the 14-day
           comparison window above, so the flag matches what the PARAMETER_SENSITIVITY detector itself
           would report for this run. */
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
                parameter_sensitivity_cofired = !reader.IsDBNull(12) && Convert.ToBoolean(reader.GetValue(12))
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

        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
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

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });

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

    private async Task CollectPendingGrants(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
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

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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

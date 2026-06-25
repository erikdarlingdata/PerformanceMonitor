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
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

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
        using (var reader = await queryCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
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
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

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

    private async Task CollectTopSpillingQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

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
    /// Re-runs Detector A's detection (standard analysis window) for the top 5 offenders.
    /// </summary>
    private async Task CollectParameterSensitiveQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
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
    FROM v_query_stats
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
    max_worker_time::DOUBLE / NULLIF(min_worker_time, 0) AS worker_ratio,
    max_grant_kb::DOUBLE / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence,
    LEFT(query_text, 500) AS query_text
FROM latest
WHERE rn = 1
AND   min_worker_time >= 10000
AND   max_worker_time >= 250000
AND   execution_count >= 20
AND   creation_time <= $2
AND   max_worker_time::DOUBLE / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC
LIMIT 5";

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
    /// Re-runs Detector B's detection for the top 5 offenders. Uses the same 14-day
    /// last_execution_time comparison window as the detector — NOT the standard analysis
    /// window — so the days-old "best plan" baseline is present.
    /// </summary>
    private async Task CollectRegressedQueries(AnalysisFinding finding, AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
WITH deduped AS
(
    SELECT
        database_name,
        query_id,
        plan_id,
        query_plan_hash,
        execution_count,
        avg_cpu_time_us,
        avg_duration_us,
        last_execution_time,
        query_text,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, first_execution_time
            ORDER BY collection_time DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   execution_type_desc = 'Regular'
    AND   last_execution_time >= $2
),
plan_agg AS
(
    SELECT
        database_name,
        query_id,
        plan_id,
        any_value(query_plan_hash) AS query_plan_hash,
        any_value(query_text) AS query_text,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count) / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count) / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        MAX(last_execution_time) AS last_exec
    FROM deduped
    WHERE rn = 1
    GROUP BY database_name, query_id, plan_id
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
        any_value(query_text) AS query_text,
        SUM(execs) AS execs,
        SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0) AS cpu_per_exec,
        SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0) AS dur_per_exec,
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
        l.query_text,
        GREATEST
        (
            l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0),
            l.dur_per_exec / NULLIF(b.dur_per_exec, 0)
        ) AS regression_factor
    FROM ranked AS l
    JOIN ranked AS b
      ON  b.database_name = l.database_name
      AND b.query_id = l.query_id
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
    LEFT(query_text, 500) AS query_text
FROM compared
WHERE regression_factor >= 2
ORDER BY regression_factor DESC
LIMIT 5";

        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart.AddDays(-14) });

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

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT database_name, query_hash,
       LEFT(MAX(query_text), 500) AS query_text,
       SUM(delta_execution_count)::BIGINT AS exec_count,
       CASE WHEN SUM(delta_execution_count) > 0
            THEN SUM(delta_worker_time)::DOUBLE / SUM(delta_execution_count) / 1000.0
            ELSE 0 END AS avg_cpu_ms,
       CASE WHEN SUM(delta_execution_count) > 0
            THEN SUM(delta_elapsed_time)::DOUBLE / SUM(delta_execution_count) / 1000.0
            ELSE 0 END AS avg_elapsed_ms,
       CASE WHEN SUM(delta_execution_count) > 0
            THEN SUM(delta_logical_reads)::DOUBLE / SUM(delta_execution_count)
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
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

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

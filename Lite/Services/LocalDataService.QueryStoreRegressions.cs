/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

/// <summary>One Query Store regression row — the DuckDB twin of Darling's
/// <c>DarlingQueryStoreRegressionReader.RegressionRow</c>. Durations and CPU are ms (converted from the
/// stored microseconds); reads are raw pages; the percents are plain deltas.</summary>
public sealed class QueryStoreRegressionRow
{
    public string DatabaseName { get; set; } = "";
    public long QueryId { get; set; }
    public double BaselineDurationMs { get; set; }
    public double RecentDurationMs { get; set; }
    public double DurationRegressionPercent { get; set; }
    public double BaselineCpuMs { get; set; }
    public double RecentCpuMs { get; set; }
    public double CpuRegressionPercent { get; set; }
    public double BaselineReads { get; set; }
    public double RecentReads { get; set; }
    public double IoRegressionPercent { get; set; }
    public double AdditionalDurationMs { get; set; }
    public long BaselineExecCount { get; set; }
    public long RecentExecCount { get; set; }
    public int BaselinePlanCount { get; set; }
    public int RecentPlanCount { get; set; }
    public string Severity { get; set; } = "";
    public string QueryTextSample { get; set; } = "";
    public DateTime? LastExecutionTime { get; set; }
}

/*
 * The Query Store regressions read (#2484) — Lite's port of Darling's, which is itself the viewer's port
 * of the Dashboard's report.query_store_regressions inline TVF. Two windowed passes over the SAME
 * v_query_store_stats view: BASELINE is every capture BEFORE the window start, RECENT is the window.
 *
 * Both arms are DEDUPED first, and that is correctness rather than performance. Query Store rows are
 * CUMULATIVE per-interval snapshots and the collector re-fetches an open interval every cycle, so the same
 * interval is stored repeatedly with a growing execution_count. This read is the most exposed of any to
 * that: the baseline arm is UNBOUNDED (potentially months) while the recent arm is a short window, so the
 * two arms have systematically different re-collection density per interval — which alone moves the
 * averages the regression percent is computed from and the 25% CPU gate, manufacturing and hiding
 * regressions for reasons that have nothing to do with the query.
 *
 * One deliberate difference from Darling's: the query-text sample comes only from MAX(query_text) on the
 * fact rows. Darling resolves it from collect.query_store_text (#2150) and falls back to the fact rows;
 * that table is a Darling-store construct with no Lite equivalent, so Lite has only the fallback. It is a
 * difference in where the SAME text comes from, not in which rows the read returns.
 */
public partial class LocalDataService
{
    /// <summary>
    /// The queries whose Query Store performance got WORSE over [now - <paramref name="hoursBack"/>, now]
    /// compared with everything collected before it, ranked by execution-count-weighted extra duration.
    /// </summary>
    public async Task<List<QueryStoreRegressionRow>> GetQueryStoreRegressionsAsync(
        int serverId, int hoursBack = 24, int maxRows = 50, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        var limitIndex = 4 + dbValues.Count;

        command.CommandText = @"
WITH deduped_baseline AS (
    /* LOAD-BEARING (correctness, not just perf) — #1841. Keep the LATEST cumulative snapshot per
       interval before aggregating; see the file header for why this read is the most exposed to it. */
    SELECT
        database_name,
        query_id,
        plan_id,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
            ORDER BY collection_time DESC, execution_count DESC
        ) AS rn
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   collection_time < $2" + dbClause + @"
),
deduped_recent AS (
    SELECT
        database_name,
        query_id,
        plan_id,
        query_text,
        execution_count,
        avg_duration_us,
        avg_cpu_time_us,
        avg_logical_io_reads,
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
        AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS DOUBLE PRECISION)) AS avg_logical_io_reads,
        CAST(SUM(execution_count) AS BIGINT) AS exec_count,
        CAST(COUNT(DISTINCT plan_id) AS INTEGER) AS plan_count
    FROM deduped_baseline
    WHERE rn = 1
    GROUP BY database_name, query_id
),
recent_performance AS (
    SELECT
        database_name,
        query_id,
        MAX(query_text) AS query_text_sample,
        AVG(CAST(avg_duration_us AS DOUBLE PRECISION)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS DOUBLE PRECISION)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS DOUBLE PRECISION)) AS avg_logical_io_reads,
        CAST(SUM(execution_count) AS BIGINT) AS exec_count,
        CAST(COUNT(DISTINCT plan_id) AS INTEGER) AS plan_count,
        MAX(last_execution_time) AS last_execution_time
    FROM deduped_recent
    WHERE rn = 1
    GROUP BY database_name, query_id
)
SELECT
    r.database_name,
    r.query_id,
    b.avg_duration_ms AS baseline_duration_ms,
    r.avg_duration_ms AS recent_duration_ms,
    (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) AS duration_regression_percent,
    b.avg_cpu_time_ms AS baseline_cpu_ms,
    r.avg_cpu_time_ms AS recent_cpu_ms,
    (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) AS cpu_regression_percent,
    b.avg_logical_io_reads AS baseline_reads,
    r.avg_logical_io_reads AS recent_reads,
    (r.avg_logical_io_reads - b.avg_logical_io_reads) * 100.0 / NULLIF(b.avg_logical_io_reads, 0) AS io_regression_percent,
    (r.avg_duration_ms - b.avg_duration_ms) * r.exec_count AS additional_duration_ms,
    b.exec_count AS baseline_exec_count,
    r.exec_count AS recent_exec_count,
    b.plan_count AS baseline_plan_count,
    r.plan_count AS recent_plan_count,
    CASE
        WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 100 THEN 'CRITICAL'
        WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 50 THEN 'HIGH'
        WHEN (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) > 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    r.query_text_sample,
    r.last_execution_time
FROM recent_performance AS r
JOIN baseline_performance AS b
  ON  b.database_name = r.database_name
  AND b.query_id = r.query_id
WHERE (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) > 25
ORDER BY additional_duration_ms DESC
LIMIT $" + limitIndex;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        command.Parameters.Add(new DuckDBParameter { Value = maxRows });

        var rows = new List<QueryStoreRegressionRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new QueryStoreRegressionRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                BaselineDurationMs = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                RecentDurationMs = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                DurationRegressionPercent = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                BaselineCpuMs = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                RecentCpuMs = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                CpuRegressionPercent = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                BaselineReads = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                RecentReads = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                IoRegressionPercent = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10)),
                AdditionalDurationMs = reader.IsDBNull(11) ? 0 : ToDouble(reader.GetValue(11)),
                BaselineExecCount = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12)),
                RecentExecCount = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13)),
                BaselinePlanCount = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14)),
                RecentPlanCount = reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15)),
                Severity = reader.IsDBNull(16) ? "" : reader.GetString(16),
                QueryTextSample = reader.IsDBNull(17) ? "" : reader.GetString(17),
                LastExecutionTime = reader.IsDBNull(18) ? null : reader.GetDateTime(18),
            });
        }

        return rows;
    }

    /// <summary>
    /// Whether this server has Query Store rows BEFORE the window, and whether it has any INSIDE it.
    /// <para>One round trip for the two facts that decide what an empty regression result means, run only
    /// on the empty path. Zero regressions is four states here, not two, and only one of them is good news.
    /// The dangerous one is a server whose entire collected history sits INSIDE the requested window: it
    /// has no BEFORE, so it can never show a regression however badly it regressed. Reads
    /// <c>v_query_store_stats</c>, the same view the read itself uses. Darling twin:
    /// <c>DarlingQueryStoreRegressionReader.RegressionCoverageSql</c>.</para>
    /// </summary>
    public async Task<(bool HasBaseline, bool HasRecent)> GetQueryStoreRegressionCoverageAsync(
        int serverId, int hoursBack = 24, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);

        command.CommandText = @"
SELECT
    EXISTS (
        SELECT 1
        FROM v_query_store_stats
        WHERE server_id = $1
        AND   collection_time < $2
    ) AS has_baseline,
    EXISTS (
        SELECT 1
        FROM v_query_store_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
    ) AS has_recent";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }
}

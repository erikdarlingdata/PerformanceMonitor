/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The service-side read behind get_query_store_regressions (#2484) - the viewer's
/// <c>ViewerDataService.QueryStoreRegressionsSql</c>, which is itself the Postgres port of the Dashboard's
/// <c>report.query_store_regressions</c> inline TVF. Copied VERBATIM apart from the row cap, which the
/// viewer hardcodes at the TVF's TOP (50) and this binds as a parameter so a caller can ask for fewer or
/// more; the gate and the ranking are untouched, so the first 50 rows of any call are the viewer's 50.
///
/// <para>A STORED read - no live monitored-server hit. Two windowed passes over the SAME
/// <c>query_store_stats</c> table: BASELINE is every capture BEFORE the window start, RECENT is the window
/// itself. Both are DEDUPED first, and that is correctness rather than performance: Query Store rows are
/// CUMULATIVE per-interval snapshots and the collector re-fetches an open interval every cycle, so the same
/// interval is stored repeatedly with a growing execution_count. This read is the most exposed of any to
/// that, because the baseline arm is UNBOUNDED (potentially months) while the recent arm is a short window:
/// the two arms have systematically different re-collection density per interval, which alone moves the
/// averages the regression percent is computed from and the 25% CPU gate - manufacturing and hiding
/// regressions for reasons that have nothing to do with the query.</para>
///
/// <para>The SQL is a public const so the tests can pin the dialect and the shape without a live Postgres.</para>
/// </summary>
internal static class DarlingQueryStoreRegressionReader
{
    /// <summary>One regression row - the viewer's <c>ViewerQueryStoreRegressionRow</c>, without the
    /// display-formatting members. Durations and CPU are ms (converted from the stored microseconds);
    /// reads are raw pages; the percents are plain deltas.</summary>
    public sealed record RegressionRow(
        string DatabaseName,
        long QueryId,
        double BaselineDurationMs,
        double RecentDurationMs,
        double DurationRegressionPercent,
        double BaselineCpuMs,
        double RecentCpuMs,
        double CpuRegressionPercent,
        double BaselineReads,
        double RecentReads,
        double IoRegressionPercent,
        double AdditionalDurationMs,
        long BaselineExecCount,
        long RecentExecCount,
        int BaselinePlanCount,
        int RecentPlanCount,
        string Severity,
        string QueryTextSample,
        DateTime? LastExecutionTime);

    /// <summary>
    /// The viewer's regression read. $1 server_id, $2 window start (the baseline is everything &lt; $2),
    /// $3 window end, $4 database filter (text[] or NULL), $5 row cap.
    /// </summary>
    public const string QueryStoreRegressionsSql = """
        WITH deduped_baseline AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
               snapshots and the collector re-fetches the OPEN interval every cycle, so the SAME interval
               (same first_execution_time) is stored repeatedly with a growing execution_count. Keep the
               LATEST snapshot per interval before aggregating. */
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
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time < $2
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
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
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        baseline_performance AS (
            SELECT
                database_name,
                query_id,
                AVG(CAST(avg_duration_us AS double precision)) / 1000.0 AS avg_duration_ms,
                AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0 AS avg_cpu_time_ms,
                AVG(CAST(avg_logical_io_reads AS double precision)) AS avg_logical_io_reads,
                CAST(SUM(execution_count) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count
            FROM deduped_baseline
            WHERE rn = 1
            GROUP BY database_name, query_id
        ),
        recent_performance AS (
            SELECT
                database_name,
                query_id,
                MAX(query_text) AS query_text_sample,
                AVG(CAST(avg_duration_us AS double precision)) / 1000.0 AS avg_duration_ms,
                AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0 AS avg_cpu_time_ms,
                AVG(CAST(avg_logical_io_reads AS double precision)) AS avg_logical_io_reads,
                CAST(SUM(execution_count) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count,
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
            /* #2150: text comes from collect.query_store_text now, and this projection's grain is exactly
               that table's key, so it resolves here with a keyed join rather than inside the aggregate.
               The MAX(query_text) sample below it stays as the fallback: it is where text lived before the
               cutover, and it is what keeps the regression rows readable for existing history. */
            COALESCE(x.query_sql_text, r.query_text_sample) AS query_text_sample,
            r.last_execution_time
        FROM recent_performance AS r
        JOIN baseline_performance AS b
          ON  b.database_name = r.database_name
          AND b.query_id = r.query_id
        LEFT JOIN query_store_text AS x
          ON  x.server_id = $1
          AND x.database_name = r.database_name
          AND x.query_id = r.query_id
        WHERE (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) > 25
        ORDER BY additional_duration_ms DESC
        LIMIT $5
        """;

    /// <summary>
    /// Whether this server has Query Store rows BEFORE the window, and whether it has any INSIDE it.
    /// <para>One round trip for the two facts that decide what an empty result means, and it is run only
    /// on the empty path. Zero regressions is four different states here, not two. No baseline and no
    /// recent rows means nothing was ever collected. A baseline with an empty window means collection may
    /// have stopped. And - the one this read has that its siblings do not - RECENT rows with no baseline
    /// means there is no BEFORE to compare against: a regression needs one, and a server whose entire
    /// collected history sits inside the requested window can never show a regression however bad it got.
    /// Reporting that as "no regressions" is the failure this exists to prevent.</para>
    /// <para>Probes the base <c>query_store_stats</c> table, the same source the read itself uses.
    /// $1 server_id, $2 window start, $3 window end.</para>
    /// </summary>
    public const string RegressionCoverageSql = """
        SELECT
            EXISTS (
                SELECT 1
                FROM query_store_stats
                WHERE server_id = $1
                AND   collection_time < $2
            ) AS has_baseline,
            EXISTS (
                SELECT 1
                FROM query_store_stats
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
            ) AS has_recent
        """;

    /// <summary>Runs <see cref="QueryStoreRegressionsSql"/>.</summary>
    public static async Task<List<RegressionRow>> GetQueryStoreRegressionsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        string? databaseName, int limit, CancellationToken cancellationToken = default)
    {
        var rows = new List<RegressionRow>();
        await using var command = postgres.CreateCommand(QueryStoreRegressionsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = string.IsNullOrWhiteSpace(databaseName) ? DBNull.Value : new[] { databaseName },
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new RegressionRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                reader.IsDBNull(7) ? 0 : Convert.ToDouble(reader.GetValue(7)),
                reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
                reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10)),
                reader.IsDBNull(11) ? 0 : Convert.ToDouble(reader.GetValue(11)),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                reader.IsDBNull(15) ? 0 : reader.GetInt32(15),
                reader.IsDBNull(16) ? "" : reader.GetString(16),
                reader.IsDBNull(17) ? "" : reader.GetString(17),
                reader.IsDBNull(18) ? null : reader.GetDateTime(18)));
        }

        return rows;
    }

    /// <summary>Runs <see cref="RegressionCoverageSql"/>.</summary>
    public static async Task<(bool HasBaseline, bool HasRecent)> GetCoverageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(RegressionCoverageSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }
}

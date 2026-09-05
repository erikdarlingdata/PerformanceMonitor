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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One Query-Store regression row — the viewer copy of the Dashboard's
/// <c>QueryStoreRegressionItem</c> (Dashboard/Models/QueryStoreRegressionItem.cs), fed by the Postgres
/// port of the Dashboard's <c>report.query_store_regressions</c> inline TVF
/// (install/47_create_reporting_views.sql). A (database, query_id) group's RECENT-window averages vs. its
/// BASELINE (everything captured before the window), with the per-metric regression percents, the
/// execution-count-weighted extra duration, the plan-count deltas, a duration-driven severity band, and
/// the latest captured query-text sample.
///
/// <para>Metric values are ms (duration/CPU, converted from µs in SQL) and raw pages (reads); the percents
/// are plain deltas the grid formats to N2%. <see cref="LastExecutionTime"/> is the Query Store row's
/// last-exec column (the SQL server's local wall clock in Darling's store), shown RAW via
/// <see cref="ViewerDataService.FormatServerClock"/> exactly like the sibling Query Store tab — NOT run
/// through the naive-UTC→local conversion the collection_time columns get. The regression read carries no
/// plan XML (the Dashboard TVF selects none either), so — like the sibling Query Store tab (View-Plan
/// deferred) — there is no plan surface; double-clicking a row opens that query's history window instead
/// (mirroring the Dashboard grid's double-click).</para>
/// </summary>
public sealed class ViewerQueryStoreRegressionRow
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

    /// <summary>Regression rows aggregate across plans, so there is no single stored plan to view (View Stored Plan hidden).</summary>
    public bool HasStoredPlan => false;

    /// <summary>Gates "Get Actual Plan (re-run)" — the service re-executes by QueryId (BuildActualPlanArgsForQueryStore).</summary>
    public bool CanGetActualPlan => QueryId != 0;
    public DateTime? LastExecutionTime { get; set; }

    /// <summary>The last-execution wall clock shown raw (the sibling Query Store tab's convention).</summary>
    public string LastExecutionTimeLocal => ViewerDataService.FormatServerClock(LastExecutionTime);
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Query-Store regressions read — the Dashboard's <c>report.query_store_regressions(@start,@end)</c>
    /// inline TVF ported to Postgres against Darling's <c>query_store_stats</c> store. Two windowed CTEs over
    /// the SAME base table: BASELINE = every capture BEFORE the window start; RECENT = the [start, end]
    /// window. Each averages the per-interval µs metrics to ms (reads stay raw pages), SUMs execution_count,
    /// and COUNTs DISTINCT plan_id; the outer projection joins them per (database, query_id), computes the
    /// duration / CPU / IO regression percents, the execution-count-weighted <c>additional_duration_ms</c>
    /// (extra total time = per-exec delta × recent exec count), and a DURATION-driven severity band, keeping
    /// only rows whose CPU regressed > 25% (the TVF's single-metric gate), ranked by additional_duration_ms
    /// and capped at the TVF's TOP (50).
    ///
    /// <para>Faithful adaptations to Darling's store (all reported, none silently changed):
    /// (1) The Dashboard TVF windows on <c>server_last_execution_time</c>; Darling windows the baseline/recent
    /// split on <c>collection_time</c> (naive UTC) — the column EVERY other Darling Query Store read windows
    /// on, and the frame the toolbar's [start,end] bounds are in (Darling's <c>last_execution_time</c> is the
    /// server's LOCAL wall clock, so windowing on it against UTC bounds would be a timezone bug).
    /// (2) The recent query-text sample comes from <c>collect.query_store_text</c> (#2150), falling back to
    /// <c>MAX(query_text)</c> on the fact rows for history collected before that table existed — either way
    /// Darling's text is already decompressed, where the TVF <c>DECOMPRESS()</c>es a compressed
    /// <c>query_sql_text</c>.
    /// (3) The stale INTENT comment on the Dashboard's C# caller (bounded/mirrored baseline, weighted averages,
    /// multi-metric, absolute minimums) does NOT match what the TVF actually runs — this ports the ACTUAL TVF
    /// (unbounded baseline, plain AVG, CPU-only > 25% gate, no minimums), which is what the Dashboard grid shows.</para>
    ///
    /// <para>$1 server_id, $2 window start (baseline is &lt; $2), $3 window end, $4 database filter (text[]).</para>
    /// </summary>
    public const string QueryStoreRegressionsSql = """
        WITH deduped_baseline AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
               snapshots and the collector re-fetches the OPEN interval every cycle, so the SAME interval
               (same first_execution_time) is stored repeatedly with a growing execution_count. Keep the
               LATEST snapshot per interval before aggregating.

               This read is especially exposed: the baseline arm is UNBOUNDED (collection_time < $2,
               potentially months) while the recent arm is a short window, so the two arms have
               systematically different re-collection density per interval. Un-deduped, that alone moves
               the AVG(avg_*) values the regression percent is computed from, and the > 25% CPU gate —
               manufacturing and hiding regressions in a direction that has nothing to do with the query.
               It also multiplies exec_count, which feeds additional_duration_ms, the sort key. */
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
               cutover, and it is what keeps the regression grid readable for existing history. */
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
        LIMIT 50
        """;

    /// <summary>
    /// The Query Store regressions for one server over the RECENT window [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>] vs. the baseline before it, ranked by execution-count-weighted extra
    /// duration descending (the TVF's ORDER BY; the grid then default-sorts by duration % like the Dashboard).
    /// </summary>
    public async Task<List<ViewerQueryStoreRegressionRow>> GetQueryStoreRegressionsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerQueryStoreRegressionRow>();

        await using var command = _dataSource.CreateCommand(QueryStoreRegressionsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerQueryStoreRegressionRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                BaselineDurationMs = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                RecentDurationMs = reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                DurationRegressionPercent = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                BaselineCpuMs = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                RecentCpuMs = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                CpuRegressionPercent = reader.IsDBNull(7) ? 0 : Convert.ToDouble(reader.GetValue(7)),
                BaselineReads = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                RecentReads = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
                IoRegressionPercent = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10)),
                AdditionalDurationMs = reader.IsDBNull(11) ? 0 : Convert.ToDouble(reader.GetValue(11)),
                BaselineExecCount = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                RecentExecCount = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                BaselinePlanCount = reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                RecentPlanCount = reader.IsDBNull(15) ? 0 : reader.GetInt32(15),
                Severity = reader.IsDBNull(16) ? "" : reader.GetString(16),
                QueryTextSample = reader.IsDBNull(17) ? "" : reader.GetString(17),
                LastExecutionTime = reader.IsDBNull(18) ? null : reader.GetDateTime(18),
            });
        }

        return rows;
    }
}

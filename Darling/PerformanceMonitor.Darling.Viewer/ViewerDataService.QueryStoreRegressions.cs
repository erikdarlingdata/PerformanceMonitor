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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

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
/// last-exec column, which is naive UTC in Darling's store — Query Store returns
/// <c>datetimeoffset</c> and <c>QueryStoreCollector</c> normalises it through
/// <c>((DateTimeOffset)…).UtcDateTime</c> before storing — so it converts through
/// <see cref="ViewerTimeHelper.ForDisplay"/>, the same conversion the collection_time columns get, and
/// like the sibling Query Store tab. The <c>sys.dm_exec_*</c> stamps that go through
/// <see cref="ViewerDataService.FormatServerClock"/> are a different frame, not a different tab: the
/// renderer follows the column, never the neighbouring grid. The regression read carries no
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

    /// <summary>The last-execution stamp, naive UTC in the store, converted to the display mode.</summary>
    public string LastExecutionTimeLocal => ViewerDataService.FormatStoredUtc(LastExecutionTime);
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The baseline's lookback (#4217, the same bound #4195 gave the MCP tool and Lite's copy of this same
    /// read): a fixed 7 days ending at the recent window's start, so the comparison period stops growing with
    /// retention. This file cannot reach <c>DarlingQueryStoreRegressionReader.BaselineLookbackDays</c> (the MCP
    /// reader lives in a project the viewer does not reference) or Lite's own copy (a separate app), so it
    /// keeps its own constant of the same value, same as those two already keep independent copies of each
    /// other.
    /// </summary>
    public const int BaselineLookbackDays = 7;

    /// <summary>
    /// The statement text of both forms (#5506): the arms (<see cref="QueryStoreRegressionBaseline.RawArmsSql"/> or
    /// <see cref="QueryStoreRegressionBaseline.WideArmsSql"/>), the aggregates, and <see cref="QueryStoreRegressionsSuffix"/>,
    /// bound to this read's parameters: $1 server_id, $2 window start (baseline is &lt; $2), $3 window end, $4 database filter
    /// (text[]), $5 baseline start (window start minus <see cref="BaselineLookbackDays"/>), $6 the baseline days read from the
    /// daily totals (date[]), $7 and $8 the live half's <c>first_execution_time</c> bounds.
    /// </summary>
    internal static string BuildQueryStoreRegressionsSql(bool wide, bool useDailyTotals) =>
        QueryStoreRegressionBaseline.Bind(
            (wide ? QueryStoreRegressionBaseline.WideArmsSql : QueryStoreRegressionBaseline.RawArmsSql)
            + "\n" + QueryStoreRegressionBaseline.AggregatesSql(useDailyTotals) + "\n" + QueryStoreRegressionsSuffix,
            "$5", "$6", "$7", "$8");

    /// <summary>
    /// The Query-Store regressions read — the Dashboard's <c>report.query_store_regressions(@start,@end)</c> inline TVF
    /// ported to Postgres against Darling's Query Store tables, and twin of the MCP tool's read
    /// (<c>DarlingQueryStoreRegressionReader</c>; the SQL is shared through <see cref="QueryStoreRegressionBaseline"/>).
    /// BASELINE = the fixed <see cref="BaselineLookbackDays"/>-day window ending at the window start; RECENT = the
    /// [start, end] window. Both sides are WEIGHTED per-execution averages in ms, the summed cost divided by the summed
    /// executions (#5506; before it the baseline was an average of the interval averages), over Regular executions, and the
    /// baseline's closed days come from <c>collect.plan_regression_daily</c> (#5448) with the days at its edges read live
    /// (<see cref="QueryStoreRegressionBaseline"/> shows that no interval is counted twice or dropped at the seams). The outer
    /// projection joins them per (database, query_id), computes the duration and CPU regression percents, the
    /// execution-count-weighted <c>additional_duration_ms</c>
    /// (extra total time = per-exec delta × recent exec count), and a DURATION-driven severity band, keeping
    /// only rows whose CPU regressed > 25% (the TVF's single-metric gate), ranked by additional_duration_ms
    /// and capped at the TVF's TOP (50). Logical reads are not part of it (#5506).
    ///
    /// <para>Faithful adaptations to Darling's store (all reported, none silently changed):
    /// (1) The Dashboard TVF windows on <c>server_last_execution_time</c>; Darling splits the baseline from the recent window
    /// on the interval's <c>last_execution_time</c> (naive UTC; the day of it is what the daily totals row by) and bounds the
    /// rows read by <c>collection_time</c>, the frame the toolbar's [start,end] bounds are in.
    /// (2) The recent query-text sample comes from <c>collect.query_store_text</c> (#2150), falling back to
    /// <c>MAX(query_text)</c> on the fact rows for history collected before that table existed — either way
    /// Darling's text is already decompressed, where the TVF <c>DECOMPRESS()</c>es a compressed
    /// <c>query_sql_text</c>.
    /// (3) The stale INTENT comment on the Dashboard's C# caller does NOT match what the TVF actually runs; this ports the
    /// ACTUAL gate (CPU-only > 25%, no minimums), with the baseline bounded per #4217.</para>
    ///
    /// <para>Since #5809 this raw form is the fallback: <see cref="QueryStoreRegressionsTableSql"/> serves wherever the
    /// interval table's gate allows.</para>
    /// </summary>
    public static readonly string QueryStoreRegressionsSql = BuildQueryStoreRegressionsSql(wide: false, useDailyTotals: true);

    private static readonly string s_rawLiveSql = BuildQueryStoreRegressionsSql(wide: false, useDailyTotals: false);

    private static readonly string s_wideLiveSql = BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: false);

    /// <summary>The projection, the gate and the ranking, shared by the raw and the table form so they cannot drift. Both
    /// sides are weighted per-execution averages (#5506); no logical reads.</summary>
    private const string QueryStoreRegressionsSuffix = """
        SELECT
            r.database_name,
            r.query_id,
            b.avg_duration_ms AS baseline_duration_ms,
            r.avg_duration_ms AS recent_duration_ms,
            (r.avg_duration_ms - b.avg_duration_ms) * 100.0 / NULLIF(b.avg_duration_ms, 0) AS duration_regression_percent,
            b.avg_cpu_time_ms AS baseline_cpu_ms,
            r.avg_cpu_time_ms AS recent_cpu_ms,
            (r.avg_cpu_time_ms - b.avg_cpu_time_ms) * 100.0 / NULLIF(b.avg_cpu_time_ms, 0) AS cpu_regression_percent,
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
               The MAX(query_text) sample stays as the fallback: it is where text lived before the
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
    /// The regression read over <c>query_store_interval_wide</c> (#5809), with the baseline's closed days from the daily
    /// totals (#5506); the same statement the MCP read runs (<c>DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql</c>)
    /// with this read's parameter numbers. The table holds ONE row per interval (its latest snapshot), so no dedupe runs; $5 is
    /// the gate's clamp (<see cref="QueryStoreIntervalWide.ClampedStart"/>). An interval is on one side of the window start
    /// only (<see cref="QueryStoreRegressionBaseline"/>).
    /// </summary>
    public static readonly string QueryStoreRegressionsTableSql = BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: true);

    /// <summary>
    /// The start of the baseline window of the regressions read whose recent window starts at <paramref name="startUtc"/>:
    /// <see cref="BaselineLookbackDays"/> days earlier. The read's baseline arm and the data-start probe take it from here, so
    /// the notice keys on the same earlier window the comparison reads.
    /// </summary>
    public static DateTime QueryStoreRegressionsBaselineStart(DateTime startUtc) => startUtc.AddDays(-BaselineLookbackDays);

    /// <summary>
    /// Where this server's query_store_stats coverage starts for the window the regressions read compares (#4966), through the
    /// shared probe (<see cref="DataWindowFloor"/>). The read compares a RECENT window with the baseline of the
    /// <see cref="BaselineLookbackDays"/> days before it, so the probe asks about the EARLIER window: it starts at
    /// <see cref="QueryStoreRegressionsBaselineStart"/>, not at <paramref name="startUtc"/>. A server added two days ago has a
    /// recent window it covers whole and a baseline of one day instead of seven; a probe over the recent window alone would say
    /// nothing. The read reads the raw table in both arms (no rollup route), so the probe is the raw table's. The grid ranks its top
    /// 50 by added duration, not by time, so the rows say nothing about how far back the comparison reached and the cap rule does
    /// not apply. Null when the window holds no row and no logged run, and when it lies wholly before the coverage.
    /// </summary>
    public Task<DateTime?> GetQueryStoreRegressionsDataStartAsync(int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DataWindowFloor.GetForServerAsync(_dataSource, DataWindowFloor.Source.ForCollectorTable("query_store_stats"), serverId,
            QueryStoreRegressionsBaselineStart(startUtc), endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>
    /// The Query Store regressions for one server over the RECENT window [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>] vs. the fixed <see cref="BaselineLookbackDays"/>-day baseline ending at
    /// <paramref name="startUtc"/> (#4217), ranked by execution-count-weighted extra duration descending (the
    /// TVF's ORDER BY; the grid then default-sorts by duration % like the Dashboard).
    /// <paramref name="literalEndUtc"/> is the end of a custom range (null for a preset), the interval table gate's clause 4
    /// exactly as the Queries grid passes it (#3953, #5809).
    /// </summary>
    public async Task<List<ViewerQueryStoreRegressionRow>> GetQueryStoreRegressionsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, DateTime? literalEndUtc = null,
        CancellationToken cancellationToken = default)
    {

        // #4217: the baseline's lower bound, computed here (not as a SQL interval) so the bound stays a plain
        // timestamp parameter — the same reason DarlingQueryStoreRegressionReader takes it as a parameter
        // rather than computing it inside the statement (an in-statement interval costs TimescaleDB its
        // plan-time chunk exclusion).
        var baselineStartUtc = QueryStoreRegressionsBaselineStart(startUtc);

        /* #5809: the interval table where the Queries grid's own gate allows, raw otherwise. */
        var schemaVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        if (schemaVersion is int version && version >= QueryStoreIntervalWideMinSchemaVersion)
        {
            var tableRows = await TryGetQueryStoreRegressionsFromTableAsync(
                serverId, startUtc, endUtc, literalEndUtc, databaseNames, baselineStartUtc, cancellationToken);
            if (tableRows is not null)
            {
                return tableRows;
            }
        }

        /* #5506: raw too reads the baseline's closed days from the daily totals, so it runs in the same kind of snapshot
           transaction as the table form: the built days it names and the live rows it reads are one view of the store. */
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        await using (var readOnly = new NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds })
        {
            await readOnly.ExecuteNonQueryAsync(cancellationToken);
        }

        var plan = await QueryStoreRegressionBaseline.ResolveAsync(
            connection, transaction, serverId, baselineStartUtc, startUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, null, cancellationToken);
        await using var command = new NpgsqlCommand(plan.BuiltDayCount > 0 ? QueryStoreRegressionsSql : s_rawLiveSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
        AddQueryStoreRegressionsParameters(command, serverId, startUtc, endUtc, databaseNames, baselineStartUtc, plan);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await ReadQueryStoreRegressionRowsAsync(reader, cancellationToken);
    }

    /// <summary>
    /// #5809: <see cref="QueryStoreRegressionsTableSql"/> behind the Queries grid's gate
    /// (<see cref="QueryStoreIntervalWide.ResolveReadAsync"/> over baseline start to window end, so the #5541 floor rule
    /// applies), the decision and the read in ONE read-only REPEATABLE READ transaction, as
    /// <c>TryGetQueryStoreTopQueriesFromTableAsync</c> does. Null when the gate says raw or anything here faults
    /// (cancellation propagates); an empty list is the table's real answer. The read binds the gate's
    /// <see cref="QueryStoreIntervalWide.WideReadPlan.ClampedStart"/>, so it reads exactly the span raw would, which is
    /// the span <see cref="GetQueryStoreRegressionsDataStartAsync"/>'s notice describes.
    /// </summary>
    internal async Task<List<ViewerQueryStoreRegressionRow>?> TryGetQueryStoreRegressionsFromTableAsync(
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? literalEndUtc, IReadOnlyList<string>? databaseNames, DateTime baselineStartUtc, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
            await using (var readOnly = new NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, baselineStartUtc, endUtc, literalEndUtc, QueryStoreIntervalWide.GridWideMinWindow,
                ViewerCommandDeadlines.CurrentInteractiveReadSeconds, logger: null, cancellationToken);
            if (!plan.UseTable)
            {
                return null;
            }

            var baselinePlan = await QueryStoreRegressionBaseline.ResolveAsync(
                connection, transaction, serverId, plan.ClampedStart, startUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, null, cancellationToken);
            await using var command = new NpgsqlCommand(baselinePlan.BuiltDayCount > 0 ? QueryStoreRegressionsTableSql : s_wideLiveSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
            AddQueryStoreRegressionsParameters(command, serverId, startUtc, endUtc, databaseNames, plan.ClampedStart, baselinePlan);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await ReadQueryStoreRegressionRowsAsync(reader, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            System.Diagnostics.Trace.TraceWarning($"#5809 regressions table read fell back to raw: {ex.GetType().Name}: {ex.Message}");
            return null;
        }
    }

    /// <summary>The eight parameters both forms of the read take: $1 server_id, $2 window start, $3 window end, $4 database
    /// filter, $5 baseline start, $6 the baseline days read from the daily totals (#5506), $7 the end of the live half's low
    /// <c>first_execution_time</c> range, $8 the start of its high range.</summary>
    private static void AddQueryStoreRegressionsParameters(
        NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, DateTime baselineStartUtc,
        QueryStoreRegressionBaseline.Plan plan)
    {
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.LowEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.HighStart, DateTimeKind.Unspecified) });
    }

    /// <summary>Reads the rows of either form of the read: both project the same <see cref="QueryStoreRegressionsSuffix"/>.</summary>
    private static async Task<List<ViewerQueryStoreRegressionRow>> ReadQueryStoreRegressionRowsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
    {
        var rows = new List<ViewerQueryStoreRegressionRow>();
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
                AdditionalDurationMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                BaselineExecCount = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                RecentExecCount = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                BaselinePlanCount = reader.IsDBNull(11) ? 0 : reader.GetInt32(11),
                RecentPlanCount = reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                Severity = reader.IsDBNull(13) ? "" : reader.GetString(13),
                QueryTextSample = reader.IsDBNull(14) ? "" : reader.GetString(14),
                LastExecutionTime = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
            });
        }

        return rows;
    }
}

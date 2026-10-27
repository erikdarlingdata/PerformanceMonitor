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

/// <summary>The regressions rows with the databases the read left out (#6654 review): the dropped ones, named for the grid's banner.
/// <see cref="DatabasesLeftOut"/> is null when none were left out.
/// <para>#6750: <see cref="Source"/> is the tier that answered (<c>interval_table</c> or <c>raw</c>). <see cref="CostBaselineStart"/> and
/// <see cref="FailureBaselineStart"/> are set only when the table read reached below raw's floor: the cost baseline (latest) and the
/// timeouts and errors baseline (the wide table) then start where their own table proves complete history, and the banner names the
/// cost start instead of raw's data start.</para></summary>
public sealed record ViewerQueryStoreRegressionsRead(
    List<ViewerQueryStoreRegressionRow> Rows, IReadOnlyList<string>? DatabasesLeftOut,
    string Source = "raw", DateTime? CostBaselineStart = null, DateTime? FailureBaselineStart = null);

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

    /// <summary>#5506: why the row is listed: <c>cost</c> (Regular CPU up more than 25%, with at least 1 s of added CPU, #6781), <c>timeouts</c> (Aborted runs per hour
    /// at least doubled), <c>errors</c> (Exception runs per hour at least doubled), or a comma-separated combination.</summary>
    public string RegressionReason { get; set; } = "";

    /// <summary>#5506: Aborted runs (timeouts, cancels): count, CPU and duration per execution, baseline and recent. Null
    /// figures read as an empty cell, the count as 0.</summary>
    public long BaselineAbortedCount { get; set; }
    public long RecentAbortedCount { get; set; }
    public double? BaselineAbortedCpuMs { get; set; }
    public double? RecentAbortedCpuMs { get; set; }
    public double? BaselineAbortedDurationMs { get; set; }
    public double? RecentAbortedDurationMs { get; set; }

    /// <summary>#5506: Exception runs (errors), the same six figures.</summary>
    public long BaselineExceptionCount { get; set; }
    public long RecentExceptionCount { get; set; }
    public double? BaselineExceptionCpuMs { get; set; }
    public double? RecentExceptionCpuMs { get; set; }
    public double? BaselineExceptionDurationMs { get; set; }
    public double? RecentExceptionDurationMs { get; set; }

    /// <summary>#5506: runs per hour for each group, so the 7-day baseline and the recent window compare in one unit.</summary>
    public double? BaselineRegularPerHour { get; set; }
    public double? RecentRegularPerHour { get; set; }
    public double? BaselineAbortedPerHour { get; set; }
    public double? RecentAbortedPerHour { get; set; }
    public double? BaselineExceptionPerHour { get; set; }
    public double? RecentExceptionPerHour { get; set; }

    /// <summary>The last-execution stamp, naive UTC in the store, converted to the display mode.</summary>
    public string LastExecutionTimeLocal => ViewerDataService.FormatStoredUtc(LastExecutionTime);
}

public sealed partial class ViewerDataService
{
    /// <summary>#6750 part 3: <paramref name="sql"/> with the self-statement test on the key table once its backfill has run
    /// (<see cref="QueryStoreSelfKeys.ReadyAsync"/>, read in this read's snapshot, closed on a store without the state table), else as it
    /// is, with the text probe. The MCP read's <c>DarlingQueryStoreRegressionReader.WithSelfKeysWhenReadyAsync</c> is the same shape.</summary>
    private static async Task<string> WithSelfKeysWhenReadyAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken cancellationToken) =>
        await QueryStoreSelfKeys.ReadyAsync(connection, transaction, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken)
            ? QueryStoreRegressionBaseline.WithSelfKeys(sql)
            : sql;

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
    /// <see cref="QueryStoreRegressionBaseline.WideArmsSql"/>), the aggregates, and <see cref="QueryStoreRegressionBaseline.SuffixSql"/>,
    /// bound to this read's parameters: $1 server_id, $2 window start (baseline is &lt; $2), $3 window end, $4 database filter
    /// (text[]), $5 baseline start (window start minus <see cref="BaselineLookbackDays"/>), $6 the baseline days read from the
    /// daily totals (date[]), $7 and $8 the live half's <c>first_execution_time</c> bounds, $9 the databases left out (#6654, text[] or NULL), $10 the start of the hours held (#6750, used only by the <c>windows</c> CTE).
    /// </summary>
    /// <para><paramref name="recentFromDaily"/> (latest only, #5506): the recent window's whole days from the daily totals,
    /// $13 the days (date[]), $14 and $15 the recent arm's live <c>first_execution_time</c> bounds (#6750 put the failure arm's row-filter
    /// start at $11 and its hours-held start at $12).</para>
    internal static string BuildQueryStoreRegressionsSql(bool wide, bool useDailyTotals, bool regularFromLatest = false, bool recentFromDaily = false) =>
        QueryStoreRegressionBaseline.Bind(
            QueryStoreRegressionBaseline.Statement(wide, useDailyTotals, regularFromLatest, recentFromDaily),
            "$5", "$6", "$7", "$8", "50", "$9", "$10", "$11", "$12", "$13", "$14", "$15");

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
    /// rows whose Regular CPU regressed > 25% with at least 1 s of added CPU over the window (the TVF's single-metric gate plus the #6781 floor) or whose Aborted or Exception runs per hour at
    /// least doubled (#5506), ranked by additional_duration_ms (failure-only rows after the cost rows, within a share of the
    /// cap) and capped at the TVF's TOP (50). Logical reads are not part of it (#5506).
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
    /// ACTUAL gate (CPU-only > 25%, no run-count minimum), plus the 1 s added-CPU floor Darling adds (#6781), with the baseline bounded per #4217.</para>
    ///
    /// <para>Since #5809 this raw form is the fallback: <see cref="QueryStoreRegressionsTableSql"/> serves wherever the
    /// interval table's gate allows.</para>
    /// </summary>
    public static readonly string QueryStoreRegressionsSql = BuildQueryStoreRegressionsSql(wide: false, useDailyTotals: true);

    private static readonly string s_rawLiveSql = BuildQueryStoreRegressionsSql(wide: false, useDailyTotals: false);

    private static readonly string s_wideLiveSql = BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: false);

    /* #5506: the table forms with the Regular arms on query_store_interval_latest, where the baseline plan's gate allows. */
    private static readonly string s_latestDailySql = BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: true, regularFromLatest: true);

    private static readonly string s_latestLiveSql = BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: false, regularFromLatest: true);

    private static readonly string s_recentDailySql = BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: true, regularFromLatest: true, recentFromDaily: true);

    private static readonly string s_recentLiveSql = BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: false, regularFromLatest: true, recentFromDaily: true);

    /// <summary>The table form for a resolved baseline plan, as the MCP read's <c>TableSqlFor</c> picks it (#5506).</summary>
    internal static string QueryStoreRegressionsTableSqlFor(QueryStoreRegressionBaseline.Plan plan) =>
        QueryStoreRegressionBaseline.ReadsRecentDays(plan)
            ? (plan.BuiltDayCount > 0 ? s_recentDailySql : s_recentLiveSql)
            : plan.RegularFromLatest
                ? (plan.BuiltDayCount > 0 ? s_latestDailySql : s_latestLiveSql)
                : (plan.BuiltDayCount > 0 ? QueryStoreRegressionsTableSql : s_wideLiveSql);

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
        CancellationToken cancellationToken = default) =>
        (await GetQueryStoreRegressionsWithScopeAsync(serverId, startUtc, endUtc, databaseNames, literalEndUtc, cancellationToken: cancellationToken)).Rows;

    /// <summary>
    /// <see cref="GetQueryStoreRegressionsAsync"/> with the databases it left out (#6654 review): the grid has no envelope, so the
    /// names come back beside the rows for the banner (<c>ViewerServerTab.DatabasesLeftOutBannerText</c>). Null when none were left
    /// out, as <see cref="DatabasesLeftOut.Distinct"/> answers.
    /// <para><paramref name="dataStartProbe"/> (#6750) is the tab's own <see cref="GetQueryStoreRegressionsDataStartAsync"/> task, which it
    /// started beside this read and shows as the "Showing since" banner: the read divides each per-hour rate by the hours from the later of
    /// its answer and the plan's start, so the grid and its banner name one instant. A caller with none gets the same probe here. A probe
    /// that throws leaves the plan's start (the banner step logs it).</para>
    /// </summary>
    public async Task<ViewerQueryStoreRegressionsRead> GetQueryStoreRegressionsWithScopeAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, DateTime? literalEndUtc = null,
        Task<DateTime?>? dataStartProbe = null, CancellationToken cancellationToken = default)
    {
        // #4217: the baseline's lower bound, computed here (not as a SQL interval) so the bound stays a plain
        // timestamp parameter — the same reason DarlingQueryStoreRegressionReader takes it as a parameter
        // rather than computing it inside the statement (an in-statement interval costs TimescaleDB its
        // plan-time chunk exclusion).
        var baselineStartUtc = QueryStoreRegressionsBaselineStart(startUtc);

        DateTime? dataStartUtc = null;
        try
        {
            dataStartUtc = await (dataStartProbe ?? GetQueryStoreRegressionsDataStartAsync(serverId, startUtc, endUtc, cancellationToken));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            System.Diagnostics.Trace.TraceWarning($"#6750 regressions data-start probe failed, so the rates divide by the plan's start: {ex.GetType().Name}: {ex.Message}");
        }

        /* #6654: with no database chosen the grid leaves out the databases the server DROPPED (named by an earlier database_states
           snapshot, absent from the newest), as get_query_store_regressions and the top-procedures ranking do (#5633 item 4): their
           queries would otherwise fill the 50-row cap. A database the user picked is read as asked; an unknown set leaves out nothing. */
        var excludedDatabases = databaseNames is { Count: > 0 }
            ? null
            : (await CurrentDatabaseScope.NarrowAsync(_dataSource, serverId, DatabaseFilter.All, windowStartUtc: baselineStartUtc, asOfUtc: literalEndUtc, logger: ViewerStoreLogger.Instance, cancellationToken: cancellationToken)).ExcludedNames;

        /* #5809: the interval table where the Queries grid's own gate allows, raw otherwise. */
        var schemaVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        if (schemaVersion is int version && version >= QueryStoreIntervalWideMinSchemaVersion)
        {
            var table = await TryReadFromTableAsync(
                serverId, startUtc, endUtc, literalEndUtc, databaseNames, baselineStartUtc, excludedDatabases, dataStartUtc, cancellationToken);
            if (table is { } t)
            {
                return new ViewerQueryStoreRegressionsRead(t.Rows, DatabasesLeftOut.Distinct(excludedDatabases), "interval_table",
                    t.BelowRawFloor ? t.CostStart : null, t.BelowRawFloor ? t.FailureStart : null);
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
            connection, transaction, serverId, baselineStartUtc, startUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, null, cancellationToken,
            clampToRawFloor: true);
        plan = QueryStoreRegressionBaseline.WithHoursHeldStart(plan, baselineStartUtc, dataStartUtc);
        /* #6750 part 3: the self-statement test on the key table once its backfill has run, read in this snapshot. */
        var rawSql = await WithSelfKeysWhenReadyAsync(connection, transaction, plan.BuiltDayCount > 0 ? QueryStoreRegressionsSql : s_rawLiveSql, cancellationToken);

        await using var command = new NpgsqlCommand(rawSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
        AddQueryStoreRegressionsParameters(command, serverId, startUtc, endUtc, databaseNames, baselineStartUtc, plan, excludedDatabases);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return new ViewerQueryStoreRegressionsRead(await ReadQueryStoreRegressionRowsAsync(reader, cancellationToken), DatabasesLeftOut.Distinct(excludedDatabases));
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
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? literalEndUtc, IReadOnlyList<string>? databaseNames, DateTime baselineStartUtc, IReadOnlyList<string>? excludedDatabases, CancellationToken cancellationToken, DateTime? dataStartUtc = null)
        => (await TryReadFromTableAsync(serverId, startUtc, endUtc, literalEndUtc, databaseNames, baselineStartUtc, excludedDatabases, dataStartUtc, cancellationToken))?.Rows;

    /// <summary>The table read with where each arm started (#6750), the same binding as the MCP read
    /// (<see cref="QueryStoreRegressionBaseline.ResolveSpansAsync"/>).</summary>
    private async Task<(List<ViewerQueryStoreRegressionRow> Rows, DateTime CostStart, DateTime FailureStart, bool BelowRawFloor)?> TryReadFromTableAsync(
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? literalEndUtc, IReadOnlyList<string>? databaseNames, DateTime baselineStartUtc, IReadOnlyList<string>? excludedDatabases, DateTime? dataStartUtc, CancellationToken cancellationToken)
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
                ViewerCommandDeadlines.CurrentInteractiveReadSeconds, logger: null, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, cancellationToken);
            if (!plan.UseTable)
            {
                return null;
            }

            /* #6750: each arm's own exact start, as the MCP read binds it. */
            var spans = await QueryStoreRegressionBaseline.ResolveSpansAsync(
                connection, transaction, serverId, baselineStartUtc, plan, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, null, cancellationToken);
            var baselinePlan = await QueryStoreRegressionBaseline.ResolveAsync(
                connection, transaction, serverId, spans.RegularStart, startUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, null, cancellationToken,
                windowEnd: endUtc, spans: spans);
            /* #6750: the cost start the plan chose, as in the MCP read, so the banner and the divisors follow it. */
            spans = QueryStoreRegressionBaseline.WithPlanStart(spans, baselinePlan);
            baselinePlan = QueryStoreRegressionBaseline.WithArmStarts(baselinePlan, spans, plan.ClampedStart, dataStartUtc);
            /* #6750 part 3: as the MCP read, the key table once its backfill has run. */
            var wideSql = await WithSelfKeysWhenReadyAsync(connection, transaction, QueryStoreRegressionsTableSqlFor(baselinePlan), cancellationToken);

            /* #5506 step 2: the same measured margin as the MCP read (DarlingQueryStoreRegressionReader.TryGetQueryStoreRegressionsFromTableAsync
               explains it): the server's longest interval in place of the 26-hour floor, or the 26 hours where it is not known. */
            var edgeMargin = await QueryStoreIntervalWide.ResolveEdgeMarginAsync(
                connection, serverId, spans.RegularStart < spans.FailureStart ? spans.RegularStart : spans.FailureStart, endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
            if (edgeMargin < QueryStoreIntervalWide.PurgeEdgeMargin)
            {
                wideSql = QueryStoreIntervalWide.WithEdgeMargin(wideSql, QueryStoreIntervalWide.EdgeMarginSqlFor(edgeMargin));
            }

            /* #5506: the latest form's high live range starts at the next midnight minus the margin, as in the MCP read. */
            baselinePlan = QueryStoreRegressionBaseline.WithMeasuredHighStart(baselinePlan, edgeMargin);

            /* #6750: the 97 MB recent-sort spill, the same SET LOCAL as the MCP read (DarlingQueryStoreRegressionReader.TableReadWorkMemSql). */
            await using (var workMem = new NpgsqlCommand(QueryStoreRegressionBaseline.TableReadWorkMemSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds })
            {
                await workMem.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var command = new NpgsqlCommand(wideSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
            AddQueryStoreRegressionsParameters(command, serverId, startUtc, endUtc, databaseNames, spans.RegularStart, baselinePlan, excludedDatabases, spans.FailureStart);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var rows = await ReadQueryStoreRegressionRowsAsync(reader, cancellationToken);
            var clamp = DateTime.SpecifyKind(plan.ClampedStart, DateTimeKind.Unspecified);
            return (rows, baselinePlan.HoursHeldStart ?? spans.RegularStart, baselinePlan.FailureHoursHeldStart ?? spans.FailureStart,
                spans.RegularStart < clamp || spans.FailureStart < clamp);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            System.Diagnostics.Trace.TraceWarning($"#5809 regressions table read fell back to raw: {ex.GetType().Name}: {ex.Message}");
            return null;
        }
    }

    /// <summary>The ten parameters both forms of the read take: $1 server_id, $2 window start, $3 window end, $4 database
    /// filter, $5 baseline start, $6 the baseline days read from the daily totals (#5506), $7 the end of the live half's low
    /// <c>first_execution_time</c> range, $8 the start of its high range, $9 the databases left out (#6654), $10 the start of the hours held
    /// (#6750, <see cref="QueryStoreRegressionBaseline.Plan.HoursHeldStart"/>), $11 the failure arm's row-filter start and $12 the start
    /// of its hours held (#6750: the same as $5 and $10 on raw).</summary>
    private static void AddQueryStoreRegressionsParameters(
        NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, DateTime baselineStartUtc,
        QueryStoreRegressionBaseline.Plan plan, IReadOnlyList<string>? excludedDatabases, DateTime? failureBaselineStartUtc = null)
    {
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.LowEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.HighStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(DatabaseFilter.Excluding(excludedDatabases).ExcludeParameter());
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.HoursHeldStart ?? baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(failureBaselineStartUtc ?? baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.FailureHoursHeldStart ?? plan.HoursHeldStart ?? baselineStartUtc, DateTimeKind.Unspecified) });
        if (QueryStoreRegressionBaseline.ReadsRecentDays(plan))
        {
            /* #5506: $13 to $15, only on the recent-days form, the only text that names them. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date, Value = plan.RecentDays });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.RecentLowEnd, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.RecentHighStart, DateTimeKind.Unspecified) });
        }
    }

    private static double? NullableMs(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToDouble(reader.GetValue(ordinal));

    /// <summary>Reads the rows of either form of the read: both project the same <see cref="QueryStoreRegressionBaseline.SuffixSql"/>.</summary>
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
                RegressionReason = reader.IsDBNull(16) ? "" : reader.GetString(16),
                BaselineAbortedCount = reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                RecentAbortedCount = reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                BaselineAbortedCpuMs = NullableMs(reader, 19),
                RecentAbortedCpuMs = NullableMs(reader, 20),
                BaselineAbortedDurationMs = NullableMs(reader, 21),
                RecentAbortedDurationMs = NullableMs(reader, 22),
                BaselineExceptionCount = reader.IsDBNull(23) ? 0 : reader.GetInt64(23),
                RecentExceptionCount = reader.IsDBNull(24) ? 0 : reader.GetInt64(24),
                BaselineExceptionCpuMs = NullableMs(reader, 25),
                RecentExceptionCpuMs = NullableMs(reader, 26),
                BaselineExceptionDurationMs = NullableMs(reader, 27),
                RecentExceptionDurationMs = NullableMs(reader, 28),
                BaselineRegularPerHour = NullableMs(reader, 29),
                RecentRegularPerHour = NullableMs(reader, 30),
                BaselineAbortedPerHour = NullableMs(reader, 31),
                RecentAbortedPerHour = NullableMs(reader, 32),
                BaselineExceptionPerHour = NullableMs(reader, 33),
                RecentExceptionPerHour = NullableMs(reader, 34),
            });
        }

        return rows;
    }
}

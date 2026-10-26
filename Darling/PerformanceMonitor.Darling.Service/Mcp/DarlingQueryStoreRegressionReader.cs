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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

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
    /// <summary>
    /// The baseline's own lookback (#4195): a fixed 7 days ending at the recent window's start, so the
    /// comparison period stops moving with retention. Before this the baseline was every retained row before
    /// the window with NO lower bound, so its cost tracked how much history the store still held rather than
    /// the window asked for — on a store with weeks of retained Query Store snapshots this read the whole
    /// retained history on every call. A regression against a baseline OLDER than 7 days is no longer caught
    /// (the difference this bound makes): a query that was slow 3 weeks ago, got fast, then regressed back to
    /// that same speed 2 days ago no longer flags, because its baseline window no longer reaches 3 weeks back.
    /// A store retaining less than 7 days of Query Store history is unaffected — the bound never reaches
    /// further back than the unbounded read already stopped.
    ///
    /// <para>Spelled out as "7 days" in <c>DarlingMcpQueryStoreRegressionTools</c>'s <c>Description</c> text
    /// too, rather than interpolated: an attribute argument must be a compile-time constant, and C# does not
    /// treat an interpolated string as one even when every placeholder is. Keep the two in sync by hand.</para>
    /// </summary>
    public const int BaselineLookbackDays = 7;


    /// <summary>One regression row - the viewer's <c>ViewerQueryStoreRegressionRow</c>, without the
    /// display-formatting members. Durations and CPU are ms (converted from the stored microseconds); the percents are
    /// plain deltas. Logical reads are not reported (#5506): they are not a useful metric.
    /// <para>Both percents are NULLABLE (#3541 A12, contract rule 5). Each divides through
    /// <c>NULLIF(baseline, 0)</c>, so a query whose baseline side is 0 has no ratio: the SQL returns NULL, and the
    /// reader used to coerce it to 0, which published the most dramatic possible regression as "no change". NULL stays
    /// NULL here and the tool says why. The CPU percent is NULL on a row flagged only for timeouts or errors, which has no
    /// cost comparison.</para>
    /// <para>#5506: <see cref="Aborted"/> and <see cref="Exception"/> are the other two groups of runs, beside the Regular
    /// figures above; <see cref="RegressionReason"/> says which of cost, timeouts and errors flagged the row.</para></summary>
    public sealed record RegressionRow(
        string DatabaseName,
        long QueryId,
        double BaselineDurationMs,
        double RecentDurationMs,
        double? DurationRegressionPercent,
        double BaselineCpuMs,
        double RecentCpuMs,
        double? CpuRegressionPercent,
        double AdditionalDurationMs,
        long BaselineExecCount,
        long RecentExecCount,
        int BaselinePlanCount,
        int RecentPlanCount,
        string Severity,
        string QueryTextSample,
        DateTime? LastExecutionTime,
        long TotalCount = 0,
        string RegressionReason = "",
        RunGroup? Aborted = null,
        RunGroup? Exception = null,
        double? BaselineRegularPerHour = null,
        double? RecentRegularPerHour = null);

    /// <summary>One group of runs (Aborted or Exception, #5506) of one query: the execution count and the per-execution CPU
    /// and duration (ms, weighted by execution count, null when the count is 0), and the count per hour, for the baseline and
    /// for the recent window.</summary>
    public sealed record RunGroup(
        long BaselineCount,
        long RecentCount,
        double? BaselineCpuMs,
        double? RecentCpuMs,
        double? BaselineDurationMs,
        double? RecentDurationMs,
        double? BaselinePerHour,
        double? RecentPerHour);

    /// <summary>
    /// The shared statement text of both reads (#5506). The arms (<see cref="QueryStoreRegressionBaseline.RawArmsSql"/> or
    /// <see cref="QueryStoreRegressionBaseline.WideArmsSql"/>), the aggregates (<see cref="QueryStoreRegressionBaseline.AggregatesSql"/>)
    /// and <see cref="QueryStoreRegressionBaseline.SuffixSql"/>, bound to this read's parameters: $1 server_id, $2 window start, $3 window
    /// end, $4 database filter (text[] or NULL), $5 row cap, $6 baseline start, $7 the baseline's days read from the daily
    /// totals (date[]), $8 and $9 the live half's <c>first_execution_time</c> bounds, $10 the databases left out (#6654, text[] or NULL), $11 the start of the hours held (#6750, <see cref="QueryStoreRegressionBaseline.Plan.HoursHeldStart"/>, used only by the <c>windows</c> CTE). <paramref name="useDailyTotals"/> false
    /// names no daily table (no run day was found, or the store has none). <paramref name="regularFromLatest"/> (table arms only)
    /// reads the three Regular arms from <c>query_store_interval_latest</c> (<see cref="QueryStoreRegressionBaseline.Plan.RegularFromLatest"/>).
    /// <paramref name="recentFromDaily"/> (latest only) reads the recent window's whole days from the daily totals
    /// (<see cref="QueryStoreRegressionBaseline.Plan.RecentDays"/>, #5506): $12 the days (date[]), $13 and $14 the recent arm's
    /// live <c>first_execution_time</c> bounds. #6750 put the failure arm's two starts at $12 (its row filters) and $13 (its hours
    /// held), so the recent days' three parameters are $14 to $16.
    /// </summary>
    internal static string BuildSql(bool wide, bool useDailyTotals, bool regularFromLatest = false, bool recentFromDaily = false) =>
        QueryStoreRegressionBaseline.Bind(
            QueryStoreRegressionBaseline.Statement(wide, useDailyTotals, regularFromLatest, recentFromDaily),
            "$6", "$7", "$8", "$9", "$5", "$10", "$11", "$12", "$13", "$14", "$15", "$16");

    /// <summary>
    /// Where each arm of a regressions read started (#6750), for the answer's notice. <see cref="CostStart"/> is the start of the
    /// Regular (cost) baseline and <see cref="FailureStart"/> the start of the timeouts and errors baseline, each the start of the hours
    /// its own per-hour rates divide by. <see cref="BelowRawFloor"/> is true only when the table read reached below raw's floor (raw's
    /// floor is after the requested baseline start): the answer then names these starts and the tables that served them. Otherwise
    /// (raw served, or raw holds the whole baseline) the answer keeps raw's notice, and both starts are raw's effective start.
    /// <see cref="EdgeMargin"/> is the first_execution_time margin the read itself used (the measured one when the read resolved it,
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> otherwise), so the coverage probe scans no wider than the read.
    /// </summary>
    public sealed record RegressionSpans(DateTime CostStart, DateTime FailureStart, bool BelowRawFloor, bool CostFromLatest, TimeSpan EdgeMargin);

    /// <summary>A regressions read with the tier that answered and, on the table path, where each arm started (#6750).</summary>
    public sealed record RegressionRead(List<RegressionRow> Rows, string Source, RegressionSpans? Spans);

    /// <summary>The table path's work_mem for the regressions statement (#6750, <see cref="QueryStoreRegressionBaseline.TableReadWorkMemSql"/>).</summary>
    internal const string TableReadWorkMemSql = QueryStoreRegressionBaseline.TableReadWorkMemSql;

    /// <summary>
    /// The viewer's regression read over raw <c>query_store_stats</c>, with the baseline's closed days from the daily totals
    /// (#5506, #5448; see <see cref="QueryStoreRegressionBaseline"/>). Since #5809 it is the fallback:
    /// <see cref="QueryStoreRegressionsTableSql"/> serves wherever the interval table's gate allows.
    /// </summary>
    public static readonly string QueryStoreRegressionsSql = BuildSql(wide: false, useDailyTotals: true);

    /// <summary>
    /// The regression read over <c>query_store_interval_wide</c> (#5809), with the baseline's closed days from the daily
    /// totals (#5506). That table holds ONE row per interval (its latest snapshot, every outcome; this read keeps Regular, as
    /// the daily totals do), so no dedupe runs. $6 is the gate's clamp (<see cref="QueryStoreIntervalWide.ClampedStart"/>).
    /// An interval belongs to the baseline or to the recent window by its <c>last_execution_time</c> against the window start
    /// (<see cref="QueryStoreRegressionBaseline"/>), so none is on both sides.
    /// </summary>
    public static readonly string QueryStoreRegressionsTableSql = BuildSql(wide: true, useDailyTotals: true);

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
    /// $1 server_id, $2 window start, $3 window end, $4 baseline start (#4195), $5 database filter (text[] or
    /// NULL, #5015: the same match the read makes, so a caller that filtered can tell a filter that matched nothing
    /// from one that matched and found nothing wrong).</para>
    /// </summary>
    public const string RegressionCoverageSql = """
        SELECT
            EXISTS (
                SELECT 1
                FROM query_store_stats
                WHERE server_id = $1
                AND   collection_time >= $4
                AND   collection_time < $2
                AND   ($5::text[] IS NULL OR database_name = ANY($5))
            ) AS has_baseline,
            EXISTS (
                SELECT 1
                FROM query_store_stats
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
                AND   ($5::text[] IS NULL OR database_name = ANY($5))
            ) AS has_recent
        """;

    /// <summary>Runs <see cref="QueryStoreRegressionsTableSql"/> where the interval table's gate allows
    /// (<see cref="TryGetQueryStoreRegressionsFromTableAsync"/>, #5809), and <see cref="QueryStoreRegressionsSql"/> over raw
    /// otherwise. <paramref name="baselineStartUtc"/> is the baseline's lower bound (#4195) - <paramref name="startUtc"/>
    /// minus <see cref="BaselineLookbackDays"/> for every caller today, but taken as a parameter rather than computed in
    /// the SQL so the bound stays a plain timestamp (an interval computed inside the statement would cost TimescaleDB its
    /// plan-time chunk exclusion).</summary>
    public static async Task<List<RegressionRow>> GetQueryStoreRegressionsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, CancellationToken cancellationToken = default)
        => (await ReadQueryStoreRegressionsAsync(
            postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, null, cancellationToken)).Rows;

    /// <summary>
    /// <see cref="GetQueryStoreRegressionsAsync"/> with the tier that answered: <see cref="ReadScope.SourceIntervalTable"/>
    /// or <see cref="ReadScope.SourceRaw"/>, the words <c>get_query_store_top</c>'s <c>history_source</c> uses (#5809).
    /// <para>This form binds no data start (the plan's own start, the read before #6750): the tool uses the overload that takes one.</para>
    /// </summary>
    public static Task<(List<RegressionRow> Rows, string Source)> ReadQueryStoreRegressionsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, CancellationToken cancellationToken = default)
        => ReadQueryStoreRegressionsAsync(postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, null, cancellationToken);

    /// <summary>
    /// <see cref="ReadQueryStoreRegressionsAsync(NpgsqlDataSource, int, DateTime, DateTime, DatabaseFilter, int, DateTime, CancellationToken)"/> with this server's data start.
    /// <para><paramref name="dataStartUtc"/> (#6750) is where THIS SERVER's Query Store data starts: the
    /// <see cref="DarlingMcpWindowNotice.Probe(NpgsqlDataSource, string, string, DateTime, DateTime, CancellationToken)"/> answer over the baseline window, the
    /// same value the note reports as <c>effective_start</c>. Both paths divide the per-hour rates by the hours from the later of it and
    /// the plan's start (<see cref="QueryStoreRegressionBaseline.WithHoursHeldStart"/>), so the note and the divisors name one instant.
    /// Null (no coverage, a failed probe, or a caller that has none) keeps the plan's start. It never changes the rows read.</para>
    /// </summary>
    public static async Task<(List<RegressionRow> Rows, string Source)> ReadQueryStoreRegressionsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, DateTime? dataStartUtc, CancellationToken cancellationToken = default)
    {
        var read = await ReadWithSpansAsync(postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, dataStartUtc, cancellationToken);
        return (read.Rows, read.Source);
    }

    /// <summary><see cref="ReadQueryStoreRegressionsAsync(NpgsqlDataSource, int, DateTime, DateTime, DatabaseFilter, int, DateTime, DateTime?, CancellationToken)"/>
    /// with where each arm started (#6750, <see cref="RegressionSpans"/>).</summary>
    public static async Task<RegressionRead> ReadWithSpansAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, DateTime? dataStartUtc, CancellationToken cancellationToken = default)
    {
        var table = await TryReadFromTableAsync(
            postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, dataStartUtc, cancellationToken);
        if (table is not null)
        {
            return table;
        }

        /* #5506: raw too reads the baseline's closed days from the daily totals, so it runs in the same kind of snapshot
           transaction: the built days it names and the live rows it reads are one view of the store. */
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        await SetReadOnlyAsync(connection, transaction, cancellationToken);
        var plan = await QueryStoreRegressionBaseline.ResolveAsync(
            connection, transaction, serverId, baselineStartUtc, startUtc, McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken,
            clampToRawFloor: true);
        /* #6750: the divisors start at this server's data start, not at the store-wide chunk floor the row clamp above reads. */
        plan = QueryStoreRegressionBaseline.WithHoursHeldStart(plan, baselineStartUtc, dataStartUtc);
        var rawSql = await WithSelfKeysWhenReadyAsync(connection, transaction, plan.BuiltDayCount > 0 ? QueryStoreRegressionsSql : s_rawLiveSql, cancellationToken);
        await using var command = new NpgsqlCommand(rawSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
        AddParameters(command, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, plan);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = await ReadRowsAsync(reader, cancellationToken);
        ReadScope.NoteSource(ReadScope.SourceRaw);
        var held = plan.HoursHeldStart ?? baselineStartUtc;
        return new RegressionRead(rows, ReadScope.SourceRaw, new RegressionSpans(held, held, false, false, QueryStoreIntervalWide.PurgeEdgeMargin));
    }

    /// <summary>The raw read with no run day: the baseline is all live and the daily table is not named (#5506).</summary>
    private static readonly string s_rawLiveSql = BuildSql(wide: false, useDailyTotals: false);

    /// <summary>The table read with no run day.</summary>
    private static readonly string s_wideLiveSql = BuildSql(wide: true, useDailyTotals: false);

    /// <summary>The table read with its Regular arms on <c>query_store_interval_latest</c> (#5506), with and without run days.</summary>
    private static readonly string s_latestDailySql = BuildSql(wide: true, useDailyTotals: true, regularFromLatest: true);

    private static readonly string s_latestLiveSql = BuildSql(wide: true, useDailyTotals: false, regularFromLatest: true);

    /// <summary>The latest forms with the recent window's whole days from the daily totals (#5506), with and without baseline run days.</summary>
    private static readonly string s_recentDailySql = BuildSql(wide: true, useDailyTotals: true, regularFromLatest: true, recentFromDaily: true);

    private static readonly string s_recentLiveSql = BuildSql(wide: true, useDailyTotals: false, regularFromLatest: true, recentFromDaily: true);

    /// <summary>The table form for a resolved baseline plan: daily totals when it has run days, and the Regular arms on
    /// <c>query_store_interval_latest</c> when its gate allows (#5506), else on the wide table. With recent days, the recent
    /// window's whole days come from the daily totals too.</summary>
    internal static string TableSqlFor(QueryStoreRegressionBaseline.Plan plan) =>
        QueryStoreRegressionBaseline.ReadsRecentDays(plan)
            ? (plan.BuiltDayCount > 0 ? s_recentDailySql : s_recentLiveSql)
            : plan.RegularFromLatest
                ? (plan.BuiltDayCount > 0 ? s_latestDailySql : s_latestLiveSql)
                : (plan.BuiltDayCount > 0 ? QueryStoreRegressionsTableSql : s_wideLiveSql);

    /// <summary>#6750 part 3: <paramref name="sql"/> with the self-statement test on the key table once its backfill has run
    /// (<see cref="QueryStoreSelfKeys.ReadyAsync"/>, read in this read's snapshot), else as it is, with the text probe.</summary>
    private static async Task<string> WithSelfKeysWhenReadyAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken cancellationToken) =>
        await QueryStoreSelfKeys.ReadyAsync(connection, transaction, McpCommandDeadlines.ReadSeconds, cancellationToken)
            ? QueryStoreRegressionBaseline.WithSelfKeys(sql)
            : sql;

    private static async Task SetReadOnlyAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var readOnly = new NpgsqlCommand(DarlingDataReader.SetTransactionReadOnlySql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
        await readOnly.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// #5809: the regression read over <c>query_store_interval_wide</c>, by <c>get_query_store_top</c>'s own tier rule
    /// (<see cref="DarlingDataReader.QueryStoreTopMayReadTable"/>, then <see cref="QueryStoreIntervalWide.ResolveReadAsync"/>)
    /// over the whole span the read compares, baseline start to window end, so the #5541 floor rule applies: a store whose
    /// table does not yet cover that span (no coverage, coverage that starts after the baseline start, a pending batch, a
    /// table floor too close to raw's) reads raw. The decision and the read share ONE read-only REPEATABLE READ snapshot,
    /// as the top and history reads do, and so do the baseline's built days (#5506). Null when the gate says raw or anything
    /// here faults (cancellation propagates), so the caller runs the raw read; an empty list is a real answer from the table
    /// and is returned as one.
    /// <para>#6750: each arm binds its own exact start (<see cref="QueryStoreRegressionBaseline.ResolveSpansAsync"/>). Before this
    /// every arm bound the gate's <see cref="QueryStoreIntervalWide.WideReadPlan.ClampedStart"/>, max(baseline start, raw's floor), so
    /// on a store whose raw tier keeps 4 days a 168 hour read had an empty cost baseline and a recent window cut to raw's floor, while
    /// the interval tables held both. Now the Regular baseline and the recent arm bind the start <c>query_store_interval_latest</c>
    /// proves (the daily days start no earlier, and the start moves down to the baseline's first midnight when that day's built
    /// totals serve it, <see cref="QueryStoreRegressionBaseline.ExtendToBuiltFirstDay"/>), and <c>nonregular_rows</c> binds the wide table's below-floor <c>ReadStart</c>
    /// (<c>@@BF</c>). Each per-hour rate divides by the hours from its own arm's start. Where raw holds the whole baseline every
    /// start is the baseline start and the read is the one before #6750.</para>
    /// </summary>
    internal static Task<List<RegressionRow>?> TryGetQueryStoreRegressionsFromTableAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, CancellationToken cancellationToken)
        => TryGetQueryStoreRegressionsFromTableAsync(postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, null, cancellationToken);

    /// <summary><see cref="TryGetQueryStoreRegressionsFromTableAsync(NpgsqlDataSource, int, DateTime, DateTime, DatabaseFilter, int, DateTime, CancellationToken)"/>
    /// with this server's data start (#6750), where the divisors start at the later of it and the gate's clamp.</summary>
    internal static async Task<List<RegressionRow>?> TryGetQueryStoreRegressionsFromTableAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, DateTime? dataStartUtc, CancellationToken cancellationToken)
        => (await TryReadFromTableAsync(postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, dataStartUtc, cancellationToken))?.Rows;

    private static async Task<RegressionRead?> TryReadFromTableAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, DateTime? dataStartUtc, CancellationToken cancellationToken)
    {
        if (!DarlingDataReader.QueryStoreTopMayReadTable(baselineStartUtc, endUtc))
        {
            return null;
        }

        var gateDecided = false;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
            await SetReadOnlyAsync(connection, transaction, cancellationToken);

            var wide = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, baselineStartUtc, endUtc, endUtc, DarlingDataReader.QueryStoreTopMinWindow,
                McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, cancellationToken);
            if (!wide.UseTable)
            {
                if (wide.DecisionFailed)
                {
                    ReadScope.Note(ReadFallback.GateFailed);
                }
                else
                {
                    /* An expected state, not a fault: a young store, a span past the horizon, a pending batch. */
                    ReadScope.Current?.Logger?.LogDebug("#5809 Query Store regressions read: the interval table's gate chose raw for server {ServerId}", serverId);
                }

                return null;
            }

            gateDecided = true;
            /* #6750: each arm's own exact start, from the table that serves it. */
            var spans = await QueryStoreRegressionBaseline.ResolveSpansAsync(
                connection, transaction, serverId, baselineStartUtc, wide, McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken);
            var plan = await QueryStoreRegressionBaseline.ResolveAsync(
                connection, transaction, serverId, spans.RegularStart, startUtc, McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken,
                windowEnd: endUtc, spans: spans);
            /* #6750: the cost start the plan chose (the baseline's first midnight when its built day serves it) is the one every
               Regular bind, the hours held and cost_baseline_start use. */
            spans = QueryStoreRegressionBaseline.WithPlanStart(spans, plan);
            plan = QueryStoreRegressionBaseline.WithArmStarts(plan, spans, wide.ClampedStart, dataStartUtc);
            var wideSql = await WithSelfKeysWhenReadyAsync(connection, transaction, TableSqlFor(plan), cancellationToken);

            /* #5506 step 2: the first_execution_time floors of the live arms reach PurgeEdgeMargin (26 hours) before their bound,
               because an interval may span a day. When the server's own intervals are shorter, the floors shrink to what they need
               (three hours at 60-minute intervals), so the day before the baseline start and the day before the window are not
               read whole (#5882, the same lookup as get_query_store_top and the aborts read). A server with no trustworthy
               measurement keeps the 26 hours. The lookup is one extra round trip, 5 to 9 ms warm. */
            var edgeMargin = await QueryStoreIntervalWide.ResolveEdgeMarginAsync(
                connection, serverId, spans.RegularStart < spans.FailureStart ? spans.RegularStart : spans.FailureStart, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);
            if (edgeMargin < QueryStoreIntervalWide.PurgeEdgeMargin)
            {
                wideSql = QueryStoreIntervalWide.WithEdgeMargin(wideSql, QueryStoreIntervalWide.EdgeMarginSqlFor(edgeMargin));
            }

            /* #5506: on the latest form the high live range starts at the next midnight minus the same margin, not at the last
               run day's midnight, so that day's rows are not read only to be dropped. */
            plan = QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, edgeMargin);

            await using (var workMem = new NpgsqlCommand(TableReadWorkMemSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await workMem.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var command = new NpgsqlCommand(wideSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
            AddParameters(command, serverId, startUtc, endUtc, databases, limit, spans.RegularStart, plan, spans.FailureStart);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var rows = await ReadRowsAsync(reader, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceIntervalTable);
            var clamp = DateTime.SpecifyKind(wide.ClampedStart, DateTimeKind.Unspecified);
            var belowRawFloor = spans.RegularStart < clamp || spans.FailureStart < clamp;
            return new RegressionRead(rows, ReadScope.SourceIntervalTable, new RegressionSpans(
                plan.HoursHeldStart ?? spans.RegularStart, plan.FailureHoursHeldStart ?? spans.FailureStart, belowRawFloor, plan.RegularFromLatest, edgeMargin));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ReadScope.Note(gateDecided ? ReadFallback.FallbackRaw : ReadFallback.GateFailed);
            LogTableFault(ReadScope.Current?.Logger, ex);
            return null;
        }
    }

    /// <summary>Set once the first table-read fault of this process has been logged at Warning.</summary>
    private static int s_tableFaultWarned;

    /// <summary>
    /// Logs a table-read fault (#5809). The first one in the process is a Warning that names the exception's message,
    /// so a store whose every call has quietly fallen back to raw shows it in the service log; every later one is Debug,
    /// so a store that faults on every call does not write a Warning per call. With no logger the Trace line unscoped
    /// callers have always had is written, and the once flag is left for the first call that has a logger.
    /// </summary>
    internal static void LogTableFault(ILogger? logger, Exception ex)
    {
        if (logger is null)
        {
            System.Diagnostics.Trace.TraceWarning($"#5809 Query Store regressions table read fell back to raw: {ex.GetType().Name}");
        }
        else if (Interlocked.Exchange(ref s_tableFaultWarned, 1) == 0)
        {
            logger.LogWarning(ex, "#5809 Query Store regressions table read fell back to raw: {Message}. Later faults in this process log at Debug.", ex.Message);
        }
        else
        {
            logger.LogDebug(ex, "#5809 Query Store regressions table read fell back to raw: {Message}", ex.Message);
        }
    }

    /// <summary>Re-arms <see cref="LogTableFault"/>'s once-per-process Warning. Tests only.</summary>
    internal static void ResetTableFaultWarningForTests() => Interlocked.Exchange(ref s_tableFaultWarned, 0);

    /// <summary>The eleven parameters both forms of the read take, in their order: $1 server_id, $2 window start, $3 window
    /// end, $4 database filter, $5 row cap, $6 baseline start, $7 the baseline days read from the daily totals (#5506), $8 the
    /// end of the live half's low <c>first_execution_time</c> range, $9 the start of its high range, $10 the databases left out
    /// (<see cref="DatabaseFilter.ExcludeParameter"/>, #6654), $11 the start of the hours held (#6750): the later of the plan's start and this server's
    /// data start (<see cref="QueryStoreRegressionBaseline.Plan.HoursHeldStart"/>). The row filters ($6) keep the requested start on every path, so the rows read are the same.
    /// #6750: $12 the failure arm's row-filter start (<paramref name="failureBaselineStartUtc"/>, the Regular start when null, as on raw)
    /// and $13 the start of its hours held (<see cref="QueryStoreRegressionBaseline.Plan.FailureHoursHeldStart"/>, else $11's value).</summary>
    internal static void AddParameters(
        NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc, DatabaseFilter databases, int limit, DateTime baselineStartUtc,
        QueryStoreRegressionBaseline.Plan plan, DateTime? failureBaselineStartUtc = null)
    {
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(databases.Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.LowEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.HighStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(databases.ExcludeParameter());
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.HoursHeldStart ?? baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(failureBaselineStartUtc ?? baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.FailureHoursHeldStart ?? plan.HoursHeldStart ?? baselineStartUtc, DateTimeKind.Unspecified) });
        if (QueryStoreRegressionBaseline.ReadsRecentDays(plan))
        {
            /* #5506: $14 to $16, only on the recent-days form, the only text that names them. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date, Value = plan.RecentDays });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.RecentLowEnd, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.RecentHighStart, DateTimeKind.Unspecified) });
        }
    }

    private static double? NullableDouble(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToDouble(reader.GetValue(ordinal));

    /// <summary>One group of runs from the six figure columns starting at <paramref name="first"/> (base count, recent count,
    /// base CPU, recent CPU, base duration, recent duration) and its two per-hour columns (#5506).</summary>
    private static RunGroup ReadGroup(NpgsqlDataReader reader, int first, int baselinePerHour, int recentPerHour) => new(
        reader.IsDBNull(first) ? 0 : reader.GetInt64(first),
        reader.IsDBNull(first + 1) ? 0 : reader.GetInt64(first + 1),
        NullableDouble(reader, first + 2),
        NullableDouble(reader, first + 3),
        NullableDouble(reader, first + 4),
        NullableDouble(reader, first + 5),
        NullableDouble(reader, baselinePerHour),
        NullableDouble(reader, recentPerHour));

    /// <summary>Reads the rows of either form of the read: both project the same <see cref="QueryStoreRegressionBaseline.SuffixSql"/>.</summary>
    internal static async Task<List<RegressionRow>> ReadRowsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
    {
        var rows = new List<RegressionRow>();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new RegressionRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? null : Convert.ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                reader.IsDBNull(7) ? null : Convert.ToDouble(reader.GetValue(7)),
                reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt32(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                reader.IsDBNull(13) ? "" : reader.GetString(13),
                reader.IsDBNull(14) ? "" : reader.GetString(14),
                reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                reader.IsDBNull(35) ? 0 : reader.GetInt64(35),
                reader.IsDBNull(16) ? "" : reader.GetString(16),
                ReadGroup(reader, 17, 31, 32),
                ReadGroup(reader, 23, 33, 34),
                NullableDouble(reader, 29),
                NullableDouble(reader, 30)));
        }

        return rows;
    }

    /// <summary>
    /// <see cref="RegressionCoverageSql"/> over the interval table that served a table read's cost arms (#6750): latest when it did,
    /// the wide table otherwise. $1 server_id, $2 window start, $3 window end, $4 the cost arm's start, $5 database filter. A row is on
    /// the baseline side when its last run is in [$4, $2), on the recent side when it is at or after $2, the line the read itself
    /// splits on. <paramref name="margin"/> is the first_execution_time margin the read used (<see cref="RegressionSpans.EdgeMargin"/>),
    /// written the way the read writes it: <see cref="QueryStoreIntervalWide.EdgeMarginSqlFor"/> when it is below
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>, the full <see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/> otherwise.
    /// <para>
    /// Measured on the large production store at 168 hours (cold): with both ranges starting the full 26 hours before their bound, this
    /// probe took 6.6 s and read 135 MB, on top of the read. <c>has_baseline</c> scanned the whole partition of the day before the cost
    /// start (151,349 rows that all failed the <c>last_execution_time</c> bound) and <c>has_recent</c> walked about 155,000 index rows
    /// that failed it. So each answer is two EXISTS joined by OR, the witness arm first: the arm whose rows start inside the side
    /// itself (a row that started and last ran there), then the edge arm for rows that started in the margin before it and last ran
    /// inside. OR is left to right, so the cheap arm usually answers and the edge arm is read only when it does not. The two
    /// <c>first_execution_time</c> ranges together equal the one range they replace, and both arms keep the <c>last_execution_time</c>
    /// lower bound, the one the index walk is filtered by.
    /// </para>
    /// </summary>
    internal static string TableCoverageSql(string table, TimeSpan margin)
    {
        var marginSql = margin < QueryStoreIntervalWide.PurgeEdgeMargin
            ? QueryStoreIntervalWide.EdgeMarginSqlFor(margin)
            : QueryStoreIntervalWide.PurgeEdgeMarginSql;
        return $$"""
        SELECT
            (
                EXISTS (
                    SELECT 1
                    FROM collect.{{table}}
                    WHERE server_id = $1
                    AND   first_execution_time >= $4
                    AND   first_execution_time < $2
                    AND   last_execution_time >= $4
                    AND   last_execution_time < $2
                    AND   ($5::text[] IS NULL OR database_name = ANY($5))
                )
                OR EXISTS (
                    SELECT 1
                    FROM collect.{{table}}
                    WHERE server_id = $1
                    AND   first_execution_time >= $4 - {{marginSql}}
                    AND   first_execution_time < $4
                    AND   last_execution_time >= $4
                    AND   last_execution_time < $2
                    AND   ($5::text[] IS NULL OR database_name = ANY($5))
                )
            ) AS has_baseline,
            (
                EXISTS (
                    SELECT 1
                    FROM collect.{{table}}
                    WHERE server_id = $1
                    AND   first_execution_time >= $2
                    AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
                    AND   last_execution_time >= $2
                    AND   collection_time <= $3
                    AND   ($5::text[] IS NULL OR database_name = ANY($5))
                )
                OR EXISTS (
                    SELECT 1
                    FROM collect.{{table}}
                    WHERE server_id = $1
                    AND   first_execution_time >= $2 - {{marginSql}}
                    AND   first_execution_time < $2
                    AND   last_execution_time >= $2
                    AND   collection_time <= $3
                    AND   ($5::text[] IS NULL OR database_name = ANY($5))
                )
            ) AS has_recent
        """;
    }

    /// <summary>Runs <see cref="TableCoverageSql"/> for the table that served <paramref name="spans"/>' cost arms.</summary>
    public static async Task<(bool HasBaseline, bool HasRecent)> GetTableCoverageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, RegressionSpans spans,
        DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(TableCoverageSql(spans.CostFromLatest ? QueryStoreIntervalLatest.TableName : QueryStoreIntervalWide.TableName, spans.EdgeMargin));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(spans.CostStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }

    /// <summary>Runs <see cref="RegressionCoverageSql"/>. <paramref name="databases"/> narrows both questions to
    /// the chosen databases, as <see cref="GetQueryStoreRegressionsAsync"/> does (#5245; <see cref="DatabaseFilter.All"/>,
    /// the default: every database).</summary>
    public static async Task<(bool HasBaseline, bool HasRecent)> GetCoverageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, DateTime baselineStartUtc,
        DatabaseFilter databases = default, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(RegressionCoverageSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return (false, false);
        return (reader.GetBoolean(0), reader.GetBoolean(1));
    }
}

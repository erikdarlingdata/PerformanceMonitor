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
    /// totals (date[]), $8 and $9 the live half's <c>first_execution_time</c> bounds. <paramref name="useDailyTotals"/> false
    /// names no daily table (no run day was found, or the store has none).
    /// </summary>
    internal static string BuildSql(bool wide, bool useDailyTotals) =>
        QueryStoreRegressionBaseline.Bind(
            (wide ? QueryStoreRegressionBaseline.WideArmsSql : QueryStoreRegressionBaseline.RawArmsSql)
            + "\n" + QueryStoreRegressionBaseline.AggregatesSql(useDailyTotals) + "\n" + QueryStoreRegressionBaseline.SuffixSql,
            "$6", "$7", "$8", "$9", "$5");

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
            postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, cancellationToken)).Rows;

    /// <summary>
    /// <see cref="GetQueryStoreRegressionsAsync"/> with the tier that answered: <see cref="ReadScope.SourceIntervalTable"/>
    /// or <see cref="ReadScope.SourceRaw"/>, the words <c>get_query_store_top</c>'s <c>history_source</c> uses (#5809).
    /// </summary>
    public static async Task<(List<RegressionRow> Rows, string Source)> ReadQueryStoreRegressionsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, CancellationToken cancellationToken = default)
    {
        var tableRows = await TryGetQueryStoreRegressionsFromTableAsync(
            postgres, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, cancellationToken);
        if (tableRows is not null)
        {
            return (tableRows, ReadScope.SourceIntervalTable);
        }

        /* #5506: raw too reads the baseline's closed days from the daily totals, so it runs in the same kind of snapshot
           transaction: the built days it names and the live rows it reads are one view of the store. */
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        await SetReadOnlyAsync(connection, transaction, cancellationToken);
        var plan = await QueryStoreRegressionBaseline.ResolveAsync(
            connection, transaction, serverId, baselineStartUtc, startUtc, McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken,
            clampToRawFloor: true);
        var rawSql = plan.BuiltDayCount > 0 ? QueryStoreRegressionsSql : s_rawLiveSql;
        await using var command = new NpgsqlCommand(rawSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
        AddParameters(command, serverId, startUtc, endUtc, databases, limit, baselineStartUtc, plan);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = await ReadRowsAsync(reader, cancellationToken);
        ReadScope.NoteSource(ReadScope.SourceRaw);
        return (rows, ReadScope.SourceRaw);
    }

    /// <summary>The raw read with no run day: the baseline is all live and the daily table is not named (#5506).</summary>
    private static readonly string s_rawLiveSql = BuildSql(wide: false, useDailyTotals: false);

    /// <summary>The table read with no run day.</summary>
    private static readonly string s_wideLiveSql = BuildSql(wide: true, useDailyTotals: false);

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
    /// <para>The table read binds the gate's <see cref="QueryStoreIntervalWide.WideReadPlan.ClampedStart"/>, not its
    /// below-floor <c>ReadStart</c>: the tool's <c>effective_start</c> notice is raw's, so the table reads exactly the
    /// span raw would. The built days start no earlier than that bound either.</para>
    /// </summary>
    internal static async Task<List<RegressionRow>?> TryGetQueryStoreRegressionsFromTableAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DatabaseFilter databases, int limit, DateTime baselineStartUtc, CancellationToken cancellationToken)
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
                McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken);
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
            var plan = await QueryStoreRegressionBaseline.ResolveAsync(
                connection, transaction, serverId, wide.ClampedStart, startUtc, McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken);
            var wideSql = plan.BuiltDayCount > 0 ? QueryStoreRegressionsTableSql : s_wideLiveSql;
            await using var command = new NpgsqlCommand(wideSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
            AddParameters(command, serverId, startUtc, endUtc, databases, limit, wide.ClampedStart, plan);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var rows = await ReadRowsAsync(reader, cancellationToken);
            ReadScope.NoteSource(ReadScope.SourceIntervalTable);
            return rows;
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

    /// <summary>The nine parameters both forms of the read take, in their order: $1 server_id, $2 window start, $3 window
    /// end, $4 database filter, $5 row cap, $6 baseline start, $7 the baseline days read from the daily totals (#5506), $8 the
    /// end of the live half's low <c>first_execution_time</c> range, $9 the start of its high range.</summary>
    private static void AddParameters(
        NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc, DatabaseFilter databases, int limit, DateTime baselineStartUtc,
        QueryStoreRegressionBaseline.Plan plan)
    {
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(databases.Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStartUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.LowEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(plan.HighStart, DateTimeKind.Unspecified) });
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

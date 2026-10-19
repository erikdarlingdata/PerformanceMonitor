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
    /// display-formatting members. Durations and CPU are ms (converted from the stored microseconds);
    /// reads are raw pages; the percents are plain deltas.
    /// <para>The three percents are NULLABLE (#3541 A12, contract rule 5). Each divides through
    /// <c>NULLIF(baseline, 0)</c>, so a query whose baseline side is 0 — no logical reads in every capture
    /// before the window, then 50,000 per execution inside it — has no ratio: the SQL returns NULL, and the
    /// reader used to coerce it to 0, which published the most dramatic possible I/O regression as
    /// <c>io_regression_percent: 0</c>, "no change". NULL stays NULL here and the tool says why. The CPU
    /// percent cannot actually arrive NULL (the WHERE gate <c>&gt; 25</c> drops a NULL comparison), but it is
    /// typed like its siblings so the three cannot drift in how they treat a missing denominator.</para></summary>
    public sealed record RegressionRow(
        string DatabaseName,
        long QueryId,
        double BaselineDurationMs,
        double RecentDurationMs,
        double? DurationRegressionPercent,
        double BaselineCpuMs,
        double RecentCpuMs,
        double? CpuRegressionPercent,
        double BaselineReads,
        double RecentReads,
        double? IoRegressionPercent,
        double AdditionalDurationMs,
        long BaselineExecCount,
        long RecentExecCount,
        int BaselinePlanCount,
        int RecentPlanCount,
        string Severity,
        string QueryTextSample,
        DateTime? LastExecutionTime,
        long TotalCount = 0);

    /// <summary>
    /// The viewer's regression read over raw <c>query_store_stats</c>. $1 server_id, $2 window start (the baseline is
    /// $6 up to $2 - #4195, <see cref="BaselineLookbackDays"/> by default), $3 window end, $4 database filter (text[]
    /// or NULL), $5 row cap, $6 baseline start. Since #5809 it is the fallback: <see cref="QueryStoreRegressionsTableSql"/>
    /// serves wherever the interval table's gate allows.
    /// </summary>
    public const string QueryStoreRegressionsSql = QueryStoreRegressionsRawPrefix + "\n" + QueryStoreRegressionsSuffix;

    /// <summary>The raw read's two deduping arms (#1841). <see cref="QueryStoreRegressionsTableSql"/> replaces only these.</summary>
    private const string QueryStoreRegressionsRawPrefix = """
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
            AND   collection_time >= $6
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
        """;

    /// <summary>
    /// Everything after the two arms, shared by <see cref="QueryStoreRegressionsSql"/> and
    /// <see cref="QueryStoreRegressionsTableSql"/> so the aggregation, the gate and the ranking cannot drift between them.
    /// </summary>
    private const string QueryStoreRegressionsSuffix = """
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
            r.last_execution_time,
            /* #6128: the rows that passed the gate, before LIMIT. A window count runs ahead of the LIMIT in the same
               statement, over rows the join and the gate already produced, so it costs no second pass. */
            COUNT(*) OVER () AS total_count
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

    /// <summary>The table read's two arms (#5809), in place of <see cref="QueryStoreRegressionsRawPrefix"/>. Declared
    /// ABOVE <see cref="QueryStoreRegressionsTableSql"/> on purpose: static field initializers run in textual order, so a
    /// prefix declared below it was still null when the field read it, and the table SQL began at
    /// <c>baseline_performance</c> and failed with 42601 on every call.</summary>
    private static readonly string QueryStoreRegressionsTablePrefix = $$"""
        WITH deduped_baseline AS (
            SELECT
                database_name,
                query_id,
                plan_id,
                execution_count,
                avg_duration_us,
                avg_cpu_time_us,
                avg_logical_io_reads,
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   collection_time >= $6
            AND   collection_time < $2
            AND   first_execution_time >= $6 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $2 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
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
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time >= $6
            AND   collection_time <= $3
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        """;

    /// <summary>
    /// The regression read over <c>query_store_interval_wide</c> (#5809): <see cref="QueryStoreRegressionsSql"/> with its
    /// two deduping arms replaced by reads of the table, which already holds ONE row per interval identity (the raw arms'
    /// <c>PARTITION BY</c> key, <c>execution_type_desc</c> included), that interval's latest snapshot, kept as raw is
    /// written. Same parameters; $6 is the gate's clamp (<see cref="QueryStoreIntervalWide.ClampedStart"/>,
    /// <c>max(baseline start, raw's chunk floor)</c>), so the table never reaches below what raw itself still holds.
    ///
    /// <para><b>Why not <c>query_store_interval_latest</c>.</b> That table keeps Regular executions only, and the raw arms
    /// count every outcome (Aborted and Exception too). The wide table keeps every outcome, so the averages and
    /// exec counts are the same function of the same intervals.</para>
    ///
    /// <para><b>Bounds.</b> Each arm keeps the raw arm's own <c>collection_time</c> window (the semantic one) and adds the
    /// #5533 <c>first_execution_time</c> range, both ends: from the arm's start minus
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/> (#4605: no row of the arm has an older first execution) to
    /// its end plus <see cref="QueryStoreIntervalWide.FirstExecUpperSlackSql"/>. That range is what
    /// <c>ix_query_store_interval_wide_server_first_exec</c> scans, so each arm reads its own days of the server's rows
    /// and no sort runs to dedupe them. The recent arm also takes <c>collection_time &gt;= $6</c>, which only matters when
    /// raw's floor is inside the recent window: raw then reads the recent arm from its floor too.</para>
    ///
    /// <para><b>The one edge where the two reads differ.</b> An interval collected on both sides of the window start (its
    /// first snapshots before $2, its latest after) is counted by raw in BOTH arms, the partial snapshot in the baseline and
    /// the latest in the recent window. The table holds only the latest, so it counts that interval in the recent arm alone.
    /// That is at most one interval per plan (the one open at $2), its partial counts blended into a 7-day baseline, and the
    /// table form is the one that does not count the same executions on both sides. The window END is exact: the gate reads
    /// raw for an end before the table's <c>applied_through</c> (its clause 4), where a latest snapshot could sit past it.</para>
    ///
    /// <para><c>1 AS rn</c> keeps <see cref="QueryStoreRegressionsSuffix"/>'s <c>WHERE rn = 1</c> true for every row:
    /// the table's row IS the raw arm's <c>rn = 1</c> row. A static readonly because the interval literals are derived
    /// from TimeSpans; <c>$$"""</c> keeps <c>$1</c> literal.</para>
    /// </summary>
    public static readonly string QueryStoreRegressionsTableSql = QueryStoreRegressionsTablePrefix + "\n" + QueryStoreRegressionsSuffix;

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

        await using var command = postgres.CreateCommand(QueryStoreRegressionsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddParameters(command, serverId, startUtc, endUtc, databases, limit, baselineStartUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = await ReadRowsAsync(reader, cancellationToken);
        ReadScope.NoteSource(ReadScope.SourceRaw);
        return (rows, ReadScope.SourceRaw);
    }

    /// <summary>
    /// #5809: the regression read over <c>query_store_interval_wide</c>, by <c>get_query_store_top</c>'s own tier rule
    /// (<see cref="DarlingDataReader.QueryStoreTopMayReadTable"/>, then <see cref="QueryStoreIntervalWide.ResolveReadAsync"/>)
    /// over the whole span the read compares, baseline start to window end, so the #5541 floor rule applies: a store whose
    /// table does not yet cover that span (no coverage, coverage that starts after the baseline start, a pending batch, a
    /// table floor too close to raw's) reads raw. The decision and the read share ONE read-only REPEATABLE READ snapshot,
    /// as the top and history reads do. Null when the gate says raw or anything here faults (cancellation propagates), so
    /// the caller runs the raw read; an empty list is a real answer from the table and is returned as one.
    /// <para>The table read binds the gate's <see cref="QueryStoreIntervalWide.WideReadPlan.ClampedStart"/>, not its
    /// below-floor <c>ReadStart</c>: the tool's <c>effective_start</c> notice is raw's, so the table reads exactly the
    /// span raw would.</para>
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
            await using (var readOnly = new NpgsqlCommand(DarlingDataReader.SetTransactionReadOnlySql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, baselineStartUtc, endUtc, endUtc, DarlingDataReader.QueryStoreTopMinWindow,
                McpCommandDeadlines.ReadSeconds, ReadScope.Current?.Logger, cancellationToken);
            if (!plan.UseTable)
            {
                if (plan.DecisionFailed)
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
            await using var command = new NpgsqlCommand(QueryStoreRegressionsTableSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
            AddParameters(command, serverId, startUtc, endUtc, databases, limit, plan.ClampedStart);
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

    /// <summary>The six parameters both forms of the read take, in their order: $1 server_id, $2 window start, $3 window
    /// end, $4 database filter, $5 row cap, $6 baseline start.</summary>
    private static void AddParameters(
        NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc, DatabaseFilter databases, int limit, DateTime baselineStartUtc)
    {
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        command.Parameters.Add(databases.Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStartUtc, DateTimeKind.Unspecified) });
    }

    /// <summary>Reads the rows of either form of the read: both project the same <see cref="QueryStoreRegressionsSuffix"/>.</summary>
    private static async Task<List<RegressionRow>> ReadRowsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
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
                reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
                reader.IsDBNull(10) ? null : Convert.ToDouble(reader.GetValue(10)),
                reader.IsDBNull(11) ? 0 : Convert.ToDouble(reader.GetValue(11)),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                reader.IsDBNull(15) ? 0 : reader.GetInt32(15),
                reader.IsDBNull(16) ? "" : reader.GetString(16),
                reader.IsDBNull(17) ? "" : reader.GetString(17),
                reader.IsDBNull(18) ? null : reader.GetDateTime(18),
                reader.IsDBNull(19) ? 0 : reader.GetInt64(19)));
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

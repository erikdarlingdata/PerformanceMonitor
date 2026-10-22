/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The 7-day baseline of the Query Store regressions read (get_query_store_regressions, its web page and the viewer's
/// Regressions grid), built from per-day totals instead of re-reading the wide rows on every call (#5506, on #5448).
///
/// <para><b>Why.</b> The read compares a recent window with the 7 days before it. Measured on a 210-server store, almost every
/// call took the wide-table arm and read every wide row of 7 days plus a margin, about 1.2 KB each, to rebuild a baseline
/// whose closed days never change. <c>collect.plan_regression_daily</c> (V168, #5448) already holds one row per plan per day:
/// executions and the sums of <c>avg_cpu_time_us * execution_count</c> and <c>avg_duration_us * execution_count</c>. A whole
/// day that is VALID (<c>built_seq = late_seq</c>, <see cref="PlanRegressionDaily.BuiltDaysSql"/>) is read from there; every
/// other part of the baseline window is read live from the source the read already uses.</para>
///
/// <para><b>The seam is exact, by construction.</b> Both halves assign an interval to the day of its latest snapshot's
/// <c>last_execution_time</c> (naive UTC), the column <c>plan_regression_daily</c> rows by. The baseline is the intervals with
/// <c>last_execution_time</c> in [baseline start, window start). The built half takes the rows of the run days (whole days that
/// are valid and lie wholly inside that range); the live half takes the rows in the range whose
/// <c>last_execution_time::date</c> is NOT a run day. Each row has one <c>last_execution_time</c>, so it is in exactly one half:
/// none is counted twice and none is dropped. The recent window is <c>last_execution_time &gt;= window start</c>, the same
/// timestamp, so no interval is in both the baseline and the recent window either (the old definition, by
/// <c>collection_time</c>, counted an interval that straddled the window start in both). The index ranges on
/// <c>first_execution_time</c> the live half uses (<see cref="LowEnd"/>, <see cref="HighStart"/>) skip the heap of the run
/// days; they only narrow the scan, the date test is what decides membership, and an interval's
/// <c>first_execution_time</c> is within a day before its <c>last_execution_time</c>
/// (<see cref="PlanRegressionDaily.BuildDaySql"/>'s bound).</para>
///
/// <para><b>Regular executions only.</b> <c>collect.query_store_interval_latest</c>, and so the daily totals, keep only
/// <c>execution_type_desc = 'Regular'</c>. So the whole read (baseline, both halves, and the recent window) counts Regular
/// executions: a baseline that kept Aborted and Exception runs on some days and not others would not add up, and a recent window
/// that kept them against a baseline that did not would flag every query that times out at a steady rate.
/// <c>get_query_store_aborts</c> reports the others.</para>
///
/// <para><b>Aborted and Exception runs beside the cost (#5506, Erik 2026-10-21).</b> Counting only Regular runs hides what a
/// timeout or an error does: one 30 s timeout among 1,000 fast runs barely moves an average, and blending them in would hide
/// it the same way. So the read reports THREE groups per query, baseline and recent, side by side: Regular (the cost the gate
/// compares), Aborted (timeouts, cancels) and Exception (errors), each with an execution count and CPU and duration per
/// execution (weighted by execution count, NULL when the count is 0), and a count per hour for each, because the recent window
/// and the baseline differ in length. The Aborted and Exception rows are read live from the wide table (or the raw table) for
/// both sides; the daily totals hold none. A query is flagged for COST (Regular CPU up more than <see cref="CostGatePercent"/>%),
/// for TIMEOUTS or ERRORS (that group's per-hour rate in the recent window at least <see cref="FailureRateMultiple"/> times the
/// baseline's, or any at all where the baseline had none, with at least <see cref="FailureMinRecentRuns"/> such runs in the
/// recent window), or for several; <c>regression_reason</c> says which.</para>
/// </summary>
public static class QueryStoreRegressionBaseline
{
    /// <summary>The cost gate (#5506 keeps today's): a query qualifies for <c>cost</c> when its Regular CPU per execution is
    /// more than this percent above its baseline.</summary>
    public const int CostGatePercent = PerformanceMonitor.Common.QueryStoreRegressionRules.CostGatePercent;

    /// <summary>A rise in timeouts or errors qualifies when the recent per-hour rate is at least this many times the baseline's
    /// (#5506). A nonzero recent rate where the baseline had none qualifies whatever the multiple.</summary>
    public const int FailureRateMultiple = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureRateMultiple;

    /// <summary>A rise in timeouts or errors needs at least this many such runs in the recent window (#5506), so one stray
    /// timeout on a quiet query does not flag it.</summary>
    public const int FailureMinRecentRuns = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureMinRecentRuns;

    /// <summary>A timeouts-only or errors-only row is HIGH when its rate is at least this many times the baseline's (or the
    /// baseline had none), MEDIUM otherwise (#5506). Cost rows keep the duration bands; a failure row never gets CRITICAL,
    /// which stays the band of a duration more than doubled.</summary>
    public const int FailureHighMultiple = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureHighMultiple;

    /// <summary>Rows with no cost rise (timeouts or errors only) may take at most this share of the row cap, as
    /// <c>cap / FailureShareDivisor</c>, so a flood of error rows cannot push every cost row past the cap (#5506). Cost rows
    /// take the rest, and either side takes what the other leaves free.</summary>
    public const int FailureShareDivisor = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureShareDivisor;

    /// <summary>The partial index predicate and the read's predicate for the rows that are not Regular (#5506). The same text
    /// in both places, so the planner can match the read to the background partial index
    /// (<see cref="QueryStoreBackgroundIndexes.WideNonRegularFirstExec"/>). Without the index the read still answers, just
    /// slower. Hand-kept in step with that spec's <c>IndexDefinition</c>; the live test builds the index and checks the plan
    /// uses it.</summary>
    public const string NonRegularPredicate = "execution_type_desc <> 'Regular'";

    /// <summary>
    /// What <see cref="ResolveAsync"/> decided: the run days to read from the daily totals, and the two bounds of the live
    /// half's <c>first_execution_time</c> ranges. <see cref="LowEnd"/>: the live low range is
    /// <c>first_execution_time &lt; LowEnd</c>. <see cref="HighStart"/>: the live high range is
    /// <c>first_execution_time &gt;= HighStart</c>. With no run day both are the window start, so the low range alone is the
    /// whole baseline and the high range is empty.
    /// </summary>
    public sealed record Plan(DateOnly[] RunDays, DateTime LowEnd, DateTime HighStart)
    {
        /// <summary>How many days came from the daily totals.</summary>
        public int BuiltDayCount => RunDays.Length;
    }

    /// <summary>The plan that reads nothing from the daily totals: the whole baseline is live.</summary>
    public static Plan AllLive(DateTime windowStart) =>
        new(Array.Empty<DateOnly>(), DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified), DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));

    /// <summary>
    /// Picks the run days for a baseline of [<paramref name="baselineStart"/>, <paramref name="windowStart"/>) from the built
    /// days of one server. A day is taken only when it is a WHOLE valid build (no watermark: a partial build leaves part of its
    /// day live, and the open days read live) that lies wholly inside the baseline: its midnight is at or after the baseline
    /// start and its next midnight is at or before the window start. The days must be one run of consecutive days, because the
    /// live half's index ranges are the two ends outside it: the LONGEST run is taken (the later one on a tie) and a valid day
    /// beside a stale one is read live. The read runs inside the caller's transaction, under a savepoint it always rolls back
    /// to, so a store without the table (or a failed statement) costs the speedup and leaves the transaction usable; the
    /// answer is then the all-live plan.
    ///
    /// <para><b>Two gates the built days need (#5506, the review of #6260).</b> (1) <b>Pending.</b> A day reads valid
    /// (<c>built_seq = late_seq</c>) even when a late batch for it failed its apply into <c>query_store_interval_latest</c> and
    /// went pending (the trigger bump rolls back with the failed apply), while the wide table already holds those executions.
    /// So the totals would undercount. PLAN_REGRESSION takes built days only after <c>QueryStoreIntervalLatest.ReadsTableAsync</c>
    /// says there is no pending batch (<c>ReadSourceInputsSql</c>); this read asks the same question first, and with any pending
    /// batch for the server it reads every day live. (2) <b>The raw floor.</b> On the raw arm
    /// (<paramref name="clampToRawFloor"/>) the live rows start where raw's oldest chunk does, and the tool's
    /// <c>effective_start</c> says so; a whole day below that floor must not come from the totals, or the baseline would hold
    /// days the notice says were not read. The start is raised to the floor (the same <c>query_store_stats</c> chunk catalog
    /// <c>RawChunkFloorSql</c> reads). The wide arm already passes the gate's clamped start.</para>
    /// </summary>
    public static async Task<Plan> ResolveAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime baselineStart, DateTime windowStart,
        int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken, bool clampToRawFloor = false)
    {
        const string savepoint = "qs_regression_built_days";
        List<PlanRegressionDaily.BuiltDay> built;
        await transaction.SaveAsync(savepoint, cancellationToken);
        try
        {
            bool hasPending;
            bool hasTimescale;
            await using (var inputs = new NpgsqlCommand(QueryStoreIntervalLatest.ReadSourceInputsSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
            {
                inputs.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
                await using var reader = await inputs.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                hasPending = reader.GetBoolean(1);
                hasTimescale = reader.GetBoolean(2);
            }

            if (hasPending)
            {
                logger?.LogDebug("#5506 regressions read: server {ServerId} has a pending interval_latest batch, so its baseline is read live", serverId);
                return AllLive(windowStart);
            }

            if (clampToRawFloor && hasTimescale)
            {
                await using var floors = new NpgsqlCommand(QueryStoreIntervalLatest.RawChunkFloorSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
                if (await floors.ExecuteScalarAsync(cancellationToken) is DateTime rawFloor && rawFloor > baselineStart)
                {
                    baselineStart = rawFloor;
                }
            }

            built = await QueryStoreIntervalLatest.ReadBuiltDaysAsync(
                connection, transaction, serverId, baselineStart.Date, commandTimeoutSeconds, logger,
                ex => ex is OperationCanceledException, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "#5506 regressions read: the built-days gate failed for server {ServerId}; reading the baseline live", serverId);
            return AllLive(windowStart);
        }
        finally
        {
            await transaction.RollbackAsync(savepoint, CancellationToken.None);
        }

        return Choose(built, baselineStart, windowStart);
    }

    /// <summary>The selection of <see cref="ResolveAsync"/>, over the built days it read.</summary>
    internal static Plan Choose(IEnumerable<PlanRegressionDaily.BuiltDay> built, DateTime baselineStart, DateTime windowStart)
    {
        var start = DateTime.SpecifyKind(baselineStart, DateTimeKind.Unspecified);
        var end = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified);
        var days = built
            .Where(b => b.Through is null)
            .Select(b => b.Day)
            .Where(d => d.ToDateTime(TimeOnly.MinValue) >= start && d.ToDateTime(TimeOnly.MinValue).AddDays(1) <= end)
            .Distinct()
            .OrderBy(d => d)
            .ToList();
        if (days.Count == 0)
        {
            return AllLive(windowStart);
        }

        var bestFrom = 0;
        var bestLength = 0;
        var runFrom = 0;
        for (var i = 1; i <= days.Count; i++)
        {
            if (i == days.Count || days[i] != days[i - 1].AddDays(1))
            {
                var length = i - runFrom;
                if (length >= bestLength)
                {
                    bestFrom = runFrom;
                    bestLength = length;
                }

                runFrom = i;
            }
        }

        var run = days.GetRange(bestFrom, bestLength);
        var first = DateTime.SpecifyKind(run[0].ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified);
        var last = DateTime.SpecifyKind(run[^1].ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified);
        /* A row of a day after the run last_execution_time >= (last day + 1), and its first execution is within a day before it
           (BuildDaySql's own bound): first_execution_time >= the last run day's midnight. A row of a day before the run has a
           first execution below the first run day's midnight. */
        return new Plan(run.ToArray(), first, last);
    }

    /// <summary>The tokens of the shared statements: the baseline start parameter, the run days (date[]), and the two bounds
    /// of the live half's <c>first_execution_time</c> ranges (timestamp). Each twin binds its own parameter numbers
    /// (<see cref="Bind"/>), because the MCP read has a row cap at $5 and the viewer's does not.</summary>
    public const string BaselineStartToken = "@@B";
    public const string RunDaysToken = "@@DAYS";
    public const string LowEndToken = "@@LOW";
    public const string HighStartToken = "@@HIGH";

    /// <summary>The row cap: <c>$5</c> in the MCP read (a parameter), the literal 50 in the viewer's (#5506).</summary>
    public const string LimitToken = "@@LIMIT";

    /// <summary>The statement with its tokens bound to the twin's parameter numbers, for example <c>$6</c>, <c>$7</c>,
    /// <c>$8</c>, <c>$9</c>.</summary>
    public static string Bind(string sql, string baselineStart, string runDays, string lowEnd, string highStart, string limit) =>
        sql.Replace(LimitToken, limit, StringComparison.Ordinal)
           .Replace(BaselineStartToken, baselineStart, StringComparison.Ordinal)
           .Replace(RunDaysToken, "(" + runDays + "::date[])", StringComparison.Ordinal)
           .Replace(LowEndToken, "(" + lowEnd + "::timestamp)", StringComparison.Ordinal)
           .Replace(HighStartToken, "(" + highStart + "::timestamp)", StringComparison.Ordinal);

    /// <summary>
    /// The raw read's arms (#5506). ONE dedupe over every Regular snapshot collected from the baseline start to the window end
    /// keeps the latest snapshot per interval (#1841); the baseline and the recent window then split that set by the
    /// interval's <c>last_execution_time</c>, so an interval is on one side only. The baseline's live half drops the run
    /// days. $1 server_id, $2 window start, $3 window end, $4 database filter. It has no <c>first_execution_time</c> index to
    /// skip the run days with, so this arm still reads the whole span; it is the fallback, and the baseline it computes is
    /// the same one.
    /// </summary>
    public const string RawArmsSql = """
        WITH latest AS (
            /* LOAD-BEARING (correctness, not just perf) - #1841. The rows are CUMULATIVE per-interval snapshots and the
               collector re-fetches the OPEN interval every cycle, so the SAME interval is stored repeatedly with a growing
               execution_count. Keep the LATEST snapshot per interval, once, over the whole span, so an interval that crosses
               the window start is the latest snapshot's alone. Every outcome is deduped here (#5506); the Regular cost arms
               below keep Regular only, because the daily totals hold no other kind, and the Aborted and Exception rows
               are their own arm (nonregular_rows). */
            SELECT
                database_name,
                query_id,
                plan_id,
                execution_type_desc,
                query_text,
                execution_count,
                avg_duration_us,
                avg_cpu_time_us,
                last_execution_time,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= @@B
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        deduped_baseline AS (
            SELECT database_name, query_id, plan_id, execution_count, avg_duration_us, avg_cpu_time_us, 1 AS rn
            FROM latest
            WHERE rn = 1
            AND   execution_type_desc = 'Regular'
            AND   last_execution_time >= @@B
            AND   last_execution_time < $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@DAYS))
        ),
        deduped_recent AS (
            SELECT database_name, query_id, plan_id, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time, 1 AS rn
            FROM latest
            WHERE rn = 1
            AND   execution_type_desc = 'Regular'
            AND   last_execution_time >= $2
        ),
        nonregular_rows AS (
            /* #5506: the Aborted and Exception rows, the same dedupe, both sides in one set. is_recent splits them at the
               window start by the interval's last_execution_time, the line the Regular arms use. */
            SELECT database_name, query_id, execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time,
                   (last_execution_time >= $2) AS is_recent
            FROM latest
            WHERE rn = 1
            AND   execution_type_desc <> 'Regular'
            AND   last_execution_time >= @@B
        ),
        """;

    /// <summary>
    /// The interval table's arms (#5506, #5809): <c>query_store_interval_wide</c> holds ONE row per interval, its latest
    /// snapshot, so no dedupe runs. The baseline's live half is two ranges of <c>first_execution_time</c>, the rows before
    /// the run days and the rows from the last run day on (<see cref="Plan"/>), so the heap of the run days is never
    /// touched; the date test inside each range is what decides membership. The recent arm keeps the interval's
    /// <c>last_execution_time</c> at or after the window start, the same line the baseline stops at.
    /// </summary>
    public static readonly string WideArmsSql = $$"""
        WITH deduped_baseline AS (
            SELECT database_name, query_id, plan_id, execution_count, avg_duration_us, avg_cpu_time_us, 1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   execution_type_desc = 'Regular'
            AND   first_execution_time >= @@B - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time < @@LOW
            AND   last_execution_time >= @@B
            AND   last_execution_time < $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@DAYS))
            AND   ($4::text[] IS NULL OR database_name = ANY($4))

            UNION ALL

            SELECT database_name, query_id, plan_id, execution_count, avg_duration_us, avg_cpu_time_us, 1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   execution_type_desc = 'Regular'
            AND   first_execution_time >= @@HIGH
            AND   first_execution_time < $2
            AND   last_execution_time >= @@B
            AND   last_execution_time < $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@DAYS))
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        deduped_recent AS (
            SELECT database_name, query_id, plan_id, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time, 1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   execution_type_desc = 'Regular'
            AND   collection_time >= @@B
            AND   collection_time <= $3
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
            AND   last_execution_time >= $2
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        nonregular_rows AS (
            /* #5506: the Aborted and Exception rows of the baseline and the recent window, read live in one scan. The predicate
               is the partial background index's own (NonRegularPredicate), so on a store that has built it the scan reads only
               those rows; on one that has not, it reads what the Regular arms read, just slower. The daily totals hold no
               such row. is_recent splits at the window start by last_execution_time, as the Regular arms do. */
            SELECT database_name, query_id, execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time,
                   (last_execution_time >= $2) AS is_recent
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   {{NonRegularPredicate}}
            AND   first_execution_time >= @@B - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
            AND   last_execution_time >= @@B
            AND   (last_execution_time < $2 OR collection_time <= $3)
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        """;

    /// <summary>The run days' per-plan totals from <c>plan_regression_daily</c> (#5448), as <c>baseline_parts</c>' first part.
    /// Every day read here is a whole valid build, and the live arm holds every other row of the baseline window, so no
    /// interval is in both.</summary>
    private const string DailyPartSql = """
            SELECT
                d.database_name,
                d.query_id,
                d.plan_id,
                d.execs AS execs,
                d.cpu_us_sum AS cpu_us,
                d.dur_us_sum AS dur_us
            FROM plan_regression_daily AS d
            WHERE d.server_id = $1
            AND   d.day = ANY(@@DAYS)
            AND   ($4::text[] IS NULL OR d.database_name = ANY($4))

            UNION ALL

""";

    private const string LivePartSql = """
            SELECT
                b.database_name,
                b.query_id,
                b.plan_id,
                CAST(b.execution_count AS numeric) AS execs,
                CAST(b.avg_cpu_time_us AS numeric) * b.execution_count AS cpu_us,
                CAST(b.avg_duration_us AS numeric) * b.execution_count AS dur_us
            FROM deduped_baseline AS b
            WHERE b.rn = 1
""";

    /// <summary>
    /// The two aggregates both twins share, after their arms (#5506). <c>baseline_parts</c> is the run days' per-plan totals
    /// from <c>plan_regression_daily</c> (only when <paramref name="useDailyTotals"/>: with no run day the table is not named,
    /// so a store without it still answers) unioned with the live half's weighted totals; <c>baseline_performance</c> and
    /// <c>recent_performance</c> both divide the sum of cost by the sum of executions, the weighted per-execution average
    /// (before this the baseline was an average of interval averages). Cost is CPU and duration only: logical reads are not
    /// read.
    /// </summary>
    public static string AggregatesSql(bool useDailyTotals) =>
        "        baseline_parts AS (\n" + (useDailyTotals ? DailyPartSql : "") + LivePartSql + "        ),\n" + AggregatesTailSql;

    private const string AggregatesTailSql = """
        baseline_performance AS (
            SELECT
                database_name,
                query_id,
                CAST(SUM(dur_us) AS double precision) / NULLIF(SUM(execs), 0) / 1000.0 AS avg_duration_ms,
                CAST(SUM(cpu_us) AS double precision) / NULLIF(SUM(execs), 0) / 1000.0 AS avg_cpu_time_ms,
                CAST(SUM(execs) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count
            FROM baseline_parts
            GROUP BY database_name, query_id
        ),
        recent_performance AS (
            SELECT
                database_name,
                query_id,
                MAX(query_text) AS query_text_sample,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) AS double precision) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_duration_ms,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) AS double precision) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_cpu_time_ms,
                CAST(SUM(execution_count) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count,
                MAX(last_execution_time) AS last_execution_time
            FROM deduped_recent
            WHERE rn = 1
            GROUP BY database_name, query_id
        ),
        failure_performance AS (
            /* #5506: Aborted (timeouts, cancels) and Exception (errors) per query, baseline and recent side by side: the run
               count, and the summed cost the per-execution figures divide (weighted by execution count). */
            SELECT
                database_name,
                query_id,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Aborted'), 0) AS bigint) AS base_aborted_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Aborted') AS double precision) AS base_aborted_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Aborted') AS double precision) AS base_aborted_dur_us,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Aborted'), 0) AS bigint) AS recent_aborted_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Aborted') AS double precision) AS recent_aborted_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Aborted') AS double precision) AS recent_aborted_dur_us,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Exception'), 0) AS bigint) AS base_exception_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Exception') AS double precision) AS base_exception_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Exception') AS double precision) AS base_exception_dur_us,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Exception'), 0) AS bigint) AS recent_exception_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Exception') AS double precision) AS recent_exception_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Exception') AS double precision) AS recent_exception_dur_us,
                MAX(last_execution_time) FILTER (WHERE is_recent) AS recent_last_time
            FROM nonregular_rows
            GROUP BY database_name, query_id
        )
        """;

    /// <summary>
    /// The projection, the flags, the ranking and the cap of both twins (#5506), after the aggregates. Bound with
    /// <see cref="Bind"/>: <c>@@LIMIT</c> is the row cap. A query is a candidate when it has recent Regular runs, or recent
    /// Aborted or Exception runs reaching <see cref="FailureMinRecentRuns"/>; <c>is_cost</c> is Regular CPU up more than
    /// <see cref="CostGatePercent"/>%, <c>is_timeouts</c> and <c>is_errors</c> are that group's recent per-hour rate at least
    /// <see cref="FailureRateMultiple"/> times the baseline's (any at all where the baseline had none) with at least
    /// <see cref="FailureMinRecentRuns"/> recent runs. The per-hour rates divide by each window's own length (the baseline
    /// window is [baseline start, window start), the recent window [window start, window end]).
    ///
    /// <para><b>Severity.</b> A cost row keeps the duration bands (CRITICAL over 100%, HIGH over 50%, MEDIUM over 25%, else LOW).
    /// A failure row is HIGH when its rate is at least <see cref="FailureHighMultiple"/> times the baseline's or the baseline
    /// had none, else MEDIUM; a row with both takes the higher band. A failure never reads CRITICAL: that stays the band of a
    /// query whose duration more than doubled, so a timeout flood cannot outrank the worst cost regressions.</para>
    ///
    /// <para><b>The cap.</b> Cost rows (including rows that also have a failure reason) are ranked by added duration, failure-only
    /// rows by their recent failed runs. Failure-only rows take at most <c>cap / <see cref="FailureShareDivisor"/></c> slots
    /// (rounded down), more only when there are fewer cost rows than the rest of the cap; cost rows take every other slot. So a flood of
    /// error rows cannot push the cost rows past the cap, and a store with no cost regression still shows its failures. Cost rows
    /// come first in the output.</para>
    /// </summary>
    public static readonly string SuffixSql = $$"""
        ,
        windows AS (
            SELECT
                CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamp) - CAST(@@B AS timestamp))) / 3600.0 AS double precision) AS base_hours,
                CAST(EXTRACT(EPOCH FROM (CAST($3 AS timestamp) - CAST($2 AS timestamp))) / 3600.0 AS double precision) AS recent_hours
        ),
        candidate_keys AS (
            SELECT database_name, query_id FROM recent_performance
            UNION
            SELECT database_name, query_id FROM failure_performance
            WHERE recent_aborted_execs >= {{FailureMinRecentRuns}} OR recent_exception_execs >= {{FailureMinRecentRuns}}
        ),
        joined AS (
            SELECT
                k.database_name,
                k.query_id,
                b.avg_duration_ms AS base_dur_ms,
                r.avg_duration_ms AS recent_dur_ms,
                b.avg_cpu_time_ms AS base_cpu_ms,
                r.avg_cpu_time_ms AS recent_cpu_ms,
                COALESCE(b.exec_count, 0) AS base_execs,
                COALESCE(r.exec_count, 0) AS recent_execs,
                COALESCE(b.plan_count, 0) AS base_plans,
                COALESCE(r.plan_count, 0) AS recent_plans,
                r.query_text_sample,
                COALESCE(r.last_execution_time, f.recent_last_time) AS last_execution_time,
                COALESCE(f.base_aborted_execs, 0) AS base_aborted_execs,
                COALESCE(f.recent_aborted_execs, 0) AS recent_aborted_execs,
                f.base_aborted_cpu_us,
                f.recent_aborted_cpu_us,
                f.base_aborted_dur_us,
                f.recent_aborted_dur_us,
                COALESCE(f.base_exception_execs, 0) AS base_exception_execs,
                COALESCE(f.recent_exception_execs, 0) AS recent_exception_execs,
                f.base_exception_cpu_us,
                f.recent_exception_cpu_us,
                f.base_exception_dur_us,
                f.recent_exception_dur_us,
                w.base_hours,
                w.recent_hours
            FROM candidate_keys AS k
            LEFT JOIN recent_performance AS r ON r.database_name = k.database_name AND r.query_id = k.query_id
            LEFT JOIN baseline_performance AS b ON b.database_name = k.database_name AND b.query_id = k.query_id
            LEFT JOIN failure_performance AS f ON f.database_name = k.database_name AND f.query_id = k.query_id
            CROSS JOIN windows AS w
        ),
        flagged AS (
            SELECT
                j.*,
                (j.recent_dur_ms - j.base_dur_ms) * 100.0 / NULLIF(j.base_dur_ms, 0) AS duration_regression_percent,
                (j.recent_cpu_ms - j.base_cpu_ms) * 100.0 / NULLIF(j.base_cpu_ms, 0) AS cpu_regression_percent,
                COALESCE((j.recent_cpu_ms - j.base_cpu_ms) * 100.0 / NULLIF(j.base_cpu_ms, 0) > {{CostGatePercent}}, false) AS is_cost,
                COALESCE(j.recent_aborted_execs >= {{FailureMinRecentRuns}}
                    AND (j.base_aborted_execs = 0
                         OR (j.recent_aborted_execs / NULLIF(j.recent_hours, 0)) >= {{FailureRateMultiple}} * (j.base_aborted_execs / NULLIF(j.base_hours, 0))), false) AS is_timeouts,
                COALESCE(j.recent_exception_execs >= {{FailureMinRecentRuns}}
                    AND (j.base_exception_execs = 0
                         OR (j.recent_exception_execs / NULLIF(j.recent_hours, 0)) >= {{FailureRateMultiple}} * (j.base_exception_execs / NULLIF(j.base_hours, 0))), false) AS is_errors
            FROM joined AS j
        ),
        qualified AS (
            SELECT
                f.*,
                CASE
                    WHEN NOT f.is_cost THEN 0
                    WHEN f.duration_regression_percent > 100 THEN 4
                    WHEN f.duration_regression_percent > 50 THEN 3
                    WHEN f.duration_regression_percent > 25 THEN 2
                    ELSE 1
                END AS cost_rank,
                GREATEST(
                    CASE WHEN NOT f.is_timeouts THEN 0
                         WHEN f.base_aborted_execs = 0
                              OR (f.recent_aborted_execs / NULLIF(f.recent_hours, 0)) >= {{FailureHighMultiple}} * (f.base_aborted_execs / NULLIF(f.base_hours, 0)) THEN 3
                         ELSE 2 END,
                    CASE WHEN NOT f.is_errors THEN 0
                         WHEN f.base_exception_execs = 0
                              OR (f.recent_exception_execs / NULLIF(f.recent_hours, 0)) >= {{FailureHighMultiple}} * (f.base_exception_execs / NULLIF(f.base_hours, 0)) THEN 3
                         ELSE 2 END) AS failure_rank,
                (f.recent_dur_ms - f.base_dur_ms) * f.recent_execs AS additional_duration_ms
            FROM flagged AS f
            WHERE f.is_cost OR f.is_timeouts OR f.is_errors
        ),
        ranked AS (
            SELECT
                q.*,
                ROW_NUMBER() OVER
                (
                    PARTITION BY q.is_cost
                    ORDER BY CASE WHEN q.is_cost THEN COALESCE(q.additional_duration_ms, 0) ELSE q.recent_aborted_execs + q.recent_exception_execs END DESC,
                             q.database_name, q.query_id
                ) AS class_rn,
                COUNT(*) FILTER (WHERE q.is_cost) OVER () AS cost_total,
                COUNT(*) FILTER (WHERE NOT q.is_cost) OVER () AS failure_total,
                COUNT(*) OVER () AS total_count
            FROM qualified AS q
        ),
        slots AS (
            /* Failure-only rows: their share of the cap, or more when the cost rows leave slots free. Cost rows take the rest. */
            SELECT
                GREATEST(LEAST(MAX(failure_total), CAST(@@LIMIT AS integer) / {{FailureShareDivisor}}),
                         LEAST(MAX(failure_total), CAST(@@LIMIT AS integer) - MAX(cost_total))) AS failure_slots
            FROM ranked
        )
        SELECT
            r.database_name,
            r.query_id,
            r.base_dur_ms AS baseline_duration_ms,
            r.recent_dur_ms AS recent_duration_ms,
            r.duration_regression_percent,
            r.base_cpu_ms AS baseline_cpu_ms,
            r.recent_cpu_ms AS recent_cpu_ms,
            r.cpu_regression_percent,
            COALESCE(r.additional_duration_ms, 0) AS additional_duration_ms,
            r.base_execs AS baseline_exec_count,
            r.recent_execs AS recent_exec_count,
            r.base_plans AS baseline_plan_count,
            r.recent_plans AS recent_plan_count,
            CASE GREATEST(r.cost_rank, r.failure_rank)
                WHEN 4 THEN 'CRITICAL'
                WHEN 3 THEN 'HIGH'
                WHEN 2 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS severity,
            /* #2150: text comes from collect.query_store_text now, and this projection's grain is exactly
               that table's key, so it resolves here with a keyed join rather than inside the aggregate.
               The MAX(query_text) sample stays as the fallback: it is where text lived before the
               cutover, and it is what keeps the regression rows readable for existing history. */
            COALESCE(x.query_sql_text, r.query_text_sample) AS query_text_sample,
            r.last_execution_time,
            /* #5506: why the row is here: cost, timeouts, errors, or a combination in that order, comma separated. */
            array_to_string(ARRAY_REMOVE(ARRAY[
                CASE WHEN r.is_cost THEN 'cost' END,
                CASE WHEN r.is_timeouts THEN 'timeouts' END,
                CASE WHEN r.is_errors THEN 'errors' END], NULL), ',') AS regression_reason,
            r.base_aborted_execs AS base_aborted_count,
            r.recent_aborted_execs AS recent_aborted_count,
            r.base_aborted_cpu_us / NULLIF(r.base_aborted_execs, 0) / 1000.0 AS base_aborted_cpu_ms,
            r.recent_aborted_cpu_us / NULLIF(r.recent_aborted_execs, 0) / 1000.0 AS recent_aborted_cpu_ms,
            r.base_aborted_dur_us / NULLIF(r.base_aborted_execs, 0) / 1000.0 AS base_aborted_dur_ms,
            r.recent_aborted_dur_us / NULLIF(r.recent_aborted_execs, 0) / 1000.0 AS recent_aborted_dur_ms,
            r.base_exception_execs AS base_exception_count,
            r.recent_exception_execs AS recent_exception_count,
            r.base_exception_cpu_us / NULLIF(r.base_exception_execs, 0) / 1000.0 AS base_exception_cpu_ms,
            r.recent_exception_cpu_us / NULLIF(r.recent_exception_execs, 0) / 1000.0 AS recent_exception_cpu_ms,
            r.base_exception_dur_us / NULLIF(r.base_exception_execs, 0) / 1000.0 AS base_exception_dur_ms,
            r.recent_exception_dur_us / NULLIF(r.recent_exception_execs, 0) / 1000.0 AS recent_exception_dur_ms,
            /* Executions per hour, so the baseline (7 days) and the recent window (hours_back) compare in one unit. */
            r.base_execs / NULLIF(r.base_hours, 0) AS baseline_regular_per_hour,
            r.recent_execs / NULLIF(r.recent_hours, 0) AS recent_regular_per_hour,
            r.base_aborted_execs / NULLIF(r.base_hours, 0) AS baseline_aborted_per_hour,
            r.recent_aborted_execs / NULLIF(r.recent_hours, 0) AS recent_aborted_per_hour,
            r.base_exception_execs / NULLIF(r.base_hours, 0) AS baseline_exception_per_hour,
            r.recent_exception_execs / NULLIF(r.recent_hours, 0) AS recent_exception_per_hour,
            /* #6128: the rows that passed the flags, before the cap. */
            r.total_count
        FROM ranked AS r
        CROSS JOIN slots AS s
        LEFT JOIN query_store_text AS x
          ON  x.server_id = $1
          AND x.database_name = r.database_name
          AND x.query_id = r.query_id
        WHERE (r.is_cost AND r.class_rn <= CAST(@@LIMIT AS integer) - s.failure_slots)
        OR    (NOT r.is_cost AND r.class_rn <= s.failure_slots)
        ORDER BY r.is_cost DESC,
                 CASE WHEN r.is_cost THEN COALESCE(r.additional_duration_ms, 0) END DESC,
                 r.recent_aborted_execs + r.recent_exception_execs DESC,
                 r.database_name,
                 r.query_id
        LIMIT @@LIMIT
        """;
}

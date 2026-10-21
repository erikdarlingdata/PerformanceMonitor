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
/// </summary>
public static class QueryStoreRegressionBaseline
{
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
    /// </summary>
    public static async Task<Plan> ResolveAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime baselineStart, DateTime windowStart,
        int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken)
    {
        const string savepoint = "qs_regression_built_days";
        List<PlanRegressionDaily.BuiltDay> built;
        await transaction.SaveAsync(savepoint, cancellationToken);
        try
        {
            built = await QueryStoreIntervalLatest.ReadBuiltDaysAsync(
                connection, transaction, serverId, baselineStart.Date, commandTimeoutSeconds, logger,
                ex => ex is OperationCanceledException, cancellationToken);
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

    /// <summary>The statement with its tokens bound to the twin's parameter numbers, for example <c>$6</c>, <c>$7</c>,
    /// <c>$8</c>, <c>$9</c>.</summary>
    public static string Bind(string sql, string baselineStart, string runDays, string lowEnd, string highStart) =>
        sql.Replace(BaselineStartToken, baselineStart, StringComparison.Ordinal)
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
               the window start is the latest snapshot's alone. Regular executions only (#5506): the daily totals hold no
               other kind, and a baseline that counted Aborted runs on some days and not on others would not add up. */
            SELECT
                database_name,
                query_id,
                plan_id,
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
            AND   execution_type_desc = 'Regular'
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        deduped_baseline AS (
            SELECT database_name, query_id, plan_id, execution_count, avg_duration_us, avg_cpu_time_us, 1 AS rn
            FROM latest
            WHERE rn = 1
            AND   last_execution_time >= @@B
            AND   last_execution_time < $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@DAYS))
        ),
        deduped_recent AS (
            SELECT database_name, query_id, plan_id, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time, 1 AS rn
            FROM latest
            WHERE rn = 1
            AND   last_execution_time >= $2
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
        )
        """;
}

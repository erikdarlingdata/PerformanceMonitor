/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SQL and day arithmetic for the per-day per-plan totals of <c>collect.query_store_interval_latest</c>
/// (<c>collect.plan_regression_daily</c>, #5448), which PLAN_REGRESSION reads for CLOSED days so a run no longer
/// re-aggregates 14 days of the interval table. This file is the storage half: the rung that creates the tables and the
/// trigger is V167 (<see cref="PgMigrations"/>), and the builder, the read and the wiring that use these constants
/// follow in their own lanes.
///
/// <para><b>Row day is the day of <c>last_execution_time</c></b> (naive UTC), the column PLAN_REGRESSION's window filters
/// on. A day's row is the plan's totals over the interval rows whose <c>last_execution_time</c> falls in that day, so
/// summing the closed days and the live days reproduces the 14-day aggregate. Gain is about 4-5x fewer blocks read, not
/// 14x: the last three days stay live (see <see cref="ClosedDayLagDays"/>), and they hold about a fifth of the rows.</para>
///
/// <para><b>Closing a day.</b> A day D is closed, and so built and read from the table, once <c>D &lt;= T - 3</c> for
/// today T: one day for D to have passed, <see cref="PlanRegressionSkewMarginDays"/> for a monitored server whose clock
/// runs ahead of the collector's, and one for the longest span of a Query Store interval, which is what lets a row's
/// <c>first_execution_time</c> sit a day before its <c>last_execution_time</c>.</para>
/// </summary>
public static class PlanRegressionDaily
{
    /// <summary>The window PLAN_REGRESSION describes, in days; also how far back the builder fills closed days.</summary>
    public const int WindowDays = 14;

    /// <summary>Days of clock skew between a monitored server and the collector that the closing of a day tolerates.</summary>
    public const int PlanRegressionSkewMarginDays = 1;

    /// <summary>
    /// A day D is closed when <c>D &lt;= T - ClosedDayLagDays</c>: one day past, <see cref="PlanRegressionSkewMarginDays"/>
    /// of skew, and one day for an interval's span. The three newest days always come from the live table.
    /// </summary>
    public const int ClosedDayLagDays = 1 + PlanRegressionSkewMarginDays + 1;

    /// <summary>How far back (in days) V167's trigger marks a day stale: a row older than this can never be read, because
    /// the read's window is <see cref="WindowDays"/> and a row's day can be a day after the day of its first execution.</summary>
    public const int MarkLateWindowDays = WindowDays + 2;

    /// <summary>
    /// Builds one closed day of one server's totals: the plan-level aggregate PLAN_REGRESSION's
    /// <c>plan_agg</c> step computes, summed rather than divided so days recombine exactly. $1 server_id (integer),
    /// $2 the day D (date), $3 D + 1 day (timestamp), $4 D - 1 day (timestamp). Returns the number of rows inserted,
    /// which the builder records as <c>source_rows</c>. The caller deletes the day's rows first, in the same
    /// transaction (the unique index would refuse a second build of a day).
    ///
    /// <para><b>The bounds are the day's own, and the parity argument is in two parts.</b> A row belongs to D when its
    /// <c>last_execution_time</c> is in <c>[D, D + 1 day)</c>, which is the filter the read applies to a window whose edge
    /// is day-aligned. Its <c>first_execution_time</c> is then in <c>[D - 1 day, D + 1 day)</c>, because one Query Store
    /// interval spans at most a day (the read's <c>first_execution_time &gt;= $4</c> is implied by its
    /// <c>last_execution_time</c> filter for the same reason, and is kept to bound the table's partitioning column for chunk
    /// exclusion). <c>collection_time &gt;= $4</c> mirrors the read's own collection bound, so the build and the live read
    /// keep the same snapshots. The two routes can differ only for a row whose <c>collection_time</c> is more than a day
    /// before its <c>last_execution_time</c>.</para>
    ///
    /// <para>Every parameter is cast where it is used, so each has one type however many times it appears. The sums are
    /// <c>numeric</c>, so the product of an average and an execution count cannot overflow <c>bigint</c>.</para>
    /// </summary>
    public const string BuildDaySql = @"
WITH built AS
(
    INSERT INTO collect.plan_regression_daily
    (
        server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash,
        execs, cpu_us_sum, dur_us_sum, last_exec, is_forced_plan, force_failure_count
    )
    SELECT
        $1::integer, $2::date, l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash,
        SUM(l.execution_count),
        SUM(l.avg_cpu_time_us::numeric * l.execution_count),
        SUM(l.avg_duration_us::numeric * l.execution_count),
        MAX(l.last_execution_time),
        bool_or(l.is_forced_plan),
        MAX(l.force_failure_count)
    FROM collect.query_store_interval_latest AS l
    WHERE l.server_id = $1::integer
    AND   l.first_execution_time >= $4::timestamp
    AND   l.first_execution_time < $3::timestamp
    AND   l.last_execution_time >= $2::date
    AND   l.last_execution_time < $3::timestamp
    AND   l.collection_time >= $4::timestamp
    GROUP BY l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash
    RETURNING 1
)
SELECT count(*)::bigint FROM built;";
}

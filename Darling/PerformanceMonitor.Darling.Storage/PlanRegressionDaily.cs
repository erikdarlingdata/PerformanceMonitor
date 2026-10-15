/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SQL and day arithmetic for the per-day per-plan totals of <c>collect.query_store_interval_latest</c>
/// (<c>collect.plan_regression_daily</c>, #5448), which PLAN_REGRESSION reads for its built days so a run no longer
/// re-aggregates 14 days of the interval table. Since #5812 every day of the window is built, the open ones as partial days
/// (their rows below a watermark), so the live half of the read covers only the rows after each day's watermark. This file is the storage half: the rung that creates the tables and the
/// trigger is V168 (<see cref="PgMigrations"/>), the builder is the hourly-tick tenant at the bottom of this file
/// (<see cref="RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/>), and the read is <c>PgFactCollector.PlanRegressionDailySql</c> in the Analysis project.
///
/// <para><b>Row day is the day of <c>last_execution_time</c></b> (naive UTC), the column PLAN_REGRESSION's window filters
/// on. A day's row is the plan's totals over the interval rows whose <c>last_execution_time</c> falls in that day, so
/// summing the built days and the live rows reproduces the 14-day aggregate.</para>
///
/// <para><b>Whole and partial builds (#5812).</b> A whole build covers every row of its day and is valid while
/// <c>built_seq = late_seq</c>. A partial build covers the day's rows whose <c>first_execution_time</c> is below
/// <c>built_through</c> and is valid while <c>partial_seq = late_seq</c>; it leaves <c>built_seq</c> NULL, so code from
/// before V184, which reads only <c>built_seq = late_seq</c>, reads a partial day live and never loses its newest rows. A
/// late row (the trigger) moves <c>late_seq</c>, which invalidates either kind until the day is built again.</para>
/// </summary>
public static class PlanRegressionDaily
{
    /// <summary>The window PLAN_REGRESSION describes, in days; also how far back the builder fills closed days.</summary>
    public const int WindowDays = 14;

    /// <summary>Days of clock skew between a monitored server and the collector that the closing of a day tolerates.</summary>
    public const int PlanRegressionSkewMarginDays = 1;

    /// <summary>
    /// A test pin only since #5812: before it, a day D was built only once <c>D &lt;= T - ClosedDayLagDays</c> (one day past,
    /// <see cref="PlanRegressionSkewMarginDays"/> of skew, one day for an interval's span) and the three newest days came from
    /// the live table. The builder now builds every day of the window, the open ones as partial days, and no code reads this.
    /// </summary>
    public const int ClosedDayLagDays = 1 + PlanRegressionSkewMarginDays + 1;

    /// <summary>How many days back the hourly cleanup keeps built days: a day older than this can never be read, because
    /// the read's window is <see cref="WindowDays"/> and a row's day can be a day after the day of its first execution.
    /// V168's trigger clamps on the whole day 17 days back, one day wider, so it can mark every day this keeps (a late row
    /// for T-16 has its first execution on T-17).</summary>
    public const int MarkLateWindowDays = WindowDays + 2;

    /// <summary>The open days (newer than <see cref="ClosedDayLagDays"/>) are built too, as partial days (#5812): a build covers
    /// the rows whose <c>first_execution_time</c> is below its watermark, <c>built_through</c>, which is this many hours before
    /// the current hour by the store's clock. It sits one hour under the bound of V184's trigger (one hour before the current
    /// hour), so a write whose transaction began up to an hour before a build still marks the day it changes, and the
    /// interval that has just ended, which the collector still writes, stays above the watermark and in the live half.</summary>
    public const int PartialWatermarkLagHours = 2;

    /// <summary>The late-row trigger's upper bound, in hours before the current hour by the store's clock (V184): a write whose
    /// <c>first_execution_time</c> is below it marks its day and the next. V184's SQL interpolates this constant, and
    /// <see cref="PartialWatermarkLagHours"/> must stay at least one hour more (a pinned pair, #5812): with the watermark at or
    /// above the trigger's bound, a write below the watermark could land without marking its day, and its row would then be
    /// in neither the totals nor the live half.</summary>
    public const int LateTriggerLagHours = 1;

    /// <summary>The newest day the builder builds, in days after today: tomorrow, so a row of a server whose clock runs ahead,
    /// dated tomorrow with a first execution below the watermark, is in a built day and the live half of the read can start at
    /// the watermark (#5812). An interval spans at most a day, so no row dated later can start below it.</summary>
    public const int NewestBuiltDayAhead = 1;

    /// <summary>
    /// Builds one day of one server's totals, whole or partial (#5812): the plan-level aggregate PLAN_REGRESSION's
    /// <c>plan_agg</c> step computes, summed rather than divided so days recombine exactly. $1 server_id (integer),
    /// $2 the day D (date), $3 D + 1 day (timestamp), $4 D - 1 day (timestamp), $5 the watermark (timestamp, the
    /// <c>built_through</c> the build is stamped with: D + 1 day for a whole day, less for a partial one, #5812). Returns the number of rows inserted,
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
        execs, cpu_us_sum, dur_us_sum, last_exec, is_forced_plan, force_failure_count, rowcount_sum
    )
    SELECT
        $1::integer, $2::date, l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash,
        SUM(l.execution_count),
        SUM(l.avg_cpu_time_us::numeric * l.execution_count),
        SUM(l.avg_duration_us::numeric * l.execution_count),
        MAX(l.last_execution_time),
        bool_or(l.is_forced_plan),
        MAX(l.force_failure_count),
        -- #5631: NULL when any interval of the day has no avg_rowcount, so a missing count never reads as a small one. COALESCE: a day whose execution counts are all NULL is 0, not NULL, so the daily read's COUNT(rowcount_sum) guard does not blank the whole plan.
        CASE WHEN COUNT(*) = COUNT(l.avg_rowcount) THEN COALESCE(SUM(l.avg_rowcount::numeric * l.execution_count), 0) END
    FROM collect.query_store_interval_latest AS l
    WHERE l.server_id = $1::integer
    AND   l.first_execution_time >= $4::timestamp
    AND   l.first_execution_time < $3::timestamp
    AND   l.first_execution_time < $5::timestamp
    AND   l.last_execution_time >= $2::date
    AND   l.last_execution_time < $3::timestamp
    AND   l.collection_time >= $4::timestamp
    GROUP BY l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash
    RETURNING 1
)
SELECT count(*)::bigint FROM built;";

    /// <summary>The most builds of the plan's first queue (days never built, or whose <c>late_seq</c> moved) one tick runs. The
    /// first fill of a store is 15 builds per server (T-14 through today), so a store with many servers takes several ticks;
    /// until it is done a read takes the days not yet built live. The second queue, partial days that advance, is capped
    /// separately at one build per server (see <see cref="PlanSql"/>), so the open days never use up this one (#5812).</summary>
    public const int MaxBuildsPerTick = 120;

    /// <summary>The wall-clock budget of one tick: no new build starts once this much time has passed since the tick
    /// began. A build already running finishes (or hits its own timeout), so a tick can overrun by one build.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(10);

    /// <summary>The server-side <c>statement_timeout</c> each build's transaction runs under.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    /// <summary>The client-side deadline for each command: just above the server-side one, so the store's own cancel is the
    /// one that fires.</summary>
    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>Makes sure the day has a built row, so the tick's transaction has a row to read <c>late_seq</c> from and to
    /// stamp. The trigger may have created it already (a late row marks its days), and then this does nothing. $1 server_id,
    /// $2 day.</summary>
    public const string EnsureBuiltSql = @"
INSERT INTO collect.plan_regression_daily_built (server_id, day)
VALUES ($1::integer, $2::date)
ON CONFLICT (server_id, day) DO NOTHING;";

    /// <summary>The day's <c>late_seq</c> as the builder sees it before it aggregates: the value the build is stamped with
    /// (<c>built_seq</c>), so a late apply that lands during the build leaves the day invalid and it is rebuilt, and the build's
    /// watermark (#5812): D + 1 day, or <see cref="PartialWatermarkLagHours"/> before the current hour by the store's own
    /// clock when that is earlier, so the trigger's bound and this one read one clock. $1 server_id, $2 day.</summary>
    public static readonly string SeqSql = $@"
SELECT late_seq,
       LEAST(($2::date + 1)::timestamp, date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '{PartialWatermarkLagHours} hours')
FROM collect.plan_regression_daily_built
WHERE server_id = $1::integer
AND   day = $2::date;";

    /// <summary>Stamps a finished build. $1 server_id, $2 day, $3 the <c>late_seq</c> read before the build, $4 built_at, $5 the
    /// rows inserted (<c>source_rows</c>), $6 the watermark the build covered. A whole build (its watermark at the next
    /// midnight) stamps <c>built_seq</c> and clears <c>partial_seq</c> and <c>built_through</c>; a partial one stamps
    /// <c>partial_seq</c> and <c>built_through</c> and clears <c>built_seq</c> (#5812), so code from before V184, which takes
    /// a day as built whenever <c>built_seq = late_seq</c>, reads a partial day live instead of as a whole day.</summary>
    public const string MarkBuiltSql = @"
UPDATE collect.plan_regression_daily_built
SET built_seq = CASE WHEN $6::timestamp >= ($2::date + 1)::timestamp THEN $3::bigint END,
    partial_seq = CASE WHEN $6::timestamp >= ($2::date + 1)::timestamp THEN NULL ELSE $3::bigint END,
    built_through = CASE WHEN $6::timestamp >= ($2::date + 1)::timestamp THEN NULL ELSE $6::timestamp END,
    built_at = $4::timestamp,
    source_rows = $5::bigint
WHERE server_id = $1::integer
AND   day = $2::date;";

    /// <summary>Clears one (server, day) of totals before it is rebuilt: <see cref="BuildDaySql"/> inserts, and the unique
    /// index would refuse a second build. $1 server_id, $2 day.</summary>
    public const string DeleteDaySql = @"
DELETE FROM collect.plan_regression_daily
WHERE server_id = $1::integer
AND   day = $2::date;";

    /// <summary>One builder per server at a time: a transaction-scoped advisory lock keyed by the server. False means
    /// another builder (a second service, or a tick that overran) holds it, and this one skips the item. $1 server_id.</summary>
    public const string LockSql = @"
SELECT pg_try_advisory_xact_lock(hashtextextended('plan_regression_daily:' || $1::integer::text, 0));";

    /// <summary>
    /// The built days of one server that the PLAN_REGRESSION read may take its totals from (#5448). $1 server_id, $2 M (the
    /// window's day-aligned start). A day is valid when its build saw every late row (<c>built_seq = late_seq</c> for a whole
    /// build, <c>partial_seq = late_seq</c> for a partial one, #5812) and the
    /// interval table's coverage claim reaches the whole day, which is <c>day &gt;= filled_since::date + 1</c>: the first day
    /// after the one <c>filled_since</c> falls in, because that day may be only partly covered. The fact runs this first, in
    /// the same REPEATABLE READ transaction as the daily read, so both see one snapshot of the built days and of the totals;
    /// an empty or failed answer sends the read to the exact-bound statement unchanged. The second column is the day's
    /// watermark (#5812): NULL for a whole build, whatever <c>built_through</c> holds (a builder from before V184 stamps
    /// <c>built_seq</c> and leaves <c>built_through</c> as it was), and <c>built_through</c> for a valid partial build.
    /// </summary>
    public const string BuiltDaysSql = @"
SELECT b.day, CASE WHEN b.built_seq = b.late_seq THEN NULL ELSE b.built_through END
FROM collect.plan_regression_daily_built AS b
JOIN collect.query_store_interval_latest_coverage AS c
  ON c.server_id = b.server_id
WHERE b.server_id = $1::integer
AND   b.day >= $2::date
AND   (b.built_seq = b.late_seq OR (b.partial_seq = b.late_seq AND b.built_through IS NOT NULL))
AND   b.day >= c.filled_since::date + 1
ORDER BY b.day;";

    /// <summary>The servers the builder works for: the enabled ones with an interval coverage claim and no pending batch (a
    /// pending batch means some raw rows are not yet applied, so the day's totals could not be trusted). The first CTE of
    /// <see cref="PlanSql"/>, <see cref="AdvanceIdleSql"/> and <see cref="StampTomorrowSql"/>.</summary>
    private const string BuilderServersSql = @"
    SELECT c.server_id, c.filled_since
    FROM collect.query_store_interval_latest_coverage AS c
    JOIN collect.servers AS sv ON sv.server_id = c.server_id
    WHERE sv.is_enabled
    AND   NOT EXISTS (SELECT 1 FROM collect.query_store_interval_latest_pending AS p WHERE p.server_id = c.server_id)";

    /// <summary>An EXISTS over the interval rows of server <paramref name="server"/> dated <paramref name="day"/> (by
    /// <c>last_execution_time</c>) whose first execution is in [<paramref name="from"/>, <paramref name="below"/>): the rows a
    /// build of that day would add. A probe of the (server, first execution) index over that span only.</summary>
    private static string RowsInSpan(string server, string day, string from, string below) => $@"EXISTS (SELECT 1 FROM collect.query_store_interval_latest AS l
                   WHERE l.server_id = {server}
                   AND   l.first_execution_time >= {from}
                   AND   l.first_execution_time < {below}
                   AND   l.last_execution_time >= {day}
                   AND   l.last_execution_time < ({day} + 1)::timestamp)";

    /// <summary>
    /// The builds due now, in two queues (#5812). $1 now (timestamp), $2 the first queue's cap. A server's days start the day
    /// after its <c>filled_since</c>, since the table cannot speak for earlier days, and run from <see cref="WindowDays"/> back
    /// through <see cref="NewestBuiltDayAhead"/> ahead.
    ///
    /// <para><b>Queue 1, the days with no valid build</b> (never built, or their <c>late_seq</c> moved since the stamp), any day
    /// of the series, newest day first, then by server, at most $2. A store that is behind after a migration or an outage gets
    /// back the days nearest the window's open end first. Tomorrow is in it only when a row dated tomorrow starts below the
    /// watermark: its empty stamp is <see cref="StampTomorrowSql"/>'s, which is not a build.</para>
    ///
    /// <para><b>Queue 2, the valid partial days whose watermark can advance</b> and that have rows to add: a row of the day
    /// whose first execution is in [<c>built_through</c>, the watermark a build would stamp now). An idle server's partial day
    /// is advanced by <see cref="AdvanceIdleSql"/> instead, without a build. Oldest build first (<c>built_at</c>), which is
    /// the round-robin, at most one per server: the open days never use up queue 1's cap, and a fleet larger than the cap
    /// still gets every server's today advanced each tick, under <see cref="MaxTickDuration"/>.</para>
    /// </summary>
    public static readonly string PlanSql = $@"
WITH s AS
(
{BuilderServersSql}
),
days AS
(
    SELECT s.server_id, d.day, b.built_at, b.built_through,
           COALESCE(b.built_seq = b.late_seq, false) AS is_whole,
           COALESCE(b.partial_seq = b.late_seq AND b.built_through IS NOT NULL, false) AS is_partial,
           LEAST((d.day + 1)::timestamp, date_trunc('hour', $1::timestamp) - interval '{PartialWatermarkLagHours} hours') AS through_now
    FROM s
    CROSS JOIN LATERAL
    (
        SELECT g::date AS day
        FROM generate_series(($1::timestamp::date - {WindowDays})::timestamp, ($1::timestamp::date + {NewestBuiltDayAhead})::timestamp, interval '1 day') AS g
    ) AS d
    LEFT JOIN collect.plan_regression_daily_built AS b
      ON b.server_id = s.server_id AND b.day = d.day
    WHERE d.day >= s.filled_since::date + 1
),
due AS
(
    SELECT x.server_id, x.day, 1 AS queue, row_number() OVER (ORDER BY x.day DESC, x.server_id) AS slot
    FROM days AS x
    WHERE NOT x.is_whole
    AND   NOT x.is_partial
    AND   CASE WHEN x.day <= $1::timestamp::date THEN true
               ELSE {RowsInSpan("x.server_id", "x.day", "(x.day - 1)::timestamp", "x.through_now")} END

    UNION ALL

    SELECT x.server_id, x.day, 2 AS queue, row_number() OVER (ORDER BY x.built_at, x.server_id, x.day) AS slot
    FROM days AS x
    WHERE NOT x.is_whole
    AND   x.is_partial
    AND   x.built_through < x.through_now
    AND   {RowsInSpan("x.server_id", "x.day", "x.built_through", "x.through_now")}
)
SELECT due.server_id, due.day
FROM due
WHERE due.slot <= CASE WHEN due.queue = 1 THEN $2::integer ELSE (SELECT count(*) FROM s) END
ORDER BY due.queue, due.slot;";

    /// <summary>
    /// Advances the watermark of every valid partial day that has nothing to add, without a build (#5812): no row of the day
    /// has its first execution in [<c>built_through</c>, the watermark a build would stamp now), so the totals already cover
    /// every row below the new watermark. A day whose new watermark is its next midnight is stamped whole. Without this, a
    /// server that goes idle in the evening would leave that day partial for good, and its old watermark would hold the
    /// read's live floor down for the whole window. The watermark is read from the store's clock, as <see cref="SeqSql"/>
    /// reads it, so it sits <see cref="PartialWatermarkLagHours"/> - <see cref="LateTriggerLagHours"/> hours below the
    /// trigger's bound: a row written into the span after this statement's snapshot is below that bound, so its trigger moves
    /// the day's <c>late_seq</c>, and the row either fails <c>partial_seq = late_seq</c> on the recheck of the locked row or
    /// leaves the stamped day stale. Returns the days advanced. No parameters.
    /// </summary>
    public static readonly string AdvanceIdleSql = $@"
WITH s AS
(
{BuilderServersSql}
),
w AS
(
    SELECT date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '{PartialWatermarkLagHours} hours' AS w
)
UPDATE collect.plan_regression_daily_built AS b
SET built_seq = CASE WHEN w.w >= (b.day + 1)::timestamp THEN b.partial_seq END,
    partial_seq = CASE WHEN w.w >= (b.day + 1)::timestamp THEN NULL ELSE b.partial_seq END,
    built_through = CASE WHEN w.w >= (b.day + 1)::timestamp THEN NULL ELSE w.w END
FROM s, w
WHERE b.server_id = s.server_id
AND   b.partial_seq = b.late_seq
AND   b.built_seq IS DISTINCT FROM b.late_seq
AND   b.built_through < LEAST((b.day + 1)::timestamp, w.w)
AND   NOT {RowsInSpan("b.server_id", "b.day", "b.built_through", "LEAST((b.day + 1)::timestamp, w.w)")};";

    /// <summary>
    /// Stamps tomorrow (<see cref="NewestBuiltDayAhead"/>) built through the current watermark with no totals, without a
    /// build (#5812), for every server whose tomorrow has no valid build, no totals rows, and no row dated tomorrow whose first
    /// execution is below the watermark. Tomorrow almost never has rows, and while it is not built the read's live floor is
    /// today's midnight (the first unbuilt day less one day) instead of today's watermark. The stamp takes the
    /// <c>late_seq</c> this statement's snapshot saw, and the conflict arm writes only while the row still holds it, so a
    /// row written after the snapshot (whose trigger moves <c>late_seq</c>) leaves tomorrow unstamped. Nor does it overwrite a build another
    /// builder committed after the snapshot: the conflict arm writes only while the row still holds no valid build. $1 now (timestamp).
    /// Returns the days stamped.
    /// </summary>
    public static readonly string StampTomorrowSql = $@"
WITH s AS
(
{BuilderServersSql}
),
t AS
(
    SELECT ($1::timestamp::date + {NewestBuiltDayAhead}) AS day,
           LEAST(($1::timestamp::date + {NewestBuiltDayAhead} + 1)::timestamp,
                 date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '{PartialWatermarkLagHours} hours') AS w
)
INSERT INTO collect.plan_regression_daily_built AS b
    (server_id, day, late_seq, built_seq, partial_seq, built_through, built_at, source_rows)
SELECT s.server_id, t.day, COALESCE(o.late_seq, 0),
       CASE WHEN t.w >= (t.day + 1)::timestamp THEN COALESCE(o.late_seq, 0) END,
       CASE WHEN t.w >= (t.day + 1)::timestamp THEN NULL ELSE COALESCE(o.late_seq, 0) END,
       CASE WHEN t.w >= (t.day + 1)::timestamp THEN NULL ELSE t.w END,
       $1::timestamp, 0
FROM s
CROSS JOIN t
LEFT JOIN collect.plan_regression_daily_built AS o
  ON o.server_id = s.server_id AND o.day = t.day
WHERE t.day >= s.filled_since::date + 1
AND   NOT COALESCE(o.built_seq = o.late_seq OR (o.partial_seq = o.late_seq AND o.built_through IS NOT NULL), false)
AND   NOT EXISTS (SELECT 1 FROM collect.plan_regression_daily AS p WHERE p.server_id = s.server_id AND p.day = t.day)
AND   NOT {RowsInSpan("s.server_id", "t.day", "(t.day - 1)::timestamp", "t.w")}
ON CONFLICT (server_id, day) DO UPDATE
SET built_seq = EXCLUDED.built_seq,
    partial_seq = EXCLUDED.partial_seq,
    built_through = EXCLUDED.built_through,
    built_at = EXCLUDED.built_at,
    source_rows = 0
WHERE b.late_seq = EXCLUDED.late_seq
AND   NOT COALESCE(b.built_seq = b.late_seq OR (b.partial_seq = b.late_seq AND b.built_through IS NOT NULL), false);";

    /// <summary>
    /// Removes the expired built rows (days older than <see cref="MarkLateWindowDays"/>, and every row of a server that is not
    /// enabled) and returns their keys. $1 now (timestamp). The first of the cleanup's two steps; see
    /// <see cref="GcTotalsSql"/> for the second and for why the cleanup is driven from this small table.
    /// </summary>
    public static readonly string GcBuiltSql = $@"
DELETE FROM collect.plan_regression_daily_built
WHERE day < ($1::timestamp - interval '{MarkLateWindowDays} days')::date
   OR server_id NOT IN (SELECT server_id FROM collect.servers WHERE is_enabled)
RETURNING server_id, day;";

    /// <summary>
    /// Removes one expired (server, day) of totals: $1 server_id, $2 day. The predicate is the two leading columns of
    /// <c>ux_plan_regression_daily</c> with constants, so each is an index probe and reads nothing but that day's rows.
    ///
    /// <para><b>Why two steps, driven from the built table.</b> The built table holds one row per server and day (a few
    /// hundred), the totals table holds every plan of every day. A delete on the totals table with its own
    /// <c>day &lt; X OR server_id NOT IN (...)</c> predicate cannot use the unique index and reads every totals row on every
    /// hourly tick, even when nothing is due. Joining the totals delete to the expired built rows in ONE statement does not
    /// fix that: the planner cannot know how few rows the first delete returns and picks a hash join with a sequential scan
    /// of the totals. So the cleanup deletes the built rows first, then removes each returned key with this statement, in
    /// one transaction. When nothing has expired no totals statement runs at all. This relies on every totals day having
    /// a built row, which <see cref="BuildDayAsync"/> keeps true: the stamp that ends a build throws when the built row is
    /// gone, and the build's transaction rolls its totals back with it.</para>
    /// </summary>
    public const string GcTotalsSql = @"
DELETE FROM collect.plan_regression_daily
WHERE server_id = $1::integer
AND   day = $2::date;";

    /// <summary>One valid built day as the read sees it (#5812): its totals cover the day's rows whose
    /// <c>first_execution_time</c> is below <paramref name="Through"/>. NULL (a build made before V184) is the whole day.</summary>
    public readonly record struct BuiltDay(DateOnly Day, DateTime? Through)
    {
        /// <summary>The watermark with NULL read as the whole day: the next midnight, above every first execution of a row
        /// of the day.</summary>
        public DateTime ThroughOrDayEnd =>
            DateTime.SpecifyKind(Through ?? Day.AddDays(1).ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified);
    }

    /// <summary>One planned build.</summary>
    public readonly record struct Build(int ServerId, DateOnly Day);

    /// <summary>What one tick did. <paramref name="Skipped"/> counts items another builder held the server's lock for.
    /// <paramref name="Advanced"/> counts the partial days whose watermark moved without a build (<see cref="AdvanceIdleSql"/>),
    /// <paramref name="Stamped"/> the tomorrows stamped with no totals (<see cref="StampTomorrowSql"/>).</summary>
    public readonly record struct TickResult(int Built, int Failed, int Skipped, long DaysRemoved, int Deferred = 0, int Advanced = 0, int Stamped = 0);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The builds due at <paramref name="nowUtc"/>: <see cref="PlanSql"/>'s first queue (at most
    /// <see cref="MaxBuildsPerTick"/>), then its second (at most one per server).</summary>
    public static async Task<IReadOnlyList<Build>> PlanBuildsAsync(
        NpgsqlConnection connection, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
        }

        return builds;
    }

    /// <summary>The two stamps that are not builds (#5812), in this order: <see cref="AdvanceIdleSql"/>, then
    /// <see cref="StampTomorrowSql"/>. Each is one statement in its own transaction. Returns how many days each stamped.</summary>
    public static async Task<(int Advanced, int Stamped)> StampWithoutBuildAsync(
        NpgsqlConnection connection, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        int advanced;
        await using (var advance = new NpgsqlCommand(AdvanceIdleSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            advanced = await advance.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var stamp = new NpgsqlCommand(StampTomorrowSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        stamp.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        var stamped = await stamp.ExecuteNonQueryAsync(cancellationToken);
        return (advanced, stamped);
    }

    /// <summary>Removes the expired days and the days of servers that are not enabled from both tables, in ONE transaction
    /// (the totals and their built rows never disagree about which days exist); returns how many built (server, day) rows
    /// went. The built rows go first and name the totals to remove (<see cref="GcTotalsSql"/>).</summary>
    public static async Task<long> GcAsync(NpgsqlConnection connection, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var expired = new List<(int ServerId, DateOnly Day)>();
        await using (var command = new NpgsqlCommand(GcBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                expired.Add((reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
            }
        }

        foreach (var (serverId, day) in expired)
        {
            await using var totals = new NpgsqlCommand(GcTotalsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            AddServerDay(totals, serverId, day);
            await totals.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return expired.Count;
    }

    /// <summary>
    /// Builds one (server, day): makes sure the built row exists, then in ONE transaction takes the server's advisory lock
    /// (false: another builder has it, nothing is written and the result is null), reads <c>late_seq</c> as s0, deletes the
    /// day's totals, inserts them, and stamps <c>built_seq = s0</c>. A late apply that bumps <c>late_seq</c> after s0 was read
    /// leaves the day invalid for a reader (<c>built_seq &lt;&gt; late_seq</c>) and due again, so a race costs one more build,
    /// never a wrong total. Returns the rows inserted, or null when the lock was not free.
    /// </summary>
    /// <param name="afterSeqRead">A test seam, called between reading s0 and aggregating, so a pin can land a late apply at
    /// the point the race matters. Null in the service.</param>
    public static async Task<long?> BuildDayAsync(
        NpgsqlConnection connection, int serverId, DateOnly day, DateTime nowUtc, CancellationToken cancellationToken,
        Func<CancellationToken, Task>? afterSeqRead = null)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using (var ensure = new NpgsqlCommand(EnsureBuiltSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(ensure, serverId, day);
            await ensure.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var @lock = new NpgsqlCommand(LockSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            @lock.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            if (!(bool)(await @lock.ExecuteScalarAsync(cancellationToken))!)
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        long seq;
        DateTime builtThrough;
        await using (var read = new NpgsqlCommand(SeqSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(read, serverId, day);
            await using var seqReader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await seqReader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException(
                    $"The built row for server {serverId} day {day:yyyy-MM-dd} is gone, so the build is rolled back.");
            }

            seq = seqReader.GetInt64(0);
            builtThrough = seqReader.GetDateTime(1);
        }

        if (afterSeqRead is not null)
        {
            await afterSeqRead(cancellationToken);
        }

        await using (var delete = new NpgsqlCommand(DeleteDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(delete, serverId, day);
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        long sourceRows;
        var dayStart = day.ToDateTime(TimeOnly.MinValue);
        await using (var insert = new NpgsqlCommand(BuildDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(dayStart.AddDays(1)) });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(dayStart.AddDays(-1)) });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(builtThrough) });
            sourceRows = Convert.ToInt64(await insert.ExecuteScalarAsync(cancellationToken));
        }

        await using (var mark = new NpgsqlCommand(MarkBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            AddServerDay(mark, serverId, day);
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = seq });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = sourceRows });
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(builtThrough) });

            /* The built row is made before the transaction (see EnsureBuiltSql), so a second service's cleanup can remove it
               in between, for a server that was just disabled. Then the stamp updates nothing, and committing would leave
               totals no built row owns, which the cleanup (driven by built rows) would never remove. Throwing rolls the
               build back; the tick logs a failed build and the next tick plans it again. */
            if (await mark.ExecuteNonQueryAsync(cancellationToken) != 1)
            {
                throw new InvalidOperationException(
                    $"The built row for server {serverId} day {day:yyyy-MM-dd} is gone, so the build is rolled back.");
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return sourceRows;
    }

    private static void AddServerDay(NpgsqlCommand command, int serverId, DateOnly day)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
    }

    /// <summary>
    /// One maintenance tick, the hourly store-maintenance tick's seventh tenant (#5448): remove expired days, advance the idle
    /// partial days and stamp tomorrow without a build (#5812, <see cref="StampWithoutBuildAsync"/>), plan, then build each
    /// item in its own transaction, in the plan's order. A build that fails is logged as a WARNING (naming the server and
    /// day) and the loop goes on: the day stays live for readers and the next tick plans it again. Once
    /// <see cref="MaxTickDuration"/> has passed no further build starts, and the planned items left are reported as deferred.
    /// </summary>
    public static Task<TickResult> RunTickAsync(NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, logger, () => clock.Elapsed, cancellationToken);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, ILogger, CancellationToken)"/> with the elapsed time since
    /// the tick began supplied by <paramref name="elapsed"/>, read before each build starts.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int built = 0, failed = 0, skipped = 0, started = 0;

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var removed = await GcAsync(connection, nowUtc, cancellationToken);
        var (advanced, stamped) = await StampWithoutBuildAsync(connection, nowUtc, cancellationToken);
        var plan = await PlanBuildsAsync(connection, nowUtc, cancellationToken);

        foreach (var item in plan)
        {
            if (elapsed() >= MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                if (await BuildDayAsync(connection, item.ServerId, item.Day, nowUtc, cancellationToken) is null)
                {
                    skipped++;
                }
                else
                {
                    built++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "PLAN_REGRESSION daily totals: building server {ServerId} day {Day:yyyy-MM-dd} failed and is retried next tick (the day stays live for readers): {Message}",
                    item.ServerId, item.Day, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        var result = new TickResult(built, failed, skipped, removed, deferred, advanced, stamped);
        logger.LogInformation("PLAN_REGRESSION daily totals: built {Built}, failed {Failed}, skipped {Skipped}, deferred {Deferred}, advanced {Advanced} and stamped {Stamped} without a build, removed {Removed} day(s), {ElapsedMs} ms",
            built, failed, skipped, deferred, advanced, stamped, removed, (long)elapsed().TotalMilliseconds);
        return result;
    }
}

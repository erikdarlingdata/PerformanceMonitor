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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The closed-day rollups of <c>collect.query_store_interval_wide</c> that the Query Store grid and its slicer read whole UTC days
/// from (#9111), so a long window reads the interval table only for its edges.
///
/// <para><b>Three tables.</b> <c>collect.query_store_grid_daily</c> holds one row per (server, UTC day, database, query, plan, query hash,
/// execution type, replica role), the grid's own group, with every part the grid's columns recombine from: the execution count, the
/// eleven execution-weighted sums (<c>avg_x * execution_count</c>), the eleven minimum and maximum pairs and the plan-state columns
/// under the aggregate the grid applies. <c>collect.query_store_slicer_hourly</c> holds one row per (server, UTC hour, database, query)
/// with the five weighted sums the slicer adds up; the query stays in the key so the slicer's distinct query count is exact for any bin
/// of whole hours. <c>collect.query_store_closed_day_built</c> says which (server, day) both tables hold.</para>
///
/// <para><b>Clutter (#9148, <see cref="ClutterVersion"/>).</b> The same pass also builds two rollups of raw <c>collect.query_store_stats</c>
/// for <c>get_query_store_clutter</c>'s plan-churn arm, placed by <c>collection_time</c> (<see cref="ClutterCreateSql"/>). They share
/// the built row, its claim, its marks and the cleanup: a raw batch written into a closed day marks it
/// (<see cref="TryMarkLateCollectionDayAsync"/>) as an interval row does, and a rebuild redoes all four tables.</para>
///
/// <para><b>Placement.</b> A row belongs to the day and hour of <c>COALESCE(interval_start_time_utc, collection_time)</c>, the
/// expression the grid and the slicer place a row by, stored as naive UTC. A window is <c>[start, end)</c> on that expression, so a
/// whole day inside the window is the same rows in the rollup and in the interval table.</para>
///
/// <para><b>What equal means.</b> The weighted sums are sums of whole numbers held as double precision, in the rollup as in the
/// interval-table read. Below 2^53 such a sum is exact whatever the order it is added in, so a read that takes whole days from the
/// rollup returns the interval-table read's digits. Minimums, maximums, counts and <c>bool_or</c> recombine exactly at any size.</para>
///
/// <para><b>When a day is built.</b> A day is closed once its end plus <see cref="CloseAfter"/> has passed: the longest interval a row
/// can span plus the collector's longest catch-up, so no interval starting in the day is still being written. A closed day is built
/// once, in one transaction that takes the day's built row first, clears the day from both tables and fills them again. The interval
/// table's writer marks a day <c>rebuild_needed</c> when it writes a row into a day that is no longer open
/// (<see cref="MarkLateDaysSql"/>); the reader leaves a marked day to the interval table and the next tick builds it again. The mark
/// and the build take the same built row, so a row written while its day is being built is either read by that build or marks the
/// day after it.</para>
///
/// <para><b>Retention (#9287).</b> The interval table is purged by <c>first_execution_time</c>, which can sit up to
/// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> below a row's own day, so near the retention edge the interval table can already
/// have lost early-keyed rows of a day that is still inside retention. A built day holds what was collected for that day, which is the
/// truer answer, so a built day is kept and served until the whole day is before the retention edge. A tick removes every day wholly
/// before the edge (<see cref="FirstWholeDay"/> is the first day wholly inside it) from both tables and leaves the newest removed day in
/// the built table as <c>removed</c>. That row only moves forward: no day at or before it is built again. A reader takes only days
/// wholly inside retention on the store's clock, so a tick that has not run yet, or a retention that was just lowered, never serves a
/// day past the edge. <b>The exception:</b> an edge day served from its rollup answers as collected, and the same day read from the
/// interval table (the fault fallback, or a day with no rollup) can lack the rows the purge dropped early. A day the purge may already have
/// thinned, by time alone (<see cref="MayBeThinned"/>), is never built, and never rebuilt, from the interval table: a rebuild mark on
/// it is cleared and its rollup stays. The reader takes a server's days only while its newest build is no older than
/// <see cref="FreshFor"/>.</para>
/// </summary>
public static class QueryStoreClosedDayRollup
{
    /// <summary>The migration that creates the four tables.</summary>
    public const int Version = 283;

    /// <summary>The eleven averages the grid weights by execution count; each has an <c>_wsum</c> column in the daily table.</summary>
    internal static readonly string[] WeightedColumns =
    [
        "avg_duration_us", "avg_cpu_time_us", "avg_logical_io_reads", "avg_logical_io_writes", "avg_physical_io_reads", "avg_rowcount",
        "avg_clr_time_us", "avg_tempdb_space_used", "avg_log_bytes_used", "avg_query_max_used_memory", "avg_num_physical_io_reads",
    ];

    /// <summary>The eleven metrics whose <c>min_</c> and <c>max_</c> the grid shows.</summary>
    internal static readonly string[] MinMaxMetrics =
    [
        "duration_us", "cpu_time_us", "logical_io_reads", "logical_io_writes", "physical_io_reads", "clr_time_us", "rowcount",
        "log_bytes_used", "tempdb_space_used", "query_max_used_memory", "num_physical_io_reads",
    ];

    /// <summary>The grid's six group columns, in the daily table's key order.</summary>
    public const string GridKeyColumns = "database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role";

    private static string Lines(IEnumerable<string> parts) => string.Join(",\n    ", parts);

    /// <summary>The daily table's value columns, in one order for the table, the build and the readers.</summary>
    public static readonly string GridValueColumns = string.Join(", ",
        new[] { "module_name", "execution_count" }
            .Concat(WeightedColumns.Select(c => c + "_wsum"))
            .Concat(MinMaxMetrics.SelectMany(m => new[] { "min_" + m, "max_" + m }))
            .Concat(["min_dop", "max_dop", "last_execution_time", "query_plan_hash", "is_forced_plan", "plan_forcing_type", "first_execution_time",
                "plan_type", "force_failure_count", "last_force_failure_reason", "compatibility_level"]));

    /// <summary>
    /// The value columns as read from an interval-table row (alias-free): the same names and types as <see cref="GridValueColumns"/>,
    /// so an interval row and a daily row stack in one <c>UNION ALL</c> and recombine under one set of aggregates.
    /// </summary>
    public static readonly string GridValueColumnsFromInterval = string.Join(", ",
        new[] { "module_name", "execution_count" }
            .Concat(WeightedColumns.Select(c => $"CAST({c} AS double precision) * execution_count AS {c}_wsum"))
            .Concat(MinMaxMetrics.SelectMany(m => new[] { $"CAST(min_{m} AS double precision) AS min_{m}", $"CAST(max_{m} AS double precision) AS max_{m}" }))
            .Concat(["min_dop", "max_dop", "last_execution_time", "query_plan_hash", "is_forced_plan", "plan_forcing_type", "first_execution_time",
                "plan_type", "force_failure_count", "last_force_failure_reason", "compatibility_level"]));

    /// <summary>V283: the four tables, created empty. The service's tick fills them; no data moves in the migration.</summary>
    public static readonly string CreateSql = $@"
/* #9111: the Query Store grid's closed-day rollup. One row per (server, UTC day of the interval's start, the grid's six group
   columns). NULL key columns are stored as they are. */
CREATE TABLE IF NOT EXISTS collect.query_store_grid_daily
(
    server_id integer NOT NULL,
    day_utc date NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    query_hash text,
    execution_type_desc text,
    replica_role text,
    module_name text,
    execution_count bigint,
    {Lines(WeightedColumns.Select(c => c + "_wsum double precision"))},
    {Lines(MinMaxMetrics.SelectMany(m => new[] { $"min_{m} double precision", $"max_{m} double precision" }))},
    min_dop bigint,
    max_dop bigint,
    last_execution_time timestamp,
    query_plan_hash text,
    is_forced_plan boolean,
    plan_forcing_type text,
    first_execution_time timestamp,
    plan_type text,
    force_failure_count bigint,
    last_force_failure_reason text,
    compatibility_level integer
);

/* NULLS NOT DISTINCT: replica_role and the other key columns are NULL for many rows, and two NULLs are one group. Leads with
   (server_id, day_utc), so it also serves a window's range of days and the delete of one day. */
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_grid_daily
ON collect.query_store_grid_daily (server_id, day_utc, {GridKeyColumns})
NULLS NOT DISTINCT;

/* The grid fetches the kept keys' rows by key. */
CREATE INDEX IF NOT EXISTS idx_query_store_grid_daily_query
ON collect.query_store_grid_daily (server_id, database_name, query_id, day_utc);

/* #9111: the slicer's closed-day rollup. One row per (server, UTC hour of the interval's start, database, query). */
CREATE TABLE IF NOT EXISTS collect.query_store_slicer_hourly
(
    server_id integer NOT NULL,
    hour_utc timestamp NOT NULL,
    database_name text,
    query_id bigint,
    cpu_wsum double precision,
    duration_wsum double precision,
    logical_reads_wsum double precision,
    logical_writes_wsum double precision,
    physical_reads_wsum double precision
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_slicer_hourly
ON collect.query_store_slicer_hourly (server_id, hour_utc, database_name, query_id)
NULLS NOT DISTINCT;

/* Which (server, day) both rollups hold. rebuild_needed: a row was written into the day after it was built, so readers leave the day
   to the interval table until it is built again. removed: retention took the day; the newest such row per server stays, and no day
   at or before it is built again. */
CREATE TABLE IF NOT EXISTS collect.query_store_closed_day_built
(
    server_id integer NOT NULL,
    day_utc date NOT NULL,
    built_at timestamp NOT NULL,
    source_rows bigint NOT NULL,
    rebuild_needed boolean NOT NULL DEFAULT false,
    removed boolean NOT NULL DEFAULT false,
    CONSTRAINT pk_query_store_closed_day_built PRIMARY KEY (server_id, day_utc)
);

/* The tick heartbeat: one row per server, written at the start of every tick, built day or not. ticked_at is the instant after which
   the collected rows are the ones no tick has looked at yet (the tick's clock less HeartbeatMargin). A reader takes a server's built days
   only while the heartbeat is no older than HeartbeatFreshFor, which stands in for a writer's mark when the writer is an older service
   that neither ticks nor marks. */
CREATE TABLE IF NOT EXISTS collect.query_store_closed_day_tick
(
    server_id integer NOT NULL PRIMARY KEY,
    ticked_at timestamp NOT NULL
);";

    /// <summary>The migration that adds the Query Store clutter rollups (#9148) to the same closed-day pass.</summary>
    public const int ClutterVersion = 285;

    /// <summary>
    /// #9148: the two closed-day rollups of raw <c>collect.query_store_stats</c> that <c>get_query_store_clutter</c>'s plan-churn arm reads
    /// whole UTC days from. A row belongs to the day of its <c>collection_time</c>, the column the arm's window is on.
    /// <c>query_store_clutter_plan_daily</c> holds one row per (server, day, database, query, plan) with the plan's first and last sighting
    /// that day; <c>query_store_clutter_collections_daily</c> holds one row per (server, day, database) with the number of distinct
    /// <c>collection_time</c> values. A collection belongs to exactly one day, so the daily counts add up to the window's count, and the
    /// minimum of the daily first sightings and the maximum of the daily last sightings are the window's. <c>clutter_built_at</c> on the
    /// built table says the day's clutter rows come from the build stamped <c>built_at</c>: a day built before this rung, or by a build
    /// that does not know these tables, has a different value, so the reader leaves it to the raw table and the next tick builds it again.
    /// Created empty, and idempotent.
    /// </summary>
    public const string ClutterCreateSql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_clutter_plan_daily
(
    server_id integer NOT NULL,
    day_utc date NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    first_seen timestamp NOT NULL,
    last_seen timestamp NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_clutter_plan_daily
ON collect.query_store_clutter_plan_daily (server_id, day_utc, database_name, query_id, plan_id)
NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS collect.query_store_clutter_collections_daily
(
    server_id integer NOT NULL,
    day_utc date NOT NULL,
    database_name text,
    collections bigint NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_clutter_collections_daily
ON collect.query_store_clutter_collections_daily (server_id, day_utc, database_name)
NULLS NOT DISTINCT;

ALTER TABLE collect.query_store_closed_day_built ADD COLUMN IF NOT EXISTS clutter_built_at timestamp;";

    /// <summary>A day is closed once its end plus this has passed: the longest interval a row can span plus the collector's longest
    /// catch-up, so no interval that starts in the day is still being written.</summary>
    public static readonly TimeSpan CloseAfter = QueryStoreTopDaily.FinalBuildAfter;

    /// <summary>
    /// #9287: the first day wholly inside the retention edge (<paramref name="nowUtc"/> less <paramref name="retentionDays"/>): the edge's
    /// own date when the edge is at midnight, the next day otherwise. The rollups keep every day from the edge's date, so the day that
    /// holds the edge is kept but never served. A day before it is wholly past the edge and goes. Retention is the interval table's
    /// (<see cref="QueryStoreIntervalPartitions.WideHorizonDays"/>), and the readers and the tick use the same edge.
    /// </summary>
    public static DateOnly FirstWholeDay(DateTime nowUtc, int retentionDays)
    {
        var edge = nowUtc.AddDays(-retentionDays);
        var date = DateOnly.FromDateTime(edge);
        return edge.TimeOfDay == TimeSpan.Zero ? date : date.AddDays(1);
    }

    /// <summary>#9287: the same day as <see cref="FirstWholeDay"/>, on the store's clock, for a statement. $n is the store's now as a
    /// timestamp and $m the retention days.</summary>
    internal static string FirstWholeDaySql(string now, string retentionDays) =>
        $"(({now} - make_interval(days => {retentionDays})) + interval '1 day' - interval '1 microsecond')::date";

    /// <summary>
    /// #9287: whether the purge of the interval table can already have dropped a row of <paramref name="day"/>, by time alone. The purge
    /// deletes on <c>first_execution_time</c> below its time less the retention, and a row of a day can be keyed up to
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> before the day starts (a row with no interval start is placed by its collection
    /// time), so the earliest the cutoff can reach a row of the day is when the day's start less that margin is below the edge. A built day
    /// that may be thinned is never built again from the interval table, and a never-built one is not built. Retention is the interval
    /// table's (<see cref="QueryStoreIntervalPartitions.WideHorizonDays"/>), the one the tick and the readers both use. No read of the
    /// table is needed.
    /// </summary>
    public static bool MayBeThinned(DateOnly day, DateTime nowUtc, int retentionDays) =>
        day.ToDateTime(TimeOnly.MinValue) - QueryStoreIntervalWide.PurgeEdgeMargin < nowUtc.AddDays(-retentionDays);

    /// <summary>#9148: whether the raw stats table still holds <paramref name="day"/> whole, from its oldest <c>collection_time</c>
    /// (<see cref="StatsFloorSql"/>): the day is whole when it is on or after the first midnight at or after the floor. Clutter days are
    /// keyed by that column and the table is purged on it (whole chunks), so the day has no margin below it. No floor means nothing was purged.</summary>
    public static bool StatsHoldDayWhole(DateTime? floor, DateOnly day)
    {
        if (floor is not { } f)
        {
            return true;
        }

        var firstWhole = DateOnly.FromDateTime(f);
        if (f.TimeOfDay > TimeSpan.Zero)
        {
            firstWhole = firstWhole.AddDays(1);
        }

        return day >= firstWhole;
    }

    /// <summary>A reader takes a server's built days only while the server's newest build is no older than this. A closed day is
    /// built every day, so a healthy tick is always inside it; a tick that stopped is outside it before the purge can reach a day
    /// the cleanup left.</summary>
    public static readonly TimeSpan FreshFor = TimeSpan.FromHours(36);

    /// <summary>The most (server, day) builds one tick plans: high enough that <see cref="MaxTickDuration"/> is the bound that holds.</summary>
    public const int MaxBuildsPerTick = 1000;

    /// <summary>The wall-clock budget of one tick: no new build starts once this much time has passed since the tick began.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(5);

    /// <summary>The server-side <c>statement_timeout</c> each build's transaction runs under.</summary>
    public const int BuildStatementTimeoutSeconds = 60;

    private const int CommandTimeoutSeconds = BuildStatementTimeoutSeconds + 30;

    private static readonly string StatementTimeoutSql = $"SET LOCAL statement_timeout = '{BuildStatementTimeoutSeconds}s'";

    /// <summary>One day's rows of the interval table for one server, by the grid's own predicate: the interval's start inside the day,
    /// with the <c>first_execution_time</c> range the grid's read carries so the day's partitions serve it. $1 server_id, $2 day.</summary>
    private static readonly string DayRowsWhereSql = $@"
    WHERE w.server_id = $1
      AND COALESCE(w.interval_start_time_utc, w.collection_time) >= $2::timestamp
      AND COALESCE(w.interval_start_time_utc, w.collection_time) < $2::timestamp + interval '1 day'
      AND w.first_execution_time >= $2::timestamp - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
      AND w.first_execution_time <= $2::timestamp + interval '1 day' + {QueryStoreIntervalWide.IntervalStartFirstExecMarginSql}";

    /// <summary>Takes the day's built row before anything is read, so a writer marking the same day waits for this build or is read
    /// by it. A removed day stays removed. $1 server_id, $2 day, $3 built_at.</summary>
    public const string ClaimDaySql = @"
INSERT INTO collect.query_store_closed_day_built AS b (server_id, day_utc, built_at, source_rows, rebuild_needed)
VALUES ($1, $2, $3, 0, false)
ON CONFLICT (server_id, day_utc) DO UPDATE
SET built_at = EXCLUDED.built_at,
    rebuild_needed = false
WHERE NOT b.removed
RETURNING 1;";

    /// <summary>Clears one (server, day) of the daily table. $1 server_id, $2 day.</summary>
    public const string DeleteGridDaySql = @"
DELETE FROM collect.query_store_grid_daily
WHERE server_id = $1
  AND day_utc = $2;";

    /// <summary>Clears one (server, day) of the hourly table. $1 server_id, $2 day.</summary>
    public const string DeleteSlicerDaySql = @"
DELETE FROM collect.query_store_slicer_hourly
WHERE server_id = $1
  AND hour_utc >= $2::timestamp
  AND hour_utc < $2::timestamp + interval '1 day';";

    /// <summary>Fills one (server, day) of the daily table and returns the interval rows it read. $1 server_id, $2 day.</summary>
    public static readonly string BuildGridDaySql = $@"
WITH built AS
(
    INSERT INTO collect.query_store_grid_daily (server_id, day_utc, {GridKeyColumns}, {GridValueColumns})
    SELECT
        w.server_id, $2, w.database_name, w.query_id, w.plan_id, w.query_hash, w.execution_type_desc, w.replica_role,
        MAX(w.module_name), CAST(SUM(w.execution_count) AS bigint),
        {Lines(WeightedColumns.Select(c => $"SUM(CAST(w.{c} AS double precision) * w.execution_count)"))},
        {Lines(MinMaxMetrics.SelectMany(m => new[] { $"MIN(CAST(w.min_{m} AS double precision))", $"MAX(CAST(w.max_{m} AS double precision))" }))},
        MIN(w.min_dop), MAX(w.max_dop), MAX(w.last_execution_time), MAX(w.query_plan_hash), bool_or(w.is_forced_plan),
        MAX(w.plan_forcing_type), MIN(w.first_execution_time), MAX(w.plan_type), MAX(w.force_failure_count),
        MAX(w.last_force_failure_reason), MAX(w.compatibility_level)
    FROM collect.query_store_interval_wide AS w{DayRowsWhereSql}
    GROUP BY w.server_id, w.database_name, w.query_id, w.plan_id, w.query_hash, w.execution_type_desc, w.replica_role
    RETURNING 1
)
SELECT
    (SELECT COUNT(*) FROM collect.query_store_interval_wide AS w{DayRowsWhereSql}) AS source_rows,
    (SELECT COUNT(*) FROM built) AS built_rows;";

    /// <summary>Fills one (server, day) of the hourly table. $1 server_id, $2 day. The five sums are the slicer's own products.</summary>
    public static readonly string BuildSlicerDaySql = $@"
INSERT INTO collect.query_store_slicer_hourly
    (server_id, hour_utc, database_name, query_id, cpu_wsum, duration_wsum, logical_reads_wsum, logical_writes_wsum, physical_reads_wsum)
SELECT
    w.server_id, date_trunc('hour', COALESCE(w.interval_start_time_utc, w.collection_time)), w.database_name, w.query_id,
    SUM(CAST(w.avg_cpu_time_us AS double precision) * w.execution_count),
    SUM(CAST(w.avg_duration_us AS double precision) * w.execution_count),
    SUM(CAST(w.avg_logical_io_reads AS double precision) * w.execution_count),
    SUM(CAST(w.avg_logical_io_writes AS double precision) * w.execution_count),
    SUM(CAST(w.avg_physical_io_reads AS double precision) * w.execution_count)
FROM collect.query_store_interval_wide AS w{DayRowsWhereSql}
GROUP BY w.server_id, date_trunc('hour', COALESCE(w.interval_start_time_utc, w.collection_time)), w.database_name, w.query_id;";

    /// <summary>#9148: clears one (server, day) of the clutter plan table. $1 server_id, $2 day.</summary>
    public const string DeleteClutterPlanDaySql = @"
DELETE FROM collect.query_store_clutter_plan_daily
WHERE server_id = $1
  AND day_utc = $2;";

    /// <summary>#9148: clears one (server, day) of the clutter collections table. $1 server_id, $2 day.</summary>
    public const string DeleteClutterCollectionsDaySql = @"
DELETE FROM collect.query_store_clutter_collections_daily
WHERE server_id = $1
  AND day_utc = $2;";

    /// <summary>#9148: fills one (server, day) of the clutter plan table from raw <c>query_store_stats</c>: each plan's first and last
    /// <c>collection_time</c> in the day. The predicate is the clutter read's own columns, so the day is an index-only range. $1 server_id, $2 day.</summary>
    public const string BuildClutterPlanDaySql = @"
INSERT INTO collect.query_store_clutter_plan_daily (server_id, day_utc, database_name, query_id, plan_id, first_seen, last_seen)
SELECT s.server_id, $2, s.database_name, s.query_id, s.plan_id, MIN(s.collection_time), MAX(s.collection_time)
FROM collect.query_store_stats AS s
WHERE s.server_id = $1
  AND s.collection_time >= $2::timestamp
  AND s.collection_time < $2::timestamp + interval '1 day'
GROUP BY s.server_id, s.database_name, s.query_id, s.plan_id;";

    /// <summary>#9148: fills one (server, day) of the clutter collections table: the distinct <c>collection_time</c> values per database
    /// in the day. $1 server_id, $2 day.</summary>
    public const string BuildClutterCollectionsDaySql = @"
INSERT INTO collect.query_store_clutter_collections_daily (server_id, day_utc, database_name, collections)
SELECT c.server_id, $2, c.database_name, COUNT(*)
FROM
(
    SELECT DISTINCT s.server_id, s.database_name, s.collection_time
    FROM collect.query_store_stats AS s
    WHERE s.server_id = $1
      AND s.collection_time >= $2::timestamp
      AND s.collection_time < $2::timestamp + interval '1 day'
) AS c
GROUP BY c.server_id, c.database_name;";

    /// <summary>#9384: removes the day's hourly-fold stamp (<see cref="QueryStoreClutterDayFold"/>). A day the grid builds or removes takes its
    /// stamp from the grid row, and the day is no longer served on the fold's say. $1 server_id, $2 day.</summary>
    public const string DeleteClutterDayStampSql = @"
DELETE FROM collect.query_store_clutter_day_built
WHERE server_id = $1
  AND day_utc = $2;";

    /// <summary>#9148: says the day's clutter rows come from this build. $1 server_id, $2 day.</summary>
    public const string RecordClutterBuiltSql = @"
UPDATE collect.query_store_closed_day_built
SET clutter_built_at = built_at
WHERE server_id = $1
  AND day_utc = $2;";

    /// <summary>#9287: whether the day's clutter rows come from its newest build: its row exists, is not removed, and says so. Read before
    /// <see cref="ClaimDaySql"/> moves <c>built_at</c>. #9384: or the hourly fold stamped the day (<see cref="QueryStoreClutterDayFold"/>), whose
    /// rows are the day's too. A marked stamp does not count: a late row inserts one for a day nothing folded, so it says nothing about the day's rows. $1 server_id, $2 day.</summary>
    public const string HasClutterFromBuildSql = @"
SELECT EXISTS
(
    SELECT 1
    FROM collect.query_store_closed_day_built AS b
    WHERE b.server_id = $1
      AND b.day_utc = $2
      AND NOT b.removed
      AND b.clutter_built_at = b.built_at
)
OR EXISTS
(
    SELECT 1
    FROM collect.query_store_clutter_day_built AS c
    WHERE c.server_id = $1
      AND c.day_utc = $2
      AND NOT c.rebuild_needed
);";

    /// <summary>Records how many interval rows the build read. $1 server_id, $2 day, $3 source_rows.</summary>
    public const string RecordSourceRowsSql = @"
UPDATE collect.query_store_closed_day_built
SET source_rows = $3
WHERE server_id = $1
  AND day_utc = $2;";

    /// <summary>
    /// Marks the days a batch wrote into as needing a rebuild. $1 server_id, $2 the days (a <c>date[]</c>), $3 now. A day with no built
    /// row gets one already marked, so a build of that day running at the same moment cannot record it as built without this row:
    /// the insert waits on the build's own row and then marks it. A removed day stays as it is.
    /// </summary>
    public const string MarkLateDaysSql = @"
INSERT INTO collect.query_store_closed_day_built AS b (server_id, day_utc, built_at, source_rows, rebuild_needed)
SELECT $1, d.day_utc, $3, 0, true
FROM unnest($2::date[]) AS d(day_utc)
ON CONFLICT (server_id, day_utc) DO UPDATE
SET rebuild_needed = true
WHERE NOT b.rebuild_needed
  AND NOT b.removed;";

    /// <summary>
    /// The days of <paramref name="writtenDays"/> that can already be built at <paramref name="nowUtc"/>: a day is built no sooner
    /// than <see cref="CloseAfter"/> (more than a day) after it ends, so today and yesterday are never among them and an ordinary
    /// batch marks nothing.
    /// </summary>
    public static DateOnly[] LateDays(IEnumerable<DateOnly> writtenDays, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(writtenDays);
        var yesterday = DateOnly.FromDateTime(nowUtc).AddDays(-1);
        return writtenDays.Where(d => d < yesterday).Distinct().ToArray();
    }

    /// <summary>Runs <see cref="MarkLateDaysSql"/> for the late days of a batch, in the batch's own transaction.</summary>
    public static async Task MarkLateDaysAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateOnly[] lateDays, DateTime nowUtc, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(lateDays);
        if (lateDays.Length == 0)
        {
            return;
        }

        await using var command = new NpgsqlCommand(MarkLateDaysSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = lateDays });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The savepoint <see cref="TryMarkLateCollectionDayAsync"/> runs behind.</summary>
    public const string LateCollectionDaySavepointName = "clutter_late_day";

    /// <summary>
    /// #9148: marks the day of a raw <c>query_store_stats</c> batch's <c>collection_time</c> for a rebuild when that day may already be
    /// built, in the batch's own transaction, so the clutter rollups do not serve a day without the rows the batch wrote into it. A live
    /// batch is stamped now, so it is never late and sends nothing; a backfill or an import of an older collection marks its day. It runs
    /// behind its own savepoint and never throws for a fault (the batch's rows must commit either way): a failed mark is logged, and the
    /// day keeps its rollup until it is next built. A store below <see cref="ClutterVersion"/> has no rollup to mark. Returns whether it
    /// marked.
    /// </summary>
    public static async Task<bool> TryMarkLateCollectionDayAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime collectionTime, DateTime nowUtc, int commandTimeoutSeconds,
        ILogger? logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        var lateDays = LateDays([DateOnly.FromDateTime(collectionTime)], nowUtc);
        var lateHour = QueryStoreClutterHourly.IsClosed(QueryStoreClutterHourly.FloorHour(collectionTime), nowUtc);
        if (lateDays.Length == 0 && !lateHour)
        {
            return false;
        }

        await transaction.SaveAsync(LateCollectionDaySavepointName, cancellationToken);
        try
        {
            bool hasDaily, hasHourly;
            await using (var probe = new NpgsqlCommand(
                "SELECT to_regclass('collect.query_store_clutter_plan_daily') IS NOT NULL, to_regclass('collect.query_store_clutter_hour_built') IS NOT NULL",
                connection, transaction) { CommandTimeout = commandTimeoutSeconds })
            await using (var probed = await probe.ExecuteReaderAsync(cancellationToken))
            {
                await probed.ReadAsync(cancellationToken);
                hasDaily = probed.GetBoolean(0);
                hasHourly = probed.GetBoolean(1);
            }

            var marked = false;
            if (hasDaily && lateDays.Length > 0)
            {
                await MarkLateDaysAsync(connection, transaction, serverId, lateDays, nowUtc, commandTimeoutSeconds, cancellationToken);
                marked = true;
            }

            /* #9173: the hourly clutter rollups close an hour a grace after it ends, long before a day closes, so a batch can be late for its
               hour while its day is still open. */
            if (hasHourly && lateHour)
            {
                marked |= await QueryStoreClutterHourly.TryMarkLateHourAsync(connection, transaction, serverId, collectionTime, nowUtc, commandTimeoutSeconds, cancellationToken);
            }

            /* #9384: the clutter day is built from its hours soon after midnight, so a late row for a closed hour marks the hour's day too.
               The service migrates the store before any collector write, so the stamp table is there and this does not probe for it. */
            if (lateHour)
            {
                marked |= await QueryStoreClutterDayFold.TryMarkLateDayAsync(connection, transaction, serverId, collectionTime, nowUtc, commandTimeoutSeconds, cancellationToken);
            }

            await transaction.ReleaseAsync(LateCollectionDaySavepointName, cancellationToken);
            return marked;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await transaction.RollbackAsync(LateCollectionDaySavepointName, cancellationToken);
            logger?.LogWarning(ex, "Query Store closed-day rollups: marking server {ServerId} collection {Collection:yyyy-MM-dd HH:mm} for a rebuild failed; its clutter rollups stay as built until the day or hour is next built: {Message}",
                serverId, collectionTime, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// The builds due now, newest day first, so the short windows are served within the first ticks after an upgrade and the older
    /// days follow. $1 now, $2 retention days, $3 <see cref="CloseAfter"/>, $4 the cap. Servers are the
    /// enabled ones with an interval-table coverage claim, and a server's days start the day after its <c>filled_since</c>. Days run
    /// from the first day wholly inside retention (<see cref="FirstWholeDay"/>) through the newest closed day, above the server's newest removed day. A day is due when
    /// it has no built row or its row is marked, or (#9148) its clutter rows do not come from its newest build. <c>clutter_only</c> is true
    /// for a day due for that last reason alone: <see cref="PlanBuildsAsync"/> builds it only when raw holds the day whole.
    /// </summary>
    public static readonly string PlanSql = $@"
SELECT s.server_id, d.day, (b.server_id IS NOT NULL AND NOT b.rebuild_needed) AS clutter_only
FROM
(
    SELECT c.server_id, c.filled_since
    FROM collect.query_store_interval_wide_coverage AS c
    JOIN collect.servers AS sv ON sv.server_id = c.server_id
    WHERE sv.is_enabled
) AS s
CROSS JOIN LATERAL
(
    SELECT g::date AS day
    FROM generate_series(({FirstWholeDaySql("$1", "$2")})::timestamp, (($1 - $3)::date - 1)::timestamp, interval '1 day') AS g
) AS d
LEFT JOIN collect.query_store_closed_day_built AS b
  ON b.server_id = s.server_id AND b.day_utc = d.day
WHERE d.day >= s.filled_since::date + 1
  AND d.day > COALESCE((SELECT max(t.day_utc) FROM collect.query_store_closed_day_built AS t WHERE t.server_id = s.server_id AND t.removed), '-infinity'::date)
  AND (b.server_id IS NULL OR b.rebuild_needed OR b.clutter_built_at IS DISTINCT FROM b.built_at)
ORDER BY d.day DESC, s.server_id
LIMIT $4;";

    /// <summary>Marks every day wholly before the retention edge (#9287) as removed and returns their keys. $1 now, $2 retention days.</summary>
    public const string GcMarkRemovedSql = @"
UPDATE collect.query_store_closed_day_built
SET removed = true,
    rebuild_needed = false
WHERE day_utc < ($1 - make_interval(days => $2))::date
  AND NOT removed
RETURNING server_id, day_utc;";

    /// <summary>Keeps only the newest removed day of each server: it is the line no build goes back across.</summary>
    public const string GcPruneRemovedSql = @"
DELETE FROM collect.query_store_closed_day_built AS b
WHERE b.removed
  AND EXISTS (SELECT 1 FROM collect.query_store_closed_day_built AS n
              WHERE n.server_id = b.server_id AND n.removed AND n.day_utc > b.day_utc);";

    /// <summary>How far before its own clock a tick sets the heartbeat. <c>collection_time</c> is stamped when the collector reads the
    /// snapshot and the row commits after that, so a row collected a moment before a tick can commit after the tick read the day. A
    /// write that takes longer than this to commit is not covered.</summary>
    public static readonly TimeSpan HeartbeatMargin = TimeSpan.FromMinutes(10);

    /// <summary>A reader takes a server's built days only while the server's heartbeat is no older than this (it is set
    /// <see cref="HeartbeatMargin"/> behind the tick's clock, so a tick every hour keeps it well inside). An older service writes no
    /// heartbeat, so under one the reads go to the interval table within this time of the last tick, and a late row it wrote before
    /// then is not seen until that time has passed.</summary>
    public static readonly TimeSpan HeartbeatFreshFor = TimeSpan.FromHours(2);

    /// <summary>The enabled servers with an interval-table coverage claim, each with its heartbeat (null before its first tick); the
    /// servers <see cref="PlanSql"/> plans builds for.</summary>
    public const string HeartbeatServersSql = @"
SELECT c.server_id, t.ticked_at
FROM collect.query_store_interval_wide_coverage AS c
JOIN collect.servers AS sv ON sv.server_id = c.server_id
LEFT JOIN collect.query_store_closed_day_tick AS t ON t.server_id = c.server_id
WHERE sv.is_enabled;";

    /// <summary>
    /// Marks every built day that holds a row collected after the server's last heartbeat as needing a rebuild, and returns how many days it
    /// marked. This is the mark an older service never writes: the first tick after a gap in the heartbeat makes it on that service's
    /// behalf, from the rows themselves. A day with no built row is planned for a build anyway. $1 server_id, $2 the server's last heartbeat.
    /// </summary>
    public const string MarkChangedDaysSql = @"
WITH late AS
(
    SELECT DISTINCT CAST(COALESCE(w.interval_start_time_utc, w.collection_time) AS date) AS day_utc
    FROM collect.query_store_interval_wide AS w
    WHERE w.server_id = $1
      AND w.collection_time > $2
),
marked AS
(
    UPDATE collect.query_store_closed_day_built AS b
    SET rebuild_needed = true
    FROM late AS l
    WHERE b.server_id = $1
      AND b.day_utc = l.day_utc
      AND NOT b.removed
      AND NOT b.rebuild_needed
    RETURNING 1
)
SELECT COUNT(*) FROM marked;";

    /// <summary>Records the heartbeat; it never moves back. $1 server_id, $2 the heartbeat.</summary>
    public const string RecordHeartbeatSql = @"
INSERT INTO collect.query_store_closed_day_tick AS t (server_id, ticked_at)
VALUES ($1, $2)
ON CONFLICT (server_id) DO UPDATE
SET ticked_at = EXCLUDED.ticked_at
WHERE t.ticked_at < EXCLUDED.ticked_at;";

    /// <summary>
    /// Whether the server's heartbeat is missing or older than $2, so the tick has not run lately and the server's built days are
    /// not known to be current. A current service marks a late row's day in the same transaction as the row, and ticks every hour; an
    /// older service does neither, so its heartbeat ages. The test reads one row of the heartbeat table: the interval table has no
    /// <c>collection_time</c> index and is partitioned by <c>first_execution_time</c>, so a read that looked for rows collected since
    /// the last tick would walk every partition of the server. $1 server_id.
    /// </summary>
    public const string StaleHeartbeatSql = @"
SELECT NOT EXISTS
(
    SELECT 1
    FROM collect.query_store_closed_day_tick AS t
    WHERE t.server_id = $1
      AND t.ticked_at >= (now() AT TIME ZONE 'UTC') - $2
);";

    /// <summary>
    /// The built days of one server a reader may take, oldest first. $1 server_id, $2 the first day, $3 the day after the last,
    /// $4 <see cref="FreshFor"/>, $5 the retention days, $6 the store's now (null for its own clock). No row comes back when the server's newest build is older than $4 on the store's
    /// clock. #9287: only days wholly inside retention on the store's clock come back (<see cref="FirstWholeDay"/>), so a day past the
    /// edge that the cleanup has not removed yet is never served, and a day near the edge is served from its rollup as collected.
    /// </summary>
    public static readonly string ServedDaysSql = $@"
SELECT b.day_utc
FROM collect.query_store_closed_day_built AS b
WHERE b.server_id = $1
  AND b.day_utc >= $2
  AND b.day_utc < $3
  AND b.day_utc >= {FirstWholeDaySql("COALESCE($6, now() AT TIME ZONE 'UTC')", "$5")}
  AND NOT b.rebuild_needed
  AND NOT b.removed
  AND (SELECT max(n.built_at) FROM collect.query_store_closed_day_built AS n WHERE n.server_id = $1 AND NOT n.removed)
      >= (now() AT TIME ZONE 'UTC') - $4
ORDER BY b.day_utc;";

    /// <summary>
    /// #9148: <see cref="ServedDaysSql"/> for the clutter rollups: the same days, less any whose clutter rows do not come from the day's
    /// newest build (a day built before <see cref="ClutterVersion"/>, or rebuilt by a build that does not fill the clutter tables).
    /// </summary>
    private static readonly string ServedGridClutterDaysSelect = $@"
SELECT b.day_utc
FROM collect.query_store_closed_day_built AS b
WHERE b.server_id = $1
  AND b.day_utc >= $2
  AND b.day_utc < $3
  AND b.day_utc >= {FirstWholeDaySql("COALESCE($6, now() AT TIME ZONE 'UTC')", "$5")}
  AND NOT b.rebuild_needed
  AND NOT b.removed
  AND b.clutter_built_at = b.built_at
  AND (SELECT max(n.built_at) FROM collect.query_store_closed_day_built AS n WHERE n.server_id = $1 AND NOT n.removed)
      >= (now() AT TIME ZONE 'UTC') - $4";

    /// <summary>#9148: the clutter days the grid built, in order, as <see cref="ServedClutterDaysSql"/> says.</summary>
    public static readonly string ServedClutterDaysSql = ServedGridClutterDaysSelect + @"
ORDER BY day_utc;";

    /// <summary>
    /// #9384: <see cref="ServedClutterDaysSql"/> and the days the hourly fold stamped and no late row has marked since
    /// (<see cref="QueryStoreClutterDayFold"/>), on a store at <see cref="QueryStoreClutterDayFold.Version"/> or above. A folded day needs no grid
    /// build, so it is not held to the grid's newest build. The day heartbeat check <c>ResolveAsync</c> makes after the days are read is the one
    /// that says a service that marks late rows is running.
    /// </summary>
    public static readonly string ServedClutterDaysWithFoldedSql = ServedGridClutterDaysSelect + $@"
UNION
SELECT c.day_utc
FROM collect.query_store_clutter_day_built AS c
WHERE c.server_id = $1
  AND c.day_utc >= $2
  AND c.day_utc < $3
  AND c.day_utc >= {FirstWholeDaySql("COALESCE($6, now() AT TIME ZONE 'UTC')", "$5")}
  AND NOT c.rebuild_needed
ORDER BY day_utc;";

    /// <summary>One planned build.</summary>
    public readonly record struct Build(int ServerId, DateOnly Day);

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(
        int Built, int Failed, long DaysRemoved, int Deferred, int HoursBuilt = 0, int HoursFailed = 0, long HoursRemoved = 0, int HoursDeferred = 0,
        int ClutterDaysFolded = 0, string? PruneFailure = null, int ClutterDaysFailed = 0);

    /// <summary>The whole built UTC days a read takes from the rollups: <c>[Start, End)</c>, both at midnight.</summary>
    public readonly record struct ServedDays(DateTime Start, DateTime End);

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, (DateTime NowUtc, int RetentionDays)> ReadClocks = new();

    /// <summary>
    /// #9287: a test seam. A live test whose seeded days sit in the past, on a store whose clock is the real one, tells the served-day
    /// reads which clock and retention to clip to for the database it runs in, as the tick it ran was given them. No caller in the
    /// service sets it, so a read clips to the store's own clock and <see cref="QueryStoreIntervalPartitions.WideHorizonDays"/>.
    /// </summary>
    internal static void SetReadClockForTests(string database, DateTime nowUtc, int retentionDays) => ReadClocks[database] = (nowUtc, retentionDays);

    private static (DateTime? NowUtc, int RetentionDays) ReadClockFor(NpgsqlConnection connection) =>
        ReadClocks.TryGetValue(connection.Database ?? string.Empty, out var set) ? (set.NowUtc, set.RetentionDays) : (null, QueryStoreIntervalPartitions.WideHorizonDays);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>
    /// The longest unbroken run of built days that lies wholly inside <c>[readStart, end)</c>, or null when there is none. The read
    /// takes that run from the rollups and everything on either side of it (the window's partial first and last days, the open days
    /// and any day that is not built) from the interval table. Run it on the read's own connection and transaction, on a store at
    /// <see cref="Version"/> or above.
    /// </summary>
    public static async Task<ServedDays?> ResolveServedDaysAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime readStart, DateTime end, int commandTimeoutSeconds, CancellationToken cancellationToken) =>
        (await ResolveAsync(ServedDaysSql, connection, transaction, serverId, readStart, end, commandTimeoutSeconds, cancellationToken)).Served;

    /// <summary>
    /// #9287: <see cref="ResolveServedDaysAsync"/> plus the reason no run was taken, so a slow read can say whether it read rollups. The
    /// run is the same one, and the reason is <see cref="SourceReason.Served"/> when there is one. #9363: <c>BuiltDays</c> is every day
    /// the one statement returned (built, current, inside retention), so a built day outside the longest run can be told from one that is
    /// not built, without another read.
    /// </summary>
    public static Task<(ServedDays? Served, SourceReason Reason, IReadOnlyList<DateOnly> BuiltDays)> ResolveServedDaysWithReasonAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime readStart, DateTime end, int commandTimeoutSeconds, CancellationToken cancellationToken) =>
        ResolveAsync(ServedDaysSql, connection, transaction, serverId, readStart, end, commandTimeoutSeconds, cancellationToken);

    /// <summary>
    /// #9148: <see cref="ResolveServedDaysAsync"/> for the clutter rollups (<see cref="ServedClutterDaysSql"/>), on a store at
    /// <see cref="ClutterVersion"/> or above. The run is of whole days inside <c>[readStart, end)</c>. #9384: <paramref name="foldedDays"/> adds the days
    /// the hourly fold built (<see cref="ServedClutterDaysWithFoldedSql"/>), on a store that has its stamp table.
    /// </summary>
    public static async Task<ServedDays?> ResolveServedClutterDaysAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime readStart, DateTime end, int commandTimeoutSeconds, CancellationToken cancellationToken,
        bool foldedDays = false) =>
        (await ResolveAsync(foldedDays ? ServedClutterDaysWithFoldedSql : ServedClutterDaysSql, connection, transaction, serverId, readStart, end, commandTimeoutSeconds, cancellationToken)).Served;

    private static async Task<(ServedDays? Served, SourceReason Reason, IReadOnlyList<DateOnly> BuiltDays)> ResolveAsync(
        string servedSql, NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, DateTime readStart, DateTime end, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var first = DateOnly.FromDateTime(readStart);
        if (readStart.TimeOfDay > TimeSpan.Zero)
        {
            first = first.AddDays(1);
        }

        var afterLast = DateOnly.FromDateTime(end);
        if (afterLast <= first)
        {
            return (null, SourceReason.NoWholeDay, Array.Empty<DateOnly>());
        }

        var days = new List<DateOnly>();
        await using (var command = new NpgsqlCommand(servedSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = first });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = afterLast });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = FreshFor });
            var (clockOverride, retention) = ReadClockFor(connection);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retention });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = clockOverride is { } at ? Unspecified(at) : DBNull.Value });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                days.Add(reader.GetFieldValue<DateOnly>(0));
            }
        }

        var run = LongestRun(days);
        if (run is not { } served)
        {
            return (null, SourceReason.NoBuiltDays, days);
        }

        /* A server whose tick has not run lately is not known to be current: an older service marks nothing, so its late rows are not marked. */
        await using var stale = new NpgsqlCommand(StaleHeartbeatSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        stale.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        stale.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = HeartbeatFreshFor });
        return (bool)(await stale.ExecuteScalarAsync(cancellationToken))! ? (null, SourceReason.HeartbeatStale, days) : (served, SourceReason.Served, days);
    }

    /// <summary>#9287: why a read took no day from the rollups, or <see cref="Served"/> when it took a run.</summary>
    public enum SourceReason
    {
        /// <summary>A run of built days came from the rollups.</summary>
        Served,
        /// <summary>The window holds no whole UTC day, so there was nothing to take.</summary>
        NoWholeDay,
        /// <summary>No run of current built days, or the newest build is older than <see cref="FreshFor"/>.</summary>
        NoBuiltDays,
        /// <summary>Days are built, but the server's heartbeat is older than <see cref="HeartbeatFreshFor"/>.</summary>
        HeartbeatStale,
        /// <summary>The store is below the rollups' version, or the read's bin is not a whole number of hours.</summary>
        Unavailable,
        /// <summary>A read that took rollup days faulted and was run again on the interval table alone.</summary>
        FellBack,
    }

    /// <summary>
    /// #9287: where one read's days came from. <see cref="RollupDays"/> are the UTC days taken from the rollups and
    /// <see cref="IntervalDays"/> every other UTC day the window touches, read from the interval table (a partial first or last day and
    /// the open day count as days). A caller reads the day lists, not the clock, to see whether closed days were read as intervals.
    /// </summary>
    public sealed record ReadSource(
        IReadOnlyList<DateOnly> RollupDays, IReadOnlyList<DateOnly> IntervalDays, SourceReason Reason, bool EdgeDaysAsCollected = false)
    {
        /// <summary>#9287: the shape the interval-table statements took, from <see cref="DescribeShape"/>, or null where the read did not say
        /// (the slicer read). <see cref="Describe"/> appends it, so a slow-load line shows which shape a cold read got.</summary>
        public string? Shape { get; init; }

        /// <summary>#9363: for each of <see cref="IntervalDays"/>, why the interval table served it, or null where the read could not say
        /// (<see cref="DescribeSource(DateTime, DateTime, ServedDays?, SourceReason, bool)"/> was given no clock). Null as a whole lists the days bare.</summary>
        public IReadOnlyList<(DateOnly Day, string? Label)>? IntervalNotes { get; init; }

        /// <summary>The slow-load line's words, for example <c>3 days from rollups (2026-11-12 to 2026-11-14), 7 days from the interval table
        /// (2026-11-11 partial first day, ...)</c>, then the <see cref="Shape"/> when there is one. #9363: the UTC days are named, and each
        /// interval-table day carries the reason it was not served from a rollup.</summary>
        public string Describe()
        {
            static string Days(int n) => n == 1 ? "1 day" : $"{n} days";
            var rollupList = RollupDays.Count == 0 ? string.Empty : $" ({FormatDays(RollupDays.Select(d => (d, (string?)null)))})";
            var intervalList = IntervalDays.Count == 0
                ? string.Empty
                : $" ({FormatDays(IntervalNotes ?? IntervalDays.Select(d => (d, (string?)null)).ToList())})";
            var line = RollupDays.Count == 0
                ? $"{Days(IntervalDays.Count)} from the interval table{intervalList}, no rollup days: {Why()}"
                : $"{Days(RollupDays.Count)} from rollups{rollupList}{(EdgeDaysAsCollected ? EdgeDaysNote : string.Empty)}, {Days(IntervalDays.Count)} from the interval table{intervalList}";
            return Shape is { Length: > 0 } shape ? $"{line}, {shape}" : line;
        }

        /// <summary>#9363: the days oldest first, a run of consecutive days with one label as <c>a to b label</c>, a single day as <c>a label</c>.</summary>
        private static string FormatDays(IEnumerable<(DateOnly Day, string? Label)> days)
        {
            static string Date(DateOnly d) => d.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
            var parts = new List<string>();
            DateOnly? first = null, last = null;
            string? label = null;
            void Flush()
            {
                if (first is not { } f || last is not { } l)
                {
                    return;
                }

                var range = f == l ? Date(f) : $"{Date(f)} to {Date(l)}";
                parts.Add(label is { Length: > 0 } ? $"{range} {label}" : range);
            }

            foreach (var (day, dayLabel) in days.OrderBy(d => d.Day))
            {
                if (last is { } previous && day == previous.AddDays(1) && dayLabel == label)
                {
                    last = day;
                    continue;
                }

                Flush();
                first = last = day;
                label = dayLabel;
            }

            Flush();
            return string.Join(", ", parts);
        }

        /// <summary>The grid banner's note, or null when nothing is worth saying: rollups served, or there was no whole day to take.</summary>
        public string? BannerNote() => Reason switch
        {
            SourceReason.HeartbeatStale or SourceReason.NoBuiltDays or SourceReason.FellBack => $"closed-day rollups not used: {Why()}",
            _ => null,
        };

        internal string Why() => Reason switch
        {
            SourceReason.HeartbeatStale => "the server's heartbeat is older than 2 hours",
            SourceReason.NoBuiltDays => "its closed days are not built yet",
            SourceReason.FellBack => "the rollup read failed",
            SourceReason.NoWholeDay => "no whole day in the window",
            SourceReason.Unavailable => "rollups not available for this read",
            _ => "rollups used",
        };
    }

    /// <summary>#9287: said after the rollup days when the served run holds a day the purge may already have thinned
    /// (<see cref="MayBeThinned"/>). Such a day answers as collected: the same day read from the interval table can lack rows the
    /// purge dropped early.</summary>
    public const string EdgeDaysNote = " (edge days as collected)";

    /// <summary>
    /// #9287: whether the run <paramref name="served"/> holds a day <see cref="MayBeThinned"/> says the purge can have thinned, on the
    /// read's clock and retention (the ones <see cref="ServedDaysSql"/> clips to). False for no run. Time alone, no query.
    /// </summary>
    internal static bool HoldsEdgeDays(NpgsqlConnection connection, ServedDays? served)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (served is not { } run)
        {
            return false;
        }

        var (nowUtc, retentionDays) = ReadClockFor(connection);
        return MayBeThinned(DateOnly.FromDateTime(run.Start), nowUtc ?? DateTime.UtcNow, retentionDays);
    }

    /// <summary>
    /// #9287: the source of a read over <c>[readStart, end]</c> that took <paramref name="served"/> (null for none) from the rollups, for
    /// <paramref name="reason"/> when it took none. A window that ends exactly at midnight does not touch the day that starts there.
    /// </summary>
    public static ReadSource DescribeSource(DateTime readStart, DateTime end, ServedDays? served, SourceReason reason, bool edgeDaysAsCollected = false)
        => DescribeSource(readStart, end, served, reason, edgeDaysAsCollected, null, null, null);

    /// <summary>
    /// #9363: <see cref="DescribeSource(DateTime, DateTime, ServedDays?, SourceReason, bool)"/> that also says, for each day the interval
    /// table served, why no rollup did. <paramref name="nowUtc"/> and <paramref name="retentionDays"/> (the read's clock and the interval
    /// table's retention, as <see cref="HoldsEdgeDays"/> takes them) tell a day that is not closed yet and a day the purge can already have
    /// thinned (never built, <see cref="MayBeThinned"/>) from the rest. <paramref name="builtDays"/> is what the served-days statement already
    /// returned, so a built day outside the longest run is told from a day with no usable rollup. Without a clock only the window's partial
    /// first and last days are labelled. A day built after the read began cannot be told from one not built yet: the statement runs in the
    /// read's snapshot, so both are the same absence.
    /// </summary>
    public static ReadSource DescribeSource(
        DateTime readStart, DateTime end, ServedDays? served, SourceReason reason, bool edgeDaysAsCollected,
        DateTime? nowUtc, int? retentionDays, IReadOnlyList<DateOnly>? builtDays)
    {
        var rollup = new List<DateOnly>();
        var interval = new List<DateOnly>();
        var last = DateOnly.FromDateTime(end);
        if (end.TimeOfDay == TimeSpan.Zero && end > readStart)
        {
            last = last.AddDays(-1);
        }

        for (var day = DateOnly.FromDateTime(readStart); day <= last; day = day.AddDays(1))
        {
            var midnight = day.ToDateTime(TimeOnly.MinValue);
            (served is { } run && midnight >= run.Start && midnight < run.End ? rollup : interval).Add(day);
        }

        var source = new ReadSource(rollup, interval, served is null && reason == SourceReason.Served ? SourceReason.NoBuiltDays : reason, edgeDaysAsCollected && served is not null);
        var firstDay = DateOnly.FromDateTime(readStart);
        var notes = new List<(DateOnly Day, string? Label)>(interval.Count);
        foreach (var day in interval)
        {
            var midnight = day.ToDateTime(TimeOnly.MinValue);
            string? label;
            if (midnight < readStart || midnight.AddDays(1) > end)
            {
                label = day == firstDay ? "partial first day" : nowUtc is { } open && day >= DateOnly.FromDateTime(open) ? "open day" : "partial last day";
            }
            else if (nowUtc is not { } now)
            {
                label = null;
            }
            else if (midnight.AddDays(1) + CloseAfter > now)
            {
                label = "not closed yet";
            }
            else if (builtDays is not null && builtDays.Contains(day))
            {
                /* A built day keeps its rollup even at the retention edge (the builder skips only a thinned day that was never built), so
                   a built day the read still took from the interval table is outside the longest run, whatever the time says. */
                label = served is null ? "no rollups for this read" : "built but outside the longest run";
            }
            else if (retentionDays is { } retention && MayBeThinned(day, now, retention))
            {
                label = "never built at the retention edge";
            }
            else
            {
                label = served is null ? "no rollups for this read" : "no rollup for this day";
            }

            notes.Add((day, label));
        }

        return source with { IntervalNotes = notes };
    }

    /// <summary>#9363: <see cref="DescribeSource(DateTime, DateTime, ServedDays?, SourceReason, bool, DateTime?, int?, IReadOnlyList{DateOnly}?)"/>
    /// on the clock and retention the served-day statement clipped to for <paramref name="connection"/> (the store's own, unless a test set them).</summary>
    internal static ReadSource DescribeSourceFor(
        NpgsqlConnection connection, DateTime readStart, DateTime end, ServedDays? served, SourceReason reason, IReadOnlyList<DateOnly>? builtDays)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var (nowUtc, retentionDays) = ReadClockFor(connection);
        return DescribeSource(readStart, end, served, reason, HoldsEdgeDays(connection, served), nowUtc ?? DateTime.UtcNow, retentionDays, builtDays);
    }

    /// <summary>
    /// #9287: the shape of a Query Store grid read's interval-table statements, for its slow-load line: the edge margin the read used
    /// (<paramref name="edgeMargin"/>, from <see cref="QueryStoreIntervalWide.ResolveEdgeMarginAndLongestSpanAsync"/>), "fell back to" when it is the full
    /// <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> because the interval length could not be trusted, the longest interval span the
    /// split arms' upper bound uses (<paramref name="longestSpan"/>, "fell back to" when it is the full
    /// <see cref="QueryStoreIntervalWide.IntervalSpanMargin"/>), and whether the arms are split (<paramref name="splitBuilt"/>, the partial
    /// index is built) or the read kept its one-arm bounds.
    /// </summary>
    public static string DescribeShape(TimeSpan edgeMargin, TimeSpan longestSpan, bool splitBuilt)
    {
        var culture = System.Globalization.CultureInfo.InvariantCulture;
        var marginHours = edgeMargin.TotalHours.ToString("0.#", culture);
        var spanHours = longestSpan.TotalHours.ToString("0.#", culture);
        var margin = edgeMargin < QueryStoreIntervalWide.PurgeEdgeMargin ? $"edge margin {marginHours} h" : $"edge margin fell back to {marginHours} h";
        var span = longestSpan < QueryStoreIntervalWide.IntervalSpanMargin ? $"span {spanHours} h" : $"span fell back to {spanHours} h";
        return $"{margin}, {span}, {(splitBuilt ? "split arms" : "one arm, index not built")}";
    }

    /// <summary>The longest run of consecutive days in <paramref name="days"/> (sorted, oldest first); the newest such run on a tie.</summary>
    public static ServedDays? LongestRun(IReadOnlyList<DateOnly> days)
    {
        ArgumentNullException.ThrowIfNull(days);
        if (days.Count == 0)
        {
            return null;
        }

        int bestStart = 0, bestLength = 1, runStart = 0;
        for (var i = 1; i <= days.Count; i++)
        {
            if (i < days.Count && days[i] == days[i - 1].AddDays(1))
            {
                continue;
            }

            if (i - runStart >= bestLength)
            {
                bestStart = runStart;
                bestLength = i - runStart;
            }

            runStart = i;
        }

        return new ServedDays(
            days[bestStart].ToDateTime(TimeOnly.MinValue),
            days[bestStart + bestLength - 1].AddDays(1).ToDateTime(TimeOnly.MinValue));
    }

    /// <summary>
    /// The tick's first step, for every server and whether or not anything is built after it: moves the heartbeat to
    /// <paramref name="nowUtc"/> less <see cref="HeartbeatMargin"/>. When the last heartbeat is older than <see cref="HeartbeatFreshFor"/>
    /// (an older service ran in between, or ticks stopped), it first marks the built days that hold a row collected since it, in the same
    /// transaction, so the tick's builds redo them and the heartbeat never passes a row its day was built without. A fresh heartbeat
    /// skips the mark: a current service marks a late row's day itself, and the scan that finds them walks every partition of the server.
    /// A server whose step fails keeps its old heartbeat and is tried again next tick. Returns how many days it marked.
    /// </summary>
    public static async Task<int> HeartbeatAsync(NpgsqlConnection connection, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);

        var servers = new List<(int ServerId, DateTime? Previous)>();
        await using (var list = new NpgsqlCommand(HeartbeatServersSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await list.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.Add((reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1)));
            }
        }

        var marked = 0;
        var beat = Unspecified(nowUtc - HeartbeatMargin);
        foreach (var (serverId, previous) in servers)
        {
            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                if (previous is { } last && last < nowUtc - HeartbeatFreshFor)
                {
                    await using var mark = new NpgsqlCommand(MarkChangedDaysSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
                    mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                    mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(last) });
                    marked += Convert.ToInt32(await mark.ExecuteScalarAsync(cancellationToken));
                }

                await using (var record = new NpgsqlCommand(RecordHeartbeatSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
                {
                    record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                    record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = beat });
                    await record.ExecuteNonQueryAsync(cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Query Store closed-day rollups: the heartbeat of server {ServerId} failed and is retried next tick: {Message}", serverId, ex.Message);
            }
        }

        return marked;
    }

    /// <summary>The builds due at <paramref name="nowUtc"/>, newest day first.</summary>
    public static async Task<IReadOnlyList<Build>> PlanBuildsAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var builds = new List<Build>();
        var clutterOnly = new List<bool>();
        await using var command = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = CloseAfter });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            builds.Add(new Build(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
            clutterOnly.Add(reader.GetBoolean(2));
        }

        await reader.CloseAsync();

        /* #9287: a day the purge may already have thinned (MayBeThinned) is never built, or rebuilt, from the interval table. A never-built one
           stays with the interval table. A built one keeps its rollup: its rebuild mark is cleared, so the reader keeps serving it. */
        var plan = new List<Build>(builds.Count);
        var floors = new Dictionary<int, DateTime?>();
        for (var i = 0; i < builds.Count; i++)
        {
            var build = builds[i];
            if (!MayBeThinned(build.Day, nowUtc, retentionDays))
            {
                /* #9287: a built day due only because its clutter rows are not from its newest build is rebuilt only when raw holds the day
                   whole. Otherwise the build would take no clutter from raw and the day's clutter would stay unserved, so it would be due
                   again every tick. The floor is read once per server per tick. */
                if (clutterOnly[i])
                {
                    if (!floors.TryGetValue(build.ServerId, out var floor))
                    {
                        floor = await ReadFloorAsync(connection, null, StatsFloorSql, build.ServerId, cancellationToken);
                        floors[build.ServerId] = floor;
                    }

                    if (!StatsHoldDayWhole(floor, build.Day))
                    {
                        continue;
                    }
                }

                plan.Add(build);
                continue;
            }

            await using var clear = new NpgsqlCommand(ClearThinDayMarkSql, connection) { CommandTimeout = CommandTimeoutSeconds };
            clear.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = build.ServerId });
            clear.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = build.Day });
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }

        return plan;
    }

    /// <summary>#9287: clears the rebuild mark of a built day that <see cref="MayBeThinned"/>, so its rollup stays served.
    /// $1 server_id, $2 day.</summary>
    public const string ClearThinDayMarkSql = @"
UPDATE collect.query_store_closed_day_built
SET rebuild_needed = false
WHERE server_id = $1
  AND day_utc = $2
  AND rebuild_needed
  AND NOT removed;";

    /// <summary>#9287: the oldest <c>collection_time</c> the raw stats table holds for one server, the floor
    /// <see cref="StatsHoldDayWhole"/> reads for the clutter rollups. $1 server_id.</summary>
    public const string StatsFloorSql = @"
SELECT MIN(s.collection_time)
FROM collect.query_store_stats AS s
WHERE s.server_id = $1;";

    /// <summary>#9383: removes the clutter stamp of a built day that holds no clutter row and ends at or before its server's oldest raw row: a
    /// build before #9287 stamped it empty, and the clutter route served it as "no plans" where raw never held it. With the stamp gone the day is
    /// not served from the rollups, and it is not built again because raw does not hold it whole (<see cref="StatsHoldDayWhole"/>), so the span
    /// reads as not collected. A day with clutter rows keeps its stamp. A server with no raw row is left alone. The oldest row is one read of the
    /// raw table's <c>(server_id, collection_time)</c> index. Only a stamp built when raw could no longer hold its day is removed (built at or after
    /// the day's start plus the days raw keeps): a stamp built while raw held the day is a quiet day, and it stays. $1 the days raw keeps
    /// (<see cref="QueryStoreClutterHourly.RawHeldDaysAsync"/>).</summary>
    public const string UnstampEmptyClutterDaysSql = @"
UPDATE collect.query_store_closed_day_built AS b
SET clutter_built_at = NULL
FROM
(
    SELECT s.server_id, fl.t
    FROM collect.servers AS s
    CROSS JOIN LATERAL
    (
        SELECT f.collection_time AS t
        FROM collect.query_store_stats AS f
        WHERE f.server_id = s.server_id
        ORDER BY f.collection_time
        LIMIT 1
    ) AS fl
) AS f
WHERE b.server_id = f.server_id
  AND NOT b.removed
  AND b.clutter_built_at = b.built_at
  AND (b.day_utc + 1)::timestamp <= f.t
  AND b.clutter_built_at >= b.day_utc::timestamp + make_interval(days => $1)
  AND NOT EXISTS (SELECT 1 FROM collect.query_store_clutter_plan_daily AS p WHERE p.server_id = b.server_id AND p.day_utc = b.day_utc)
  AND NOT EXISTS (SELECT 1 FROM collect.query_store_clutter_collections_daily AS c WHERE c.server_id = b.server_id AND c.day_utc = b.day_utc);";

    /// <summary>#9383: runs <see cref="UnstampEmptyClutterDaysSql"/> with the days raw keeps; returns how many days lost their stamp.</summary>
    public static async Task<long> UnstampEmptyClutterDaysAsync(NpgsqlConnection connection, int rawHeldDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(UnstampEmptyClutterDaysSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rawHeldDays });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<DateTime?> ReadFloorAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, int serverId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        return await command.ExecuteScalarAsync(cancellationToken) is DateTime floor ? floor : null;
    }

    /// <summary>Removes the days wholly before the retention edge (#9287) from both rollups and leaves each server's newest removed day in the built
    /// table, in one transaction; returns how many (server, day) went.</summary>
    public static async Task<long> GcAsync(
        NpgsqlConnection connection, DateTime nowUtc, int retentionDays, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var expired = new List<(int ServerId, DateOnly Day)>();
        await using (var command = new NpgsqlCommand(GcMarkRemovedSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                expired.Add((reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1)));
            }
        }

        foreach (var (serverId, day) in expired)
        {
            await DeleteDayAsync(connection, transaction, serverId, day, cancellationToken);
        }

        if (expired.Count > 0)
        {
            await using var prune = new NpgsqlCommand(GcPruneRemovedSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            await prune.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return expired.Count;
    }

    private static async Task DeleteDayAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateOnly day, CancellationToken cancellationToken, bool keepClutter = false)
    {
        foreach (var sql in keepClutter
            ? new[] { DeleteGridDaySql, DeleteSlicerDaySql, DeleteClutterDayStampSql }
            : new[] { DeleteGridDaySql, DeleteSlicerDaySql, DeleteClutterPlanDaySql, DeleteClutterCollectionsDaySql, DeleteClutterDayStampSql })
        {
            await using var delete = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Builds one (server, day) in one transaction: the built row is taken first, the day is cleared from both tables and filled
    /// again. Returns the interval rows read, or -1 when the day is removed and nothing was built.
    /// </summary>
    public static async Task<long> BuildDayAsync(
        NpgsqlConnection connection, int serverId, DateOnly day, DateTime nowUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var timeout = new NpgsqlCommand(StatementTimeoutSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            await timeout.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Read before the claim: the claim sets built_at, and the clutter rows are the previous build's only while this still holds. */
        bool hadClutter;
        await using (var had = new NpgsqlCommand(HasClutterFromBuildSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            had.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            had.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            hadClutter = await had.ExecuteScalarAsync(cancellationToken) is true;
        }

        await using (var claim = new NpgsqlCommand(ClaimDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            claim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            claim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            claim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            if (await claim.ExecuteScalarAsync(cancellationToken) is null)
            {
                await transaction.RollbackAsync(cancellationToken);
                return -1;
            }
        }

        /* #9287: raw query_store_stats is purged sooner than the rollups. A day it no longer holds whole keeps the clutter rows its previous
           build took from raw whole (and the stamp that says so), and only the grid and the slicer are built again. A day with no such rows
           (never built, built before the clutter rollups, or its clutter not from its previous build) gets no clutter and no stamp, so it
           stays unserved and the clutter route reads raw for it. */
        var rawWhole = StatsHoldDayWhole(await ReadFloorAsync(connection, transaction, StatsFloorSql, serverId, cancellationToken), day);
        var keepClutter = !rawWhole && hadClutter;
        var buildClutter = rawWhole;
        await DeleteDayAsync(connection, transaction, serverId, day, cancellationToken, keepClutter);

        long sourceRows;
        await using (var grid = new NpgsqlCommand(BuildGridDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            grid.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            grid.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            sourceRows = Convert.ToInt64(await grid.ExecuteScalarAsync(cancellationToken));
        }

        await using (var slicer = new NpgsqlCommand(BuildSlicerDaySql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            slicer.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            slicer.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            await slicer.ExecuteNonQueryAsync(cancellationToken);
        }

        /* #9148: the clutter rollups of raw query_store_stats, in the same transaction and under the same claim, so a late raw row marks
           and rebuilds the day the way a late interval row does. */
        foreach (var sql in keepClutter
            ? new[] { RecordClutterBuiltSql }
            : buildClutter ? new[] { BuildClutterPlanDaySql, BuildClutterCollectionsDaySql, RecordClutterBuiltSql } : Array.Empty<string>())
        {
            await using var clutter = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            clutter.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            clutter.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            await clutter.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var record = new NpgsqlCommand(RecordSourceRowsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = day });
            record.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = sourceRows });
            await record.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return sourceRows;
    }

    /// <summary>
    /// One maintenance tick: remove the days retention took, plan, then build each day in its own transaction, newest first. A build
    /// that fails is logged and the loop goes on; the next tick plans it again. Once <see cref="MaxTickDuration"/> has passed no
    /// further build starts, and the planned days left are reported as deferred.
    /// </summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, CancellationToken cancellationToken) =>
        RunTickAsync(dataSource, nowUtc, retentionDays, null, logger, cancellationToken);

    /// <summary>
    /// <see cref="RunTickAsync(NpgsqlDataSource, DateTime, int, ILogger, CancellationToken)"/> that also runs the hourly clutter step (#9173,
    /// <see cref="QueryStoreClutterHourly"/>) when <paramref name="rawRetentionDays"/> is given: the retention raw <c>query_store_stats</c>
    /// is purged on, which the hourly rollups follow. Null skips the hourly step.
    /// </summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, int? rawRetentionDays, ILogger logger, CancellationToken cancellationToken,
        Func<CancellationToken, Task<IBatchWalPacer?>>? pacerFactory = null)
    {
        var clock = Stopwatch.StartNew();
        return RunTickAsync(dataSource, nowUtc, retentionDays, rawRetentionDays, logger, () => clock.Elapsed, cancellationToken, pacerFactory);
    }

    /// <summary><see cref="RunTickAsync(NpgsqlDataSource, DateTime, int, ILogger, CancellationToken)"/> with the elapsed time since the
    /// tick began supplied by <paramref name="elapsed"/>, read before each build starts.</summary>
    public static Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken) =>
        RunTickAsync(dataSource, nowUtc, retentionDays, null, logger, elapsed, cancellationToken);

    /// <summary>The tick, with the hourly step (<paramref name="rawRetentionDays"/>) and the elapsed clock. <paramref name="pacerFactory"/> builds the
    /// WAL pacer the hourly step's sliced prune runs under (#9690); null runs it unpaced.</summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int retentionDays, int? rawRetentionDays, ILogger logger, Func<TimeSpan> elapsed, CancellationToken cancellationToken,
        Func<CancellationToken, Task<IBatchWalPacer?>>? pacerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(elapsed);

        int built = 0, failed = 0, started = 0;

        await using var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.QueryStoreClosedDay, cancellationToken);
        await HeartbeatAsync(connection, nowUtc, logger, cancellationToken);

        /* #9173: the hourly clutter step first: an hour costs little and is what a 24-hour read waits on, and it stops at its own share of the
           tick so a backlog of hours leaves the daily builds their time. It has its own catch, so a failure leaves the day work standing. */
        QueryStoreClutterHourly.TickResult hours = default;
        QueryStoreClutterDayFold.TickResult folded = default;
        if (rawRetentionDays is { } rawDays)
        {
            var hoursRan = false;
            try
            {
                hours = await QueryStoreClutterHourly.RunAsync(connection, nowUtc, rawDays, logger, elapsed, cancellationToken, pacerFactory);
                hoursRan = true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Query Store hourly clutter rollups could not run; the next tick retries: {Message}", ex.Message);
                /* #9721: the throw is the step's failure: one counted failure, so the step reads failed and not ok. hoursRan stays false, so the fold is skipped. */
                hours = default(QueryStoreClutterHourly.TickResult) with { Failed = 1 };
            }

            /* #9384: then the clutter days the hours now complete, so a day closes soon after midnight and not at the grid's close. Its own catch too.
               It runs only after the hourly step ran: that step also removes the empty hour stamps that raw never held (#9383), and a day folded
               from stamps it has not yet removed would be stamped as folded and served as empty clutter. */
            if (hoursRan)
            {
                try
                {
                    folded = await QueryStoreClutterDayFold.RunAsync(connection, nowUtc, retentionDays, logger, elapsed, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(ex, "Query Store clutter days from hours could not run; the next tick retries: {Message}", ex.Message);
                    folded = default(QueryStoreClutterDayFold.TickResult) with { Failed = 1 };
                }
            }
        }

        var removed = await GcAsync(connection, nowUtc, retentionDays, cancellationToken);

        /* #9383: before the plan, so a day it unstamps is planned with no stamp and, raw not holding it whole, is not built. */
        var rawHeldDays = await QueryStoreClutterHourly.RawHeldDaysAsync(connection, rawRetentionDays, cancellationToken);
        var unstamped = rawHeldDays is { } held ? await UnstampEmptyClutterDaysAsync(connection, held, cancellationToken) : 0L;
        var plan = await PlanBuildsAsync(connection, nowUtc, retentionDays, cancellationToken);

        foreach (var item in plan)
        {
            if (elapsed() >= MaxTickDuration)
            {
                break;
            }

            started++;
            try
            {
                await BuildDayAsync(connection, item.ServerId, item.Day, nowUtc, cancellationToken);
                built++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(ex, "Query Store closed-day rollups: building server {ServerId} day {Day:yyyy-MM-dd} failed and is retried next tick: {Message}",
                    item.ServerId, item.Day, ex.Message);
            }
        }

        var deferred = plan.Count - started;
        logger.LogInformation("Query Store closed-day rollups: built {Built}, failed {Failed}, deferred {Deferred}, removed {Removed} day(s), unstamped {Unstamped} empty clutter day(s), {ElapsedMs} ms",
            built, failed, deferred, removed, unstamped, (long)elapsed().TotalMilliseconds);
        return new TickResult(built, failed, removed, deferred, hours.Built, hours.Failed, hours.Removed, hours.Deferred, folded.Built, hours.PruneFailure, folded.Failed);
    }
}

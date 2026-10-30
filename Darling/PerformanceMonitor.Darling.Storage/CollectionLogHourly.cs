/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly rollup of <c>collect.collection_log</c> (V186, #5810): one row per (server, collector, closed hour) holding every
/// decomposable aggregate <c>get_collection_health</c> reads, plus a duration histogram, so the 7-day read sums about
/// 168 rows per collector instead of scanning and aggregating every run of the week.
///
/// <para><b>What a row holds.</b> Counts and sums add across hours; the AVG is kept as <c>duration_sum</c> and
/// <c>duration_count</c> so the read computes <c>sum::numeric / count</c>, which is what PostgreSQL's <c>AVG(integer)</c>
/// computes. The maxima and the newest instants take a MAX across hours. Exact p95 cannot be rebuilt from hourly rows, so
/// each row carries <c>duration_histogram</c>, a count of timed runs per slot of <see cref="DurationBucketBounds"/>, and the
/// read reports the upper bound of the slot that holds the 95th percentile.</para>
///
/// <para><b>How it is kept.</b> <see cref="RollUpAsync"/> runs on the store self-metrics sweep, next to
/// <c>collector_cost</c>'s flush. One statement upserts the last <see cref="RecomputeHours"/> closed hours on every pass,
/// so a run written late into an hour that is already rolled is counted on the next pass, plus at most
/// <see cref="CatchUpHours"/> older closed hours the table is missing, newest first, so the first passes after the upgrade
/// backfill the retention window a day at a time. A second statement deletes hours older than <see cref="RetentionHours"/>.</para>
///
/// <para><b>Coverage.</b> A rolled hour holds a row for every collector of every server that ran in it: an hour in which
/// a collector did not run has no row for it. Any other hour (the open hour, a backlog still being caught up, hours while a
/// pass is failing) is only in the raw log, and the read takes it from there. The status predicates are baked in
/// when an hour is rolled, so a predicate change reaches hours rolled after it (and the last two hours on its first pass),
/// not older ones, until they age out.</para>
/// </summary>
public static class CollectionLogHourly
{
    /// <summary>
    /// Hours kept: 7 days plus 3 hours. The health window is 7 days; its first hour is partial and read from the raw log,
    /// so the table needs every closed hour after it. The margin covers a sweep that runs late.
    /// </summary>
    public const int RetentionHours = 7 * 24 + 3;

    /// <summary>Closed hours every pass recomputes from the raw log, to count runs written late into an hour.</summary>
    public const int RecomputeHours = 2;

    /// <summary>The most missing closed hours one pass rolls up besides the <see cref="RecomputeHours"/> it always
    /// recomputes, newest first (coordinator ruling on #5810): a store with a 7-day backlog is caught up in about 7
    /// passes, and no pass reads more than its own hours of the raw log (at most this many plus <see cref="RecomputeHours"/>):
    /// the scan is per hour, so a pass late in a backfill reads no more than the first.</summary>
    public const int CatchUpHours = 24;

    /// <summary>Command timeout of each of the pass's two statements.</summary>
    public const int PassTimeoutSeconds = 120;

    /// <summary>The pass's own slice of the sweep budget, so a slow backfill cannot cancel what runs after it.</summary>
    public static readonly TimeSpan PassSliceBudget = TimeSpan.FromSeconds(PassTimeoutSeconds);

    /// <summary>Number of log-spaced bounds in <see cref="DurationBucketBounds"/>.</summary>
    public const int BoundCount = 40;

    /// <summary>The top bound, one hour in milliseconds.</summary>
    public const int TopBoundMs = 3_600_000;

    /// <summary>
    /// The histogram's slot bounds in milliseconds, shared by the writer (<see cref="UpsertSql"/>, which passes them as its
    /// array parameter) and the read. Bound <c>i</c> (0 to 39) is <c>ceil(3,600,000 ^ (i / 39))</c>: 40 log-spaced bounds
    /// from 1 ms to 1 hour, each about 1.47 times the one before, so a slot's upper bound is within 47% of any duration in it.
    /// Strictly increasing (the ceiling keeps the small bounds distinct: 1, 2, 3, 4, 5, 7, 11, ...).
    /// <para>Slot <c>s</c> of a histogram counts the timed runs whose duration has exactly <c>s</c> bounds below it
    /// (<see cref="SlotOf"/>): slot 0 holds durations up to 1 ms, slot <c>s</c> (1 to 39) holds durations above bound
    /// <c>s - 1</c> and up to bound <c>s</c>, and slot 40 holds durations above an hour. So a histogram has
    /// <see cref="SlotCount"/> slots, and slot <c>s</c>'s upper bound is bound <c>s</c> (<see cref="UpperBoundOfSlot"/>).</para>
    /// </summary>
    public static readonly int[] DurationBucketBounds = BuildBounds();

    /// <summary>Slots in a histogram: one per bound plus the slot above the top bound.</summary>
    public const int SlotCount = BoundCount + 1;

    private static int[] BuildBounds()
    {
        var bounds = new int[BoundCount];
        for (var i = 0; i < BoundCount; i++)
        {
            var bound = (int)Math.Ceiling(Math.Pow(TopBoundMs, (double)i / (BoundCount - 1)));
            bounds[i] = i == 0 ? bound : Math.Max(bound, bounds[i - 1] + 1);
        }

        bounds[BoundCount - 1] = TopBoundMs;
        return bounds;
    }

    /// <summary>The slot a duration counts in: the number of bounds below it. The SQL twin is
    /// <c>width_bucket(duration_ms - 1, bounds)</c>, which for an integer duration counts the bounds at or below
    /// <c>duration_ms - 1</c>, the same number.</summary>
    public static int SlotOf(long durationMs)
    {
        var slot = 0;
        while (slot < BoundCount && DurationBucketBounds[slot] < durationMs)
        {
            slot++;
        }

        return slot;
    }

    /// <summary>The largest duration a slot can hold, or null for the slot above an hour, which has no upper bound.</summary>
    public static int? UpperBoundOfSlot(int slot) =>
        slot >= 0 && slot < BoundCount ? DurationBucketBounds[slot] : null;

    /// <summary>The smallest duration a slot can hold: one more than the bound below it, or 0 for the first slot.</summary>
    public static int LowerBoundOfSlot(int slot) =>
        slot <= 0 ? 0 : DurationBucketBounds[Math.Min(slot, BoundCount) - 1] + 1;

    /// <summary>
    /// #5810 review: the smallest value the exact p95 can have, given the p95 the rollup read reports. The read reports
    /// <c>LEAST(upper bound of the p95's slot, max duration)</c>, a value inside that slot either way, so the value's own
    /// slot is the p95's slot and its lower bound is the floor. The peak-cycle arithmetic charges this, so bucketing alone
    /// never raises a risk or names a peak collector the exact p95 would not.
    /// </summary>
    public static double P95FloorMs(double reportedP95Ms) =>
        reportedP95Ms <= 0 ? 0 : LowerBoundOfSlot(SlotOf((long)Math.Ceiling(reportedP95Ms)));

    private static string HistogramSql =>
        "ARRAY[" + string.Join(", ", Enumerable.Range(0, SlotCount).Select(s => $"COUNT(*) FILTER (WHERE w.slot = {s})")) + "]::integer[]";

    /// <summary>
    /// The aggregates of one row, in <see cref="ValueColumns"/> order, over a subquery aliased <c>w</c> that has the raw
    /// log's columns plus <c>slot</c> (<c>width_bucket(duration_ms - 1, bounds)</c>). Each is the aggregate
    /// <c>DarlingDataReader.CollectionHealthSql</c>'s <c>health</c> computes over the same rows. The writer runs it per
    /// hour, and the read (<c>DarlingDataReader.CollectionHealthComposedSql</c>) runs it per collector over the slices the
    /// table does not hold, so a rolled hour and a raw slice cannot count a run differently.
    /// </summary>
    internal static readonly string AggregatesSql = $@"COUNT(*)::integer,
    COUNT(*) FILTER (WHERE w.status = 'SUCCESS' AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})::integer,
    COUNT(*) FILTER (WHERE w.status = 'ERROR')::integer,
    COUNT(*) FILTER (WHERE w.status = 'PERMISSIONS')::integer,
    COUNT(*) FILTER (WHERE w.status = 'YIELDED')::integer,
    COUNT(*) FILTER (WHERE {EnumeratedCollectorDriver.AbandonedRunPredicateSql})::integer,
    COUNT(*) FILTER (WHERE w.status = 'EXTENSION_MISSING')::integer,
    COUNT(*) FILTER (WHERE w.status = 'SESSION_MISSING')::integer,
    COUNT(CASE WHEN w.status = 'SUCCESS' THEN w.error_message END)::integer,
    COUNT(*) FILTER (WHERE w.rows_collected > 0)::integer,
    COALESCE(SUM(w.rows_collected), 0)::bigint,
    COALESCE(SUM(w.duration_ms), 0)::bigint,
    COUNT(w.duration_ms)::integer,
    MAX(w.duration_ms),
    MAX(w.slowest_item_ms),
    MAX(w.collection_time) FILTER (WHERE w.status IN ('SUCCESS', 'SKIPPED')),
    MAX(w.collection_time),
    MAX(w.collection_time) FILTER (WHERE w.status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING')),
    MAX(w.collection_time) FILTER (WHERE w.status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING')
                                     AND w.error_message IS NOT NULL),
    MAX(w.collection_time) FILTER (WHERE w.status = 'SUCCESS' AND w.error_message IS NOT NULL),
    MAX(w.collection_time) FILTER (WHERE w.status = 'PERMISSIONS'),
    MAX(w.collection_time) FILTER (WHERE w.status IS NULL
                                      OR w.status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})),
    MAX(w.collection_time) FILTER (WHERE w.rows_collected > 0),
    MAX(w.collection_time) FILTER (WHERE NOT (w.status = 'SUCCESS'
                                              AND COALESCE(w.rows_collected, 0) = 0
                                              AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})),
    {HistogramSql}";

    /// <summary>The bounds as a SQL array literal, for a statement that inlines them instead of binding them.</summary>
    internal static string BoundsArraySql => "'{" + string.Join(",", DurationBucketBounds) + "}'::integer[]";

    /// <summary>The columns the upsert writes, in order, after the key.</summary>
    internal const string ValueColumns = @"total_runs, success_count, error_count, permission_denied_count, yield_count,
    abandoned_count, extension_missing_count, session_missing_count, note_count, runs_with_rows, rows_stored,
    duration_sum, duration_count, max_duration_ms, max_slowest_item_ms,
    last_success_time, last_run_time, last_error_time, last_failure_text_time, last_note_time, last_denied_time,
    last_non_skip_time, last_productive_time, last_streak_break_time, duration_histogram";

    /// <summary>
    /// One pass's upsert. <c>$1</c> is the current hour (exclusive end), <c>$2</c> the oldest hour kept, <c>$3</c> the first
    /// hour every pass recomputes, <c>$4</c> <see cref="DurationBucketBounds"/>. It rolls the hours from <c>$3</c> on, plus at
    /// most <see cref="CatchUpHours"/> closed hours from <c>$2</c> on that hold raw rows and no rolled row, newest
    /// first. Which hours are missing is a store-wide fact: one statement rolls an hour for every server at once. Each aggregate is the one
    /// <c>DarlingDataReader.CollectionHealthSql</c>'s <c>health</c> computes over the same rows, per hour.
    /// </summary>
    internal static readonly string UpsertSql = $@"
WITH todo AS
(
    -- The last two closed hours, always (late writers).
    SELECT r.hour_start
    FROM generate_series($3::timestamp, $1::timestamp - interval '1 hour', interval '1 hour') AS r(hour_start)
    UNION ALL
    -- At most {CatchUpHours} older closed hours the table does not hold yet, newest first: a backlog (the first passes
    -- after the upgrade, or passes that failed) is caught up a day per pass instead of in one statement. An hour with
    -- no raw row is not missing, so an outage cannot hold the cap on hours that will never get a row. The series runs
    -- newest first and is filtered in place, so the LIMIT stops the probes at the last hour it takes: a raw-log probe
    -- of a compressed chunk costs tens of milliseconds, and a sort above the filter would make every pass probe every
    -- missing hour. So the series is sorted first, in a subquery the filter cannot be pushed into (OFFSET 0), and the
    -- outer ORDER BY states the newest-first contract, which that sorted input satisfies without a second sort. The
    -- probes sit inside a CASE so the planner keeps them as per-row filters (an EXISTS in WHERE can become a hash join,
    -- which loses the order), and the raw probe runs only for an hour the table lacks.
    SELECT m.hour_start
    FROM
    (
        SELECT g.hour_start
        FROM
        (
            SELECT s.hour_start
            FROM generate_series($3::timestamp - interval '1 hour', $2::timestamp, interval '-1 hour') AS s(hour_start)
            ORDER BY s.hour_start DESC
            OFFSET 0
        ) AS g
        WHERE CASE
                  WHEN EXISTS (SELECT 1 FROM collect.collection_log_hourly AS h WHERE h.hour_start = g.hour_start)
                  THEN FALSE
                  ELSE EXISTS (SELECT 1
                               FROM collect.collection_log AS x
                               WHERE x.collection_time >= g.hour_start
                               AND   x.collection_time < g.hour_start + interval '1 hour'
                               AND   x.{CollectionCycleRow.NotCycleRowSql})
              END
        ORDER BY g.hour_start DESC
        LIMIT {CatchUpHours}
    ) AS m
)
INSERT INTO collect.collection_log_hourly
(
    server_id, collector_name, hour_start,
    {ValueColumns}
)
SELECT
    w.server_id,
    w.collector_name,
    w.hour_start,
    {AggregatesSql}
FROM
(
    -- Per todo hour (#5810 review): each hour's scan is bounded on both sides by that hour, so a pass reads only its own
    -- hours and chunk exclusion runs per hour. One range from the oldest todo hour on made pass k of a backfill read k days.
    -- OFFSET 0 keeps the subquery from being pulled up into a plain join, which the planner ran as one scan of the
    -- whole retained log against the todo list (4.6 s on a 1.05M-row plant, against 1.45 s for the one-range scan).
    SELECT
        l.server_id,
        l.collector_name,
        l.collection_time,
        l.status,
        l.error_message,
        l.rows_collected,
        l.duration_ms,
        l.slowest_item_ms,
        t.hour_start,
        width_bucket(l.duration_ms - 1, $4::integer[]) AS slot
    FROM todo AS t
    CROSS JOIN LATERAL
    (
        SELECT x.*
        FROM collect.collection_log AS x
        WHERE x.collection_time >= t.hour_start
        AND   x.collection_time < t.hour_start + interval '1 hour'
        AND   x.{CollectionCycleRow.NotCycleRowSql}
        OFFSET 0
    ) AS l
) AS w
GROUP BY
    w.server_id,
    w.collector_name,
    w.hour_start
ON CONFLICT (server_id, collector_name, hour_start) DO UPDATE
SET
    {string.Join(@",
    ", ValueColumns.Split(',').Select(c => c.Trim()).Select(c => $"{c} = EXCLUDED.{c}"))};";

    /// <summary>The bounded retention DELETE, like <c>collector_cost</c>'s: hours older than <c>$1</c>, through the
    /// <c>hour_start</c> index.</summary>
    internal const string RetentionDeleteSql = @"
DELETE FROM collect.collection_log_hourly
WHERE hour_start < $1::timestamp;";

    /// <summary>
    /// One pass: the upsert, then the retention DELETE. <paramref name="nowUtc"/> is the service clock in UTC, the clock
    /// <c>collection_time</c> is written in. Returns the number of hour rows upserted.
    /// </summary>
    public static async Task<int> RollUpAsync(NpgsqlConnection connection, DateTime nowUtc, ILogger? logger, CancellationToken cancellationToken)
    {
        var currentHour = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, 0, 0, DateTimeKind.Unspecified);
        var oldestKept = currentHour.AddHours(-RetentionHours);

        await using var upsert = new NpgsqlCommand(UpsertSql, connection) { CommandTimeout = PassTimeoutSeconds };
        upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = currentHour });
        upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = oldestKept });
        upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = currentHour.AddHours(-RecomputeHours) });
        upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = DurationBucketBounds });
        var upserted = await upsert.ExecuteNonQueryAsync(cancellationToken);

        await using var retention = new NpgsqlCommand(RetentionDeleteSql, connection) { CommandTimeout = PassTimeoutSeconds };
        retention.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = oldestKept });
        await retention.ExecuteNonQueryAsync(cancellationToken);

        logger?.LogDebug("collection_log_hourly pass upserted {Rows} hour rows", upserted);
        return upserted;
    }
}

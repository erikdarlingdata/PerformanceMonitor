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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

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
/// <para><b>How it is kept.</b> <see cref="RunPassAsync"/> runs on the store self-metrics sweep, next to
/// <c>collector_cost</c>'s flush. One statement upserts every closed hour from the hour after the newest rolled hour to the
/// current hour, and always the last <see cref="RecomputeHours"/> closed hours, so a run written late into an hour that is
/// already rolled is counted on the next pass. On an empty table (the first pass after the upgrade) the same statement
/// backfills the whole retention window. A second statement deletes hours older than <see cref="RetentionHours"/>.</para>
///
/// <para><b>Coverage.</b> Every closed hour from the oldest kept hour through the newest rolled hour is in the table for
/// every collector that ran in it: an hour in which a collector did not run has no row for it. Hours after the newest rolled
/// hour (the open hour, and closed hours while a pass is failing) are only in the raw log. The status predicates are baked in
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

    /// <summary>Command timeout of each of the pass's two statements.</summary>
    public const int PassTimeoutSeconds = 120;

    /// <summary>The pass's own slice of the sweep budget, so a slow backfill cannot cancel what runs after it.</summary>
    internal static readonly TimeSpan PassSliceBudget = TimeSpan.FromSeconds(PassTimeoutSeconds);

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

    private static string HistogramSql =>
        "ARRAY[" + string.Join(", ", Enumerable.Range(0, SlotCount).Select(s => $"COUNT(*) FILTER (WHERE w.slot = {s})")) + "]::integer[]";

    /// <summary>The columns the upsert writes, in order, after the key.</summary>
    private const string ValueColumns = @"total_runs, success_count, error_count, permission_denied_count, yield_count,
    abandoned_count, extension_missing_count, session_missing_count, note_count, runs_with_rows, rows_stored,
    duration_sum, duration_count, max_duration_ms, max_slowest_item_ms,
    last_success_time, last_run_time, last_error_time, last_failure_text_time, last_note_time, last_denied_time,
    last_non_skip_time, last_productive_time, last_streak_break_time, duration_histogram";

    /// <summary>
    /// One pass's upsert. <c>$1</c> is the current hour (exclusive end), <c>$2</c> the oldest hour kept, <c>$3</c> the first
    /// hour every pass recomputes, <c>$4</c> <see cref="DurationBucketBounds"/>. It rolls every hour from the hour after the
    /// newest rolled hour, or from <c>$3</c> when that is earlier, and never below <c>$2</c>. Each aggregate is the one
    /// <c>DarlingDataReader.CollectionHealthSql</c>'s <c>health</c> computes over the same rows, per hour.
    /// </summary>
    internal static readonly string UpsertSql = $@"
WITH bounds AS
(
    SELECT
        GREATEST($2::timestamp,
                 LEAST(COALESCE(MAX(h.hour_start) + interval '1 hour', $2::timestamp), $3::timestamp)) AS from_hour
    FROM collect.collection_log_hourly AS h
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
    COUNT(*)::integer,
    COUNT(*) FILTER (WHERE w.status = 'SUCCESS')::integer,
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
    MAX(w.collection_time) FILTER (WHERE w.status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING')),
    MAX(w.collection_time) FILTER (WHERE w.status = 'SUCCESS' AND w.error_message IS NOT NULL),
    MAX(w.collection_time) FILTER (WHERE w.status = 'PERMISSIONS'),
    MAX(w.collection_time) FILTER (WHERE w.status IS NULL
                                      OR w.status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})),
    MAX(w.collection_time) FILTER (WHERE w.rows_collected > 0),
    MAX(w.collection_time) FILTER (WHERE NOT (w.status = 'SUCCESS'
                                              AND COALESCE(w.rows_collected, 0) = 0
                                              AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})),
    {HistogramSql}
FROM
(
    SELECT
        l.server_id,
        l.collector_name,
        l.collection_time,
        l.status,
        l.error_message,
        l.rows_collected,
        l.duration_ms,
        l.slowest_item_ms,
        date_trunc('hour', l.collection_time) AS hour_start,
        width_bucket(l.duration_ms - 1, $4::integer[]) AS slot
    FROM collect.collection_log AS l
    WHERE l.collection_time >= (SELECT b.from_hour FROM bounds AS b)
    AND   l.collection_time < $1::timestamp
    AND   {CollectionCycleRow.NotCycleRowSql}
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
    public static async Task<int> RunPassAsync(NpgsqlConnection connection, DateTime nowUtc, ILogger? logger, CancellationToken cancellationToken)
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

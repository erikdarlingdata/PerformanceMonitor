/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store's own WAL figures as an hourly series (#9419), read from the <c>wal</c> rows <see cref="StoreWalFigures"/> writes. The rows hold
/// CUMULATIVE counters, so this class takes the difference between each row and the one before it, the way <see cref="DarlingStoreMetricsReader.CheckpointerReading"/>
/// does for the checkpointer. The arithmetic is pure so it is tested without a store.
/// </summary>
internal static class DarlingStoreWalReader
{
    /// <summary>The longest window the series covers, one point per sweep, so a week of hourly points is the most a response carries.</summary>
    public const int MaxHoursBack = 168;

    /// <summary>The default window, a day.</summary>
    public const int DefaultHoursBack = 24;

    /// <summary>The grant that answers a missing <c>pg_ls_waldir()</c> privilege. Named once, shown in the read's message.</summary>
    public const string DirGrantMessage =
        "The role the service connects to the store as cannot read pg_wal's size, so the pg_wal figures are empty and are not zero. " +
        "Run GRANT pg_monitor TO <that role>; (or GRANT EXECUTE ON FUNCTION pg_ls_waldir() TO <that role>;). The next hourly row has them.";

    /// <summary>
    /// The WAL rows inside the window plus the newest row before it, which is only the baseline the first point is differenced against.
    /// $1 is the window start, naive UTC. Bounded by the window (at most <see cref="MaxHoursBack"/> hours of hourly rows), so it takes no row cap.
    /// </summary>
    public const string SeriesSql = $@"
SELECT
    metric_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    wal_buffers_full,
    wal_write_count,
    wal_sync_count,
    wal_write_ms,
    wal_sync_ms,
    wal_stats_reset,
    wal_dir_bytes,
    wal_dir_files
FROM collect.store_metrics
WHERE object_kind = '{StoreWalFigures.WalObjectKind}'
AND   metric_time >= coalesce(
          (SELECT max(p.metric_time) FROM collect.store_metrics AS p
           WHERE p.object_kind = '{StoreWalFigures.WalObjectKind}' AND p.metric_time < $1), $1)
ORDER BY metric_time";

    /// <summary>One WAL row as stored. Every counter is the server's cumulative value at <paramref name="MetricTime"/> (naive UTC). The write and sync
    /// figures are null on PostgreSQL 18 and later, and the directory figures are null when the role could not read <c>pg_wal</c>.</summary>
    public sealed record Sample(
        DateTime MetricTime,
        long? Records,
        long? FullPageImages,
        long? Bytes,
        long? BuffersFull,
        long? WriteCount,
        long? SyncCount,
        long? WriteMs,
        long? SyncMs,
        DateTime? StatsReset,
        long? DirBytes,
        long? DirFiles);

    /// <summary>One hour of the series. The delta fields are null unless <paramref name="Status"/> is <c>observed</c>. The directory figures are the row's own
    /// reading at its time, not a difference.</summary>
    public sealed record Point(
        DateTime HourEnding,
        string Status,
        double? IntervalSeconds,
        long? WalBytes,
        long? Records,
        long? FullPageImages,
        long? BuffersFull,
        long? WriteCount,
        long? SyncCount,
        long? WriteMs,
        long? SyncMs,
        long? DirBytes,
        long? DirFiles,
        DateTime? StatsReset);

    /// <summary>The whole answer: a status word, the points, and the state of the <c>pg_wal</c> figures.</summary>
    public sealed record Reading(string Status, string? Message, IReadOnlyList<Point> Points, string DirStatus, string? DirMessage);

    /// <summary>The status words of a point.</summary>
    public const string Observed = "observed";

    /// <summary>The counters or the stats reset stamp say the server's statistics were reset between the two rows: no delta is stated.</summary>
    public const string Reset = "reset";

    /// <summary>The first row the store holds, with no earlier row to difference against.</summary>
    public const string NoPrevious = "no_previous";

    private static long? Delta(long? newer, long? older) => newer is long n && older is long o ? n - o : null;

    /// <summary>
    /// True when the server's statistics were reset between the two rows: <c>pg_stat_wal.stats_reset</c> moved (null to a value counts, since that is the
    /// first reset) or any counter both rows carry fell. The stats reset stamp is the sure sign. A fall is the backstop for a row written before a reset
    /// stamp was readable.
    /// </summary>
    public static bool IsReset(Sample older, Sample newer)
    {
        if (older.StatsReset != newer.StatsReset)
        {
            return true;
        }

        return Delta(newer.Records, older.Records) < 0
            || Delta(newer.FullPageImages, older.FullPageImages) < 0
            || Delta(newer.Bytes, older.Bytes) < 0
            || Delta(newer.BuffersFull, older.BuffersFull) < 0
            || Delta(newer.WriteCount, older.WriteCount) < 0
            || Delta(newer.SyncCount, older.SyncCount) < 0
            || Delta(newer.WriteMs, older.WriteMs) < 0
            || Delta(newer.SyncMs, older.SyncMs) < 0;
    }

    /// <summary>
    /// Turns the rows (oldest first) into points. A row earlier than <paramref name="windowStart"/> is the baseline for the first point and is not itself a
    /// point. The interval is MEASURED from the two stamps, never assumed to be an hour. Pure.
    /// </summary>
    public static IReadOnlyList<Point> BuildPoints(IReadOnlyList<Sample> samples, DateTime windowStart)
    {
        var points = new List<Point>();
        Sample? previous = null;
        foreach (var sample in samples)
        {
            if (previous is not null && sample.MetricTime >= windowStart)
            {
                if (IsReset(previous, sample))
                {
                    points.Add(new Point(sample.MetricTime, Reset, null, null, null, null, null, null, null, null, null,
                        sample.DirBytes, sample.DirFiles, sample.StatsReset));
                }
                else
                {
                    points.Add(new Point(
                        sample.MetricTime,
                        Observed,
                        (sample.MetricTime - previous.MetricTime).TotalSeconds,
                        Delta(sample.Bytes, previous.Bytes),
                        Delta(sample.Records, previous.Records),
                        Delta(sample.FullPageImages, previous.FullPageImages),
                        Delta(sample.BuffersFull, previous.BuffersFull),
                        Delta(sample.WriteCount, previous.WriteCount),
                        Delta(sample.SyncCount, previous.SyncCount),
                        Delta(sample.WriteMs, previous.WriteMs),
                        Delta(sample.SyncMs, previous.SyncMs),
                        sample.DirBytes,
                        sample.DirFiles,
                        sample.StatsReset));
                }
            }
            else if (previous is null && sample.MetricTime >= windowStart)
            {
                points.Add(new Point(sample.MetricTime, NoPrevious, null, null, null, null, null, null, null, null, null,
                    sample.DirBytes, sample.DirFiles, sample.StatsReset));
            }

            previous = sample;
        }

        return points;
    }

    /// <summary>
    /// The reading for the rows. The directory status follows the NEWEST row: its directory figures empty means the role could not read <c>pg_wal</c> when
    /// that row was written, which is the <c>precondition</c> the issue asks for, never a zero. Pure.
    /// </summary>
    public static Reading From(IReadOnlyList<Sample> samples, DateTime windowStart)
    {
        var points = BuildPoints(samples, windowStart);
        if (points.Count == 0)
        {
            return new Reading(
                "empty",
                "No WAL rows in the window. The hourly sweep writes one within an hour of a service start on this build. PostgreSQL below 14 has no pg_stat_wal, so a store on it never has one.",
                points, "empty", null);
        }

        var newest = samples[^1];
        return newest.DirBytes is null
            ? new Reading("ok", null, points, "precondition", DirGrantMessage)
            : new Reading("ok", null, points, "ok", null);
    }

    /// <summary>
    /// Reads <see cref="SeriesSql"/> for the last <paramref name="hoursBack"/> hours and builds the reading. A store below V305 has no WAL columns, and
    /// the read answers <c>unavailable</c> for it instead of an error or an empty series.
    /// </summary>
    public static async Task<Reading> GetAsync(NpgsqlDataSource postgres, int hoursBack, DateTime utcNow, CancellationToken cancellationToken = default)
    {
        var windowStart = DateTime.SpecifyKind(utcNow.AddHours(-hoursBack), DateTimeKind.Unspecified);
        var samples = new List<Sample>();
        try
        {
            await using var command = postgres.CreateCommand(SeriesSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.AddWithValue(windowStart);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                samples.Add(new Sample(
                    reader.GetDateTime(0),
                    Long(reader, 1), Long(reader, 2), Long(reader, 3), Long(reader, 4), Long(reader, 5), Long(reader, 6), Long(reader, 7), Long(reader, 8),
                    reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                    Long(reader, 10), Long(reader, 11)));
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            return new Reading(
                "unavailable",
                "The store's schema is below V305, which adds the WAL columns. Upgrade the service so the store migrates, then wait an hour for the first row.",
                Array.Empty<Point>(), "unavailable", null);
        }

        return From(samples, windowStart);
    }

    private static long? Long(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);

    /// <summary>The block <c>get_store_metrics</c> publishes. Naive-UTC stamps go out as ISO 8601 with a Z.</summary>
    public static object ToBlock(Reading reading, int hoursBack) => new
    {
        status = reading.Status,
        hours_back = hoursBack,
        message = reading.Message,
        pg_wal_status = reading.DirStatus,
        pg_wal_message = reading.DirMessage,
        points = reading.Points.Select(p => new
        {
            hour_ending = Stamp(p.HourEnding),
            status = p.Status,
            interval_seconds = p.IntervalSeconds,
            wal_bytes = p.WalBytes,
            wal_records = p.Records,
            full_page_images = p.FullPageImages,
            wal_buffers_full = p.BuffersFull,
            write_count = p.WriteCount,
            sync_count = p.SyncCount,
            write_ms = p.WriteMs,
            sync_ms = p.SyncMs,
            pg_wal_bytes = p.DirBytes,
            pg_wal_files = p.DirFiles,
            stats_reset = p.StatsReset is DateTime r ? Stamp(r) : null,
        }),
    };

    private static string Stamp(DateTime naiveUtc) =>
        DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
}

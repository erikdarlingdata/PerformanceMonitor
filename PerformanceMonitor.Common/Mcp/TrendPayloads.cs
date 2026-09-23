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
using System.Text.Json;

namespace PerformanceMonitor.Common;

/// <summary>One file I/O series the window saw activity on, in rank order (#3897): a (database, file type)
/// pair, or one file when the read is scoped to a database (<see cref="FileName"/> is null in the first grain).
/// Both SKUs' readers return this shape, so the payload built from it is one builder's output.</summary>
internal sealed record FileIoSeries(
    string DatabaseName, string FileType, string? FileName, long Files, long StallMs, long Ops, long SeriesRank);

/// <summary>One bucketed file I/O point for one series (#3897). Latencies are the bucket's summed stall over
/// its summed operations — null over no operations — and the peaks are the worst single collection's.</summary>
internal sealed record FileIoPoint(
    DateTime BucketStart, string DatabaseName, string FileType, string? FileName,
    long Reads, long Writes, double StallReadMs, double StallWriteMs,
    double? AvgReadLatencyMs, double? AvgWriteLatencyMs, double? PeakReadLatencyMs, double? PeakWriteLatencyMs);

/// <summary>One LCK% wait type over the whole window (#3897): its summed wait over the collections whose
/// interval was knowable, the seconds those covered, and its worst single collection's rate.</summary>
internal sealed record LockWaitType(string WaitType, double TotalWaitMs, double RatedSeconds, double? PeakWaitTimeMsPerSecond);

/// <summary>One bucketed point of the lock-wait FAMILY (#3897): every LCK% type's wait summed per collection,
/// rated over the collection's interval, then time-weighted across the bucket; the peak is the worst single
/// collection's family rate.</summary>
internal sealed record LockWaitPoint(DateTime BucketStart, double WaitTimeMsPerSecond, double? PeakWaitTimeMsPerSecond);

/// <summary>
/// The wire shapes of the two bucketed trends both SKUs serve with the same fields (#3897) —
/// <c>get_file_io_trend</c> and <c>get_lock_wait_trend</c>. Built here, once, from the records above, so Lite and
/// Darling cannot publish different keys, orders or sentences for the same read: the parity their tool bodies
/// used to keep by copying is now kept by construction.
/// </summary>
internal static class TrendPayloads
{
    /// <summary>How many ranked file I/O series keep their own line; the rest fold into one "(other)" line.</summary>
    public const int ChartedSeries = 5;

    /// <summary>The label every dimension of the folded line takes — the custom-view composer's residual label
    /// (<c>ComposeCompiler.OtherSeriesLabel</c>), so the web viewer names a residual one way.</summary>
    public const string OtherLabel = "(other)";

    /// <summary>
    /// How many ranked series keep their own line: all of them when folding would leave a single series in the
    /// "(other)" line — a line pooling one series is that series under a worse name — else <see cref="ChartedSeries"/>.
    /// </summary>
    public static int ChartedFor(int activeSeries) =>
        activeSeries <= ChartedSeries + 1 ? activeSeries : ChartedSeries;

    /// <summary>How many lines the answer draws: the charted series, plus the "(other)" line when anything folds.</summary>
    public static int LinesFor(int activeSeries) =>
        ChartedFor(activeSeries) + (activeSeries > ChartedFor(activeSeries) ? 1 : 0);

    /// <summary>
    /// <c>get_file_io_trend</c>'s data envelope. <paramref name="scope"/> is the database the read was narrowed
    /// to (per-file grain), null for the per-(database, file type) grain. The <c>series</c> legend's totals are
    /// summed from the served points, so the legend and the lines cannot disagree about a series; its file counts
    /// come from the ranking.
    /// </summary>
    public static string FileIoTrend(
        string serverName, int hoursBack, string? scope, IReadOnlyList<FileIoSeries> series, IReadOnlyList<FileIoPoint> points,
        int bucketMinutes, bool requested, int autoBudget, object discontinuities)
    {
        var charted = ChartedFor(series.Count);
        var folded = series.Count - charted;
        var perFile = scope is not null;

        var legend = new List<object>();
        foreach (var s in series.Take(charted))
        {
            legend.Add(Legend(s.DatabaseName, s.FileType, s.FileName, s.Files, perFile,
                points.Where(p => p.DatabaseName == s.DatabaseName && p.FileType == s.FileType && p.FileName == s.FileName)));
        }

        if (folded > 0)
        {
            legend.Add(Legend(OtherLabel, OtherLabel, perFile ? OtherLabel : null, series.Skip(charted).Sum(s => s.Files), perFile,
                points.Where(p => p.DatabaseName == OtherLabel && p.FileType == OtherLabel)));
        }

        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hoursBack,
            ["database_name"] = scope,
            ["series_grain"] = perFile ? "file" : "database_file_type",
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.AggregateNote(bucketMinutes, requested, autoBudget),
            ["series_active"] = series.Count,
            ["series_charted"] = charted,
            ["series_folded"] = folded,
            ["series_note"] = FileIoSeriesNote(charted, folded, perFile),
            ["series"] = legend,
            ["trend"] = points.Select(p => perFile
                ? (object)new
                {
                    time = Stamp(p.BucketStart),
                    database_name = p.DatabaseName,
                    file_type = p.FileType,
                    file_name = p.FileName,
                    reads = p.Reads,
                    writes = p.Writes,
                    avg_read_latency_ms = Round2(p.AvgReadLatencyMs),
                    avg_write_latency_ms = Round2(p.AvgWriteLatencyMs),
                    peak_read_latency_ms = Round2(p.PeakReadLatencyMs),
                    peak_write_latency_ms = Round2(p.PeakWriteLatencyMs),
                }
                : new
                {
                    time = Stamp(p.BucketStart),
                    database_name = p.DatabaseName,
                    file_type = p.FileType,
                    reads = p.Reads,
                    writes = p.Writes,
                    avg_read_latency_ms = Round2(p.AvgReadLatencyMs),
                    avg_write_latency_ms = Round2(p.AvgWriteLatencyMs),
                    peak_read_latency_ms = Round2(p.PeakReadLatencyMs),
                    peak_write_latency_ms = Round2(p.PeakWriteLatencyMs),
                }),
            /* #3653 A5: trailing, after the points, on the data envelope only. */
            ["discontinuities"] = discontinuities,
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>What a line is and how the lines were chosen — the one place the ranking and the fold are said.</summary>
    private static string FileIoSeriesNote(int charted, int folded, bool perFile)
    {
        var grain = perFile
            ? "Each line is one file of the requested database"
            : "Each line is one database's data or log files, pooled (file_type ROWS or LOG); pass database_name to chart one database per file";
        var fold = folded > 0
            ? $" The {charted.ToString(CultureInfo.InvariantCulture)} with the most I/O stall in the window (read + write ms) have their own line; the other {folded.ToString(CultureInfo.InvariantCulture)} are folded into one '{OtherLabel}' line, pooled collection by collection, so its latency is theirs combined."
            : " Every line that did I/O in the window is shown, heaviest stall first.";
        return grain + "." + fold;
    }

    /// <summary>One legend row: the series' window totals, summed from its own served points.</summary>
    private static object Legend(string databaseName, string fileType, string? fileName, long files, bool perFile, IEnumerable<FileIoPoint> ownPoints)
    {
        var mine = ownPoints.ToList();
        var reads = mine.Sum(p => p.Reads);
        var writes = mine.Sum(p => p.Writes);
        var stallRead = mine.Sum(p => p.StallReadMs);
        var stallWrite = mine.Sum(p => p.StallWriteMs);
        var avgRead = reads > 0 ? stallRead / reads : (double?)null;
        var avgWrite = writes > 0 ? stallWrite / writes : (double?)null;
        var peakRead = mine.Max(p => p.PeakReadLatencyMs);
        var peakWrite = mine.Max(p => p.PeakWriteLatencyMs);

        return perFile
            ? new
            {
                database_name = databaseName,
                file_type = fileType,
                file_name = fileName,
                files,
                reads,
                writes,
                avg_read_latency_ms = Round2(avgRead),
                avg_write_latency_ms = Round2(avgWrite),
                peak_read_latency_ms = Round2(peakRead),
                peak_write_latency_ms = Round2(peakWrite),
            }
            : new
            {
                database_name = databaseName,
                file_type = fileType,
                files,
                reads,
                writes,
                avg_read_latency_ms = Round2(avgRead),
                avg_write_latency_ms = Round2(avgWrite),
                peak_read_latency_ms = Round2(peakRead),
                peak_write_latency_ms = Round2(peakWrite),
            };
    }

    /// <summary>
    /// The empty answer for a read scoped to one database that saw no I/O in the window. The unscoped empty
    /// answers keep their sentences (the collector-level two states); this one is about the NAME, because a
    /// misspelt database and an idle one both land here and only a list of the real names tells them apart.
    /// </summary>
    public static string FileIoScopeEmptyMessage(string serverName, string scope, int hoursBack) =>
        $"No read or write activity was recorded for database '{scope}' on {serverName} in the last {hoursBack} hour(s). The name must match a collected database exactly (get_file_io_stats lists them); omit database_name for every database.";

    /// <summary>
    /// <c>get_lock_wait_trend</c>'s data envelope: the family's bucketed rate, and the per-type legend that says
    /// which LCK types made it up. <paramref name="types"/> is every LCK% type collected in the window; the ones
    /// that never waited are counted, not listed.
    /// </summary>
    public static string LockWaitTrend(
        string serverName, int hoursBack, IReadOnlyList<LockWaitType> types, IReadOnlyList<LockWaitPoint> points,
        int bucketMinutes, bool requested, int autoBudget)
    {
        var waited = types.Where(t => t.TotalWaitMs > 0)
            .OrderByDescending(t => t.TotalWaitMs)
            .ThenBy(t => t.WaitType, StringComparer.Ordinal)
            .ToList();
        var idle = types.Count - waited.Count;

        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hoursBack,
            ["bucket"] = TrendBuckets.Word(bucketMinutes),
            ["bucket_minutes"] = bucketMinutes,
            ["aggregate_note"] = TrendBuckets.AggregateNote(bucketMinutes, requested, autoBudget),
            ["wait_types_idle"] = idle,
            ["wait_types_note"] = waited.Count > 0
                ? $"trend is the whole LCK% family summed; wait_types lists every type that waited in the window, heaviest first{(idle > 0 ? $", and {idle.ToString(CultureInfo.InvariantCulture)} more were collected and never waited" : string.Empty)}. Hand one to get_wait_trend to chart it alone."
                : $"No LCK% type waited in this window: the {idle.ToString(CultureInfo.InvariantCulture)} collected all stayed at zero, so the trend below is a measured zero, not missing data.",
            ["wait_types"] = waited.Select(t => new
            {
                wait_type = t.WaitType,
                total_wait_ms = Math.Round(t.TotalWaitMs, 0),
                /* Four places, not the trend's three: a type that waited 24 ms across a day is 0.0003 ms/sec, and
                   rounded to three it would read as a type that never waited — which is what wait_types_idle
                   counts. */
                wait_time_ms_per_second = t.RatedSeconds > 0 ? Math.Round(t.TotalWaitMs / t.RatedSeconds, 4) : (double?)null,
                peak_wait_time_ms_per_second = t.PeakWaitTimeMsPerSecond is { } peak ? Math.Round(peak, 4) : (double?)null,
            }),
            ["trend"] = points.Select(p => new
            {
                collection_time = Stamp(p.BucketStart),
                wait_time_ms_per_second = Math.Round(p.WaitTimeMsPerSecond, 3),
                peak_wait_time_ms_per_second = p.PeakWaitTimeMsPerSecond is { } peak ? Math.Round(peak, 3) : (double?)null,
            }),
        };

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>A point's stamp in the store's own frame (naive UTC, printed like every collection time).</summary>
    private static string Stamp(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified).ToString("o", CultureInfo.InvariantCulture);

    private static double? Round2(double? value) => value.HasValue ? Math.Round(value.Value, 2) : null;
}

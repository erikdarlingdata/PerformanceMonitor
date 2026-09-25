/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// What a trend read is allowed to hand back: the total points it aims for when the caller leaves the width to
/// it (<see cref="AutoPoints"/>), and the most it will serve when the caller names a width (<see cref="MaxPoints"/>).
/// Points are counted across every series, because a payload's size is what the budget protects.
/// </summary>
internal readonly record struct TrendBudget(int AutoPoints, int MaxPoints)
{
    /// <summary>An MCP answer: sized for a model's context, capped at the read's own ceiling.</summary>
    public static TrendBudget Mcp(int maxPoints) => new(TrendBuckets.McpPointBudget, maxPoints);

    /// <summary>The web viewer's charts over <c>/api/read</c>: a browser draws far more points than a model should
    /// read, and a chart at the MCP budget would plot a day of file I/O as a couple of dozen hourly dots. Still a
    /// bound, so a week-long chart no longer ships every collection.</summary>
    public static readonly TrendBudget Chart = new(TrendBuckets.ChartPointBudget, TrendBuckets.ChartPointBudget);
}

/// <summary>
/// How the windowed trend reads size their points (#3897), shared by both SKUs so the same request buckets the same
/// way on Lite and Darling.
///
/// <para><b>Why a trend buckets at all.</b> These reads used to return every collection: a point per minute per
/// series, so a day of <c>get_file_io_trend</c> was 12,500 rows and 1.4 MB, and a week was 9.8 MB — more than a
/// model's whole context for one call, with the store answering in under a quarter of a second. The size was the
/// window times the collection cadence, which nobody chose. Now each point summarizes one fixed-width bucket, the
/// width is chosen so the whole answer lands near <see cref="McpPointBudget"/> points, and the caller can name a
/// width (<c>bucket_minutes</c>) up to the read's own cap.</para>
///
/// <para><b>The width comes from a ladder</b> (<see cref="LadderMinutes"/>) rather than from a division, so a
/// window gets a width a person would pick — 10 minutes for a day, an hour for a week — and every width divides a
/// day, so no bucket straddles UTC midnight. Both SKUs bucket from the same origin (<see cref="OriginSql"/>), so
/// a width that does NOT divide a day, which a caller may still ask for, bins the same rows the same way on both.</para>
///
/// <para><b>Points are counted with the edge bucket</b> (<see cref="PointsFor"/>): a window that does not start on
/// a bucket boundary touches one more bucket than its length divided by the width, so the count that decides the
/// width and the count that enforces the cap are the one the payload will actually hold.</para>
///
/// <para>Refuses rather than clamps (#3541's "refuse what you cannot honor"): a width out of range, or one that
/// would put more points on the wire than the read's cap, is answered with <see cref="McpHelpers.Refusal"/> naming
/// the width that would fit — never quietly served at a different width.</para>
/// </summary>
internal static class TrendBuckets
{
    /// <summary>The widths auto-sizing chooses from, in minutes. Each divides 1,440, so buckets tile a UTC day.</summary>
    public static readonly int[] LadderMinutes = { 1, 2, 5, 10, 15, 30, 60, 120, 180, 240, 360, 720, 1440 };

    /// <summary>The widest bucket a caller may ask for: one day. The same bound <c>get_query_heatmap</c> puts on its bins.</summary>
    public const int MaxBucketMinutes = 1440;

    /// <summary>The total points an MCP answer aims for when the caller leaves the width to the read: a day is
    /// 10-minute points on a single series, a week hourly ones.</summary>
    public const int McpPointBudget = 200;

    /// <summary>The web charts' budget (<see cref="TrendBudget.Chart"/>): a day stays per-minute on one series.</summary>
    public const int ChartPointBudget = 1500;

    /* Each read's cap on an explicitly requested width, sized so the largest answer the read can give stays under
       256 KB (the bytes per point differ fourfold across the reads). The light single-series reads can serve a
       whole day at one-minute points; the wide ones cannot. Darling.Tests' TrendPayloadBudgetLiveTests measures
       every read's largest answer on a week of one-minute collections and pins it under 320 KB. */

    /// <summary><c>get_file_io_trend</c>: about 220 bytes a point.</summary>
    public const int FileIoMaxPoints = 1000;

    /// <summary><c>get_lock_wait_trend</c>: about 120 bytes a point.</summary>
    public const int LockWaitMaxPoints = 2000;

    /// <summary><c>get_query_duration_trend</c> / <c>get_procedure_duration_trend</c>: about 170 bytes a point.</summary>
    public const int DurationMaxPoints = 1500;

    /// <summary><c>get_pg_io_trend</c>: about 400 bytes a point, the widest of the five.</summary>
    public const int PgIoMaxPoints = 650;

    /// <summary><c>get_pg_database_trend</c>: about 270 bytes a point.</summary>
    public const int PgDatabaseMaxPoints = 1000;

    /* The rest of the trend family (#3960), sized the same way. */

    /// <summary><c>get_wait_trend</c>: about 170 bytes a point.</summary>
    public const int WaitMaxPoints = 1500;

    /// <summary><c>get_cpu_utilization</c>: about 190 bytes a point.</summary>
    public const int CpuMaxPoints = 1300;

    /// <summary><c>get_tempdb_trend</c>: about 300 bytes a point.</summary>
    public const int TempDbMaxPoints = 800;

    /// <summary><c>get_memory_trend</c>: about 200 bytes a point.</summary>
    public const int MemoryMaxPoints = 1200;

    /// <summary><c>get_perfmon_trend</c>: about 140 bytes a point.</summary>
    public const int PerfmonMaxPoints = 1800;

    /// <summary><c>get_pg_query_duration_trend</c>: about 170 bytes a point.</summary>
    public const int PgQueryDurationMaxPoints = 1500;

    /// <summary><c>get_pg_cpu_utilization</c> (#4193): about 500 bytes a point, the widest of the family — the
    /// CPU/ACU pair (each with its own peak), the capacity trio and the six V136 host-memory columns.</summary>
    public const int PgCpuMaxPoints = 500;

    /// <summary>
    /// The fixed origin every bucketing read aligns to, as a SQL literal both dialects accept (PostgreSQL
    /// <c>date_bin</c>, DuckDB <c>time_bucket</c>). A midnight, so every ladder width lands on round clock times.
    /// </summary>
    public const string OriginSql = "TIMESTAMP '2000-01-01 00:00:00'";

    /// <summary>The <c>bucket_minutes</c> parameter's description, shared VERBATIM by every bucketed trend on both
    /// SKUs, and short on purpose: descriptions are paid for on every turn (#3898), and the payload's
    /// <c>aggregate_note</c> carries the reading guidance where it is used.</summary>
    public const string BucketMinutesDescription = "Minutes per point (1-1440). Omit to size to the window.";

    /// <summary>
    /// The most points a window can put on the wire at <paramref name="bucketMinutes"/> across
    /// <paramref name="seriesCount"/> series: the window's length in buckets plus the one its unaligned start
    /// cuts into, per series.
    /// </summary>
    public static int PointsFor(int windowMinutes, int bucketMinutes, int seriesCount)
    {
        var buckets = (windowMinutes + bucketMinutes - 1) / bucketMinutes + 1;
        return buckets * Math.Max(1, seriesCount);
    }

    /// <summary>The narrowest ladder width that keeps the window within <paramref name="budget"/> points; a day when
    /// even that does not (unreachable inside the seven-day window cap at any sane series count).</summary>
    public static int AutoMinutes(int windowMinutes, int seriesCount, int budget)
    {
        foreach (var width in LadderMinutes)
        {
            if (PointsFor(windowMinutes, width, seriesCount) <= budget)
            {
                return width;
            }
        }

        return MaxBucketMinutes;
    }

    /// <summary>The narrowest whole-minute width, at or above <paramref name="floor"/>, that fits under
    /// <paramref name="maxPoints"/> — what a cap refusal names as the width that would be served.</summary>
    public static int NarrowestFitting(int windowMinutes, int seriesCount, int maxPoints, int floor = 1)
    {
        for (var width = Math.Max(1, floor); width < MaxBucketMinutes; width++)
        {
            if (PointsFor(windowMinutes, width, seriesCount) <= maxPoints)
            {
                return width;
            }
        }

        return MaxBucketMinutes;
    }

    /// <summary>
    /// Resolves the bucket width for one read: the caller's <paramref name="requested"/> width when it is in range
    /// and fits <paramref name="budget"/>'s cap, the ladder's pick for the budget's auto target when the caller sent
    /// none. Returns null when <paramref name="bucketMinutes"/> is usable, the refusal envelope when it is not.
    /// </summary>
    /// <param name="hoursBack">The window's length (already validated by <see cref="McpHelpers.ValidateWindow"/>).</param>
    /// <param name="requested">The caller's <c>bucket_minutes</c>; null to size automatically.</param>
    /// <param name="seriesCount">How many series the answer will carry (1 for a single-series read).</param>
    /// <param name="budget">The auto target and the explicit cap.</param>
    /// <param name="bucketMinutes">The resolved width, meaningful only when this returns null.</param>
    public static string? Resolve(int hoursBack, int? requested, int seriesCount, TrendBudget budget, out int bucketMinutes)
    {
        var windowMinutes = hoursBack * 60;

        if (requested is not int width)
        {
            bucketMinutes = AutoMinutes(windowMinutes, seriesCount, budget.AutoPoints);
            return null;
        }

        bucketMinutes = width;
        var rangeError = ValidateWidth(width);
        if (rangeError != null)
        {
            return rangeError;
        }

        var points = PointsFor(windowMinutes, width, seriesCount);
        if (points > budget.MaxPoints)
        {
            var fits = NarrowestFitting(windowMinutes, seriesCount, budget.MaxPoints, width);
            var across = seriesCount > 1 ? $" across {seriesCount} series" : string.Empty;
            return McpHelpers.Refusal(
                "bucket_minutes",
                $"bucket_minutes {width} over {hoursBack} hour(s){across} is up to {points.ToString(CultureInfo.InvariantCulture)} points, over this read's {budget.MaxPoints.ToString(CultureInfo.InvariantCulture)}-point cap. Use bucket_minutes {fits} or wider, or narrow hours_back (as_of moves the window) for finer points.");
        }

        return null;
    }

    /// <summary>
    /// The range half of <see cref="Resolve"/>: the refusal for a width outside one minute to one day, null for one
    /// inside it or none. A read that learns its series count from the data (<c>get_file_io_trend</c>) calls this
    /// before reading anything, because an unusable width is a fault in the request whatever the window holds, and
    /// then <see cref="Resolve"/> once it knows how many lines it will draw — so a cap refusal names the width that
    /// fits THOSE lines, not a guess.
    /// </summary>
    public static string? ValidateWidth(int? requested) =>
        requested is int width && (width < 1 || width > MaxBucketMinutes)
            ? McpHelpers.Refusal(
                "bucket_minutes",
                $"Invalid bucket_minutes value '{width}'. Must be between 1 and {MaxBucketMinutes} (one day), or omitted to size the points to the window.")
            : null;

    /// <summary>
    /// The refusal for a width the hourly rollup cannot serve: that tier's points are whole hours, so a width finer
    /// than an hour, or one that is not a whole number of hours, would be a different series than the one asked
    /// for. Null when <paramref name="bucketMinutes"/> is a whole number of hours or was not requested.
    /// </summary>
    public static string? RequireWholeHours(int? requested, int rawRetentionDays)
    {
        if (requested is not int width || width % 60 == 0)
        {
            return null;
        }

        return McpHelpers.Refusal(
            "bucket_minutes",
            $"bucket_minutes {width} cannot be served for this window: it reaches past the raw tier's {rawRetentionDays}-day retention, so the hourly rollup answers it, and that rollup's points are whole hours. Use a multiple of 60, or a window inside the last {rawRetentionDays} days for finer points.");
    }

    /// <summary>
    /// The width an hourly-tier read serves: the caller's own (already refused by <see cref="RequireWholeHours"/>
    /// unless it is a whole number of hours), or the automatic width raised to the rollup's hour — a finer
    /// automatic width would ask the rollup for points it does not hold.
    /// </summary>
    public static int OnHourlyTier(int? requested, int resolved) => requested ?? Math.Max(60, resolved);

    /// <summary>The width as a word for the payload's <c>bucket</c> key: <c>1 minute</c>, <c>10 minutes</c>,
    /// <c>1 hour</c>, <c>6 hours</c>, <c>1 day</c>. The hourly tier's existing spelling (<c>1 hour</c>) is the same
    /// word, so an hourly answer reads as it always did.</summary>
    public static string Word(int bucketMinutes) => bucketMinutes switch
    {
        1 => "1 minute",
        1440 => "1 day",
        60 => "1 hour",
        _ when bucketMinutes % 60 == 0 => $"{bucketMinutes / 60} hours",
        _ => $"{bucketMinutes} minutes",
    };

    /// <summary>The width as the adjective the prose needs (<c>a 10-minute bucket</c>, <c>a 6-hour bucket</c>).</summary>
    public static string Adjective(int bucketMinutes) => bucketMinutes switch
    {
        1440 => "1-day",
        _ when bucketMinutes % 60 == 0 => $"{bucketMinutes / 60}-hour",
        _ => $"{bucketMinutes}-minute",
    };

    /// <summary>
    /// The sentence every bucketed trend of counts and rates owes its reader about what a point IS, shared by both
    /// SKUs and every such read so the words cannot drift. Leads with the grain, because a rolled-up series read as
    /// raw is the one misreading this whole change must not introduce; then how each kind of figure was rolled up
    /// (counts summed, rates and ratios recomputed from those sums — never an average of averages, which would
    /// weight a quiet collection the same as a busy one); then what survives the rollup (the extreme single
    /// collection, so a spike is not averaged away); then how to get finer points. A read of levels says
    /// <see cref="LevelNote"/> instead.
    /// </summary>
    public static string AggregateNote(int bucketMinutes, bool requested, int budgetPoints) =>
        $"Each point summarizes the collections in one {Adjective(bucketMinutes)} bucket and is stamped at the bucket's start (the first point at the window's start). "
        + "Counts are summed across the bucket, and rates and ratios are recomputed from those sums over the seconds the collections covered — never averaged from per-collection values — so a bucket the window cuts short holds smaller counts but a true rate. "
        + "peak_* and worst_* are the single most extreme collection inside the bucket, so a spike survives the rollup. "
        + Sizing(requested, budgetPoints);

    /// <summary>
    /// <see cref="AggregateNote"/>'s twin for a read of LEVELS — memory, tempdb space, CPU percentages, a gauge
    /// counter (#3960) — where summing would be meaningless: each level is averaged over the bucket's samples and the
    /// highest single sample kept as the peak. <paramref name="firstAtWindowStart"/> is false for CPU, whose points
    /// sit on each sample's own instant and a collection can report a sample from before the window's start.
    /// </summary>
    public static string LevelNote(int bucketMinutes, bool requested, int budgetPoints, bool firstAtWindowStart = true) =>
        $"Each point averages the samples in one {Adjective(bucketMinutes)} bucket and is stamped at the bucket's start{(firstAtWindowStart ? " (the first point at the window's start)" : string.Empty)}. "
        + "A level is averaged, never summed, and peak_* is the highest single sample inside the bucket, so a spike survives the rollup. "
        + Sizing(requested, budgetPoints);

    /// <summary>The closing sentence both notes share: where the width came from, and how to ask for another.</summary>
    private static string Sizing(bool requested, int budgetPoints) =>
        requested
            ? "The width is the bucket_minutes you passed."
            : $"The width was chosen to keep this answer near {budgetPoints.ToString(CultureInfo.InvariantCulture)} points; pass bucket_minutes (1-{MaxBucketMinutes}) for another width, or narrow hours_back for finer points.";
}

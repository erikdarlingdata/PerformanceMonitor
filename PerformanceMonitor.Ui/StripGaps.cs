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

namespace PerformanceMonitor.Ui;

/// <summary>
/// #6651 (#6667): the time-range strip's gap rule, one definition for the Darling Viewer's and Lite's
/// <c>TimeRangeSlicerControl</c> (their copies were identical). Beside <see cref="TimeSeriesGaps"/>, which breaks the
/// ScottPlot charts' lines; this one decides where the strip's own line and area stop.
/// </summary>
public static class StripGaps
{
    /* #6651 (#5644 W7): the strip's gap rule is the web strip's (chart-gaps.js gapThresholdMs): a jump between two neighbouring real
       samples of more than three times the series' median spacing, never under five minutes, and a series with no steady cadence (the
       75th percentile of the spacings over three times the 25th) or fewer than two spacings never breaks. */
    private const int GapFactor = 3;
    private static readonly long GapFloorTicks = TimeSpan.FromMinutes(5).Ticks;

    /// <summary>The break threshold in ticks for ascending sample times, or null when nothing breaks (the web's Infinity).</summary>
    private static long? GapThresholdTicks(List<long> ticks)
    {
        var deltas = new List<long>();
        for (var i = 1; i < ticks.Count; i++)
        {
            var d = ticks[i] - ticks[i - 1];
            if (d > 0)
            {
                deltas.Add(d);
            }
        }

        if (deltas.Count < 2)
        {
            return null;
        }

        deltas.Sort();
        long Quantile(double p) => deltas[(int)Math.Round((deltas.Count - 1) * p, MidpointRounding.AwayFromZero)];
        if (Quantile(0.75) > GapFactor * Quantile(0.25))
        {
            return null;
        }

        var mid = deltas.Count / 2;
        var median = deltas.Count % 2 == 1 ? (double)deltas[mid] : (deltas[mid - 1] + deltas[mid]) / 2.0;
        return Math.Max((long)(GapFactor * median), GapFloorTicks);
    }

    /// <summary>
    /// #6651 (#5644 W7): the pieces the strip's line and area are drawn in, as (first index, count) into the filled buckets. The gap
    /// decision is made on the REAL buckets (<paramref name="realTimes"/>, before <c>FillEmptyBuckets</c> gave every hour a
    /// bucket, where no gap shows): where two neighbouring real buckets are further apart than the web rule's threshold, the filled
    /// buckets between them are left out of the drawing and the line breaks across them. The fill still serves the bars and the axis.
    /// A short hole (under the threshold) keeps its zero buckets in the line, as before.
    /// </summary>
    public static List<(int Start, int Count)> RunsOverFilledBuckets(IReadOnlyList<DateTime> bucketTimes, IReadOnlyList<DateTime> realTimes)
    {
        var real = realTimes.Select(t => FloorToHour(t).Ticks).Distinct().OrderBy(t => t).ToList();
        var threshold = GapThresholdTicks(real);
        var gaps = new List<(long Lo, long Hi)>();
        if (threshold.HasValue)
        {
            for (var i = 0; i + 1 < real.Count; i++)
            {
                if (real[i + 1] - real[i] > threshold.Value)
                {
                    gaps.Add((real[i], real[i + 1]));
                }
            }
        }

        var runs = new List<(int Start, int Count)>();
        var start = -1;
        var count = 0;
        for (var i = 0; i < bucketTimes.Count; i++)
        {
            var t = FloorToHour(bucketTimes[i]).Ticks;
            var missing = gaps.Exists(g => t > g.Lo && t < g.Hi);
            if (!missing)
            {
                if (count == 0)
                {
                    start = i;
                }

                count++;
            }

            var breaksAfter = i + 1 < bucketTimes.Count
                && gaps.Exists(g => t >= g.Lo && FloorToHour(bucketTimes[i + 1]).Ticks <= g.Hi);
            if ((missing || breaksAfter || i + 1 == bucketTimes.Count) && count > 0)
            {
                runs.Add((start, count));
                start = -1;
                count = 0;
            }
        }

        return runs;
    }

    private static DateTime FloorToHour(DateTime dt) =>
        new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, dt.Kind);
}

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
/// #7624: the read count an Overview I/O latency point needs behind it before the lane may mark it an anomaly. An average over a
/// handful of reads is noise (one read at 80 ms is a red dot for the whole bucket). This is the web strip's rule
/// (<c>minReadsFor</c> and <c>ANOMALY_MIN_READS_PER_15_MIN</c> in <c>wwwroot/js/pages/overview-strips.js</c>, #7621): 250 reads per fifteen minutes
/// of the span the point's reads were counted over, the bar the PostgreSQL I/O baseline puts on a bucket
/// (<c>PgTargetScorer.IoBaselineBucketMinimumReads</c>, which Lite and this project cannot reach, hence the constant here).
/// </summary>
public static class OverviewIoReadFloor
{
    /// <summary>The reads a bucket needs per fifteen minutes of the span its reads were counted over.</summary>
    public const double MinReadsPer15Minutes = 250;

    /// <summary>
    /// The reads a point counted over <paramref name="spanMinutes"/> needs; a span not stated (zero or less) counts as fifteen minutes,
    /// as the web strip does. Never below one.
    /// </summary>
    public static long MinReadsFor(double spanMinutes)
    {
        var span = spanMinutes > 0 ? spanMinutes : 15;
        return Math.Max(1, (long)Math.Ceiling(MinReadsPer15Minutes * span / 15));
    }

    /// <summary>
    /// The reads every point of the series needs: the floor for the larger of the bucket width and the collection step (the median gap
    /// between the points' own times). A collector scheduled slower than the bucket leaves one point per collection, and that point
    /// holds the reads of the whole step, not of one bucket.
    /// </summary>
    public static long MinReadsFor(int bucketMinutes, IEnumerable<DateTime> pointTimes)
    {
        var times = pointTimes.Distinct().OrderBy(t => t).ToList();
        var span = (double)bucketMinutes;
        if (times.Count > 1)
        {
            var steps = new List<double>(times.Count - 1);
            for (var i = 1; i < times.Count; i++)
                steps.Add((times[i] - times[i - 1]).TotalMinutes);
            steps.Sort();
            var mid = steps.Count >> 1;
            var median = steps.Count % 2 == 1 ? steps[mid] : (steps[mid - 1] + steps[mid]) / 2;
            if (median > span) span = median;
        }

        return MinReadsFor(span);
    }

    /// <summary>
    /// Whether a point is an anomaly: outside the band, above it only at or past the lane's absolute floor, and backed by at least
    /// <paramref name="minReads"/> reads (0 means the lane has no read floor).
    /// </summary>
    public static bool IsAnomaly(double value, double upper, double lower, double minAnomalyValue, long reads, long minReads)
        => (minReads <= 0 || reads >= minReads) && ((value > upper && value >= minAnomalyValue) || value < lower);
}

/*
 * Performance Monitor
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license text.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// How an event-count lane (the Overview's Blocking &amp; Deadlocking lane) sizes its bars and answers a hover (#6284).
/// The reads bucket events to the minute, so one deadlock is one bucket. A bar a fixed 30 seconds wide was a
/// sub-pixel sliver in a 24-hour window (the reporter's "tiny line"), and the hover read 0 unless the pointer sat
/// within a minute of the bucket, which at that zoom is under a pixel. Both now follow the window the lane draws:
/// a bar is about one 150th of the window wide (6 to 7 pixels at a usual plot width) and a hover within that width
/// of a bar reads the bar's own count. Short windows keep the old 30-second bar and one-minute reach.
/// </summary>
internal static class EventLaneGeometry
{
    /// <summary>The narrowest a bar is drawn: 30 seconds, in OLE-automation days.</summary>
    public const double MinBarWidthDays = 30.0 / 86400.0;

    /// <summary>The narrowest hover reach: one minute (the bucket width), in OLE-automation days.</summary>
    public const double MinHoverToleranceDays = 1.0 / 1440.0;

    /// <summary>The share of the window one bar covers. 1/150 is 6 to 7 pixels at a usual lane width.</summary>
    public const double BarShareOfWindow = 1.0 / 150.0;

    /// <summary>The width, in days, to draw a bucket's bar over a window of <paramref name="windowSpanDays"/>.</summary>
    public static double BarWidthDays(double windowSpanDays)
        => double.IsFinite(windowSpanDays) && windowSpanDays > 0
            ? Math.Max(MinBarWidthDays, windowSpanDays * BarShareOfWindow)
            : MinBarWidthDays;

    /// <summary>How far, in days, a pointer may sit from a bucket's time and still read that bucket's count.</summary>
    public static double HoverToleranceDays(double windowSpanDays)
        => Math.Max(MinHoverToleranceDays, BarWidthDays(windowSpanDays));

    /// <summary>
    /// The value of the bucket nearest <paramref name="targetX"/>, null when there is no usable bucket. An event series
    /// whose nearest bucket is further than <paramref name="toleranceDays"/> away reads 0 (nothing happened there); a
    /// level series (CPU, memory) always reads its nearest sample. <paramref name="times"/> is ascending.
    /// </summary>
    public static double? NearestValue(double[]? times, double[]? values, double targetX, bool isEventBased, double toleranceDays)
    {
        if (times == null || values == null || times.Length == 0)
            return null;

        int lo = 0, hi = times.Length - 1;
        while (lo < hi)
        {
            int mid = (lo + hi) / 2;
            if (times[mid] < targetX)
                lo = mid + 1;
            else
                hi = mid;
        }

        int best = lo;
        if (lo > 0 && Math.Abs(times[lo - 1] - targetX) < Math.Abs(times[lo] - targetX))
            best = lo - 1;

        double val = values[best];
        if (double.IsNaN(val)) return null;

        if (isEventBased && Math.Abs(times[best] - targetX) > toleranceDays)
            return 0;

        return val;
    }
}

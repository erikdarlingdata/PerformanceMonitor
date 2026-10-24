/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6629: the time a collector's byte sum was measured over, so a bytes-per-day figure divides by the time actually measured and not by
/// the whole window. A store younger than the window, or one that just upgraded to V219 (only the newest runs carry bytes), has a sum that
/// covers only part of the window: dividing it by the window understates the day's traffic by the ratio. The span runs from the
/// collector's first run in the window that has a byte figure to the window's end (now), at least <see cref="MinimumHours"/> hours (so
/// one run read a minute ago does not scale to a thousand-fold figure) and at most the window. One rule for the web's MCP reads and the
/// Viewer's grid.
/// </summary>
public static class CollectorBytesSpan
{
    /// <summary>The shortest span a per-day figure divides by: one hour.</summary>
    public const double MinimumHours = 1;

    /// <summary>
    /// The days a sum was measured over: <paramref name="nowUtc"/> minus <paramref name="firstMeasuredUtc"/>, floored at
    /// <see cref="MinimumHours"/> and capped at <paramref name="windowDays"/> (the cap wins when the window is shorter than the floor).
    /// </summary>
    public static double MeasuredDays(DateTime firstMeasuredUtc, DateTime nowUtc, double windowDays)
    {
        var days = (nowUtc - firstMeasuredUtc).TotalDays;
        return Math.Min(Math.Max(days, MinimumHours / 24.0), windowDays);
    }

    /// <summary>A byte sum scaled to one day over <paramref name="measuredDays"/>.</summary>
    public static long PerDay(long sum, double measuredDays) => (long)Math.Round(sum / measuredDays);

    /// <summary>The measured span in hours, rounded to one decimal, as the reads report it (<c>bytes_measured_hours</c>).</summary>
    public static double Hours(double measuredDays) => Math.Round(measuredDays * 24.0, 1);
}

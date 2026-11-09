/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The width of a time-range strip's buckets, chosen from the window the strip shows. The strip plots one point per bucket at the bucket's
/// start, so hourly buckets over a one-hour window are one or two points, and the line ends at the last whole hour while the grid below holds
/// rows up to the window's end. A window of up to two hours takes the bucket its collectors fill, a minute; a longer window keeps the hourly
/// strip. The read that groups the rows and the strip that fills and draws them both take the width from here, over the same window, so they
/// agree on every bucket's stamp. Both widths divide a day, so a grid from any midnight (the SQL origin is one, and so is the tick epoch) lands
/// on the same bucket starts.
/// </summary>
public static class SlicerBuckets
{
    /// <summary>The bucket width, in minutes, for a strip over [<paramref name="startUtc"/>, <paramref name="endUtc"/>].</summary>
    public static int MinutesFor(DateTime startUtc, DateTime endUtc) => MinutesFor(endUtc - startUtc);

    /// <summary>The bucket width, in minutes, for a strip over a window of <paramref name="window"/>: a minute up to two hours, else an hour.</summary>
    public static int MinutesFor(TimeSpan window) => window.TotalHours <= 2 ? 1 : 60;

    /// <summary>The start of the <paramref name="minutes"/>-wide bucket <paramref name="at"/> falls in; the kind is kept.</summary>
    public static DateTime Floor(DateTime at, int minutes)
    {
        var width = minutes * TimeSpan.TicksPerMinute;
        return new DateTime(at.Ticks - at.Ticks % width, at.Kind);
    }

    /// <summary>The end of the bucket <paramref name="at"/> falls in, or <paramref name="at"/> itself when it sits on a bucket boundary.</summary>
    public static DateTime Ceiling(DateTime at, int minutes)
    {
        var floored = Floor(at, minutes);
        return floored == at ? at : floored.AddMinutes(minutes);
    }
}

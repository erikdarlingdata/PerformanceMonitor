/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6651 (#5644 W7): the Lite time strip breaks its line and area where the collector was off, by the web strip's rule (three times
/// the median spacing, five-minute floor, no break for an irregular cadence or under two spacings), decided from the real buckets
/// before LoadData fills every hour. These run the path the Queries tabs use: a new control, LoadData, then the pieces it draws.
/// </summary>
public sealed class TimeRangeSlicerGapTests
{
    private static List<TimeSliceBucket> Hourly(DateTime start, int count)
        => Enumerable.Range(0, count).Select(i => new TimeSliceBucket { BucketTime = start.AddHours(i), Value = 10 + i }).ToList();

    private static List<(int Start, int Count)> RunsAfterLoad(List<TimeSliceBucket> real, DateTime windowStart, DateTime windowEnd)
        => StaTestThread.Run(() =>
        {
            var slicer = new TimeRangeSlicerControl();
            slicer.LoadData(real, "Sessions", windowStart, windowEnd);
            return slicer.DrawnRuns.ToList();
        });

    /// <summary>The collector was off overnight: LoadData fills every hour of the window (for the bars and the axis), and the line the
    /// strip draws still breaks across the hours the real buckets did not have. Judged on the filled buckets no gap would ever show.</summary>
    [Fact]
    public void Strip_BreaksWhereTheCollectorWasOff_InsteadOfDrawingAcrossIt()
    {
        var evening = Hourly(new DateTime(2026, 10, 22, 18, 0, 0, DateTimeKind.Utc), 4);   // 18:00-21:00
        var morning = Hourly(new DateTime(2026, 10, 23, 7, 0, 0, DateTimeKind.Utc), 4);    // 07:00-10:00
        var start = new DateTime(2026, 10, 22, 18, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 10, 23, 10, 30, 0, DateTimeKind.Utc);

        var runs = RunsAfterLoad(evening.Concat(morning).ToList(), start, end);

        /* 17 filled hours; indexes 4-12 (22:00-06:00) are the missing stretch and are not drawn. */
        Assert.Equal(new[] { (0, 4), (13, 4) }, runs);
    }

    [Fact]
    public void Strip_WithAShortHole_OrFewBuckets_StaysOneRun()
    {
        var start = new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc);
        var holey = Hourly(start, 5).Concat(Hourly(start.AddHours(7), 5)).ToList();   // two missing hours: a 3h jump is not over 3 x the 1h median
        Assert.Equal(new[] { (0, 12) }, RunsAfterLoad(holey, start, start.AddHours(11)));

        /* Fewer than two spacings have no rhythm to be silent against, so two buckets 9 hours apart never break. */
        var two = new List<TimeSliceBucket> { Hourly(start, 1)[0], Hourly(start.AddHours(9), 1)[0] };
        Assert.Equal(new[] { (0, 10) }, RunsAfterLoad(two, start, start.AddHours(9)));

        var steady = Hourly(start, 20);
        Assert.Equal(new[] { (0, 20) }, RunsAfterLoad(steady, start, start.AddHours(19)));
    }
}

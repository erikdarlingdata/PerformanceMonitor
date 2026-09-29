/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the Overview lanes' X-axis window on a US Eastern server. Each lane sample is plotted at its own instant in
/// the display frame; the axis window has to be the same frame and the same length. A preset range is hoursBack REAL
/// hours: subtracting hoursBack from the display end as wall clock starts it an hour off when the 2026 spring change
/// (8 March, 07:00 UTC, 02:00 EST to 03:00 EDT) falls inside the window. The conversion is the pure
/// <c>ConvertToDisplay</c> core, so the process-wide display statics are not touched.
/// </summary>
public sealed class CorrelatedLanesXAxisWindowTests
{
    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    private static DateTime At(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    [Fact]
    public void PresetRange_ServerTime_AcrossSpringForward_StartsHoursBackRealHoursBeforeNow()
    {
        /* 09:00 UTC on 8 March is 05:00 EDT; six real hours earlier is 03:00 UTC = 22:00 EST the evening before. */
        var (start, end) = CorrelatedTimelineLanesControl.GetXAxisWindow(
            6, null, null, At(2026, 3, 8, 9, 0), TimeDisplayMode.ServerTime, Eastern());

        Assert.Equal(At(2026, 3, 8, 5, 0), end);
        Assert.Equal(At(2026, 3, 7, 22, 0), start);
    }

    [Fact]
    public void PresetRange_UtcDisplay_IsHoursBackOfUtc()
    {
        var (start, end) = CorrelatedTimelineLanesControl.GetXAxisWindow(
            6, null, null, At(2026, 3, 8, 9, 0), TimeDisplayMode.UTC, Eastern());

        Assert.Equal(At(2026, 3, 8, 9, 0), end);
        Assert.Equal(At(2026, 3, 8, 3, 0), start);
    }

    [Fact]
    public void CustomRange_ConvertsEachBoundWithItsOwnOffset()
    {
        /* 12:00 UTC on 1 March is 07:00 EST; 12:00 UTC on 9 March is 08:00 EDT. */
        var (start, end) = CorrelatedTimelineLanesControl.GetXAxisWindow(
            6, At(2026, 3, 1, 12, 0), At(2026, 3, 9, 12, 0), At(2026, 9, 29, 12, 0), TimeDisplayMode.ServerTime, Eastern());

        Assert.Equal(At(2026, 3, 1, 7, 0), start);
        Assert.Equal(At(2026, 3, 9, 8, 0), end);
    }

    [Fact]
    public void OneBoundOnly_FallsBackToThePresetWindow()
    {
        var utcNow = At(2026, 3, 8, 9, 0);
        var preset = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, utcNow, TimeDisplayMode.ServerTime, Eastern());

        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(
            6, At(2026, 3, 1, 12, 0), null, utcNow, TimeDisplayMode.ServerTime, Eastern()));
        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(
            6, null, At(2026, 3, 9, 12, 0), utcNow, TimeDisplayMode.ServerTime, Eastern()));
    }
}

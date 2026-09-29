/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the Overview lanes' X-axis window. Each lane sample is plotted at its own UTC instant, so the axis window is
/// the UTC window as is, in whatever display zone the labels are drawn: a preset range is hoursBack REAL hours, which
/// wall-clock arithmetic in a zone with a clock change inside the window (the 2026 US spring change is 8 March, 07:00
/// UTC, 02:00 EST to 03:00 EDT) would start an hour off. The window is pure, so the process-wide display statics are
/// not touched.
/// </summary>
public sealed class CorrelatedLanesXAxisWindowTests
{
    private static DateTime At(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    [Fact]
    public void PresetRange_AcrossSpringForward_StartsHoursBackRealHoursBeforeNow()
    {
        /* 09:00 UTC on 8 March, six real hours back is 03:00 UTC, the same six hours on an Eastern axis that
           crosses the clock change. */
        var (start, end) = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, At(2026, 3, 8, 9, 0));

        Assert.Equal(At(2026, 3, 8, 9, 0), end);
        Assert.Equal(At(2026, 3, 8, 3, 0), start);
        Assert.Equal(TimeSpan.FromHours(6), end - start);
    }

    [Fact]
    public void CustomRange_IsTheTypedUtcInstantsAsIs()
    {
        /* The bounds are UTC instants, on either side of a clock change in any zone: no offset is applied. */
        var (start, end) = CorrelatedTimelineLanesControl.GetXAxisWindow(
            6, At(2026, 3, 1, 12, 0), At(2026, 3, 9, 12, 0), At(2026, 9, 29, 12, 0));

        Assert.Equal(At(2026, 3, 1, 12, 0), start);
        Assert.Equal(At(2026, 3, 9, 12, 0), end);
    }

    [Fact]
    public void OneBoundOnly_FallsBackToThePresetWindow()
    {
        var utcNow = At(2026, 3, 8, 9, 0);
        var preset = CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, null, utcNow);

        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(6, At(2026, 3, 1, 12, 0), null, utcNow));
        Assert.Equal(preset, CorrelatedTimelineLanesControl.GetXAxisWindow(6, null, At(2026, 3, 9, 12, 0), utcNow));
    }
}

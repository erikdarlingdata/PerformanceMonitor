/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: every window is a pair of UTC instants from the moment it is made to the read that fetches it. The chart
/// axis is the preset's real hours or the custom pair as held; a drill is real minutes around an instant; the
/// Overview comparison is the one window defined by a wall clock ("the same hours yesterday"), and it is aligned on
/// the server's zone through <see cref="TimeZoneInfo"/> because the shared chart project does not reference the
/// server-clock type.
/// </summary>
public sealed class TimeWindowsTests
{
    [Fact]
    public void ADrillInTheRepeatedHour_OpensSixtyRealMinutes()
    {
        /* The chart X is the UTC instant, so a click at 05:45Z (the first 01:45) and one at 06:30Z (the second 01:30) each
           open the sixty real minutes around them. Wall-clock arithmetic would read both as a 01:xx point and collapse
           the window to one instant. */
        var first = TimeWindows.Drill(DisplayZoneFixtures.At(2026, 11, 1, 5, 45), 30, 30);
        var second = TimeWindows.Drill(DisplayZoneFixtures.At(2026, 11, 1, 6, 30), 30, 30);

        Assert.Equal((DisplayZoneFixtures.At(2026, 11, 1, 5, 15), DisplayZoneFixtures.At(2026, 11, 1, 6, 15)), first);
        Assert.Equal((DisplayZoneFixtures.At(2026, 11, 1, 6, 0), DisplayZoneFixtures.At(2026, 11, 1, 7, 0)), second);
        Assert.Equal(TimeSpan.FromMinutes(60), first.ToUtc - first.FromUtc);
        Assert.Equal(TimeSpan.FromMinutes(60), second.ToUtc - second.FromUtc);
        Assert.Equal(DateTimeKind.Unspecified, first.FromUtc.Kind);
    }

    [Fact]
    public void ADrill_IsRealMinutesBeforeAndAfter_WhateverTheSplit()
    {
        var window = TimeWindows.Drill(DisplayZoneFixtures.At(2026, 3, 8, 7, 10), 15, 45);

        Assert.Equal((DisplayZoneFixtures.At(2026, 3, 8, 6, 55), DisplayZoneFixtures.At(2026, 3, 8, 7, 55)), window);
    }

    [Fact]
    public void TheChartAxis_IsThePresetsRealHours_OrTheCustomPairAsHeld()
    {
        var now = DisplayZoneFixtures.At(2026, 3, 8, 8, 30);

        /* A 4-hour preset across the spring change is 4 real hours: 04:30Z to 08:30Z, not 4 wall-clock hours. */
        Assert.Equal((DisplayZoneFixtures.At(2026, 3, 8, 4, 30), now), TimeWindows.ChartAxis(4, null, null, now));

        var custom = TimeWindows.ChartAxis(4, DisplayZoneFixtures.At(2026, 11, 1, 6, 30), DisplayZoneFixtures.At(2026, 11, 1, 7, 30), now);
        Assert.Equal((DisplayZoneFixtures.At(2026, 11, 1, 6, 30), DisplayZoneFixtures.At(2026, 11, 1, 7, 30)), custom);

        /* A lone bound is not a range: the preset stands. */
        Assert.Equal((DisplayZoneFixtures.At(2026, 3, 8, 4, 30), now), TimeWindows.ChartAxis(4, DisplayZoneFixtures.At(2026, 1, 1, 0), null, now));
    }

    [Fact]
    public void OverviewReference_AlignsOnTheServerWallClock()
    {
        /* Current 13:00Z-17:00Z on the spring change day is 09:00-13:00 on the server's clock (EDT). Yesterday's 09:00-13:00 was
           in EST, an hour later in UTC: 14:00Z-18:00Z. Subtracting 24 real hours would give 13:00Z-17:00Z, an hour off. */
        var reference = TimeWindows.OverviewReference(
            DisplayZoneFixtures.At(2026, 3, 8, 13), DisplayZoneFixtures.At(2026, 3, 8, 17), 1, DisplayZoneFixtures.Eastern);

        Assert.Equal((DisplayZoneFixtures.At(2026, 3, 7, 14), DisplayZoneFixtures.At(2026, 3, 7, 18)), reference);

        /* A row of that window lands back on the current axis at the same wall clock. */
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 13), TimeWindows.GhostX(DisplayZoneFixtures.At(2026, 3, 7, 14), 1, DisplayZoneFixtures.Eastern));
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 17), TimeWindows.GhostX(DisplayZoneFixtures.At(2026, 3, 7, 18), 1, DisplayZoneFixtures.Eastern));
    }

    [Fact]
    public void OverviewReference_ReadsAFromAndAToOfTheRepeatedHourByTheRangeRule()
    {
        /* The current window 2026-11-02 05:00Z-06:00Z is 00:00-01:00 on the server's clock (EST). One day back is 00:00-01:00
           on the autumn change day. 00:00 happened once (04:00Z); 01:00 happened twice (05:00Z and 06:00Z), and the end of
           a range takes the later, the widest instants those labels can mean. */
        var reference = TimeWindows.OverviewReference(
            DisplayZoneFixtures.At(2026, 11, 2, 5), DisplayZoneFixtures.At(2026, 11, 2, 6), 1, DisplayZoneFixtures.Eastern);

        Assert.Equal((DisplayZoneFixtures.At(2026, 11, 1, 4), DisplayZoneFixtures.At(2026, 11, 1, 6)), reference);
    }

    [Fact]
    public void AGhostRow_ThatLandsInTheRepeatedHour_TakesTheFirstOccurrence()
    {
        /* A row from the 31st at 01:30 (05:30Z, EDT) shifted forward one day is 01:30 on the 1st: the head's rule for a
           repeated stamp is the first occurrence. */
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 5, 30),
            TimeWindows.GhostX(DisplayZoneFixtures.At(2026, 10, 31, 5, 30), 1, DisplayZoneFixtures.Eastern));
    }
}

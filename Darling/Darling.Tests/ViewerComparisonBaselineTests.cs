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
/// The Queries tab's Compare baseline (<see cref="ViewerServerTab.ShiftComparisonRange"/>): Yesterday is one day
/// back, Last week and Same day last week are seven days back, and the baseline never overlaps the current
/// window. A window longer than the shift (a 7-day window compared with Yesterday) ends its baseline where the
/// current window starts, so the two periods are disjoint and the grid compares the window with the period
/// before it rather than mostly with itself.
/// </summary>
public sealed class ViewerComparisonBaselineTests
{
    private static readonly DateTime End = new(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void Yesterday_OnAnEightHourWindow_IsAStraightShift()
    {
        var start = End.AddHours(-8);

        var baseline = ViewerServerTab.ShiftComparisonRange(1, start, End);

        Assert.NotNull(baseline);
        Assert.Equal(start.AddDays(-1), baseline!.Value.From);
        Assert.Equal(End.AddDays(-1), baseline.Value.To);
    }

    [Fact]
    public void LastWeek_OnASevenDayWindow_IsAStraightShift()
    {
        var start = End.AddDays(-7);

        var baseline = ViewerServerTab.ShiftComparisonRange(2, start, End);

        Assert.NotNull(baseline);
        Assert.Equal(start.AddDays(-7), baseline!.Value.From);
        Assert.Equal(End.AddDays(-7), baseline.Value.To);
        Assert.Equal(ViewerServerTab.ShiftComparisonRange(3, start, End), baseline);
    }

    [Fact]
    public void Yesterday_OnASevenDayWindow_EndsWhereTheCurrentWindowStarts()
    {
        var start = End.AddDays(-7);

        var baseline = ViewerServerTab.ShiftComparisonRange(1, start, End);

        Assert.NotNull(baseline);
        Assert.Equal(start.AddDays(-1), baseline!.Value.From);
        Assert.Equal(start, baseline.Value.To);
    }

    [Fact]
    public void LastWeek_OnAThirtyDayWindow_EndsWhereTheCurrentWindowStarts()
    {
        var start = End.AddDays(-30);

        var baseline = ViewerServerTab.ShiftComparisonRange(2, start, End);

        Assert.NotNull(baseline);
        Assert.Equal(start.AddDays(-7), baseline!.Value.From);
        Assert.Equal(start, baseline.Value.To);
    }

    [Fact]
    public void None_IsNoComparison()
    {
        Assert.Null(ViewerServerTab.ShiftComparisonRange(0, End.AddHours(-1), End));
        Assert.Null(ViewerServerTab.ShiftComparisonRange(-1, End.AddHours(-1), End));
    }
}

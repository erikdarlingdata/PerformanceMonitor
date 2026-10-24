/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6629: bytes per day divide by the time actually measured, not the whole window. The rule is one helper, called by the web's MCP reads
/// and by the Viewer.
/// </summary>
public sealed class CollectorBytesSpanTests
{
    private static readonly DateTime Now = new(2026, 10, 20, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TwentyFiveMinutesMeasured_DividesByTheOneHourFloor()
    {
        var days = CollectorBytesSpan.MeasuredDays(Now.AddMinutes(-25), Now, 7);

        Assert.Equal(1.0 / 24, days, 9);
        Assert.Equal(1.0, CollectorBytesSpan.Hours(days));
        Assert.Equal(2_400, CollectorBytesSpan.PerDay(100, days));
    }

    [Fact]
    public void ThreeDaysMeasuredInASevenDayWindow_DividesByThreeDays()
    {
        var days = CollectorBytesSpan.MeasuredDays(Now.AddDays(-3), Now, 7);

        Assert.Equal(3.0, days, 9);
        Assert.Equal(72.0, CollectorBytesSpan.Hours(days));
        Assert.Equal(1_000, CollectorBytesSpan.PerDay(3_000, days));
    }

    [Fact]
    public void AFullWindow_DividesByTheWholeWindow()
    {
        var days = CollectorBytesSpan.MeasuredDays(Now.AddDays(-7), Now, 7);

        Assert.Equal(7.0, days, 9);
        Assert.Equal(168.0, CollectorBytesSpan.Hours(days));
        Assert.Equal(1_000, CollectorBytesSpan.PerDay(7_000, days));
    }

    [Theory]
    [InlineData(-9, 7)]
    [InlineData(-400, 7)]
    public void ARunOlderThanTheWindow_IsCappedAtTheWindow(double daysAgo, double windowDays)
    {
        Assert.Equal(windowDays, CollectorBytesSpan.MeasuredDays(Now.AddDays(daysAgo), Now, windowDays), 9);
    }

    [Fact]
    public void AWindowShorterThanTheFloor_IsCappedAtTheWindow_NotTheFloor()
    {
        var window = 0.5 / 24;

        Assert.Equal(window, CollectorBytesSpan.MeasuredDays(Now.AddMinutes(-10), Now, window), 9);
    }

    [Fact]
    public void AFirstRunInTheFuture_StillGetsTheFloor()
    {
        Assert.Equal(1.0 / 24, CollectorBytesSpan.MeasuredDays(Now.AddMinutes(5), Now, 7), 9);
    }
}

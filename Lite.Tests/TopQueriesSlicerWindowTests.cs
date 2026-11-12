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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Queries grids read the window the time strip was loaded with, and the strip's quick-range chips and handles send a range
/// inside that window. A range that reaches past the window's ends reads rows the grid's own load never reads, so the grid fills from
/// the "All" chip and empties again on the next refresh. Built on the shared STA thread from the real control, loaded the way the
/// Top Queries by Duration tab loads it (a window that does not start on the hour), with the chip clicked.
/// </summary>
public sealed class TopQueriesSlicerWindowTests
{
    private static readonly DateTime WindowStart = new(2026, 10, 22, 10, 37, 20, DateTimeKind.Utc);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(24);

    private static List<TimeSliceBucket> Hourly(DateTime start, int count)
        => Enumerable.Range(0, count).Select(i => new TimeSliceBucket { BucketTime = start.AddHours(i), Value = 10 + i }).ToList();

    private static SlicerRangeEventArgs ClickChip(string chip)
        => StaTestThread.Run(() =>
        {
            var slicer = new TimeRangeSlicerControl();
            slicer.Measure(new Size(800, 130));
            slicer.Arrange(new Rect(0, 0, 800, 130));
            slicer.LoadData(Hourly(new DateTime(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc), 23), "Total CPU (ms)", WindowStart, WindowEnd);

            SlicerRangeEventArgs? sent = null;
            slicer.RangeChanged += (_, e) => sent = e;

            var chips = (StackPanel)slicer.FindName("QuickRangeChips");
            var button = chips.Children.OfType<Button>().Single(b => Equals(b.Content, chip));
            button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

            Assert.NotNull(sent);
            return sent!;
        });

    /// <summary>"All" selects the whole loaded window, so the range it sends is that window: not the hour before its start and not the
    /// hour after its end. The grid then reads exactly what its own load read, and the two agree.</summary>
    [Fact]
    public void AllChip_SendsTheLoadedWindow_NotTheHoursAroundIt()
    {
        var sent = ClickChip("All");

        Assert.Equal(WindowStart, sent.StartUtc);
        Assert.Equal(WindowEnd, sent.EndUtc);
    }

    /// <summary>A chip inside the window still lands its start on the hour the strip's buckets draw, and its end is the window's end,
    /// never the hour after it.</summary>
    [Fact]
    public void OneHourChip_SnapsItsStartToTheHour_AndEndsAtTheWindowEnd()
    {
        var sent = ClickChip("1h");

        Assert.Equal(new DateTime(2026, 10, 23, 9, 0, 0, DateTimeKind.Utc), sent.StartUtc);
        Assert.Equal(WindowEnd, sent.EndUtc);
    }
}

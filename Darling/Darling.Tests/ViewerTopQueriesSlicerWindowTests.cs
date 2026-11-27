/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's twin of Lite's time-strip window test: the Queries grids read the window the strip was loaded with, and the strip's
/// quick-range chips send a range inside that window, so the "All" chip reads exactly what the grid's own load read.
/// </summary>
public sealed class ViewerTopQueriesSlicerWindowTests
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

    [Fact]
    public void AllChip_SendsTheLoadedWindow_NotTheHoursAroundIt()
    {
        var sent = ClickChip("All");

        Assert.Equal(WindowStart, sent.StartUtc);
        Assert.Equal(WindowEnd, sent.EndUtc);
    }

    [Fact]
    public void OneHourChip_SnapsItsStartToTheHour_AndEndsAtTheWindowEnd()
    {
        var sent = ClickChip("1h");

        Assert.Equal(new DateTime(2026, 10, 23, 9, 0, 0, DateTimeKind.Utc), sent.StartUtc);
        Assert.Equal(WindowEnd, sent.EndUtc);
    }

    /// <summary>
    /// A selection that begins inside the strip starts on a bar's left edge, and that bar counts a row stamped on the edge. The grid
    /// reads leave out a row stamped on their start, so the selection hands them a start one microsecond (the store's precision)
    /// earlier and the row on the edge is read.
    /// </summary>
    [Fact]
    public void ASelectionInsideTheStrip_HandsTheGridReadsAStartOneMicrosecondEarlier()
    {
        var sent = ClickChip("1h");

        Assert.True(sent.StartUtc > WindowStart);
        Assert.Equal(sent.StartUtc.AddTicks(-TimeSpan.TicksPerMicrosecond), sent.ReadStartUtc);
    }

    /// <summary>
    /// "All" selects the loaded window, which the grid's own load reads without a row stamped on its start: the selection's read
    /// start is the window's start, so it reads the rows the load read.
    /// </summary>
    [Fact]
    public void ASelectionOfTheWholeStrip_HandsTheGridReadsTheWindowStart()
    {
        var sent = ClickChip("All");

        Assert.Equal(WindowStart, sent.ReadStartUtc);
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Shapes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Deadlocks strip keeps its hourly buckets over a short window. Past hour at 13:25 with two deadlocks at 12:40 and 12:50: the
/// read returns one bucket stamped 12:00 with a count of 2, and the strip draws it (two hourly stamps, 12:00 and 13:00, and an area with
/// height) instead of walking minute stamps from 12:25 and dropping the bucket stamped before the window.
/// </summary>
public sealed class ViewerSlicerHourlyBucketsTests
{
    [Fact]
    public void TheDeadlocksStrip_KeepsAnHourlyBucketStampedBeforeAOneHourWindow()
    {
        var start = new DateTime(2026, 11, 7, 12, 25, 0, DateTimeKind.Utc);
        var end = start.AddHours(1);
        var buckets = new List<TimeSliceBucket> { new() { BucketTime = new DateTime(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc), SessionCount = 2, Value = 2 } };

        var (drawnBuckets, areaHeight) = StaTestThread.Run(() =>
        {
            var slicer = new TimeRangeSlicerControl { Width = 800, Height = 130 };
            slicer.Measure(new Size(800, 130));
            slicer.Arrange(new Rect(0, 0, 800, 130));
            slicer.UpdateLayout();
            slicer.LoadData(buckets, "Deadlocks", start, end);
            slicer.Redraw();

            var area = slicer.SlicerCanvas.Children.OfType<Path>().Single(p => p.Fill is not null && p.Stroke is null);
            return (slicer.DrawnRuns.Sum(r => r.Count), area.Data.Bounds.IsEmpty ? 0 : area.Data.Bounds.Height);
        });

        Assert.Equal(2, drawnBuckets);
        Assert.True(areaHeight > 1, $"the strip is flat: the area is {areaHeight:F1} px tall");
    }
}

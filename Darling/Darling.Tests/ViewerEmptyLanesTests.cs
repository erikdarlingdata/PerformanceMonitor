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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>#8644: the Viewer's Overview lanes draw the one empty look, and a lane that drew it gets its UTC time axis back when rows arrive.</summary>
public sealed class ViewerEmptyLanesTests
{
    private static List<(double Time, double Value)> Series(double from, double to, double low, double high) =>
        Enumerable.Range(0, 24).Select(i => (from + (to - from) * i / 23.0, low + (high - low) * i / 23.0)).ToList();

    private static ScottPlot.WPF.WpfPlot[] LaneCharts(CorrelatedTimelineLanesControl lanes) =>
        [lanes.CpuChart, lanes.WaitStatsChart, lanes.BlockingChart, lanes.MemoryChart, lanes.FileIoChart];

    private static void DrawLanes(CorrelatedTimelineLanesControl lanes, bool withData)
    {
        var to = DateTime.UtcNow.ToOADate();
        var from = DateTime.UtcNow.AddHours(-6).ToOADate();
        List<(double Time, double Value)> Rows(double low, double high) => withData ? Series(from, to, low, high) : [];
        lanes.UpdateCpuLane(Rows(5, 90), Rows(8, 95));
        lanes.UpdateLane(lanes.WaitStatsChart, "Wait", Rows(150, 900), "#FFB74D");
        lanes.UpdateBlockingLane(Rows(1, 6), Rows(0, 2));
        lanes.UpdateLane(lanes.MemoryChart, "Buffer Pool", Rows(2000, 9000), "#4FC3F7");
        lanes.UpdateLane(lanes.FileIoChart, "I/O", Rows(1, 40), "#81C784");
    }

    [Fact]
    public void AnEmptyLane_IsTheEmptyLook_AndGetsItsUtcTimeAxisBack_WhenRowsArrive() =>
        StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl();

            DrawLanes(lanes, withData: false);
            foreach (var chart in LaneCharts(lanes))
            {
                Assert.False(chart.Plot.Axes.Left.IsVisible, "the empty lane still draws its Y axis");
                Assert.False(chart.Plot.Axes.Bottom.IsVisible, "the empty lane still draws its X axis");
                Assert.False(chart.Plot.Grid.IsVisible, "the empty lane still draws gridlines");
                Assert.Contains(chart.Plot.GetPlottables<ScottPlot.Plottables.Annotation>(), a => a.LabelText.Replace("\n", " ").StartsWith("No ", StringComparison.Ordinal));
            }

            DrawLanes(lanes, withData: true);
            foreach (var chart in LaneCharts(lanes))
            {
                Assert.IsType<DisplayZoneTickGenerator>(chart.Plot.Axes.Bottom.TickGenerator);
                Assert.True(chart.Plot.Axes.Bottom.IsVisible);
                Assert.True(chart.Plot.Axes.Left.IsVisible);
                Assert.True(chart.Plot.Grid.IsVisible);
            }
        });

    [Fact]
    public void AnEmptyLane_KeepsTheSharedXRange_SoTheLanesStayAligned() =>
        StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl();
            DrawLanes(lanes, withData: true);
            lanes.UpdateLane(lanes.MemoryChart, "Buffer Pool", [], "#4FC3F7");
            lanes.SyncXAxes(6, null, null);

            var limits = LaneCharts(lanes).Select(c => c.Plot.Axes.GetLimits()).ToList();
            Assert.All(limits, l => Assert.Equal(limits[0].Left, l.Left, precision: 9));
            Assert.All(limits, l => Assert.Equal(limits[0].Right, l.Right, precision: 9));
            Assert.True(limits[0].Right > limits[0].Left + 0.1, "the shared range is the window, not the empty look's -1 to 1");
        });
}

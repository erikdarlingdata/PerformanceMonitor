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
using ScottPlot.Plottables;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7571: the Current Waits tab's Waiting Tasks and Blocked Sessions charts read one row per collection time and key. A
/// line per key joined moments hours apart into a ramp, and fell straight down where two keys shared a moment. Each chart
/// now draws a bar at each collection time with the keys stacked on it, so the tests draw the chart's own bars and read
/// back what is on the plot.
/// </summary>
public sealed class ViewerCurrentWaitsStackedBarTests
{
    private static readonly DateTime T0 = new(2026, 11, 1, 3, 0, 0, DateTimeKind.Utc);

    private static double Oa(DateTime t) => t.ToOADate();

    private static List<WaitingTaskTrendPoint> Waits(params (DateTime Time, string Type, long Ms)[] rows)
        => rows.Select(r => new WaitingTaskTrendPoint(r.Time, r.Type, r.Ms)).ToList();

    [Fact]
    public void TwoWaitTypesAtOneTime_AreOneBarPositionWithTwoStackedSegments_NotALine()
    {
        var plot = StaTestThread.Run(() =>
        {
            var p = new ScottPlot.Plot();
            var data = Waits((T0, "LATCH_EX", 4), (T0, "CXCONSUMER", 164));
            var tallest = ViewerServerTab.AddWaitingTaskBars(p, null, data, Oa(T0.AddDays(-1)), Oa(T0));
            Assert.Equal(168, tallest);
            return p;
        });

        Assert.Empty(plot.GetPlottables<Scatter>());
        var bars = plot.GetPlottables<BarPlot>().ToList();
        Assert.Equal(2, bars.Count);

        /* Wait types in name order: CXCONSUMER on the bottom, LATCH_EX stacked on it, both at the one position. */
        Assert.Equal(new[] { "CXCONSUMER", "LATCH_EX" }, bars.Select(b => b.LegendText).ToArray());
        var bottom = Assert.Single(bars[0].Bars);
        var top = Assert.Single(bars[1].Bars);
        Assert.Equal(Oa(T0), bottom.Position, 6);
        Assert.Equal(bottom.Position, top.Position);
        Assert.Equal((0d, 164d), (bottom.ValueBase, bottom.Value));
        Assert.Equal((164d, 168d), (top.ValueBase, top.Value));
    }

    [Fact]
    public void AFewCollectionTimesHoursApart_StayWideEnoughToSee()
    {
        var window = (Start: T0.AddHours(-24), End: T0);
        var plot = StaTestThread.Run(() =>
        {
            var p = new ScottPlot.Plot();
            var data = Waits((T0.AddHours(-20), "CXCONSUMER", 40), (T0.AddHours(-9), "CXCONSUMER", 130), (T0.AddHours(-1), "LATCH_EX", 4));
            ViewerServerTab.AddWaitingTaskBars(p, null, data, Oa(window.Start), Oa(window.End));
            return p;
        });

        var all = plot.GetPlottables<BarPlot>().SelectMany(b => b.Bars).ToList();
        Assert.Equal(3, all.Count);
        Assert.All(all, b => Assert.Equal(1.0 / 120, b.Size, 6)); /* a 24 hour window: 12 minutes a bar, not the 2 minutes of the closest collections */
    }

    [Fact]
    public void ManyCollectionTimes_GetBarsNarrowerThanTheirSpacing()
    {
        var plot = StaTestThread.Run(() =>
        {
            var p = new ScottPlot.Plot();
            var data = Waits(Enumerable.Range(0, 60).Select(i => (T0.AddMinutes(i), "CXCONSUMER", 10L + i)).ToArray());
            ViewerServerTab.AddWaitingTaskBars(p, null, data, Oa(T0.AddMinutes(-5)), Oa(T0.AddHours(1)));
            return p;
        });

        var bars = Assert.Single(plot.GetPlottables<BarPlot>()).Bars;
        Assert.Equal(60, bars.Count());
        Assert.All(bars, b => Assert.True(b.Size < 1.0 / 1440, "bars at one-minute spacing must not touch their neighbours"));
    }

    [Fact]
    public void BlockedSessions_StackByDatabase_AtEachCollectionTime()
    {
        var plot = StaTestThread.Run(() =>
        {
            var p = new ScottPlot.Plot();
            var data = new List<BlockedSessionTrendPoint>
            {
                new(T0, "Sales", 3),
                new(T0, "Orders", 2),
                new(T0.AddHours(2), "Sales", 1),
            };
            Assert.Equal(5, ViewerServerTab.AddBlockedSessionBars(p, null, data, Oa(T0.AddHours(-1)), Oa(T0.AddHours(3))));
            return p;
        });

        Assert.Empty(plot.GetPlottables<Scatter>());
        var bars = plot.GetPlottables<BarPlot>().ToList();
        Assert.Equal(new[] { "Orders", "Sales" }, bars.Select(b => b.LegendText).ToArray());
        Assert.Equal((0d, 2d), (bars[0].Bars.Single().ValueBase, bars[0].Bars.Single().Value));
        Assert.Equal(new[] { (2d, 5d), (0d, 1d) }, bars[1].Bars.Select(b => (b.ValueBase, b.Value)).ToArray());
    }

    [Fact]
    public void HoverOverAStackedSegment_NamesItsWaitTypeItsOwnValueAndTheTime()
    {
        var text = StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "ms", displayZone: () => TimeZoneInfo.Utc);
            var data = Waits((T0, "LATCH_EX", 4), (T0, "CXCONSUMER", 164), (T0.AddHours(3), "CXCONSUMER", 40));
            ViewerServerTab.AddWaitingTaskBars(chart.Plot, hover, data, Oa(T0.AddHours(-1)), Oa(T0.AddHours(4)));
            chart.Plot.Axes.AutoScale();
            using var render = chart.Plot.GetImage(1000, 400);

            return hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(Oa(T0), 168)));
        });

        Assert.NotNull(text);
        var lines = text!.Split('\n');
        Assert.Equal("LATCH_EX", lines[0]);
        Assert.Equal("4 ms", lines[1]);
        Assert.Equal("03:00:00", lines[2]);
    }
}

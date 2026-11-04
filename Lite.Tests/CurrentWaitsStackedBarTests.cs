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
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using ScottPlot.Plottables;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7571: the Current Waits tab's Waiting Tasks and Blocked Sessions charts read one row per collection time and key. A
/// line per key joined moments hours apart into a ramp, and fell straight down where two keys shared a moment. Each chart
/// now draws a bar at each collection time with the keys stacked on it, so the tests draw the chart's own bars and read
/// back what is on the plot.
/// </summary>
public sealed class CurrentWaitsStackedBarTests
{
    private static readonly DateTime T0 = new(2026, 11, 1, 3, 0, 0, DateTimeKind.Utc);

    private static double Oa(DateTime t) => t.ToOADate();

    private static List<WaitingTaskTrendPoint> Waits(params (DateTime Time, string Type, long Ms)[] rows)
        => rows.Select(r => new WaitingTaskTrendPoint { CollectionTime = r.Time, WaitType = r.Type, TotalWaitMs = r.Ms }).ToList();

    [Fact]
    public void TwoWaitTypesAtOneTime_AreOneBarPositionWithTwoStackedSegments_NotALine()
    {
        var plot = StaTestThread.Run(() =>
        {
            var p = new ScottPlot.Plot();
            var data = Waits((T0, "LATCH_EX", 4), (T0, "CXCONSUMER", 164));
            var tallest = ServerTab.AddWaitingTaskBars(p, null, data, Oa(T0.AddDays(-1)), Oa(T0));
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
            ServerTab.AddWaitingTaskBars(p, null, data, Oa(window.Start), Oa(window.End));
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
            ServerTab.AddWaitingTaskBars(p, null, data, Oa(T0.AddMinutes(-5)), Oa(T0.AddHours(1)));
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
                new() { CollectionTime = T0, DatabaseName = "Sales", BlockedCount = 3 },
                new() { CollectionTime = T0, DatabaseName = "Orders", BlockedCount = 2 },
                new() { CollectionTime = T0.AddHours(2), DatabaseName = "Sales", BlockedCount = 1 },
            };
            Assert.Equal(5, ServerTab.AddBlockedSessionBars(p, null, data, Oa(T0.AddHours(-1)), Oa(T0.AddHours(3))));
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
            ServerTab.AddWaitingTaskBars(chart.Plot, hover, data, Oa(T0.AddHours(-1)), Oa(T0.AddHours(4)));
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

    [Fact]
    public void HoverInTheLowerPartOfAnUpperSegment_NamesThatSegment_NotTheOneBelow()
    {
        var text = StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "ms", displayZone: () => TimeZoneInfo.Utc);
            var data = Waits((T0, "LATCH_EX", 100), (T0, "CXCONSUMER", 164), (T0.AddHours(3), "CXCONSUMER", 40));
            ServerTab.AddWaitingTaskBars(chart.Plot, hover, data, Oa(T0.AddHours(-1)), Oa(T0.AddHours(4)));
            chart.Plot.Axes.AutoScale();
            using var render = chart.Plot.GetImage(1000, 400);

            /* 25 ms into LATCH_EX's 164 to 264 segment: nearer the top of CXCONSUMER (164) than its own top (264). */
            return hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(Oa(T0), 189)));
        });

        Assert.NotNull(text);
        var lines = text!.Split('\n');
        Assert.Equal("LATCH_EX", lines[0]);
        Assert.Equal("100 ms", lines[1]);
    }

    [Fact]
    public void MemoryPressureEventsShape_HoverInsideTheSevereSegment_NamesSevere_AndTheOsBarBesideIt()
    {
        /* The Memory pressure events chart (ServerTab.UpdateMemoryPressureEventsChart) stacks severe on medium at x - offset (SQL Server)
           and x + offset (Operating System), and registers each plot with ChartHoverHelper.Add. This draws that same shape through the
           same helper, so the inside-the-segment rule that serves it is tested here. */
        var (sqlSevere, sqlMedium, osMedium) = StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "events", displayZone: () => TimeZoneInfo.Utc);
            double hour = 1.0 / 24.0, size = hour * 0.4, offset = hour * 0.22, x = Oa(T0);
            ScottPlot.Bar B(double pos, double lo, double hi) => new() { Position = pos, ValueBase = lo, Value = hi, Size = size, LineWidth = 0 };
            foreach (var (bars, label) in new[]
            {
                (new[] { B(x - offset, 0, 2) }, "SQL Server (medium)"),
                (new[] { B(x - offset, 2, 10) }, "SQL Server (severe)"),
                (new[] { B(x + offset, 0, 3) }, "Operating System (medium)"),
            })
                hover.Add(chart.Plot.Add.Bars(bars), label);
            chart.Plot.Axes.SetLimitsX(Oa(T0.AddHours(-1)), Oa(T0.AddHours(2)));
            chart.Plot.Axes.SetLimitsY(0, 12);
            using var render = chart.Plot.GetImage(1000, 400);

            /* 3 events up the SQL Server bar: inside the severe segment (2 to 10), only 1 above the medium top, 7 below the severe top. */
            return (hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(x - offset, 3))),
                hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(x - offset, 1))),
                hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(x + offset, 1.5))));
        });

        Assert.Equal("SQL Server (severe)", sqlSevere!.Split('\n')[0]);
        Assert.Equal("8 events", sqlSevere.Split('\n')[1]);
        Assert.Equal("SQL Server (medium)", sqlMedium!.Split('\n')[0]);
        Assert.Equal("Operating System (medium)", osMedium!.Split('\n')[0]);
    }
}

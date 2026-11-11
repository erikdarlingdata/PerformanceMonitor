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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8644: an empty chart is the one muted sentence with no default -10 to 10 axes and no gridlines, wherever it is drawn, and Duration
/// Trends breaks a line across a span in which no collector ran.
/// </summary>
public sealed class EmptyChartsAndGapsTests
{
    private static readonly DateTime Start = new(2026, 11, 1, 0, 0, 0, DateTimeKind.Utc);

    private static void AssertEmptyLook(ScottPlot.WPF.WpfPlot chart, string sentence)
    {
        Assert.False(chart.Plot.Axes.Left.IsVisible, "the empty chart still draws its Y axis");
        Assert.False(chart.Plot.Axes.Bottom.IsVisible, "the empty chart still draws its X axis");
        Assert.False(chart.Plot.Grid.IsVisible, "the empty chart still draws gridlines");
        var said = chart.Plot.GetPlottables<ScottPlot.Plottables.Text>().Select(t => t.LabelText)
            .Concat(chart.Plot.GetPlottables<ScottPlot.Plottables.Annotation>().Select(a => a.LabelText))
            .Select(t => t.Replace("\n", " ")).ToList();
        Assert.Contains(sentence, said);
    }

    [Fact]
    public void AnEmptyBlockingLane_IsTheEmptyLook_NotAGriddedChart() =>
        StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl();
            var none = new List<(double Time, double Value)>();
            lanes.UpdateBlockingLane(none, none, windowSpanDays: 1);
            AssertEmptyLook(lanes.BlockingChart, "No Blocking data in this window");
        });

    [Fact]
    public void AnEmptySickSpinlocksChart_SaysNoDataInThisWindow_WithNoAxes() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            new SystemHealthChartRenderer(new ChartRenderHelper(), t => t)
                .RenderSickSpinlocksChart(chart, null, new List<SystemHealthRecord>(), Start.ToOADate(), Start.AddHours(1).ToOADate());
            AssertEmptyLook(chart, "No Sick Spinlocks data in this window");
            Assert.DoesNotContain("No data for selected time range", chart.Plot.GetPlottables<ScottPlot.Plottables.Text>().Select(t => t.LabelText));
        });

    private static CollectorDurationBucket Bucket(string collector, double hours, double ms) => new()
    {
        CollectorName = collector,
        BucketStart = Start.AddHours(hours),
        MaxDurationMs = ms,
        AverageDurationMs = ms,
        RunCount = 1,
    };

    [Fact]
    public void ACollectorWithTwoBuckets_BreaksItsLineAcrossATwentyHourSpanNoCollectorRan() =>
        StaTestThread.Run(() =>
        {
            var buckets = new List<CollectorDurationBucket>();
            /* wait_stats runs every half hour for ten hours, goes quiet for twenty, then runs again; slow_collector has one bucket on each side. */
            for (var k = 0; k <= 20; k++) buckets.Add(Bucket("wait_stats", k * 0.5, 100));
            for (var k = 0; k <= 20; k++) buckets.Add(Bucket("wait_stats", 30 + k * 0.5, 100));
            buckets.Add(Bucket("slow_collector", 10, 3000));
            buckets.Add(Bucket("slow_collector", 30, 12000));

            var chart = new ScottPlot.WPF.WpfPlot();
            ServerTab.PlotCollectorDurationSeries(chart, null, ServerTab.BuildCollectorDurationSeries(buckets));
            var slow = chart.Plot.GetPlottables<ScottPlot.Plottables.Scatter>().Single(s => s.LegendText == "slow_collector");
            Assert.Contains(slow.Data.GetScatterPoints(), p => double.IsNaN(p.Y));
        });

    [Fact]
    public void ASeriesTooShortForACadence_StillBreaksWhereAllSeriesStopped()
    {
        var busy = Enumerable.Range(0, 10).Select(i => i * 0.01).Concat(Enumerable.Range(0, 10).Select(i => 1.0 + i * 0.01)).ToArray();
        var outages = TimeSeriesGaps.OutagesAcross(new[] { busy });
        Assert.Single(outages);

        var (xs, ys) = TimeSeriesGaps.BreakAtGaps(new[] { 0.0, 1.0 }, new[] { 3000.0, 12000.0 }, outages);
        Assert.Equal(3, xs.Length);
        Assert.True(double.IsNaN(ys[1]));

        var (sameXs, _) = TimeSeriesGaps.BreakAtGaps(new[] { 0.0, 0.05 }, new[] { 1.0, 2.0 }, outages);
        Assert.Equal(2, sameXs.Length);
    }

    private static List<(double Time, double Value)> Series(double from, double to, double low, double high) =>
        Enumerable.Range(0, 24).Select(i => (from + (to - from) * i / 23.0, low + (high - low) * i / 23.0)).ToList();

    private static ScottPlot.WPF.WpfPlot[] LaneCharts(CorrelatedTimelineLanesControl lanes) =>
        [lanes.CpuChart, lanes.WaitStatsChart, lanes.BlockingChart, lanes.MemoryChart, lanes.FileIoChart];

    /// <summary>Draws all five lanes: the CPU, wait, blocking, memory and I/O lanes, each with rows when <paramref name="withData"/>, else empty.</summary>
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

    /// <summary>A lane that drew the empty look (no axes, no grid) and then gets rows on the next refresh has a UTC date-time bottom axis, a left axis and gridlines again.</summary>
    [Fact]
    public void ALaneThatDrewTheEmptyLook_GetsItsUtcTimeAxisBack_WhenRowsArrive() =>
        StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl();
            lanes.Initialize(null!, 1, () => TimeZoneInfo.Utc);

            DrawLanes(lanes, withData: false);
            foreach (var chart in LaneCharts(lanes))
            {
                Assert.False(chart.Plot.Axes.Bottom.IsVisible);
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

    /// <summary>The lanes share one time ruler: an empty lane is given the same X range as the lanes beside it.</summary>
    [Fact]
    public void AnEmptyLane_KeepsTheSharedXRange_SoTheLanesStayAligned() =>
        StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl();
            lanes.Initialize(null!, 1, () => TimeZoneInfo.Utc);
            DrawLanes(lanes, withData: true);
            lanes.UpdateLane(lanes.MemoryChart, "Buffer Pool", [], "#4FC3F7");
            AssertEmptyLook(lanes.MemoryChart, "No Buffer Pool data in this window");
            lanes.SyncXAxes(6, null, null);

            var limits = LaneCharts(lanes).Select(c => c.Plot.Axes.GetLimits()).ToList();
            Assert.All(limits, l => Assert.Equal(limits[0].Left, l.Left, precision: 9));
            Assert.All(limits, l => Assert.Equal(limits[0].Right, l.Right, precision: 9));
            Assert.True(limits[0].Right > limits[0].Left + 0.1, "the shared range is the window, not the empty look's -1 to 1");
        });

    /// <summary>A chart the shared renderer drew empty, then fed rows, has the UTC date-time bottom axis and its axes back.</summary>
    [Fact]
    public void ACounterChartThatWasEmpty_GetsItsAxesBack_WhenRowsArrive() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var renderer = new SystemHealthChartRenderer(new ChartRenderHelper(), t => t, () => TimeZoneInfo.Utc);
            double x0 = Start.ToOADate(), x1 = Start.AddHours(1).ToOADate();
            renderer.RenderCounterChart(chart, null, new List<SystemHealthRecord>(), r => r.BadPagesDetected, "Bad Pages", "#E57373", x0, x1);
            AssertEmptyLook(chart, "No Bad Pages data in this window");

            var rows = new List<SystemHealthRecord>
            {
                new() { EventTime = Start.AddMinutes(10), BadPagesDetected = 1 },
                new() { EventTime = Start.AddMinutes(20), BadPagesDetected = 3 },
            };
            renderer.RenderCounterChart(chart, null, rows, r => r.BadPagesDetected, "Bad Pages", "#E57373", x0, x1);
            Assert.IsType<DisplayZoneTickGenerator>(chart.Plot.Axes.Bottom.TickGenerator);
            Assert.True(chart.Plot.Axes.Bottom.IsVisible);
            Assert.True(chart.Plot.Axes.Left.IsVisible);
            Assert.True(chart.Plot.Grid.IsVisible);
        });
}

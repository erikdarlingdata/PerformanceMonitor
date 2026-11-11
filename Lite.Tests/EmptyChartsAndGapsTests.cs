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
}

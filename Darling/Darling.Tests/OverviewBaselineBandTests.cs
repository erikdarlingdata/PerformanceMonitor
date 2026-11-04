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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using ScottPlot.Plottables;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7433: the Overview lanes shade the normal range (mean plus or minus two standard deviations) behind each series. They
/// drew it with <c>Plot.Add.HorizontalSpan(lower, upper)</c>, a range on the X axis, so the band sat near 1899 and never
/// appeared. These tests build the real control, run each lane that draws a band, and read what ScottPlot holds: the band
/// is a Y-axis span at the right limits, it is the first plottable (so it paints behind the series and the bars), and it
/// does not move the lane's Y axis, even when the band's top is far above the series.
/// </summary>
public sealed class OverviewBaselineBandTests
{
    public enum Lane { Cpu, Blocking, Wait }

    private static readonly double T0 = new DateTime(2026, 10, 26, 0, 0, 0, DateTimeKind.Unspecified).ToOADate();

    private static List<(double Time, double Value)> Series(double lo, double hi) =>
        Enumerable.Range(0, 48).Select(i => (T0 + i / 48.0, lo + (hi - lo) * (i % 12) / 11.0)).ToList();

    private static BaselineBucket Bucket(double mean, double sd) =>
        new() { Mean = mean, StdDev = sd, SampleCount = 500 };

    /// <summary>Draws the lane (with or without a baseline) and returns the plot it drew on.</summary>
    private static WpfPlot Draw(CorrelatedTimelineLanesControl control, Lane lane, BaselineBucket? baseline)
    {
        switch (lane)
        {
            case Lane.Cpu:
                control.UpdateCpuLane(Series(5, 90), Series(8, 95), baseline);
                return control.CpuChart;
            case Lane.Blocking:
                control.UpdateBlockingLane(Series(1, 6), Series(0, 2).Take(5).ToList(), baseline, windowSpanDays: 1);
                return control.BlockingChart;
            default:
                control.UpdateLane(control.WaitStatsChart, "Wait", Series(150, 900), "#FFB74D", baseline: baseline, minAnomalyValue: 100);
                return control.WaitStatsChart;
        }
    }

    private static CorrelatedTimelineLanesControl NewControl()
    {
        var control = new CorrelatedTimelineLanesControl();
        control.Initialize(null!, 1);
        return control;
    }

    [Theory]
    [InlineData(Lane.Cpu, 40, 10)]
    [InlineData(Lane.Blocking, 3, 1)]
    [InlineData(Lane.Wait, 500, 100)]
    // A band whose top is far above the series (and, for the lower edge, clamped at zero): the lane must keep its own axis.
    [InlineData(Lane.Cpu, 300, 500)]
    [InlineData(Lane.Blocking, 5000, 1000)]
    [InlineData(Lane.Wait, 90000, 40000)]
    public void TheBand_IsAYAxisSpanBehindTheSeries_AndLeavesTheYAxisAlone(Lane lane, double mean, double sd)
    {
        StaTestThread.Run(() =>
        {
            var control = NewControl();
            var withoutBand = Draw(control, lane, baseline: null).Plot.Axes.GetLimits();

            var baseline = Bucket(mean, sd);
            var plot = Draw(control, lane, baseline).Plot;

            var spans = plot.GetPlottables<VerticalSpan>().ToList();
            var span = Assert.Single(spans);
            Assert.Empty(plot.GetPlottables<HorizontalSpan>());

            // The span's limits are on Y: the band's own edges, and nothing on X (a HorizontalSpan reports the reverse).
            var limits = span.GetAxisLimits();
            Assert.Equal(Math.Max(0, mean - 2 * baseline.EffectiveStdDev), limits.Bottom, 6);
            Assert.Equal(mean + 2 * baseline.EffectiveStdDev, limits.Top, 6);
            Assert.True(double.IsNaN(limits.Left) && double.IsNaN(limits.Right), $"the band reports X limits [{limits.Left}, {limits.Right}]");

            // Painted first, so behind the bars, the lines and the anomaly dots.
            Assert.Same(span, plot.PlottableList[0]);

            // The lane's Y axis is the one the series alone gave it.
            var withBand = plot.Axes.GetLimits();
            Assert.Equal(withoutBand.Bottom, withBand.Bottom, 6);
            Assert.Equal(withoutBand.Top, withBand.Top, 6);
        });
    }
}

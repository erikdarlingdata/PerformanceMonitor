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
using PerformanceMonitorLite.Controls;
using ScottPlot.Plottables;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7433: the Overview lanes (Lite) shade the normal range (mean plus or minus two standard deviations) behind each series. They
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

    /// <summary>The bucket as one band over the day the series sits in (#7644: the lanes take bands per local hour).</summary>
    private static IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>? Bands(BaselineBucket? bucket) =>
        bucket == null ? null : [PerformanceMonitor.Ui.BaselineBand.MakeStep(T0, T0 + 1, bucket.Mean, bucket.EffectiveStdDev)];

    /// <summary>Draws the lane (with or without a baseline) and returns the plot it drew on.</summary>
    private static WpfPlot Draw(CorrelatedTimelineLanesControl control, Lane lane, BaselineBucket? baseline)
    {
        switch (lane)
        {
            case Lane.Cpu:
                control.UpdateCpuLane(Series(5, 90), Series(8, 95), Bands(baseline));
                return control.CpuChart;
            case Lane.Blocking:
                control.UpdateBlockingLane(Series(1, 6), Series(0, 2).Take(5).ToList(), Bands(baseline), windowSpanDays: 1);
                return control.BlockingChart;
            default:
                control.UpdateLane(control.WaitStatsChart, "Wait", Series(150, 900), "#FFB74D", bands: Bands(baseline), minAnomalyValue: 100);
                return control.WaitStatsChart;
        }
    }

    private static CorrelatedTimelineLanesControl NewControl()
    {
        var control = new CorrelatedTimelineLanesControl();
        control.Initialize(null!, 1, () => TimeZoneInfo.Utc);
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

            var span = Assert.Single(plot.GetPlottables<Rectangle>());
            Assert.Empty(plot.GetPlottables<HorizontalSpan>());

            // The band's Y limits are its own edges (#7433: the range is on Y, never on X), and its X limits the hour it covers.
            var limits = span.GetAxisLimits();
            Assert.Equal(Math.Max(0, mean - 2 * baseline.EffectiveStdDev), limits.Bottom, 6);
            Assert.Equal(mean + 2 * baseline.EffectiveStdDev, limits.Top, 6);
            Assert.Equal(T0, limits.Left, 6);
            Assert.Equal(T0 + 1, limits.Right, 6);

            // Painted first, so behind the bars, the lines and the anomaly dots.
            Assert.Same(span, plot.PlottableList[0]);

            // The lane's Y axis is the one the series alone gave it.
            var withBand = plot.Axes.GetLimits();
            Assert.Equal(withoutBand.Bottom, withBand.Bottom, 6);
            Assert.Equal(withoutBand.Top, withBand.Top, 6);
        });
    }

    /// <summary>
    /// #7678: a run of neighbours outside the band is ONE segment and a point alone is one dot, not a 6 px circle per point (a third of a quiet
    /// server's points marked hid the line). Points 2 to 4 and the 6th are above the band: one three-point segment and one dot.
    /// </summary>
    [Fact]
    public void ARunAboveTheBand_IsOneSegment_AndALonePointIsOneDot()
    {
        StaTestThread.Run(() =>
        {
            var control = NewControl();
            IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep> steps =
                [PerformanceMonitor.Ui.BaselineBand.MakeStep(T0 - 1, T0 + 1, 100, 50)]; // 0 to 200
            double[] values = [100, 500, 500, 500, 100, 500, 100];
            var data = values.Select((v, i) => (T0 + i / 1440.0, v)).ToList();

            control.UpdateLane(control.WaitStatsChart, "Wait", data, "#FFB74D", bands: steps, minAnomalyValue: 100);

            var plot = control.WaitStatsChart.Plot;
            Assert.Equal([T0 + 1 / 1440.0, T0 + 2 / 1440.0, T0 + 3 / 1440.0, T0 + 5 / 1440.0], Darling.Tests.OverviewLaneMarks.MarkedX(plot));
            var shapes = Darling.Tests.OverviewLaneMarks.Shapes(plot);
            Assert.Equal(2, shapes.Count);
            Assert.Contains((3, false), shapes);
            Assert.Contains((1, true), shapes);
        });
    }

    /// <summary>The same run rule on the CPU lane, which has its own marker site.</summary>
    [Fact]
    public void TheCpuLane_DrawsARunAsOneSegment_AndALonePointAsOneDot()
    {
        StaTestThread.Run(() =>
        {
            var control = NewControl();
            IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep> steps =
                [PerformanceMonitor.Ui.BaselineBand.MakeStep(T0 - 1, T0 + 1, 10, 5)]; // 0 to 20
            double[] values = [10, 90, 90, 90, 10, 90, 10];
            var data = values.Select((v, i) => (T0 + i / 1440.0, v)).ToList();

            control.UpdateCpuLane(data, data, steps);

            var shapes = Darling.Tests.OverviewLaneMarks.Shapes(control.CpuChart.Plot);
            Assert.Equal(2, shapes.Count);
            Assert.Contains((3, false), shapes);
            Assert.Contains((1, true), shapes);
        });
    }

    /// <summary>
    /// #7678: ScottPlot paints plottables in the order they were added. The marks are added AFTER the lane's own 1.5 px line, so a run reads as
    /// the line's stretch redrawn thicker in the anomaly colour and the line is not painted over it. Both the CPU lane and the other lanes.
    /// </summary>
    [Fact]
    public void TheMarks_ArePaintedOverTheLanesOwnLine()
    {
        StaTestThread.Run(() =>
        {
            var control = NewControl();
            IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep> steps =
                [PerformanceMonitor.Ui.BaselineBand.MakeStep(T0 - 1, T0 + 1, 10, 5)]; // 0 to 20
            double[] values = [10, 90, 90, 10, 90, 10];
            var data = values.Select((v, i) => (T0 + i / 1440.0, v)).ToList();

            control.UpdateLane(control.WaitStatsChart, "Wait", data, "#FFB74D", bands: steps, minAnomalyValue: 1);
            Assert.True(Darling.Tests.OverviewLaneMarks.MarkedCount(control.WaitStatsChart.Plot) > 0);
            Assert.True(Darling.Tests.OverviewLaneMarks.MarksComeAfterTheSeries(control.WaitStatsChart.Plot), "the other lanes draw the marks before the line");

            control.UpdateCpuLane(data, data, steps);
            Assert.True(Darling.Tests.OverviewLaneMarks.MarkedCount(control.CpuChart.Plot) > 0);
            Assert.True(Darling.Tests.OverviewLaneMarks.MarksComeAfterTheSeries(control.CpuChart.Plot), "the CPU lane draws the marks before the line");
        });
    }
}

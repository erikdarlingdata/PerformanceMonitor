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
using PerformanceMonitor.Ui;
using PerformanceMonitor.Darling.Analysis;
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

    /// <summary>#7644: one step wide enough to cover the whole series (the band is a step per local hour now).</summary>
    private static IReadOnlyList<BaselineBand.HourStep> Bucket(double mean, double sd) =>
        [BaselineBand.MakeStep(T0 - 1, T0 + 2, mean, sd)];

    /// <summary>Draws the lane (with or without a baseline) and returns the plot it drew on.</summary>
    private static WpfPlot Draw(CorrelatedTimelineLanesControl control, Lane lane, IReadOnlyList<BaselineBand.HourStep>? baseline)
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

            // #7644: the band is one rectangle per hour (a VerticalSpan would run the whole X axis, the old flat band).
            var rectangle = Assert.Single(plot.GetPlottables<Rectangle>());
            Assert.Empty(plot.GetPlottables<HorizontalSpan>());

            // The rectangle's Y edges are the band's own edges, and its X edges are the step's hour.
            var limits = rectangle.GetAxisLimits();
            Assert.Equal(Math.Max(0, mean - 2 * sd), limits.Bottom, 6);
            Assert.Equal(mean + 2 * sd, limits.Top, 6);
            Assert.Equal(T0 - 1, limits.Left, 6);
            Assert.Equal(T0 + 2, limits.Right, 6);

            // Painted first, so behind the bars, the lines and the anomaly dots.
            Assert.Same(rectangle, plot.PlottableList[0]);

            // The lane's Y axis is the one the series alone gave it.
            var withBand = plot.Axes.GetLimits();
            Assert.Equal(withoutBand.Bottom, withBand.Bottom, 6);
            Assert.Equal(withoutBand.Top, withBand.Top, 6);
        });
    }

    /// <summary>
    /// #7644: the band steps by hour, and each point is judged against its own hour's step. Two hours with different bands and a
    /// third with none: 500 is above the first hour's band (marked), below the second hour's band (marked, a drop), 1000 sits inside
    /// the second hour's band (not marked), and 5000 in the hour with no step is not marked at all. One flat band could not give this answer.
    /// </summary>
    [Fact]
    public void EachPoint_IsJudgedAgainstItsOwnHoursStep_AndAnHourWithNoStepGetsNoMarker()
    {
        StaTestThread.Run(() =>
        {
            var control = NewControl();
            double Hour(double minutes) => T0 + minutes / 1440.0;
            IReadOnlyList<BaselineBand.HourStep> steps =
            [
                BaselineBand.MakeStep(Hour(0), Hour(60), 100, 50),     // 0 to 200
                BaselineBand.MakeStep(Hour(60), Hour(120), 1000, 100), // 800 to 1200
            ];
            var data = new List<(double Time, double Value)>
            {
                (Hour(10), 500),   // above the first hour's band
                (Hour(70), 500),   // below the second hour's band
                (Hour(90), 1000),  // inside the second hour's band
                (Hour(130), 5000), // an hour with no step
            };

            control.UpdateLane(control.WaitStatsChart, "Wait", data, "#FFB74D", baseline: steps, minAnomalyValue: 100);

            var plot = control.WaitStatsChart.Plot;
            Assert.Equal(2, plot.GetPlottables<Rectangle>().Count());
            var dots = plot.GetPlottables<Scatter>().Where(s => s.LineWidth == 0).SelectMany(s => s.Data.GetScatterPoints()).Select(p => p.X).OrderBy(x => x).ToList();
            Assert.Equal([Hour(10), Hour(70)], dots);
        });
    }

    /// <summary>#7644: an hour with no step draws no band, so a lane whose every hour has none draws no rectangle and no marker.</summary>
    [Fact]
    public void ALaneWithNoSteps_DrawsNoBandAndNoMarker()
    {
        StaTestThread.Run(() =>
        {
            var control = NewControl();
            control.UpdateLane(control.WaitStatsChart, "Wait", Series(150, 9000), "#FFB74D",
                baseline: Array.Empty<BaselineBand.HourStep>(), minAnomalyValue: 100);

            var plot = control.WaitStatsChart.Plot;
            Assert.Empty(plot.GetPlottables<Rectangle>());
            Assert.DoesNotContain(plot.GetPlottables<Scatter>(), s => s.LineWidth == 0);
        });
    }

    /// <summary>
    /// #7644: the Viewer's steps come from the shared entry's hours. An hour whose bucket has no samples or no dispersion is dropped,
    /// and the first and last hours are cut to the window so the band does not hang past the lane's edges.
    /// </summary>
    [Fact]
    public void ToBandSteps_DropsUnusableHours_AndClipsToTheWindow()
    {
        var start = new DateTime(2026, 11, 3, 10, 30, 0, DateTimeKind.Utc);
        var end = start.AddHours(3);
        BaselineBucket B(double mean, double sd, int n) => new() { Mean = mean, StdDev = sd, SampleCount = n };
        var steps = new List<PgBaselineProvider.BaselineStep>
        {
            new(new DateTime(2026, 11, 3, 10, 0, 0, DateTimeKind.Utc), new DateTime(2026, 11, 3, 11, 0, 0, DateTimeKind.Utc), B(10, 2, 40)),
            new(new DateTime(2026, 11, 3, 11, 0, 0, DateTimeKind.Utc), new DateTime(2026, 11, 3, 12, 0, 0, DateTimeKind.Utc), B(10, 2, 0)),
            new(new DateTime(2026, 11, 3, 12, 0, 0, DateTimeKind.Utc), new DateTime(2026, 11, 3, 13, 0, 0, DateTimeKind.Utc), B(0, 0, 40)),
            new(new DateTime(2026, 11, 3, 13, 0, 0, DateTimeKind.Utc), new DateTime(2026, 11, 3, 14, 0, 0, DateTimeKind.Utc), B(20, 5, 40)),
        };

        var result = ViewerDataService.ToBandSteps(steps, start, end);

        Assert.Equal(2, result.Count);
        Assert.Equal(start.ToOADate(), result[0].FromX, 9);
        Assert.Equal(new DateTime(2026, 11, 3, 11, 0, 0).ToOADate(), result[0].ToX, 9);
        Assert.Equal(6, result[0].Lower, 6);
        Assert.Equal(14, result[0].Upper, 6);
        Assert.Equal(new DateTime(2026, 11, 3, 13, 0, 0).ToOADate(), result[1].FromX, 9);
        Assert.Equal(end.ToOADate(), result[1].ToX, 9);
        Assert.Equal(10, result[1].Lower, 6);
        Assert.Equal(30, result[1].Upper, 6);
    }
}

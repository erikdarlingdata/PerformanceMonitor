/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Regression tests for <see cref="ChartStyle.ComputeYLimitsWithLegendPadding"/> — the pure Y-limit math
/// shared by every trend chart in all three apps (Lite, Dashboard, and the Darling viewer). Pins the fix
/// for the magnitude-scaled negative floor bug: a non-negative series (dataYMin &gt;= 0) must floor the
/// axis at 0, NOT at -5% of the data max. The old middle branch put a big dead-band under high-value
/// charts (e.g. Wait Stats peaking ~6000 ms/sec dropped to a -300 floor). Only genuinely-negative data
/// keeps a below-zero margin. Mirror of Dashboard.Tests for parity.
/// </summary>
public class ChartStyleYLimitsTests
{
    [Fact]
    public void ZeroDataMin_FloorsAtZero_NoNegativeDeadBand()
    {
        // The exact bug: callers pass an explicit dataYMin == 0, which used to hit the middle branch
        // and produce yMin = -(range * 0.05) = -300 for a 0..6000 chart. It must now floor at 0.
        var (yMin, _) = ChartStyle.ComputeYLimitsWithLegendPadding(0, 6000);
        Assert.Equal(0.0, yMin);
    }

    [Fact]
    public void PositiveDataMin_FloorsAtZero()
    {
        var (yMin, _) = ChartStyle.ComputeYLimitsWithLegendPadding(50, 6000);
        Assert.Equal(0.0, yMin);
    }

    [Fact]
    public void NegativeDataMin_KeepsTenPercentBelowZeroMargin()
    {
        // range = 100; genuinely-negative data keeps its 10%-of-range breathing room below the min.
        var (yMin, _) = ChartStyle.ComputeYLimitsWithLegendPadding(-10, 90);
        Assert.Equal(-20.0, yMin, 6);   // -10 - (100 * 0.10)
    }

    [Fact]
    public void TopPadding_IsFifteenPercentOfRange()
    {
        var (_, yMax) = ChartStyle.ComputeYLimitsWithLegendPadding(0, 6000);
        Assert.Equal(6900.0, yMax, 6);  // 6000 + (6000 * 0.15)
    }

    [Fact]
    public void FlatData_ExpandsRangeAndStillFloorsAtZero()
    {
        // dataYMax <= dataYMin forces range to 1 so a flat non-negative series still gets a visible band.
        var (yMin, yMax) = ChartStyle.ComputeYLimitsWithLegendPadding(5, 5);
        Assert.Equal(0.0, yMin);
        Assert.Equal(6.15, yMax, 6);    // dataYMax -> 6, + (1 * 0.15)
    }

    [Fact]
    public void AChartOfOnlyZeros_ShowsAFlatLineOnAYAxisThatStartsAtZero()
    {
        var plot = new ScottPlot.Plot();
        var line = plot.Add.Scatter(new double[] { 1, 2, 3, 4 }, new double[] { 0, 0, 0, 0 });
        line.LegendText = "Blocked";
        plot.Axes.SetLimitsY(-1, 1); // what the flat line autoscales to

        ChartStyle.FloorAllZeroYAxis(plot);

        var limits = plot.Axes.GetLimits();
        Assert.Equal(0.0, limits.Bottom);
        Assert.True(limits.Top > 0);
    }

    [Fact]
    public void AChartWithRealValues_KeepsItsYAxis()
    {
        var plot = new ScottPlot.Plot();
        var line = plot.Add.Scatter(new double[] { 1, 2, 3 }, new double[] { -5, 0, 5 });
        line.LegendText = "Delta";
        plot.Axes.SetLimitsY(-6, 6);

        ChartStyle.FloorAllZeroYAxis(plot);

        Assert.Equal(-6.0, plot.Axes.GetLimits().Bottom);
    }
}

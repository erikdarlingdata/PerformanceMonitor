/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Ui;
using ScottPlot;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7606: a count chart (Blocked Sessions, Deadlocks, task and event counts) labels whole numbers only, and a time chart keeps room
/// to the right of its plot so a label centred on the window's end is not cut off. Both are drawn through a real render, since the
/// tick density and the gutter depend on the pixel size ScottPlot lays out.
/// </summary>
public sealed class CountAxisTests
{
    private static readonly DateTime End = new(2026, 11, 3, 18, 0, 0, DateTimeKind.Utc);

    /// <summary>A plot with the Y limits the tabs give a series peaking at <paramref name="dataMax"/>, rendered at <paramref name="height"/> px.</summary>
    private static Plot Render(double dataMax, int height, bool count)
    {
        var plot = new Plot();
        plot.Add.Scatter(new[] { End.AddHours(-24).ToOADate(), End.ToOADate() }, new[] { 0.0, dataMax });
        plot.Axes.Left.TickGenerator = count ? new CountTickGenerator() : new MinimumTickGenerator();
        var (yMin, yMax) = ChartStyle.ComputeYLimitsWithLegendPadding(0, dataMax);
        plot.Axes.SetLimitsY(yMin, yMax);
        plot.RenderInMemory(1180, height);
        return plot;
    }

    private static double[] Majors(Plot plot) => plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor).Select(t => t.Position).ToArray();

    [Theory]
    [InlineData(1, 300)]
    [InlineData(1, 800)]
    [InlineData(2, 300)]
    [InlineData(2, 800)]
    [InlineData(3, 800)]
    public void CountAxis_LabelsWholeNumbersOnly_WithOneAboveZero(double dataMax, int height)
    {
        var ticks = Majors(Render(dataMax, height, count: true));

        Assert.All(ticks, t => Assert.Equal(Math.Round(t), t));
        Assert.Contains(ticks, t => t > 0);
    }

    [Fact]
    public void NonCountAxis_KeepsItsDecimals()
    {
        // The same limits on a plain chart: 0.5 stays, so the count rule is the mark, not a global change.
        Assert.Contains(Majors(Render(1, 800, count: false)), t => Math.Abs(t - 0.5) < 1e-9);
    }

    [Fact]
    public void CountAxis_LargeCounts_StayWholeAndSparse()
    {
        var ticks = Majors(Render(12500, 300, count: true));

        Assert.All(ticks, t => Assert.Equal(Math.Round(t), t));
        Assert.InRange(ticks.Length, 2, 12);
    }

    [Fact]
    public void WholeOnly_AllZeroAxis_StillGetsZeroAndOne()
    {
        var ticks = CountTickGenerator.WholeOnly(Array.Empty<Tick>(), 0, 1);

        Assert.Equal(new[] { 0.0, 1.0 }, ticks.Select(t => t.Position).ToArray());
        Assert.Equal(new[] { "0", "1" }, ticks.Select(t => t.Label).ToArray());
    }

    private static float RightGutter(Plot plot)
    {
        var layout = plot.RenderManager.LastRender.Layout;
        return layout.FigureRect.Right - layout.DataRect.Right;
    }

    private static Plot TimePlot(bool time)
    {
        var plot = new Plot();
        plot.Add.Scatter(new[] { End.AddHours(-24).ToOADate(), End.ToOADate() }, new[] { 0.0, 1.0 });
        if (time) plot.Axes.DateTimeTicksBottomUtc(() => TimeZoneInfo.Utc);
        plot.Axes.SetLimitsX(End.AddHours(-24).ToOADate(), End.ToOADate());
        plot.RenderInMemory(1180, 400);
        return plot;
    }

    [Fact]
    public void TimeAxis_KeepsRoomForAHalfLabelRightOfThePlot()
    {
        // "18:00" at 13 px is about 38 px wide: half of it must fit between the plot's edge and the chart's.
        Assert.True(RightGutter(TimePlot(time: true)) >= 20, "a label on the window's end would be cut off");
    }

    [Fact]
    public void GroupAlignment_KeepsTheTimeAxisGutter()
    {
        var a = TimePlot(time: true);
        var b = TimePlot(time: true);
        ChartGroupAligner.Align(new[] { (a, 1180, 400), (b, 1180, 400) });
        a.RenderInMemory(1180, 400);

        Assert.True(RightGutter(a) >= 20, "the aligner cleared the time axis' right gutter");
    }
}

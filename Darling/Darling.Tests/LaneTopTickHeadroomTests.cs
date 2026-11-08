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
/// #8307: an Overview lane's plot is flush with the lane's top edge, so a Y tick on the top of the range has its label cut there.
/// <see cref="LaneTopTickHeadroom"/> raises the top of the range until the highest labelled tick sits half a label below the edge.
/// Real headless renders, as <see cref="LaneAxisAlignerTests"/> are: ScottPlot draws to a raster surface, so the ticks it picks
/// and the data rectangle it lays out are the real ones, with no window.
/// </summary>
public sealed class LaneTopTickHeadroomTests
{
    /// <summary>A lane as the Overview builds one: no top axis, no title, the bottom axis trimmed to its tick marks.</summary>
    private static Plot Lane(double yMin, double yMax)
    {
        var plot = new Plot();
        plot.Axes.Top.IsVisible = false;
        plot.Axes.Bottom.MaximumSize = 6;
        plot.Axes.SetLimitsX(0, 1);
        plot.Axes.SetLimitsY(yMin, yMax);
        return plot;
    }

    private static (double Position, string Label) TopTick(Plot plot)
    {
        var limits = plot.Axes.GetLimits();
        var top = plot.Axes.Left.TickGenerator.Ticks
            .Where(t => t.IsMajor && !string.IsNullOrEmpty(t.Label) && t.Position >= limits.Bottom && t.Position <= limits.Top)
            .OrderByDescending(t => t.Position)
            .First();
        return (top.Position, top.Label);
    }

    private static double GapAboveTopTick(Plot plot)
    {
        var limits = plot.Axes.GetLimits();
        var (position, _) = TopTick(plot);
        return plot.RenderManager.LastRender.DataRect.Height * (limits.Top - position) / (limits.Top - limits.Bottom);
    }

    [Theory]
    [InlineData(0, 1)] // the empty lane: "1" on the edge
    [InlineData(0, 105)] // the CPU lane: "100" a few pixels under it
    [InlineData(0, 2)] // the quiet blocking lane
    public void ATopTickOnOrNearThePlotsEdge_GetsHalfALabelOfHeadroom(double yMin, double yMax)
    {
        var plot = Lane(yMin, yMax);
        plot.RenderInMemory(LaneAxisAligner.MeasureWidth, 130);
        var before = TopTick(plot);
        Assert.True(GapAboveTopTick(plot) < LaneTopTickHeadroom.NeededPixels(plot, before.Label), "the fixture's top tick already has room");

        var gap = LaneTopTickHeadroom.Apply(plot, 130);
        plot.RenderInMemory(LaneAxisAligner.MeasureWidth, 130);

        var after = TopTick(plot);
        var limits = plot.Axes.GetLimits();
        Assert.Equal(yMin, limits.Bottom, 9);
        Assert.True(limits.Top > yMax, $"the top stayed at {limits.Top}");
        Assert.Equal(before.Label, after.Label);
        Assert.True(gap + LaneTopTickHeadroom.Tolerance >= LaneTopTickHeadroom.NeededPixels(plot, after.Label), $"the top tick sits {gap:0.##} px under the edge");
        Assert.Equal(gap, GapAboveTopTick(plot), 3);
    }

    [Fact]
    public void ATopTickThatAlreadyHasRoom_LeavesTheLimitsAlone()
    {
        var plot = Lane(0, 11);
        plot.RenderInMemory(LaneAxisAligner.MeasureWidth, 130);
        var before = plot.Axes.GetLimits();
        Assert.True(GapAboveTopTick(plot) >= LaneTopTickHeadroom.NeededPixels(plot, TopTick(plot).Label), "the fixture's top tick is cut");

        LaneTopTickHeadroom.Apply(plot, 130);

        var after = plot.Axes.GetLimits();
        Assert.Equal(before.Top, after.Top, 9);
        Assert.Equal(before.Bottom, after.Bottom, 9);
    }

    [Fact]
    public void ASecondPass_ChangesNothing()
    {
        var plot = Lane(0, 1);
        LaneTopTickHeadroom.Apply(plot, 130);
        var once = plot.Axes.GetLimits();

        LaneTopTickHeadroom.Apply(plot, 130);

        var twice = plot.Axes.GetLimits();
        Assert.Equal(once.Top, twice.Top, 9);
        Assert.Equal(once.Bottom, twice.Bottom, 9);
    }

    [Fact]
    public void ACountAxis_KeepsItsWholeTopTick_AndGivesItRoom()
    {
        var plot = Lane(0, 1);
        ChartStyle.UseCountAxis(plot);

        var gap = LaneTopTickHeadroom.Apply(plot, 130);
        plot.RenderInMemory(LaneAxisAligner.MeasureWidth, 130);

        var top = TopTick(plot);
        Assert.Equal(1, top.Position, 9);
        Assert.True(gap + LaneTopTickHeadroom.Tolerance >= LaneTopTickHeadroom.NeededPixels(plot, top.Label), $"the \"{top.Label}\" tick sits {gap:0.##} px under the edge");
        Assert.All(plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor), t => Assert.Equal(Math.Round(t.Position), t.Position, 9));
    }

    [Fact]
    public void AShorterLane_GivesALargerShareOfItsRange()
    {
        var tall = Lane(0, 1);
        LaneTopTickHeadroom.Apply(tall, 300);
        var shortLane = Lane(0, 1);
        LaneTopTickHeadroom.Apply(shortLane, 130);

        Assert.True(shortLane.Axes.GetLimits().Top > tall.Axes.GetLimits().Top,
            $"the short lane's top is {shortLane.Axes.GetLimits().Top}, the tall lane's {tall.Axes.GetLimits().Top}");
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Overview tooltip's triangle belongs to the lane's baseline, which belongs to the lane's first series (#6787):
/// Total CPU, added after SQL CPU, showed a red triangle judged against SQL CPU's band with no red dot on the chart.
/// </summary>
public sealed class CrosshairBaselineArrowTests
{
    // A CPU lane: SQL CPU's band is 10..30 and a rise only counts from 10%.
    private static string? Cpu(int seriesIndex, double value)
        => CorrelatedCrosshairManager.BaselineArrowSymbol(30, 10, 10, false, seriesIndex, value);

    [Fact]
    public void FirstSeriesAboveBand_GetsRedTriangle() => Assert.Equal("▲", Cpu(0, 50));

    [Fact]
    public void SecondSeriesWithTheSameValue_GetsNoArrow() => Assert.Null(Cpu(1, 50));

    [Fact]
    public void SecondSeriesBelowBand_GetsNoArrow() => Assert.Null(Cpu(1, 2));

    [Fact]
    public void FirstSeriesBelowBand_GetsDownTriangle() => Assert.Equal("▼", Cpu(0, 2));

    [Fact]
    public void FirstSeriesInsideBand_GetsNoArrow() => Assert.Null(Cpu(0, 20));

    [Fact]
    public void FirstSeriesAboveBandButUnderTheMinimum_GetsNoArrow()
        => Assert.Null(CorrelatedCrosshairManager.BaselineArrowSymbol(3, 1, 10, false, 0, 5));

    [Fact]
    public void LaneWithoutABaseline_GetsNoArrow()
        => Assert.Null(CorrelatedCrosshairManager.BaselineArrowSymbol(null, null, 10, false, 0, 90));

    [Fact]
    public void EventLane_FirstSeriesSpike_GetsRedTriangle_AndSecondSeriesDoesNot()
    {
        // Blocking reports per minute: mean 5, so 20 is a spike; Deadlocks (second series) is not judged by it.
        Assert.Equal("▲", CorrelatedCrosshairManager.BaselineArrowSymbol(8, 2, 0, true, 0, 20));
        Assert.Null(CorrelatedCrosshairManager.BaselineArrowSymbol(8, 2, 0, true, 1, 20));
    }

    // #7624: a point above the band is a ▲ in the tooltip only when its reads reach the floor, as its red dot is only drawn then.
    private static string? HoverIo(long[]? reads, long minReads, double hoverX)
    {
        string? arrow = null;
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            using var manager = new CorrelatedCrosshairManager();
            manager.AddLane(chart, "I/O ms", "ms");
            manager.SetLaneBaseline(chart, 0, 2, minAnomalyValue: 2);
            var times = new[] { 100.0, 101.0, 102.0 };
            manager.SetLaneData(chart, times, new[] { 5.0, 5.0, 5.0 }, reads: reads, minReads: minReads);
            arrow = manager.HoverArrowAt(chart, hoverX);
        });
        return arrow;
    }

    [Fact]
    public void IoPointAboveTheBandWithTooFewReads_GetsNoTooltipArrow()
        => Assert.Null(HoverIo(new long[] { 1000, 10, 1000 }, 250, 101));

    [Fact]
    public void IoPointAboveTheBandWithEnoughReads_GetsTheTooltipArrow()
        => Assert.Equal("▲", HoverIo(new long[] { 1000, 10, 1000 }, 250, 100));

    [Fact]
    public void IoPointWithNoEntryInTheReadsList_CountsAsZeroReads_AsTheDotDoes()
        => Assert.Null(HoverIo(new long[] { 1000 }, 250, 102));

    /// <summary>#7644: with a stepped band the tooltip judges the value against the step its X falls in, and an X in an hour with no step gets no arrow.</summary>
    private static string? HoverStepped(double hoverX)
    {
        string? arrow = null;
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            using var manager = new CorrelatedCrosshairManager();
            manager.AddLane(chart, "CPU", "%");
            // 100 to 101: a band of 0 to 2. 101 to 102: a band of 8 to 12. Nothing from 102 on.
            manager.SetLaneBaselineSteps(chart, [BaselineBand.MakeStep(100, 101, 1, 0.5), BaselineBand.MakeStep(101, 102, 10, 1)]);
            manager.SetLaneData(chart, new[] { 100.5, 101.5, 102.5 }, new[] { 5.0, 5.0, 5.0 });
            arrow = manager.HoverArrowAt(chart, hoverX);
        });
        return arrow;
    }

    [Fact]
    public void SteppedBand_ValueAboveItsHoursBand_GetsTheUpArrow()
        => Assert.Equal("\u25B2", HoverStepped(100.5));

    [Fact]
    public void SteppedBand_ValueBelowTheNextHoursBand_GetsTheDownArrow()
        => Assert.Equal("\u25BC", HoverStepped(101.5));

    [Fact]
    public void SteppedBand_AnHourWithNoStep_GetsNoArrow()
        => Assert.Null(HoverStepped(102.5));
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

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
}

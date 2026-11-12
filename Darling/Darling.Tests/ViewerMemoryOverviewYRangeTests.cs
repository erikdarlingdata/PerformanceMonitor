/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Reflection;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's Memory Overview chart draws four series on one Y axis: Total Server Memory, Target Memory,
/// Buffer Pool and Memory Grants. Its Y range must cover every one of them, so a Target or a grant sample
/// above Total is drawn inside the plot. <see cref="ViewerServerTab.MemoryOverviewYMax"/> is the pure
/// range calculation behind the chart; these facts feed its result through the shared headroom helper and
/// check that no plotted sample lands above the top. Lite's Memory Overview has the same facts in
/// Lite.Tests.
/// </summary>
public class ViewerMemoryOverviewYRangeTests
{
    /* Total flat at 0.9 GB with a Target that leaves it for 1.5 GB: the engine's ceiling moving on a host
       with no max server memory cap while the workload holds Total where it is. */
    private static readonly double[] Total = [0.9, 0.9, 0.9, 0.9, 0.9];
    private static readonly double[] Target = [0.9, 1.5, 1.2, 0.9, 1.5];
    private static readonly double[] BufferPool = [0.6, 0.6, 0.6, 0.6, 0.6];

    private static double DataMax(params double[][] series)
    {
        var method = typeof(ViewerServerTab).GetMethod(
            "MemoryOverviewYMax",
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public,
            [typeof(double[][])]);

        Assert.True(
            method is not null,
            "ViewerServerTab.MemoryOverviewYMax(params double[][]) is not defined: the Memory Overview sizes its Y range from Total Server Memory alone.");

        return (double)method!.Invoke(null, [series])!;
    }

    private static double TopOfRange(params double[][] series) =>
        ChartStyle.ComputeYLimitsWithLegendPadding(0, DataMax(series)).YMax;

    [Fact]
    public void TargetMemoryAboveTotal_IsInsideTheRange()
    {
        var grants = new[] { 0.0, 0.0, 0.0, 0.0, 0.0 };

        var top = TopOfRange(Total, Target, BufferPool, grants);

        Assert.True(Target.Max() <= top, $"Target Memory peaks at {Target.Max()} GB; the range tops out at {top} GB.");
        Assert.True(Total.Max() <= top);
        Assert.True(BufferPool.Max() <= top);
    }

    [Fact]
    public void MemoryGrantsAboveTotal_AreInsideTheRange()
    {
        var target = new[] { 0.9, 0.9, 0.9, 0.9, 0.9 };
        var grants = new[] { 0.0, 2.4, 0.3, 0.0, 0.0 };

        var top = TopOfRange(Total, target, BufferPool, grants);

        Assert.True(grants.Max() <= top, $"Memory Grants peak at {grants.Max()} GB; the range tops out at {top} GB.");
    }

    [Fact]
    public void TotalAsTheLargestSeries_KeepsTheRangeTotalAlreadyHad()
    {
        var target = new[] { 0.9, 0.9, 0.9, 0.9, 0.9 };
        var grants = new[] { 0.0, 0.1, 0.0, 0.0, 0.0 };

        var top = TopOfRange(Total, target, BufferPool, grants);

        Assert.Equal(ChartStyle.ComputeYLimitsWithLegendPadding(0, Total.Max()).YMax, top, 9);
    }

    [Fact]
    public void AnEmptySeries_IsSkipped()
    {
        Assert.Equal(Target.Max(), DataMax(Total, Target, []), 9);
    }
}

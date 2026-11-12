/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7386: Lite's Overview lanes draw one shared X axis, and it starts at the later of the picked start and the server's data
/// start. The real lanes are built on the shared STA thread and read back; the pure rule is covered by Darling's
/// <c>ChartAxisStartTests</c> (the helper is shared code).
/// </summary>
public sealed class ChartAxisStartLiteTests
{
    private static (HashSet<double> Lefts, double Right) LanesAxis(DateTime? dataStart) =>
        StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl { ServerDataStartUtc = dataStart };
            lanes.SyncXAxes(24, null, null);
            var charts = new[] { lanes.CpuChart, lanes.WaitStatsChart, lanes.BlockingChart, lanes.MemoryChart, lanes.FileIoChart };
            return (charts.Select(c => c.Plot.Axes.GetLimits().Left).ToHashSet(), lanes.CpuChart.Plot.Axes.GetLimits().Right);
        });

    [Fact]
    public void TheLanesOnAServerWithThreeHours_ShareAStartNearItsDataStart()
    {
        var (lefts, right) = LanesAxis(DateTime.UtcNow.AddHours(-3));

        Assert.Single(lefts);
        Assert.InRange(lefts.Single(), DateTime.UtcNow.AddHours(-3.3).ToOADate(), DateTime.UtcNow.AddHours(-2.9).ToOADate());
        Assert.InRange(right, DateTime.UtcNow.AddMinutes(-1).ToOADate(), DateTime.UtcNow.AddMinutes(1).ToOADate());
    }

    [Fact]
    public void TheLanesWithNoKnownDataStart_KeepThePickedDay()
    {
        var (lefts, _) = LanesAxis(null);

        Assert.Single(lefts);
        Assert.True(lefts.Single() < DateTime.UtcNow.AddHours(-23).ToOADate());
    }

    [Fact]
    public void ALaneWithAPointOlderThanTheServersDataStart_KeepsItInsideTheSharedAxis()
    {
        var oldest = DateTime.UtcNow.AddHours(-10);
        var (lefts, _) = StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl { ServerDataStartUtc = DateTime.UtcNow.AddHours(-3) };
            lanes.MemoryChart.Plot.Add.Scatter(new[] { oldest.ToOADate(), DateTime.UtcNow.ToOADate() }, new[] { 1.0, 2.0 });
            lanes.SyncXAxes(24, null, null);
            var charts = new[] { lanes.CpuChart, lanes.WaitStatsChart, lanes.BlockingChart, lanes.MemoryChart, lanes.FileIoChart };
            return (charts.Select(c => c.Plot.Axes.GetLimits().Left).ToHashSet(), 0.0);
        });

        Assert.Single(lefts);
        Assert.True(lefts.Single() <= oldest.ToOADate());
    }
}

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
using System.Reflection;
using System.Windows;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7386: charts that draw something other than collected points must not decide where the shared X axis starts. These run the real
/// paths: the Overview lanes with their crosshair lines attached and the baseline band drawn (as a refresh leaves them), and the
/// Blocking tab's charts, whose zero baselines and padded series reach back to the picked start. Each of them left the axis at the
/// picked start (a full day for a server with three hours of history) before the baselines stopped counting.
/// </summary>
public sealed class ChartAxisStartSyntheticTests
{
    private static readonly string[] LaneNames = ["CpuChart", "WaitStatsChart", "BlockingChart", "MemoryChart", "FileIoChart"];

    private static List<(double Time, double Value)> ThreeHours() =>
        Enumerable.Range(0, 37).Select(i => (DateTime.UtcNow.AddHours(-3).AddMinutes(i * 5).ToOADate(), 10.0 + i % 7)).ToList();

    private static T Private<T>(object target, string name) =>
        (T)target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(target)!;

    /* Invoke needs every parameter: the optional ones the caller leaves off (UpdateLane's #7624 read arguments) are filled with their defaults. */
    private static void Call(object target, string name, params object?[] args)
    {
        var method = target.GetType().GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)!;
        var full = Enumerable.Range(0, method.GetParameters().Length).Select(i => i < args.Length ? args[i] : Type.Missing).ToArray();
        method.Invoke(target, full);
    }

    /// <summary>Draws the five Overview lanes the way a refresh leaves them: data three hours back, the baseline band and mean line on the
    /// lanes that have a baseline, the crosshair manager's hidden vertical lines attached just before the axes are synced.</summary>
    private static (HashSet<double> Lefts, double Right) LanesAfterARefresh(bool withData)
    {
        return StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl { ServerDataStartUtc = DateTime.UtcNow.AddHours(-3) };
            var manager = new CorrelatedCrosshairManager();
            var charts = LaneNames.Select(n => (ScottPlot.WPF.WpfPlot)lanes.FindName(n)!).ToArray();
            for (var i = 0; i < charts.Length; i++)
            {
                manager.AddLane(charts[i], LaneNames[i], "u");
            }

            typeof(CorrelatedTimelineLanesControl).GetField("_crosshairManager", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(lanes, manager);

            var baseline = new BaselineBucket { Mean = 20, StdDev = 5, SampleCount = 100, Median = 20, Mad = 3, Tier = BaselineTier.Flat };
            Assert.True(baseline.EffectiveStdDev > 0);
            var data = withData ? ThreeHours() : new List<(double Time, double Value)>();
            var empty = new List<(double Time, double Value)>();
            manager.PrepareForRefresh();
            Call(lanes, "UpdateLane", lanes.CpuChart, "SQL CPU", data, "#4FC3F7", null, null, baseline, 0.0);
            Call(lanes, "UpdateLane", lanes.WaitStatsChart, "Wait ms/sec", data, "#FFB74D", null, null, baseline, 100.0);
            Call(lanes, "UpdateBlockingLane", withData ? data.Take(3).ToList() : empty, empty, baseline, 1.0, "reports");
            Call(lanes, "UpdateLane", lanes.MemoryChart, "Buffer Pool MB", data, "#CE93D8", null, null, null, 0.0);
            Call(lanes, "UpdateLane", lanes.FileIoChart, "I/O ms", data, "#81C784", null, null, baseline, 2.0);

            /* The order a refresh uses: the lines go on after the lanes are drawn and before the axes are synced. */
            manager.ReattachVLines();
            lanes.SyncXAxes(24, null, null);
            return (charts.Select(c => c.Plot.Axes.GetLimits().Left).ToHashSet(), lanes.CpuChart.Plot.Axes.GetLimits().Right);
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheOverviewLanes_WithTheirCrosshairLinesAndBaselineBand_StillShareTheTrimmedStart(bool withData)
    {
        var (lefts, _) = LanesAfterARefresh(withData);

        Assert.Single(lefts);
        Assert.InRange(lefts.Single(), DateTime.UtcNow.AddHours(-3.4).ToOADate(), DateTime.UtcNow.AddHours(-2.9).ToOADate());
    }

    public enum TabChart { LockWait, BlockingTrend, DeadlockTrend, BlockingDuration }

    /// <summary>Draws one Blocking tab chart on the real tab for a 24 hour range, the server's data starting three hours ago, and returns its X limits.</summary>
    private static (double Left, double Right) BlockingChartAxis(TabChart which, DateTime? firstPoint, DateTime? dataStart)
    {
        return StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("NumericCell"))
            {
                added = new ResourceDictionary
                {
                    Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
                };
                app.Resources.MergedDictionaries.Add(added);
            }

            try
            {
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "axis-start-test", "axis-start-test", true, null));
                var window = new Window { Content = tab, Width = 1800, Height = 1000 };
                window.Show();
                try
                {
                    var picker = tab.FindName("RangePicker")!;
                    picker.GetType().GetProperty("Value")!.SetValue(picker, TimeRangeSpec.Relative(TimeSpan.FromHours(24)));
                    tab.ServerDataStartUtc = dataStart;
                    var times = firstPoint is DateTime t
                        ? Enumerable.Range(0, 20).Select(i => t.AddMinutes(i * 5)).ToList()
                        : new List<DateTime>();
                    string chartName;
                    switch (which)
                    {
                        case TabChart.LockWait:
                            chartName = "LockWaitTrendChart";
                            tab.RenderLockWaitTrendChart(times.Select(x => new LockWaitTrendPoint(x, "LCK_M_X", 5)).ToList());
                            break;
                        case TabChart.BlockingTrend:
                            chartName = "BlockingTrendChart";
                            tab.RenderBlockingTrendChart(times.Select(x => new BlockingTrendPoint(x, 2)).ToList());
                            break;
                        case TabChart.DeadlockTrend:
                            chartName = "DeadlockTrendChart";
                            tab.RenderDeadlockTrendChart(times.Select(x => new BlockingTrendPoint(x, 1)).ToList());
                            break;
                        default:
                            chartName = "BlockingDurationChart";
                            tab.RenderBlockingDurationChart(times.Select(x => new BlockingDurationStatsPoint(x, 1, 1000, 1000, 500)).ToList());
                            break;
                    }

                    var limits = ((ScottPlot.WPF.WpfPlot)tab.FindName(chartName)!).Plot.Axes.GetLimits();
                    return (limits.Left, limits.Right);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                if (added is not null)
                {
                    app.Resources.MergedDictionaries.Remove(added);
                }
            }
        });
    }

    [Theory]
    [InlineData(TabChart.LockWait)]
    [InlineData(TabChart.BlockingTrend)]
    [InlineData(TabChart.DeadlockTrend)]
    [InlineData(TabChart.BlockingDuration)]
    public void ABlockingTabChart_WithDataFromThreeHoursBack_StartsWhereTheServerDataStarts(TabChart chart)
    {
        var (left, right) = BlockingChartAxis(chart, DateTime.UtcNow.AddHours(-3), DateTime.UtcNow.AddHours(-3));

        Assert.InRange(left, DateTime.UtcNow.AddHours(-3.4).ToOADate(), DateTime.UtcNow.AddHours(-2.9).ToOADate());
        Assert.InRange(right, DateTime.UtcNow.AddMinutes(-2).ToOADate(), DateTime.UtcNow.AddMinutes(2).ToOADate());
    }

    [Theory]
    [InlineData(TabChart.LockWait)]
    [InlineData(TabChart.BlockingTrend)]
    [InlineData(TabChart.DeadlockTrend)]
    [InlineData(TabChart.BlockingDuration)]
    public void ABlockingTabChart_WithNoDataAtAll_KeepsTheSameStartAsTheOnesWithData(TabChart chart)
    {
        var (left, _) = BlockingChartAxis(chart, null, DateTime.UtcNow.AddHours(-3));

        Assert.InRange(left, DateTime.UtcNow.AddHours(-3.4).ToOADate(), DateTime.UtcNow.AddHours(-2.9).ToOADate());
    }

    [Theory]
    [InlineData(TabChart.LockWait)]
    [InlineData(TabChart.BlockingTrend)]
    [InlineData(TabChart.DeadlockTrend)]
    [InlineData(TabChart.BlockingDuration)]
    public void ABlockingTabChart_WithAPointOlderThanTheServerDataStart_NeverHidesIt(TabChart chart)
    {
        var oldest = DateTime.UtcNow.AddHours(-10);
        var (left, _) = BlockingChartAxis(chart, oldest, DateTime.UtcNow.AddHours(-3));

        Assert.True(left <= oldest.ToOADate(), $"axis starts at {DateTime.FromOADate(left):u}, after the oldest point");
        Assert.True(left >= DateTime.UtcNow.AddHours(-24.1).ToOADate());
    }

    [Fact]
    public void ABaselineOrAPaddedSeries_NeverDecidesTheStart_ButItsRealPointsStillDo()
    {
        var now = DateTime.UtcNow;
        var plot = new ScottPlot.Plot();
        ChartAxisStart.Baseline(plot.Add.Scatter(new[] { now.AddHours(-24).ToOADate(), now.ToOADate() }, new[] { 0.0, 0.0 }));
        Assert.Null(ChartAxisStart.EarliestPlotted(plot));

        var padded = new[] { now.AddHours(-24).ToOADate(), now.AddHours(-3).ToOADate(), now.AddHours(-2).ToOADate() };
        ChartAxisStart.PaddedFrom(plot.Add.Scatter(padded, new[] { 0.0, 4.0, 5.0 }), padded[1]);
        Assert.Equal(now.AddHours(-3).ToOADate(), ChartAxisStart.EarliestPlotted(plot)!.Value.ToOADate(), precision: 6);

        var hidden = plot.Add.Scatter(new[] { now.AddHours(-20).ToOADate() }, new[] { 1.0 });
        hidden.IsVisible = false;
        Assert.Equal(now.AddHours(-3).ToOADate(), ChartAxisStart.EarliestPlotted(plot)!.Value.ToOADate(), precision: 6);

        plot.Add.Scatter(new[] { now.AddHours(-8).ToOADate() }, new[] { 1.0 });
        Assert.Equal(now.AddHours(-8).ToOADate(), ChartAxisStart.EarliestPlotted(plot)!.Value.ToOADate(), precision: 6);
    }
}

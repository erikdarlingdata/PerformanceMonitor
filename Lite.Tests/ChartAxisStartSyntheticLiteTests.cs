/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using Darling.Tests;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7386, Lite's side of Darling's <c>ChartAxisStartSyntheticTests</c>: the Overview lanes with the crosshair manager's lines attached
/// and the baseline band drawn, and the Blocking tab's charts whose zero baselines and padded series reach back to the picked start.
/// Before the baselines stopped counting, each of these kept the picked day for a server with three hours of history.
/// </summary>
[Collection("credential-manager")]
public sealed class ChartAxisStartSyntheticLiteTests
{
    private static readonly string[] LaneNames = ["CpuChart", "WaitStatsChart", "BlockingChart", "MemoryChart", "FileIoChart"];

    private static List<(double Time, double Value)> ThreeHours() =>
        Enumerable.Range(0, 37).Select(i => (DateTime.UtcNow.AddHours(-3).AddMinutes(i * 5).ToOADate(), 10.0 + i % 7)).ToList();

    private static void Call(object target, string name, params object?[] args) =>
        target.GetType().GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(target, args);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheOverviewLanes_WithTheirCrosshairLinesAndBaselineBand_StillShareTheTrimmedStart(bool withData)
    {
        var lefts = StaTestThread.Run(() =>
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
            var methods = typeof(CorrelatedTimelineLanesControl).GetMethods(BindingFlags.Instance | BindingFlags.NonPublic);

            manager.PrepareForRefresh();
            foreach (var chart in new[] { lanes.CpuChart, lanes.WaitStatsChart, lanes.MemoryChart, lanes.FileIoChart })
            {
                var update = methods.Single(m => m.Name == "UpdateLane");
                var args = update.GetParameters().Select(p => p.HasDefaultValue ? p.DefaultValue : null).ToArray();
                args[0] = chart;
                args[1] = "lane";
                args[2] = data;
                args[3] = "#4FC3F7";
                var baselineAt = update.GetParameters().ToList().FindIndex(p => p.ParameterType == typeof(BaselineBucket));
                if (baselineAt >= 0)
                {
                    args[baselineAt] = baseline;
                }

                update.Invoke(lanes, args);
            }

            /* The order a refresh uses: the crosshair lines go on after the lanes are drawn and before the axes are synced. */
            manager.ReattachVLines();
            lanes.SyncXAxes(24, null, null);
            return charts.Select(c => c.Plot.Axes.GetLimits().Left).ToHashSet();
        });

        Assert.Single(lefts);
        Assert.InRange(lefts.Single(), DateTime.UtcNow.AddHours(-3.4).ToOADate(), DateTime.UtcNow.AddHours(-2.9).ToOADate());
    }

    public enum TabChart { LockWait, BlockingTrend, DeadlockTrend, BlockingDuration }

    private static double BlockingChartLeft(TabChart which, DateTime? firstPoint)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ChartAxisStartSyn_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() =>
            {
                var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                ResourceDictionary? added = null;
                if (!app.Resources.Contains("NumericCell"))
                {
                    added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
                    app.Resources.MergedDictionaries.Add(added);
                }

                try
                {
                    return DrawAndRead(which, firstPoint, duckDb);
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
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static double DrawAndRead(TabChart which, DateTime? firstPoint, DuckDbInitializer duckDb)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "axis-start-test", DisplayName = "axis-start-test", IsEnabled = true };
        var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
        tab.ServerDataStartUtc = DateTime.UtcNow.AddHours(-3);
        var times = firstPoint is DateTime t
            ? Enumerable.Range(0, 20).Select(i => t.AddMinutes(i * 5)).ToList()
            : new List<DateTime>();
        string chartName;
        switch (which)
        {
            case TabChart.LockWait:
                chartName = "LockWaitTrendChart";
                tab.UpdateLockWaitTrendChart(times.Select(x => new LockWaitTrendPoint { CollectionTime = x, WaitType = "LCK_M_X", WaitTimeMsPerSecond = 5 }).ToList(), 24, null, null);
                break;
            case TabChart.BlockingTrend:
                chartName = "BlockingTrendChart";
                tab.UpdateBlockingTrendChart(times.Select(x => new TrendPoint { Time = x, Count = 2 }).ToList(), 24, null, null);
                break;
            case TabChart.DeadlockTrend:
                chartName = "DeadlockTrendChart";
                tab.UpdateDeadlockTrendChart(times.Select(x => new TrendPoint { Time = x, Count = 1 }).ToList(), 24, null, null);
                break;
            default:
                chartName = "BlockingDurationChart";
                tab.UpdateBlockingDurationChart(times.Select(x => new BlockingDurationStatsPoint(x, 1, 1000, 1000, 500)).ToList(), 24, null, null);
                break;
        }

        return ((ScottPlot.WPF.WpfPlot)tab.FindName(chartName)!).Plot.Axes.GetLimits().Left;
    }

    [Theory]
    [InlineData(TabChart.LockWait)]
    [InlineData(TabChart.BlockingTrend)]
    [InlineData(TabChart.DeadlockTrend)]
    [InlineData(TabChart.BlockingDuration)]
    public void ABlockingTabChart_WithDataFromThreeHoursBack_OrNoData_StartsWhereTheServerDataStarts(TabChart chart)
    {
        Assert.InRange(BlockingChartLeft(chart, DateTime.UtcNow.AddHours(-3)), DateTime.UtcNow.AddHours(-3.4).ToOADate(), DateTime.UtcNow.AddHours(-2.9).ToOADate());
        Assert.InRange(BlockingChartLeft(chart, null), DateTime.UtcNow.AddHours(-3.4).ToOADate(), DateTime.UtcNow.AddHours(-2.9).ToOADate());
    }

    [Theory]
    [InlineData(TabChart.LockWait)]
    [InlineData(TabChart.BlockingTrend)]
    [InlineData(TabChart.DeadlockTrend)]
    [InlineData(TabChart.BlockingDuration)]
    public void ABlockingTabChart_WithAPointOlderThanTheServerDataStart_NeverHidesIt(TabChart chart)
    {
        var oldest = DateTime.UtcNow.AddHours(-10);

        Assert.True(BlockingChartLeft(chart, oldest) <= oldest.ToOADate());
    }
}

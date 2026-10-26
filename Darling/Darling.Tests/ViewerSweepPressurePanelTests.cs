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
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6384: the Collection Health tab's Sweep Pressure sub-tab. The real SQL Server tab is built on the shared STA thread and given a
/// report, so a panel whose controls the XAML does not name, or whose grids stay empty for a report that has rows, fails here.
/// </summary>
public sealed class ViewerSweepPressurePanelTests
{
    private static SweepPressureReport Report(params HeaviestCollectorRow[] heaviest) =>
        new(new SweepPressure(
                BusyMsPerMinute: 7_500, BusyPercent: 12.5, Verdict: "OK", PeakCycleMs: 65_000, PeakCyclePercent: 108.3,
                PeakCycleRisk: "BODY_OVERRUN", PeakCollectorName: "memory_clerks", PeakCollectorPeakRunMs: 12_000,
                PeakCollectorAvgDurationMs: 12_000, PeakCollectorFrequencyMinutes: 5),
            heaviest);

    private static (int StatLines, int GridRows, string Message, Visibility MessageVisibility) OnTab(SweepPressureReport? report)
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
                    new DarlingServer(1, "sweep-panel-test", "sweep-panel-test", true, null));
                var window = new Window { Content = tab, Width = 1800, Height = 1000 };
                window.Show();
                try
                {
                    /* Showing the window starts the tab's own database read on a thread-pool continuation; the drill-down skips it the same way. */
                    typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                    tab.ShowSweepPressure(report);
                    var stats = (DataGrid)tab.FindName("SweepPressureStatsGrid");
                    var heaviestGrid = (DataGrid)tab.FindName("HeaviestCollectorsGrid");
                    var message = (TextBlock)tab.FindName("SweepPressureNoDataMessage");
                    return (
                        (stats.ItemsSource as IEnumerable<SweepStatRow>)?.Count() ?? 0,
                        (heaviestGrid.ItemsSource as IEnumerable<HeaviestCollectorGridRow>)?.Count() ?? 0,
                        message.Text,
                        message.Visibility);
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

    [Fact]
    public void AReportWithRows_FillsTheStatLinesAndTheHeaviestGrid_AndHidesTheEmptyMessage()
    {
        var shown = OnTab(Report(
            new HeaviestCollectorRow("query_stats", 5_000, 9_000, 50_000, 1, 5_000, 15.0),
            new HeaviestCollectorRow("memory_clerks", 12_000, 12_000, 12_000, 5, 2_400, 20.0)));

        Assert.Equal(6, shown.StatLines);
        Assert.Equal(2, shown.GridRows);
        Assert.Equal(Visibility.Collapsed, shown.MessageVisibility);
    }

    [Fact]
    public void AReportWithNoTimedCollector_SaysSoInTheWebsOwnWords_WithTheStatLinesStillShown()
    {
        var shown = OnTab(Report());

        Assert.Equal(6, shown.StatLines);
        Assert.Equal(0, shown.GridRows);
        Assert.Equal(SweepPressurePanel.NoTimings, shown.Message);
        Assert.Equal(Visibility.Visible, shown.MessageVisibility);
    }

    [Fact]
    public void NoReport_ClearsBothGrids_AndSaysThereAreNoCollectionLogRows()
    {
        var shown = OnTab(null);

        Assert.Equal(0, shown.StatLines);
        Assert.Equal(0, shown.GridRows);
        Assert.Equal(SweepPressurePanel.NoLogRows, shown.Message);
        Assert.Equal(Visibility.Visible, shown.MessageVisibility);
    }

    [Theory]
    [InlineData(250, "250 ms")]
    [InlineData(999, "999 ms")]
    [InlineData(1_000, "1.0 s")]
    [InlineData(59_000, "59.0 s")]
    [InlineData(90_000, "1.5 min")]
    [InlineData(7_200_000, "2.0 h")]
    public void TheDurationsAreWordedAsTheWebWordsThem(double ms, string expected)
    {
        var previous = System.Globalization.CultureInfo.CurrentCulture;
        System.Globalization.CultureInfo.CurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
        try
        {
            Assert.Equal(expected, SweepPressurePanel.FormatMs(ms));
        }
        finally
        {
            System.Globalization.CultureInfo.CurrentCulture = previous;
        }
    }
}

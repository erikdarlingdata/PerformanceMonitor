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
using System.Windows.Controls;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// System Events > Contention Events at the 900x600 minimum window: the Non-Yielding Tasks and Latch Warnings plots stand as tall
/// as the Bad Pages plot in the same cell of the Corruption Events grid, with no band of nothing under their axes. Only Sick
/// Spinlocks and CPU Comparison carry legends, and a legend under one plot of an aligned group is room taken from every plot of
/// the group, so the two counter charts share a group with each other and not with the two legend charts. The Darling Viewer's
/// twin is ViewerSystemEventsContentionPlotsFillTheirCardsTests in Darling.Tests.
/// </summary>
[Collection("credential-manager")]
public sealed class SystemEventsContentionPlotsFillTheirCardsTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    /// <summary>How much shorter than the Bad Pages plot a counter plot on Contention Events may be: a rounding margin, not a legend's height.</summary>
    private const double Tolerance = 6;

    [Fact]
    public void TheCounterCharts_StandAsTallAsCorruptionEvents_AtTheMinimumWindow()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ContentionPlots_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var problems = StaTestThread.Run(() => Run(duckDb));
            Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static List<string> Run(DuckDbInitializer duckDb)
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
            var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "contention-plots", DisplayName = "contention-plots", IsEnabled = true };
            var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
            typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var main = (TabControl)tab.FindName("MainTabControl");
                main.SelectedItem = main.Items.OfType<TabItem>().Single(i => Equals(i.Header, "System Events"));
                var sub = (TabControl)tab.FindName("SystemEventsSubTabControl");
                var renderer = (SystemHealthChartRenderer)typeof(ServerTab).GetProperty("SysHealthRenderer", Instance)!.GetValue(tab)!;
                return Measure(window, tab, sub, renderer);
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
    }

    /// <summary>
    /// Renders the Corruption Events grid first, for the Bad Pages plot's height, then the four Contention Events charts with the
    /// series they list (two counters, the spinlock types and the two CPU lines), aligns each group as the tab does, and compares.
    /// </summary>
    internal static List<string> Measure(Window window, FrameworkElement tab, TabControl sub, SystemHealthChartRenderer renderer)
    {
        var data = Records();
        var xMin = data[0].EventTime!.Value.AddMinutes(-10).ToOADate();
        var xMax = data[^1].EventTime!.Value.AddMinutes(10).ToOADate();

        sub.SelectedIndex = 0;
        Settle(window);
        var badPages = Chart(tab, "BadPagesChart");
        renderer.RenderCounterChart(badPages, null, data, d => d.BadPagesDetected, "Bad Pages", "#E74C3C", xMin, xMax);
        Align(window, badPages);
        var reference = PlotHeight(badPages);

        sub.SelectedIndex = 1;
        Settle(window);
        var nonYielding = Chart(tab, "NonYieldingTasksChart");
        var latch = Chart(tab, "LatchWarningsChart");
        renderer.RenderCounterChart(nonYielding, null, data, d => d.NonYieldingTasksReported, "Non-Yielding Tasks", "#E74C3C", xMin, xMax);
        renderer.RenderCounterChart(latch, null, data, d => d.LatchWarnings, "Latch Warnings", "#3498DB", xMin, xMax);
        renderer.RenderSickSpinlocksChart(Chart(tab, "SickSpinlocksChart"), null, data, xMin, xMax);
        renderer.RenderCpuComparisonChart(Chart(tab, "CpuComparisonChart"), null, data, xMin, xMax);
        Align(window, nonYielding, latch, Chart(tab, "SickSpinlocksChart"), Chart(tab, "CpuComparisonChart"));

        var problems = new List<string>();
        if (reference < 100)
        {
            problems.Add($"Bad Pages plot is only {reference:0.#} px tall, so there is nothing to compare against");
        }
        foreach (var (name, chart) in new[] { ("Non-Yielding Tasks", nonYielding), ("Latch Warnings", latch) })
        {
            var height = PlotHeight(chart);
            if (height < reference - Tolerance)
            {
                problems.Add($"{name}: the plot is {height:0.#} px tall against Bad Pages' {reference:0.#} px in the same cell, a {reference - height:0.#} px band of nothing under its axis");
            }
        }
        return problems;
    }

    /// <summary>Runs the group aligner for each chart's group now, as its timer would after the render, then lets the layout settle.</summary>
    private static void Align(Window window, params WpfPlot[] charts)
    {
        Settle(window);
        foreach (var group in charts.Select(ChartGroupAligner.FindGroup).OfType<FrameworkElement>().Distinct())
        {
            ChartGroupAligner.Check(group);
        }
        Settle(window);
    }

    /// <summary>The plot's data area, in pixels, as its last layout at the control's own size leaves it.</summary>
    private static double PlotHeight(WpfPlot chart)
    {
        /* #8644: at the size the control paints at (device pixels), as the group aligner measures: at a 250% Windows scale the control's own size draws a plot a third as tall. */
        chart.Plot.RenderInMemory(Math.Max(1, (int)(chart.ActualWidth * chart.DisplayScale)), Math.Max(1, (int)(chart.ActualHeight * chart.DisplayScale)));
        return chart.Plot.RenderManager.LastRender.Layout.DataRect.Height;
    }

    private static WpfPlot Chart(FrameworkElement tab, string name) => (WpfPlot)tab.FindName(name);

    private static void Settle(Window window) => ClippedTextProbe.Settle(window);

    /// <summary>Six SYSTEM snapshots ten minutes apart, with a sick spinlock type and both CPU figures, so the two legend charts list their series.</summary>
    internal static List<SystemHealthRecord> Records()
    {
        var end = new DateTime(2026, 11, 8, 12, 0, 0, DateTimeKind.Unspecified);
        return Enumerable.Range(0, 6).Select(i => new SystemHealthRecord
        {
            EventTime = end.AddMinutes(-10 * (5 - i)),
            BadPagesDetected = i,
            NonYieldingTasksReported = i,
            LatchWarnings = i * 2,
            SpinlockBackoffs = 100 + i,
            SickSpinlockType = "LOCK_HASH",
            SqlCpuUtilization = 20 + i,
            SystemCpuUtilization = 40 + i,
        }).ToList();
    }
}

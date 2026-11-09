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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's System Events > Contention Events at a 900x600 window: the Non-Yielding Tasks and Latch Warnings plots stand as
/// tall as the Bad Pages plot in the same cell of the Corruption Events grid, with no band of nothing under their axes. Only Sick
/// Spinlocks and CPU Comparison carry legends, and a legend under one plot of an aligned group is room taken from every plot of
/// the group, so the two counter charts share a group with each other and not with the two legend charts. Lite's twin is
/// SystemEventsContentionPlotsFillTheirCardsTests in Lite.Tests.
/// </summary>
public sealed class ViewerSystemEventsContentionPlotsFillTheirCardsTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    /// <summary>How much shorter than the Bad Pages plot a counter plot on Contention Events may be: a rounding margin, not a legend's height.</summary>
    private const double Tolerance = 6;

    [Fact]
    public void TheCounterCharts_StandAsTallAsCorruptionEvents_AtTheMinimumWindow()
    {
        var problems = StaTestThread.Run(Run);
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    private static List<string> Run()
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }
        try
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "contention-plots", "contention-plots", true, null));
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var main = (TabControl)tab.FindName("InnerTabs");
                main.SelectedItem = main.Items.OfType<TabItem>().Single(i => Equals(i.Header, "System Events"));
                var sub = (TabControl)tab.FindName("SystemEventsSubTabControl");
                var renderer = (SystemHealthChartRenderer)typeof(ViewerServerTab).GetProperty("SysHealthRenderer", Instance)!.GetValue(tab)!;
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
    private static List<string> Measure(Window window, FrameworkElement tab, TabControl sub, SystemHealthChartRenderer renderer)
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
        foreach (var group in charts.Select(c => ChartGroupAligner.FindGroup(c)).OfType<FrameworkElement>().Distinct())
        {
            ChartGroupAligner.Check(group);
        }
        Settle(window);
    }

    /// <summary>The plot's data area, in pixels, as its last layout at the control's own size leaves it.</summary>
    private static double PlotHeight(WpfPlot chart)
    {
        chart.Plot.RenderInMemory(Math.Max(1, (int)chart.ActualWidth), Math.Max(1, (int)chart.ActualHeight));
        return chart.Plot.RenderManager.LastRender.Layout.DataRect.Height;
    }

    private static WpfPlot Chart(FrameworkElement tab, string name) => (WpfPlot)tab.FindName(name);

    private static void Settle(Window window) => ClippedTextProbe.Settle(window);

    /// <summary>Six SYSTEM snapshots ten minutes apart, with a sick spinlock type and both CPU figures, so the two legend charts list their series.</summary>
    private static List<SystemHealthRecord> Records()
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

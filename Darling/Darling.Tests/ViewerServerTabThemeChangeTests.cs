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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8898: the charts on an open Viewer server tab follow a theme change at once. A chart takes the theme in its own draw method, so before
/// the fix every chart on a tab that was already open kept the old theme's white or dark figure until its next load. The real tab is built
/// off screen, the theme is switched through <see cref="ThemeManager.Apply"/> (what Settings does for a pick, its live preview and the
/// revert on Close), and each chart's figure background is read back.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerServerTabThemeChangeTests
{
    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private static void Themed(Action<Application> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = ThemeUri;
            try
            {
                ThemeManager.Apply("Dark");
                body(app);
            }
            finally
            {
                ThemeManager.Apply(previous);
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in before)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }

                ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
            }
        });

    private static ViewerServerTab NewTab()
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "theme-change-test", "theme-change-test", true, null));
        typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
        return tab;
    }

    /// <summary>Every chart the tab owns, found by walking the tree (the XAML charts and the Overview lanes) plus the code-built PostgreSQL trend chart added here.</summary>
    private static List<WpfPlot> AllCharts(ViewerServerTab tab) => ChartLayoutProbe.Logical<WpfPlot>(tab).ToList();

    private static string Figure(WpfPlot chart) => chart.Plot.FigureBackground.Color.ToHex();

    private static WpfPlot AddPgTrendChart(ViewerServerTab tab)
    {
        var host = new StackPanel();
        var made = typeof(ViewerServerTab).GetMethod("PgTrendChart", Instance)!.Invoke(tab, new object[] { host, "theme-test", "ms", "Theme test" })!;
        return (WpfPlot)made.GetType().GetField("Item1")!.GetValue(made)!;
    }

    [Fact]
    public void EveryChartOnAnOpenServerTab_TakesTheNewTheme_WhenTheThemeChanges() =>
        Themed(_ =>
        {
            var tab = NewTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var pgChart = AddPgTrendChart(tab);
                var charts = AllCharts(tab);
                Assert.True(charts.Count > 30, $"expected the tab's whole chart set, found {charts.Count}");
                var darkFigure = Figure(charts[0]);

                ThemeManager.Apply("Light");

                var lightFigure = Figure(charts[0]);
                Assert.NotEqual(darkFigure, lightFigure);
                var stale = charts.Concat(new[] { pgChart }).Where(c => Figure(c) != lightFigure).Select(c => string.IsNullOrEmpty(c.Name) ? "(unnamed chart)" : c.Name).ToList();
                Assert.True(stale.Count == 0, $"{stale.Count} charts kept the old theme after a change to Light: {string.Join(", ", stale)}");

                ThemeManager.Apply("Dark");
                Assert.All(charts.Concat(new[] { pgChart }), c => Assert.Equal(darkFigure, Figure(c)));
            }
            finally
            {
                window.Close();
                tab.Dispose();
            }
        });

    [Fact]
    public void AClosedServerTab_StopsFollowingTheTheme() =>
        Themed(_ =>
        {
            var tab = NewTab();
            var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var chart = AllCharts(tab).First();
                var darkFigure = Figure(chart);
                tab.Dispose();

                ThemeManager.Apply("Light");

                Assert.Equal(darkFigure, Figure(chart));
            }
            finally
            {
                window.Close();
            }
        });
}

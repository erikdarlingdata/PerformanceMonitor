/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9109: a theme change restyles the charts of an open Lite server tab and changes colours only. The theme sets the axis margins, and
/// ScottPlot refits every axis when a margin is set, so the restyle used to drop the limits the load had set until the next refresh.
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
public sealed class ChartRestyleKeepsAxesLiteTests
{
    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");

    private static (double, double, double, double, object, object) Axes(WpfPlot chart) =>
        (chart.Plot.Axes.Bottom.Min, chart.Plot.Axes.Bottom.Max, chart.Plot.Axes.Left.Min, chart.Plot.Axes.Left.Max,
            chart.Plot.Axes.Left.TickGenerator, chart.Plot.Axes.Bottom.TickGenerator);

    [Fact]
    public void EveryChartOnAnOpenLiteServerTab_KeepsItsAxes_WhenTheThemeChanges()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ChartRestyleAxes_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var moved = StaTestThread.Run(() =>
            {
                var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                var before = app.Resources.MergedDictionaries.ToList();
                var previous = ThemeManager.CurrentTheme;
                ThemeManager.ThemeUriProvider = ThemeUri;
                try
                {
                    ThemeManager.Apply("Dark");
                    var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "restyle-axes-test", DisplayName = "restyle-axes-test", IsEnabled = true };
                    var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                    typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                    var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                    StaTestThread.ShowOffScreen(window);
                    try
                    {
                        var charts = ChartLayoutProbe.Logical<WpfPlot>(tab).ToList();
                        Assert.True(charts.Count > 20, $"expected the tab's whole chart set, found {charts.Count}");
                        foreach (var chart in charts)
                        {
                            chart.Plot.Add.Scatter(new double[] { 1, 2, 3 }, new double[] { 10, 20, 67 });
                            chart.Plot.Axes.SetLimits(0, 100, 0, 105);
                        }

                        var axesBefore = charts.Select(Axes).ToList();
                        var darkFigure = charts[0].Plot.FigureBackground.Color.ToHex();

                        ThemeManager.Apply("Light");

                        Assert.NotEqual(darkFigure, charts[0].Plot.FigureBackground.Color.ToHex());
                        return charts.Where((c, i) => !Equals(axesBefore[i], Axes(c))).Select(c => string.IsNullOrEmpty(c.Name) ? "(unnamed chart)" : c.Name).ToList();
                    }
                    finally
                    {
                        window.Close();
                    }
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

            Assert.True(moved.Count == 0, $"{moved.Count} charts changed their axes on a theme change: {string.Join(", ", moved)}");
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch (IOException) { }
        }
    }
}

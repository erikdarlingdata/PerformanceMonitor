/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8220: the Queries > Query Heatmap axes at the app's windows. The Y labels (seven duration buckets) do not touch and the X labels
/// (one per few buckets) stay on one line with room between them, however many buckets the window holds.
/// </summary>
[Collection("credential-manager")]
public sealed class QueryHeatmapLabelTests
{
    private readonly ITestOutputHelper _output;

    public QueryHeatmapLabelTests(ITestOutputHelper output) => _output = output;

    [Theory]
    [InlineData("DarkTheme", 900, 600, 288)]
    [InlineData("LightTheme", 900, 600, 288)]
    [InlineData("CoolBreezeTheme", 900, 600, 288)]
    [InlineData("DarkTheme", 900, 600, 2016)]
    [InlineData("DarkTheme", 1280, 800, 288)]
    [InlineData("LightTheme", 1280, 800, 2016)]
    [InlineData("CoolBreezeTheme", 1280, 800, 288)]
    public void HeatmapLabels_AreReadable(string theme, int width, int height, int buckets)
    {
        var dir = Path.Combine(Path.GetTempPath(), "HeatmapLabels_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var problems = StaTestThread.Run(() => WithTheme(theme, () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "heatmap", DisplayName = "heatmap", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = width, Height = height, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var chart = (WpfPlot)tab.FindName("QueryHeatmapChart");
                    for (var node = (DependencyObject)chart; node is not null; node = LogicalTreeHelper.GetParent(node))
                    {
                        if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
                        {
                            tabs.SelectedItem = item;
                        }
                    }

                    var result = new HeatmapResult
                    {
                        BucketLabels = new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
                        TimeBuckets = Enumerable.Range(0, buckets).Select(i => DateTime.UtcNow.AddMinutes((i - buckets) * 5)).ToArray(),
                        Intensities = new double[7, buckets],
                        CellDetails = new HeatmapCell[7, buckets],
                    };
                    for (var r = 0; r < 7; r++)
                    {
                        for (var c = 0; c < buckets; c++)
                        {
                            result.Intensities[r, c] = (r + c) % 5 == 0 ? 0 : 1 + ((r * 7) + c) % 40;
                        }
                    }

                    typeof(ServerTab).GetMethod("UpdateQueryHeatmapChart", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(tab, new object[] { result });
                    ChartLayoutProbe.Settle(window, new[] { chart });
                    ChartLayoutProbe.SaveWindow(window, $"heatmap-lite-{theme}-{width}x{height}-{buckets}");

                    var found = ChartLayoutProbe.MeasureHeatmapLabels(chart, buckets, 7, out var reading);
                    _output.WriteLine($"{theme} {width}x{height} x {buckets}: {reading}");
                    return found;
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.True(problems.Count == 0, $"{theme} {width}x{height} with {buckets} buckets:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>();
        if (!app.Resources.Contains("NumericCell"))
        {
            added.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
            added.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }

        try
        {
            return body();
        }
        finally
        {
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Remove(dictionary);
            }
        }
    }
}

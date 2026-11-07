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
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8220: the Viewer's Query Heatmap axes at the app's windows. The Y labels (seven duration buckets) do not touch and the X labels
/// stay on one line with room between them, however many buckets the window holds.
/// </summary>
public sealed class ViewerQueryHeatmapLabelTests
{
    private readonly ITestOutputHelper _output;

    public ViewerQueryHeatmapLabelTests(ITestOutputHelper output) => _output = output;

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
        var problems = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "heatmap", "heatmap", true, null));
            typeof(ViewerServerTab).GetField("_refreshInFlight", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!.SetValue(tab, true);
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = width, Height = height, Left = 0, Top = 0 };
            window.Show();
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
                        result.Intensities[r, c] = (r + c) % 5 == 0 ? 0 : 1 + (((r * 7) + c) % 40);
                    }
                }

                typeof(ViewerServerTab).GetMethod("UpdateQueryHeatmapChart", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(tab, new object[] { result });
                ChartLayoutProbe.Settle(window, new[] { chart });
                ChartLayoutProbe.SaveWindow(window, $"heatmap-viewer-{theme}-{width}x{height}-{buckets}");

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

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}

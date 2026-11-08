/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8220: the Viewer's FinOps > Version Store (PVS) hosted the way the main window hosts it. The trend chart keeps a plot of at least
/// 120 DIPs, and the grid under it keeps its space: it is at least 120 DIPs tall and either fully in view or reachable by scrolling.
/// </summary>
public sealed class ViewerFinOpsPvsLayoutTests
{
    private readonly ITestOutputHelper _output;

    public ViewerFinOpsPvsLayoutTests(ITestOutputHelper output) => _output = output;

    [Theory]
    [InlineData("DarkTheme", 900, 600)]
    [InlineData("LightTheme", 900, 600)]
    [InlineData("CoolBreezeTheme", 900, 600)]
    [InlineData("DarkTheme", 1280, 800)]
    [InlineData("LightTheme", 1280, 800)]
    [InlineData("CoolBreezeTheme", 1280, 800)]
    public void PvsGrid_KeepsItsSpaceInView_NextToTheTrendChart(string theme, int width, int height)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var tab = new FinOpsTab();
            var outer = new TabControl();
            outer.Items.Add(new TabItem { Header = "FinOps", Content = tab });
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(outer), Width = width, Height = height, Left = 0, Top = 0 };
            window.Show();
            try
            {
                var grid = (DataGrid)tab.FindName("FinOpsPvsStatsDataGrid");
                var chart = (WpfPlot)tab.FindName("FinOpsPvsTrendChart");
                for (var node = (DependencyObject)grid; node is not null; node = LogicalTreeHelper.GetParent(node))
                {
                    if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
                    {
                        tabs.SelectedItem = item;
                    }
                }

                var trend = new List<PvsTrendPoint>();
                var from = DateTime.UtcNow.AddDays(-7);
                for (var d = 0; d < 5; d++)
                {
                    for (var p = 0; p < 60; p++)
                    {
                        trend.Add(new PvsTrendPoint($"SalesDatabase_{d}", from.AddHours(p * 2.8), 100 + (d * 40) + (30 * Math.Sin((p / 8.0) + d)), 1.5 + d));
                    }
                }

                typeof(FinOpsTab).GetMethod("RenderPvsTrendChart", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(tab, new object[] { trend });
                ChartLayoutProbe.Settle(window, new[] { chart });

                var found = new List<string>();
                var origin = grid.TransformToAncestor(window).Transform(new Point(0, 0));
                var content = (FrameworkElement)window.Content;
                var gridBottom = origin.Y + grid.ActualHeight;
                var hostBottom = content.ActualHeight - 36;
                _output.WriteLine($"{theme} {width}x{height}: grid {grid.ActualHeight:0} DIPs tall, bottom {gridBottom:0} of {hostBottom:0}; chart {chart.ActualWidth:0}x{chart.ActualHeight:0}; legend {ChartLayoutProbe.ReadLegend(chart)}");
                if (grid.ActualHeight < 120)
                {
                    found.Add($"the grid is {grid.ActualHeight:0} DIPs tall, under 120");
                }

                if (gridBottom > hostBottom + 1 && FirstScrollViewerAbove(grid) is null)
                {
                    found.Add($"the grid ends at {gridBottom:0}, below the {hostBottom:0} the window shows, and nothing scrolls");
                }

                foreach (var problem in ChartLayoutProbe.MeasureLegendRule(chart))
                {
                    found.Add($"FinOpsPvsTrendChart: {problem}");
                }

                ChartLayoutProbe.SaveWindow(window, $"viewer-{theme}-{width}x{height}");
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, $"{theme} {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    private static ScrollViewer? FirstScrollViewerAbove(DependencyObject element)
    {
        for (var node = LogicalTreeHelper.GetParent(element); node is not null; node = LogicalTreeHelper.GetParent(node))
        {
            if (node is ScrollViewer sv && sv.VerticalScrollBarVisibility != ScrollBarVisibility.Disabled)
            {
                return sv;
            }
        }

        return null;
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

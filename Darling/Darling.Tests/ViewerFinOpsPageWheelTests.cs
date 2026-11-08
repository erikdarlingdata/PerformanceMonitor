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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8226: the FinOps Utilization and Optimization pages scroll as one, and their grids sit under the page's scroll area. A grid's own
/// scroll viewer marks the mouse wheel handled even when it has nothing left to scroll, so the page takes the wheel from a grid that is
/// at its end (#6133), while a grid that can still scroll that way keeps it; and a grid with hundreds of rows scrolls inside a capped
/// height instead of stretching the page by its whole length. The real tab is hosted on the shared STA thread with the theme merged,
/// the wheel is raised over the grid the way the input system raises it, and the page's offset is read.
/// </summary>
public sealed class ViewerFinOpsPageWheelTests
{
    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    public static IEnumerable<object[]> Pages()
    {
        yield return new object[] { "Optimization", "FinOpsOptimizationScroll", "FinOpsIdleDatabasesDataGrid" };
        yield return new object[] { "Utilization", "FinOpsUtilizationScroll", "FinOpsTopTotalGrid" };
    }

    [Theory]
    [MemberData(nameof(Pages))]
    public void TheWheelOverAGridAtItsEnd_ScrollsThePage_AndAGridThatCanStillScroll_KeepsIt(string header, string pageName, string gridName)
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", () =>
        {
            var found = new List<string>();
            var tab = new FinOpsTab();
            FillGrids(tab);
            var window = new Window { Content = tab, Width = 1000, Height = 600, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var tabs = ChartLayoutProbe.Logical<TabControl>(tab).First();
                tabs.SelectedItem = tabs.Items.OfType<TabItem>().First(t => t.Header as string == header);
                Settle(window);

                var page = Find<ScrollViewer>(tab, pageName);
                var grid = Find<DataGrid>(tab, gridName);
                var inner = Visual<ScrollViewer>(grid).FirstOrDefault();
                if (inner is null)
                {
                    return new List<string> { $"{gridName} drew no scroll viewer of its own" };
                }

                if (page.ScrollableHeight <= 0)
                {
                    found.Add($"{header}: the page has nothing to scroll ({page.ExtentHeight:F0} px of content in {page.ViewportHeight:F0} px)");
                }

                /* A long grid scrolls inside a capped height; it does not stretch the page by its whole length. */
                if (inner.ScrollableHeight <= 0)
                {
                    found.Add($"{gridName}: {grid.Items.Count} rows do not scroll inside the grid; it is {grid.ActualHeight:F0} px tall");
                    return found;
                }

                /* The grid at its top can still scroll down, so a wheel down over it scrolls the grid, not the page. */
                page.ScrollToTop();
                inner.ScrollToTop();
                Settle(window);
                Wheel(inner, -120);
                Settle(window);
                if (page.VerticalOffset > 0)
                {
                    found.Add($"{header}: the page moved to {page.VerticalOffset:F0} for a wheel over {gridName}, which could still scroll");
                }

                if (inner.VerticalOffset <= 0)
                {
                    found.Add($"{gridName}: the grid did not scroll for a wheel over it while it could");
                }

                /* The grid at its end cannot take a wheel down, so the page does. */
                page.ScrollToTop();
                inner.ScrollToEnd();
                Settle(window);
                Wheel(inner, -120);
                Settle(window);
                if (page.VerticalOffset <= 0)
                {
                    found.Add($"{header}: the page did not move for a wheel over {gridName} at its end");
                }
            }
            finally
            {
                window.Close();
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>Raises the wheel over <paramref name="target"/> as the input system does: the preview tunnels first; the bubbling event follows only when the preview was not handled. The timestamp is not read, so it is a constant.</summary>
    private static void Wheel(UIElement target, int delta)
    {
        var preview = new MouseWheelEventArgs(Mouse.PrimaryDevice, 0, delta) { RoutedEvent = UIElement.PreviewMouseWheelEvent, Source = target };
        target.RaiseEvent(preview);
        if (!preview.Handled)
        {
            target.RaiseEvent(new MouseWheelEventArgs(Mouse.PrimaryDevice, 0, delta) { RoutedEvent = UIElement.MouseWheelEvent, Source = target });
        }
    }

    /// <summary>Sixty rows in the two grids the wheel is raised over, so they outgrow a capped height; a dozen in the rest, so each page outgrows its viewport.</summary>
    private static void FillGrids(FinOpsTab tab)
    {
        Find<DataGrid>(tab, "FinOpsTopTotalGrid").ItemsSource = Enumerable.Range(1, 60).Select(i => new { DatabaseDisplay = $"db{i}", DatabaseName = $"db{i}", PctCpu = 1.0 * i, PctIo = 1.0, CpuTimeMs = 100L * i, ExecutionCount = 10L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsTopAvgGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseDisplay = $"db{i}", DatabaseName = $"db{i}", AvgCpuMs = 1.0 * i, AvgIoMb = 1.0, ExecutionCount = 10L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsProvisioningTrendGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DayDisplay = $"day {i}", Status = "RIGHT_SIZED", StatusDisplay = "Right sized", AvgCpuPct = 1.0, P95CpuPct = 2.0, MaxCpuPct = 3.0, MemoryRatio = 1.0 }).ToList();
        Find<DataGrid>(tab, "FinOpsIdleDatabasesDataGrid").ItemsSource = Enumerable.Range(1, 60).Select(i => new { DatabaseName = $"db{i}", TotalSizeMb = 1.0 * i }).ToList();
        Find<DataGrid>(tab, "FinOpsTempdbPressureDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Metric = $"metric {i}", CurrentMb = 1.0, PeakMb = 2.0 }).ToList();
        Find<DataGrid>(tab, "FinOpsWaitCategorySummaryDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { Category = $"category {i}", TotalWaitTimeMs = 1000L * i, WaitingTasks = 10L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsExpensiveQueriesDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DatabaseName = $"db{i}", QueryText = $"select {i}", TotalCpuMs = 100L * i }).ToList();
        Find<DataGrid>(tab, "FinOpsMemoryGrantEfficiencyDataGrid").ItemsSource = Enumerable.Range(1, 12).Select(i => new { DayDisplay = $"day {i}", Day = new DateTime(2026, 1, i), AvgGrantedMb = 1.0, AvgUsedMb = 0.5 }).ToList();
    }

    private static IEnumerable<T> Visual<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var below in Visual<T>(child))
            {
                yield return below;
            }
        }
    }

    private static T Find<T>(FrameworkElement tab, string name) where T : FrameworkElement =>
        tab.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the tab");

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        Dispatcher.CurrentDispatcher.Invoke(DispatcherPriority.Loaded, () => { });
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>
        {
            new() { Source = new Uri(ViewerPack + theme + ".xaml") },
            new() { Source = ThemeManager.SharedStylesUri },
        };
        foreach (var dictionary in added)
        {
            app.Resources.MergedDictionaries.Add(dictionary);
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

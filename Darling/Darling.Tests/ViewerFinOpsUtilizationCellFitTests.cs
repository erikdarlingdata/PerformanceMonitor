/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8113: the FinOps Utilization sub-tab's two "Top Databases" grids show execution counts and CPU totals that reach ten or more digits
/// on a busy server. A numeric column narrower than its widest value is clipped at the cell edge with no ellipsis, so "115,538,069" reads
/// as "115,538,06", a plausible number a tenth of the real one. The real tab is built on the shared STA thread, themed as the app themes
/// it, given rows with nine- to twelve-digit counts, and laid out at 1280 px: every numeric cell's realized TextBlock must have the room
/// its whole formatted value needs. Lite's twin is FinOpsUtilizationCellFitTests in Lite.Tests.
/// </summary>
public sealed class ViewerFinOpsUtilizationCellFitTests
{
    /// <summary>WPF formats a binding's StringFormat in the element's language, en-US unless an app overrides it, not the thread culture.</summary>
    private static readonly CultureInfo Formatting = CultureInfo.GetCultureInfo("en-US");

    [Fact]
    public void TheUtilizationGrids_ShowNineToTwelveDigitCountsInFull_At1280()
    {
        var cut = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1280, Height = 900 };
            window.Show();
            try
            {
                ((FrameworkElement)tab.FindName("FinOpsUtilizationContent")).Visibility = Visibility.Visible;
                ((FrameworkElement)tab.FindName("FinOpsSummaryContent")).Visibility = Visibility.Visible;
                ((FrameworkElement)tab.FindName("FinOpsNoUtilizationMessage")).Visibility = Visibility.Collapsed;
                var total = (DataGrid)tab.FindName("FinOpsTopTotalGrid");
                var avg = (DataGrid)tab.FindName("FinOpsTopAvgGrid");
                total.ItemsSource = Rows();
                avg.ItemsSource = Rows();
                ClippedTextProbe.Settle(window);
                total.BringIntoView();
                ClippedTextProbe.Settle(window);

                var found = CutCells("Top Databases by Total CPU", total,
                    1_234_567_890L.ToString("N0", Formatting), 115_538_069L.ToString("N0", Formatting), 999_999_999_999L.ToString("N0", Formatting));
                found.AddRange(CutCells("Top Databases by Avg CPU / Execution", avg,
                    1_234_567.89m.ToString("N2", Formatting), 123_456.78m.ToString("N2", Formatting), 999_999_999_999L.ToString("N0", Formatting)));
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(cut.Count == 0, string.Join(Environment.NewLine, cut));
    }

    /// <summary>The busiest database on a large server (the issue's 115,538,069 executions), one at twelve digits, and one small row.</summary>
    private static List<TopResourceConsumerRow> Rows() =>
    [
        new() { DatabaseName = "tenant_a", CpuTimeMs = 1_234_567_890, ExecutionCount = 115_538_069, IoTotalMb = 12_345.67m, PctCpu = 61.2m, PctIo = 12.5m, AvgCpuMs = 10.68m, AvgIoMb = 0.11m },
        new() { DatabaseName = "tenant_b", CpuTimeMs = 987_654_321_098, ExecutionCount = 999_999_999_999, IoTotalMb = 98_765.43m, PctCpu = 30.1m, PctIo = 80.0m, AvgCpuMs = 1_234_567.89m, AvgIoMb = 123_456.78m },
        new() { DatabaseName = "tenant_c", CpuTimeMs = 4_210, ExecutionCount = 12_609, IoTotalMb = 1.5m, PctCpu = 0.3m, PctIo = 0.1m, AvgCpuMs = 0.33m, AvgIoMb = 0.01m },
    ];

    /// <summary>
    /// Every realized cell of <paramref name="grid"/> whose text needs more width than its TextBlock was given, plus any of
    /// <paramref name="expected"/> that no cell shows (the rows did not render, so a clean result would prove nothing).
    /// </summary>
    private static List<string> CutCells(string gridName, DataGrid grid, params string[] expected)
    {
        var found = new List<string>();
        var texts = new List<string>();
        foreach (var cell in ClippedTextProbe.Descendants(grid).OfType<DataGridCell>())
        {
            var block = ClippedTextProbe.Descendants(cell).OfType<TextBlock>().FirstOrDefault();
            if (block is null || string.IsNullOrWhiteSpace(block.Text))
            {
                continue;
            }

            texts.Add(block.Text);
            var natural = ClippedTextProbe.NaturalWidth(block);
            if (natural > block.ActualWidth + 1)
            {
                found.Add($"{gridName} / {cell.Column.Header}: '{block.Text}' needs {natural:0} px but has {block.ActualWidth:0} in a {cell.ActualWidth:0} px cell");
            }
        }

        foreach (var text in expected)
        {
            if (!texts.Contains(text))
            {
                found.Add($"{gridName}: no cell shows '{text}'; the rows did not render ({texts.Count} cells seen)");
            }
        }

        return found;
    }

    /// <summary>Runs <paramref name="body"/> with the Viewer's dark theme in the application scope, where a control's StaticResource lookups resolve at load.</summary>
    private static T WithTheme<T>(Func<T> body)
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

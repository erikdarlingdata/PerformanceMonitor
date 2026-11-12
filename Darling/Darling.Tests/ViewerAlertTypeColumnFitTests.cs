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
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Markup;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8392: the Alert History grid's Alert Type cell shows every alert type in the catalogue whole, or ellipsized with its full name as the
/// tooltip, in a window 1920 wide and at the 900x600 minimum window. The real tab is built on the shared STA thread with the Viewer's
/// theme, bound to rows carrying the longest names the catalogue holds, and each Alert Type cell's text is measured against its cell.
/// Lite's twin is AlertTypeColumnFitTests. The host that holds the tab is sized explicitly inside a window that fits a 1024x768 screen.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerAlertTypeColumnFitTests
{
    /// <summary>The two longest alert types in the catalogue (37 characters each) and the one the cut was seen on.</summary>
    internal static readonly string[] LongestAlertTypes =
    {
        "Store Checkpointer Pressure Recovered",
        "PostgreSQL Replication Slot Retention",
        "Store Checkpointer Pressure",
    };

    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "Darling", "PerformanceMonitor.Darling.Viewer"));

    private static ResourceDictionary Theme() =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(ViewerDir(), "Themes", "DarkTheme.xaml")));

    [Theory]
    [InlineData(1920, 1080)]
    [InlineData(900, 600)]
    public void TheAlertTypeCell_ShowsTheLongestAlertTypesWhole_OrEllipsizedWithTheFullNameAsTooltip(double width, double height)
    {
        var found = StaTestThread.Run(() =>
        {
            var tab = new AlertsHistoryTab();
            var host = new Border { Child = tab, Width = width, Height = Math.Min(height, 700), HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            var window = new Window { Content = host, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            window.Resources.MergedDictionaries.Add(Theme());
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var grid = (DataGrid)tab.FindName("AlertsDataGrid");
                grid.ItemsSource = LongestAlertTypes.Select((name, i) => new ViewerAlertRow
                {
                    AlertTime = new DateTime(2026, 11, 8, 12, 0, 0, DateTimeKind.Utc).AddMinutes(-i),
                    ServerId = 1,
                    ServerName = "srv-a",
                    MetricName = name,
                    CurrentValue = 12,
                    ThresholdValue = 10,
                    AlertSent = true,
                    NotificationType = "email",
                    Muted = false,
                }).ToList();
                ClippedTextProbe.Settle(window);
                return Problems(grid, LongestAlertTypes, $"{width}x{height}");
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(found.Count == 0, ClippedTextProbe.Describe(found));
    }

    /// <summary>The Alert Type column: the one bound to <c>MetricName</c>.</summary>
    private static bool IsAlertTypeColumn(DataGridColumn column) =>
        column is DataGridBoundColumn bound && bound.Binding is Binding binding && binding.Path?.Path == "MetricName";

    private static string? TooltipText(object? tooltip) => tooltip as string ?? (tooltip as ToolTip)?.Content as string;

    /// <summary>Everything wrong with the Alert Type cells of the rendered grid: a name that is cut with no ellipsis, an ellipsized name
    /// with no tooltip of the full text, a name from <paramref name="expected"/> that no row shows, or a grid that realized no row at all.</summary>
    private static List<string> Problems(DataGrid grid, IEnumerable<string> expected, string at)
    {
        var found = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var rows = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>().ToList();
        if (rows.Count == 0)
        {
            found.Add($"[{at}] the grid realized no row");
            return found;
        }

        foreach (var row in rows)
        {
            foreach (var cell in ClippedTextProbe.Descendants(row).OfType<DataGridCell>().Where(c => IsAlertTypeColumn(c.Column)))
            {
                var block = ClippedTextProbe.Descendants(cell).OfType<TextBlock>().FirstOrDefault(b => b.IsVisible && b.Text.Length > 0);
                if (block is null)
                {
                    found.Add($"[{at}] an Alert Type cell shows no text");
                    continue;
                }

                seen.Add(block.Text);
                var right = block.TransformToAncestor(cell).TransformBounds(new Rect(block.RenderSize)).Right;
                var natural = ClippedTextProbe.NaturalWidth(block);
                var inside = right <= cell.ActualWidth + 1;
                var whole = inside && natural <= block.ActualWidth + 1;
                var tooltip = TooltipText(block.ToolTip ?? cell.ToolTip);
                var ellipsized = inside && block.TextTrimming != TextTrimming.None && string.Equals(tooltip, block.Text, StringComparison.Ordinal);
                if (!whole && !ellipsized)
                {
                    found.Add($"[{at}] '{block.Text}' needs {natural:F0} px, ends at {right:F0} in a {cell.ActualWidth:F0} px cell"
                        + (block.TextTrimming == TextTrimming.None ? ", with no ellipsis" : "")
                        + (tooltip is null ? " and no tooltip" : $", tooltip '{tooltip}'"));
                }
            }
        }

        foreach (var name in expected.Where(n => !seen.Contains(n)))
        {
            found.Add($"[{at}] no Alert Type cell shows '{name}'");
        }

        return found;
    }
}

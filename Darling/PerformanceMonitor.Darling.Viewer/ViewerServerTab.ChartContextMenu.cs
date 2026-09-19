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
using System.IO;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media.Imaging;
using Microsoft.Win32;
using ScottPlot.WPF;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The per-chart right-click Copy / Save / Export menu — a faithful <b>Darling-local</b> port of Lite's
/// <c>ContextMenuHelper.SetupChartContextMenu</c> (Lite/Helpers/ContextMenuHelper.cs:185). Lite's helper is a
/// Lite-only static class that reads Lite's <c>App.CsvSeparator</c>, so it is ported here rather than shared —
/// the same choice #1409 made for the drill-down menus, and consistent with the viewer being a Lite front-end
/// copy. The single behavioral difference from Lite is inherent to the viewer: it never re-executes a query,
/// so there is no live-plan surface here (the menu is pure chart chrome).
///
/// <para><b>Coexistence with the B1 drill-downs (#1409).</b> Each drill-down chart's menu is ONE
/// <see cref="ContextMenu"/> = [drill-down item] + [separator] + [these copy/save/export items], exactly like
/// Lite: <see cref="BuildChartContextMenu"/> builds the copy/export items and takes over right-click (via the
/// shared <c>AttachChartContextMenu</c> in ViewerServerTab.DrillDown.cs), then the drill-down wiring
/// <c>Insert</c>s its item at the top of the SAME menu. The only chart with no drill-down (collector duration,
/// which Lite also leaves drill-less) gets this menu on its own via <see cref="WireChartContextMenus"/>.</para>
/// </summary>
public partial class ViewerServerTab
{
    /* The menu-item headers — the single source of truth shared by the real WPF construction below and the
       pure ChartMenuHeaderOrder contract the tests pin. */
    internal const string CopyImageHeader = "_Copy Image";
    internal const string SaveImageHeader = "_Save Image As...";
    internal const string OpenInNewWindowHeader = "_Open in New Window";
    internal const string RevertHeader = "_Revert (or double-click)";
    internal const string ExportDataToCsvHeader = "_Export Data to CSV...";
    internal const string ShowDataSourceHeader = "Show _Data Source";

    /// <summary>Sentinel for a <see cref="Separator"/> in the pure <see cref="ChartMenuHeaderOrder"/> contract.</summary>
    internal const string MenuSeparatorMarker = "---";

    /// <summary>
    /// The canonical ordered header list of a per-chart menu (pure, unit-tested): the drill-down item first
    /// (when the chart has one) followed by a separator, then the copy/save/export block, and the optional
    /// Show Data Source tail. This is the contract <see cref="BuildChartContextMenu"/> +
    /// <c>AddChartDrillDownMenuItem</c> compose — the test pins that a drill-down chart's menu carries BOTH the
    /// drill-down item AND the copy/export items, drill-down first (so #1409's drill-downs still resolve).
    /// </summary>
    internal static IReadOnlyList<string> ChartMenuHeaderOrder(string? drillDownLabel, bool includeDataSource)
    {
        var items = new List<string>();
        if (drillDownLabel is not null)
        {
            items.Add(drillDownLabel);
            items.Add(MenuSeparatorMarker);
        }

        items.Add(CopyImageHeader);
        items.Add(SaveImageHeader);
        items.Add(OpenInNewWindowHeader);
        items.Add(MenuSeparatorMarker);
        items.Add(RevertHeader);
        items.Add(MenuSeparatorMarker);
        items.Add(ExportDataToCsvHeader);
        if (includeDataSource)
        {
            items.Add(MenuSeparatorMarker);
            items.Add(ShowDataSourceHeader);
        }

        return items;
    }

    /* The chart-data CSV shape: DateTime,Series,Value (Lite's ContextMenuHelper CSV export verbatim). Pure so
       the tests pin the header + row format without a WpfPlot. */
    private static readonly string[] s_chartCsvColumns = { "DateTime", "Series", "Value" };

    /// <summary>The chart-data CSV header line ("DateTime{sep}Series{sep}Value").</summary>
    internal static string ChartCsvHeaderLine(string separator) => string.Join(separator, s_chartCsvColumns);

    /// <summary>One chart-data CSV row: an invariant <c>yyyy-MM-dd HH:mm:ss</c> timestamp, the escaped series
    /// name, and the invariant value (Lite's exact row shape).</summary>
    internal static string FormatChartCsvLine(DateTime dateTime, string series, double value, string separator) =>
        string.Join(separator, new[]
        {
            dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            CsvEscape(series, separator),
            value.ToString(CultureInfo.InvariantCulture),
        });

    /// <summary>RFC-4180 CSV quoting (Lite's ContextMenuHelper.CsvEscape).</summary>
    internal static string CsvEscape(string value, string separator)
    {
        if (value.Contains(separator, StringComparison.Ordinal) || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            return "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
        }

        return value;
    }

    /// <summary>
    /// Builds the copy/save/export <see cref="ContextMenu"/> for a chart and takes right-click over from
    /// ScottPlot (Lite's <c>SetupChartContextMenu</c>). Returns the menu so a drill-down caller can
    /// <c>Insert</c> its item at the top of the SAME instance; charts without a drill-down get the returned
    /// menu as-is. Also wires double-click → revert/autoscale (Lite parity).
    /// </summary>
    private ContextMenu BuildChartContextMenu(WpfPlot chart, string chartName, string? dataSource = null)
    {
        var menu = new ContextMenu();

        var copyItem = new MenuItem { Header = CopyImageHeader, Icon = new TextBlock { Text = "\U0001f4cb" } };
        copyItem.Click += (_, _) => CopyChartImage(chart);
        menu.Items.Add(copyItem);

        var saveItem = new MenuItem { Header = SaveImageHeader, Icon = new TextBlock { Text = "\U0001f4be" } };
        saveItem.Click += (_, _) => SaveChartImage(chart, chartName);
        menu.Items.Add(saveItem);

        var openWindowItem = new MenuItem { Header = OpenInNewWindowHeader, Icon = new TextBlock { Text = "\U0001f5d7" } };
        openWindowItem.Click += (_, _) => OpenChartInNewWindow(chart, chartName);
        menu.Items.Add(openWindowItem);

        menu.Items.Add(new Separator());

        var autoscaleItem = new MenuItem { Header = RevertHeader, Icon = new TextBlock { Text = "↩" } };
        autoscaleItem.Click += (_, _) => RevertChartAxes(chart);
        menu.Items.Add(autoscaleItem);

        menu.Items.Add(new Separator());

        var exportCsvItem = new MenuItem { Header = ExportDataToCsvHeader, Icon = new TextBlock { Text = "\U0001f4ca" } };
        exportCsvItem.Click += (_, _) => ExportChartDataToCsv(chart, chartName);
        menu.Items.Add(exportCsvItem);

        /* Show Data Source only when a source string is supplied — matches Lite (no ServerTab chart wires one,
           so like Lite the item is absent by default; the capability is ported for parity). */
        if (!string.IsNullOrEmpty(dataSource))
        {
            menu.Items.Add(new Separator());
            var dataSourceItem = new MenuItem { Header = ShowDataSourceHeader, Icon = new TextBlock { Text = "ℹ" } };
            dataSourceItem.Click += (_, _) => MessageBox.Show(
                $"Data Source:\n\n{dataSource}", "Chart Data Source", MessageBoxButton.OK, MessageBoxImage.Information);
            menu.Items.Add(dataSourceItem);
        }

        /* Own right-click (removes ScottPlot's context/pan responses + PreviewMouseRightButtonDown) — the
           shared path with the drill-down charts and the query heatmap (ViewerServerTab.DrillDown.cs). */
        AttachChartContextMenu(chart, menu);

        /* Revert on double-click too (Lite parity): drop ScottPlot's own double-click autoscale so ours (which
           also clears an active click-isolate) runs instead. */
        chart.UserInputProcessor.UserActionResponses.RemoveAll(r =>
            r.GetType().Name.Contains("DoubleClick", StringComparison.Ordinal));
        chart.PreviewMouseDoubleClick += (_, e) =>
        {
            e.Handled = true;
            RevertChartAxes(chart);
        };

        return menu;
    }

    /// <summary>
    /// Wires the copy/save/export menu onto every chart that has NO drill-down (Lite gives every chart the
    /// menu; the drill-down charts get theirs in <c>WireChartDrillDowns</c>, the heatmap in
    /// <c>WireHeatmapDrillDownMenu</c>). Called from the constructor after the drill-downs.
    /// </summary>
    private void WireChartContextMenus()
    {
        /* Only the collector-duration chart is menu-only here — Lite leaves its collector-duration chart
           drill-less too. The other Darling-only charts (latch/spinlock, CPU scheduler, plan cache, session
           counts, the eight System Events charts) now carry a "Show Active Queries at This Time" drill-down,
           so they get their copy/export menu from WireChartDrillDowns instead (one menu per chart — no
           double-menuing). */
        BuildChartContextMenu(CollectorDurationChart, "Collector_Duration");
    }

    private static void CopyChartImage(WpfPlot chart)
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"chart_copy_{Guid.NewGuid()}.png");
        try
        {
            chart.Plot.SavePng(tempFile, (int)chart.ActualWidth, (int)chart.ActualHeight);
            var bitmap = new BitmapImage();
            bitmap.BeginInit();
            bitmap.CacheOption = BitmapCacheOption.OnLoad;
            bitmap.UriSource = new Uri(tempFile);
            bitmap.EndInit();
            bitmap.Freeze();
            Clipboard.SetDataObject(new DataObject(DataFormats.Bitmap, bitmap), false);
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    private static void SaveChartImage(WpfPlot chart, string chartName)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture);
        var saveDialog = new SaveFileDialog
        {
            Filter = "PNG Image|*.png|JPEG Image|*.jpg|BMP Image|*.bmp",
            FileName = $"{chartName}_{timestamp}.png",
            DefaultExt = ".png",
        };

        if (saveDialog.ShowDialog() == true)
        {
            chart.Plot.SavePng(saveDialog.FileName, (int)chart.ActualWidth, (int)chart.ActualHeight);
        }
    }

    private static void OpenChartInNewWindow(WpfPlot chart, string chartName)
    {
        var newWindow = new Window
        {
            Title = chartName.Replace("_", " ", StringComparison.Ordinal),
            Width = 800,
            Height = 600,
        };

        var tempFile = Path.Combine(Path.GetTempPath(), $"chart_temp_{Guid.NewGuid()}.png");
        try
        {
            chart.Plot.SavePng(tempFile, 800, 600);
            var image = new Image();
            var bitmap = new BitmapImage();
            bitmap.BeginInit();
            bitmap.CacheOption = BitmapCacheOption.OnLoad;
            bitmap.UriSource = new Uri(tempFile);
            bitmap.EndInit();
            bitmap.Freeze();
            image.Source = bitmap;
            newWindow.Content = image;
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }

        newWindow.Show();
    }

    private void RevertChartAxes(WpfPlot chart)
    {
        /* Clear an active click-isolate first so it doesn't leave series dimmed / state stale (Lite parity). */
        if (ChartHoverHelper.TryGetForChart(chart, out var h)) h.Restore();

        /* Revert re-pins X to the toolbar's settable window (+ auto-fit Y) instead of AutoScale()'ing to the
           data range — a bare AutoScale fits X to the data plus ScottPlot's ~10% side margins, which re-creates
           the symmetric dead-space the window-pin campaign removed, on every Revert / double-click. The Query
           Heatmap is the one exception: its X axis is categorical (bucket columns), so it keeps AutoScale. */
        if (ReferenceEquals(chart, QueryHeatmapChart))
        {
            chart.Plot.Axes.AutoScale();
        }
        else
        {
            var (startUtc, endUtc) = GetWindowUtc();
            chart.Plot.Axes.SetLimitsX(
                ViewerTimeHelper.ForDisplay(startUtc).ToOADate(),
                ViewerTimeHelper.ForDisplay(endUtc).ToOADate());
            chart.Plot.Axes.AutoScaleY();
        }

        chart.Refresh();
    }

    private static void ExportChartDataToCsv(WpfPlot chart, string chartName)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture);
        var saveDialog = new SaveFileDialog
        {
            Filter = "CSV Files|*.csv|All Files|*.*",
            FileName = $"{chartName}_data_{timestamp}.csv",
            DefaultExt = ".csv",
        };

        if (saveDialog.ShowDialog() != true) return;

        try
        {
            var sep = ViewerExportSettings.CsvSeparator;
            var sb = new StringBuilder();
            sb.AppendLine(ChartCsvHeaderLine(sep));

            var seriesIndex = 1;
            foreach (var plottable in chart.Plot.GetPlottables())
            {
                if (plottable is ScottPlot.Plottables.Scatter scatter)
                {
                    var seriesName = scatter.LegendText ?? $"Series{seriesIndex}";
                    foreach (var point in scatter.Data.GetScatterPoints())
                    {
                        /* #1944's gap markers are fabricated mid-gap timestamps with NaN values -
                           rendering artifacts, never collected data. Since #3653 A7 the perfmon chart also
                           plots an UNKNOWABLE point (a stored interval of 0 - a restart's fabricated delta) as
                           NaN at its real timestamp; it is a collection with no value, and exporting it as 0
                           would be the lie the chart stopped telling. Exports carry only real rows. */
                        if (double.IsNaN(point.Y))
                        {
                            continue;
                        }

                        sb.AppendLine(FormatChartCsvLine(DateTime.FromOADate(point.X), seriesName, point.Y, sep));
                    }

                    seriesIndex++;
                }
            }

            File.WriteAllText(saveDialog.FileName, sb.ToString(), Encoding.UTF8);
            MessageBox.Show($"Data exported to:\n{saveDialog.FileName}", "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Error exporting data:\n\n{ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }
}

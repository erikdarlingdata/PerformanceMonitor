/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using ScottPlot.WPF;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Shared context menu helpers for DataGrid copy/export and chart save/export operations.
/// Used by standalone windows (history, collection log, manage servers, settings)
/// and all ScottPlot chart controls.
/// </summary>
public static class ContextMenuHelper
{
    public static string GetCellValue(DataGridColumn col, object item)
    {
        if (col is DataGridBoundColumn boundCol
            && boundCol.Binding is Binding binding)
        {
            var prop = item.GetType().GetProperty(binding.Path.Path);
            return FormatForExport(prop?.GetValue(item));
        }

        if (col is DataGridTemplateColumn templateCol && templateCol.CellTemplate != null)
        {
            var content = templateCol.CellTemplate.LoadContent();
            if (content is TextBlock textBlock)
            {
                var textBinding = BindingOperations.GetBinding(textBlock, TextBlock.TextProperty);
                if (textBinding != null)
                {
                    var prop = item.GetType().GetProperty(textBinding.Path.Path);
                    return FormatForExport(prop?.GetValue(item));
                }
            }
        }

        return "";
    }

    public static void CopyCell(object sender)
    {
        var grid = FindParentDataGrid(sender);
        if (grid?.CurrentCell.Column == null || grid.CurrentItem == null) return;

        var value = GetCellValue(grid.CurrentCell.Column, grid.CurrentItem);
        if (value.Length > 0) ClipboardText.TrySetDataObject(value);
    }

    public static void CopyRow(object sender)
    {
        var grid = FindParentDataGrid(sender);
        if (grid?.CurrentItem == null) return;

        var sb = new StringBuilder();
        foreach (var col in grid.Columns)
        {
            sb.Append(GetCellValue(col, grid.CurrentItem));
            sb.Append('\t');
        }
        ClipboardText.TrySetDataObject(sb.ToString().TrimEnd('\t'));
    }

    public static void CopyAllRows(object sender)
    {
        var grid = FindParentDataGrid(sender);
        if (grid?.Items == null) return;

        var sb = new StringBuilder();

        foreach (var col in grid.Columns)
        {
            sb.Append(DataGridClipboardBehavior.GetHeaderText(col));
            sb.Append('\t');
        }
        sb.AppendLine();

        foreach (var item in grid.Items)
        {
            foreach (var col in grid.Columns)
            {
                sb.Append(GetCellValue(col, item));
                sb.Append('\t');
            }
            sb.AppendLine();
        }

        ClipboardText.TrySetDataObject(sb.ToString());
    }

    public static void ExportToCsv(object sender, string defaultFilePrefix)
    {
        var grid = FindParentDataGrid(sender);
        if (grid?.Items == null || grid.Items.Count == 0) return;

        var dialog = new SaveFileDialog
        {
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
            DefaultExt = ".csv",
            FileName = $"{defaultFilePrefix}_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };

        if (dialog.ShowDialog() != true) return;

        var sb = new StringBuilder();
        var sep = App.CsvSeparator;

        var headers = new List<string>();
        foreach (var col in grid.Columns)
        {
            headers.Add(CsvEscape(DataGridClipboardBehavior.GetHeaderText(col), sep));
        }
        sb.AppendLine(string.Join(sep, headers));

        foreach (var item in grid.Items)
        {
            var values = new List<string>();
            foreach (var col in grid.Columns)
            {
                values.Add(CsvEscape(GetCellValue(col, item), sep));
            }
            sb.AppendLine(string.Join(sep, values));
        }

        try
        {
            File.WriteAllText(dialog.FileName, sb.ToString(), Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to export: {ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private static string FormatForExport(object? value)
    {
        if (value == null) return "";
        if (value is IFormattable formattable)
            return formattable.ToString(null, CultureInfo.InvariantCulture);
        return value.ToString() ?? "";
    }

    private static string CsvEscape(string value, string separator)
    {
        if (value.Contains(separator, StringComparison.Ordinal) || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /// <summary>The time text of a chart CSV export line, the format the export has always used.</summary>
    private const string ChartCsvTimeFormat = "yyyy-MM-dd HH:mm:ss";

    /// <summary>
    /// The header line of a chart's CSV export (#4766). The time column names the zone its times are written in
    /// ("DateTime (UTC)", "DateTime (Eastern Standard Time)"), so a file opened later still says which clock they are
    /// on. Pure, so a test calls it without the WPF handler.
    /// </summary>
    internal static string ChartCsvHeader(string separator, TimeZoneInfo displayZone)
    {
        var time = $"DateTime ({displayZone.Id})";
        return string.Join(separator, new[] { CsvEscape(time, separator), "Series", "Value" });
    }

    /// <summary>
    /// One data line of a chart's CSV export (#4766). A ServerTab chart plots the naive-UTC instant as X, so the
    /// time column is that instant read in the zone (<see cref="DisplayZone.ToDisplay"/>): the same text the chart's
    /// axis shows for it, and a point in the repeated hour of a fall-back day is written as the wall time it reads
    /// there. Pure, so a test calls it without the WPF handler.
    /// </summary>
    internal static string ChartCsvLine(double x, string seriesName, double y, string separator, TimeZoneInfo displayZone)
    {
        var plotted = DateTime.FromOADate(x);
        var shown = DisplayZone.ToDisplay(plotted, displayZone);
        return string.Join(separator, new[]
        {
            shown.ToString(ChartCsvTimeFormat, CultureInfo.InvariantCulture),
            CsvEscape(seriesName, separator),
            y.ToString(CultureInfo.InvariantCulture)
        });
    }

    /// <summary>
    /// Sets up a context menu for a ScottPlot chart with standard options:
    /// Copy Image, Save Image As, Open in New Window, Revert, Export Data to CSV.
    /// <paramref name="revertAction"/> lets a windowed caller (ServerTab) re-pin the X axis to its current
    /// settable time window on Revert / double-click instead of AutoScale()'ing to the data range (which
    /// re-introduces ScottPlot's ~10% side dead-space); windowless callers omit it and fall back to AutoScale.
    /// <paramref name="displayZone"/> is the zone the chart's own axis labels are drawn in (#4766): the CSV export
    /// writes each point's time in it and names it in the header (<see cref="ChartCsvLine"/>,
    /// <see cref="ChartCsvHeader"/>). It is a function so the export reads the zone as it is when the user clicks,
    /// after any display-mode switch. It is required: every chart this menu is set up for plots the naive-UTC
    /// instant as X, so none is left to export it unconverted.
    /// </summary>
    public static ContextMenu SetupChartContextMenu(WpfPlot chart, string chartName, Func<TimeZoneInfo> displayZone, string? dataSource = null, Action<WpfPlot>? revertAction = null)
    {
        var contextMenu = new ContextMenu();

        // Copy Image
        var copyItem = new MenuItem { Header = "_Copy Image", Icon = new TextBlock { Text = "\U0001f4cb" } };
        copyItem.Click += (s, e) =>
        {
            var tempFile = Path.Combine(Path.GetTempPath(), $"chart_copy_{Guid.NewGuid()}.png");
            try
            {
                chart.Plot.SavePng(tempFile, (int)chart.ActualWidth, (int)chart.ActualHeight);
                var bitmap = new System.Windows.Media.Imaging.BitmapImage();
                bitmap.BeginInit();
                bitmap.CacheOption = System.Windows.Media.Imaging.BitmapCacheOption.OnLoad;
                bitmap.UriSource = new Uri(tempFile);
                bitmap.EndInit();
                bitmap.Freeze();
                ClipboardText.TrySetDataObject(new DataObject(DataFormats.Bitmap, bitmap));
            }
            finally
            {
                if (File.Exists(tempFile)) File.Delete(tempFile);
            }
        };
        contextMenu.Items.Add(copyItem);

        // Save Image As
        var saveItem = new MenuItem { Header = "_Save Image As...", Icon = new TextBlock { Text = "\U0001f4be" } };
        saveItem.Click += (s, e) =>
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture);
            var defaultFileName = $"{chartName}_{timestamp}.png";
            var saveDialog = new SaveFileDialog
            {
                Filter = "PNG Image|*.png|JPEG Image|*.jpg|BMP Image|*.bmp",
                FileName = defaultFileName,
                DefaultExt = ".png"
            };
            if (saveDialog.ShowDialog() == true)
            {
                chart.Plot.SavePng(saveDialog.FileName, (int)chart.ActualWidth, (int)chart.ActualHeight);
            }
        };
        contextMenu.Items.Add(saveItem);

        // Open in New Window
        var openWindowItem = new MenuItem { Header = "_Open in New Window", Icon = new TextBlock { Text = "\U0001f5d7" } };
        openWindowItem.Click += (s, e) =>
        {
            var newWindow = new Window
            {
                Title = chartName.Replace("_", " ", StringComparison.Ordinal),
                Width = 800,
                Height = 600
            };
            var tempFile = Path.Combine(Path.GetTempPath(), $"chart_temp_{Guid.NewGuid()}.png");
            try
            {
                chart.Plot.SavePng(tempFile, 800, 600);
                var image = new System.Windows.Controls.Image();
                var bitmap = new System.Windows.Media.Imaging.BitmapImage();
                bitmap.BeginInit();
                bitmap.CacheOption = System.Windows.Media.Imaging.BitmapCacheOption.OnLoad;
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
        };
        contextMenu.Items.Add(openWindowItem);

        contextMenu.Items.Add(new Separator());

        // Revert (re-pin to the settable window when a revertAction is supplied; AutoScale otherwise)
        var autoscaleItem = new MenuItem { Header = "_Revert (or double-click)", Icon = new TextBlock { Text = "\u21a9" } };
        autoscaleItem.Click += (s, e) => RevertChart(chart, revertAction);
        contextMenu.Items.Add(autoscaleItem);

        contextMenu.Items.Add(new Separator());

        // Export Data to CSV
        var exportCsvItem = new MenuItem { Header = "_Export Data to CSV...", Icon = new TextBlock { Text = "\U0001f4ca" } };
        exportCsvItem.Click += (s, e) =>
        {
            var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture);
            var defaultFileName = $"{chartName}_data_{timestamp}.csv";
            var saveDialog = new SaveFileDialog
            {
                Filter = "CSV Files|*.csv|All Files|*.*",
                FileName = defaultFileName,
                DefaultExt = ".csv"
            };
            if (saveDialog.ShowDialog() == true)
            {
                try
                {
                    var sb = new StringBuilder();
                    var sep = App.CsvSeparator;
                    var zone = displayZone();
                    sb.AppendLine(ChartCsvHeader(sep, zone));

                    var plottables = chart.Plot.GetPlottables();
                    int seriesIndex = 1;
                    foreach (var plottable in plottables)
                    {
                        if (plottable is ScottPlot.Plottables.Scatter scatter)
                        {
                            var seriesName = scatter.LegendText ?? $"Series{seriesIndex}";
                            var points = scatter.Data.GetScatterPoints();

                            foreach (var point in points)
                            {
                                /* #1944's gap markers are fabricated mid-gap timestamps with NaN values -
                                   rendering artifacts, never collected data. Since #3653 A7 the perfmon chart
                                   also plots an UNKNOWABLE point (a stored interval of 0 - a restart's fabricated
                                   delta) as NaN at its real timestamp; it is a collection with no value, and
                                   exporting it as 0 would be the lie the chart stopped telling. Exports carry
                                   only real rows. */
                                if (double.IsNaN(point.Y))
                                {
                                    continue;
                                }

                                sb.AppendLine(ChartCsvLine(point.X, seriesName, point.Y, sep, zone));
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
        };
        contextMenu.Items.Add(exportCsvItem);

        // Show Data Source (if provided)
        if (!string.IsNullOrEmpty(dataSource))
        {
            contextMenu.Items.Add(new Separator());

            var dataSourceItem = new MenuItem { Header = "Show _Data Source", Icon = new TextBlock { Text = "\u2139" } };
            dataSourceItem.Click += (s, e) =>
            {
                MessageBox.Show(
                    $"Data Source:\n\n{dataSource}",
                    "Chart Data Source",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
            };
            contextMenu.Items.Add(dataSourceItem);
        }

        // Disable ScottPlot's default right-click context menu handling
        chart.UserInputProcessor.UserActionResponses.RemoveAll(r =>
            r.GetType().Name.Contains("Context", StringComparison.Ordinal) ||
            r.GetType().Name.Contains("RightClick", StringComparison.Ordinal) ||
            r.GetType().Name.Contains("Menu", StringComparison.Ordinal));

        // Use PreviewMouseRightButtonDown to show context menu before ScottPlot handles it
        chart.PreviewMouseRightButtonDown += (s, e) =>
        {
            e.Handled = true;
            contextMenu.PlacementTarget = chart;
            contextMenu.Placement = PlacementMode.MousePoint;
            contextMenu.IsOpen = true;
        };

        // Disable ScottPlot's default double-click behaviors
        chart.UserInputProcessor.UserActionResponses.RemoveAll(r =>
            r.GetType().Name.Contains("DoubleClick", StringComparison.Ordinal));

        // Use PreviewMouseDoubleClick for revert/autoscale
        chart.PreviewMouseDoubleClick += (s, e) =>
        {
            e.Handled = true;
            RevertChart(chart, revertAction);
        };

        return contextMenu;
    }

    /// <summary>
    /// Shared Revert body for the menu item + double-click. A ServerTab caller supplies
    /// <paramref name="revertAction"/> to re-pin X to the current settable window (+ auto-fit Y); it also
    /// clears any active click-isolate. Windowless callers (standalone windows) have no window to pin to, so
    /// this clears the isolate and falls back to AutoScale.
    /// </summary>
    private static void RevertChart(WpfPlot chart, Action<WpfPlot>? revertAction)
    {
        if (revertAction != null)
        {
            revertAction(chart);
            return;
        }

        // Clear an active click-isolate first so it doesn't leave series dimmed / state stale.
        if (ChartHoverHelper.TryGetForChart(chart, out var h)) h.Restore();
        chart.Plot.Axes.AutoScale();
        chart.Refresh();
    }
}

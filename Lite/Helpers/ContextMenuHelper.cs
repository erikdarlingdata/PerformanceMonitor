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

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Shared context menu helpers for DataGrid copy/export and chart save/export operations.
/// Used by standalone windows (history, collection log, manage servers, settings)
/// and all ScottPlot chart controls.
/// </summary>
public static class ContextMenuHelper
{
    public static DataGrid? FindParentDataGrid(object sender)
    {
        if (sender is not MenuItem menuItem) return null;
        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        while (target != null && target is not DataGrid)
        {
            target = VisualTreeHelper.GetParent(target) as FrameworkElement;
        }
        return target as DataGrid;
    }

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
        if (value.Length > 0) Clipboard.SetDataObject(value, false);
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
        Clipboard.SetDataObject(sb.ToString().TrimEnd('\t'), false);
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

        Clipboard.SetDataObject(sb.ToString(), false);
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

    /// <summary>
    /// Sets up a context menu for a ScottPlot chart with standard options:
    /// Copy Image, Save Image As, Open in New Window, Revert, Export Data to CSV.
    /// </summary>
    public static void SetupChartContextMenu(WpfPlot chart, string chartName, string? dataSource = null)
    {
        var contextMenu = new ContextMenu();

        // Copy Image
        var copyItem = new MenuItem { Header = "Copy Image", Icon = new TextBlock { Text = "\U0001f4cb" } };
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
                Clipboard.SetDataObject(new DataObject(DataFormats.Bitmap, bitmap), false);
            }
            finally
            {
                if (File.Exists(tempFile)) File.Delete(tempFile);
            }
        };
        contextMenu.Items.Add(copyItem);

        // Save Image As
        var saveItem = new MenuItem { Header = "Save Image As...", Icon = new TextBlock { Text = "\U0001f4be" } };
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
        var openWindowItem = new MenuItem { Header = "Open in New Window", Icon = new TextBlock { Text = "\U0001f5d7" } };
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

        // Revert (Autoscale)
        var autoscaleItem = new MenuItem { Header = "Revert (or double-click)", Icon = new TextBlock { Text = "\u21a9" } };
        autoscaleItem.Click += (s, e) =>
        {
            chart.Plot.Axes.AutoScale();
            chart.Refresh();
        };
        contextMenu.Items.Add(autoscaleItem);

        contextMenu.Items.Add(new Separator());

        // Export Data to CSV
        var exportCsvItem = new MenuItem { Header = "Export Data to CSV...", Icon = new TextBlock { Text = "\U0001f4ca" } };
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
                    sb.AppendLine(string.Join(sep, new[] { "DateTime", "Series", "Value" }));

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
                                var dateTime = DateTime.FromOADate(point.X);
                                sb.AppendLine(string.Join(sep, new[]
                                {
                                    dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                                    CsvEscape(seriesName, sep),
                                    point.Y.ToString(CultureInfo.InvariantCulture)
                                }));
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

            var dataSourceItem = new MenuItem { Header = "Show Data Source", Icon = new TextBlock { Text = "\u2139" } };
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
            chart.Plot.Axes.AutoScale();
            chart.Refresh();
        };
    }
}

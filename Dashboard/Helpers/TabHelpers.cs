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
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using ScottPlot.WPF;

namespace PerformanceMonitorDashboard.Helpers
{
    /// <summary>
    /// Shared utility methods for tab controls, charts, and data export.
    /// Extracted from ServerTab to enable reuse across UserControls.
    /// </summary>
    public static class TabHelpers
    {
        /// <summary>
        /// Returns true if a double-click originated from a DataGridRow (not a header).
        /// Use at the top of MouseDoubleClick handlers to prevent header clicks from
        /// triggering row actions.
        /// </summary>
        public static bool IsDoubleClickOnRow(DependencyObject originalSource)
        {
            var dep = originalSource;
            while (dep != null)
            {
                if (dep is DataGridRow) return true;
                if (dep is DataGridColumnHeader) return false;
                dep = VisualTreeHelper.GetParent(dep);
            }
            return false;
        }

        /// <summary>
        /// Material Design 300-level color palette for chart data series.
        /// Soft pastels optimized for dark backgrounds, ordered to map 1:1
        /// with common ScottPlot stock colors (Blue→[0], Green→[1], etc.).
        /// </summary>
        public static readonly ScottPlot.Color[] ChartColors = new[]
        {
            ScottPlot.Color.FromHex("#4FC3F7"), // [0]  Light Blue 300
            ScottPlot.Color.FromHex("#81C784"), // [1]  Green 300
            ScottPlot.Color.FromHex("#FFB74D"), // [2]  Orange 300
            ScottPlot.Color.FromHex("#E57373"), // [3]  Red 300
            ScottPlot.Color.FromHex("#BA68C8"), // [4]  Purple 300
            ScottPlot.Color.FromHex("#4DD0E1"), // [5]  Cyan 300
            ScottPlot.Color.FromHex("#FFF176"), // [6]  Yellow 300
            ScottPlot.Color.FromHex("#F06292"), // [7]  Pink 300
            ScottPlot.Color.FromHex("#AED581"), // [8]  Light Green 300
            ScottPlot.Color.FromHex("#90A4AE"), // [9]  Blue Grey 300
            ScottPlot.Color.FromHex("#A1887F"), // [10] Brown 300
            ScottPlot.Color.FromHex("#7986CB"), // [11] Indigo 300
            ScottPlot.Color.FromHex("#FF7043"), // [12] Deep Orange 300
            ScottPlot.Color.FromHex("#80DEEA"), // [13] Cyan 200
            ScottPlot.Color.FromHex("#FFE082"), // [14] Amber 200
            ScottPlot.Color.FromHex("#CE93D8"), // [15] Purple 200
            ScottPlot.Color.FromHex("#EF9A9A"), // [16] Red 200
            ScottPlot.Color.FromHex("#C5E1A5"), // [17] Light Green 200
            ScottPlot.Color.FromHex("#FFCC80"), // [18] Orange 200
            ScottPlot.Color.FromHex("#B0BEC5"), // [19] Blue Grey 200
        };

        /// <summary>
        /// Poison waits — always selected by default. These indicate critical resource exhaustion.
        /// </summary>
        public static readonly string[] PoisonWaits = new[]
        {
            "THREADPOOL",
            "RESOURCE_SEMAPHORE",
            "RESOURCE_SEMAPHORE_QUERY_COMPILE"
        };

        /// <summary>
        /// Usual suspect waits — always selected by default. Common performance-relevant wait types.
        /// </summary>
        public static readonly string[] UsualSuspectWaits = new[]
        {
            "SOS_SCHEDULER_YIELD",
            "CXPACKET",
            "CXCONSUMER",
            "PAGEIOLATCH_SH",
            "PAGEIOLATCH_EX",
            "WRITELOG"
        };

        /// <summary>
        /// Prefix patterns for usual suspect waits (e.g. PAGELATCH_EX, PAGELATCH_SH, etc.)
        /// </summary>
        public static readonly string[] UsualSuspectPrefixes = new[] { "PAGELATCH_" };

        /// <summary>
        /// Returns the set of wait types that should be selected by default:
        /// poison waits + usual suspects + top 10 by total wait time (deduped), capped at 30.
        /// The availableWaitTypes list must be sorted by total wait time descending.
        /// </summary>
        public static HashSet<string> GetDefaultWaitTypes(IList<string> availableWaitTypes)
        {
            var defaults = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // 1. Poison waits that exist in data
            foreach (var w in PoisonWaits)
                if (availableWaitTypes.Contains(w)) defaults.Add(w);

            // 2. Usual suspects — exact matches
            foreach (var w in UsualSuspectWaits)
                if (availableWaitTypes.Contains(w)) defaults.Add(w);

            // 3. Usual suspects — prefix matches
            foreach (var prefix in UsualSuspectPrefixes)
                foreach (var w in availableWaitTypes)
                    if (w.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        defaults.Add(w);

            // 4. Top 10 by total wait time (items not already in the set), hard cap at 30 total
            int added = 0;
            foreach (var w in availableWaitTypes)
            {
                if (defaults.Count >= 30) break;
                if (added >= 10) break;
                if (defaults.Add(w))
                {
                    added++;
                }
            }

            return defaults;
        }

        /// <summary>
        /// Applies the Darling Data dark theme to a ScottPlot chart.
        /// </summary>
        public static void ApplyDarkModeToChart(WpfPlot chart)
        {
            // Grafana-inspired dark theme colors
            var darkBackground = ScottPlot.Color.FromHex("#22252b");
            var darkerBackground = ScottPlot.Color.FromHex("#111217");
            var textColor = ScottPlot.Color.FromHex("#9DA5B4");
            var gridColor = ScottPlot.Colors.White.WithAlpha(40);

            chart.Plot.FigureBackground.Color = darkBackground;
            chart.Plot.DataBackground.Color = darkerBackground;
            chart.Plot.Axes.Color(textColor);
            chart.Plot.Grid.MajorLineColor = gridColor;
            chart.Plot.Legend.BackgroundColor = darkBackground;
            chart.Plot.Legend.FontColor = ScottPlot.Color.FromHex("#E4E6EB");
            chart.Plot.Legend.OutlineColor = ScottPlot.Color.FromHex("#2a2d35");
            chart.Plot.Legend.Alignment = ScottPlot.Alignment.LowerCenter;
            chart.Plot.Legend.Orientation = ScottPlot.Orientation.Horizontal;
            chart.Plot.Axes.Margins(bottom: 0); // No bottom margin - SetChartYLimitsWithLegendPadding handles Y-axis

            // Explicitly set axis tick label colors (needed after DateTimeTicksBottom() is called)
            chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
            chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
            chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
            chart.Plot.Axes.Left.Label.ForeColor = textColor;
        }

        /// <summary>
        /// Reapplies dark mode text colors to chart axes.
        /// Call this AFTER DateTimeTicksBottom() or other axis modifications.
        /// </summary>
        public static void ReapplyAxisColors(WpfPlot chart)
        {
            var textColor = ScottPlot.Color.FromHex("#9DA5B4");
            chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
            chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
            chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
            chart.Plot.Axes.Left.Label.ForeColor = textColor;
        }

        /// <summary>
        /// Locks the vertical axis of a chart so mouse wheel zooming only affects the time (X) axis.
        /// Also reapplies dark mode axis colors after DateTimeTicksBottom() modifications.
        /// </summary>
        public static void LockChartVerticalAxis(WpfPlot chart)
        {
            var limits = chart.Plot.Axes.GetLimits();
            var rule = new ScottPlot.AxisRules.LockedVertical(
                chart.Plot.Axes.Left,
                limits.Bottom,
                limits.Top);
            chart.Plot.Axes.Rules.Clear();
            chart.Plot.Axes.Rules.Add(rule);

            // Reapply axis colors after DateTimeTicksBottom() may have reset them
            ReapplyAxisColors(chart);
        }

        /// <summary>
        /// Sets Y-axis limits with appropriate padding for charts with horizontal legends at bottom.
        /// Call this BEFORE LockChartVerticalAxis.
        /// </summary>
        public static void SetChartYLimitsWithLegendPadding(WpfPlot chart, double dataYMin = 0, double dataYMax = 0)
        {
            // If no explicit values provided, use auto-calculated limits
            if (dataYMin == 0 && dataYMax == 0)
            {
                var limits = chart.Plot.Axes.GetLimits();
                dataYMin = limits.Bottom;
                dataYMax = limits.Top;
            }

            // Handle edge cases
            if (dataYMax <= dataYMin)
            {
                dataYMax = dataYMin + 100;
            }

            // Calculate padding: 5% above for breathing room
            double range = dataYMax - dataYMin;
            double topPadding = range * 0.05;

            /* Only add bottom padding if dataYMin is above zero - don't go negative */
            double yMin = dataYMin >= 0 ? 0 : dataYMin - (range * 0.10);
            double yMax = dataYMax + topPadding;

            chart.Plot.Axes.SetLimitsY(yMin, yMax);
        }

        /// <summary>
        /// Applies dark theme styling to a WPF Calendar control.
        /// </summary>
        public static void ApplyDarkThemeToCalendar(System.Windows.Controls.Calendar calendar)
        {
            var darkBg = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#111217"));
            var lightBg = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#22252b"));
            var whiteFg = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB"));
            var mutedFg = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#6B7280"));
            var accentBg = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#2eaef1"));

            calendar.Background = darkBg;
            calendar.Foreground = whiteFg;
            calendar.BorderBrush = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#2a2d35"));

            // Apply to all child controls recursively
            ApplyDarkThemeRecursively(calendar, darkBg, lightBg, whiteFg, mutedFg);
        }

        private static void ApplyDarkThemeRecursively(DependencyObject parent, Brush darkBg, Brush lightBg, Brush whiteFg, Brush mutedFg)
        {
            for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
            {
                var child = VisualTreeHelper.GetChild(parent, i);

                if (child is System.Windows.Controls.Primitives.CalendarItem calendarItem)
                {
                    calendarItem.Background = darkBg;
                    calendarItem.Foreground = whiteFg;
                }
                else if (child is System.Windows.Controls.Primitives.CalendarDayButton dayButton)
                {
                    dayButton.Background = Brushes.Transparent;
                    dayButton.Foreground = whiteFg;
                }
                else if (child is System.Windows.Controls.Primitives.CalendarButton calButton)
                {
                    calButton.Background = Brushes.Transparent;
                    calButton.Foreground = whiteFg;
                }
                else if (child is Button button)
                {
                    button.Background = Brushes.Transparent;
                    button.Foreground = whiteFg;
                }
                else if (child is TextBlock textBlock)
                {
                    textBlock.Foreground = whiteFg;
                }
                else if (child is Border border)
                {
                    // Apply dark background to any border with a light background
                    if (border.Background is SolidColorBrush bg)
                    {
                        // Check if it's a light color (R, G, B all > 200)
                        if (bg.Color.R > 200 && bg.Color.G > 200 && bg.Color.B > 200)
                            border.Background = darkBg;
                    }
                }
                else if (child is Grid grid)
                {
                    // Apply dark background to any grid with a light background
                    if (grid.Background is SolidColorBrush gridBg)
                    {
                        if (gridBg.Color.R > 200 && gridBg.Color.G > 200 && gridBg.Color.B > 200)
                            grid.Background = darkBg;
                    }
                }

                ApplyDarkThemeRecursively(child, darkBg, lightBg, whiteFg, mutedFg);
            }
        }

        /// <summary>
        /// Escapes a field value for proper CSV formatting.
        /// </summary>
        public static string EscapeCsvField(string field)
        {
            if (string.IsNullOrEmpty(field))
                return string.Empty;

            if (field.Contains(',', StringComparison.Ordinal) || field.Contains('"', StringComparison.Ordinal) || field.Contains('\n', StringComparison.Ordinal))
            {
                return "\"" + field.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
            }
            return field;
        }

        /// <summary>
        /// Finds a parent element of the specified type in the visual tree.
        /// </summary>
        public static T? FindParent<T>(DependencyObject child) where T : DependencyObject
        {
            var parent = VisualTreeHelper.GetParent(child);
            if (parent == null) return null;
            if (parent is T typedParent) return typedParent;
            return FindParent<T>(parent);
        }

        /// <summary>
        /// Gets the header text from a DataGridColumn.
        /// </summary>
        public static string GetColumnHeader(DataGridColumn column)
        {
            if (column.Header is string headerString)
                return headerString;
            if (column.Header is TextBlock textBlock)
                return textBlock.Text;
            if (column.Header is StackPanel stackPanel)
            {
                var headerTextBlock = stackPanel.Children.OfType<TextBlock>().FirstOrDefault();
                if (headerTextBlock != null)
                    return headerTextBlock.Text;
            }
            return string.Empty;
        }

        /// <summary>
        /// Sets MinWidth on all DataGrid columns based on their header text width.
        /// This ensures column headers are always visible even when columns are resized.
        /// </summary>
        /// <param name="dataGrid">The DataGrid to process</param>
        /// <param name="extraPadding">Additional padding beyond the text width (default 20 for margins/sort indicator)</param>
        /// <param name="filterBoxHeight">Height to account for filter TextBox if present (default 24)</param>
        public static void AutoSizeColumnMinWidths(DataGrid dataGrid, double extraPadding = 20, double filterBoxHeight = 24)
        {
            if (dataGrid == null) return;

            // Get the font info from the DataGrid or use defaults
            var fontFamily = dataGrid.FontFamily ?? new FontFamily("Segoe UI");
            var fontSize = dataGrid.FontSize > 0 ? dataGrid.FontSize : 12;
            var typeface = new Typeface(fontFamily, FontStyles.Normal, FontWeights.Normal, FontStretches.Normal);

            foreach (var column in dataGrid.Columns)
            {
                var headerText = GetColumnHeader(column);
                if (string.IsNullOrEmpty(headerText)) continue;

                // Measure the header text width
                var formattedText = new FormattedText(
                    headerText,
                    CultureInfo.CurrentCulture,
                    FlowDirection.LeftToRight,
                    typeface,
                    fontSize,
                    Brushes.Black,
                    VisualTreeHelper.GetDpi(dataGrid).PixelsPerDip);

                // Calculate MinWidth: text width + padding
                var minWidth = formattedText.Width + extraPadding;

                // Only set if larger than current MinWidth
                if (minWidth > column.MinWidth)
                {
                    column.MinWidth = minWidth;
                }
            }
        }

        /// <summary>
        /// Freezes the first N columns of a DataGrid so they stay visible when scrolling horizontally.
        /// </summary>
        /// <param name="dataGrid">The DataGrid to process</param>
        /// <param name="columnCount">Number of columns to freeze (default 2)</param>
        public static void FreezeColumns(DataGrid dataGrid, int columnCount = 2)
        {
            if (dataGrid == null) return;
            dataGrid.FrozenColumnCount = Math.Min(columnCount, dataGrid.Columns.Count);
        }

        /// <summary>
        /// Gets the text content of a cell from a DataGrid.
        /// Handles both DataGridBoundColumn and DataGridTemplateColumn.
        /// </summary>
        public static string GetCellContent(DataGrid dataGrid, DataGridCellInfo cellInfo)
        {
            /* DataGridBoundColumn — binding is directly accessible */
            if (cellInfo.Column is DataGridBoundColumn boundColumn && boundColumn.Binding is Binding binding && binding.Path != null)
            {
                var propertyName = binding.Path.Path;
                var property = cellInfo.Item.GetType().GetProperty(propertyName);
                if (property != null)
                {
                    var value = property.GetValue(cellInfo.Item);
                    return value?.ToString() ?? string.Empty;
                }
            }

            /* DataGridTemplateColumn — instantiate the template and find a TextBlock binding */
            if (cellInfo.Column is DataGridTemplateColumn templateCol && templateCol.CellTemplate != null)
            {
                var content = templateCol.CellTemplate.LoadContent();
                if (content is TextBlock textBlock)
                {
                    var textBinding = BindingOperations.GetBinding(textBlock, TextBlock.TextProperty);
                    if (textBinding != null)
                    {
                        var prop = cellInfo.Item.GetType().GetProperty(textBinding.Path.Path);
                        return prop?.GetValue(cellInfo.Item)?.ToString() ?? string.Empty;
                    }
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Gets all values from a row as a tab-delimited string.
        /// </summary>
        public static string GetRowAsText(DataGrid dataGrid, object item)
        {
            var values = GetRowValues(dataGrid, item);
            return string.Join("\t", values);
        }

        /// <summary>
        /// Gets all values from a row as a list of strings.
        /// Handles both DataGridBoundColumn and DataGridTemplateColumn.
        /// </summary>
        public static List<string> GetRowValues(DataGrid dataGrid, object item)
        {
            var values = new List<string>();
            foreach (var column in dataGrid.Columns)
            {
                /* DataGridBoundColumn — binding is directly accessible */
                if (column is DataGridBoundColumn boundColumn && boundColumn.Binding is Binding binding)
                {
                    var propertyName = binding.Path.Path;
                    var property = item.GetType().GetProperty(propertyName);
                    if (property != null)
                    {
                        var value = property.GetValue(item);
                        values.Add(value?.ToString() ?? string.Empty);
                    }
                }
                /* DataGridTemplateColumn — instantiate the template and find a TextBlock binding */
                else if (column is DataGridTemplateColumn templateCol && templateCol.CellTemplate != null)
                {
                    var content = templateCol.CellTemplate.LoadContent();
                    if (content is TextBlock textBlock)
                    {
                        var textBinding = BindingOperations.GetBinding(textBlock, TextBlock.TextProperty);
                        if (textBinding != null)
                        {
                            var prop = item.GetType().GetProperty(textBinding.Path.Path);
                            values.Add(prop?.GetValue(item)?.ToString() ?? string.Empty);
                        }
                        else
                        {
                            values.Add(string.Empty);
                        }
                    }
                    else
                    {
                        values.Add(string.Empty);
                    }
                }
            }
            return values;
        }

        /// <summary>
        /// Finds a DataGrid from a ContextMenu's placement target.
        /// </summary>
        public static DataGrid? FindDataGridFromContextMenu(ContextMenu contextMenu)
        {
            if (contextMenu.PlacementTarget is DataGridRow row)
            {
                return FindParent<DataGrid>(row);
            }
            return null;
        }

        /// <summary>
        /// Sets up a context menu for a ScottPlot chart with standard options:
        /// Copy Image, Save Image As, Open in New Window, Revert, Export Data to CSV, Show Data Source.
        /// </summary>
        /// <param name="chart">The WpfPlot chart control</param>
        /// <param name="chartName">A descriptive name for the chart (used in filenames)</param>
        /// <param name="dataSource">Optional SQL view/table name that populates this chart</param>
        public static void SetupChartContextMenu(WpfPlot chart, string chartName, string? dataSource = null)
        {
            var contextMenu = new ContextMenu();

            // Copy Image
            var copyItem = new MenuItem { Header = "Copy Image", Icon = new TextBlock { Text = "📋" } };
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
                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(new System.Windows.DataObject(System.Windows.DataFormats.Bitmap, bitmap), false);
                }
                finally
                {
                    if (File.Exists(tempFile)) File.Delete(tempFile);
                }
            };
            contextMenu.Items.Add(copyItem);

            // Save Image As
            var saveItem = new MenuItem { Header = "Save Image As...", Icon = new TextBlock { Text = "💾" } };
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
            var openWindowItem = new MenuItem { Header = "Open in New Window", Icon = new TextBlock { Text = "🗗" } };
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
            var autoscaleItem = new MenuItem { Header = "Revert (or double-click)", Icon = new TextBlock { Text = "↩" } };
            autoscaleItem.Click += (s, e) =>
            {
                chart.Plot.Axes.AutoScale();
                chart.Refresh();
            };
            contextMenu.Items.Add(autoscaleItem);

            contextMenu.Items.Add(new Separator());

            // Export Data to CSV
            var exportCsvItem = new MenuItem { Header = "Export Data to CSV...", Icon = new TextBlock { Text = "📊" } };
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
                        sb.AppendLine("DateTime,Series,Value");

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
                                    sb.AppendLine(CultureInfo.InvariantCulture, $"{dateTime:yyyy-MM-dd HH:mm:ss},{EscapeCsvField(seriesName)},{point.Y}");
                                }
                                seriesIndex++;
                            }
                        }

                        File.WriteAllText(saveDialog.FileName, sb.ToString());
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

                var dataSourceItem = new MenuItem { Header = "Show Data Source", Icon = new TextBlock { Text = "ℹ" } };
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

        /// <summary>
        /// Aggregates time series data by timestamp, summing duplicate values.
        /// Returns only actual data points - no gap filling, no boundary extension.
        /// Lines will connect directly between data points.
        /// </summary>
        public static (double[] xs, double[] ys) FillTimeSeriesGaps(
            IEnumerable<DateTime> timePoints,
            IEnumerable<double> values)
        {
            // Group by time and sum values (handles multiple databases at same timestamp)
            // Only return points where this series has actual data
            var aggregated = timePoints.Zip(values, (t, v) => new { Time = t, Value = v })
                                       .GroupBy(x => x.Time)
                                       .OrderBy(g => g.Key)
                                       .Select(g => new { Time = g.Key, Value = g.Sum(x => x.Value) })
                                       .ToList();

            var xs = aggregated.Select(p => p.Time.ToOADate()).ToArray();
            var ys = aggregated.Select(p => p.Value).ToArray();

            return (xs, ys);
        }
    }
}

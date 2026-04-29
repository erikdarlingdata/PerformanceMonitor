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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;


namespace PerformanceMonitorDashboard.Controls
{
    public partial class SystemEventsContent : UserControl
    {
        #region Context Menu Handlers

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid && grid.CurrentCell.Column != null)
                {
                    var cellContent = TabHelpers.GetCellContent(grid, grid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(cellContent, false);
                    }
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid && grid.SelectedItem != null)
                {
                    var rowText = TabHelpers.GetRowAsText(grid, grid.SelectedItem);
                    if (!string.IsNullOrEmpty(rowText))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(rowText, false);
                    }
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var sb = new StringBuilder();

                    // Header row
                    var headers = grid.Columns.Select(c => Helpers.DataGridClipboardBehavior.GetHeaderText(c));
                    sb.AppendLine(string.Join("\t", headers));

                    // Data rows
                    foreach (var item in grid.Items)
                    {
                        var values = new List<string>();
                        foreach (var column in grid.Columns)
                        {
                            var binding = (column as DataGridBoundColumn)?.Binding as System.Windows.Data.Binding;
                            if (binding != null)
                            {
                                var prop = item.GetType().GetProperty(binding.Path.Path);
                                var value = prop?.GetValue(item)?.ToString() ?? string.Empty;
                                values.Add(value);
                            }
                        }
                        sb.AppendLine(string.Join("\t", values));
                    }

                    if (sb.Length > 0)
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(sb.ToString(), false);
                    }
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var dialog = new SaveFileDialog
                    {
                        Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                        DefaultExt = ".csv",
                        FileName = $"SystemEvents_Export_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
                    };

                    if (dialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Header row
                            var sep = TabHelpers.CsvSeparator;
                            var headers = grid.Columns.Select(c => TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(c), sep));
                            sb.AppendLine(string.Join(sep, headers));

                            // Data rows
                            foreach (var item in grid.Items)
                            {
                                var values = new List<string>();
                                foreach (var column in grid.Columns)
                                {
                                    var binding = (column as DataGridBoundColumn)?.Binding as System.Windows.Data.Binding;
                                    if (binding != null)
                                    {
                                        var prop = item.GetType().GetProperty(binding.Path.Path);
                                        values.Add(TabHelpers.EscapeCsvField(TabHelpers.FormatForExport(prop?.GetValue(item)), sep));
                                    }
                                }
                                sb.AppendLine(string.Join(sep, values));
                            }

                            File.WriteAllText(dialog.FileName, sb.ToString());
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Error exporting to CSV: {ex.Message}", ex);
                            MessageBox.Show($"Error exporting to CSV: {ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }

        #endregion
    }
}

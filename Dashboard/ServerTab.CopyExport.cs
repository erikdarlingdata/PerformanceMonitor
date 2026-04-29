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
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        // ====================================================================
        // Context Menu Event Handlers
        // ====================================================================

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (cellContent != null)
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
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.SelectedItem != null)
                {
                    var rowText = GetRowAsText(dataGrid, dataGrid.SelectedItem);
                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(rowText, false);
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new StringBuilder();

                    // Add headers
                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                    {
                        if (column is DataGridBoundColumn boundColumn)
                        {
                            headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                        }
                    }
                    sb.AppendLine(string.Join("	", headers));

                    // Add all rows
                    foreach (var item in dataGrid.Items)
                    {
                        sb.AppendLine(GetRowAsText(dataGrid, item));
                    }

                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"export_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Add headers
                            var headers = new List<string>();
                            foreach (var column in dataGrid.Columns)
                            {
                                if (column is DataGridBoundColumn)
                                {
                                    headers.Add(EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column)));
                                }
                            }
                            sb.AppendLine(string.Join(",", headers));

                            // Add all rows
                            foreach (var item in dataGrid.Items)
                            {
                                var values = GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(",", values.Select(v => EscapeCsvField(v))));
                            }

                            File.WriteAllText(saveFileDialog.FileName, sb.ToString());
                            MessageBox.Show($"Data exported successfully to:\n{saveFileDialog.FileName}", "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                        }
                        catch (Exception ex)
                        {
                            MessageBox.Show($"Error exporting data:\n\n{ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }

        private DataGrid? FindDataGridFromContextMenu(ContextMenu contextMenu)
        {
            if (contextMenu.PlacementTarget is DataGridRow row)
            {
                return FindParent<DataGrid>(row);
            }
            return null;
        }

        private T? FindParent<T>(DependencyObject child) where T : DependencyObject
        {
            return TabHelpers.FindParent<T>(child);
        }

        private string GetCellContent(DataGrid dataGrid, DataGridCellInfo cellInfo)
        {
            var column = cellInfo.Column as DataGridBoundColumn;
            if (column?.Binding is Binding binding && binding.Path != null)
            {
                var propertyName = binding.Path.Path;
                var property = cellInfo.Item.GetType().GetProperty(propertyName);
                if (property != null)
                {
                    var value = property.GetValue(cellInfo.Item);
                    return value?.ToString() ?? string.Empty;
                }
            }
            return string.Empty;
        }

        private string GetRowAsText(DataGrid dataGrid, object item)
        {
            var values = GetRowValues(dataGrid, item);
            return string.Join("	", values);
        }

        private List<string> GetRowValues(DataGrid dataGrid, object item)
        {
            var values = new List<string>();
            foreach (var column in dataGrid.Columns)
            {
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
            }
            return values;
        }

        private string GetColumnHeader(DataGridColumn column)
        {
            return TabHelpers.GetColumnHeader(column);
        }

        private string EscapeCsvField(string field)
        {
            return TabHelpers.EscapeCsvField(field);
        }
    }
}

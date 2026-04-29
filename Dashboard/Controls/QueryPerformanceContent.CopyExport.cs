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
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class QueryPerformanceContent : UserControl
    {
        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = TabHelpers.GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                    {
                        Clipboard.SetDataObject(cellContent, false);
                    }
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.SelectedItem != null)
                {
                    var rowText = TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem);
                    Clipboard.SetDataObject(rowText, false);
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new StringBuilder();

                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                    {
                        if (column is DataGridBoundColumn)
                        {
                            headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                        }
                    }
                    sb.AppendLine(string.Join("\t", headers));

                    foreach (var item in dataGrid.Items)
                    {
                        sb.AppendLine(TabHelpers.GetRowAsText(dataGrid, item));
                    }

                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void CopyReproScript_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not MenuItem menuItem || menuItem.Parent is not ContextMenu contextMenu) return;

            var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
            if (dataGrid?.SelectedItem == null) return;

            var item = dataGrid.SelectedItem;
            string? queryText = null;
            string? databaseName = null;
            string? planXml = null;
            string source = "Query";

            /* Extract data based on item type */
            switch (item)
            {
                case QuerySnapshotItem qs:
                    queryText = qs.QueryText;
                    databaseName = qs.DatabaseName;
                    planXml = qs.QueryPlan;
                    source = "Active Queries";
                    break;
                case QueryStatsItem qst:
                    queryText = qst.QueryText;
                    databaseName = qst.DatabaseName;
                    planXml = qst.QueryPlanXml;
                    source = "Query Stats";
                    break;
                case QueryStoreItem qsi:
                    queryText = qsi.QueryText;
                    databaseName = qsi.DatabaseName;
                    planXml = qsi.QueryPlanXml;
                    source = "Query Store";
                    break;
                case ProcedureStatsItem ps:
                    queryText = ps.ObjectName;
                    databaseName = ps.DatabaseName;
                    planXml = null; /* Procedures don't have plan XML in the model */
                    source = "Procedure Stats";
                    break;
                default:
                    MessageBox.Show("Copy Repro Script is not available for this data type.", "Not Available", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
            }

            if (string.IsNullOrWhiteSpace(queryText))
            {
                MessageBox.Show("No query text available for this row.", "No Query Text", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var script = ReproScriptBuilder.BuildReproScript(queryText, databaseName, planXml, isolationLevel: null, source);

            try
            {
                Clipboard.SetDataObject(script, false);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to copy to clipboard: {ex.Message}", "Clipboard Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"query_performance_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            var headers = new List<string>();
                            foreach (var column in dataGrid.Columns)
                            {
                                if (column is DataGridBoundColumn)
                                {
                                    headers.Add(TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column), TabHelpers.CsvSeparator));
                                }
                            }
                            sb.AppendLine(string.Join(TabHelpers.CsvSeparator, headers));

                            foreach (var item in dataGrid.Items)
                            {
                                var values = TabHelpers.GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(TabHelpers.CsvSeparator, values.Select(v => TabHelpers.EscapeCsvField(v, TabHelpers.CsvSeparator))));
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
    }
}

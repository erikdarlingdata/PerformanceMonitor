/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /* DataGrid copy helpers */
    /// <summary>
    /// Finds the parent DataGrid from a context menu opened on a DataGridRow.
    /// </summary>
    private static DataGrid? FindParentDataGrid(MenuItem menuItem)
    {
        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        while (target != null && target is not DataGrid)
        {
            target = System.Windows.Media.VisualTreeHelper.GetParent(target) as FrameworkElement;
        }
        return target as DataGrid;
    }

    /// <summary>
    /// Gets a cell value from a row item for any column type (bound or template).
    /// Template columns are inspected for a TextBlock binding in their CellTemplate.
    /// </summary>
    private static string GetCellValue(DataGridColumn col, object item)
    {
        /* DataGridBoundColumn — binding is directly accessible */
        if (col is DataGridBoundColumn boundCol
            && boundCol.Binding is System.Windows.Data.Binding binding)
        {
            var prop = item.GetType().GetProperty(binding.Path.Path);
            return FormatForExport(prop?.GetValue(item));
        }

        /* DataGridTemplateColumn — instantiate the template and find a TextBlock binding */
        if (col is DataGridTemplateColumn templateCol && templateCol.CellTemplate != null)
        {
            var content = templateCol.CellTemplate.LoadContent();
            if (content is TextBlock textBlock)
            {
                var textBinding = System.Windows.Data.BindingOperations.GetBinding(textBlock, TextBlock.TextProperty);
                if (textBinding != null)
                {
                    var prop = item.GetType().GetProperty(textBinding.Path.Path);
                    return FormatForExport(prop?.GetValue(item));
                }
            }
        }

        return "";
    }

    private static string FormatForExport(object? value)
    {
        if (value == null) return "";
        if (value is IFormattable formattable)
            return formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture);
        return value.ToString() ?? "";
    }

    private void CopyCell_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentCell.Column == null || grid.CurrentItem == null) return;

        var value = GetCellValue(grid.CurrentCell.Column, grid.CurrentItem);
        if (value.Length > 0) Clipboard.SetDataObject(value, false);
    }

    private void CopyRow_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        var sb = new StringBuilder();
        foreach (var col in grid.Columns)
        {
            sb.Append(GetCellValue(col, grid.CurrentItem));
            sb.Append('\t');
        }
        Clipboard.SetDataObject(sb.ToString().TrimEnd('\t'), false);
    }

    private void CopyAllRows_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null) return;

        var sb = new StringBuilder();

        /* Header */
        foreach (var col in grid.Columns)
        {
            sb.Append(Helpers.DataGridClipboardBehavior.GetHeaderText(col));
            sb.Append('\t');
        }
        sb.AppendLine();

        /* Rows */
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

    private async void CopyReproScript_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        string? queryText = null;
        string? databaseName = null;
        string? planXml = null;
        string? isolationLevel = null;
        string source = "Query";

        switch (grid.CurrentItem)
        {
            case QuerySnapshotRow snapshot:
                queryText = snapshot.QueryText;
                databaseName = snapshot.DatabaseName;
                planXml = snapshot.QueryPlan;
                isolationLevel = snapshot.TransactionIsolationLevel;
                source = "Active Queries";
                break;

            case QueryStatsRow stats:
                queryText = stats.QueryText;
                databaseName = stats.DatabaseName;
                source = "Top Queries (dm_exec_query_stats)";
                /* Fetch plan on-demand from SQL Server */
                if (!string.IsNullOrEmpty(stats.QueryHash))
                {
                    try
                    {
                        var connStr = _server.GetConnectionString(_credentialService);
                        planXml = await LocalDataService.FetchQueryPlanOnDemandAsync(connStr, stats.QueryHash);
                    }
                    catch { /* Plan fetch failed — continue without plan */ }
                }
                break;

            case QueryStoreRow qs:
                queryText = qs.QueryText;
                databaseName = qs.DatabaseName;
                source = "Query Store";
                /* Fetch plan on-demand from Query Store */
                if (qs.PlanId > 0 && !string.IsNullOrEmpty(qs.DatabaseName))
                {
                    try
                    {
                        var connStr = _server.GetConnectionString(_credentialService);
                        planXml = await LocalDataService.FetchQueryStorePlanAsync(connStr, qs.DatabaseName, qs.PlanId);
                    }
                    catch { /* Plan fetch failed — continue without plan */ }
                }
                break;

            default:
                /* Not a supported grid for repro scripts — copy query text if available */
                var textProp = grid.CurrentItem.GetType().GetProperty("QueryText");
                queryText = textProp?.GetValue(grid.CurrentItem)?.ToString();
                if (string.IsNullOrEmpty(queryText))
                {
                    return;
                }
                var dbProp = grid.CurrentItem.GetType().GetProperty("DatabaseName");
                databaseName = dbProp?.GetValue(grid.CurrentItem)?.ToString();
                break;
        }

        if (string.IsNullOrEmpty(queryText))
        {
            return;
        }

        var script = ReproScriptBuilder.BuildReproScript(queryText, databaseName, planXml, isolationLevel, source);

        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() operation.
           See: https://github.com/dotnet/wpf/issues/9901 */
        Clipboard.SetDataObject(script, false);
    }

    private void ExportToCsv_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.Items == null || grid.Items.Count == 0) return;

        var dialog = new SaveFileDialog
        {
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
            DefaultExt = ".csv",
            FileName = $"{_server.DisplayName}_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
        };

        if (dialog.ShowDialog() != true) return;

        var sb = new StringBuilder();
        var sep = App.CsvSeparator;

        /* Header */
        var headers = new List<string>();
        foreach (var col in grid.Columns)
        {
            headers.Add(CsvEscape(DataGridClipboardBehavior.GetHeaderText(col), sep));
        }
        sb.AppendLine(string.Join(sep, headers));

        /* Rows */
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

    private static string CsvEscape(string value, string separator)
    {
        if (value.Contains(separator, StringComparison.Ordinal) || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
        {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}

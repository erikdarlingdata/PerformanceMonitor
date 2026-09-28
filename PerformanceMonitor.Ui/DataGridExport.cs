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
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Shared copy/export operations for DataGrid context menus, used by both Dashboard and Lite.
/// The four context-menu handlers (Copy Cell / Copy Row / Copy All Rows / Export to CSV) are
/// identical across ~29 sites apart from the CSV file-name prefix and the user's separator
/// preference, so they live here once.
///
/// Headers and rows both iterate <see cref="DataGrid.Columns"/> in lock-step (bound AND template
/// columns), so the two never misalign -- a bug the older Dashboard handlers had, where headers
/// were filtered to bound columns but rows emitted every column.
///
/// Each operation is split into a pure builder (returns the string -- unit-testable headlessly)
/// and a thin wrapper that performs the clipboard / SaveFileDialog / file side effect.
/// </summary>
public static class DataGridExport
{
    /* ---- side-effecting wrappers (wired to the context-menu MenuItem handlers) ---- */

    /// <summary>Copies the current cell's value to the clipboard.</summary>
    public static void CopyCell(object sender)
    {
        var grid = FindDataGrid(sender);
        if (grid == null) return;

        var cell = grid.CurrentCell;
        if (cell.Column == null || cell.Item == null) return;

        var value = GetCellValue(cell.Column, cell.Item);
        if (value.Length > 0) ClipboardText.TrySetText(value);
    }

    /// <summary>
    /// Copies a row (all columns, tab-delimited) to the clipboard -- the row the context menu was
    /// opened on, falling back to the current or selected row. Row-attached context menus don't
    /// select the row on right-click, so CurrentItem alone is often null here.
    /// </summary>
    public static void CopyRow(object sender)
    {
        var grid = FindDataGrid(sender);
        if (grid == null) return;

        var item = FindRowItem(sender) ?? grid.CurrentItem ?? grid.SelectedItem;
        if (item == null) return;

        ClipboardText.TrySetText(string.Join("\t", GetRowValues(grid, item)));
    }

    /// <summary>Copies the whole grid (header + all rows, tab-delimited) to the clipboard.</summary>
    public static void CopyAllRows(object sender)
    {
        var grid = FindDataGrid(sender);
        if (grid == null || grid.Items.Count == 0) return;

        ClipboardText.TrySetText(BuildClipboardText(grid));
    }

    /// <summary>
    /// Prompts for a file and exports the whole grid to CSV. <paramref name="fileNamePrefix"/>
    /// seeds the suggested file name; <paramref name="separator"/> is the user's CSV separator
    /// preference (each app passes its own).
    /// </summary>
    public static void ExportToCsv(object sender, string fileNamePrefix, string separator)
    {
        var grid = FindDataGrid(sender);
        if (grid == null || grid.Items.Count == 0) return;

        var dialog = new SaveFileDialog
        {
            FileName = $"{fileNamePrefix}_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
            DefaultExt = ".csv",
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, BuildCsv(grid, separator), Encoding.UTF8);
            MessageBox.Show($"Data exported successfully to:\n{dialog.FileName}", "Export Complete",
                MessageBoxButton.OK, MessageBoxImage.Information);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Error exporting data:\n\n{ex.Message}", "Export Error",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    /* ---- pure builders (unit-testable) ---- */

    /// <summary>Builds the tab-delimited header+rows text used by Copy All Rows.</summary>
    public static string BuildClipboardText(DataGrid grid)
    {
        var sb = new StringBuilder();
        sb.AppendLine(string.Join("\t", grid.Columns.Select(DataGridClipboardBehavior.GetHeaderText)));
        foreach (var item in grid.Items)
        {
            sb.AppendLine(string.Join("\t", GetRowValues(grid, item)));
        }
        return sb.ToString();
    }

    /// <summary>Builds the CSV text (header + rows, escaped) for the given separator.</summary>
    public static string BuildCsv(DataGrid grid, string separator)
    {
        var sb = new StringBuilder();
        sb.AppendLine(string.Join(separator,
            grid.Columns.Select(c => CsvEscape(DataGridClipboardBehavior.GetHeaderText(c), separator))));
        foreach (var item in grid.Items)
        {
            sb.AppendLine(string.Join(separator,
                GetRowValues(grid, item).Select(v => CsvEscape(v, separator))));
        }
        return sb.ToString();
    }

    /// <summary>Gets one string per column for a row, in column order (empty for columns with no extractable value).</summary>
    public static List<string> GetRowValues(DataGrid grid, object item)
    {
        var values = new List<string>(grid.Columns.Count);
        foreach (var column in grid.Columns)
        {
            values.Add(GetCellValue(column, item));
        }
        return values;
    }

    /// <summary>
    /// Extracts a cell value for any column type. Bound columns read their binding path directly;
    /// template columns are instantiated and inspected for a TextBlock binding. Unhandled column
    /// types yield an empty string so the value count always matches the column count.
    /// </summary>
    public static string GetCellValue(DataGridColumn column, object item)
    {
        if (column is DataGridBoundColumn boundColumn
            && boundColumn.Binding is Binding binding
            && binding.Path != null)
        {
            var property = item.GetType().GetProperty(binding.Path.Path);
            return FormatForExport(property?.GetValue(item));
        }

        if (column is DataGridTemplateColumn templateColumn && templateColumn.CellTemplate != null)
        {
            var content = templateColumn.CellTemplate.LoadContent();
            if (content is TextBlock textBlock)
            {
                var textBinding = BindingOperations.GetBinding(textBlock, TextBlock.TextProperty);
                if (textBinding != null && textBinding.Path != null)
                {
                    var property = item.GetType().GetProperty(textBinding.Path.Path);
                    return FormatForExport(property?.GetValue(item));
                }
            }
        }

        return string.Empty;
    }

    /// <summary>Formats a value for export: invariant culture for IFormattable, else ToString.</summary>
    public static string FormatForExport(object? value)
    {
        if (value == null) return string.Empty;
        if (value is IFormattable formattable) return formattable.ToString(null, CultureInfo.InvariantCulture);
        return value.ToString() ?? string.Empty;
    }

    /// <summary>Escapes a value for CSV: quote when it contains the separator, a quote, or a newline.</summary>
    public static string CsvEscape(string value, string separator)
    {
        if (string.IsNullOrEmpty(value)) return string.Empty;
        if (value.Contains(separator, StringComparison.Ordinal)
            || value.Contains('"')
            || value.Contains('\n')
            || value.Contains('\r'))
        {
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    /* ---- internals ---- */

    /// <summary>Resolves the DataGrid that owns the context menu the handler fired from.</summary>
    private static DataGrid? FindDataGrid(object sender)
    {
        if (sender is not MenuItem menuItem || menuItem.Parent is not ContextMenu contextMenu)
            return null;

        if (contextMenu.PlacementTarget is DataGrid grid)
            return grid;

        DependencyObject? target = contextMenu.PlacementTarget;
        while (target != null && target is not DataGrid)
        {
            target = VisualTreeHelper.GetParent(target);
        }
        return target as DataGrid;
    }

    /// <summary>
    /// The data item of the DataGridRow the context menu was opened on (for row- or cell-attached
    /// menus), or null when the menu is attached directly to the DataGrid.
    /// </summary>
    private static object? FindRowItem(object sender)
    {
        if (sender is not MenuItem menuItem || menuItem.Parent is not ContextMenu contextMenu)
            return null;

        DependencyObject? target = contextMenu.PlacementTarget;
        while (target != null && target is not DataGridRow)
        {
            target = VisualTreeHelper.GetParent(target);
        }
        return (target as DataGridRow)?.Item;
    }
}

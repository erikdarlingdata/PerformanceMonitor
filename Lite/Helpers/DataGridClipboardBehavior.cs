/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Attached behavior that fixes DataGrid clipboard copy when column headers
/// use StackPanel (e.g. filter button + text). Without this, copied headers
/// show "System.Windows.Controls.StackPanel" instead of the column name.
/// </summary>
public static class DataGridClipboardBehavior
{
    public static readonly DependencyProperty FixHeaderCopyProperty =
        DependencyProperty.RegisterAttached(
            "FixHeaderCopy",
            typeof(bool),
            typeof(DataGridClipboardBehavior),
            new PropertyMetadata(false, OnFixHeaderCopyChanged));

    public static bool GetFixHeaderCopy(DependencyObject obj) => (bool)obj.GetValue(FixHeaderCopyProperty);
    public static void SetFixHeaderCopy(DependencyObject obj, bool value) => obj.SetValue(FixHeaderCopyProperty, value);

    private static void OnFixHeaderCopyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is DataGrid grid && (bool)e.NewValue)
        {
            grid.CopyingRowClipboardContent += FixHeaderCopy;
        }
    }

    /// <summary>
    /// Extracts the display text from a DataGrid column header.
    /// Handles StackPanel headers (filter button + TextBlock) by returning the TextBlock text.
    /// </summary>
    public static string GetHeaderText(DataGridColumn column)
    {
        if (column.Header is StackPanel sp)
        {
            var tb = sp.Children.OfType<TextBlock>().FirstOrDefault();
            if (tb != null) return tb.Text;
        }
        return column.Header?.ToString() ?? "";
    }

    /// <summary>
    /// Event handler that can be wired directly to DataGrid.CopyingRowClipboardContent.
    /// </summary>
    public static void FixHeaderCopy(object? sender, DataGridRowClipboardEventArgs e)
    {
        if (!e.IsColumnHeadersRow) return;

        for (int i = 0; i < e.ClipboardRowContent.Count; i++)
        {
            var cell = e.ClipboardRowContent[i];
            if (cell.Column?.Header is StackPanel sp)
            {
                var tb = sp.Children.OfType<TextBlock>().FirstOrDefault();
                if (tb != null)
                {
                    e.ClipboardRowContent[i] = new DataGridClipboardCellContent(cell.Item, cell.Column, tb.Text);
                }
            }
        }
    }
}

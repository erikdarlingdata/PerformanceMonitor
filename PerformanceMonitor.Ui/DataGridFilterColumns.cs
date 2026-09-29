/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Decorates a DataGrid's bound columns with per-column filter buttons at runtime, so a grid
/// authored with plain-string headers gains the same filter affordance as hand-built
/// StackPanel headers — without repeating the StackPanel+Button markup on every column.
/// Pair with <see cref="DataGridFilterManager{T}"/>: each button's Tag is the column's binding
/// path, which is the property name the manager filters on and looks up when styling the icons.
/// </summary>
public static class DataGridFilterColumns
{
    /// <summary>
    /// Wraps each bound column's plain-string header in a horizontal StackPanel containing a
    /// filter button (styled via the "ColumnFilterButtonStyle" resource, Tag = binding path) and
    /// the original header text. Template columns, unbound columns, and columns whose header is
    /// already a StackPanel are left untouched, so the call is idempotent.
    /// </summary>
    public static void AddFilterButtons(DataGrid grid, RoutedEventHandler onFilterClick)
    {
        foreach (var column in grid.Columns)
        {
            if (column is not DataGridBoundColumn bound) continue;
            if (bound.Binding is not Binding binding) continue;
            var path = binding.Path?.Path;
            if (string.IsNullOrEmpty(path)) continue;
            if (column.Header is not string headerText) continue; // skip already-decorated / non-text headers

            var filterButton = new Button
            {
                Tag = path,
                Margin = new Thickness(0, 0, 4, 0)
            };
            filterButton.SetResourceReference(FrameworkElement.StyleProperty, "ColumnFilterButtonStyle");
            filterButton.Click += onFilterClick;

            var header = new StackPanel { Orientation = Orientation.Horizontal };
            header.Children.Add(filterButton);
            header.Children.Add(new TextBlock
            {
                Text = headerText,
                FontWeight = FontWeights.Bold,
                VerticalAlignment = VerticalAlignment.Center
            });

            column.Header = header;
        }
    }
}

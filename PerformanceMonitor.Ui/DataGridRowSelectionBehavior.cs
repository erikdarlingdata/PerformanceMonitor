/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Makes right-clicking a DataGrid row select it. WPF's DataGrid does not select a row on
/// right-click by default, so context-menu actions that read <c>SelectedItem</c> (e.g. "View Plan")
/// silently do nothing whenever the prior left-click selection was cleared — most commonly by a
/// background auto-refresh re-binding <c>ItemsSource</c>, which also drops the row's highlight.
/// Selecting the row under the cursor on right-click makes the menu always act on the row the user
/// actually clicked. Call <see cref="Enable"/> once at application startup to apply this app-wide.
/// </summary>
public static class DataGridRowSelectionBehavior
{
    private static bool _enabled;

    /// <summary>
    /// Registers a class handler so every <see cref="DataGrid"/> in the app selects the row under
    /// the cursor on right-click. Idempotent.
    /// </summary>
    public static void Enable()
    {
        if (_enabled) return;
        _enabled = true;

        EventManager.RegisterClassHandler(
            typeof(DataGrid),
            UIElement.PreviewMouseRightButtonDownEvent,
            new MouseButtonEventHandler(OnPreviewRightButtonDown));
    }

    private static void OnPreviewRightButtonDown(object sender, MouseButtonEventArgs e)
    {
        // Walk up from the actual clicked element (a cell's content) to its DataGridRow.
        var dep = e.OriginalSource as DependencyObject;
        while (dep != null && dep is not DataGridRow)
            dep = VisualTreeHelper.GetParent(dep);

        if (dep is DataGridRow row && sender is DataGrid grid)
        {
            row.IsSelected = true;
            grid.SelectedItem = row.Item;
        }
    }
}

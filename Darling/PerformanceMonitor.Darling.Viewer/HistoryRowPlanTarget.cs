/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Which history snapshot a plan action means (#7175). The query-stats and procedure history grids list one row
/// per snapshot, and a query that recompiled has a different plan on its older rows. A plan action that ignores the
/// row reads the newest plan for the key and files it under the older row.
/// </summary>
internal static class HistoryRowPlanTarget
{
    /// <summary>
    /// The row a plan action was raised for. The per-row Download button sits in a cell whose DataContext is that
    /// row, and a click on it does not always move the grid's CurrentItem, so the button's own row wins. The
    /// context menu has no row of its own, so it uses the grid's CurrentItem, the row that was right-clicked (the
    /// same source "View Cached Plan" reads), then the selection. Null when no row can be named.
    /// </summary>
    internal static T? Resolve<T>(object? sender, DataGrid grid) where T : class
    {
        if (sender is Button { DataContext: T own })
        {
            return own;
        }

        return (grid.CurrentItem ?? grid.SelectedItem) as T;
    }
}

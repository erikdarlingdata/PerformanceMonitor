/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Whether a mouse event landed on a grid row (#7210). WPF raises <c>MouseDoubleClick</c> on the grid for a double-click
/// anywhere inside it: a column header (to auto-size or sort twice), the scrollbar track, the empty area under the rows.
/// A drill handler that reads <c>SelectedItem</c> would open the selected row's window for those. Same walk as
/// <c>AlertsHistoryTab.AlertsDataGrid_MouseDoubleClick</c>: up from the click target to a <see cref="DataGridRow"/>.
/// </summary>
internal static class DataGridRowHit
{
    /// <summary>The <see cref="DataGridRow"/> the click target sits in, or null for a header, scrollbar or empty-area click.</summary>
    public static DataGridRow? RowOf(object? originalSource)
    {
        var node = originalSource as DependencyObject;
        while (node is not null && node is not DataGridRow && node is not DataGridColumnHeader && node is not DataGrid)
        {
            // A text Run is not a Visual: step through the logical tree for it, the visual tree for everything else.
            node = node is Visual or System.Windows.Media.Media3D.Visual3D
                ? VisualTreeHelper.GetParent(node)
                : LogicalTreeHelper.GetParent(node);
        }

        return node as DataGridRow;
    }

    /// <summary>True when the click landed on a row (not on a header, scrollbar or the empty area).</summary>
    public static bool IsOnRow(object? originalSource) => RowOf(originalSource) is not null;
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>
    /// Opens the deadlock graph viewer for the right-clicked deadlock row. Lite carries the graph XML on
    /// the row, so there is no fetch — the parse runs off the UI thread; only the render touches the UI.
    /// </summary>
    private async void ViewDeadlockGraph_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is not DeadlockProcessDetail row) return;
        await OpenDeadlockGraphAsync(row);
    }

    /// <summary>Double-clicking a deadlock row opens the same graph viewer (no toolbar button — see plan).</summary>
    private async void DeadlockGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (DeadlockGrid.SelectedItem is not DeadlockProcessDetail row) return;
        await OpenDeadlockGraphAsync(row);
    }

    private async Task OpenDeadlockGraphAsync(DeadlockProcessDetail row)
    {
        try
        {
            // Parse off the UI thread (CPU-bound XML walk); the control renders the finished model. An
            // empty/unparseable graph yields an empty model and the control shows its empty state — same as
            // the Dashboard path, so the two apps behave identically.
            var model = await Task.Run(() => DeadlockGraphParser.Parse(row.DeadlockGraphXml));

            var control = new DeadlockGraphControl();
            control.LoadModel(model);
            GraphViewerWindow.ShowGraph(
                Window.GetWindow(this),
                control,
                $"Deadlock Graph — {_server.DisplayName}");
        }
        catch (Exception ex)
        {
            MessageBox.Show(
                Window.GetWindow(this)!,
                $"Failed to build the deadlock graph view:\n\n{ex.Message}",
                "Deadlock Graph Error", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }
}

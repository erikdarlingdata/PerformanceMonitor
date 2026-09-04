/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Deadlocks deadlock-graph viewer (W1e) — copied from Lite's <c>ServerTab.Deadlock.cs</c>. The row
/// already carries the graph XML (fetched with the grid), so there is no second fetch; the shared
/// <see cref="DeadlockGraphParser"/> parse runs off the UI thread (CPU-bound XML walk) and only the render
/// touches the UI. Reuses the shared .Ui <see cref="DeadlockGraphControl"/> + <see cref="GraphViewerWindow"/>.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>Right-click "View Deadlock Graph" on a deadlock row.</summary>
    private async void ViewDeadlockGraph_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is not DeadlockProcessDetail row) return;
        await OpenDeadlockGraphAsync(row);
    }

    /// <summary>
    /// "View Victim Plan" on a deadlock row: opens the BEST-EFFORT victim plan the deadlock carries
    /// (deadlocks.victim_query_plan_xml, #1368 / V7 — one plan per deadlock, threaded onto every process
    /// row) in the shared Plan Viewer host — NO live SQL, the same stored-plan surface Top Queries uses.
    /// The context item is gated per row on <see cref="DeadlockProcessDetail.HasVictimQueryPlan"/> (a NULL
    /// plan — the common case, and always so under Lite — shows it disabled), so this only fires with a
    /// captured plan; the guard here is belt-and-braces. Lite's "Get Actual Plan" (live) has no viewer
    /// equivalent and is omitted everywhere.
    /// </summary>
    private void ViewVictimQueryPlan_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        if (FindParentDataGrid(menuItem)?.CurrentItem is not DeadlockProcessDetail row || !row.HasVictimQueryPlan) return;
        /* queryText is null, not row.SqlText: the victim plan rides on every process row of the deadlock, so
           the right-clicked row's own statement text would mis-pair on a non-victim row. The plan XML carries
           its own statement text; the side panel simply stays empty (unlike the blocking rows, which each own
           their blocked/blocking SQL+plan pair and so pass it). */
        _ = OpenPlanTab(row.VictimQueryPlanXml!, "Victim Plan", null);
    }

    /// <summary>Double-clicking a deadlock row opens the same graph viewer.</summary>
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
            // the Lite/Dashboard path, so all three hosts behave identically.
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

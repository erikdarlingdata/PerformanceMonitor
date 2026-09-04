/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The standalone Plan Viewer surface for the Darling viewer's top tab strip (<c>MainTabs</c>), ported
/// from Lite's <c>MainWindow.PlanViewer.cs</c>: a hidden-until-used, closable "Plan Viewer" aggregate tab
/// (<c>MainWindowPlanViewerTab</c>) that hosts the SHARED <see cref="PlanViewerControl"/> as closable
/// sub-tabs. Unlike the per-server Plan Viewer (<see cref="ViewerServerTab"/>'s <c>OpenPlanTab</c>, which
/// fetches a STORED plan from Postgres), this surface reads no store data and needs no server connection --
/// the user supplies the plan by opening a .sqlplan / plan-XML file, pasting plan XML, or dragging a file
/// in. Opened / focused from the sidebar's "Open Plan Viewer" button.
///
/// The tab management itself (empty "New Plan" sub-tabs, open/paste/drag-drop, plan rendering, unique
/// labelling, teardown) is the shared <see cref="StandalonePlanViewerController"/> -- this file keeps only
/// the app-specific outer-container reveal/close (<c>MainTabs</c> vs the no-servers empty state) and forwards
/// the XAML-wired drag-over / drop / key-down handlers.
/// </summary>
public partial class MainWindow
{
    /* Lazy (a field initializer can't reference the instance-named MainWindowPlanTabControl). */
    private StandalonePlanViewerController? _planViewerControllerField;
    private StandalonePlanViewerController PlanViewerController =>
        _planViewerControllerField ??= new StandalonePlanViewerController(MainWindowPlanTabControl);

    private void OpenPlanViewerButton_Click(object sender, RoutedEventArgs e)
    {
        PlanViewerController.EnsureInitialized();
        /* The Plan Viewer is server-independent: show it even when no servers are registered (in which
           case the no-servers empty state otherwise owns the content column with MainTabs collapsed). */
        EmptyStatePanel.Visibility = Visibility.Collapsed;
        MainTabs.Visibility = Visibility.Visible;
        MainWindowPlanViewerTab.Visibility = Visibility.Visible;
        if (MainWindowPlanViewerTab.IsSelected)
        {
            // Already on Plan Viewer -- just add a new empty sub-tab
            PlanViewerController.AddNewEmptyPlanSubTab();
        }
        else
        {
            MainWindowPlanViewerTab.IsSelected = true;
            PlanViewerController.AddNewEmptyPlanSubTab();
        }
        Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(() => MainWindowPlanTabControl.Focus()));
    }

    /// <summary>
    /// Opens a STORED plan in the standalone Plan Viewer surface -- the destination for a "View Plan" click on
    /// the FinOps aggregate tab's High Impact / Expensive Queries grids (raised via <c>FinOpsTab.PlanRequested</c>).
    /// That aggregate tab has no per-server plan host, so it feeds the plan XML it already holds here: reveal the
    /// Plan Viewer aggregate tab and render the plan in a fresh sub-tab. Unlike <see cref="OpenPlanViewerButton_Click"/>
    /// (which opens an empty sub-tab for the user to paste/open into), this loads a plan the caller supplies.
    /// </summary>
    private void OpenStoredPlanInPlanViewer(string planXml, string label, string? queryText)
    {
        PlanViewerController.EnsureInitialized();
        /* Server-independent surface: show it even when the no-servers empty state otherwise owns the column. */
        EmptyStatePanel.Visibility = Visibility.Collapsed;
        MainTabs.Visibility = Visibility.Visible;
        MainWindowPlanViewerTab.Visibility = Visibility.Visible;
        MainWindowPlanViewerTab.IsSelected = true;

        var subTab = PlanViewerController.AddNewEmptyPlanSubTab();
        // Fire-and-forget (FinOps view-plan, not paste): discard the Task (CS4014 is an error here).
        _ = PlanViewerController.LoadPlanIntoSubTab(subTab, planXml, label, queryText);
    }

    private void MainWindowPlanViewerClose_Click(object sender, RoutedEventArgs e)
    {
        // Cleanup each sub-tab's viewer + reset the inner tab control (shared), then restore this app's shell.
        PlanViewerController.Reset();
        MainWindowPlanViewerTab.Visibility = Visibility.Collapsed;

        /* Return the content column to the state it was in before the Plan Viewer opened: if no servers
           are registered, restore the no-servers empty state (MainTabs collapsed, mirroring
           LoadServersAsync's no-servers branch); otherwise select the first visible non-plan tab. */
        if (_fleet.All.Count > 0)
        {
            foreach (TabItem item in MainTabs.Items)
            {
                if (item.Visibility == Visibility.Visible && item != MainWindowPlanViewerTab)
                {
                    item.IsSelected = true;
                    break;
                }
            }
        }
        else
        {
            MainTabs.Visibility = Visibility.Collapsed;
            EmptyStatePanel.Visibility = Visibility.Visible;
        }
    }

    private void MainWindowPlanViewer_DragOver(object sender, DragEventArgs e) =>
        PlanViewerController.HandleDragOver(e);

    private void MainWindowPlanViewer_Drop(object sender, DragEventArgs e) =>
        PlanViewerController.HandleDrop(e);

    private void MainWindowPlanViewer_KeyDown(object sender, System.Windows.Input.KeyEventArgs e) =>
        PlanViewerController.HandleKeyDown(e);
}

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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Blocked Process Reports block-chain viewer (W1e) — copied from Lite's <c>ServerTab.BlockChain.cs</c>
/// with the store rewired to Postgres. Fetch + reconstruct + tree-build run off the UI thread; only the
/// render touches the UI. The window is derived straight from the clicked row's event_time with NO clock
/// conversion (the reconstruction query filters on the same naive-UTC column). Reuses the shared .Ui
/// <see cref="BlockingChainControl"/> + <see cref="GraphViewerWindow"/> and the shared
/// <see cref="BlockingChainReconstructor"/>, bridged to the Common model by the viewer's copied
/// <see cref="BlockingChainViewerProjection"/>.
/// </summary>
public partial class ViewerServerTab
{
    // Right-click "View Block Chain" on a blocked-process-report row.
    private async void ViewBlockChain_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is ViewerBlockedProcessRow row)
            await OpenBlockChainForRowAsync(row);
    }

    // Double-click a blocked-process-report row.
    private async void BlockedProcessReportGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (BlockedProcessReportGrid.SelectedItem is ViewerBlockedProcessRow row)
            await OpenBlockChainForRowAsync(row);
    }

    // Reconstruct around the clicked row's own event time (+/- this many minutes) rather than the slicer
    // selection, so double-clicking any visible row reliably captures that event's chain. Wide enough to
    // span a blocking episode's report re-fires; SessionKey + edge-precise selection guard against merging.
    private const int ChainWindowMinutes = 5;

    /// <summary>
    /// Opens the block-chain viewer scoped to ONE chain — the chain the clicked session belongs to, rooted
    /// at its lead blocker, with the clicked session highlighted. Fetch + reconstruct + tree-build run off
    /// the UI thread; only the render touches the UI.
    /// </summary>
    private async Task OpenBlockChainForRowAsync(ViewerBlockedProcessRow row)
    {
        var spid = row.BlockedSpid;
        if (spid <= 0) return;
        var ecid = row.BlockedEcid;
        var monitorLoop = row.MonitorLoop;   // the clicked event's episode; scopes the reconstruction match

        try
        {
            // row.EventTime is read from the same event_time column the reconstruction query filters on, so
            // derive the window straight from it — NO clock conversion (the store is naive UTC and the pair-row
            // query filters on event_time). Fall back to the slicer selection / tab window only when a row has
            // no event_time. The slicer emits UTC, which the reads take directly.
            DateTime start, end;
            if (row.EventTime.HasValue)
            {
                start = row.EventTime.Value.AddMinutes(-ChainWindowMinutes);
                end = row.EventTime.Value.AddMinutes(ChainWindowMinutes);
            }
            else
            {
                var startUtc = BlockingSlicer.SelectionStartUtc;
                var endUtc = BlockingSlicer.SelectionEndUtc;
                if (startUtc.HasValue && endUtc.HasValue)
                {
                    start = startUtc.Value;
                    end = endUtc.Value;
                }
                else
                {
                    (start, end) = GetWindowUtc();
                }
            }

            var model = await Task.Run(async () =>
            {
                var rows = await _dataService.GetBlockingPairRowsAsync(_server.ServerId, start, end);
                var reconstruction = BlockingChainReconstructor.Reconstruct(
                    rows, maxDepth: 50, maxPairs: 5000, stepBudget: 100_000, scopeByMonitorLoop: true);
                return BlockingChainViewerProjection.BuildModelForSession(
                    reconstruction, monitorLoop, spid, ecid, ViewerTimeHelper.ForServerClockDisplay);
            });

            if (model == null)
            {
                MessageBox.Show(
                    Window.GetWindow(this)!,
                    $"No reconstructable blocking chain for SPID {spid} in the selected range.\n\n" +
                    "The session may not have been blocked (or blocking) in any captured blocked-process " +
                    "report or DMV blocking snapshot in this window.",
                    "No Block Chain",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var control = new BlockingChainControl();
            control.LoadModel(model, spid, ecid, BlockingChainViewerProjection.EmptyStateDetail);
            GraphViewerWindow.ShowGraph(
                Window.GetWindow(this),
                control,
                $"Block Chain — SPID {spid} on {_server.DisplayName}");
        }
        catch (Exception ex)
        {
            MessageBox.Show(
                Window.GetWindow(this)!,
                $"Failed to build the blocking-chain view:\n\n{ex.Message}",
                "Block Chain Error",
                MessageBoxButton.OK,
                MessageBoxImage.Warning);
        }
    }
}

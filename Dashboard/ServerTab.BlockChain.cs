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
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        // Right-click "View Block Chain" on a blocking-events row.
        private async void ViewBlockChain_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not MenuItem menuItem) return;
            if (menuItem.Parent is not ContextMenu cm) return;
            var grid = FindDataGridFromContextMenu(cm);
            if (grid?.SelectedItem is BlockingEventItem row)
                await OpenBlockChainForRowAsync(row);
        }

        // Double-click a blocking-events row.
        private async void BlockingEventsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (BlockingEventsDataGrid.SelectedItem is BlockingEventItem row)
                await OpenBlockChainForRowAsync(row);
        }

        // Reconstruct around the clicked row's own event time (+/- this many minutes) rather than the
        // slicer selection, so double-clicking any visible row reliably captures that event's chain (the
        // row itself is a blocked/blocker pair, so it's always in the window). Wide enough to span a
        // blocking episode's report re-fires; the SessionKey + edge-precise selection guard against merging
        // unrelated chains.
        private const int ChainWindowMinutes = 5;

        /// <summary>
        /// Opens the block-chain viewer scoped to ONE chain — the chain the clicked session belongs to,
        /// rooted at its lead blocker, with the clicked session highlighted. Fetch + reconstruct +
        /// tree-build run off the UI thread.
        /// </summary>
        private async Task OpenBlockChainForRowAsync(BlockingEventItem row)
        {
            if (_databaseService == null) return;

            var spid = row.Spid ?? 0;
            if (spid <= 0) return;
            var ecid = row.Ecid ?? 0;
            var monitorLoop = row.MonitorLoop;   // the clicked event's episode; scopes the reconstruction match

            try
            {
                // event_time is server-local (same column the reconstruction query filters), so derive the
                // window straight from it — no clock conversion needed. Fall back to the tab range only if a
                // row has no event_time.
                DateTime start, end;
                if (row.EventTime.HasValue)
                {
                    start = row.EventTime.Value.AddMinutes(-ChainWindowMinutes);
                    end = row.EventTime.Value.AddMinutes(ChainWindowMinutes);
                }
                else if (BlockingSlicer.HasNarrowedSelection
                    && BlockingSlicer.SelectionStart.HasValue
                    && BlockingSlicer.SelectionEnd.HasValue)
                {
                    start = BlockingSlicer.SelectionStart.Value;
                    end = BlockingSlicer.SelectionEnd.Value;
                }
                else
                {
                    (start, end) = GetLockingSlicerTimeRange(_blockingHoursBack, _blockingFromDate, _blockingToDate);
                }

                // Fetch (async DB) + reconstruct (CPU-bound over up to 5000 rows) + select the one chain that
                // contains the clicked (blocker -> blocked) edge, all off the UI thread.
                var model = await Task.Run(async () =>
                {
                    var rows = await _databaseService.GetBlockingPairRowsAsync(start, end);
                    var reconstruction = BlockingChainReconstructor.Reconstruct(
                        rows, maxDepth: 50, maxPairs: 5000, stepBudget: 100_000, scopeByMonitorLoop: true);
                    return BlockingChainViewerProjection.BuildModelForSession(
                        reconstruction, monitorLoop, spid, ecid);
                });

                if (model == null)
                {
                    MessageBox.Show(
                        Window.GetWindow(this)!,
                        $"No reconstructable blocking chain for SPID {spid} in the selected range.\n\n" +
                        "The session may not have been part of a blocked-process report whose wait crossed " +
                        "the blocked-process threshold in this window.",
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
                    $"Block Chain — SPID {spid} on {_serverConnection.DisplayName}");
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
}

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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    // Right-click "View Block Chain" on a blocked-process-report row.
    private async void ViewBlockChain_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem is BlockedProcessReportRow row)
            await OpenBlockChainForRowAsync(row);
    }

    // Double-click a blocked-process-report row.
    private async void BlockedProcessReportGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (BlockedProcessReportGrid.SelectedItem is BlockedProcessReportRow row)
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
    private async Task OpenBlockChainForRowAsync(BlockedProcessReportRow row)
    {
        var spid = row.BlockedSpid;
        if (spid <= 0) return;
        var ecid = row.BlockedEcid;
        var monitorLoop = row.MonitorLoop;   // the clicked event's episode; scopes the reconstruction match

        try
        {
            // row.EventTime is read from the same event_time column the reconstruction query filters on, so
            // derive the window straight from it — NO clock conversion (converting a value that's already in
            // the column's clock would shift it the wrong way). Fall back to the slicer/tab range only when a
            // row has no event_time.
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
                    start = ServerTimeHelper.ToServerTime(startUtc.Value);
                    end = ServerTimeHelper.ToServerTime(endUtc.Value);
                }
                else
                {
                    (start, end) = GetBlockingServerRange();
                }
            }

            // GetBlockingPairRowsAsync uses OpenConnectionAsync, which already takes the DuckDB read lock —
            // running it inside Task.Run keeps the lock acquire/release on a background thread (never the UI).
            var model = await Task.Run(async () =>
            {
                var rows = await _dataService.GetBlockingPairRowsAsync(_serverId, start, end);
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

    /// <summary>
    /// The current Blocking-tab server-local range, used when the slicer has no narrowed selection /
    /// no data. Mirrors LoadBlockingSlicerAsync's range computation.
    /// </summary>
    private (DateTime start, DateTime end) GetBlockingServerRange()
    {
        var hoursBack = GetHoursBack();
        DateTime? fromDate = null, toDate = null;
        if (IsCustomRange)
        {
            var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
            var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
            if (fromLocal.HasValue && toLocal.HasValue)
            {
                fromDate = ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                toDate = ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode);
            }
        }
        return GetSlicerTimeRange(hoursBack, fromDate, toDate);
    }
}

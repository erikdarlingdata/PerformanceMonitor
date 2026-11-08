/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using System.Windows;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Recommendations sub-tab — the last deferred FinOps sub-tab, reproduced MONITOR-SIDE. Lite's
/// Recommendations mixed collected reads with LIVE target queries; the headless viewer runs every check over the
/// already-collected Postgres store (<see cref="ViewerDataService.GetRecommendationsAsync"/>) — NO live SQL. This
/// partial projects the resulting rows (Category / Severity / Confidence / Finding / Detail / Est. Savings, sorted
/// by severity in the read) into the grid; the selected server's monthly budget (<c>_server.MonthlyCostUsd</c>)
/// drives the savings estimates (0 → the Est. Savings column stays blank, mirroring Lite). Load-on-view + a
/// Refresh button, like the other FinOps sub-tabs.
/// </summary>
public partial class FinOpsTab
{
    /// <summary>Reads the collected recommendations for this server and repaints the grid + count indicator.</summary>
    private async Task LoadFinOpsRecommendationsAsync()
    {
        /* #8115: while the read runs the sub-tab says so; empty headers under the caption are otherwise the same picture as no findings. */
        FinOpsRecommendationsCountIndicator.Text = FinOpsRecommendationsCountText.Loading;
        FinOpsRecommendationsNoDataMessage.Visibility = Visibility.Collapsed;
        try
        {
            var (data, skippedNote) = await _dataService.GetRecommendationsWithNoteAsync(_server.ServerId, _server.MonthlyCostUsd);

            /* #5558: databases this server holds only as an Availability Group secondary copy are left to the primary's findings. */
            FinOpsRecommendationsSecondaryNote.Text = skippedNote ?? "";
            FinOpsRecommendationsSecondaryNote.Visibility = skippedNote is null ? Visibility.Collapsed : Visibility.Visible;
            _finopsRecommendationsFilterMgr!.UpdateData(data);
            FinOpsRecommendationsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            FinOpsRecommendationsCountIndicator.Text = FinOpsRecommendationsCountText.For(data.Count);
        }
        catch
        {
            /* The shell's status bar carries the error; whatever ended the load, the count must not keep saying it is loading over the
               rows the grid already shows. */
            FinOpsRecommendationsCountIndicator.Text = FinOpsRecommendationsCountText.Failed;
            throw;
        }
    }

    /// <summary>Refresh button — re-runs the checks over the latest collected data with the shell's status-bar error surfacing.</summary>
    private async void FinOpsRefreshRecommendations_Click(object sender, RoutedEventArgs e)
        => await RunFinOpsLoad(LoadFinOpsRecommendationsAsync);
}

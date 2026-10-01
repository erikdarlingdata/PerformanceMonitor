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
        var data = await _dataService.GetRecommendationsAsync(_server.ServerId, _server.MonthlyCostUsd);

        _finopsRecommendationsFilterMgr!.UpdateData(data);
        FinOpsRecommendationsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        FinOpsRecommendationsCountIndicator.Text = data.Count > 0
            ? $"{data.Count} recommendation(s)"
            : "no recommendations";
    }

    /// <summary>Refresh button — re-runs the checks over the latest collected data with the shell's status-bar error surfacing.</summary>
    private async void FinOpsRefreshRecommendations_Click(object sender, RoutedEventArgs e)
        => await RunFinOpsLoad(LoadFinOpsRecommendationsAsync);
}

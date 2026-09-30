/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Queries-tab comparison overlay (W1f-1) — Lite's <c>ServerTab.Comparison.cs</c> ported. The shared
/// "Compare:" combo above the sub-tabs picks a baseline period (Yesterday / Last week / Same day last
/// week); when active, each sub-tab hides its normal grid and shows a collapsed comparison grid bound to
/// the shared <see cref="PerformanceMonitor.Ui.ComparisonItemBase"/> derivatives (delta % + NEW/GONE
/// badges), sorted NEW-first then by duration-delta descending. Changing the combo reloads the active
/// sub-tab (which re-applies the comparison over the toolbar's current window). The baseline is a
/// straight shift of the current window (no Lite display-mode conversion), and the baseline banner
/// renders the window in the viewer machine's local time.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// Changing the Compare combo reloads the active Queries sub-tab through the shell's overlap guard,
    /// so <see cref="LoadQueriesAsync"/> re-applies the comparison mode over the current window. Gated on
    /// <see cref="System.Windows.FrameworkElement.IsLoaded"/> so the SelectedIndex="0" set at build time
    /// is ignored.
    /// </summary>
    private async void CompareToCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;

        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Comparison overlays only the three grid sub-tabs (Top Queries / Top Procedures / Query Store); every
    /// other sub-tab (Performance Trends / Active Queries / Query Heatmap from W1f-2, plus the regressions
    /// and plan-correction grids) has no baseline overlay, so the Compare combo is reset to None and
    /// disabled there (Lite's <c>UpdateCompareDropdownState</c>, Queries slice). Called on every sub-tab
    /// change and once at init for the default (Performance Trends) state.
    /// </summary>
    private void UpdateCompareDropdownState()
    {
        var supported = QueriesSubTabControl.SelectedIndex
            is TopQueriesSubTabIndex or TopProceduresSubTabIndex or QueryStoreSubTabIndex;

        if (supported)
        {
            CompareToCombo.IsEnabled = true;
            CompareToCombo.Opacity = 1.0;
            CompareToCombo.ToolTip = "Compare the current window against a baseline period";
        }
        else
        {
            CompareToCombo.SelectedIndex = 0; /* None — no comparison on the non-grid sub-tabs */
            CompareToCombo.IsEnabled = false;
            CompareToCombo.Opacity = 0.5;
            CompareToCombo.ToolTip = "Comparison is not available for this sub-tab";
        }
    }

    /// <summary>
    /// The baseline window for the current Compare selection — a straight shift of the current window
    /// (Yesterday = -1 day, Last week / Same day last week = -7 days). Null when Compare is "None".
    /// </summary>
    private (DateTime From, DateTime To)? GetComparisonRange(DateTime currentStart, DateTime currentEnd)
    {
        if (CompareToCombo == null || CompareToCombo.SelectedIndex <= 0) return null;

        return CompareToCombo.SelectedIndex switch
        {
            1 => (currentStart.AddDays(-1), currentEnd.AddDays(-1)),   // Yesterday
            2 => (currentStart.AddDays(-7), currentEnd.AddDays(-7)),   // Last week
            3 => (currentStart.AddDays(-7), currentEnd.AddDays(-7)),   // Same day last week
            _ => null,
        };
    }

    private void SetComparisonMode(
        DataGrid normalGrid, DataGrid comparisonGrid, TextBlock banner,
        bool active, (DateTime From, DateTime To)? baselineRange)
    {
        normalGrid.Visibility = active ? Visibility.Collapsed : Visibility.Visible;
        comparisonGrid.Visibility = active ? Visibility.Visible : Visibility.Collapsed;
        banner.Visibility = active ? Visibility.Visible : Visibility.Collapsed;

        if (active && baselineRange.HasValue)
        {
            var from = ViewerTimeHelper.FormatForDisplay(baselineRange.Value.From, "yyyy-MM-dd HH:mm");
            var to = ViewerTimeHelper.FormatForDisplay(baselineRange.Value.To, "yyyy-MM-dd HH:mm");
            banner.Text = $"Comparing against baseline: {from} → {to}";
        }
    }

    private async Task RefreshQueryStatsComparisonAsync(DateTime currentStart, DateTime currentEnd)
    {
        var baseline = GetComparisonRange(currentStart, currentEnd);
        if (baseline == null)
        {
            SetComparisonMode(QueryStatsGrid, QueryStatsComparisonGrid, QueryStatsComparisonBanner, active: false, null);
            return;
        }

        SetComparisonMode(QueryStatsGrid, QueryStatsComparisonGrid, QueryStatsComparisonBanner, active: true, baseline);

        var items = await _dataService.GetQueryStatsComparisonAsync(
            _server.ServerId, currentStart, currentEnd, baseline.Value.From, baseline.Value.To, databaseNames: SelectedDatabaseFilter);
        QueryStatsComparisonGrid.ItemsSource = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();
    }

    private async Task RefreshProcStatsComparisonAsync(DateTime currentStart, DateTime currentEnd)
    {
        var baseline = GetComparisonRange(currentStart, currentEnd);
        if (baseline == null)
        {
            SetComparisonMode(ProcedureStatsGrid, ProcStatsComparisonGrid, ProcStatsComparisonBanner, active: false, null);
            return;
        }

        SetComparisonMode(ProcedureStatsGrid, ProcStatsComparisonGrid, ProcStatsComparisonBanner, active: true, baseline);

        var items = await _dataService.GetProcedureStatsComparisonAsync(
            _server.ServerId, currentStart, currentEnd, baseline.Value.From, baseline.Value.To, databaseNames: SelectedDatabaseFilter);
        ProcStatsComparisonGrid.ItemsSource = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();
    }

    private async Task RefreshQueryStoreComparisonAsync(DateTime currentStart, DateTime currentEnd)
    {
        var baseline = GetComparisonRange(currentStart, currentEnd);
        if (baseline == null)
        {
            SetComparisonMode(QueryStoreGrid, QueryStoreComparisonGrid, QueryStoreComparisonBanner, active: false, null);
            return;
        }

        SetComparisonMode(QueryStoreGrid, QueryStoreComparisonGrid, QueryStoreComparisonBanner, active: true, baseline);

        var items = await _dataService.GetQueryStoreComparisonAsync(
            _server.ServerId, currentStart, currentEnd, baseline.Value.From, baseline.Value.To, databaseNames: SelectedDatabaseFilter);
        QueryStoreComparisonGrid.ItemsSource = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();
    }
}

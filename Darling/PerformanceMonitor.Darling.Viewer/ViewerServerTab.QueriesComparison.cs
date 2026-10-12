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
using PerformanceMonitor.Ui;

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
        if (!IsLoaded || _suppressCompareRefresh) return;

        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// What the disabled Compare box says when the pointer rests on it (walk finding V10b: the old tooltip was never shown on a
    /// disabled control). It names the sub-tabs <see cref="IsComparisonSupported"/> accepts.
    /// </summary>
    internal const string CompareUnavailableToolTip =
        "Compare works on the Top Queries, Top Procedures and Query Store sub-tabs of Queries. It is off on this tab.";

    /// <summary>
    /// Comparison overlays only the three grid sub-tabs of Queries (Top Queries / Top Procedures / Query Store). Every other inner
    /// tab, and every other Queries sub-tab (Performance Trends / Active Queries / Query Heatmap, plus the regressions and
    /// plan-correction grids), has no baseline overlay. Lite's Overview tab also draws a comparison; the Viewer's does not.
    /// </summary>
    internal static bool IsComparisonSupported(int innerTabIndex, int queriesSubTabIndex) =>
        innerTabIndex == QueriesInnerTabIndex
        && queriesSubTabIndex is TopQueriesSubTabIndex or TopProceduresSubTabIndex or QueryStoreSubTabIndex;

    /// <summary>
    /// Where there is no comparison the Compare combo is reset to None and disabled (Lite's <c>UpdateCompareDropdownState</c>).
    /// Called on every inner-tab and Queries sub-tab change and once at init for the default (Overview) state. It used to look at
    /// the Queries sub-tab only, so after a visit to Top Queries the box stayed enabled on Wait Stats, File I/O and every other
    /// tab where it does nothing.
    /// </summary>
    private void UpdateCompareDropdownState()
    {
        var supported = IsComparisonSupported(InnerTabs.SelectedIndex, QueriesSubTabControl.SelectedIndex);

        if (supported)
        {
            CompareToCombo.IsEnabled = true;
            CompareToCombo.Opacity = 1.0;
            CompareToCombo.ToolTip = "Compare the current window against a baseline period";
        }
        else
        {
            /* The program writes the combo here, not the user: no reload from it. The inner-tab or sub-tab switch that called
               this loads the tab already, and a second load for the reset would be a wasted read. */
            var wasSuppressed = _suppressCompareRefresh;
            _suppressCompareRefresh = true;
            try
            {
                CompareToCombo.SelectedIndex = 0; /* None: no comparison here */
            }
            finally
            {
                _suppressCompareRefresh = wasSuppressed;
            }

            CompareToCombo.IsEnabled = false;
            CompareToCombo.Opacity = 0.5;
            CompareToCombo.ToolTip = CompareUnavailableToolTip;
        }
    }

    private bool _suppressCompareRefresh;

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

    private async Task RefreshQueryStatsComparisonAsync(DateTime currentStart, DateTime currentEnd, ViewerLoadTimer? timer = null)
    {
        var baseline = GetComparisonRange(currentStart, currentEnd);
        if (baseline == null)
        {
            SetComparisonMode(QueryStatsGrid, QueryStatsComparisonGrid, QueryStatsComparisonBanner, active: false, null);
            return;
        }

        SetComparisonMode(QueryStatsGrid, QueryStatsComparisonGrid, QueryStatsComparisonBanner, active: true, baseline);

        var items = await Timed(timer, "comparison read", _dataService.GetQueryStatsComparisonAsync(
            _server.ServerId, currentStart, currentEnd, baseline.Value.From, baseline.Value.To, databaseNames: SelectedDatabaseFilter));
        /* Release walk V12d: a baseline period the store holds nothing for (Yesterday on a store a few hours old) says so. */
        QueryStatsComparisonBanner.Text = ComparisonBaselineNote.Banner(QueryStatsComparisonBanner.Text, items);
        QueryStatsComparisonGrid.ItemsSource = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();
    }

    private async Task RefreshProcStatsComparisonAsync(DateTime currentStart, DateTime currentEnd, ViewerLoadTimer? timer = null)
    {
        var baseline = GetComparisonRange(currentStart, currentEnd);
        if (baseline == null)
        {
            SetComparisonMode(ProcedureStatsGrid, ProcStatsComparisonGrid, ProcStatsComparisonBanner, active: false, null);
            return;
        }

        SetComparisonMode(ProcedureStatsGrid, ProcStatsComparisonGrid, ProcStatsComparisonBanner, active: true, baseline);

        var items = await Timed(timer, "comparison read", _dataService.GetProcedureStatsComparisonAsync(
            _server.ServerId, currentStart, currentEnd, baseline.Value.From, baseline.Value.To, databaseNames: SelectedDatabaseFilter));
        /* Release walk V12d: a baseline period the store holds nothing for (Yesterday on a store a few hours old) says so. */
        ProcStatsComparisonBanner.Text = ComparisonBaselineNote.Banner(ProcStatsComparisonBanner.Text, items);
        ProcStatsComparisonGrid.ItemsSource = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();
    }

    private async Task RefreshQueryStoreComparisonAsync(DateTime currentStart, DateTime currentEnd, ViewerLoadTimer? timer = null)
    {
        var baseline = GetComparisonRange(currentStart, currentEnd);
        if (baseline == null)
        {
            SetComparisonMode(QueryStoreGrid, QueryStoreComparisonGrid, QueryStoreComparisonBanner, active: false, null);
            return;
        }

        SetComparisonMode(QueryStoreGrid, QueryStoreComparisonGrid, QueryStoreComparisonBanner, active: true, baseline);

        var items = await Timed(timer, "comparison read", _dataService.GetQueryStoreComparisonAsync(
            _server.ServerId, currentStart, currentEnd, baseline.Value.From, baseline.Value.To, databaseNames: SelectedDatabaseFilter));
        /* Release walk V12d: a baseline period the store holds nothing for (Yesterday on a store a few hours old) says so. */
        QueryStoreComparisonBanner.Text = ComparisonBaselineNote.Banner(QueryStoreComparisonBanner.Text, items);
        QueryStoreComparisonGrid.ItemsSource = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();
    }
}

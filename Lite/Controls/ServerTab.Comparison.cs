/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private async void CompareToCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _isRefreshing) return;

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

        await RefreshOverviewAsync(hoursBack, fromDate, toDate);

        // Also refresh comparison grids
        try
        {
            var currentEnd = toDate ?? DateTime.UtcNow;
            var currentStart = fromDate ?? currentEnd.AddHours(-hoursBack);
            await RefreshQueryStatsComparisonAsync(currentStart, currentEnd);
            await RefreshProcStatsComparisonAsync(currentStart, currentEnd);
            await RefreshQueryStoreComparisonAsync(currentStart, currentEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] Comparison refresh failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Computes the reference time range for the comparison overlay based on the
    /// current Compare dropdown selection and the active time range.
    /// Returns null if "None" is selected.
    /// </summary>
    private (DateTime From, DateTime To)? GetComparisonRange()
    {
        if (CompareToCombo == null || CompareToCombo.SelectedIndex <= 0) return null;

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

        var currentEnd = toDate ?? DateTime.UtcNow;
        var currentStart = fromDate ?? currentEnd.AddHours(-hoursBack);

        return CompareToCombo.SelectedIndex switch
        {
            1 => (currentStart.AddDays(-1), currentEnd.AddDays(-1)),   // Yesterday
            2 => (currentStart.AddDays(-7), currentEnd.AddDays(-7)),   // Last week
            3 => (currentStart.AddDays(-7), currentEnd.AddDays(-7)),   // Same day last week
            _ => null
        };
    }

    /* #2933: CompareToCombo_SelectionChanged sets no in-flight flag, so two Compare changes overlap and
       the LATER-STARTING read can land first, leaving the grid showing the baseline the operator moved
       off. The grids are on screen while that happens, so no visibility change comes along to repaint
       them. A scope re-verify cannot separate Yesterday -> Last week -> Yesterday: the third read's
       range equals the first's, so the first still compares current and lands last. Each comparison
       grid gets its own generation instead, which compares identity rather than range.

       Only the comparison loaders take this. ServerTab's main-tab Refresh*Async loaders are deliberately
       NOT here: their races all route through the bail-only _isRefreshing, where the user-visible half is
       the DROPPED trigger (a time-range change mid-pass leaves the charts on the old window while the
       combo shows the new one), and a generation cannot fix a load that never started. That wants the
       viewer's coalescing replay, which needs _isRefreshing split from the event-suppression duty it also
       serves in TimeDisplayMode_SelectionChanged, ServerTab.DrillDown.cs and ServerTab.Grids.cs. */
    private readonly PerformanceMonitor.Ui.ScopedLoadGenerations _loads = new();

    private bool IsQueryStatsComparisonActive => GetComparisonRange() != null;

    private void SetQueryStatsComparisonMode(bool active, (DateTime From, DateTime To)? baselineRange = null)
    {
        QueryStatsGrid.Visibility = active ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
        QueryStatsComparisonGrid.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        QueryStatsComparisonBanner.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;

        if (active && baselineRange.HasValue)
        {
            var from = ServerTimeHelper.FormatServerTime(baselineRange.Value.From);
            var to = ServerTimeHelper.FormatServerTime(baselineRange.Value.To);
            QueryStatsComparisonBanner.Text = $"Comparing against baseline: {from} → {to}";
        }
    }

    private async System.Threading.Tasks.Task RefreshQueryStatsComparisonAsync(DateTime currentStart, DateTime currentEnd)
    {
        var gen = _loads.Claim(nameof(RefreshQueryStatsComparisonAsync));
        var baselineRange = GetComparisonRange();
        if (baselineRange == null)
        {
            SetQueryStatsComparisonMode(false);
            return;
        }

        SetQueryStatsComparisonMode(true, baselineRange);

        var items = await Task.Run(() => _dataService.GetQueryStatsComparisonAsync(
            _serverId, currentStart, currentEnd,
            baselineRange.Value.From, baselineRange.Value.To, SelectedDatabaseFilter));
        if (_loads.Superseded(nameof(RefreshQueryStatsComparisonAsync), gen)) return;

        // Sort: NEW first, then by duration delta descending, GONE last
        var sorted = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();

        QueryStatsComparisonGrid.ItemsSource = sorted;
    }

    private void SetProcStatsComparisonMode(bool active, (DateTime From, DateTime To)? baselineRange = null)
    {
        ProcedureStatsGrid.Visibility = active ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
        ProcStatsComparisonGrid.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        ProcStatsComparisonBanner.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;

        if (active && baselineRange.HasValue)
        {
            var from = ServerTimeHelper.FormatServerTime(baselineRange.Value.From);
            var to = ServerTimeHelper.FormatServerTime(baselineRange.Value.To);
            ProcStatsComparisonBanner.Text = $"Comparing against baseline: {from} → {to}";
        }
    }

    private async System.Threading.Tasks.Task RefreshProcStatsComparisonAsync(DateTime currentStart, DateTime currentEnd)
    {
        var gen = _loads.Claim(nameof(RefreshProcStatsComparisonAsync));
        var baselineRange = GetComparisonRange();
        if (baselineRange == null)
        {
            SetProcStatsComparisonMode(false);
            return;
        }

        SetProcStatsComparisonMode(true, baselineRange);

        var items = await Task.Run(() => _dataService.GetProcedureStatsComparisonAsync(
            _serverId, currentStart, currentEnd,
            baselineRange.Value.From, baselineRange.Value.To, SelectedDatabaseFilter));
        if (_loads.Superseded(nameof(RefreshProcStatsComparisonAsync), gen)) return;

        var sorted = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();

        ProcStatsComparisonGrid.ItemsSource = sorted;
    }

    private void SetQueryStoreComparisonMode(bool active, (DateTime From, DateTime To)? baselineRange = null)
    {
        QueryStoreGrid.Visibility = active ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
        QueryStoreComparisonGrid.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        QueryStoreComparisonBanner.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;

        if (active && baselineRange.HasValue)
        {
            var from = ServerTimeHelper.FormatServerTime(baselineRange.Value.From);
            var to = ServerTimeHelper.FormatServerTime(baselineRange.Value.To);
            QueryStoreComparisonBanner.Text = $"Comparing against baseline: {from} → {to}";
        }
    }

    private async System.Threading.Tasks.Task RefreshQueryStoreComparisonAsync(DateTime currentStart, DateTime currentEnd)
    {
        var gen = _loads.Claim(nameof(RefreshQueryStoreComparisonAsync));
        var baselineRange = GetComparisonRange();
        if (baselineRange == null)
        {
            SetQueryStoreComparisonMode(false);
            return;
        }

        SetQueryStoreComparisonMode(true, baselineRange);

        var items = await Task.Run(() => _dataService.GetQueryStoreComparisonAsync(
            _serverId, currentStart, currentEnd,
            baselineRange.Value.From, baselineRange.Value.To, SelectedDatabaseFilter));
        if (_loads.Superseded(nameof(RefreshQueryStoreComparisonAsync), gen)) return;

        var sorted = items
            .OrderBy(x => x.SortGroup)
            .ThenByDescending(x => x.SortableDurationDelta)
            .ToList();

        QueryStoreComparisonGrid.ItemsSource = sorted;
    }

    private bool IsComparisonSupportedOnCurrentTab()
    {
        return MainTabControl.SelectedIndex switch
        {
            0 => true, // Overview — correlated timeline lanes
            2 => QueriesSubTabControl.SelectedIndex is 2 or 3 or 4, // Top Queries / Top Procedures / Query Store
            _ => false
        };
    }

    private void UpdateCompareDropdownState()
    {
        var supported = IsComparisonSupportedOnCurrentTab();

        if (supported)
        {
            CompareToCombo.IsEnabled = true;
            CompareToCombo.Opacity = 1.0;
            CompareToCombo.ToolTip = "Compare current period against a baseline";
        }
        else
        {
            CompareToCombo.SelectedIndex = 0;
            CompareToCombo.IsEnabled = false;
            CompareToCombo.Opacity = 0.5;
            CompareToCombo.ToolTip = "Comparison is not available for this tab";
        }
    }
}

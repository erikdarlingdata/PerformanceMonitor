/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>
    /// Public entry point to trigger a data refresh from outside.
    /// Loads only the visible tab — other tabs load on demand when clicked.
    /// </summary>
    public async void RefreshData()
    {
        await RefreshAllDataAsync();
    }

    /* Deadlock-graph XML parsing (XElement.Parse + deep Descendants traversal per row) is heavy
       enough to hitch the dispatcher on the Blocking tab; run it on the thread pool so only the
       grid bind stays on the UI thread. */
    private Task<List<DeadlockProcessDetail>> ParseDeadlocksOffUiThreadAsync(List<DeadlockRow> rows)
    {
        /* #1319: deadlocks have no database_name column (the per-process DB is inside the graph XML), so
           the global database filter is applied client-side on the parsed per-process rows. Snapshot the
           selection on the UI thread; empty = All (unfiltered). */
        var selected = _selectedDatabases.Count == 0
            ? null
            : new HashSet<string>(_selectedDatabases, StringComparer.OrdinalIgnoreCase);
        return Task.Run(() =>
        {
            var details = DeadlockProcessDetail.ParseFromRows(rows);
            return selected == null
                ? details
                : details.Where(d => selected.Contains(d.DatabaseName)).ToList();
        });
    }

    /// <summary>
    /// The current toolbar window as (hoursBack, fromDate, toDate) in server time — the single derivation
    /// shared by the data refresh and the per-chart Revert / double-click axis re-pin, so both read the same
    /// window. A preset leaves from/to null (charts fall back to now − hoursBack); a valid custom range
    /// converts the local picker dates/times to server time.
    /// </summary>
    /// <param name="utcOffsetMinutes">
    /// Whose server time the returned bounds are in. Every read given this window converts the bounds back
    /// out to UTC using an offset of its own, and the two have to be the same server's or the pair stops
    /// cancelling — so this argument is chosen to match the read being fed, not chosen once for the tab.
    /// <c>ServerTimeHelper.UtcOffsetMinutes</c> for the sub-tab reads, which take their offset from the
    /// selected tab; this tab's own <c>UtcOffsetMinutes</c> for the badge read, which takes its offset from
    /// the server it names.
    /// </param>
    private (int hoursBack, DateTime? fromDate, DateTime? toDate) GetCurrentWindow(int utcOffsetMinutes)
    {
        var hoursBack = GetHoursBack();

        DateTime? fromDate = null;
        DateTime? toDate = null;
        if (IsCustomRange)
        {
            var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
            var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
            if (fromLocal.HasValue && toLocal.HasValue)
            {
                fromDate = ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode, utcOffsetMinutes);
                toDate = ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode, utcOffsetMinutes);
            }
        }

        return (hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task RefreshAllDataAsync()
    {
        if (_isRefreshing) return;
        _isRefreshing = true;

        /* The selected tab's offset, because the sub-tab reads below convert back out to UTC with that
           same offset and the two applications have to name one server to cancel. */
        var (hoursBack, fromDate, toDate) = GetCurrentWindow(ServerTimeHelper.UtcOffsetMinutes);

        try
        {
            using var _profiler = Helpers.MethodProfiler.StartTiming($"ServerTab-{_server?.DisplayName}");

            /* When this server tab isn't the selected one, its charts/grids aren't on screen —
               skip the heavy sub-tab data refresh and just keep the alert badge current. Mark
               dirty so the sub-tab is refreshed when the tab is selected again (IsVisibleChanged). */
            if (IsVisible)
            {
                await RefreshVisibleTabAsync(hoursBack, fromDate, toDate, subTabOnly: true);
            }
            else
            {
                _refreshPendingWhileHidden = true;
            }
            /* Always keep alert badge current even when Blocking tab is not visible. Deliberately not
               given the window above: that one is in the SELECTED tab's server time, and this runs on
               every tab's timer regardless of which is selected. It derives its own. */
            if (MainTabControl.SelectedIndex != 8)
                await RefreshAlertCountsAsync();

            /* #1591: same reasoning as the alert badge above — a permission-denied collector is only visible on
               the Collection Health tab, which is precisely why it went unnoticed. Badge it from every tab. */
            await RefreshPermissionDeniedBadgeAsync();

            var tz = ServerTimeHelper.GetTimezoneLabel(ServerTimeHelper.CurrentDisplayMode);
            ConnectionStatusText.Text = $"Last refresh: {DateTime.Now:HH:mm:ss} ({tz})";
        }
        catch (Exception ex)
        {
            ConnectionStatusText.Text = $"Error: {ex.Message}";
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshAllDataAsync failed: {ex}");
        }
        finally
        {
            _isRefreshing = false;
        }
    }

    private async System.Threading.Tasks.Task RefreshVisibleTabAsync(int hoursBack, DateTime? fromDate, DateTime? toDate, bool subTabOnly = false)
    {
        switch (MainTabControl.SelectedIndex)
        {
            case 0: await RefreshOverviewAsync(hoursBack, fromDate, toDate); break;
            case 1: await RefreshWaitStatsAsync(hoursBack, fromDate, toDate); break;
            case 2: await RefreshQueriesAsync(hoursBack, fromDate, toDate, subTabOnly); break;
            case 3: break; // Plan Viewer — no queries
            case 4: await RefreshCpuAsync(hoursBack, fromDate, toDate); break;
            case 5: await RefreshMemoryAsync(hoursBack, fromDate, toDate, subTabOnly); break;
            case 6: await RefreshFileIoAsync(hoursBack, fromDate, toDate); break;
            case 7: await RefreshTempDbAsync(hoursBack, fromDate, toDate); break;
            case 8: await RefreshBlockingAsync(hoursBack, fromDate, toDate, subTabOnly); break;
            case 9: await RefreshPerfmonAsync(hoursBack, fromDate, toDate); break;
            case 10: await RefreshRunningJobsAsync(hoursBack, fromDate, toDate); break;
            case 11: await RefreshConfigurationAsync(hoursBack, fromDate, toDate); break;
            case 12: await RefreshDailySummaryAsync(hoursBack, fromDate, toDate); break;
            case 13: await RefreshLatchSpinlockAsync(hoursBack, fromDate, toDate); break;
            case 14: await RefreshCpuSchedulerAsync(hoursBack, fromDate, toDate); break;
            case 15: await RefreshPlanCacheAsync(hoursBack, fromDate, toDate); break;
            case 16: await RefreshSessionStatsAsync(hoursBack, fromDate, toDate); break;
            case 17: await RefreshCollectionHealthAsync(hoursBack, fromDate, toDate); break;
            case 18: await RefreshSystemEventsAsync(hoursBack, fromDate, toDate); break;
            case 19: await RefreshConfigChangesAsync(hoursBack, fromDate, toDate); break;
            case 20: await RefreshLongQueriesAsync(hoursBack, fromDate, toDate); break;
        }
    }

    /// <summary>
    /// Lightweight alert-only refresh — fetches blocking + deadlock counts and fires AlertCountsChanged.
    /// Runs on every timer tick when the Blocking tab is NOT visible so the tab badge stays current.
    ///
    /// <para>Derives its own window instead of taking the caller's, and derives it in THIS tab's
    /// <c>UtcOffsetMinutes</c> rather than in the selected tab's. Every other windowed read here sits
    /// behind the <c>IsVisible</c> gate, where this tab is the selected tab and the two offsets are the
    /// same value; the badge is the one that runs for a background tab, whose server can be in a
    /// different zone from the one on screen. The same offset goes into the picker conversion and into
    /// <see cref="LocalDataService.GetAlertCountsAsync"/>, which is what keeps the two applications
    /// cancelling in the UTC and Local display modes while leaving the ServerTime default correct.</para>
    /// </summary>
    private async System.Threading.Tasks.Task RefreshAlertCountsAsync()
    {
        try
        {
            var (hoursBack, fromDate, toDate) = GetCurrentWindow(UtcOffsetMinutes);
            var (blockingCount, deadlockCount, latestEventTime) = await Task.Run(() => _dataService.GetAlertCountsAsync(_serverId, hoursBack, fromDate, toDate, UtcOffsetMinutes));
            AlertCountsChanged?.Invoke(blockingCount, deadlockCount, latestEventTime);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshAlertCountsAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// #1591: badges the Collection Health tab header with the number of collectors that were permission-denied.
    /// Runs on every refresh, like the alert badge, so an empty tab caused by a missing grant is discoverable
    /// without knowing to go looking for it. Never throws — a failed badge must not break the refresh.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshPermissionDeniedBadgeAsync()
    {
        try
        {
            var denied = await Task.Run(() => _dataService.GetPermissionDeniedCollectorCountAsync(_serverId));
            CollectionHealthTab.Header = denied > 0
                ? $"Collection Health ({denied} no permission)"
                : "Collection Health";
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshPermissionDeniedBadgeAsync failed: {ex.Message}");
        }
    }

    /* ───────────────────────────── Per-tab refresh methods ───────────────────────────── */

    /// <summary>Tab 1 — Wait Stats</summary>
    private async System.Threading.Tasks.Task RefreshWaitStatsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var waitTypesTask = Task.Run(() => _dataService.GetDistinctWaitTypesAsync(_serverId, hoursBack, fromDate, toDate));
            await waitTypesTask;
            PopulateWaitTypePicker(waitTypesTask.Result);
            await UpdateWaitStatsChartFromPickerAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshWaitStatsAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 2 — Queries</summary>
    private async System.Threading.Tasks.Task RefreshQueriesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate, bool subTabOnly = false)
    {
        try
        {
            if (subTabOnly)
            {
                /* Timer tick: only refresh the visible sub-tab (8 queries → 1-4) */
                switch (QueriesSubTabControl.SelectedIndex)
                {
                    case 0: // Performance Trends — 4 trend charts
                        var qdt = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QueryDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetQueryDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
                        var pdt = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ProcDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetProcedureDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
                        var qsdt = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QsDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetQueryStoreDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
                        var ect = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ExecutionTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetExecutionCountTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
                        await System.Threading.Tasks.Task.WhenAll(qdt, pdt, qsdt, ect);
                        UpdateQueryDurationTrendChart(qdt.Result, hoursBack, fromDate, toDate);
                        UpdateProcDurationTrendChart(pdt.Result, hoursBack, fromDate, toDate);
                        UpdateQueryStoreDurationTrendChart(qsdt.Result, hoursBack, fromDate, toDate);
                        UpdateExecutionCountTrendChart(ect.Result, hoursBack, fromDate, toDate);
                        break;
                    case 1: // Active Queries
                        var snapshots = await Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
                        _querySnapshotsFilterMgr!.UpdateData(snapshots);
                        LiveSnapshotIndicator.Text = "";
                        _ = LoadActiveQueriesSlicerAsync();
                        break;
                    case 2: // Top Queries by Duration
                        var queryStats = await Task.Run(() => _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes, SelectedDatabaseFilter));
                        _queryStatsFilterMgr!.UpdateData(queryStats);
                        SetDefaultSortIfNone(QueryStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
                        _ = LoadQueryStatsSlicerAsync();
                        {
                            var cEnd = toDate ?? DateTime.UtcNow;
                            var cStart = fromDate ?? cEnd.AddHours(-hoursBack);
                            await RefreshQueryStatsComparisonAsync(cStart, cEnd);
                        }
                        break;
                    case 3: // Top Procedures by Duration
                        var procStats = await Task.Run(() => _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes, SelectedDatabaseFilter));
                        _procStatsFilterMgr!.UpdateData(procStats);
                        SetDefaultSortIfNone(ProcedureStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
                        _ = LoadProcStatsSlicerAsync();
                        {
                            var cEnd = toDate ?? DateTime.UtcNow;
                            var cStart = fromDate ?? cEnd.AddHours(-hoursBack);
                            await RefreshProcStatsComparisonAsync(cStart, cEnd);
                        }
                        break;
                    case 4: // Query Store by Duration
                        var qsData = await Task.Run(() => _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate, SelectedDatabaseFilter));
                        _queryStoreFilterMgr!.UpdateData(qsData);
                        SetDefaultSortIfNone(QueryStoreGrid, "TotalDurationMs", ListSortDirection.Descending);
                        _ = LoadQueryStoreSlicerAsync();
                        {
                            var cEnd = toDate ?? DateTime.UtcNow;
                            var cStart = fromDate ?? cEnd.AddHours(-hoursBack);
                            await RefreshQueryStoreComparisonAsync(cStart, cEnd);
                        }
                        break;
                    case 5: // Plan Corrections
                        var planCorrections = await Task.Run(() => SafeQueryAsync(() => _dataService.GetPlanCorrectionsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter)));
                        _planCorrectionFilterMgr!.UpdateData(planCorrections);
                        SetDefaultSortIfNone(PlanCorrectionGrid, "Score", ListSortDirection.Descending);
                        break;
                    case 6: // Query Heatmap
                        var hmMetric = (HeatmapMetric)HeatmapMetricCombo.SelectedIndex;
                        var hmData = await Task.Run(() => _dataService.GetQueryHeatmapAsync(_serverId, hmMetric, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
                        AppLogger.Info("ServerTab", $"[{_server.DisplayName}] Heatmap: {hmData.TimeBuckets.Length} time buckets, {hmData.Intensities.GetLength(0)}x{hmData.Intensities.GetLength(1)} grid");
                        UpdateQueryHeatmapChart(hmData);
                        break;
                }
                return;
            }

            /* Full refresh: load all sub-tabs */
            var snapshotsTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.Snapshots", () => Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter)));
            var queryStatsTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QueryStats", () => Task.Run(() => _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes, SelectedDatabaseFilter)));
            var procStatsTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ProcStats", () => Task.Run(() => _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes, SelectedDatabaseFilter)));
            var queryStoreTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QueryStore", () => Task.Run(() => _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate, SelectedDatabaseFilter)));
            var planCorrectionTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.PlanCorrections", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetPlanCorrectionsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var queryDurationTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QueryDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetQueryDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var procDurationTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ProcDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetProcedureDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var queryStoreDurationTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QsDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetQueryStoreDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var executionCountTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ExecutionTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetExecutionCountTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var heatmapTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.Heatmap", () => Task.Run(async () =>
            {
                try { return await _dataService.GetQueryHeatmapAsync(_serverId, (HeatmapMetric)Dispatcher.Invoke(() => HeatmapMetricCombo.SelectedIndex), hoursBack, fromDate, toDate, SelectedDatabaseFilter); }
                catch { return new HeatmapResult(); }
            }));

            await System.Threading.Tasks.Task.WhenAll(
                snapshotsTask, queryStatsTask, procStatsTask, queryStoreTask, planCorrectionTask,
                queryDurationTrendTask, procDurationTrendTask, queryStoreDurationTrendTask, executionCountTrendTask,
                heatmapTask);

            _querySnapshotsFilterMgr!.UpdateData(snapshotsTask.Result);
            LiveSnapshotIndicator.Text = "";

            _ = LoadActiveQueriesSlicerAsync();

            _queryStatsFilterMgr!.UpdateData(queryStatsTask.Result);
            SetDefaultSortIfNone(QueryStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
            _ = LoadQueryStatsSlicerAsync();
            {
                var cEnd = toDate ?? DateTime.UtcNow;
                var cStart = fromDate ?? cEnd.AddHours(-hoursBack);
                await RefreshQueryStatsComparisonAsync(cStart, cEnd);
            }
            _procStatsFilterMgr!.UpdateData(procStatsTask.Result);
            SetDefaultSortIfNone(ProcedureStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
            _ = LoadProcStatsSlicerAsync();
            {
                var cEnd2 = toDate ?? DateTime.UtcNow;
                var cStart2 = fromDate ?? cEnd2.AddHours(-hoursBack);
                await RefreshProcStatsComparisonAsync(cStart2, cEnd2);
            }
            _queryStoreFilterMgr!.UpdateData(queryStoreTask.Result);
            SetDefaultSortIfNone(QueryStoreGrid, "TotalDurationMs", ListSortDirection.Descending);
            _ = LoadQueryStoreSlicerAsync();
            {
                var cEnd3 = toDate ?? DateTime.UtcNow;
                var cStart3 = fromDate ?? cEnd3.AddHours(-hoursBack);
                await RefreshQueryStoreComparisonAsync(cStart3, cEnd3);
            }
            _planCorrectionFilterMgr!.UpdateData(planCorrectionTask.Result);
            SetDefaultSortIfNone(PlanCorrectionGrid, "Score", ListSortDirection.Descending);

            UpdateQueryDurationTrendChart(queryDurationTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateProcDurationTrendChart(procDurationTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateQueryStoreDurationTrendChart(queryStoreDurationTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateExecutionCountTrendChart(executionCountTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateQueryHeatmapChart(heatmapTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshQueriesAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 0 — Overview (Correlated Timeline Lanes)</summary>
    private async System.Threading.Tasks.Task RefreshOverviewAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var comparison = GetComparisonRange();
            await CorrelatedLanes.RefreshAsync(hoursBack, fromDate, toDate, comparison);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshOverviewAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 4 — CPU</summary>
    private async System.Threading.Tasks.Task RefreshCpuAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var cpuTask = Task.Run(() => _dataService.GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate));
            await cpuTask;
            UpdateCpuChart(cpuTask.Result, hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshCpuAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 5 — Memory</summary>
    private async System.Threading.Tasks.Task RefreshMemoryAsync(int hoursBack, DateTime? fromDate, DateTime? toDate, bool subTabOnly = false)
    {
        try
        {
            if (subTabOnly)
            {
                /* Timer tick: only refresh the visible sub-tab (5 queries → 1-2) */
                switch (MemorySubTabControl.SelectedIndex)
                {
                    case 0: // Overview — memory stats + trend
                        var memStats = await Task.Run(() => _dataService.GetLatestMemoryStatsAsync(_serverId));
                        var memTrend = await Task.Run(() => _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        var memGrantTrend = await Task.Run(() => _dataService.GetMemoryGrantTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        UpdateMemorySummary(memStats);
                        UpdateMemoryChart(memTrend, memGrantTrend, hoursBack, fromDate, toDate);
                        break;
                    case 1: // Memory Clerks
                        var clerkTypes = await Task.Run(() => _dataService.GetDistinctMemoryClerkTypesAsync(_serverId, hoursBack, fromDate, toDate));
                        PopulateMemoryClerkPicker(clerkTypes);
                        await UpdateMemoryClerksChartFromPickerAsync();
                        break;
                    case 2: // Memory Grants
                        var grantChart = await Task.Run(() => _dataService.GetMemoryGrantChartDataAsync(_serverId, hoursBack, fromDate, toDate));
                        UpdateMemoryGrantCharts(grantChart, hoursBack, fromDate, toDate);
                        break;
                    case 3: // Memory Pressure Events
                        var pressureEvents = await Task.Run(() => _dataService.GetMemoryPressureEventsAsync(_serverId, hoursBack, fromDate, toDate));
                        UpdateMemoryPressureEventsChart(pressureEvents, hoursBack, fromDate, toDate);
                        break;
                }
                return;
            }

            /* Full refresh: load all sub-tabs */
            var memoryTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryStats", () => Task.Run(() => _dataService.GetLatestMemoryStatsAsync(_serverId)));
            var memoryTrendTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryTrend", () => Task.Run(() => _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryClerkTypesTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryClerks", () => Task.Run(() => _dataService.GetDistinctMemoryClerkTypesAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryGrantTrendTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryGrantTrend", () => Task.Run(() => _dataService.GetMemoryGrantTrendAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryGrantChartTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryGrants", () => Task.Run(() => _dataService.GetMemoryGrantChartDataAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryPressureEventsTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryPressureEvents", () => Task.Run(() => _dataService.GetMemoryPressureEventsAsync(_serverId, hoursBack, fromDate, toDate)));

            await System.Threading.Tasks.Task.WhenAll(memoryTask, memoryTrendTask, memoryClerkTypesTask, memoryGrantTrendTask, memoryGrantChartTask, memoryPressureEventsTask);

            UpdateMemorySummary(memoryTask.Result);
            UpdateMemoryChart(memoryTrendTask.Result, memoryGrantTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateMemoryGrantCharts(memoryGrantChartTask.Result, hoursBack, fromDate, toDate);
            UpdateMemoryPressureEventsChart(memoryPressureEventsTask.Result, hoursBack, fromDate, toDate);
            PopulateMemoryClerkPicker(memoryClerkTypesTask.Result);
            await UpdateMemoryClerksChartFromPickerAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshMemoryAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 6 — File I/O</summary>
    private async System.Threading.Tasks.Task RefreshFileIoAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var fileIoTrendTask = Helpers.MethodProfiler.TimeAsync("FileIo.LatencyTrend", () => Task.Run(() => _dataService.GetFileIoLatencyTrendAsync(_serverId, hoursBack, fromDate, toDate)));
            var fileIoThroughputTask = Helpers.MethodProfiler.TimeAsync("FileIo.ThroughputTrend", () => Task.Run(() => _dataService.GetFileIoThroughputTrendAsync(_serverId, hoursBack, fromDate, toDate)));

            await System.Threading.Tasks.Task.WhenAll(fileIoTrendTask, fileIoThroughputTask);

            UpdateFileIoCharts(fileIoTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateFileIoThroughputCharts(fileIoThroughputTask.Result, hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshFileIoAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 7 — TempDB</summary>
    private async System.Threading.Tasks.Task RefreshTempDbAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var tempDbTask = Helpers.MethodProfiler.TimeAsync("TempDb.Trend", () => Task.Run(() => _dataService.GetTempDbTrendAsync(_serverId, hoursBack, fromDate, toDate)));
            var tempDbFileIoTask = Helpers.MethodProfiler.TimeAsync("TempDb.FileIoTrend", () => Task.Run(() => _dataService.GetTempDbFileIoTrendAsync(_serverId, hoursBack, fromDate, toDate)));

            await System.Threading.Tasks.Task.WhenAll(tempDbTask, tempDbFileIoTask);

            UpdateTempDbChart(tempDbTask.Result, hoursBack, fromDate, toDate);
            UpdateTempDbSizeChart(tempDbTask.Result, hoursBack, fromDate, toDate);
            UpdateTempDbFileIoChart(tempDbFileIoTask.Result, hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshTempDbAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 8 — Blocking</summary>
    private async System.Threading.Tasks.Task RefreshBlockingAsync(int hoursBack, DateTime? fromDate, DateTime? toDate, bool subTabOnly = false)
    {
        try
        {
            if (subTabOnly)
            {
                /* Timer tick: only refresh the visible sub-tab (7 queries → 1-3) + lightweight alert counts */
                switch (BlockingSubTabControl.SelectedIndex)
                {
                    case 0: // Trends — 3 trend charts
                        var lwt = Helpers.MethodProfiler.TimeAsync("Locking.LockWaitTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLockWaitTrendAsync(_serverId, hoursBack, fromDate, toDate))));
                        var bt = Helpers.MethodProfiler.TimeAsync("Locking.BlockingTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
                        var dt = Helpers.MethodProfiler.TimeAsync("Locking.DeadlockTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate))));
                        await System.Threading.Tasks.Task.WhenAll(lwt, bt, dt);
                        UpdateLockWaitTrendChart(lwt.Result, hoursBack, fromDate, toDate);
                        UpdateBlockingTrendChart(bt.Result, hoursBack, fromDate, toDate);
                        UpdateDeadlockTrendChart(dt.Result, hoursBack, fromDate, toDate);
                        break;
                    case 1: // Current Waits — 2 charts
                        var cwd = Helpers.MethodProfiler.TimeAsync("Locking.WaitingTaskTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetWaitingTaskTrendAsync(_serverId, hoursBack, fromDate, toDate))));
                        var cwb = Helpers.MethodProfiler.TimeAsync("Locking.BlockedSessionTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetBlockedSessionTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
                        await System.Threading.Tasks.Task.WhenAll(cwd, cwb);
                        UpdateCurrentWaitsDurationChart(cwd.Result, hoursBack, fromDate, toDate);
                        UpdateCurrentWaitsBlockedChart(cwb.Result, hoursBack, fromDate, toDate);
                        break;
                    case 2: // Blocked Process Reports
                        var bpr = await Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
                        using (Helpers.MethodProfiler.StartTiming("Locking.BindBlockedGrid"))
                            _blockedProcessFilterMgr!.UpdateData(bpr);
                        await LoadBlockingSlicerAsync();
                        break;
                    case 3: // Deadlocks
                        var dlr = await Task.Run(() => _dataService.GetRecentDeadlocksAsync(_serverId, hoursBack, fromDate, toDate));
                        var dlrDetails = await ParseDeadlocksOffUiThreadAsync(dlr);
                        using (Helpers.MethodProfiler.StartTiming("Locking.BindDeadlockGrid"))
                            _deadlockFilterMgr!.UpdateData(dlrDetails);
                        await LoadDeadlockSlicerAsync();
                        break;
                    case 4: // Blocking Stats — blocking + deadlock severity (4 charts + summary strip)
                        var bdsStats = Helpers.MethodProfiler.TimeAsync("Locking.BlockingDurationStats", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetBlockingDurationStatsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
                        var bdsCount = Helpers.MethodProfiler.TimeAsync("Locking.DeadlockTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate))));
                        var bdsSeverity = Helpers.MethodProfiler.TimeAsync("Locking.DeadlockSeverityStats", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetDeadlockSeverityStatsAsync(_serverId, hoursBack, fromDate, toDate))));
                        await System.Threading.Tasks.Task.WhenAll(bdsStats, bdsCount, bdsSeverity);
                        UpdateBlockingDurationChart(bdsStats.Result, hoursBack, fromDate, toDate);
                        UpdateBlockingTotalDurationChart(bdsStats.Result, hoursBack, fromDate, toDate);
                        UpdateDeadlockWaitChart(bdsSeverity.Result, hoursBack, fromDate, toDate);
                        UpdateDeadlockTotalWaitChart(bdsSeverity.Result, hoursBack, fromDate, toDate);
                        UpdateBlockingStatsSummary(bdsStats.Result, bdsCount.Result, bdsSeverity.Result);
                        break;
                }
                /* Always keep alert badge current when Blocking tab is visible */
                await RefreshAlertCountsAsync();
                return;
            }

            /* Full refresh: load all sub-tabs */
            var blockedProcessTask = Helpers.MethodProfiler.TimeAsync("Locking.BlockedProcessReports", () => Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter)));
            var deadlockTask = Helpers.MethodProfiler.TimeAsync("Locking.Deadlocks", () => Task.Run(() => _dataService.GetRecentDeadlocksAsync(_serverId, hoursBack, fromDate, toDate)));
            var lockWaitTrendTask = Helpers.MethodProfiler.TimeAsync("Locking.LockWaitTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLockWaitTrendAsync(_serverId, hoursBack, fromDate, toDate))));
            var blockingTrendTask = Helpers.MethodProfiler.TimeAsync("Locking.BlockingTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var deadlockTrendTask = Helpers.MethodProfiler.TimeAsync("Locking.DeadlockTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate))));
            var currentWaitsDurationTask = Helpers.MethodProfiler.TimeAsync("Locking.WaitingTaskTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetWaitingTaskTrendAsync(_serverId, hoursBack, fromDate, toDate))));
            var currentWaitsBlockedTask = Helpers.MethodProfiler.TimeAsync("Locking.BlockedSessionTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetBlockedSessionTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var blockingDurationStatsTask = Helpers.MethodProfiler.TimeAsync("Locking.BlockingDurationStats", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetBlockingDurationStatsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var deadlockSeverityStatsTask = Helpers.MethodProfiler.TimeAsync("Locking.DeadlockSeverityStats", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetDeadlockSeverityStatsAsync(_serverId, hoursBack, fromDate, toDate))));

            await System.Threading.Tasks.Task.WhenAll(
                blockedProcessTask, deadlockTask,
                lockWaitTrendTask, blockingTrendTask, deadlockTrendTask,
                currentWaitsDurationTask, currentWaitsBlockedTask,
                blockingDurationStatsTask, deadlockSeverityStatsTask);

            /* Parse deadlock graphs off the UI thread (this was the Blocking-tab hitch). Time the
               remaining UI-thread render steps so any new hot spot is pinpointed (bind vs charts). */
            var deadlockDetails = await ParseDeadlocksOffUiThreadAsync(deadlockTask.Result);
            using (Helpers.MethodProfiler.StartTiming("Locking.BindBlockedGrid"))
                _blockedProcessFilterMgr!.UpdateData(blockedProcessTask.Result);
            using (Helpers.MethodProfiler.StartTiming("Locking.BindDeadlockGrid"))
                _deadlockFilterMgr!.UpdateData(deadlockDetails);

            using (Helpers.MethodProfiler.StartTiming("Locking.RenderTrendCharts"))
            {
                UpdateLockWaitTrendChart(lockWaitTrendTask.Result, hoursBack, fromDate, toDate);
                UpdateBlockingTrendChart(blockingTrendTask.Result, hoursBack, fromDate, toDate);
                UpdateDeadlockTrendChart(deadlockTrendTask.Result, hoursBack, fromDate, toDate);
                UpdateCurrentWaitsDurationChart(currentWaitsDurationTask.Result, hoursBack, fromDate, toDate);
                UpdateCurrentWaitsBlockedChart(currentWaitsBlockedTask.Result, hoursBack, fromDate, toDate);
                /* Blocking Stats severity sub-tab (4 charts + summary strip): the block-duration aggregate
                   reconciles with the blocking-incident trend (same XE→DMV source), the deadlock severity with
                   the deadlock count (same v_deadlocks window). */
                UpdateBlockingDurationChart(blockingDurationStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateBlockingTotalDurationChart(blockingDurationStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateDeadlockWaitChart(deadlockSeverityStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateDeadlockTotalWaitChart(deadlockSeverityStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateBlockingStatsSummary(blockingDurationStatsTask.Result, deadlockTrendTask.Result, deadlockSeverityStatsTask.Result);
            }

            await LoadBlockingSlicerAsync();
            await LoadDeadlockSlicerAsync();

            /* Notify parent of alert counts for tab badge */
            var blockingCount = blockedProcessTask.Result.Count;
            var deadlockCount = deadlockTask.Result.Count;
            DateTime? latestEventTime = null;
            if (blockingCount > 0 || deadlockCount > 0)
            {
                var latestBlocking = blockedProcessTask.Result.Max(r => (DateTime?)r.EventTime);
                var latestDeadlock = deadlockTask.Result.Max(r => (DateTime?)r.DeadlockTime);
                latestEventTime = latestBlocking > latestDeadlock ? latestBlocking : latestDeadlock;
            }
            AlertCountsChanged?.Invoke(blockingCount, deadlockCount, latestEventTime);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshBlockingAsync failed: {ex.Message}");
        }
    }

    // ── Blocking Slicer ──

    private string _blockingSlicerMetric = "Events";
    private List<TimeSliceBucket>? _blockingSlicerData;

    private async System.Threading.Tasks.Task LoadBlockingSlicerAsync()
    {
        try
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

            var data = await Task.Run(() => _dataService.GetBlockingSlicerDataAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
            _blockingSlicerData = data;
            _blockingSlicerMetric = "Events";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
            if (data.Count > 0)
                BlockingSlicer.LoadData(data, "Blocking Events", slicerStart, slicerEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadBlockingSlicerAsync failed: {ex.Message}");
        }
    }

    // ── Deadlock Slicer ──

    private List<TimeSliceBucket>? _deadlockSlicerData;

    private async System.Threading.Tasks.Task LoadDeadlockSlicerAsync()
    {
        try
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

            var data = await Task.Run(() => _dataService.GetDeadlockSlicerDataAsync(_serverId, hoursBack, fromDate, toDate));
            _deadlockSlicerData = data;
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
            if (data.Count > 0)
                DeadlockSlicer.LoadData(data, "Deadlocks", slicerStart, slicerEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadDeadlockSlicerAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 9 — Perfmon</summary>
    private async System.Threading.Tasks.Task RefreshPerfmonAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var perfmonCountersTask = Task.Run(() => _dataService.GetDistinctPerfmonCountersAsync(_serverId, hoursBack, fromDate, toDate));
            await perfmonCountersTask;
            PopulatePerfmonPicker(perfmonCountersTask.Result);
            await UpdatePerfmonChartFromPickerAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshPerfmonAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 10 — Running Jobs</summary>
    private async System.Threading.Tasks.Task RefreshRunningJobsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var runningJobsTask = Task.Run(() => SafeQueryAsync(() => _dataService.GetRunningJobsAsync(_serverId)));
            await runningJobsTask;
            _runningJobsFilterMgr!.UpdateData(runningJobsTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshRunningJobsAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 11 — Configuration</summary>
    private async System.Threading.Tasks.Task RefreshConfigurationAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var serverConfigTask = Helpers.MethodProfiler.TimeAsync("Config.ServerConfig", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatestServerConfigAsync(_serverId))));
            var databaseConfigTask = Helpers.MethodProfiler.TimeAsync("Config.DatabaseConfig", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatestDatabaseConfigAsync(_serverId, SelectedDatabaseFilter))));
            var databaseScopedConfigTask = Helpers.MethodProfiler.TimeAsync("Config.DatabaseScopedConfig", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatestDatabaseScopedConfigAsync(_serverId, SelectedDatabaseFilter))));
            var queryStoreHealthTask = Helpers.MethodProfiler.TimeAsync("Config.QueryStoreHealth", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatestQueryStoreHealthAsync(_serverId, SelectedDatabaseFilter))));
            var automaticTuningTask = Helpers.MethodProfiler.TimeAsync("Config.AutomaticTuning", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatestAutomaticTuningAsync(_serverId, SelectedDatabaseFilter))));
            var traceFlagsTask = Helpers.MethodProfiler.TimeAsync("Config.TraceFlags", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatestTraceFlagsAsync(_serverId))));

            await System.Threading.Tasks.Task.WhenAll(serverConfigTask, databaseConfigTask, databaseScopedConfigTask, queryStoreHealthTask, automaticTuningTask, traceFlagsTask);

            _serverConfigFilterMgr!.UpdateData(serverConfigTask.Result);
            _databaseConfigFilterMgr!.UpdateData(databaseConfigTask.Result);
            _dbScopedConfigFilterMgr!.UpdateData(databaseScopedConfigTask.Result);
            _queryStoreHealthFilterMgr!.UpdateData(queryStoreHealthTask.Result);
            _automaticTuningFilterMgr!.UpdateData(automaticTuningTask.Result);
            _traceFlagsFilterMgr!.UpdateData(traceFlagsTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshConfigurationAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 12 — Daily Summary (Performance Calendar month heatmap).</summary>
    private async System.Threading.Tasks.Task RefreshDailySummaryAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        await LoadCalendarMonthAsync(DailyCalendar.DisplayMonth);
    }

    /// <summary>Tab 17 — Collection Health</summary>
    private async System.Threading.Tasks.Task RefreshCollectionHealthAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var collectionHealthTask = Helpers.MethodProfiler.TimeAsync("CollectionHealth.Health", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetCollectionHealthAsync(_serverId))));
            var collectionLogTask = Helpers.MethodProfiler.TimeAsync("CollectionHealth.Log", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetRecentCollectionLogAsync(_serverId, hoursBack, fromDate, toDate))));

            await System.Threading.Tasks.Task.WhenAll(collectionHealthTask, collectionLogTask);

            _collectionHealthFilterMgr!.UpdateData(collectionHealthTask.Result);
            _collectionLogFilterMgr!.UpdateData(collectionLogTask.Result);
            UpdateCollectorDurationChart(collectionLogTask.Result, hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshCollectionHealthAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Wraps a query in a try/catch so it returns an empty list on failure instead of faulting.
    /// </summary>
    private static async Task<List<T>> SafeQueryAsync<T>(Func<Task<List<T>>> query)
    {
        try
        {
            return await query();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"Trend query failed: {ex.Message}");
            return new List<T>();
        }
    }
}

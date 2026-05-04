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

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>
    /// Public entry point to trigger a data refresh from outside.
    /// Loads only the visible tab — other tabs load on demand when clicked.
    /// </summary>
    public async void RefreshData()
    {
        await RefreshAllDataAsync(fullRefresh: false);
    }

    private async System.Threading.Tasks.Task RefreshAllDataAsync(bool fullRefresh = false)
    {
        if (_isRefreshing) return;
        _isRefreshing = true;

        var hoursBack = GetHoursBack();

        /* Get custom date range if selected, converting local picker dates/times to server time */
        DateTime? fromDate = null;
        DateTime? toDate = null;
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

        try
        {
            using var _profiler = Helpers.MethodProfiler.StartTiming($"ServerTab-{_server?.DisplayName}");

            if (fullRefresh)
            {
                await RefreshAllTabsAsync(hoursBack, fromDate, toDate);
            }
            else
            {
                await RefreshVisibleTabAsync(hoursBack, fromDate, toDate, subTabOnly: true);
                /* Always keep alert badge current even when Blocking tab is not visible */
                if (MainTabControl.SelectedIndex != 8)
                    await RefreshAlertCountsAsync(hoursBack, fromDate, toDate);
            }

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
            case 13: await RefreshCollectionHealthAsync(hoursBack, fromDate, toDate); break;
        }
    }

    /// <summary>
    /// Lightweight alert-only refresh — fetches blocking + deadlock counts and fires AlertCountsChanged.
    /// Runs on every timer tick when the Blocking tab is NOT visible so the tab badge stays current.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshAlertCountsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var (blockingCount, deadlockCount, latestEventTime) = await _dataService.GetAlertCountsAsync(_serverId, hoursBack, fromDate, toDate);
            AlertCountsChanged?.Invoke(blockingCount, deadlockCount, latestEventTime);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshAlertCountsAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Full refresh of all tabs — used for first load, manual refresh, and time range changes.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshAllTabsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var loadSw = Stopwatch.StartNew();

        /* Load all tabs in parallel */
        var snapshotsTask = _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate);
        var cpuTask = _dataService.GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate);
        var memoryTask = _dataService.GetLatestMemoryStatsAsync(_serverId);
        var memoryTrendTask = _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate);
        var queryStatsTask = _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes);
        var procStatsTask = _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes);
        var fileIoTrendTask = _dataService.GetFileIoLatencyTrendAsync(_serverId, hoursBack, fromDate, toDate);
        var fileIoThroughputTask = _dataService.GetFileIoThroughputTrendAsync(_serverId, hoursBack, fromDate, toDate);
        var tempDbTask = _dataService.GetTempDbTrendAsync(_serverId, hoursBack, fromDate, toDate);
        var tempDbFileIoTask = _dataService.GetTempDbFileIoTrendAsync(_serverId, hoursBack, fromDate, toDate);
        var deadlockTask = _dataService.GetRecentDeadlocksAsync(_serverId, hoursBack, fromDate, toDate);
        var blockedProcessTask = _dataService.GetRecentBlockedProcessReportsAsync(_serverId, hoursBack, fromDate, toDate);
        var waitTypesTask = _dataService.GetDistinctWaitTypesAsync(_serverId, hoursBack, fromDate, toDate);
        var memoryClerkTypesTask = _dataService.GetDistinctMemoryClerkTypesAsync(_serverId, hoursBack, fromDate, toDate);
        var perfmonCountersTask = _dataService.GetDistinctPerfmonCountersAsync(_serverId, hoursBack, fromDate, toDate);
        var queryStoreTask = _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate);
        var memoryGrantTrendTask = _dataService.GetMemoryGrantTrendAsync(_serverId, hoursBack, fromDate, toDate);
        var memoryGrantChartTask = _dataService.GetMemoryGrantChartDataAsync(_serverId, hoursBack, fromDate, toDate);
        var memoryPressureEventsTask = _dataService.GetMemoryPressureEventsAsync(_serverId, hoursBack, fromDate, toDate);
        var serverConfigTask = SafeQueryAsync(() => _dataService.GetLatestServerConfigAsync(_serverId));
        var databaseConfigTask = SafeQueryAsync(() => _dataService.GetLatestDatabaseConfigAsync(_serverId));
        var databaseScopedConfigTask = SafeQueryAsync(() => _dataService.GetLatestDatabaseScopedConfigAsync(_serverId));
        var traceFlagsTask = SafeQueryAsync(() => _dataService.GetLatestTraceFlagsAsync(_serverId));
        var runningJobsTask = SafeQueryAsync(() => _dataService.GetRunningJobsAsync(_serverId));
        var collectionHealthTask = SafeQueryAsync(() => _dataService.GetCollectionHealthAsync(_serverId));
        var collectionLogTask = SafeQueryAsync(() => _dataService.GetRecentCollectionLogAsync(_serverId, hoursBack));
        var dailySummaryTask = _dataService.GetDailySummaryAsync(_serverId, _dailySummaryDate);
        /* Core data tasks */
        await System.Threading.Tasks.Task.WhenAll(
            snapshotsTask, cpuTask, memoryTask, memoryTrendTask,
            queryStatsTask, procStatsTask, fileIoTrendTask, fileIoThroughputTask, tempDbTask, tempDbFileIoTask,
            deadlockTask, blockedProcessTask, waitTypesTask, memoryClerkTypesTask, perfmonCountersTask,
            queryStoreTask, memoryGrantTrendTask, memoryGrantChartTask, memoryPressureEventsTask,
            serverConfigTask, databaseConfigTask, databaseScopedConfigTask, traceFlagsTask,
            runningJobsTask, collectionHealthTask, collectionLogTask, dailySummaryTask);

        /* Trend chart tasks - run separately so failures don't kill the whole refresh */
        var lockWaitTrendTask = SafeQueryAsync(() => _dataService.GetLockWaitTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var blockingTrendTask = SafeQueryAsync(() => _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var deadlockTrendTask = SafeQueryAsync(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var queryDurationTrendTask = SafeQueryAsync(() => _dataService.GetQueryDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var procDurationTrendTask = SafeQueryAsync(() => _dataService.GetProcedureDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var queryStoreDurationTrendTask = SafeQueryAsync(() => _dataService.GetQueryStoreDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var executionCountTrendTask = SafeQueryAsync(() => _dataService.GetExecutionCountTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var currentWaitsDurationTask = SafeQueryAsync(() => _dataService.GetWaitingTaskTrendAsync(_serverId, hoursBack, fromDate, toDate));
        var currentWaitsBlockedTask = SafeQueryAsync(() => _dataService.GetBlockedSessionTrendAsync(_serverId, hoursBack, fromDate, toDate));

        await System.Threading.Tasks.Task.WhenAll(
            lockWaitTrendTask, blockingTrendTask, deadlockTrendTask,
            queryDurationTrendTask, procDurationTrendTask, queryStoreDurationTrendTask, executionCountTrendTask,
            currentWaitsDurationTask, currentWaitsBlockedTask);

        loadSw.Stop();

        /* Log data counts and timing for diagnostics */
        AppLogger.DataDiag("ServerTab", $"[{_server.DisplayName}] serverId={_serverId} hoursBack={hoursBack} dataLoad={loadSw.ElapsedMilliseconds}ms");
        AppLogger.DataDiag("ServerTab", $"  Snapshots: {snapshotsTask.Result.Count}, CPU: {cpuTask.Result.Count}");
        AppLogger.DataDiag("ServerTab", $"  Memory: {(memoryTask.Result != null ? "1" : "null")}, MemoryTrend: {memoryTrendTask.Result.Count}");
        AppLogger.DataDiag("ServerTab", $"  QueryStats: {queryStatsTask.Result.Count}, ProcStats: {procStatsTask.Result.Count}");
        AppLogger.DataDiag("ServerTab", $"  FileIoTrend: {fileIoTrendTask.Result.Count}");
        AppLogger.DataDiag("ServerTab", $"  TempDb: {tempDbTask.Result.Count}, BlockedProcessReports: {blockedProcessTask.Result.Count}, Deadlocks: {deadlockTask.Result.Count}");
        AppLogger.DataDiag("ServerTab", $"  WaitTypes: {waitTypesTask.Result.Count}, PerfmonCounters: {perfmonCountersTask.Result.Count}, QueryStore: {queryStoreTask.Result.Count}");

        /* Update grids (via filter managers to preserve active filters) */
        _querySnapshotsFilterMgr!.UpdateData(snapshotsTask.Result);
        LiveSnapshotIndicator.Text = "";
        _queryStatsFilterMgr!.UpdateData(queryStatsTask.Result);
        SetDefaultSortIfNone(QueryStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
        {
            var cEnd = toDate ?? DateTime.UtcNow;
            var cStart = fromDate ?? cEnd.AddHours(-hoursBack);
            await RefreshQueryStatsComparisonAsync(cStart, cEnd);
        }
        _procStatsFilterMgr!.UpdateData(procStatsTask.Result);
        SetDefaultSortIfNone(ProcedureStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
        {
            var cEnd2 = toDate ?? DateTime.UtcNow;
            var cStart2 = fromDate ?? cEnd2.AddHours(-hoursBack);
            await RefreshProcStatsComparisonAsync(cStart2, cEnd2);
        }
        _blockedProcessFilterMgr!.UpdateData(blockedProcessTask.Result);
        _deadlockFilterMgr!.UpdateData(DeadlockProcessDetail.ParseFromRows(deadlockTask.Result));
        _queryStoreFilterMgr!.UpdateData(queryStoreTask.Result);
        SetDefaultSortIfNone(QueryStoreGrid, "TotalDurationMs", ListSortDirection.Descending);
        {
            var cEnd3 = toDate ?? DateTime.UtcNow;
            var cStart3 = fromDate ?? cEnd3.AddHours(-hoursBack);
            await RefreshQueryStoreComparisonAsync(cStart3, cEnd3);
        }
        _serverConfigFilterMgr!.UpdateData(serverConfigTask.Result);
        _databaseConfigFilterMgr!.UpdateData(databaseConfigTask.Result);
        _dbScopedConfigFilterMgr!.UpdateData(databaseScopedConfigTask.Result);
        _traceFlagsFilterMgr!.UpdateData(traceFlagsTask.Result);
        _runningJobsFilterMgr!.UpdateData(runningJobsTask.Result);
        _collectionHealthFilterMgr!.UpdateData(collectionHealthTask.Result);
        _collectionLogFilterMgr!.UpdateData(collectionLogTask.Result);
        var dailySummary = await dailySummaryTask;
        DailySummaryGrid.ItemsSource = dailySummary != null
            ? new List<DailySummaryRow> { dailySummary } : null;
        DailySummaryNoData.Visibility = dailySummary == null
            ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        UpdateCollectorDurationChart(collectionLogTask.Result);

        /* Update memory summary */
        UpdateMemorySummary(memoryTask.Result);

        /* Update charts */
        UpdateCpuChart(cpuTask.Result);
        UpdateMemoryChart(memoryTrendTask.Result, memoryGrantTrendTask.Result);
        UpdateTempDbChart(tempDbTask.Result);
        UpdateTempDbFileIoChart(tempDbFileIoTask.Result);
        UpdateFileIoCharts(fileIoTrendTask.Result);
        UpdateFileIoThroughputCharts(fileIoThroughputTask.Result);
        UpdateLockWaitTrendChart(lockWaitTrendTask.Result, hoursBack, fromDate, toDate);
        UpdateBlockingTrendChart(blockingTrendTask.Result, hoursBack, fromDate, toDate);
        UpdateDeadlockTrendChart(deadlockTrendTask.Result, hoursBack, fromDate, toDate);
        UpdateCurrentWaitsDurationChart(currentWaitsDurationTask.Result, hoursBack, fromDate, toDate);
        UpdateCurrentWaitsBlockedChart(currentWaitsBlockedTask.Result, hoursBack, fromDate, toDate);
        UpdateQueryDurationTrendChart(queryDurationTrendTask.Result);
        UpdateProcDurationTrendChart(procDurationTrendTask.Result);
        UpdateQueryStoreDurationTrendChart(queryStoreDurationTrendTask.Result);
        UpdateExecutionCountTrendChart(executionCountTrendTask.Result);
        UpdateMemoryGrantCharts(memoryGrantChartTask.Result);
        UpdateMemoryPressureEventsChart(memoryPressureEventsTask.Result, hoursBack, fromDate, toDate);

        /* Populate pickers (preserve selections) */
        PopulateWaitTypePicker(waitTypesTask.Result);
        PopulateMemoryClerkPicker(memoryClerkTypesTask.Result);
        PopulatePerfmonPicker(perfmonCountersTask.Result);

        /* Update picker-driven charts */
        await UpdateWaitStatsChartFromPickerAsync();
        await UpdateMemoryClerksChartFromPickerAsync();
        await UpdatePerfmonChartFromPickerAsync();

        /* Notify parent of alert counts for tab badge.
           Include the latest event timestamp so acknowledgement is only
           cleared when genuinely new events arrive, not when the time range changes. */
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

    /* ───────────────────────────── Per-tab refresh methods ───────────────────────────── */

    /// <summary>Tab 0 — Wait Stats</summary>
    private async System.Threading.Tasks.Task RefreshWaitStatsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var waitTypesTask = _dataService.GetDistinctWaitTypesAsync(_serverId, hoursBack, fromDate, toDate);
            await waitTypesTask;
            PopulateWaitTypePicker(waitTypesTask.Result);
            await UpdateWaitStatsChartFromPickerAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshWaitStatsAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 1 — Queries</summary>
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
                        var qdt = SafeQueryAsync(() => _dataService.GetQueryDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        var pdt = SafeQueryAsync(() => _dataService.GetProcedureDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        var qsdt = SafeQueryAsync(() => _dataService.GetQueryStoreDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        var ect = SafeQueryAsync(() => _dataService.GetExecutionCountTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        await System.Threading.Tasks.Task.WhenAll(qdt, pdt, qsdt, ect);
                        UpdateQueryDurationTrendChart(qdt.Result);
                        UpdateProcDurationTrendChart(pdt.Result);
                        UpdateQueryStoreDurationTrendChart(qsdt.Result);
                        UpdateExecutionCountTrendChart(ect.Result);
                        break;
                    case 1: // Active Queries
                        var snapshots = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate);
                        _querySnapshotsFilterMgr!.UpdateData(snapshots);
                        LiveSnapshotIndicator.Text = "";
                        _ = LoadActiveQueriesSlicerAsync();
                        break;
                    case 2: // Top Queries by Duration
                        var queryStats = await _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes);
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
                        var procStats = await _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes);
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
                        var qsData = await _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate);
                        _queryStoreFilterMgr!.UpdateData(qsData);
                        SetDefaultSortIfNone(QueryStoreGrid, "TotalDurationMs", ListSortDirection.Descending);
                        _ = LoadQueryStoreSlicerAsync();
                        {
                            var cEnd = toDate ?? DateTime.UtcNow;
                            var cStart = fromDate ?? cEnd.AddHours(-hoursBack);
                            await RefreshQueryStoreComparisonAsync(cStart, cEnd);
                        }
                        break;
                    case 5: // Query Heatmap
                        var hmMetric = (HeatmapMetric)HeatmapMetricCombo.SelectedIndex;
                        var hmData = await _dataService.GetQueryHeatmapAsync(_serverId, hmMetric, hoursBack, fromDate, toDate);
                        AppLogger.Info("ServerTab", $"[{_server.DisplayName}] Heatmap: {hmData.TimeBuckets.Length} time buckets, {hmData.Intensities.GetLength(0)}x{hmData.Intensities.GetLength(1)} grid");
                        UpdateQueryHeatmapChart(hmData);
                        break;
                }
                return;
            }

            /* Full refresh: load all sub-tabs */
            var snapshotsTask = _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate);
            var queryStatsTask = _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes);
            var procStatsTask = _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, UtcOffsetMinutes);
            var queryStoreTask = _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate);
            var queryDurationTrendTask = SafeQueryAsync(() => _dataService.GetQueryDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var procDurationTrendTask = SafeQueryAsync(() => _dataService.GetProcedureDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var queryStoreDurationTrendTask = SafeQueryAsync(() => _dataService.GetQueryStoreDurationTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var executionCountTrendTask = SafeQueryAsync(() => _dataService.GetExecutionCountTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var heatmapTask = Task.Run(async () =>
            {
                try { return await _dataService.GetQueryHeatmapAsync(_serverId, (HeatmapMetric)Dispatcher.Invoke(() => HeatmapMetricCombo.SelectedIndex), hoursBack, fromDate, toDate); }
                catch { return new HeatmapResult(); }
            });

            await System.Threading.Tasks.Task.WhenAll(
                snapshotsTask, queryStatsTask, procStatsTask, queryStoreTask,
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

            UpdateQueryDurationTrendChart(queryDurationTrendTask.Result);
            UpdateProcDurationTrendChart(procDurationTrendTask.Result);
            UpdateQueryStoreDurationTrendChart(queryStoreDurationTrendTask.Result);
            UpdateExecutionCountTrendChart(executionCountTrendTask.Result);
            UpdateQueryHeatmapChart(heatmapTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshQueriesAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 3 — CPU</summary>
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

    private async System.Threading.Tasks.Task RefreshCpuAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var cpuTask = _dataService.GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate);
            await cpuTask;
            UpdateCpuChart(cpuTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshCpuAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 4 — Memory</summary>
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
                        var memStats = await _dataService.GetLatestMemoryStatsAsync(_serverId);
                        var memTrend = await _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate);
                        var memGrantTrend = await _dataService.GetMemoryGrantTrendAsync(_serverId, hoursBack, fromDate, toDate);
                        UpdateMemorySummary(memStats);
                        UpdateMemoryChart(memTrend, memGrantTrend);
                        break;
                    case 1: // Memory Clerks
                        var clerkTypes = await _dataService.GetDistinctMemoryClerkTypesAsync(_serverId, hoursBack, fromDate, toDate);
                        PopulateMemoryClerkPicker(clerkTypes);
                        await UpdateMemoryClerksChartFromPickerAsync();
                        break;
                    case 2: // Memory Grants
                        var grantChart = await _dataService.GetMemoryGrantChartDataAsync(_serverId, hoursBack, fromDate, toDate);
                        UpdateMemoryGrantCharts(grantChart);
                        break;
                    case 3: // Memory Pressure Events
                        var pressureEvents = await _dataService.GetMemoryPressureEventsAsync(_serverId, hoursBack, fromDate, toDate);
                        UpdateMemoryPressureEventsChart(pressureEvents, hoursBack, fromDate, toDate);
                        break;
                }
                return;
            }

            /* Full refresh: load all sub-tabs */
            var memoryTask = _dataService.GetLatestMemoryStatsAsync(_serverId);
            var memoryTrendTask = _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var memoryClerkTypesTask = _dataService.GetDistinctMemoryClerkTypesAsync(_serverId, hoursBack, fromDate, toDate);
            var memoryGrantTrendTask = _dataService.GetMemoryGrantTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var memoryGrantChartTask = _dataService.GetMemoryGrantChartDataAsync(_serverId, hoursBack, fromDate, toDate);
            var memoryPressureEventsTask = _dataService.GetMemoryPressureEventsAsync(_serverId, hoursBack, fromDate, toDate);

            await System.Threading.Tasks.Task.WhenAll(memoryTask, memoryTrendTask, memoryClerkTypesTask, memoryGrantTrendTask, memoryGrantChartTask, memoryPressureEventsTask);

            UpdateMemorySummary(memoryTask.Result);
            UpdateMemoryChart(memoryTrendTask.Result, memoryGrantTrendTask.Result);
            UpdateMemoryGrantCharts(memoryGrantChartTask.Result);
            UpdateMemoryPressureEventsChart(memoryPressureEventsTask.Result, hoursBack, fromDate, toDate);
            PopulateMemoryClerkPicker(memoryClerkTypesTask.Result);
            await UpdateMemoryClerksChartFromPickerAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshMemoryAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 5 — File I/O</summary>
    private async System.Threading.Tasks.Task RefreshFileIoAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var fileIoTrendTask = _dataService.GetFileIoLatencyTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var fileIoThroughputTask = _dataService.GetFileIoThroughputTrendAsync(_serverId, hoursBack, fromDate, toDate);

            await System.Threading.Tasks.Task.WhenAll(fileIoTrendTask, fileIoThroughputTask);

            UpdateFileIoCharts(fileIoTrendTask.Result);
            UpdateFileIoThroughputCharts(fileIoThroughputTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshFileIoAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 6 — TempDB</summary>
    private async System.Threading.Tasks.Task RefreshTempDbAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var tempDbTask = _dataService.GetTempDbTrendAsync(_serverId, hoursBack, fromDate, toDate);
            var tempDbFileIoTask = _dataService.GetTempDbFileIoTrendAsync(_serverId, hoursBack, fromDate, toDate);

            await System.Threading.Tasks.Task.WhenAll(tempDbTask, tempDbFileIoTask);

            UpdateTempDbChart(tempDbTask.Result);
            UpdateTempDbFileIoChart(tempDbFileIoTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshTempDbAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 7 — Blocking</summary>
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
                        var lwt = SafeQueryAsync(() => _dataService.GetLockWaitTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        var bt = SafeQueryAsync(() => _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        var dt = SafeQueryAsync(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        await System.Threading.Tasks.Task.WhenAll(lwt, bt, dt);
                        UpdateLockWaitTrendChart(lwt.Result, hoursBack, fromDate, toDate);
                        UpdateBlockingTrendChart(bt.Result, hoursBack, fromDate, toDate);
                        UpdateDeadlockTrendChart(dt.Result, hoursBack, fromDate, toDate);
                        break;
                    case 1: // Current Waits — 2 charts
                        var cwd = SafeQueryAsync(() => _dataService.GetWaitingTaskTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        var cwb = SafeQueryAsync(() => _dataService.GetBlockedSessionTrendAsync(_serverId, hoursBack, fromDate, toDate));
                        await System.Threading.Tasks.Task.WhenAll(cwd, cwb);
                        UpdateCurrentWaitsDurationChart(cwd.Result, hoursBack, fromDate, toDate);
                        UpdateCurrentWaitsBlockedChart(cwb.Result, hoursBack, fromDate, toDate);
                        break;
                    case 2: // Blocked Process Reports
                        var bpr = await _dataService.GetRecentBlockedProcessReportsAsync(_serverId, hoursBack, fromDate, toDate);
                        _blockedProcessFilterMgr!.UpdateData(bpr);
                        await LoadBlockingSlicerAsync();
                        break;
                    case 3: // Deadlocks
                        var dlr = await _dataService.GetRecentDeadlocksAsync(_serverId, hoursBack, fromDate, toDate);
                        _deadlockFilterMgr!.UpdateData(DeadlockProcessDetail.ParseFromRows(dlr));
                        await LoadDeadlockSlicerAsync();
                        break;
                }
                /* Always keep alert badge current when Blocking tab is visible */
                await RefreshAlertCountsAsync(hoursBack, fromDate, toDate);
                return;
            }

            /* Full refresh: load all sub-tabs */
            var blockedProcessTask = _dataService.GetRecentBlockedProcessReportsAsync(_serverId, hoursBack, fromDate, toDate);
            var deadlockTask = _dataService.GetRecentDeadlocksAsync(_serverId, hoursBack, fromDate, toDate);
            var lockWaitTrendTask = SafeQueryAsync(() => _dataService.GetLockWaitTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var blockingTrendTask = SafeQueryAsync(() => _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var deadlockTrendTask = SafeQueryAsync(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var currentWaitsDurationTask = SafeQueryAsync(() => _dataService.GetWaitingTaskTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var currentWaitsBlockedTask = SafeQueryAsync(() => _dataService.GetBlockedSessionTrendAsync(_serverId, hoursBack, fromDate, toDate));

            await System.Threading.Tasks.Task.WhenAll(
                blockedProcessTask, deadlockTask,
                lockWaitTrendTask, blockingTrendTask, deadlockTrendTask,
                currentWaitsDurationTask, currentWaitsBlockedTask);

            _blockedProcessFilterMgr!.UpdateData(blockedProcessTask.Result);
            _deadlockFilterMgr!.UpdateData(DeadlockProcessDetail.ParseFromRows(deadlockTask.Result));

            UpdateLockWaitTrendChart(lockWaitTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateBlockingTrendChart(blockingTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateDeadlockTrendChart(deadlockTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateCurrentWaitsDurationChart(currentWaitsDurationTask.Result, hoursBack, fromDate, toDate);
            UpdateCurrentWaitsBlockedChart(currentWaitsBlockedTask.Result, hoursBack, fromDate, toDate);

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
    private List<Models.TimeSliceBucket>? _blockingSlicerData;

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

            var data = await _dataService.GetBlockingSlicerDataAsync(_serverId, hoursBack, fromDate, toDate);
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

    private List<Models.TimeSliceBucket>? _deadlockSlicerData;

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

            var data = await _dataService.GetDeadlockSlicerDataAsync(_serverId, hoursBack, fromDate, toDate);
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

    /// <summary>Tab 8 — Perfmon</summary>
    private async System.Threading.Tasks.Task RefreshPerfmonAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var perfmonCountersTask = _dataService.GetDistinctPerfmonCountersAsync(_serverId, hoursBack, fromDate, toDate);
            await perfmonCountersTask;
            PopulatePerfmonPicker(perfmonCountersTask.Result);
            await UpdatePerfmonChartFromPickerAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshPerfmonAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 9 — Running Jobs</summary>
    private async System.Threading.Tasks.Task RefreshRunningJobsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var runningJobsTask = SafeQueryAsync(() => _dataService.GetRunningJobsAsync(_serverId));
            await runningJobsTask;
            _runningJobsFilterMgr!.UpdateData(runningJobsTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshRunningJobsAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 10 — Configuration</summary>
    private async System.Threading.Tasks.Task RefreshConfigurationAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var serverConfigTask = SafeQueryAsync(() => _dataService.GetLatestServerConfigAsync(_serverId));
            var databaseConfigTask = SafeQueryAsync(() => _dataService.GetLatestDatabaseConfigAsync(_serverId));
            var databaseScopedConfigTask = SafeQueryAsync(() => _dataService.GetLatestDatabaseScopedConfigAsync(_serverId));
            var traceFlagsTask = SafeQueryAsync(() => _dataService.GetLatestTraceFlagsAsync(_serverId));

            await System.Threading.Tasks.Task.WhenAll(serverConfigTask, databaseConfigTask, databaseScopedConfigTask, traceFlagsTask);

            _serverConfigFilterMgr!.UpdateData(serverConfigTask.Result);
            _databaseConfigFilterMgr!.UpdateData(databaseConfigTask.Result);
            _dbScopedConfigFilterMgr!.UpdateData(databaseScopedConfigTask.Result);
            _traceFlagsFilterMgr!.UpdateData(traceFlagsTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshConfigurationAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 11 — Daily Summary</summary>
    private async System.Threading.Tasks.Task RefreshDailySummaryAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var dailySummaryTask = _dataService.GetDailySummaryAsync(_serverId, _dailySummaryDate);
            var dailySummary = await dailySummaryTask;
            DailySummaryGrid.ItemsSource = dailySummary != null
                ? new List<DailySummaryRow> { dailySummary } : null;
            DailySummaryNoData.Visibility = dailySummary == null
                ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshDailySummaryAsync failed: {ex.Message}");
        }
    }

    /// <summary>Tab 12 — Collection Health</summary>
    private async System.Threading.Tasks.Task RefreshCollectionHealthAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var collectionHealthTask = SafeQueryAsync(() => _dataService.GetCollectionHealthAsync(_serverId));
            var collectionLogTask = SafeQueryAsync(() => _dataService.GetRecentCollectionLogAsync(_serverId, hoursBack));

            await System.Threading.Tasks.Task.WhenAll(collectionHealthTask, collectionLogTask);

            _collectionHealthFilterMgr!.UpdateData(collectionHealthTask.Result);
            _collectionLogFilterMgr!.UpdateData(collectionLogTask.Result);
            UpdateCollectorDurationChart(collectionLogTask.Result);
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

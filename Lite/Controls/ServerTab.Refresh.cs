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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using CollectorEngineCapability = PerformanceMonitor.Collectors.CollectorEngineCapability;

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
    /// The current toolbar window as (hoursBack, fromUtc, toUtc) -- the single derivation shared by the data refresh and
    /// the per-chart Revert / double-click axis re-pin, so both read the same window. A preset leaves from/to null
    /// (charts fall back to now - hoursBack); a custom range is the pair of naive-UTC instants the tab holds
    /// (#4766), handed to every read as it is: no clock takes part, so the read, the slicer and the alert badge all
    /// see the same two instants whichever display zone the pickers show.
    /// </summary>
    private (int hoursBack, DateTime? fromUtc, DateTime? toUtc) GetCurrentWindowUtc()
    {
        /* A range the pickers show but the tab has not yet held (their defaults, filled before any edit) is taken from them once. */
        if (IsCustomRange && !_customRange.IsCustom)
        {
            CaptureCustomRangeEdit(null);
        }

        return CurrentWindowUtc(GetHoursBack(), IsCustomRange, _customRange);
    }

    /// <summary>
    /// Reads this server's clock (from <c>server_properties</c>) again. Awaited at the start of every refresh
    /// rather than cached for the tab's life: a server that moves to a new zone, or upgrades to an engine that
    /// reports a zone id, is picked up on the next refresh (#4766). A failed read, or nothing collected yet,
    /// keeps the clock the tab already has (the fixed offset the connect probe read until the first collected
    /// row arrives). A visible tab also installs the clock as the process-wide one, so its grids and chart
    /// labels convert with it.
    /// </summary>
    internal async System.Threading.Tasks.Task RefreshServerClockAsync()
    {
        try
        {
            var clock = await Task.Run(() => _dataService.GetServerClockAsync(_serverId));
            if (clock is not null)
            {
                _serverClock = clock;
                if (IsVisible)
                {
                    ServerTimeHelper.ActiveServerClock = clock;
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Debug("ServerTab", $"[{_server.DisplayName}] server clock read failed, keeping the current clock: {ex.Message}");
        }
    }

    /// <summary>Shows the shared note above a master target's Blocking or Deadlocks list when it has separately monitored
    /// sibling databases; the lists keep master's server-wide rows while the counts skip them. Hidden otherwise.</summary>
    private void ApplySeparatelyMonitoredListNote(System.Windows.Controls.TextBlock noteText)
    {
        var note = PerformanceMonitorLite.Analysis.SeparatelyMonitoredScope.ListNote(_serverId);
        noteText.Text = note ?? "";
        noteText.Visibility = note is null ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
    }

    private async System.Threading.Tasks.Task RefreshAllDataAsync()
    {
        if (_isRefreshing) return;
        _isRefreshing = true;

        /* Read the clock again first: every conversion below (the pickers' rendering, the chart X values) goes
           through it, and it is never cached for the life of the tab (#4766). Never throws. */
        await RefreshServerClockAsync();

        /* The server's zone can change under the held range (its clock was just read again, or another tab switched
           the display mode), so the pickers are shown again from the held instants before the window is read. */
        RenderCustomRange();

        /* The window is the held range as UTC instants (#4766): every read below takes the same two instants. */
        var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

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
            /* Always keep the alert badge current even when the Blocking tab is not visible.
               RefreshAlertCountsAsync reads this tab's own held UTC window (GetCurrentWindowUtc). */
            if (MainTabControl.SelectedIndex != 8)
                await RefreshAlertCountsAsync();

            /* #1591: same reasoning as the alert badge above — a permission-denied collector is only visible on
               the Collection Health tab, which is precisely why it went unnoticed. Badge it from every tab. */
            await RefreshPermissionDeniedBadgeAsync();

            /* #4766: the time is the refresh instant read in the tab's display zone, and the label beside it names that
               zone at that same instant on the TAB's own clock, so the two agree in all three display modes and on
               either side of a daylight-saving change (DateTime.Now is this machine's zone, whatever the label, and
               the active clock follows whichever tab is selected). */
            var refreshedUtc = DateTime.UtcNow;
            var tz = ServerTimeHelper.GetTimezoneLabel(ServerTimeHelper.CurrentDisplayMode, _serverClock, refreshedUtc);
            var refreshedAt = PerformanceMonitor.Ui.DisplayZone.ToDisplay(refreshedUtc, GetPickerZone());
            ConnectionStatusText.Text = $"Last refresh: {refreshedAt:HH:mm:ss} ({tz})";
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

    /// <summary>
    /// The engine edition this tab uses: the connection check's answer when it has one, and otherwise the newest collected
    /// <c>server_properties</c> row's, read through <paramref name="readStoredEdition"/>. Only the unknown edition (0, from
    /// a check that failed) asks the store. A failed store read stays unknown, and the unknown edition makes no claim.
    /// </summary>
    internal static async System.Threading.Tasks.Task<int> ResolveEngineEditionAsync(int checkedEdition, Func<System.Threading.Tasks.Task<int>> readStoredEdition)
    {
        if (checkedEdition != CollectorEngineCapability.UnknownEngineEdition)
        {
            return checkedEdition;
        }

        try
        {
            return await readStoredEdition();
        }
        catch (Exception)
        {
            return CollectorEngineCapability.UnknownEngineEdition;
        }
    }

    /// <summary>
    /// Fills in the engine edition when the connection check could not read it (<see cref="ResolveEngineEditionAsync"/>).
    /// Awaited before every tab load, so the Azure SQL Database texts never depend on that one check, and a no-op once
    /// the edition is known.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshEngineEditionAsync()
    {
        if (_engineEdition != CollectorEngineCapability.UnknownEngineEdition)
        {
            return;
        }

        _engineEdition = await ResolveEngineEditionAsync(_engineEdition, () => Task.Run(() => _dataService.GetSqlEngineEditionAsync(_serverId)));

        /* The constructor decided the msdb warning from the connection check's edition, which was unknown. */
        RunningJobsMsdbWarning.Visibility = RunningJobsMsdbWarningVisibility(_hasMsdbAccess, _isAzureSqlDatabase, _isAwsRds);
    }

    private async System.Threading.Tasks.Task RefreshVisibleTabAsync(int hoursBack, DateTime? fromDate, DateTime? toDate, bool subTabOnly = false)
    {
        await RefreshEngineEditionAsync();

        if (TabReadsCollectorRuns(MainTabControl.SelectedIndex))
        {
            await RefreshCollectorRunsAsync();
        }

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
    /// <para>Derives its own window instead of taking the caller's: <see cref="GetCurrentWindowUtc"/> reads THIS
    /// tab's toolbar, a preset's hours or the naive-UTC pair the tab holds for a custom range (#4766). Every other
    /// windowed read here sits behind the <c>IsVisible</c> gate; the badge is the one that runs for a background
    /// tab, whose server can be in a different zone from the one on screen. The window is UTC instants, so no clock
    /// takes part and the badge counts the same window as the tab's own grids, whichever display zone is showing.</para>
    /// </summary>
    private async System.Threading.Tasks.Task RefreshAlertCountsAsync()
    {
        try
        {
            var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();
            var (blockingCount, deadlockCount, latestEventTime) = await Task.Run(() => _dataService.GetAlertCountsAsync(_serverId, hoursBack, fromDate, toDate));
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
        /* Lite's own database after a fatal error, read from memory rather than the database, which is the thing
           that failed. While nothing can be stored, the header says so in place of the permission badge. */
        var localDatabase = _dataService.LocalDatabaseHealth;
        LocalDatabaseBanner.Text = localDatabase.StatusLine ?? string.Empty;
        LocalDatabaseBanner.Visibility = localDatabase.StatusLine is null ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
        if (localDatabase.CollectionStopped)
        {
            CollectionHealthTab.Header = "Collection Health (stopped)";
            return;
        }

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
            var waitTypesTask = Task.Run(() => _dataService.GetDistinctWaitTypesForPickerAsync(_serverId, hoursBack, fromDate, toDate));
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
                        var disc = Helpers.MethodProfiler.TimeAsync("QueryPerformance.Discontinuities", () => Task.Run(() => SafeDiscontinuitiesAsync(hoursBack, fromDate, toDate)));
                        await System.Threading.Tasks.Task.WhenAll(qdt, pdt, qsdt, ect, disc);
                        UpdateQueryDurationTrendChart(qdt.Result, hoursBack, fromDate, toDate, disc.Result);
                        UpdateProcDurationTrendChart(pdt.Result, hoursBack, fromDate, toDate, disc.Result);
                        UpdateQueryStoreDurationTrendChart(qsdt.Result, hoursBack, fromDate, toDate, disc.Result);
                        UpdateExecutionCountTrendChart(ect.Result, hoursBack, fromDate, toDate, disc.Result);
                        break;
                    case 1: // Active Queries
                        var snapshots = await Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
                        _querySnapshotsFilterMgr!.UpdateData(snapshots);
                        LiveSnapshotIndicator.Text = "";
                        _ = LoadActiveQueriesSlicerAsync();
                        {
                            /* Where the stored snapshots start, over the SAME UTC window the grid just read
                               (GetLatestQuerySnapshotsAsync goes through GetTimeRange like the three grids below). */
                            var (windowStart4, windowEnd4) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QuerySnapshots, ActiveQueriesWindowTruncatedBanner, windowStart4, windowEnd4);
                        }
                        break;
                    case 2: // Top Queries by Duration
                        var queryStats = await Task.Run(() => _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, ServerClock, SelectedDatabaseFilter));
                        _queryStatsFilterMgr!.UpdateData(queryStats);
                        SetDefaultSortIfNone(QueryStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
                        _ = LoadQueryStatsSlicerAsync();
                        {
                            /* #4284: the comparison and the banner both read UTC collection_time, so they
                               share the SAME UTC window GetTopQueriesByCpuAsync just read
                               (LocalDataService.GetQueriesTabWindowUtc) -- computed once and handed to both. */
                            var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                            await RefreshQueryStatsComparisonAsync(windowStart, windowEnd);
                            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStats, QueryStatsWindowTruncatedBanner, windowStart, windowEnd);
                        }
                        break;
                    case 3: // Top Procedures by Duration
                        var procStats = await Task.Run(() => _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, ServerClock, SelectedDatabaseFilter));
                        _procStatsFilterMgr!.UpdateData(procStats);
                        SetDefaultSortIfNone(ProcedureStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
                        _ = LoadProcStatsSlicerAsync();
                        {
                            /* #4284: UTC comparison and banner window, computed once -- see the twin comment
                               on the Top Queries case above. */
                            var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                            await RefreshProcStatsComparisonAsync(windowStart, windowEnd);
                            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.ProcedureStats, ProcStatsWindowTruncatedBanner, windowStart, windowEnd);
                        }
                        break;
                    case 4: // Query Store by Duration
                        var qsData = await Task.Run(() => _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate, SelectedDatabaseFilter));
                        _queryStoreFilterMgr!.UpdateData(qsData);
                        SetDefaultSortIfNone(QueryStoreGrid, "TotalDurationMs", ListSortDirection.Descending);
                        _ = LoadQueryStoreSlicerAsync();
                        {
                            /* #4284: UTC comparison and banner window, computed once -- see the twin comment
                               on the Top Queries case above. */
                            var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                            await RefreshQueryStoreComparisonAsync(windowStart, windowEnd);
                            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStoreStats, QueryStoreWindowTruncatedBanner, windowStart, windowEnd);
                        }
                        break;
                    case 5: // Plan Corrections
                        var planCorrections = await Task.Run(() => SafeQueryAsync(() => _dataService.GetPlanCorrectionsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter)));
                        _planCorrectionFilterMgr!.UpdateData(planCorrections);
                        SetDefaultSortIfNone(PlanCorrectionGrid, "Score", ListSortDirection.Descending);
                        /* #4966: where the stored plan corrections start, over the SAME UTC window the grid read. */
                        await RefreshPlanCorrectionsBannerAsync(planCorrections, hoursBack, fromDate, toDate);
                        break;
                    case 6: // Query Heatmap
                        var hmMetric = (HeatmapMetric)HeatmapMetricCombo.SelectedIndex;
                        var hmData = await Task.Run(() => _dataService.GetQueryHeatmapAsync(_serverId, hmMetric, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
                        AppLogger.Info("ServerTab", $"[{_server.DisplayName}] Heatmap: {hmData.TimeBuckets.Length} time buckets, {hmData.Intensities.GetLength(0)}x{hmData.Intensities.GetLength(1)} grid");
                        UpdateQueryHeatmapChart(hmData);
                        /* #4966: where the stored query_stats rows start, over the SAME UTC window the heatmap read. */
                        await RefreshQueryHeatmapBannerAsync(hoursBack, fromDate, toDate);
                        break;
                }
                return;
            }

            /* Full refresh: load all sub-tabs */
            var snapshotsTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.Snapshots", () => Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter)));
            var queryStatsTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QueryStats", () => Task.Run(() => _dataService.GetTopQueriesByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, ServerClock, SelectedDatabaseFilter)));
            var procStatsTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ProcStats", () => Task.Run(() => _dataService.GetTopProceduresByCpuAsync(_serverId, hoursBack, 50, fromDate, toDate, ServerClock, SelectedDatabaseFilter)));
            var queryStoreTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QueryStore", () => Task.Run(() => _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack, 50, fromDate, toDate, SelectedDatabaseFilter)));
            var planCorrectionTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.PlanCorrections", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetPlanCorrectionsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var queryDurationTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QueryDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetQueryDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var procDurationTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ProcDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetProcedureDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var queryStoreDurationTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.QsDurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetQueryStoreDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            var executionCountTrendTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.ExecutionTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetExecutionCountTrendAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter))));
            /* #3653 A5: the window's baseline discontinuities, one read for the four trend charts. */
            var discontinuitiesTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.Discontinuities", () => Task.Run(() => SafeDiscontinuitiesAsync(hoursBack, fromDate, toDate)));
            var heatmapTask = Helpers.MethodProfiler.TimeAsync("QueryPerformance.Heatmap", () => Task.Run(async () =>
            {
                try { return await _dataService.GetQueryHeatmapAsync(_serverId, (HeatmapMetric)Dispatcher.Invoke(() => HeatmapMetricCombo.SelectedIndex), hoursBack, fromDate, toDate, SelectedDatabaseFilter); }
                catch { return new HeatmapResult(); }
            }));

            await System.Threading.Tasks.Task.WhenAll(
                snapshotsTask, queryStatsTask, procStatsTask, queryStoreTask, planCorrectionTask,
                queryDurationTrendTask, procDurationTrendTask, queryStoreDurationTrendTask, executionCountTrendTask,
                discontinuitiesTask, heatmapTask);

            _querySnapshotsFilterMgr!.UpdateData(snapshotsTask.Result);
            LiveSnapshotIndicator.Text = "";

            _ = LoadActiveQueriesSlicerAsync();
            {
                /* Where the stored snapshots start, over the SAME UTC window the grid read -- see the Active
                   Queries case of the sub-tab switch above. */
                var (windowStart4, windowEnd4) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QuerySnapshots, ActiveQueriesWindowTruncatedBanner, windowStart4, windowEnd4);
            }

            _queryStatsFilterMgr!.UpdateData(queryStatsTask.Result);
            SetDefaultSortIfNone(QueryStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
            _ = LoadQueryStatsSlicerAsync();
            {
                /* #4284: UTC comparison and banner window, computed once -- see the twin comment on the
                   sub-tab-switch case above. */
                var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                await RefreshQueryStatsComparisonAsync(windowStart, windowEnd);
                await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStats, QueryStatsWindowTruncatedBanner, windowStart, windowEnd);
            }
            _procStatsFilterMgr!.UpdateData(procStatsTask.Result);
            SetDefaultSortIfNone(ProcedureStatsGrid, "TotalElapsedMs", ListSortDirection.Descending);
            _ = LoadProcStatsSlicerAsync();
            {
                /* #4284: UTC comparison and banner window, computed once -- see the twin comment on the
                   sub-tab-switch case above. */
                var (windowStart2, windowEnd2) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                await RefreshProcStatsComparisonAsync(windowStart2, windowEnd2);
                await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.ProcedureStats, ProcStatsWindowTruncatedBanner, windowStart2, windowEnd2);
            }
            _queryStoreFilterMgr!.UpdateData(queryStoreTask.Result);
            SetDefaultSortIfNone(QueryStoreGrid, "TotalDurationMs", ListSortDirection.Descending);
            _ = LoadQueryStoreSlicerAsync();
            {
                /* #4284: UTC comparison and banner window, computed once -- see the twin comment on the
                   sub-tab-switch case above. */
                var (windowStart3, windowEnd3) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                await RefreshQueryStoreComparisonAsync(windowStart3, windowEnd3);
                await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStoreStats, QueryStoreWindowTruncatedBanner, windowStart3, windowEnd3);
            }
            _planCorrectionFilterMgr!.UpdateData(planCorrectionTask.Result);
            SetDefaultSortIfNone(PlanCorrectionGrid, "Score", ListSortDirection.Descending);
            /* #4966: see the Plan Corrections case of the sub-tab switch above. */
            await RefreshPlanCorrectionsBannerAsync(planCorrectionTask.Result, hoursBack, fromDate, toDate);

            UpdateQueryDurationTrendChart(queryDurationTrendTask.Result, hoursBack, fromDate, toDate, discontinuitiesTask.Result);
            UpdateProcDurationTrendChart(procDurationTrendTask.Result, hoursBack, fromDate, toDate, discontinuitiesTask.Result);
            UpdateQueryStoreDurationTrendChart(queryStoreDurationTrendTask.Result, hoursBack, fromDate, toDate, discontinuitiesTask.Result);
            UpdateExecutionCountTrendChart(executionCountTrendTask.Result, hoursBack, fromDate, toDate, discontinuitiesTask.Result);
            UpdateQueryHeatmapChart(heatmapTask.Result);
            /* #4966: see the Query Heatmap case of the sub-tab switch above. */
            await RefreshQueryHeatmapBannerAsync(hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshQueriesAsync failed: {ex.Message}");
        }
    }

    /// <summary>
    /// #4231: probes the shared window-floor helper (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>)
    /// for one of the <see cref="QueryWindowRelation"/> relations and updates that
    /// surface's "Showing since &lt;time&gt;" banner —
    /// the SAME probe and the same truncation verdict (<see cref="McpQueryTools.IsWindowTruncated"/>) the
    /// matching MCP tool uses, so the grid and the tool never disagree about whether a window was cut short.
    /// Every surface that carries the banner reaches it here: the three Queries grids, Active Queries, Current Waits,
    /// the Query Heatmap, Memory Pressure Events and Plan Corrections (the last one through
    /// <see cref="RefreshCappedGridBannerAsync{T}"/>, when its read is under its cap), and, over the toolbar's window, the
    /// System Events, Default Trace and Config Changes grids (through <c>RefreshStoredWindowBannerAsync</c>) and the two
    /// grids that read a capped page, the Collection Log and Long Queries (through the cap-aware step, like Plan
    /// Corrections). Called from the sub-tab switch
    /// and full-refresh paths below, from the slicer handlers in ServerTab.Slicers.cs, from the Active Queries
    /// drill-downs in ServerTab.DrillDown.cs and from the helpers in ServerTab.QueriesDataStart.cs — a slicer drag
    /// re-reads the same grid over a narrower window, which
    /// can itself start after the raw table's floor, so it needs the same disclosure. A probe that throws costs
    /// only the banner (<see cref="ProbeWindowFloorOrNullAsync"/> hides it and logs): the callers' own try/catch
    /// would otherwise skip whatever follows the probe, such as the Query Stats grid bind after the Active Queries
    /// banner, or the Locking tab's slicers and tab-badge counts after the Current Waits banner.
    ///
    /// <para>#4279: <paramref name="startUtc"/>/<paramref name="endUtc"/> MUST be the same UTC window the
    /// matching grid read (<see cref="LocalDataService.GetQueriesTabWindowUtc"/>, or a slicer's own
    /// <c>SlicerRangeEventArgs.StartUtc</c>/<c>EndUtc</c>) -- <see cref="LocalDataService.GetQueryWindowFloorAsync"/>
    /// compares them straight against UTC <c>collection_time</c>, with no offset conversion of its own. A
    /// server-local pair here silently shifts the probed window by the server's UTC offset.</para>
    ///
    /// <para>#4966: a window no longer than the 90-minute slack (<see cref="McpQueryTools.CanWindowBeTruncated"/>) can
    /// never get a coverage note, so the probe is not called for it: the banner is hidden and its text cleared, as for
    /// any window the probe finds nothing to report on.</para>
    ///
    /// <para>#4989: a capped grid under its cap hands <paramref name="earliestRowShownUtc"/>, the oldest of its rows by the
    /// time each is shown on. The notice then names the earlier of the probe's floor and that row
    /// (<see cref="EarlierOfFloorAndRowShown"/>): the Long Queries grid shows each completion at its event time, which on a
    /// server's first run can be up to <see cref="PerformanceMonitor.Collectors.CollectorContext.EventFallbackWindow"/> before the run that stored it,
    /// and the probe measures the run's time. Every other caller leaves it out and gets the probe's floor as before.</para>
    /// </summary>
    private async System.Threading.Tasks.Task RefreshWindowTruncatedBannerAsync(QueryWindowRelation relation, TextBlock banner, DateTime startUtc, DateTime endUtc, DateTime? earliestRowShownUtc = null)
    {
        var floor = await ProbeWindowFloorOrNullAsync(
            () => Task.Run(() => _dataService.GetQueryWindowFloorAsync(relation, _serverId, startUtc, endUtc)),
            $"[{_server.DisplayName}] {relation}", startUtc, endUtc);
        ApplyWindowFloorToBanner(banner, EarlierOfFloorAndRowShown(floor, earliestRowShownUtc), startUtc, GetPickerZone());
    }

    /// <summary>
    /// #4989: the floor a capped grid's notice is worded from when its read stayed UNDER its cap: the probe's floor
    /// (<paramref name="probedFloor"/>), or the oldest row the grid shows (<paramref name="earliestRowShownUtc"/>) when that is
    /// earlier, so a notice never names a time later than a row on the screen. The same rule as the Darling viewer's event
    /// grids (<c>ViewerEventDataStart.Of</c>). A null floor stays null: the probe found nothing to report on, its failure
    /// hides the banner, and a window no longer than the slack never gets one, and a row shown does not change any of those.
    /// For a grid whose rows are shown on the probe's own column (the Collection Log and Plan Corrections) the probe's floor is
    /// never later than its oldest row, so this returns the floor unchanged.
    /// </summary>
    internal static DateTime? EarlierOfFloorAndRowShown(DateTime? probedFloor, DateTime? earliestRowShownUtc) =>
        probedFloor is DateTime floor && earliestRowShownUtc is DateTime shown && shown < floor ? shown : probedFloor;

    /// <summary>The oldest of a capped grid's <paramref name="rows"/> by <paramref name="rowTimeUtc"/>, or null when it shows none.</summary>
    internal static DateTime? EarliestRowShown<T>(IReadOnlyCollection<T> rows, Func<T, DateTime> rowTimeUtc) =>
        rows.Count == 0 ? null : rows.Min(rowTimeUtc);

    /// <summary>
    /// The answer of a window-floor probe, or null when the probe throws, or when the window is no longer than the
    /// slack (<see cref="McpQueryTools.CanWindowBeTruncated"/>): such a window can never get a coverage note, so the
    /// probe is not called at all (#4966). The probe is only the banner's disclosure, so
    /// its failure must not unwind the refresh past what follows it: the null goes on to
    /// <see cref="ApplyWindowFloorToBanner"/>, for which a null floor hides the banner and clears its text (a "Showing
    /// since" from the last read does not outlive the answer it came from), the failure is logged at Warn, and the
    /// refresh goes on. <c>internal static</c> with the probe passed in so the tests drive a throwing probe, and count
    /// the calls a short window makes, without building the UserControl.
    /// </summary>
    internal static async System.Threading.Tasks.Task<DateTime?> ProbeWindowFloorOrNullAsync(
        Func<System.Threading.Tasks.Task<DateTime?>> probe, string what, DateTime startUtc, DateTime endUtc)
    {
        if (!McpQueryTools.CanWindowBeTruncated(startUtc, endUtc))
        {
            return null;
        }

        try
        {
            return await probe();
        }
        catch (Exception ex)
        {
            AppLogger.Warn("ServerTab", $"{what} data-start probe failed, so no \"Showing since\" banner is shown: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// #4966: the floor a CAPPED grid's "Showing since" banner is worded from. A read that returned as many rows as
    /// its cap (<paramref name="rowCap"/>, newest first) is cut short, so its reach is its oldest row,
    /// by time, not by position, whatever the store holds: that is where the grid starts, even when the store covers
    /// the whole range. Below its cap a read holds everything the store has in the range, and the probed floor
    /// (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>, null included) stands. A <paramref name="rowCap"/> of
    /// zero or less means the read has no cap. A capped floor gets NO slack: the read dropped rows for certain, so its
    /// verdict is <see cref="ApplyCappedWindowFloorToBanner"/>'s (the banner shows whenever the oldest row shown is later
    /// than the window's start), while a probed floor keeps <see cref="ApplyWindowFloorToBanner"/>'s 90-minute slack,
    /// which absorbs a first collection that lands a little after the window starts. The ONE
    /// decision for every capped grid: <see cref="RefreshCappedGridBannerAsync{T}"/> calls it, and the grids that read
    /// the newest N rows (Plan Corrections, the Collection Log and Long Queries now; Blocked Process Reports and
    /// Deadlocks as they adopt it) pass their rows, their cap and the time their rows are shown and capped on.
    /// </summary>
    internal static DateTime? CapAwareWindowFloor<T>(DateTime? probedFloor, IReadOnlyCollection<T> rows, int rowCap, Func<T, DateTime> rowTimeUtc) =>
        rowCap > 0 && rows.Count >= rowCap ? rows.Min(rowTimeUtc) : probedFloor;

    /// <summary>
    /// #4966: the banner step for a grid whose read is capped at its newest <paramref name="rowCap"/> rows. A read that
    /// reached its cap (<see cref="CapAwareWindowFloor{T}"/> answers its oldest row) is worded from that row and needs
    /// no probe, and no slack: its reach is what the grid shows, whatever the store holds, and the banner shows whenever
    /// that oldest row is later than the window's start (<see cref="ApplyCappedWindowFloorToBanner"/>), even on a range
    /// of an hour. Below its cap it is the shared step,
    /// <see cref="RefreshWindowTruncatedBannerAsync"/>, with its probe, as for every other grid, handed the oldest row the
    /// grid shows so that the notice never names a time later than it (<see cref="EarlierOfFloorAndRowShown"/>, #4989). The decision itself is
    /// <see cref="CappedGridBannerAsync{T}"/>, which this hands the two ways out: the banner worded in the tab's picker
    /// zone from the oldest row, and that shared step.
    /// </summary>
    private System.Threading.Tasks.Task RefreshCappedGridBannerAsync<T>(
        QueryWindowRelation relation, TextBlock banner, DateTime startUtc, DateTime endUtc,
        IReadOnlyCollection<T> rows, int rowCap, Func<T, DateTime> rowTimeUtc) =>
        CappedGridBannerAsync(rows, rowCap, rowTimeUtc,
            oldestRowShown => ApplyCappedWindowFloorToBanner(banner, oldestRowShown, startUtc, GetPickerZone()),
            () => RefreshWindowTruncatedBannerAsync(relation, banner, startUtc, endUtc, EarliestRowShown(rows, rowTimeUtc)));

    /// <summary>
    /// #4966: the decision inside <see cref="RefreshCappedGridBannerAsync{T}"/>. A read that reached its cap
    /// (<see cref="CapAwareWindowFloor{T}"/>) hands its oldest row to <paramref name="wordFromOldestRow"/> and never
    /// calls <paramref name="probeStep"/>; a read under its cap returns what <paramref name="probeStep"/> returns and
    /// words nothing itself. <c>internal static</c> with the two ways out passed in, as
    /// <see cref="ProbeWindowFloorOrNullAsync"/> takes its probe, so the tests run this body without building the
    /// UserControl. The banner stays out of it: every <see cref="ApplyWindowFloorToBanner"/> and
    /// <see cref="ApplyCappedWindowFloorToBanner"/> call in the tab files is handed the tab's own picker zone, and this
    /// is not a tab.
    /// </summary>
    internal static System.Threading.Tasks.Task CappedGridBannerAsync<T>(
        IReadOnlyCollection<T> rows, int rowCap, Func<T, DateTime> rowTimeUtc,
        Action<DateTime> wordFromOldestRow, Func<System.Threading.Tasks.Task> probeStep)
    {
        if (CapAwareWindowFloor(null, rows, rowCap, rowTimeUtc) is DateTime oldestRowShown)
        {
            wordFromOldestRow(oldestRowShown);
            return System.Threading.Tasks.Task.CompletedTask;
        }

        return probeStep();
    }

    /// <summary>
    /// The verdict and the banner for one PROBE result: the SAME 90-minute slack
    /// (<see cref="McpQueryTools.IsWindowTruncated"/>) the MCP tools use, which absorbs a first collection that lands a
    /// little after the window starts, then the "Showing since" text. A floor at or
    /// before the window's start (the store reaches back to it, however quiet the window's own start was) and a null
    /// floor both hide the banner. Null means the window holds nothing to report: no row for the three Queries grids
    /// and the Query Heatmap, and for Active Queries, Current Waits, Plan Corrections and Memory Pressure Events no row
    /// and no logged run of the collector either (a covered window
    /// in which nothing ran or waited answers the start, not null, so it too shows no banner). The same step serves
    /// every surface that carries the banner (the three Queries grids, Plan Corrections, the Query Heatmap, Active
    /// Queries, Current Waits and Memory Pressure Events). A capped grid whose read hit its cap does not come through
    /// here: it is judged by <see cref="ApplyCappedWindowFloorToBanner"/>, with no slack (a capped grid under its cap
    /// does come through here, by way of its probe). <c>internal static</c> so the tests drive it, with a real probe
    /// result, without building the UserControl.
    /// </summary>
    internal static bool ApplyWindowFloorToBanner(TextBlock banner, DateTime? floor, DateTime startUtc, TimeZoneInfo zone)
    {
        var truncated = McpQueryTools.IsWindowTruncated(floor, startUtc);
        SetWindowTruncatedBanner(banner, truncated, floor ?? startUtc, zone);
        return truncated;
    }

    /// <summary>
    /// #4966: the verdict and the banner for a CAPPED read's oldest row (<see cref="CapAwareWindowFloor{T}"/>), with NO
    /// slack. The 90-minute slack of <see cref="ApplyWindowFloorToBanner"/> belongs to the coverage probe: it absorbs a
    /// first collection that lands a little after the window starts. A grid that filled its row cap dropped rows for
    /// certain, so its banner shows whenever the oldest row it shows is later than the window's start (on a range of an
    /// hour too), and an oldest row at or before the start hides it. Worded from that row, in <paramref name="zone"/>.
    /// <c>internal static</c> so the tests drive it without building the UserControl.
    /// </summary>
    internal static bool ApplyCappedWindowFloorToBanner(TextBlock banner, DateTime oldestRowShown, DateTime startUtc, TimeZoneInfo zone)
    {
        var truncated = oldestRowShown > startUtc;
        SetWindowTruncatedBanner(banner, truncated, oldestRowShown, zone);
        return truncated;
    }

    /// <summary>
    /// #4231 Ruled comment: "the WPF ... grids show 'Showing since &lt;time&gt;' in the header when the window
    /// is cut short" — same words Darling's twin uses. Formats the instant <paramref name="effectiveStart"/> with
    /// DisplayZone.Format in <paramref name="zone"/>, the tab's own display zone (#4766), the way
    /// QueryStatsComparisonBanner / ProcStatsComparisonBanner / QueryStoreComparisonBanner format their
    /// baseline range on these same tabs: never the active server's clock, which follows whichever tab is
    /// selected. internal (not private) so QueryWindowTruncationTests can pin the
    /// truncated/not-truncated text without instantiating the UserControl (WPF objects still need an STA
    /// thread to construct, which the test provides; the text itself is plain string formatting).
    /// </summary>
    internal static void SetWindowTruncatedBanner(TextBlock banner, bool truncated, DateTime effectiveStart, TimeZoneInfo zone)
    {
        banner.Visibility = truncated ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        banner.Text = truncated
            ? $"Showing since {PerformanceMonitor.Ui.DisplayZone.Format(effectiveStart, zone, "yyyy-MM-dd HH:mm:ss")}"
            : string.Empty;
    }

    /// <summary>Tab 0 — Overview (Correlated Timeline Lanes)</summary>
    private async System.Threading.Tasks.Task RefreshOverviewAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            /* #4296, #4766: the lanes plot the UTC instant, and the comparison is "the same wall-clock hours N days
               earlier" on the clock of the server THIS tab monitors -- read once, so the range and the refresh use
               the same one, and taken from the tab, not from ServerTimeHelper.ActiveServerClock, which follows the
               selected tab and would put another server's hours on this one. GetOverviewComparisonRange
               (CorrelatedTimelineLanesControl.xaml.cs) builds the current window and the reference from the same
               utcNow, so a preset range is sampled once, and hands back the UTC bounds and the day count. */
            var clock = _serverClock;
            (DateTime FromUtc, DateTime ToUtc, int Days)? comparison = CompareToCombo == null
                ? null
                : CorrelatedTimelineLanesControl.GetOverviewComparisonRange(
                    CompareToCombo.SelectedIndex, hoursBack, fromDate, toDate, DateTime.UtcNow, clock);
            await CorrelatedLanes.RefreshAsync(hoursBack, fromDate, toDate, clock, comparison);
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
            var cpuTask = Task.Run(() => _dataService.GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate, frame: CpuTimeFrame.Utc));
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
                        var clerkTypes = await Task.Run(() => _dataService.GetDistinctMemoryClerkTypesForPickerAsync(_serverId, hoursBack, fromDate, toDate));
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
                        /* #4966: where the stored events start, over the SAME UTC window the chart read. */
                        await RefreshMemoryPressureEventsBannerAsync(hoursBack, fromDate, toDate);
                        break;
                }
                return;
            }

            /* Full refresh: load all sub-tabs */
            var memoryTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryStats", () => Task.Run(() => _dataService.GetLatestMemoryStatsAsync(_serverId)));
            var memoryTrendTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryTrend", () => Task.Run(() => _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryClerkTypesTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryClerks", () => Task.Run(() => _dataService.GetDistinctMemoryClerkTypesForPickerAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryGrantTrendTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryGrantTrend", () => Task.Run(() => _dataService.GetMemoryGrantTrendAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryGrantChartTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryGrants", () => Task.Run(() => _dataService.GetMemoryGrantChartDataAsync(_serverId, hoursBack, fromDate, toDate)));
            var memoryPressureEventsTask = Helpers.MethodProfiler.TimeAsync("Memory.MemoryPressureEvents", () => Task.Run(() => _dataService.GetMemoryPressureEventsAsync(_serverId, hoursBack, fromDate, toDate)));

            await System.Threading.Tasks.Task.WhenAll(memoryTask, memoryTrendTask, memoryClerkTypesTask, memoryGrantTrendTask, memoryGrantChartTask, memoryPressureEventsTask);

            UpdateMemorySummary(memoryTask.Result);
            UpdateMemoryChart(memoryTrendTask.Result, memoryGrantTrendTask.Result, hoursBack, fromDate, toDate);
            UpdateMemoryGrantCharts(memoryGrantChartTask.Result, hoursBack, fromDate, toDate);
            UpdateMemoryPressureEventsChart(memoryPressureEventsTask.Result, hoursBack, fromDate, toDate);
            /* #4966: see the Memory Pressure Events case of the timer's sub-tab refresh above. */
            await RefreshMemoryPressureEventsBannerAsync(hoursBack, fromDate, toDate);
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
                        {
                            /* Where the stored waiting-task rows start, over the SAME UTC window both charts read
                               (GetWaitingTaskTrendAsync and GetBlockedSessionTrendAsync both take it from GetTimeRange). */
                            var (windowStart5, windowEnd5) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.WaitingTasks, CurrentWaitsWindowTruncatedBanner, windowStart5, windowEnd5);
                        }
                        break;
                    case 2: // Blocked Process Reports
                        var bpr = await Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
                        using (Helpers.MethodProfiler.StartTiming("Locking.BindBlockedGrid"))
                            _blockedProcessFilterMgr!.UpdateData(bpr);
                        ApplySeparatelyMonitoredListNote(BlockedProcessReportNoteText);
                        {
                            /* Where the stored blocked-process coverage starts (#4966), over the SAME UTC window the grid read
                               resolves from GetTimeRange. */
                            var (windowStart6, windowEnd6) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.BlockedProcessReports, BlockedProcessReportsWindowTruncatedBanner, windowStart6, windowEnd6);
                        }
                        await LoadBlockingSlicerAsync();
                        break;
                    case 3: // Deadlocks
                        var dlr = await Task.Run(() => _dataService.GetRecentDeadlocksAsync(_serverId, hoursBack, fromDate, toDate));
                        var dlrDetails = await ParseDeadlocksOffUiThreadAsync(dlr);
                        using (Helpers.MethodProfiler.StartTiming("Locking.BindDeadlockGrid"))
                            _deadlockFilterMgr!.UpdateData(dlrDetails);
                        ApplySeparatelyMonitoredListNote(DeadlockNoteText);
                        {
                            /* Where the stored deadlock coverage starts (#4966), over the SAME UTC window the grid read
                               resolves from GetTimeRange. */
                            var (windowStart7, windowEnd7) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.Deadlocks, DeadlocksWindowTruncatedBanner, windowStart7, windowEnd7);
                        }
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
                   the deadlock count (same deadlock window). */
                UpdateBlockingDurationChart(blockingDurationStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateBlockingTotalDurationChart(blockingDurationStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateDeadlockWaitChart(deadlockSeverityStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateDeadlockTotalWaitChart(deadlockSeverityStatsTask.Result, hoursBack, fromDate, toDate);
                UpdateBlockingStatsSummary(blockingDurationStatsTask.Result, deadlockTrendTask.Result, deadlockSeverityStatsTask.Result);
            }

            {
                /* Where the stored waiting-task rows start, over the SAME UTC window the Current Waits charts read --
                   see the Current Waits case of the sub-tab switch above. */
                var (windowStart5, windowEnd5) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.WaitingTasks, CurrentWaitsWindowTruncatedBanner, windowStart5, windowEnd5);
            }

            {
                /* Where the stored blocked-process and deadlock coverage starts (#4966), over the SAME UTC window the two
                   grids read (both resolve it from GetTimeRange); after both grids are bound above. */
                var (windowStart8, windowEnd8) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
                await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.BlockedProcessReports, BlockedProcessReportsWindowTruncatedBanner, windowStart8, windowEnd8);
                await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.Deadlocks, DeadlocksWindowTruncatedBanner, windowStart8, windowEnd8);
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
            var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

            var data = await Task.Run(() => _dataService.GetBlockingSlicerDataAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
            _blockingSlicerData = data;
            _blockingSlicerMetric = "Events";
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(hoursBack, fromDate, toDate, DateTime.UtcNow);
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
            var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

            var data = await Task.Run(() => _dataService.GetDeadlockSlicerDataAsync(_serverId, hoursBack, fromDate, toDate));
            _deadlockSlicerData = data;
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(hoursBack, fromDate, toDate, DateTime.UtcNow);
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
            var perfmonCountersTask = Task.Run(() => _dataService.GetDistinctPerfmonCountersForPickerAsync(_serverId, hoursBack, fromDate, toDate));
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
            await RefreshRunningJobsSkippedNoteAsync();
            ShowEngineGap(RunningJobsNoDataMessage, "running_jobs", runningJobsTask.Result.Count);
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
            ShowEngineGap(ServerConfigNoDataMessage, "server_config", serverConfigTask.Result.Count);
            _databaseConfigFilterMgr!.UpdateData(databaseConfigTask.Result);
            _dbScopedConfigFilterMgr!.UpdateData(databaseScopedConfigTask.Result);
            _queryStoreHealthFilterMgr!.UpdateData(queryStoreHealthTask.Result);
            _automaticTuningFilterMgr!.UpdateData(automaticTuningTask.Result);
            _traceFlagsFilterMgr!.UpdateData(traceFlagsTask.Result);
            ShowEngineGap(TraceFlagsNoDataMessage, "trace_flags", traceFlagsTask.Result.Count);
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
            /* #4989: the Duration Trends chart reads its own buckets over the whole range, beside the grid's read. The grid's
               page is the newest CollectionLogGridCap runs, a sliver of a long range, so the chart is not fed from it. */
            var collectorDurationTask = Helpers.MethodProfiler.TimeAsync("CollectionHealth.DurationTrends", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetCollectorDurationTrendAsync(_serverId, hoursBack, fromDate, toDate))));

            await System.Threading.Tasks.Task.WhenAll(collectionHealthTask, collectionLogTask, collectorDurationTask);

            /* #4766: every row reads its time on THIS tab's server clock, not on whichever clock is active when the
               grid renders. Both grids sit in this server's own tab, so the two are the same while the tab is showing,
               and the stamped one also holds when the display mode flips or another tab is selected first. */
            var tabClock = _serverClock;
            foreach (var row in collectionHealthTask.Result) row.Clock = tabClock;
            foreach (var row in collectionLogTask.Result) row.Clock = tabClock;

            _collectionHealthFilterMgr!.UpdateData(collectionHealthTask.Result);
            _collectionLogFilterMgr!.UpdateData(collectionLogTask.Result);
            UpdateCollectorDurationChart(collectorDurationTask.Result, hoursBack, fromDate, toDate);
            /* #4989: the grid reads only the newest CollectionLogGridCap runs, so its notice goes through the cap-aware step. */
            var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
            await RefreshCappedGridBannerAsync(QueryWindowRelation.CollectionLog, CollectionLogWindowTruncatedBanner, windowStart, windowEnd, collectionLogTask.Result, LocalDataService.CollectionLogGridCap, row => row.CollectionTime);
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

    /// <summary>
    /// The Performance Trends window's baseline discontinuities (#3653 A5), on <see cref="SafeQueryAsync"/>'s
    /// discipline: a store that cannot answer this read still gets its four series drawn, unmarked, rather
    /// than an empty tab. Same window arguments as the four trend reads beside it, so the markers and the
    /// points describe one span.
    /// </summary>
    private async Task<IReadOnlyList<PerformanceMonitor.Collectors.BaselineDiscontinuity>> SafeDiscontinuitiesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            return await _dataService.GetBaselineDiscontinuitiesAsync(_serverId, hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"Baseline-discontinuity read failed; trend charts drawn without markers: {ex.Message}");
            return Array.Empty<PerformanceMonitor.Collectors.BaselineDiscontinuity>();
        }
    }
}

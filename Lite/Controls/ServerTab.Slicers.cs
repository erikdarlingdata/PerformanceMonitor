/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Controls.Primitives;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.Win32;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using ScottPlot;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private async void OnBlockingSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var bprRead = await Task.Run(() => _dataService.ReadRecentBlockedProcessReportsAsync(_serverId, 0, e.StartUtc, e.EndUtc, SelectedDatabaseFilter));
            _blockedProcessFilterMgr!.UpdateData(bprRead.Rows);
            /* A slicer drag re-reads the grid over a narrower window, which can itself start after the stored coverage does
               (#4966): the banner follows the SAME UTC pair the read took, as the Queries grids' slicer handlers do. */
            await RefreshCappedGridBannerAsync(QueryWindowRelation.BlockedProcessReports, BlockedProcessReportsWindowTruncatedBanner, e.StartUtc, e.EndUtc, bprRead.Rows, LocalDataService.BlockedProcessReportGridCap, BlockedProcessRowTimeUtc, bprRead.CappedSourceStartUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnBlockingSlicerChanged failed: {ex.Message}");
        }
    }

    private async void OnDeadlockSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var dlr = await Task.Run(() => _dataService.GetRecentDeadlocksAsync(_serverId, 0, e.StartUtc, e.EndUtc));
            _deadlockFilterMgr!.UpdateData(await ParseDeadlocksOffUiThreadAsync(dlr));
            /* Same as OnBlockingSlicerChanged (#4966): the banner follows the UTC pair this read took. */
            await RefreshCappedGridBannerAsync(QueryWindowRelation.Deadlocks, DeadlocksWindowTruncatedBanner, e.StartUtc, e.EndUtc, dlr, LocalDataService.DeadlockGridCap, DeadlockRowTimeUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnDeadlockSlicerChanged failed: {ex.Message}");
        }
    }

    // ── Active Queries Slicer ──

    private async System.Threading.Tasks.Task LoadActiveQueriesSlicerAsync()
    {
        try
        {
            var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

            // For narrow time ranges (drill-downs), pad the query by ±1 hour
            // so hourly slicer buckets overlap the display range
            DateTime? queryFrom = fromDate, queryTo = toDate;
            if (fromDate.HasValue && toDate.HasValue && (toDate.Value - fromDate.Value).TotalHours < 2)
            {
                queryFrom = fromDate.Value.AddHours(-1);
                queryTo = toDate.Value.AddHours(1);
            }

            var includeMonitorStrip = ActiveQueriesIncludeMonitor;
            var data = await Task.Run(() => _dataService.GetActiveQuerySlicerDataAsync(_serverId, hoursBack, queryFrom, queryTo, SelectedDatabaseFilter, includeMonitorStrip));
            _activeQueriesSlicerData = data;
            _activeQueriesSlicerMetric = "Sessions";
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(hoursBack, queryFrom, queryTo, DateTime.UtcNow);
            if (data.Count == 0)
                ActiveQueriesSlicer.ShowEmpty("No active query samples in the selected time window.");
            else
                ActiveQueriesSlicer.LoadData(data, "Sessions", slicerStart, slicerEnd, PerformanceMonitor.Common.SlicerBuckets.MinutesFor(slicerStart, slicerEnd));
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadActiveQueriesSlicerAsync failed: {ex.Message}");
        }
    }

    private string _activeQueriesSlicerMetric = "Sessions";
    private List<TimeSliceBucket>? _activeQueriesSlicerData;

    private async void OnActiveQueriesSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var includeMonitor = ActiveQueriesIncludeMonitor;
            var snapshots = await Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, e.StartUtc, e.EndUtc, SelectedDatabaseFilter, includeMonitor: includeMonitor));
            _querySnapshotsFilterMgr!.UpdateData(snapshots);
            LiveSnapshotIndicator.Text = "";
            /* The banner takes e.StartUtc/e.EndUtc, the UTC pair the grid read above takes (same as the three Queries grids). */
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QuerySnapshots, ActiveQueriesWindowTruncatedBanner, e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnActiveQueriesSlicerChanged failed: {ex.Message}");
        }
    }

    // ── Query Stats Slicer ──

    private string _queryStatsSlicerMetric = "TotalCpu";
    private List<TimeSliceBucket>? _queryStatsSlicerData;
    private int _queryStatsRowCount;

    /// <summary>
    /// The Top Queries grid's own empty sentence (<see cref="PerformanceMonitor.Ui.TopQueriesEmptyText"/>): the strip above it has
    /// samples, so its sentence is not showing, and the ranking returned no row. Called after each grid read with the row count,
    /// and after each strip load, which decides whether the strip's sentence or the grid's shows. The strip's own sentence stands
    /// when the window holds no samples, and nothing shows over the comparison grid.
    /// </summary>
    private void UpdateQueryStatsEmptyMessage(int? rowCount = null)
    {
        if (rowCount.HasValue) _queryStatsRowCount = rowCount.Value;
        if (QueryStatsSlicer.EmptyText is not null) return;
        var text = QueryStatsGrid.Visibility == Visibility.Visible
            ? PerformanceMonitor.Ui.TopQueriesEmptyText.Text(_queryStatsRowCount, QueryStatsSlicer.HasData)
            : null;
        QueryStatsEmptyMessage.Text = text ?? "";
        QueryStatsEmptyMessage.Visibility = text is null ? Visibility.Collapsed : Visibility.Visible;
    }

    /// <summary>Loads the strip over the two instants the grid read took, so its window is the grid's window.</summary>
    private async System.Threading.Tasks.Task LoadQueryStatsSlicerAsync(DateTime startUtc, DateTime endUtc)
    {
        try
        {
            var data = await Task.Run(() => _dataService.GetQueryStatsSlicerDataAsync(_serverId, 0, startUtc, endUtc, SelectedDatabaseFilter));
            _queryStatsSlicerData = data;
            _queryStatsSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(0, startUtc, endUtc, DateTime.UtcNow);
            if (data.Count == 0)
                QueryStatsSlicer.ShowEmpty("No query statistics in the selected time window.");
            else
                QueryStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd, PerformanceMonitor.Common.SlicerBuckets.MinutesFor(slicerStart, slicerEnd));
            UpdateQueryStatsEmptyMessage();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadQueryStatsSlicerAsync failed: {ex.Message}");
        }
    }

    private async void OnQueryStatsSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var queryStats = await ReadTopQueriesAsync(0, e.StartUtc, e.EndUtc);
            _queryStatsFilterMgr!.UpdateData(queryStats);
            UpdateQueryStatsEmptyMessage(queryStats.Count);
            /* #4284: the comparison reads UTC collection_time directly, with no offset conversion of its own
               (same as the banner below), so it takes e.StartUtc/e.EndUtc -- the same pair the grid read above takes. */
            await RefreshQueryStatsComparisonAsync(e.StartUtc, e.EndUtc);
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStats, QueryStatsWindowTruncatedBanner, e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnQueryStatsSlicerChanged failed: {ex.Message}");
        }
    }

    // ── Query Store Slicer ──

    private string _queryStoreSlicerMetric = "TotalCpu";
    private List<TimeSliceBucket>? _queryStoreSlicerData;

    private async System.Threading.Tasks.Task LoadQueryStoreSlicerAsync()
    {
        try
        {
            var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

            var data = await Task.Run(() => _dataService.GetQueryStoreSlicerDataAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
            _queryStoreSlicerData = data;
            _queryStoreSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(hoursBack, fromDate, toDate, DateTime.UtcNow);
            if (data.Count == 0)
            {
                QueryStoreSlicer.ShowEmpty(QueryStoreEmptyWindowText);
                var healthSchedule = _queryStoreHealthSchedule();
                await ShowQueryStoreEmptyReasonAsync(
                    QueryStoreSlicer, _dataService, _serverId, _server.DisplayName,
                    healthSchedule?.Enabled ?? true,
                    healthSchedule?.FrequencyMinutes is int minutes and > 0
                        ? minutes
                        : PerformanceMonitor.Collectors.CollectorScheduleDefaults.All["query_store_health"].FrequencyMinutes);
            }
            else
                QueryStoreSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd, PerformanceMonitor.Common.SlicerBuckets.MinutesFor(slicerStart, slicerEnd));
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadQueryStoreSlicerAsync failed: {ex.Message}");
        }
    }

    /// <summary>What the Query Store panel says over a window that holds no rows.</summary>
    internal const string QueryStoreEmptyWindowText = "No Query Store data in the selected time window.";

    /// <summary>
    /// #8541: the step that follows the Query Store panel saying its window is empty. It asks the store why and shows the answer on
    /// the same slicer. A readable secondary Lite does not collect Query Store from gets the sentence <c>get_query_store_top</c>
    /// gives, in place of the empty-window sentence. Otherwise a server whose newest <c>query_store_health</c> capture is older
    /// than <see cref="QueryStoreSnapshotStaleness.Threshold"/> gets the stale-capture sentence after it, which says whether that
    /// collector's schedule still takes a capture (<paramref name="healthCollectorEnabled"/>) and at what cadence
    /// (<paramref name="healthIntervalMinutes"/>). Both store reads run off the UI thread. The empty-window sentence shows first
    /// and stays when the store has no reason to give or a read fails. Nothing is shown when the slicer moved on while the store
    /// was asked (it loaded data, or says something else).
    /// </summary>
    internal static async System.Threading.Tasks.Task ShowQueryStoreEmptyReasonAsync(
        Controls.TimeRangeSlicerControl slicer, LocalDataService dataService, int serverId, string serverName, bool healthCollectorEnabled, int healthIntervalMinutes)
    {
        ArgumentNullException.ThrowIfNull(slicer);
        ArgumentNullException.ThrowIfNull(dataService);
        ArgumentNullException.ThrowIfNull(serverName);
        ArgumentOutOfRangeException.ThrowIfNegative(healthIntervalMinutes);

        var shown = slicer.EmptyText;
        if (shown is null)
        {
            return;
        }

        var (readableSecondary, staleCapture) = await System.Threading.Tasks.Task.Run(async () =>
        {
            try
            {
                if (await PerformanceMonitorLite.Mcp.McpRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(dataService, serverId, serverName) is { } secondary)
                {
                    return (ReadableSecondary: (string?)secondary, StaleCapture: (string?)null);
                }

                var captures = await dataService.GetLatestQueryStoreHealthAsync(serverId);
                return (
                    ReadableSecondary: (string?)null,
                    StaleCapture: captures.Count == 0
                        ? null
                        : QueryStoreSnapshotStaleness.PanelNote(
                            DateTime.SpecifyKind(captures[0].CaptureTime, DateTimeKind.Utc), DateTime.UtcNow, "query_store_health",
                            healthCollectorEnabled, healthIntervalMinutes));
            }
            catch (Exception ex)
            {
                AppLogger.Info("ServerTab", $"[{serverName}] ShowQueryStoreEmptyReasonAsync failed: {ex.Message}");
                return (ReadableSecondary: (string?)null, StaleCapture: (string?)null);
            }
        });
        var text = readableSecondary ?? (staleCapture is null ? null : shown + " " + staleCapture);
        if (text is not null && ReferenceEquals(slicer.EmptyText, shown))
        {
            slicer.ShowEmpty(text);
        }
    }

    private async void OnQueryStoreSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var qsData = await Task.Run(() => QueryTagGrid.ReadAsync(_dataService, _serverId, _queryTagChoice, tags => _dataService.GetQueryStoreTopQueriesAsync(_serverId, 0, 50, e.StartUtc, e.EndUtc, SelectedDatabaseFilter, tags: tags)));
            _queryStoreFilterMgr!.UpdateData(qsData);
            /* #4284: UTC comparison and banner bounds -- see the twin comment in OnQueryStatsSlicerChanged above. */
            await RefreshQueryStoreComparisonAsync(e.StartUtc, e.EndUtc);
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.QueryStoreStats, QueryStoreWindowTruncatedBanner, e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnQueryStoreSlicerChanged failed: {ex.Message}");
        }
    }

    // ── Procedure Stats Slicer ──

    private string _procStatsSlicerMetric = "TotalCpu";
    private List<TimeSliceBucket>? _procStatsSlicerData;

    private async System.Threading.Tasks.Task LoadProcStatsSlicerAsync()
    {
        try
        {
            var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

            var data = await Task.Run(() => _dataService.GetProcStatsSlicerDataAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
            _procStatsSlicerData = data;
            _procStatsSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(hoursBack, fromDate, toDate, DateTime.UtcNow);
            if (data.Count == 0)
                ProcStatsSlicer.ShowEmpty("No procedure statistics in the selected time window.");
            else
                ProcStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd, PerformanceMonitor.Common.SlicerBuckets.MinutesFor(slicerStart, slicerEnd));
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadProcStatsSlicerAsync failed: {ex.Message}");
        }
    }

    private async void OnProcStatsSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var (procStats, procLeftOut) = await ReadTopProceduresAsync(0, e.StartUtc, e.EndUtc);
            _procStatsFilterMgr!.UpdateData(procStats);
            /* #4284: UTC comparison and banner bounds -- see the twin comment in OnQueryStatsSlicerChanged above. */
            await RefreshProcStatsComparisonAsync(e.StartUtc, e.EndUtc);
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.ProcedureStats, ProcStatsWindowTruncatedBanner, e.StartUtc, e.EndUtc);
            await UpdateProcStatsLeftOutBannerAsync(procStats.Count, procLeftOut, e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnProcStatsSlicerChanged failed: {ex.Message}");
        }
    }
}

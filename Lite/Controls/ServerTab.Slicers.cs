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
            var bpr = await Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(_serverId, 0, e.StartUtc, e.EndUtc, SelectedDatabaseFilter));
            _blockedProcessFilterMgr!.UpdateData(bpr);
            /* A slicer drag re-reads the grid over a narrower window, which can itself start after the stored coverage does
               (#4966): the banner follows the SAME UTC pair the read took, as the Queries grids' slicer handlers do. */
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.BlockedProcessReports, BlockedProcessReportsWindowTruncatedBanner, e.StartUtc, e.EndUtc);
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
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.Deadlocks, DeadlocksWindowTruncatedBanner, e.StartUtc, e.EndUtc);
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

            var data = await Task.Run(() => _dataService.GetActiveQuerySlicerDataAsync(_serverId, hoursBack, queryFrom, queryTo, SelectedDatabaseFilter));
            _activeQueriesSlicerData = data;
            _activeQueriesSlicerMetric = "Sessions";
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(hoursBack, queryFrom, queryTo, DateTime.UtcNow);
            if (data.Count > 0)
                ActiveQueriesSlicer.LoadData(data, "Sessions", slicerStart, slicerEnd);
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
            var snapshots = await Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, e.StartUtc, e.EndUtc, SelectedDatabaseFilter));
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

    private async System.Threading.Tasks.Task LoadQueryStatsSlicerAsync()
    {
        try
        {
            var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

            var data = await Task.Run(() => _dataService.GetQueryStatsSlicerDataAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
            _queryStatsSlicerData = data;
            _queryStatsSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = PerformanceMonitor.Ui.TimeWindows.ChartAxis(hoursBack, fromDate, toDate, DateTime.UtcNow);
            if (data.Count > 0)
                QueryStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
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
            var queryStats = await Task.Run(() => _dataService.GetTopQueriesByCpuAsync(_serverId, 0, 50, e.StartUtc, e.EndUtc, ServerClock, SelectedDatabaseFilter));
            _queryStatsFilterMgr!.UpdateData(queryStats);
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
            if (data.Count > 0)
                QueryStoreSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadQueryStoreSlicerAsync failed: {ex.Message}");
        }
    }

    private async void OnQueryStoreSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
    {
        try
        {
            var qsData = await Task.Run(() => _dataService.GetQueryStoreTopQueriesAsync(_serverId, 0, 50, e.StartUtc, e.EndUtc, SelectedDatabaseFilter));
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
            if (data.Count > 0)
                ProcStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
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
            var procStats = await Task.Run(() => _dataService.GetTopProceduresByCpuAsync(_serverId, 0, 50, e.StartUtc, e.EndUtc, ServerClock, SelectedDatabaseFilter));
            _procStatsFilterMgr!.UpdateData(procStats);
            /* #4284: UTC comparison and banner bounds -- see the twin comment in OnQueryStatsSlicerChanged above. */
            await RefreshProcStatsComparisonAsync(e.StartUtc, e.EndUtc);
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.ProcedureStats, ProcStatsWindowTruncatedBanner, e.StartUtc, e.EndUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnProcStatsSlicerChanged failed: {ex.Message}");
        }
    }
}

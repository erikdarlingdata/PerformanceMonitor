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
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);

            var bpr = await Task.Run(() => _dataService.GetRecentBlockedProcessReportsAsync(_serverId, 0, fromServer, toServer));
            _blockedProcessFilterMgr!.UpdateData(bpr);
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
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);

            var dlr = await Task.Run(() => _dataService.GetRecentDeadlocksAsync(_serverId, 0, fromServer, toServer));
            _deadlockFilterMgr!.UpdateData(await ParseDeadlocksOffUiThreadAsync(dlr));
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

            // For narrow time ranges (drill-downs), pad the query by ±1 hour
            // so hourly slicer buckets overlap the display range
            DateTime? queryFrom = fromDate, queryTo = toDate;
            if (fromDate.HasValue && toDate.HasValue && (toDate.Value - fromDate.Value).TotalHours < 2)
            {
                queryFrom = fromDate.Value.AddHours(-1);
                queryTo = toDate.Value.AddHours(1);
            }

            var data = await Task.Run(() => _dataService.GetActiveQuerySlicerDataAsync(_serverId, hoursBack, queryFrom, queryTo));
            _activeQueriesSlicerData = data;
            _activeQueriesSlicerMetric = "Sessions";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, queryFrom, queryTo);
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
            // Slicer sends UTC dates; GetTimeRange expects server time for fromDate/toDate
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);

            var snapshots = await Task.Run(() => _dataService.GetLatestQuerySnapshotsAsync(_serverId, 0, fromServer, toServer));
            _querySnapshotsFilterMgr!.UpdateData(snapshots);
            LiveSnapshotIndicator.Text = "";
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

            var data = await Task.Run(() => _dataService.GetQueryStatsSlicerDataAsync(_serverId, hoursBack, fromDate, toDate));
            _queryStatsSlicerData = data;
            _queryStatsSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
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
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);
            var queryStats = await Task.Run(() => _dataService.GetTopQueriesByCpuAsync(_serverId, 0, 50, fromServer, toServer, UtcOffsetMinutes));
            _queryStatsFilterMgr!.UpdateData(queryStats);
            await RefreshQueryStatsComparisonAsync(fromServer, toServer);
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

            var data = await Task.Run(() => _dataService.GetQueryStoreSlicerDataAsync(_serverId, hoursBack, fromDate, toDate));
            _queryStoreSlicerData = data;
            _queryStoreSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
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
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);
            var qsData = await Task.Run(() => _dataService.GetQueryStoreTopQueriesAsync(_serverId, 0, 50, fromServer, toServer));
            _queryStoreFilterMgr!.UpdateData(qsData);
            await RefreshQueryStoreComparisonAsync(fromServer, toServer);
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

            var data = await Task.Run(() => _dataService.GetProcStatsSlicerDataAsync(_serverId, hoursBack, fromDate, toDate));
            _procStatsSlicerData = data;
            _procStatsSlicerMetric = "TotalCpu";
            var (slicerStart, slicerEnd) = GetSlicerTimeRange(hoursBack, fromDate, toDate);
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
            var fromServer = ServerTimeHelper.ToServerTime(e.StartUtc);
            var toServer = ServerTimeHelper.ToServerTime(e.EndUtc);
            var procStats = await Task.Run(() => _dataService.GetTopProceduresByCpuAsync(_serverId, 0, 50, fromServer, toServer, UtcOffsetMinutes));
            _procStatsFilterMgr!.UpdateData(procStats);
            await RefreshProcStatsComparisonAsync(fromServer, toServer);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] OnProcStatsSlicerChanged failed: {ex.Message}");
        }
    }
}

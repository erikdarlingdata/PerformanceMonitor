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
    private void BlockedProcessReportGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_blockingSlicerData == null || _blockingSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col))
        {
            if (e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
                col = b.Path.Path;
        }
        var (metric, label) = col switch
        {
            "WaitTimeMs" => ("TotalCpu", "Total Wait (sec)"),
            "BlockingSpid" => ("TotalElapsed", "Distinct Blockers"),
            "BlockedSpid" => ("TotalReads", "Distinct Blocked"),
            "DatabaseName" => ("TotalLogicalReads", "Distinct Databases"),
            _ => ("Events", "Blocking Events"),
        };

        if (metric == _blockingSlicerMetric) return;
        _blockingSlicerMetric = metric;

        foreach (var bucket in _blockingSlicerData)
        {
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "TotalElapsed" => bucket.TotalElapsed,
                "TotalReads" => bucket.TotalReads,
                "TotalLogicalReads" => bucket.TotalLogicalReads,
                _ => bucket.SessionCount,
            };
        }

        BlockingSlicer.UpdateMetric(label);
    }

    // ── Grid → Slicer Overlay (#683) ──

    private (DateTime? fromDate, DateTime? toDate) GetCurrentViewDates()
    {
        if (IsCustomRange)
        {
            var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
            var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
            if (fromLocal.HasValue && toLocal.HasValue)
                return (ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode),
                        ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode));
        }
        return (null, null);
    }

    /// <summary>
    /// Computes per-interval deltas from cumulative history values.
    /// Picks the metric field based on the current slicer sort metric.
    /// </summary>
    private static List<(DateTime TimeUtc, double Value)> ComputeQueryOverlayPoints(
        List<QueryStatsHistoryRow> history, string slicerMetric)
    {
        Func<QueryStatsHistoryRow, long> selector = slicerMetric switch
        {
            "TotalCpu" or "AvgCpu" => h => h.DeltaCpuUs,
            "TotalReads" or "AvgReads" => h => h.DeltaLogicalReads,
            "TotalWrites" => h => h.DeltaLogicalWrites,
            "TotalPhysReads" => h => h.DeltaPhysicalReads,
            _ => h => h.DeltaElapsedUs, // TotalElapsed, AvgElapsed, default
        };
        bool isMicroseconds = slicerMetric is "TotalCpu" or "AvgCpu" or "TotalElapsed" or "AvgElapsed";

        var points = new List<(DateTime TimeUtc, double Value)>();
        for (int i = 1; i < history.Count; i++)
        {
            var delta = selector(history[i]) - selector(history[i - 1]);
            if (delta > 0)
                points.Add((history[i].CollectionTime, isMicroseconds ? delta / 1000.0 : delta));
        }
        return points;
    }

    private static List<(DateTime TimeUtc, double Value)> ComputeProcOverlayPoints(
        List<ProcedureStatsHistoryRow> history, string slicerMetric)
    {
        Func<ProcedureStatsHistoryRow, long> selector = slicerMetric switch
        {
            "TotalCpu" or "AvgCpu" => h => h.DeltaCpuUs,
            "TotalReads" or "AvgReads" => h => h.DeltaLogicalReads,
            "TotalWrites" => h => h.DeltaLogicalWrites,
            "TotalPhysReads" => h => h.DeltaPhysicalReads,
            _ => h => h.DeltaElapsedUs,
        };
        bool isMicroseconds = slicerMetric is "TotalCpu" or "AvgCpu" or "TotalElapsed" or "AvgElapsed";

        var points = new List<(DateTime TimeUtc, double Value)>();
        for (int i = 1; i < history.Count; i++)
        {
            var delta = selector(history[i]) - selector(history[i - 1]);
            if (delta > 0)
                points.Add((history[i].CollectionTime, isMicroseconds ? delta / 1000.0 : delta));
        }
        return points;
    }

    private async void QueryStatsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (QueryStatsGrid.SelectedItem is not QueryStatsRow row || string.IsNullOrEmpty(row.QueryHash))
        {
            if (!_isRefreshing) QueryStatsSlicer.ClearOverlay();
            return;
        }

        try
        {
            var hoursBack = GetHoursBack();
            var (fromDate, toDate) = GetCurrentViewDates();
            var history = await Task.Run(() => _dataService.GetQueryStatsHistoryAsync(_serverId, row.DatabaseName, row.QueryHash, hoursBack, fromDate, toDate));

            var points = ComputeQueryOverlayPoints(history, _queryStatsSlicerMetric);
            QueryStatsSlicer.SetOverlay(points, row.QueryHash);
        }
        catch { QueryStatsSlicer.ClearOverlay(); }
    }

    private async void ProcedureStatsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (ProcedureStatsGrid.SelectedItem is not ProcedureStatsRow row || string.IsNullOrEmpty(row.ObjectName))
        {
            if (!_isRefreshing) ProcStatsSlicer.ClearOverlay();
            return;
        }

        try
        {
            var hoursBack = GetHoursBack();
            var (fromDate, toDate) = GetCurrentViewDates();
            var history = await Task.Run(() => _dataService.GetProcedureStatsHistoryAsync(_serverId, row.DatabaseName, row.SchemaName, row.ObjectName, hoursBack, fromDate, toDate));

            var points = ComputeProcOverlayPoints(history, _procStatsSlicerMetric);
            var label = row.ObjectName.Length > 30 ? row.ObjectName[..30] + "..." : row.ObjectName;
            ProcStatsSlicer.SetOverlay(points, label);
        }
        catch { ProcStatsSlicer.ClearOverlay(); }
    }

    private async void QueryStoreGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (QueryStoreGrid.SelectedItem is not QueryStoreRow row)
        {
            if (!_isRefreshing) QueryStoreSlicer.ClearOverlay();
            return;
        }

        try
        {
            var hoursBack = GetHoursBack();
            var (fromDate, toDate) = GetCurrentViewDates();
            var history = await Task.Run(() => _dataService.GetQueryStoreHistoryAsync(_serverId, row.DatabaseName, row.QueryId, row.PlanId, hoursBack, fromDate, toDate));

            // Query Store values are already per-interval averages, not cumulative
            Func<QueryStoreHistoryRow, double> selector = _queryStoreSlicerMetric switch
            {
                "TotalCpu" or "AvgCpu" => h => h.TotalCpuMs,
                "TotalReads" or "AvgReads" => h => h.AvgLogicalReads * h.ExecutionCount,
                _ => h => h.TotalDurationMs,
            };

            var points = history
                .Where(h => selector(h) > 0)
                .Select(h => (h.CollectionTime, selector(h)))
                .ToList();

            var qsLabel = !string.IsNullOrWhiteSpace(row.ModuleName)
                ? row.ModuleName
                : $"Query {row.QueryId} / Plan {row.PlanId}";
            QueryStoreSlicer.SetOverlay(points, qsLabel);
        }
        catch { QueryStoreSlicer.ClearOverlay(); }
    }

    private void QueryStatsGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (QueryStatsGrid.SelectedItem is not QueryStatsRow item) return;
        if (string.IsNullOrEmpty(item.DatabaseName) || string.IsNullOrEmpty(item.QueryHash)) return;

        var connStr = _credentialResolver.GetConnectionString(_server);
        var window = new Windows.QueryStatsHistoryWindow(_dataService, _serverId, item.DatabaseName, item.QueryHash, GetHoursBack(), item.QueryText, connStr);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    private void ProcedureStatsGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (ProcedureStatsGrid.SelectedItem is not ProcedureStatsRow item) return;
        if (string.IsNullOrEmpty(item.DatabaseName) || string.IsNullOrEmpty(item.ObjectName)) return;

        var connStr = _credentialResolver.GetConnectionString(_server);
        var window = new Windows.ProcedureHistoryWindow(_dataService, _serverId, item.DatabaseName, item.SchemaName, item.ObjectName, GetHoursBack(), connStr);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    private void QueryStoreGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (QueryStoreGrid.SelectedItem is not QueryStoreRow item) return;
        if (string.IsNullOrEmpty(item.DatabaseName) || item.QueryId == 0) return;

        var connStr = _credentialResolver.GetConnectionString(_server);
        var window = new Windows.QueryStoreHistoryWindow(_dataService, _serverId, item.DatabaseName, item.QueryId, item.PlanId, item.QueryText, GetHoursBack(), connStr);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }


    private void CollectionHealthGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
    {
        if (CollectionHealthGrid.SelectedItem is not CollectorHealthRow item) return;

        var window = new Windows.CollectionLogWindow(_dataService, _serverId, item.CollectorName);
        window.Owner = Window.GetWindow(this);
        window.ShowDialog();
    }

    private void QuerySnapshotsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_activeQueriesSlicerData == null || _activeQueriesSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col))
        {
            // Fall back to binding path
            if (e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
                col = b.Path.Path;
        }
        var (metric, label) = col switch
        {
            "CpuTimeMs" => ("TotalCpu", "Total CPU (ms)"),
            "TotalElapsedTimeMs" => ("TotalElapsed", "Total Elapsed (ms)"),
            "Reads" => ("TotalReads", "Total Reads"),
            "LogicalReads" => ("TotalLogicalReads", "Total Logical Reads"),
            "Writes" => ("TotalWrites", "Total Writes"),
            _ => ("Sessions", "Sessions"),
        };

        if (metric == _activeQueriesSlicerMetric) return;
        _activeQueriesSlicerMetric = metric;

        foreach (var bucket in _activeQueriesSlicerData)
        {
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "TotalElapsed" => bucket.TotalElapsed,
                "TotalReads" => bucket.TotalReads,
                "TotalLogicalReads" => bucket.TotalLogicalReads,
                "TotalWrites" => bucket.TotalWrites,
                _ => bucket.SessionCount,
            };
        }

        ActiveQueriesSlicer.UpdateMetric(label);
    }

    private void QueryStatsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_queryStatsSlicerData == null || _queryStatsSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
            col = b.Path.Path;

        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalElapsedMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgElapsedMs" => ("AvgElapsed", "Avg Duration (ms)"),
            "TotalLogicalReads" => ("TotalReads", "Total Reads"),
            "AvgReads" => ("AvgReads", "Avg Reads"),
            "TotalLogicalWrites" => ("TotalWrites", "Total Writes"),
            "TotalPhysicalReads" => ("TotalPhysReads", "Total Physical Reads"),
            _ => ("TotalCpu", "Total CPU (ms)"),
        };

        if (metric == _queryStatsSlicerMetric) return;
        _queryStatsSlicerMetric = metric;

        foreach (var bucket in _queryStatsSlicerData)
        {
            var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "AvgCpu" => bucket.TotalCpu / n,
                "TotalElapsed" => bucket.TotalElapsed,
                "AvgElapsed" => bucket.TotalElapsed / n,
                "TotalReads" => bucket.TotalReads,
                "AvgReads" => bucket.TotalReads / n,
                "TotalWrites" => bucket.TotalWrites,
                "TotalPhysReads" => bucket.TotalPhysicalReads,
                _ => bucket.TotalCpu,
            };
        }

        QueryStatsSlicer.UpdateMetric(label);

        // Re-compute overlay with new metric if a row is selected
        if (QueryStatsGrid.SelectedItem != null)
            QueryStatsGrid_SelectionChanged(QueryStatsGrid, null!);
    }

    private void QueryStoreGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_queryStoreSlicerData == null || _queryStoreSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
            col = b.Path.Path;

        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuTimeMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalDurationMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgDurationMs" => ("AvgElapsed", "Avg Duration (ms)"),
            "AvgLogicalReads" => ("TotalReads", "Avg Reads"),
            "AvgLogicalWrites" => ("TotalWrites", "Avg Writes"),
            "AvgPhysicalReads" => ("TotalReads", "Avg Physical Reads"),
            "TotalExecutions" => ("Sessions", "Executions"),
            _ => ("TotalCpu", "Total CPU (ms)"),
        };

        if (metric == _queryStoreSlicerMetric) return;
        _queryStoreSlicerMetric = metric;

        foreach (var bucket in _queryStoreSlicerData)
        {
            var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "AvgCpu" => bucket.TotalCpu / n,
                "TotalElapsed" => bucket.TotalElapsed,
                "AvgElapsed" => bucket.TotalElapsed / n,
                "TotalReads" => bucket.TotalReads,
                "TotalWrites" => bucket.TotalWrites,
                "Sessions" => bucket.SessionCount,
                _ => bucket.TotalCpu,
            };
        }

        QueryStoreSlicer.UpdateMetric(label);

        if (QueryStoreGrid.SelectedItem != null)
            QueryStoreGrid_SelectionChanged(QueryStoreGrid, null!);
    }

    private void ProcedureStatsGrid_Sorting(object sender, DataGridSortingEventArgs e)
    {
        if (_procStatsSlicerData == null || _procStatsSlicerData.Count == 0) return;

        var col = e.Column.SortMemberPath ?? "";
        if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
            col = b.Path.Path;

        var (metric, label) = col switch
        {
            "TotalCpuMs" => ("TotalCpu", "Total CPU (ms)"),
            "AvgCpuMs" => ("AvgCpu", "Avg CPU (ms)"),
            "TotalElapsedMs" => ("TotalElapsed", "Total Duration (ms)"),
            "AvgElapsedMs" => ("AvgElapsed", "Avg Duration (ms)"),
            "TotalLogicalReads" or "AvgReads" => ("TotalReads", "Total Reads"),
            "TotalLogicalWrites" => ("TotalWrites", "Total Writes"),
            "TotalPhysicalReads" => ("TotalReads", "Total Physical Reads"),
            _ => ("TotalCpu", "Total CPU (ms)"),
        };

        if (metric == _procStatsSlicerMetric) return;
        _procStatsSlicerMetric = metric;

        foreach (var bucket in _procStatsSlicerData)
        {
            var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
            bucket.Value = metric switch
            {
                "TotalCpu" => bucket.TotalCpu,
                "AvgCpu" => bucket.TotalCpu / n,
                "TotalElapsed" => bucket.TotalElapsed,
                "AvgElapsed" => bucket.TotalElapsed / n,
                "TotalReads" => bucket.TotalReads,
                "TotalWrites" => bucket.TotalWrites,
                _ => bucket.TotalCpu,
            };
        }

        ProcStatsSlicer.UpdateMetric(label);

        if (ProcedureStatsGrid.SelectedItem != null)
            ProcedureStatsGrid_SelectionChanged(ProcedureStatsGrid, null!);
    }

    private static void SetDefaultSortIfNone(DataGrid grid, string bindingPath, ListSortDirection direction)
    {
        if (grid.Items.SortDescriptions.Count > 0) return;

        grid.Items.SortDescriptions.Add(new SortDescription(bindingPath, direction));
        foreach (var column in grid.Columns)
        {
            if (column is DataGridBoundColumn bc &&
                bc.Binding is Binding b &&
                b.Path.Path == bindingPath)
            {
                column.SortDirection = direction;
                return;
            }
        }
    }
}

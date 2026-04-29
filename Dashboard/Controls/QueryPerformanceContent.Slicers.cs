/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class QueryPerformanceContent : UserControl
    {
        // ── Active Queries Slicer ──

        private async Task LoadActiveQueriesSlicerAsync()
        {
            if (_databaseService == null) return;
            try
            {
                // For narrow time ranges (drill-downs), pad the query by ±1 hour
                // so hourly slicer buckets overlap the display range
                var queryFrom = _activeQueriesFromDate;
                var queryTo = _activeQueriesToDate;
                if (queryFrom.HasValue && queryTo.HasValue && (queryTo.Value - queryFrom.Value).TotalHours < 2)
                {
                    queryFrom = queryFrom.Value.AddHours(-1);
                    queryTo = queryTo.Value.AddHours(1);
                }

                var data = await _databaseService.GetActiveQuerySlicerDataAsync(
                    _activeQueriesHoursBack, queryFrom, queryTo);
                var (slicerStart, slicerEnd) = GetSlicerTimeRange(_activeQueriesHoursBack, queryFrom, queryTo);
                if (data.Count > 0)
                    ActiveQueriesSlicer.LoadData(data, "Sessions", slicerStart, slicerEnd);
            }
            catch { }
        }

        private async void OnActiveQueriesSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
        {
            if (_databaseService == null) return;
            try
            {
                // Dashboard data is in server time; slicer sends server time directly
                var data = await _databaseService.GetQuerySnapshotsAsync(0, e.Start, e.End);
                _activeQueriesUnfilteredData = data;
                ActiveQueriesDataGrid.ItemsSource = data;
                ActiveQueriesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch { }
        }

        // ── Query Stats Slicer ──

        private List<Models.TimeSliceBucket>? _queryStatsSlicerData;
        private string _queryStatsSlicerMetric = "TotalCpu";

        private async Task LoadQueryStatsSlicerAsync()
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetQueryStatsSlicerDataAsync(
                    _queryStatsHoursBack, _queryStatsFromDate, _queryStatsToDate);
                _queryStatsSlicerData = data;
                _queryStatsSlicerMetric = "TotalCpu";
                var (slicerStart, slicerEnd) = GetSlicerTimeRange(_queryStatsHoursBack, _queryStatsFromDate, _queryStatsToDate);
                if (data.Count > 0)
                    QueryStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
            }
            catch { }
        }

        private async void OnQueryStatsSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetQueryStatsAsync(0, e.Start, e.End, fromSlicer: true);
                PopulateQueryStatsGrid(data);
            }
            catch { }
        }

        // ── Procedure Stats Slicer ──

        private List<Models.TimeSliceBucket>? _procStatsSlicerData;
        private string _procStatsSlicerMetric = "TotalCpu";

        private async Task LoadProcStatsSlicerAsync()
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetProcStatsSlicerDataAsync(
                    _procStatsHoursBack, _procStatsFromDate, _procStatsToDate);
                _procStatsSlicerData = data;
                _procStatsSlicerMetric = "TotalCpu";
                var (slicerStart, slicerEnd) = GetSlicerTimeRange(_procStatsHoursBack, _procStatsFromDate, _procStatsToDate);
                if (data.Count > 0)
                    ProcStatsSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
            }
            catch { }
        }

        private async void OnProcStatsSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetProcedureStatsAsync(0, e.Start, e.End, fromSlicer: true);
                PopulateProcStatsGrid(data);
            }
            catch { }
        }

        // ── Query Store Slicer ──

        private List<Models.TimeSliceBucket>? _queryStoreSlicerData;
        private string _queryStoreSlicerMetric = "TotalCpu";

        private async Task LoadQueryStoreSlicerAsync()
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetQueryStoreSlicerDataAsync(
                    _queryStoreHoursBack, _queryStoreFromDate, _queryStoreToDate);
                _queryStoreSlicerData = data;
                _queryStoreSlicerMetric = "TotalCpu";
                var (slicerStart, slicerEnd) = GetSlicerTimeRange(_queryStoreHoursBack, _queryStoreFromDate, _queryStoreToDate);
                if (data.Count > 0)
                    QueryStoreSlicer.LoadData(data, "Total CPU (ms)", slicerStart, slicerEnd);
            }
            catch { }
        }

        private async void OnQueryStoreSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetQueryStoreDataAsync(0, e.Start, e.End, fromSlicer: true);
                PopulateQueryStoreGrid(data);
            }
            catch { }
        }
    }
}

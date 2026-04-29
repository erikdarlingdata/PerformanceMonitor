/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Windows.Data;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitorDashboard.Helpers;
using ScottPlot.WPF;


namespace PerformanceMonitorDashboard.Controls
{
    public partial class ResourceMetricsContent : UserControl
    {
        #region TempDB Stats Tab

        private async Task RefreshTempdbStatsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Load TempDB usage stats
                var data = await _databaseService.GetTempdbStatsAsync(_tempdbStatsHoursBack, _tempdbStatsFromDate, _tempdbStatsToDate);
                LoadTempdbStatsChart(data, _tempdbStatsHoursBack, _tempdbStatsFromDate, _tempdbStatsToDate);

                // Load TempDB latency charts (moved from File I/O Latency tab)
                await LoadTempdbLatencyChartsAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading tempdb stats: {ex.Message}", ex);
            }
        }

        private async Task LoadTempdbLatencyChartsAsync()
        {
            if (_databaseService == null) return;

            DateTime rangeEnd = _tempdbStatsToDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = _tempdbStatsFromDate ?? rangeEnd.AddHours(-_tempdbStatsHoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var tempDbData = await _databaseService.GetFileIoLatencyTimeSeriesAsync(isTempDb: true, _tempdbStatsHoursBack, _tempdbStatsFromDate, _tempdbStatsToDate);
            LoadCombinedTempDbLatencyChart(tempDbData, xMin, xMax);
        }

        private void LoadCombinedTempDbLatencyChart(List<FileIoLatencyTimeSeriesItem> data, double xMin, double xMax)
        {
            DateTime rangeStart = DateTime.FromOADate(xMin);
            DateTime rangeEnd = DateTime.FromOADate(xMax);

            // Remove previously stored legend panel by reference (ScottPlot issue #4717)
            if (_legendPanels.TryGetValue(TempDbLatencyChart, out var existingPanel) && existingPanel != null)
            {
                TempDbLatencyChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[TempDbLatencyChart] = null;
            }
            TempDbLatencyChart.Plot.Clear();
            _tempDbLatencyHover?.Clear();
            TabHelpers.ApplyThemeToChart(TempDbLatencyChart);

            if (data != null && data.Count > 0)
            {
                // Aggregate all TempDB files into single read/write latency values per time point
                var aggregated = data
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => new
                    {
                        Time = g.Key,
                        AvgReadLatency = g.Average(x => (double)x.ReadLatencyMs),
                        AvgWriteLatency = g.Average(x => (double)x.WriteLatencyMs)
                    })
                    .ToList();

                // Read Latency series
                var (readXs, readYs) = TabHelpers.FillTimeSeriesGaps(
                    aggregated.Select(d => d.Time),
                    aggregated.Select(d => d.AvgReadLatency));
                var readScatter = TempDbLatencyChart.Plot.Add.Scatter(readXs, readYs);
                readScatter.LineWidth = 2;
                readScatter.MarkerSize = 5;
                readScatter.Color = TabHelpers.ChartColors[0];
                readScatter.LegendText = "Read Latency";
                _tempDbLatencyHover?.Add(readScatter, "Read Latency");

                // Write Latency series
                var (writeXs, writeYs) = TabHelpers.FillTimeSeriesGaps(
                    aggregated.Select(d => d.Time),
                    aggregated.Select(d => d.AvgWriteLatency));
                var writeScatter = TempDbLatencyChart.Plot.Add.Scatter(writeXs, writeYs);
                writeScatter.LineWidth = 2;
                writeScatter.MarkerSize = 5;
                writeScatter.Color = TabHelpers.ChartColors[2];
                writeScatter.LegendText = "Write Latency";
                _tempDbLatencyHover?.Add(writeScatter, "Write Latency");

                // Store legend panel reference for removal on refresh (ScottPlot issue #4717)
                _legendPanels[TempDbLatencyChart] = TempDbLatencyChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                TempDbLatencyChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = TempDbLatencyChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            TempDbLatencyChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempDbLatencyChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(TempDbLatencyChart);
            TempDbLatencyChart.Plot.YLabel("Latency (ms)");
            TabHelpers.LockChartVerticalAxis(TempDbLatencyChart);
            TempDbLatencyChart.Refresh();
        }

        private void LoadTempdbStatsChart(IEnumerable<TempdbStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(TempdbStatsChart, out var existingTempdbPanel) && existingTempdbPanel != null)
            {
                TempdbStatsChart.Plot.Axes.Remove(existingTempdbPanel);
                _legendPanels[TempdbStatsChart] = null;
            }
            TempdbStatsChart.Plot.Clear();
            _tempdbStatsHover?.Clear();
            TabHelpers.ApplyThemeToChart(TempdbStatsChart);

            var dataList = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<TempdbStatsItem>();
            if (dataList.Count > 0)
            {
                // User Objects series
                var (userXs, userYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.UserObjectReservedMb));
                var userScatter = TempdbStatsChart.Plot.Add.Scatter(userXs, userYs);
                userScatter.LineWidth = 2;
                userScatter.MarkerSize = 5;
                userScatter.Color = TabHelpers.ChartColors[0];
                userScatter.LegendText = "User Objects";
                _tempdbStatsHover?.Add(userScatter, "User Objects");

                // Version Store series
                var (versionXs, versionYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.VersionStoreReservedMb));
                var versionScatter = TempdbStatsChart.Plot.Add.Scatter(versionXs, versionYs);
                versionScatter.LineWidth = 2;
                versionScatter.MarkerSize = 5;
                versionScatter.Color = TabHelpers.ChartColors[1];
                versionScatter.LegendText = "Version Store";
                _tempdbStatsHover?.Add(versionScatter, "Version Store");

                // Internal Objects series
                var (internalXs, internalYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.InternalObjectReservedMb));
                var internalScatter = TempdbStatsChart.Plot.Add.Scatter(internalXs, internalYs);
                internalScatter.LineWidth = 2;
                internalScatter.MarkerSize = 5;
                internalScatter.Color = TabHelpers.ChartColors[2];
                internalScatter.LegendText = "Internal Objects";
                _tempdbStatsHover?.Add(internalScatter, "Internal Objects");

                // Unallocated (free space) series
                var (unallocXs, unallocYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.UnallocatedMb));
                if (unallocYs.Any(y => y > 0))
                {
                    var unallocScatter = TempdbStatsChart.Plot.Add.Scatter(unallocXs, unallocYs);
                    unallocScatter.LineWidth = 2;
                    unallocScatter.MarkerSize = 5;
                    unallocScatter.Color = TabHelpers.ChartColors[9];
                    unallocScatter.LegendText = "Unallocated";
                    _tempdbStatsHover?.Add(unallocScatter, "Unallocated");
                }

                // Top Task Total MB series (worst session's usage)
                var topTaskValues = dataList.Select(d => (double)(d.TopTaskTotalMb ?? 0)).ToArray();
                if (topTaskValues.Any(v => v > 0))
                {
                    var (topTaskXs, topTaskYs) = TabHelpers.FillTimeSeriesGaps(
                        dataList.Select(d => d.CollectionTime),
                        topTaskValues);
                    var topTaskScatter = TempdbStatsChart.Plot.Add.Scatter(topTaskXs, topTaskYs);
                    topTaskScatter.LineWidth = 2;
                    topTaskScatter.MarkerSize = 5;
                    topTaskScatter.Color = TabHelpers.ChartColors[3];
                    topTaskScatter.LegendText = "Top Task";
                }

                // Update summary panel with latest data point
                var latestData = dataList.LastOrDefault();
                UpdateTempdbStatsSummary(latestData);

                _legendPanels[TempdbStatsChart] = TempdbStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                TempdbStatsChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                UpdateTempdbStatsSummary(null);
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = TempdbStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            TempdbStatsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempdbStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TempdbStatsChart.Plot.Axes.AutoScaleY();
            TempdbStatsChart.Plot.YLabel("MB");
            TabHelpers.LockChartVerticalAxis(TempdbStatsChart);
            TempdbStatsChart.Refresh();
        }

        private void UpdateTempdbStatsSummary(TempdbStatsItem? data)
        {
            if (data != null)
            {
                TempdbSessionsText.Text = $"{data.TotalSessionsUsingTempdb} ({data.SessionsWithUserObjects} user, {data.SessionsWithInternalObjects} internal)";
                
                var warnings = new System.Collections.Generic.List<string>();
                if (data.VersionStoreHighWarning) warnings.Add("Version Store High");
                if (data.AllocationContentionWarning) warnings.Add("Allocation Contention");
                TempdbWarningsText.Text = warnings.Count > 0 ? string.Join(", ", warnings) : "None";
                TempdbWarningsText.Foreground = warnings.Count > 0 
                    ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.OrangeRed)
                    : (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            }
            else
            {
                TempdbSessionsText.Text = "N/A";
                TempdbWarningsText.Text = "N/A";
                TempdbWarningsText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            }
        }

        #endregion
    }
}

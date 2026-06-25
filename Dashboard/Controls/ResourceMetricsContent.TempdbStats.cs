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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;


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
                LoadTempdbSizeChart(data, _tempdbStatsHoursBack, _tempdbStatsFromDate, _tempdbStatsToDate);

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
                readScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("ReadLatency"));
                ChartStyle.StyleScatter(readScatter);
                readScatter.LegendText = "Read Latency";
                _tempDbLatencyHover?.Add(readScatter, "Read Latency");

                // Write Latency series
                var (writeXs, writeYs) = TabHelpers.FillTimeSeriesGaps(
                    aggregated.Select(d => d.Time),
                    aggregated.Select(d => d.AvgWriteLatency));
                var writeScatter = TempDbLatencyChart.Plot.Add.Scatter(writeXs, writeYs);
                writeScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("WriteLatency"));
                ChartStyle.StyleScatter(writeScatter);
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
                noDataText.LabelFontColor = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));
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
                userScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("UserObjects"));
                ChartStyle.StyleScatter(userScatter);
                userScatter.LegendText = "User Objects";
                _tempdbStatsHover?.Add(userScatter, "User Objects");

                // Version Store series
                var (versionXs, versionYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.VersionStoreReservedMb));
                var versionScatter = TempdbStatsChart.Plot.Add.Scatter(versionXs, versionYs);
                versionScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("VersionStore"));
                ChartStyle.StyleScatter(versionScatter);
                versionScatter.LegendText = "Version Store";
                _tempdbStatsHover?.Add(versionScatter, "Version Store");

                // Internal Objects series
                var (internalXs, internalYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.InternalObjectReservedMb));
                var internalScatter = TempdbStatsChart.Plot.Add.Scatter(internalXs, internalYs);
                internalScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("InternalObjects"));
                ChartStyle.StyleScatter(internalScatter);
                internalScatter.LegendText = "Internal Objects";
                _tempdbStatsHover?.Add(internalScatter, "Internal Objects");

                // Unallocated (free space) is intentionally NOT plotted here: it is almost always the
                // largest value, so it set the Y-axis and flattened the actual usage series into an
                // unreadable sliver. The tempdb total file size (used + unallocated) and its growth over
                // the window are surfaced on the dedicated TempdbSizeChart below, on their own scale.

                // Top Task Total MB series (worst session's usage)
                var topTaskValues = dataList.Select(d => (double)(d.TopTaskTotalMb ?? 0)).ToArray();
                if (topTaskValues.Any(v => v > 0))
                {
                    var (topTaskXs, topTaskYs) = TabHelpers.FillTimeSeriesGaps(
                        dataList.Select(d => d.CollectionTime),
                        topTaskValues);
                    var topTaskScatter = TempdbStatsChart.Plot.Add.Scatter(topTaskXs, topTaskYs);
                    topTaskScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("TopTempdbTask"));
                    ChartStyle.StyleScatter(topTaskScatter);
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
                noDataText.LabelFontColor = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            TempdbStatsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempdbStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TempdbStatsChart.Plot.Axes.AutoScaleY();
            TempdbStatsChart.Plot.YLabel("MB");
            TabHelpers.LockChartVerticalAxis(TempdbStatsChart);
            TempdbStatsChart.Refresh();
        }

        // Dedicated chart for tempdb TOTAL allocated size (used + unallocated free space) over time —
        // the growth trend the Unallocated band used to carry, on its own scale so it doesn't flatten
        // the usage breakdown above it.
        private void LoadTempdbSizeChart(IEnumerable<TempdbStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            TempdbSizeChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(TempdbSizeChart);
            _tempdbSizeHover?.Clear();

            var dataList = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<TempdbStatsItem>();
            if (dataList.Count > 0)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)(d.TotalReservedMb + d.UnallocatedMb)));
                var sizeScatter = TempdbSizeChart.Plot.Add.Scatter(xs, ys);
                sizeScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("UnallocatedTempdb"));
                ChartStyle.StyleScatter(sizeScatter);
                _tempdbSizeHover?.Add(sizeScatter, "Allocated MB");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = TempdbSizeChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            TempdbSizeChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempdbSizeChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TempdbSizeChart.Plot.Axes.AutoScaleY();
            TempdbSizeChart.Plot.YLabel("MB");
            TabHelpers.LockChartVerticalAxis(TempdbSizeChart);
            TempdbSizeChart.Refresh();
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

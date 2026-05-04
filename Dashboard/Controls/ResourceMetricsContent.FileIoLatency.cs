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
        #region File I/O Latency Tab

        private async Task LoadFileIoLatencyChartsAsync()
        {
            if (_databaseService == null) return;

            DateTime rangeEnd = _fileIoToDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = _fileIoFromDate ?? rangeEnd.AddHours(-_fileIoHoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var colors = TabHelpers.ChartColors;

            // Load User DB data only - TempDB latency moved to TempDB Stats tab
            var userDbData = await _databaseService.GetFileIoLatencyTimeSeriesAsync(isTempDb: false, _fileIoHoursBack, _fileIoFromDate, _fileIoToDate);
            LoadFileIoChart(UserDbReadLatencyChart, userDbData, d => d.ReadLatencyMs, "Read Latency (ms)", colors, xMin, xMax, _fileIoReadHover, d => d.ReadQueuedLatencyMs);
            LoadFileIoChart(UserDbWriteLatencyChart, userDbData, d => d.WriteLatencyMs, "Write Latency (ms)", colors, xMin, xMax, _fileIoWriteHover, d => d.WriteQueuedLatencyMs);
        }

        private void LoadFileIoChart(ScottPlot.WPF.WpfPlot chart, List<FileIoLatencyTimeSeriesItem> data, Func<FileIoLatencyTimeSeriesItem, decimal> latencySelector, string yLabel, ScottPlot.Color[] colors, double xMin, double xMax, Helpers.ChartHoverHelper? hover = null, Func<FileIoLatencyTimeSeriesItem, decimal>? queuedSelector = null)
        {
            DateTime rangeStart = DateTime.FromOADate(xMin);
            DateTime rangeEnd = DateTime.FromOADate(xMax);

            // Remove previously stored legend panel by reference (ScottPlot issue #4717)
            if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
            {
                chart.Plot.Axes.Remove(existingPanel);
                _legendPanels[chart] = null;
            }
            chart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(chart);
            hover?.Clear();

            // Check if any queued data exists (only render overlay if there's real data)
            bool hasQueuedData = queuedSelector != null && data != null && data.Any(d => queuedSelector(d) > 0);

            if (data != null && data.Count > 0)
            {
                // Get all unique time points for gap filling
                // Group by file (database + filename)
                var fileGroups = data.GroupBy(d => $"{d.DatabaseName}.{d.FileName}")
                    .Where(g => g.Any(x => latencySelector(x) > 0))
                    .OrderByDescending(g => g.Average(x => (double)latencySelector(x)))
                    .Take(10)
                    .ToList();

                int colorIndex = 0;
                foreach (var group in fileGroups)
                {
                    var fileData = group.OrderBy(d => d.CollectionTime).ToList();
                    if (fileData.Count >= 1)
                    {
                        var timePoints = fileData.Select(d => d.CollectionTime);
                        var values = fileData.Select(d => (double)latencySelector(d));
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                        var scatter = chart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        var color = colors[colorIndex % colors.Length];
                        scatter.Color = color;

                        // Use just the filename for legend (not database.filename which is redundant)
                        var fileName = fileData.First().FileName;
                        scatter.LegendText = fileName;
                        hover?.Add(scatter, fileName);

                        // Add queued I/O overlay as dashed line with same color
                        if (hasQueuedData)
                        {
                            var queuedValues = fileData.Select(d => (double)queuedSelector!(d));
                            if (queuedValues.Any(v => v > 0))
                            {
                                var (qxs, qys) = TabHelpers.FillTimeSeriesGaps(timePoints, queuedValues);
                                var queuedScatter = chart.Plot.Add.Scatter(qxs, qys);
                                queuedScatter.LineWidth = 2;
                                queuedScatter.MarkerSize = 0;
                                queuedScatter.Color = color;
                                queuedScatter.LinePattern = ScottPlot.LinePattern.Dashed;
                                queuedScatter.LegendText = $"{fileName} (queued)";
                                hover?.Add(queuedScatter, $"{fileName} (queued)");
                            }
                        }

                        colorIndex++;
                    }
                }

                if (fileGroups.Count > 0)
                {
                    // Store legend panel reference for removal on refresh (ScottPlot issue #4717)
                    _legendPanels[chart] = chart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    chart.Plot.Legend.FontSize = 12;
                }
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = chart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            chart.Plot.Axes.DateTimeTicksBottomDateChange();
            chart.Plot.Axes.SetLimitsX(xMin, xMax);
            chart.Plot.YLabel(yLabel);
            TabHelpers.LockChartVerticalAxis(chart);
            chart.Refresh();
        }

        private async Task LoadFileIoThroughputChartsAsync()
        {
            if (_databaseService == null) return;

            DateTime rangeEnd = _fileIoToDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = _fileIoFromDate ?? rangeEnd.AddHours(-_fileIoHoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var colors = TabHelpers.ChartColors;

            var throughputData = await _databaseService.GetFileIoThroughputTimeSeriesAsync(isTempDb: false, _fileIoHoursBack, _fileIoFromDate, _fileIoToDate);
            LoadFileIoChart(FileIoReadThroughputChart, throughputData, d => d.ReadThroughputMbPerSec, "Read Throughput (MB/s)", colors, xMin, xMax, _fileIoReadThroughputHover);
            LoadFileIoChart(FileIoWriteThroughputChart, throughputData, d => d.WriteThroughputMbPerSec, "Write Throughput (MB/s)", colors, xMin, xMax, _fileIoWriteThroughputHover);
        }

        #endregion
    }
}

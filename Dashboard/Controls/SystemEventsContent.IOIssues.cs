/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;


namespace PerformanceMonitorDashboard.Controls
{
    public partial class SystemEventsContent : UserControl
    {
        #region IO Issues Tab

        private async System.Threading.Tasks.Task RefreshIOIssuesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserIOIssuesAsync(_ioIssuesHoursBack, _ioIssuesFromDate, _ioIssuesToDate);
                _ioIssuesUnfilteredData = data;
                // IOIssuesDataGrid removed - chart only per todo.md #19
                LoadIOIssuesChart(data, _ioIssuesHoursBack, _ioIssuesFromDate, _ioIssuesToDate);
                LoadLongestPendingIOChart(data, _ioIssuesHoursBack, _ioIssuesFromDate, _ioIssuesToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading IO issues: {ex.Message}", ex);
            }
        }

        private void LoadIOIssuesChart(IEnumerable<HealthParserIOIssueItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(IOIssuesChart, out var existingIOIssuesPanel) && existingIOIssuesPanel != null)
            {
                IOIssuesChart.Plot.Axes.Remove(existingIOIssuesPanel);
                _legendPanels[IOIssuesChart] = null;
            }
            IOIssuesChart.Plot.Clear();
            _ioIssuesHover?.Clear();
            TabHelpers.ApplyThemeToChart(IOIssuesChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).OrderBy(d => d.EventTime).ToList() ?? new List<HealthParserIOIssueItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Group by hour
                var grouped = dataList
                    .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var timePoints = grouped.Select(g => g.Key);
                    double[] latchTimeouts = grouped.Select(g => (double)g.Sum(i => i.IoLatchTimeouts ?? 0)).ToArray();
                    double[] longIos = grouped.Select(g => (double)g.Sum(i => i.IntervalLongIos ?? 0)).ToArray();

                    if (latchTimeouts.Any(c => c > 0))
                    {
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, latchTimeouts.Select(c => c));
                        var scatter = IOIssuesChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = TabHelpers.ChartColors[3];
                        scatter.LegendText = "Latch Timeouts";
                        _ioIssuesHover?.Add(scatter, "Latch Timeouts");
                    }

                    if (longIos.Any(c => c > 0))
                    {
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, longIos.Select(c => c));
                        var scatter = IOIssuesChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = TabHelpers.ChartColors[2];
                        scatter.LegendText = "Long IOs";
                        _ioIssuesHover?.Add(scatter, "Long IOs");
                    }

                    _legendPanels[IOIssuesChart] = IOIssuesChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    IOIssuesChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = IOIssuesChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            IOIssuesChart.Plot.Axes.DateTimeTicksBottomDateChange();
            IOIssuesChart.Plot.Axes.SetLimitsX(xMin, xMax);
            IOIssuesChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(IOIssuesChart);
            IOIssuesChart.Refresh();
        }

        // IOIssuesFilter_Click removed - grid removed per todo.md #19
        private void LoadLongestPendingIOChart(IEnumerable<HealthParserIOIssueItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(LongestPendingIOChart, out var existingLongestPendingIOPanel) && existingLongestPendingIOPanel != null)
            {
                LongestPendingIOChart.Plot.Axes.Remove(existingLongestPendingIOPanel);
                _legendPanels[LongestPendingIOChart] = null;
            }
            LongestPendingIOChart.Plot.Clear();
            _longestPendingIoHover?.Clear();
            TabHelpers.ApplyThemeToChart(LongestPendingIOChart);

            var dataList = data?.Where(d => d.EventTime.HasValue && !string.IsNullOrEmpty(d.LongestPendingRequestsFilePath)).ToList() ?? new List<HealthParserIOIssueItem>();
            bool hasData = false;

            if (dataList.Count > 0)
            {
                // Get distinct file paths (top 5 by total duration)
                var filePathGroups = dataList
                    .GroupBy(d => d.LongestPendingRequestsFilePath)
                    .Select(g => new
                    {
                        FilePath = g.Key,
                        TotalDuration = g.Sum(d => double.TryParse(d.LongestPendingRequestsDurationMs, out var ms) ? ms : 0),
                        Items = g.ToList()
                    })
                    .OrderByDescending(g => g.TotalDuration)
                    .Take(5)
                    .ToList();

                if (filePathGroups.Count > 0)
                {
                    hasData = true;
                    var colors = TabHelpers.ChartColors;
                    int colorIndex = 0;

                    foreach (var group in filePathGroups)
                    {
                        // Extract just the filename from the path for the legend
                        var fileName = System.IO.Path.GetFileName(group.FilePath);
                        if (string.IsNullOrEmpty(fileName))
                            fileName = group.FilePath.Length > 30 ? "..." + group.FilePath.Substring(group.FilePath.Length - 27) : group.FilePath;

                        // Group by hour
                        var hourlyData = group.Items
                            .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                            .OrderBy(g => g.Key)
                            .ToList();

                        if (hourlyData.Count > 0)
                        {
                            var timePoints = hourlyData.Select(g => g.Key);
                            var durations = hourlyData.Select(g => g.Max(d => double.TryParse(d.LongestPendingRequestsDurationMs, out var ms) ? ms : 0)).ToArray();

                            var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                                timePoints,
                                durations.Select(d => d));

                            var scatter = LongestPendingIOChart.Plot.Add.Scatter(xs, ys);
                            scatter.LineWidth = 2;
                            scatter.MarkerSize = 5;
                            scatter.Color = colors[colorIndex % colors.Length];
                            scatter.LegendText = fileName;
                            _longestPendingIoHover?.Add(scatter, fileName);
                            colorIndex++;
                        }
                    }

                    _legendPanels[LongestPendingIOChart] = LongestPendingIOChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    LongestPendingIOChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = LongestPendingIOChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            LongestPendingIOChart.Plot.Axes.DateTimeTicksBottomDateChange();
            LongestPendingIOChart.Plot.Axes.SetLimitsX(xMin, xMax);
            LongestPendingIOChart.Plot.YLabel("Duration (ms)");
            TabHelpers.LockChartVerticalAxis(LongestPendingIOChart);
            LongestPendingIOChart.Refresh();
        }



        // ApplyIOIssuesFilters removed - grid removed per todo.md #19

        // UpdateIOIssuesFilterButtonStyles removed - grid removed per todo.md #19

        // IOIssuesFilterTextBox_TextChanged removed - grid removed per todo.md #19
        // IOIssuesNumericFilterTextBox_TextChanged removed - grid removed per todo.md #19

        #endregion
    }
}

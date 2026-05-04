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
        #region Latch Stats Tab

        private async Task RefreshLatchStatsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetLatchStatsTopNAsync(5, _latchStatsHoursBack, _latchStatsFromDate, _latchStatsToDate);
                LoadLatchStatsChart(data, _latchStatsHoursBack, _latchStatsFromDate, _latchStatsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading latch stats: {ex.Message}", ex);
            }
        }

        private void LoadLatchStatsChart(IEnumerable<LatchStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(LatchStatsChart, out var existingPanel) && existingPanel != null)
            {
                LatchStatsChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[LatchStatsChart] = null;
            }
            LatchStatsChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(LatchStatsChart);
            _latchStatsHover?.Clear();

            var dataList = data?.ToList() ?? new List<LatchStatsItem>();
            if (dataList.Count > 0)
            {
                // Get all unique time points for gap filling
                var topLatches = dataList.GroupBy(d => d.LatchClass)
                    .Select(g => new { LatchClass = g.Key, TotalWait = g.Sum(x => x.WaitTimeSec) })
                    .OrderByDescending(x => x.TotalWait)
                    .Take(5)
                    .Select(x => x.LatchClass)
                    .ToList();

                var colors = TabHelpers.ChartColors;
                int colorIndex = 0;

                foreach (var latchClass in topLatches)
                {
                    var latchData = dataList.Where(d => d.LatchClass == latchClass)
                        .OrderBy(d => d.CollectionTime)
                        .ToList();

                    if (latchData.Count >= 1)
                    {
                        var timePoints = latchData.Select(d => d.CollectionTime);
                        var values = latchData.Select(d => (double)(d.WaitTimeMsPerSecond ?? 0));
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                        var scatter = LatchStatsChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = colors[colorIndex % colors.Length];
                        scatter.LegendText = latchClass?.Length > 20 ? latchClass.Substring(0, 20) + "..." : latchClass ?? "";
                        _latchStatsHover?.Add(scatter, latchClass ?? "");
                        colorIndex++;
                    }
                }

                _legendPanels[LatchStatsChart] = LatchStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                LatchStatsChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = LatchStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            LatchStatsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            LatchStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(LatchStatsChart);
            LatchStatsChart.Plot.YLabel("Wait Time (ms/sec)");
            TabHelpers.LockChartVerticalAxis(LatchStatsChart);
            LatchStatsChart.Refresh();
        }

        #endregion
    }
}

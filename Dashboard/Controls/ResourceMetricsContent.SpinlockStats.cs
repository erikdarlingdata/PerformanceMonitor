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
        #region Spinlock Stats Tab

        private async Task RefreshSpinlockStatsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetSpinlockStatsTopNAsync(5, _spinlockStatsHoursBack, _spinlockStatsFromDate, _spinlockStatsToDate);
                LoadSpinlockStatsChart(data, _spinlockStatsHoursBack, _spinlockStatsFromDate, _spinlockStatsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading spinlock stats: {ex.Message}", ex);
            }
        }

        private void LoadSpinlockStatsChart(IEnumerable<SpinlockStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(SpinlockStatsChart, out var existingSpinlockPanel) && existingSpinlockPanel != null)
            {
                SpinlockStatsChart.Plot.Axes.Remove(existingSpinlockPanel);
                _legendPanels[SpinlockStatsChart] = null;
            }
            SpinlockStatsChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(SpinlockStatsChart);
            _spinlockStatsHover?.Clear();

            var dataList = data?.ToList() ?? new List<SpinlockStatsItem>();
            if (dataList.Count > 0)
            {
                // Get all unique time points for gap filling
                var topSpinlocks = dataList.GroupBy(d => d.SpinlockName)
                    .Select(g => new { SpinlockName = g.Key, TotalCollisions = g.Sum(x => x.CollisionsPerSecond ?? 0) })
                    .OrderByDescending(x => x.TotalCollisions)
                    .Take(5)
                    .Select(x => x.SpinlockName)
                    .ToList();

                var colors = TabHelpers.ChartColors;
                int colorIndex = 0;

                foreach (var spinlock in topSpinlocks)
                {
                    var spinlockData = dataList.Where(d => d.SpinlockName == spinlock)
                        .OrderBy(d => d.CollectionTime)
                        .ToList();

                    if (spinlockData.Count >= 1)
                    {
                        var timePoints = spinlockData.Select(d => d.CollectionTime);
                        var values = spinlockData.Select(d => (double)(d.CollisionsPerSecond ?? 0));
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                        var scatter = SpinlockStatsChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = colors[colorIndex % colors.Length];
                        scatter.LegendText = spinlock?.Length > 20 ? spinlock.Substring(0, 20) + "..." : spinlock ?? "";
                        _spinlockStatsHover?.Add(scatter, spinlock ?? "");
                        colorIndex++;
                    }
                }

                _legendPanels[SpinlockStatsChart] = SpinlockStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                SpinlockStatsChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SpinlockStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            SpinlockStatsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            SpinlockStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(SpinlockStatsChart);
            SpinlockStatsChart.Plot.YLabel("Collisions/sec");
            TabHelpers.LockChartVerticalAxis(SpinlockStatsChart);
            SpinlockStatsChart.Refresh();
        }

        #endregion
    }
}

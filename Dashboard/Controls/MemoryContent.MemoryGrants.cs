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
    public partial class MemoryContent : UserControl
    {
        #region Memory Grants

        private sealed class PoolGrantPoint
        {
            public DateTime CollectionTime { get; set; }
            public int PoolId { get; set; }
            public double AvailableMemoryMb { get; set; }
            public double GrantedMemoryMb { get; set; }
            public double UsedMemoryMb { get; set; }
            public double GranteeCount { get; set; }
            public double WaiterCount { get; set; }
            public double TimeoutErrorCountDelta { get; set; }
            public double ForcedGrantCountDelta { get; set; }
        }

        private async System.Threading.Tasks.Task RefreshMemoryGrantsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                if (!MemoryGrantSizingChart.Plot.GetPlottables().Any())
                {
                    MemoryGrantSizingLoading.IsLoading = true;
                    MemoryGrantSizingNoData.Visibility = Visibility.Collapsed;
                    MemoryGrantActivityNoData.Visibility = Visibility.Collapsed;
                }

                var data = await _databaseService.GetMemoryGrantStatsAsync(_memoryGrantsHoursBack, _memoryGrantsFromDate, _memoryGrantsToDate);
                var dataList = data.ToList();

                bool hasData = dataList.Count > 0;
                MemoryGrantSizingNoData.Visibility = hasData ? Visibility.Collapsed : Visibility.Visible;
                MemoryGrantActivityNoData.Visibility = hasData ? Visibility.Collapsed : Visibility.Visible;

                // Aggregate across resource_semaphore_id within each pool
                var aggregated = dataList
                    .GroupBy(d => new { d.CollectionTime, d.PoolId })
                    .Select(g => new PoolGrantPoint
                    {
                        CollectionTime = g.Key.CollectionTime,
                        PoolId = g.Key.PoolId,
                        AvailableMemoryMb = g.Sum(x => (double)(x.AvailableMemoryMb ?? 0)),
                        GrantedMemoryMb = g.Sum(x => (double)(x.GrantedMemoryMb ?? 0)),
                        UsedMemoryMb = g.Sum(x => (double)(x.UsedMemoryMb ?? 0)),
                        GranteeCount = g.Sum(x => (double)(x.GranteeCount ?? 0)),
                        WaiterCount = g.Sum(x => (double)(x.WaiterCount ?? 0)),
                        TimeoutErrorCountDelta = g.Sum(x => (double)(x.TimeoutErrorCountDelta ?? 0)),
                        ForcedGrantCountDelta = g.Sum(x => (double)(x.ForcedGrantCountDelta ?? 0))
                    })
                    .OrderBy(d => d.CollectionTime)
                    .ToList();

                LoadMemoryGrantSizingChart(aggregated, _memoryGrantsHoursBack, _memoryGrantsFromDate, _memoryGrantsToDate);
                LoadMemoryGrantActivityChart(aggregated, _memoryGrantsHoursBack, _memoryGrantsFromDate, _memoryGrantsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory grants: {ex.Message}");
                MemoryGrantSizingNoData.Visibility = Visibility.Visible;
                MemoryGrantActivityNoData.Visibility = Visibility.Visible;
            }
            finally
            {
                MemoryGrantSizingLoading.IsLoading = false;
                MemoryGrantActivityLoading.IsLoading = false;
            }
        }

        private void LoadMemoryGrantSizingChart(List<PoolGrantPoint> aggregated, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryGrantSizingChart, out var existingPanel) && existingPanel != null)
            {
                MemoryGrantSizingChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryGrantSizingChart] = null;
            }
            MemoryGrantSizingChart.Plot.Clear();
            _memoryGrantSizingHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryGrantSizingChart);

            var poolIds = aggregated.Select(d => d.PoolId).Distinct().OrderBy(id => id).ToList();
            int colorIndex = 0;
            var colors = TabHelpers.ChartColors;
            bool hasData = false;

            foreach (var poolId in poolIds)
            {
                var poolData = aggregated.Where(d => d.PoolId == poolId).OrderBy(d => d.CollectionTime).ToList();
                if (poolData.Count == 0) continue;
                hasData = true;

                var metrics = new (string Name, Func<PoolGrantPoint, double> Selector)[] {
                    ("Available MB", d => d.AvailableMemoryMb),
                    ("Granted MB", d => d.GrantedMemoryMb),
                    ("Used MB", d => d.UsedMemoryMb)
                };

                foreach (var (metricName, selector) in metrics)
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        poolData.Select(d => d.CollectionTime),
                        poolData.Select(selector));

                    if (xs.Length > 0)
                    {
                        var scatter = MemoryGrantSizingChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = colors[colorIndex % colors.Length];
                        var label = $"Pool {poolId}: {metricName}";
                        scatter.LegendText = label;
                        _memoryGrantSizingHover?.Add(scatter, label);
                        colorIndex++;
                    }
                }
            }

            if (hasData)
            {
                _legendPanels[MemoryGrantSizingChart] = MemoryGrantSizingChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                MemoryGrantSizingChart.Plot.Legend.FontSize = 12;
            }

            MemoryGrantSizingChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryGrantSizingChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryGrantSizingChart.Plot.YLabel("MB");
            MemoryGrantSizingChart.Plot.Axes.AutoScaleY();
            var limits = MemoryGrantSizingChart.Plot.Axes.GetLimits();
            MemoryGrantSizingChart.Plot.Axes.SetLimitsY(0, limits.Top * 1.05);
            TabHelpers.LockChartVerticalAxis(MemoryGrantSizingChart);
            MemoryGrantSizingChart.Refresh();
        }

        private void LoadMemoryGrantActivityChart(List<PoolGrantPoint> aggregated, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryGrantActivityChart, out var existingPanel) && existingPanel != null)
            {
                MemoryGrantActivityChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryGrantActivityChart] = null;
            }
            MemoryGrantActivityChart.Plot.Clear();
            _memoryGrantActivityHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryGrantActivityChart);

            var poolIds = aggregated.Select(d => d.PoolId).Distinct().OrderBy(id => id).ToList();
            int colorIndex = 0;
            var colors = TabHelpers.ChartColors;
            bool hasData = false;

            foreach (var poolId in poolIds)
            {
                var poolData = aggregated.Where(d => d.PoolId == poolId).OrderBy(d => d.CollectionTime).ToList();
                if (poolData.Count == 0) continue;
                hasData = true;

                var metrics = new (string Name, Func<PoolGrantPoint, double> Selector)[] {
                    ("Grantees", d => d.GranteeCount),
                    ("Waiters", d => d.WaiterCount),
                    ("Timeouts", d => d.TimeoutErrorCountDelta),
                    ("Forced Grants", d => d.ForcedGrantCountDelta)
                };

                foreach (var (metricName, selector) in metrics)
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        poolData.Select(d => d.CollectionTime),
                        poolData.Select(selector));

                    if (xs.Length > 0)
                    {
                        var scatter = MemoryGrantActivityChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = colors[colorIndex % colors.Length];
                        var label = $"Pool {poolId}: {metricName}";
                        scatter.LegendText = label;
                        _memoryGrantActivityHover?.Add(scatter, label);
                        colorIndex++;
                    }
                }
            }

            if (hasData)
            {
                _legendPanels[MemoryGrantActivityChart] = MemoryGrantActivityChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                MemoryGrantActivityChart.Plot.Legend.FontSize = 12;
            }

            MemoryGrantActivityChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryGrantActivityChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryGrantActivityChart.Plot.YLabel("Count");
            MemoryGrantActivityChart.Plot.Axes.AutoScaleY();
            var limits = MemoryGrantActivityChart.Plot.Axes.GetLimits();
            MemoryGrantActivityChart.Plot.Axes.SetLimitsY(0, limits.Top * 1.05);
            TabHelpers.LockChartVerticalAxis(MemoryGrantActivityChart);
            MemoryGrantActivityChart.Refresh();
        }

        #endregion
    }
}

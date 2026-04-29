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
        #region Plan Cache

        private async System.Threading.Tasks.Task RefreshPlanCacheAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetPlanCacheStatsAsync(_planCacheHoursBack, _planCacheFromDate, _planCacheToDate);
                LoadPlanCacheChart(data.ToList(), _planCacheHoursBack, _planCacheFromDate, _planCacheToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading plan cache stats: {ex.Message}");
            }
        }

        private void LoadPlanCacheChart(IEnumerable<PlanCacheStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(PlanCacheChart, out var existingPanel) && existingPanel != null)
            {
                PlanCacheChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[PlanCacheChart] = null;
            }
            PlanCacheChart.Plot.Clear();
            _planCacheHover?.Clear();
            TabHelpers.ApplyThemeToChart(PlanCacheChart);

            var dataList = data?.ToList() ?? new List<PlanCacheStatsItem>();
            if (dataList.Count > 0)
            {
                // Group by collection time and get single-use vs multi-use sizes
                var grouped = dataList.GroupBy(d => d.CollectionTime)
                    .Select(g => new {
                        Time = g.Key,
                        SingleUseSizeMb = g.Sum(x => x.SingleUseSizeMb),
                        MultiUseSizeMb = g.Sum(x => x.MultiUseSizeMb)
                    })
                    .OrderBy(x => x.Time)
                    .ToList();

                if (grouped.Count > 0)
                {
                    // Single-Use series with gap filling
                    var (singleXs, singleYs) = TabHelpers.FillTimeSeriesGaps(
                        grouped.Select(d => d.Time),
                        grouped.Select(d => (double)d.SingleUseSizeMb));

                    var singleScatter = PlanCacheChart.Plot.Add.Scatter(singleXs, singleYs);
                    singleScatter.LineWidth = 2;
                    singleScatter.MarkerSize = 5;
                    singleScatter.Color = TabHelpers.ChartColors[3];
                    singleScatter.LegendText = "Single-Use";
                    _planCacheHover?.Add(singleScatter, "Single-Use");

                    // Multi-Use series with gap filling
                    var (multiXs, multiYs) = TabHelpers.FillTimeSeriesGaps(
                        grouped.Select(d => d.Time),
                        grouped.Select(d => (double)d.MultiUseSizeMb));

                    var multiScatter = PlanCacheChart.Plot.Add.Scatter(multiXs, multiYs);
                    multiScatter.LineWidth = 2;
                    multiScatter.MarkerSize = 5;
                    multiScatter.Color = TabHelpers.ChartColors[1];
                    multiScatter.LegendText = "Multi-Use";
                    _planCacheHover?.Add(multiScatter, "Multi-Use");

                    _legendPanels[PlanCacheChart] = PlanCacheChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    PlanCacheChart.Plot.Legend.FontSize = 12;
                }

                // Update summary panel with latest data point
                var latestOldestPlan = dataList
                    .Where(d => d.OldestPlanCreateTime.HasValue)
                    .OrderByDescending(d => d.CollectionTime)
                    .FirstOrDefault();
                var latestTime = dataList.Max(d => d.CollectionTime);
                int totalPlans = dataList.Where(d => d.CollectionTime == latestTime).Sum(d => d.TotalPlans);
                UpdatePlanCacheSummary(latestOldestPlan, totalPlans);
            }
            else
            {
                UpdatePlanCacheSummary(null, 0);
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = PlanCacheChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            PlanCacheChart.Plot.Axes.DateTimeTicksBottomDateChange();
            PlanCacheChart.Plot.Axes.SetLimitsX(xMin, xMax);
            PlanCacheChart.Plot.YLabel("MB");
            // Fixed negative space for legend
            PlanCacheChart.Plot.Axes.AutoScaleY();
            var planCacheLimits = PlanCacheChart.Plot.Axes.GetLimits();
            PlanCacheChart.Plot.Axes.SetLimitsY(0, planCacheLimits.Top * 1.05);

            TabHelpers.LockChartVerticalAxis(PlanCacheChart);
            PlanCacheChart.Refresh();
        }

        private void UpdatePlanCacheSummary(PlanCacheStatsItem? oldestPlanData, int totalPlans)
        {
            if (oldestPlanData?.OldestPlanCreateTime != null)
            {
                var age = ServerTimeHelper.ServerNow - oldestPlanData.OldestPlanCreateTime.Value;
                string ageText;
                if (age.TotalDays >= 1)
                    ageText = $"{age.Days}d {age.Hours}h";
                else if (age.TotalHours >= 1)
                    ageText = $"{age.Hours}h {age.Minutes}m";
                else
                    ageText = $"{age.Minutes}m";

                PlanCacheOldestPlanText.Text = ageText;

                // Color code based on age - older is better (more stable)
                if (age.TotalHours < 1)
                    PlanCacheOldestPlanText.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.OrangeRed);
                else if (age.TotalHours < 24)
                    PlanCacheOldestPlanText.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Orange);
                else
                    PlanCacheOldestPlanText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            }
            else
            {
                PlanCacheOldestPlanText.Text = "N/A";
                PlanCacheOldestPlanText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            }

            PlanCacheTotalPlansText.Text = totalPlans > 0 ? totalPlans.ToString("N0", CultureInfo.CurrentCulture) : "N/A";
        }

        #endregion
    }
}

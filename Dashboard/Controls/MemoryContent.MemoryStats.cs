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
        #region Memory Stats

        private async System.Threading.Tasks.Task RefreshMemoryStatsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetMemoryStatsAsync(_memoryStatsHoursBack, _memoryStatsFromDate, _memoryStatsToDate);
                var dataList = data.ToList();
                LoadMemoryStatsOverviewChart(dataList, _memoryStatsHoursBack, _memoryStatsFromDate, _memoryStatsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory stats: {ex.Message}");
            }
        }

        private void LoadMemoryStatsOverviewChart(List<MemoryStatsItem> memoryData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryStatsOverviewChart, out var existingPanel) && existingPanel != null)
            {
                MemoryStatsOverviewChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryStatsOverviewChart] = null;
            }
            MemoryStatsOverviewChart.Plot.Clear();
            _memoryStatsOverviewHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryStatsOverviewChart);

            var dataList = memoryData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<MemoryStatsItem>();
            // Total Memory series with gap filling
            var (totalXs, totalYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.TotalMemoryMb));

            // Buffer Pool series with gap filling
            var (bufferXs, bufferYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.BufferPoolMb));

            // Plan Cache series with gap filling
            var (cacheXs, cacheYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.PlanCacheMb));

            // Available Physical Memory series with gap filling
            var (availXs, availYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.AvailablePhysicalMemoryMb));

            if (totalXs.Length > 0)
            {
                // Add pressure warning spans first (so they appear behind the lines)
                AddPressureWarningSpans(dataList);

                var totalScatter = MemoryStatsOverviewChart.Plot.Add.Scatter(totalXs, totalYs);
                totalScatter.LineWidth = 2;
                totalScatter.MarkerSize = 5;
                totalScatter.Color = TabHelpers.ChartColors[9];
                totalScatter.LegendText = "Total Memory";
                _memoryStatsOverviewHover?.Add(totalScatter, "Total Memory");

                var bufferScatter = MemoryStatsOverviewChart.Plot.Add.Scatter(bufferXs, bufferYs);
                bufferScatter.LineWidth = 2;
                bufferScatter.MarkerSize = 5;
                bufferScatter.Color = TabHelpers.ChartColors[0];
                bufferScatter.LegendText = "Buffer Pool";
                _memoryStatsOverviewHover?.Add(bufferScatter, "Buffer Pool");

                var cacheScatter = MemoryStatsOverviewChart.Plot.Add.Scatter(cacheXs, cacheYs);
                cacheScatter.LineWidth = 2;
                cacheScatter.MarkerSize = 5;
                cacheScatter.Color = TabHelpers.ChartColors[1];
                cacheScatter.LegendText = "Plan Cache";
                _memoryStatsOverviewHover?.Add(cacheScatter, "Plan Cache");

                var availScatter = MemoryStatsOverviewChart.Plot.Add.Scatter(availXs, availYs);
                availScatter.LineWidth = 2;
                availScatter.MarkerSize = 5;
                availScatter.Color = TabHelpers.ChartColors[2];
                availScatter.LegendText = "Available Physical";
                _memoryStatsOverviewHover?.Add(availScatter, "Available Physical");

                _legendPanels[MemoryStatsOverviewChart] = MemoryStatsOverviewChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                MemoryStatsOverviewChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryStatsOverviewChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryStatsOverviewChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryStatsOverviewChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryStatsOverviewChart.Plot.YLabel("MB");
            // Fixed negative space for legend
            MemoryStatsOverviewChart.Plot.Axes.AutoScaleY();
            var memOverviewLimits = MemoryStatsOverviewChart.Plot.Axes.GetLimits();
            MemoryStatsOverviewChart.Plot.Axes.SetLimitsY(0, memOverviewLimits.Top * 1.05);

            TabHelpers.LockChartVerticalAxis(MemoryStatsOverviewChart);
            MemoryStatsOverviewChart.Refresh();

            // Update summary panel
            UpdateMemoryStatsSummaryPanel(dataList);
        }

        private void AddPressureWarningSpans(List<MemoryStatsItem> dataList)
        {
            // Track whether we've added legend entries (only want one per type)
            bool bpLegendAdded = false;
            bool pcLegendAdded = false;

            // Find time ranges where pressure warnings are active
            foreach (var item in dataList)
            {
                if (item.BufferPoolPressureWarning || item.PlanCachePressureWarning)
                {
                    // Add a vertical line at this time point to indicate pressure
                    var x = item.CollectionTime.ToOADate();
                    var vline = MemoryStatsOverviewChart.Plot.Add.VerticalLine(x);
                    vline.LineWidth = 1;
                    vline.LinePattern = ScottPlot.LinePattern.Dotted;

                    if (item.BufferPoolPressureWarning && item.PlanCachePressureWarning)
                    {
                        vline.Color = TabHelpers.ChartColors[3].WithAlpha(0.5);
                        // Add legend entry for BP pressure (covers "both" case too)
                        if (!bpLegendAdded)
                        {
                            vline.LegendText = "BP Pressure";
                            bpLegendAdded = true;
                        }
                    }
                    else if (item.BufferPoolPressureWarning)
                    {
                        vline.Color = TabHelpers.ChartColors[3].WithAlpha(0.3);
                        if (!bpLegendAdded)
                        {
                            vline.LegendText = "BP Pressure";
                            bpLegendAdded = true;
                        }
                    }
                    else
                    {
                        vline.Color = TabHelpers.ChartColors[2].WithAlpha(0.3);
                        if (!pcLegendAdded)
                        {
                            vline.LegendText = "PC Pressure";
                            pcLegendAdded = true;
                        }
                    }
                }
            }
        }

        private void UpdateMemoryStatsSummaryPanel(List<MemoryStatsItem> dataList)
        {
            if (dataList == null || dataList.Count == 0)
            {
                MemoryStatsPhysicalText.Text = "N/A";
                MemoryStatsSqlServerText.Text = "N/A";
                MemoryStatsTargetText.Text = "N/A";
                MemoryStatsBPPercentText.Text = "N/A";
                MemoryStatsPCPercentText.Text = "N/A";
                MemoryStatsUtilPercentText.Text = "N/A";
                MemoryStatsPressureText.Text = "None";
                MemoryStatsPressureText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush");
                return;
            }

            // Use the most recent data point
            var latest = dataList.OrderByDescending(d => d.CollectionTime).First();

            // Absolute GB values
            MemoryStatsPhysicalText.Text = latest.TotalPhysicalMemoryMb.HasValue
                ? $"{latest.TotalPhysicalMemoryMb.Value / 1024.0m:F1} GB"
                : "N/A";

            MemoryStatsSqlServerText.Text = $"{latest.PhysicalMemoryInUseMb / 1024.0m:F1} GB";

            MemoryStatsTargetText.Text = latest.CommittedTargetMemoryMb.HasValue
                ? $"{latest.CommittedTargetMemoryMb.Value / 1024.0m:F1} GB"
                : "N/A";

            // Buffer Pool and Plan Cache with GB and percentage
            MemoryStatsBPPercentText.Text = latest.BufferPoolPercentage.HasValue
                ? $"{latest.BufferPoolMb / 1024.0m:F1} GB ({latest.BufferPoolPercentage:F1}%)"
                : $"{latest.BufferPoolMb / 1024.0m:F1} GB";

            MemoryStatsPCPercentText.Text = latest.PlanCachePercentage.HasValue
                ? $"{latest.PlanCacheMb / 1024.0m:F1} GB ({latest.PlanCachePercentage:F1}%)"
                : $"{latest.PlanCacheMb / 1024.0m:F1} GB";

            MemoryStatsUtilPercentText.Text = $"{latest.MemoryUtilizationPercentage}%";

            // Build pressure status text
            var pressures = new List<string>();
            if (latest.BufferPoolPressureWarning) pressures.Add("BP");
            if (latest.PlanCachePressureWarning) pressures.Add("PC");

            if (pressures.Count > 0)
            {
                MemoryStatsPressureText.Text = string.Join(", ", pressures);
                MemoryStatsPressureText.Foreground = new System.Windows.Media.SolidColorBrush(
                    System.Windows.Media.Color.FromRgb(0xFF, 0x69, 0x69)); // Light red
            }
            else
            {
                MemoryStatsPressureText.Text = "None";
                MemoryStatsPressureText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            }
        }

        #endregion
    }
}

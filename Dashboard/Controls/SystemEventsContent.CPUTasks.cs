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
        #region CPU Tasks Tab

        private async System.Threading.Tasks.Task RefreshCPUTasksAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserCPUTasksAsync(_cpuTasksHoursBack, _cpuTasksFromDate, _cpuTasksToDate);
                // Grid removed per todo.md #15 - chart + summary only
                LoadCPUTasksChart(data, _cpuTasksHoursBack, _cpuTasksFromDate, _cpuTasksToDate);
                UpdateCPUTasksSummaryPanel(data);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading CPU tasks: {ex.Message}", ex);
            }
        }

        private void LoadCPUTasksChart(IEnumerable<HealthParserCPUTasksItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(CPUTasksChart, out var existingCPUTasksPanel) && existingCPUTasksPanel != null)
            {
                CPUTasksChart.Plot.Axes.Remove(existingCPUTasksPanel);
                _legendPanels[CPUTasksChart] = null;
            }
            CPUTasksChart.Plot.Clear();
            _cpuTasksHover?.Clear();
            TabHelpers.ApplyThemeToChart(CPUTasksChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserCPUTasksItem>();
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

                    // Workers Created series
                    double[] workersCreated = grouped.Select(g => (double)g.Max(i => i.WorkersCreated ?? 0)).ToArray();
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, workersCreated.Select(c => c));
                    var scatter = CPUTasksChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[0];
                    scatter.LegendText = "Workers Created";
                    _cpuTasksHover?.Add(scatter, "Workers Created");

                    // Max Workers threshold line (horizontal)
                    var maxWorkersValue = dataList.Max(d => d.MaxWorkers ?? 0);
                    if (maxWorkersValue > 0)
                    {
                        var hLine = CPUTasksChart.Plot.Add.HorizontalLine(maxWorkersValue);
                        hLine.Color = TabHelpers.ChartColors[2];
                        hLine.LineWidth = 2;
                        hLine.LinePattern = ScottPlot.LinePattern.Dashed;
                        hLine.LegendText = $"Max Workers ({maxWorkersValue})";
                    }

                    // Add scatter point markers for serious issues (grouped by hour at Y=0)
                    // Unresolvable Deadlocks - red points
                    var unresolvableDLByHour = dataList
                        .Where(d => d.HasUnresolvableDeadlockOccurred == true && d.EventTime.HasValue)
                        .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                        .Select(g => new { Time = g.Key, Count = g.Count() })
                        .OrderBy(x => x.Time)
                        .ToList();
                    if (unresolvableDLByHour.Count > 0)
                    {
                        var dlXs = unresolvableDLByHour.Select(b => b.Time.ToOADate()).ToArray();
                        var dlYs = unresolvableDLByHour.Select(b => 0.0).ToArray();
                        var dlScatter = CPUTasksChart.Plot.Add.Scatter(dlXs, dlYs);
                        dlScatter.LineWidth = 0;
                        dlScatter.Color = TabHelpers.ChartColors[3];
                        dlScatter.LegendText = "Unresolvable DL";
                        dlScatter.MarkerSize = 10;
                        dlScatter.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                    }

                    // Scheduler Deadlocks - orange points
                    var schedDLByHour = dataList
                        .Where(d => d.HasDeadlockedSchedulersOccurred == true && d.EventTime.HasValue)
                        .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                        .Select(g => new { Time = g.Key, Count = g.Count() })
                        .OrderBy(x => x.Time)
                        .ToList();
                    if (schedDLByHour.Count > 0)
                    {
                        var schedXs = schedDLByHour.Select(b => b.Time.ToOADate()).ToArray();
                        var schedYs = schedDLByHour.Select(b => 0.0).ToArray();
                        var schedScatter = CPUTasksChart.Plot.Add.Scatter(schedXs, schedYs);
                        schedScatter.LineWidth = 0;
                        schedScatter.Color = TabHelpers.ChartColors[2];
                        schedScatter.LegendText = "Sched Deadlock";
                        schedScatter.MarkerSize = 10;
                        schedScatter.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                    }

                    // Blocking events - yellow points sized by count per hour
                    var blockingByHour = dataList
                        .Where(d => d.DidBlockingOccur == true && d.EventTime.HasValue)
                        .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                        .Select(g => new { Time = g.Key, Count = g.Count() })
                        .OrderBy(x => x.Time)
                        .ToList();
                    if (blockingByHour.Count > 0)
                    {
                        // Place markers at Y=0 (bottom of chart)
                        var blockingXs = blockingByHour.Select(b => b.Time.ToOADate()).ToArray();
                        var blockingYs = blockingByHour.Select(b => 0.0).ToArray(); // At bottom
                        var blockingScatter = CPUTasksChart.Plot.Add.Scatter(blockingXs, blockingYs);
                        blockingScatter.LineWidth = 0; // No connecting line
                        blockingScatter.Color = TabHelpers.ChartColors[6];
                        blockingScatter.LegendText = "Blocking";
                        // Size points based on count - min 8, max 20, scaled by count
                        var maxCount = blockingByHour.Max(b => b.Count);
                        var sizes = blockingByHour.Select(b => 8f + (12f * b.Count / Math.Max(maxCount, 1))).ToArray();
                        // ScottPlot 5 doesn't support per-point sizes easily, so use average size
                        var avgSize = sizes.Average();
                        blockingScatter.MarkerSize = (float)Math.Max(avgSize, 10);
                        blockingScatter.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                    }

                    _legendPanels[CPUTasksChart] = CPUTasksChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    CPUTasksChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = CPUTasksChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            CPUTasksChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CPUTasksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            CPUTasksChart.Plot.YLabel("Workers");
            TabHelpers.LockChartVerticalAxis(CPUTasksChart);
            CPUTasksChart.Refresh();
        }

        private void UpdateCPUTasksSummaryPanel(List<HealthParserCPUTasksItem> dataList)
        {
            if (dataList == null || dataList.Count == 0)
            {
                CPUTasksUnresolvableDLText.Text = "0";
                CPUTasksSchedDLText.Text = "0";
                CPUTasksBlockingText.Text = "0";
                CPUTasksPendingNoBlockText.Text = "0";
                // Reset colors
                CPUTasksUnresolvableDLText.Foreground = new System.Windows.Media.SolidColorBrush(
                    (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));
                CPUTasksSchedDLText.Foreground = new System.Windows.Media.SolidColorBrush(
                    (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));
                return;
            }

            // Unresolvable Deadlocks count
            var unresolvableDLCount = dataList.Count(d => d.HasUnresolvableDeadlockOccurred == true);
            CPUTasksUnresolvableDLText.Text = unresolvableDLCount.ToString("N0", CultureInfo.CurrentCulture);
            CPUTasksUnresolvableDLText.Foreground = unresolvableDLCount > 0
                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Red)
                : new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));

            // Scheduler Deadlocks count
            var schedDLCount = dataList.Count(d => d.HasDeadlockedSchedulersOccurred == true);
            CPUTasksSchedDLText.Text = schedDLCount.ToString("N0", CultureInfo.CurrentCulture);
            CPUTasksSchedDLText.Foreground = schedDLCount > 0
                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Red)
                : new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));

            // Blocking Events count
            var blockingCount = dataList.Count(d => d.DidBlockingOccur == true);
            CPUTasksBlockingText.Text = blockingCount.ToString("N0", CultureInfo.CurrentCulture);

            // Pending w/o Blocking - events with pending tasks > 0 but no blocking
            var pendingNoBlockCount = dataList.Count(d => (d.PendingTasks ?? 0) > 0 && d.DidBlockingOccur != true);
            CPUTasksPendingNoBlockText.Text = pendingNoBlockCount.ToString("N0", CultureInfo.CurrentCulture);
        }

        // CPUTasksFilter_Click removed - grid removed per todo.md #15

        // ApplyCPUTasksFilters removed - grid removed per todo.md #15

        // UpdateCPUTasksFilterButtonStyles removed - grid removed per todo.md #15

        // CPUTasksFilterTextBox_TextChanged removed - grid removed per todo.md #15

        // CPUTasksNumericFilterTextBox_TextChanged removed - grid removed per todo.md #15

        // CPUTasksBoolFilter_Changed removed - grid removed per todo.md #15

        // ApplyCPUTasksFilter removed - grid removed per todo.md #15

        #endregion
    }
}

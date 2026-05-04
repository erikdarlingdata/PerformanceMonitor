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
        #region Memory Node OOM Tab

        private async System.Threading.Tasks.Task RefreshMemoryNodeOOMAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserMemoryNodeOOMAsync(_memoryNodeOOMHoursBack, _memoryNodeOOMFromDate, _memoryNodeOOMToDate);
                _memoryNodeOOMUnfilteredData = data;

                // Load charts
                LoadMemoryNodeOOMUtilChart(data, _memoryNodeOOMHoursBack, _memoryNodeOOMFromDate, _memoryNodeOOMToDate);
                LoadMemoryNodeOOMMemoryChart(data, _memoryNodeOOMHoursBack, _memoryNodeOOMFromDate, _memoryNodeOOMToDate);
                LoadMemoryNodeOOMChart(data, _memoryNodeOOMHoursBack, _memoryNodeOOMFromDate, _memoryNodeOOMToDate);

                // Update memory state indicators
                UpdateMemoryStateIndicators(data);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory node OOM: {ex.Message}", ex);
            }
        }

        private void LoadMemoryNodeOOMChart(IEnumerable<HealthParserMemoryNodeOOMItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryNodeOOMChart, out var existingMemoryNodeOOMPanel) && existingMemoryNodeOOMPanel != null)
            {
                MemoryNodeOOMChart.Plot.Axes.Remove(existingMemoryNodeOOMPanel);
                _legendPanels[MemoryNodeOOMChart] = null;
            }
            MemoryNodeOOMChart.Plot.Clear();
            _memoryNodeOomHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryNodeOOMChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserMemoryNodeOOMItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Group by hour and count OOM events
                var grouped = dataList
                    .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        grouped.Select(g => g.Key),
                        grouped.Select(g => (double)g.Count()));

                    var scatter = MemoryNodeOOMChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[3];
                    scatter.LegendText = "OOM Event Count";
                    _memoryNodeOomHover?.Add(scatter, "OOM Event Count");

                    _legendPanels[MemoryNodeOOMChart] = MemoryNodeOOMChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryNodeOOMChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryNodeOOMChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryNodeOOMChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryNodeOOMChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryNodeOOMChart.Plot.YLabel("Event Count");
            TabHelpers.LockChartVerticalAxis(MemoryNodeOOMChart);
            MemoryNodeOOMChart.Refresh();
        }

        private void LoadMemoryNodeOOMUtilChart(IEnumerable<HealthParserMemoryNodeOOMItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryNodeOOMUtilChart, out var existingPanel) && existingPanel != null)
            {
                MemoryNodeOOMUtilChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryNodeOOMUtilChart] = null;
            }
            MemoryNodeOOMUtilChart.Plot.Clear();
            _memoryNodeOomUtilHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryNodeOOMUtilChart);

            var dataList = data?.Where(d => d.EventTime.HasValue && d.MemoryUtilizationPct.HasValue).ToList() ?? new List<HealthParserMemoryNodeOOMItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                var xs = dataList.Select(d => d.EventTime!.Value.ToOADate()).ToArray();
                var ys = dataList.Select(d => (double)d.MemoryUtilizationPct!.Value).ToArray();

                if (xs.Length > 0)
                {
                    hasData = true;
                    var scatter = MemoryNodeOOMUtilChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[0];
                    _memoryNodeOomUtilHover?.Add(scatter, "Memory Utilization %");
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryNodeOOMUtilChart.Plot.Add.Text("No data", xCenter, 50);
                noDataText.LabelFontSize = 12;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryNodeOOMUtilChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryNodeOOMUtilChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.LockChartVerticalAxis(MemoryNodeOOMUtilChart);
            MemoryNodeOOMUtilChart.Refresh();
        }

        private void LoadMemoryNodeOOMMemoryChart(IEnumerable<HealthParserMemoryNodeOOMItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryNodeOOMMemoryChart, out var existingPanel) && existingPanel != null)
            {
                MemoryNodeOOMMemoryChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryNodeOOMMemoryChart] = null;
            }
            MemoryNodeOOMMemoryChart.Plot.Clear();
            _memoryNodeOomMemoryHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryNodeOOMMemoryChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserMemoryNodeOOMItem>();
            bool hasData = false;

            if (dataList.Count > 0)
            {
                // Target Memory (Green)
                var targetData = dataList.Where(d => d.TargetKb.HasValue).ToList();
                if (targetData.Count > 0)
                {
                    hasData = true;
                    var xs = targetData.Select(d => d.EventTime!.Value.ToOADate()).ToArray();
                    var ys = targetData.Select(d => (double)d.TargetKb!.Value / 1024.0).ToArray();
                    var scatter = MemoryNodeOOMMemoryChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[1];
                    scatter.LegendText = "Target";
                    _memoryNodeOomMemoryHover?.Add(scatter, "Target");
                }

                // Committed Memory (Orange)
                var committedData = dataList.Where(d => d.CommittedKb.HasValue).ToList();
                if (committedData.Count > 0)
                {
                    hasData = true;
                    var xs = committedData.Select(d => d.EventTime!.Value.ToOADate()).ToArray();
                    var ys = committedData.Select(d => (double)d.CommittedKb!.Value / 1024.0).ToArray();
                    var scatter = MemoryNodeOOMMemoryChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[2];
                    scatter.LegendText = "Committed";
                    _memoryNodeOomMemoryHover?.Add(scatter, "Committed");
                }

                // Total Page File (Purple)
                var totalPFData = dataList.Where(d => d.TotalPageFileKb.HasValue).ToList();
                if (totalPFData.Count > 0)
                {
                    hasData = true;
                    var xs = totalPFData.Select(d => d.EventTime!.Value.ToOADate()).ToArray();
                    var ys = totalPFData.Select(d => (double)d.TotalPageFileKb!.Value / 1024.0).ToArray();
                    var scatter = MemoryNodeOOMMemoryChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[4];
                    scatter.LegendText = "Total Page File";
                    _memoryNodeOomMemoryHover?.Add(scatter, "Total Page File");
                }

                // Available Page File (Cyan)
                var availPFData = dataList.Where(d => d.AvailablePageFileKb.HasValue).ToList();
                if (availPFData.Count > 0)
                {
                    hasData = true;
                    var xs = availPFData.Select(d => d.EventTime!.Value.ToOADate()).ToArray();
                    var ys = availPFData.Select(d => (double)d.AvailablePageFileKb!.Value / 1024.0).ToArray();
                    var scatter = MemoryNodeOOMMemoryChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[5];
                    scatter.LegendText = "Avail Page File";
                    _memoryNodeOomMemoryHover?.Add(scatter, "Avail Page File");
                }

                if (hasData)
                {
                    _legendPanels[MemoryNodeOOMMemoryChart] = MemoryNodeOOMMemoryChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryNodeOOMMemoryChart.Plot.Legend.FontSize = 11;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryNodeOOMMemoryChart.Plot.Add.Text("No data", xCenter, 0.5);
                noDataText.LabelFontSize = 12;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryNodeOOMMemoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryNodeOOMMemoryChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.LockChartVerticalAxis(MemoryNodeOOMMemoryChart);
            MemoryNodeOOMMemoryChart.Refresh();
        }

        private void UpdateMemoryStateIndicators(IEnumerable<HealthParserMemoryNodeOOMItem> data)
        {
            // Indicator colors
            var healthyBrush = new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#81C784"));
            var warningBrush = new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#FFD54F"));
            var criticalBrush = new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#E57373"));
            var unknownBrush = new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#888888"));

            var dataList = data?.ToList() ?? new List<HealthParserMemoryNodeOOMItem>();

            // Count occurrences of each memory state
            int sysHighCount = dataList.Count(d => string.Equals(d.IsSystemPhysicalMemoryHigh, "true", StringComparison.OrdinalIgnoreCase));
            int sysLowCount = dataList.Count(d => string.Equals(d.IsSystemPhysicalMemoryLow, "true", StringComparison.OrdinalIgnoreCase));
            int procMemLowCount = dataList.Count(d => string.Equals(d.IsProcessPhysicalMemoryLow, "true", StringComparison.OrdinalIgnoreCase));
            int procVirtLowCount = dataList.Count(d => string.Equals(d.IsProcessVirtualMemoryLow, "true", StringComparison.OrdinalIgnoreCase));

            // Update count text
            SysMemHighCountText.Text = $"({sysHighCount})";
            SysMemLowCountText.Text = $"({sysLowCount})";
            ProcMemLowCountText.Text = $"({procMemLowCount})";
            ProcVirtLowCountText.Text = $"({procVirtLowCount})";

            // Update indicator colors
            // Sys High: Green is good (memory available), so show green when count > 0
            SysMemHighIndicator.Fill = sysHighCount > 0 ? healthyBrush : unknownBrush;

            // Sys Low: Red is bad (low memory), show critical if any occurrences
            SysMemLowIndicator.Fill = sysLowCount > 0 ? criticalBrush : unknownBrush;

            // Process Physical Memory Low: Red is bad
            ProcMemLowIndicator.Fill = procMemLowCount > 0 ? criticalBrush : unknownBrush;

            // Process Virtual Memory Low: Red is bad
            ProcVirtLowIndicator.Fill = procVirtLowCount > 0 ? warningBrush : unknownBrush;
        }

        // MemoryNodeOOM filter methods removed - DataGrid removed per GitHub issue #13

        #endregion
    }
}

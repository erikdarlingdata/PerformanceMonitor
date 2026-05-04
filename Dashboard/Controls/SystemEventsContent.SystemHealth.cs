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
        #region System Health Tab

        private async System.Threading.Tasks.Task RefreshSystemHealthAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserSystemHealthAsync(_systemHealthHoursBack, _systemHealthFromDate, _systemHealthToDate);
                _systemHealthUnfilteredData = data;
                // SystemHealthDataGrid removed - chart only per todo.md #18
                LoadCorruptionEventsCharts(data, _systemHealthHoursBack, _systemHealthFromDate, _systemHealthToDate);
                LoadContentionEventsCharts(data, _systemHealthHoursBack, _systemHealthFromDate, _systemHealthToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading system health: {ex.Message}", ex);
            }
        }

        private void LoadCorruptionEventsCharts(List<HealthParserSystemHealthItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            // Calculate the time range for X-axis limits
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var orderedData = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<HealthParserSystemHealthItem>();
            bool hasData = orderedData.Count > 0;
            // Bad Pages Detected Chart
            BadPagesChart.Plot.Clear();
            _badPagesHover?.Clear();
            TabHelpers.ApplyThemeToChart(BadPagesChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.BadPagesDetected ?? 0)));
                var scatter = BadPagesChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[3];
                _badPagesHover?.Add(scatter, "Bad Pages");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BadPagesChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BadPagesChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BadPagesChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BadPagesChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(BadPagesChart);
            BadPagesChart.Refresh();

            // Interval Dump Requests Chart
            DumpRequestsChart.Plot.Clear();
            _dumpRequestsHover?.Clear();
            TabHelpers.ApplyThemeToChart(DumpRequestsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.IntervalDumpRequests ?? 0)));
                var scatter = DumpRequestsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[2];
                _dumpRequestsHover?.Add(scatter, "Dump Requests");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = DumpRequestsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            DumpRequestsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            DumpRequestsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            DumpRequestsChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(DumpRequestsChart);
            DumpRequestsChart.Refresh();

            // Access Violations Chart
            AccessViolationsChart.Plot.Clear();
            _accessViolationsHover?.Clear();
            TabHelpers.ApplyThemeToChart(AccessViolationsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.IsAccessViolationOccurred ?? 0)));
                var scatter = AccessViolationsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[4];
                _accessViolationsHover?.Add(scatter, "Access Violations");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = AccessViolationsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            AccessViolationsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            AccessViolationsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            AccessViolationsChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(AccessViolationsChart);
            AccessViolationsChart.Refresh();

            // Write Access Violations Chart
            WriteAccessViolationsChart.Plot.Clear();
            _writeAccessViolationsHover?.Clear();
            TabHelpers.ApplyThemeToChart(WriteAccessViolationsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.WriteAccessViolationCount ?? 0)));
                var scatter = WriteAccessViolationsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[0];
                _writeAccessViolationsHover?.Add(scatter, "Write Access Violations");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = WriteAccessViolationsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            WriteAccessViolationsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            WriteAccessViolationsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            WriteAccessViolationsChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(WriteAccessViolationsChart);
            WriteAccessViolationsChart.Refresh();
        }


        private void LoadContentionEventsCharts(List<HealthParserSystemHealthItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            // Calculate the time range for X-axis limits
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var orderedData = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<HealthParserSystemHealthItem>();
            bool hasData = orderedData.Count > 0;
            // Non-Yielding Tasks Chart
            NonYieldingTasksChart.Plot.Clear();
            _nonYieldingTasksHover?.Clear();
            TabHelpers.ApplyThemeToChart(NonYieldingTasksChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.NonYieldingTasksReported ?? 0)));
                var scatter = NonYieldingTasksChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[3];
                _nonYieldingTasksHover?.Add(scatter, "Non-Yielding Tasks");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = NonYieldingTasksChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            NonYieldingTasksChart.Plot.Axes.DateTimeTicksBottomDateChange();
            NonYieldingTasksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            NonYieldingTasksChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(NonYieldingTasksChart);
            NonYieldingTasksChart.Refresh();

            // Latch Warnings Chart
            LatchWarningsChart.Plot.Clear();
            _latchWarningsHover?.Clear();
            TabHelpers.ApplyThemeToChart(LatchWarningsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.LatchWarnings ?? 0)));
                var scatter = LatchWarningsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[2];
                _latchWarningsHover?.Add(scatter, "Latch Warnings");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = LatchWarningsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            LatchWarningsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            LatchWarningsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            LatchWarningsChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(LatchWarningsChart);
            LatchWarningsChart.Refresh();

            // Sick Spinlocks by Type Chart (multi-series with legend)
            if (_legendPanels.TryGetValue(SickSpinlocksChart, out var existingSickSpinlocksPanel) && existingSickSpinlocksPanel != null)
            {
                SickSpinlocksChart.Plot.Axes.Remove(existingSickSpinlocksPanel);
                _legendPanels[SickSpinlocksChart] = null;
            }
            SickSpinlocksChart.Plot.Clear();
            _sickSpinlocksHover?.Clear();
            TabHelpers.ApplyThemeToChart(SickSpinlocksChart);
            if (hasData)
            {
                // Group by spinlock type and create a series for each
                var spinlockTypes = orderedData
                    .Where(d => !string.IsNullOrEmpty(d.SickSpinlockType))
                    .Select(d => d.SickSpinlockType)
                    .Distinct()
                    .Take(5) // Limit to top 5 types to avoid chart clutter
                    .ToList();

                var colors = TabHelpers.ChartColors;
                int colorIndex = 0;

                foreach (var spinlockType in spinlockTypes)
                {
                    var typeData = orderedData
                        .Where(d => d.SickSpinlockType == spinlockType)
                        .ToList();

                    if (typeData.Count > 0)
                    {
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                            typeData.Select(d => d.CollectionTime),
                            typeData.Select(d => (double)(d.SpinlockBackoffs ?? 1))); // Use backoffs count or 1 if null
                        var scatter = SickSpinlocksChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = colors[colorIndex % colors.Length];
                        scatter.LegendText = spinlockType ?? "Unknown";
                        _sickSpinlocksHover?.Add(scatter, spinlockType ?? "Unknown");
                        colorIndex++;
                    }
                }

                if (spinlockTypes.Count > 0)
                {
                    _legendPanels[SickSpinlocksChart] = SickSpinlocksChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    SickSpinlocksChart.Plot.Legend.FontSize = 12;
                }
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SickSpinlocksChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            SickSpinlocksChart.Plot.Axes.DateTimeTicksBottomDateChange();
            SickSpinlocksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            SickSpinlocksChart.Plot.YLabel("Backoffs");
            TabHelpers.LockChartVerticalAxis(SickSpinlocksChart);
            SickSpinlocksChart.Refresh();

            // CPU Comparison Chart (SQL CPU vs System CPU)
            if (_legendPanels.TryGetValue(CpuComparisonChart, out var existingCpuComparisonPanel) && existingCpuComparisonPanel != null)
            {
                CpuComparisonChart.Plot.Axes.Remove(existingCpuComparisonPanel);
                _legendPanels[CpuComparisonChart] = null;
            }
            CpuComparisonChart.Plot.Clear();
            _cpuComparisonHover?.Clear();
            TabHelpers.ApplyThemeToChart(CpuComparisonChart);
            if (hasData)
            {
                // System CPU series
                var (sysXs, sysYs) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.SystemCpuUtilization ?? 0)));
                var sysScatter = CpuComparisonChart.Plot.Add.Scatter(sysXs, sysYs);
                sysScatter.LineWidth = 2;
                sysScatter.MarkerSize = 5;
                sysScatter.Color = TabHelpers.ChartColors[0];
                sysScatter.LegendText = "System CPU %";
                _cpuComparisonHover?.Add(sysScatter, "System CPU %");

                // SQL CPU series
                var (sqlXs, sqlYs) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.SqlCpuUtilization ?? 0)));
                var sqlScatter = CpuComparisonChart.Plot.Add.Scatter(sqlXs, sqlYs);
                sqlScatter.LineWidth = 2;
                sqlScatter.MarkerSize = 5;
                sqlScatter.Color = TabHelpers.ChartColors[1];
                sqlScatter.LegendText = "SQL CPU %";
                _cpuComparisonHover?.Add(sqlScatter, "SQL CPU %");

                _legendPanels[CpuComparisonChart] = CpuComparisonChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                CpuComparisonChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = CpuComparisonChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            CpuComparisonChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CpuComparisonChart.Plot.Axes.SetLimitsX(xMin, xMax);
            CpuComparisonChart.Plot.Axes.SetLimitsY(0, 100); // Fixed Y-axis for CPU percentage
            CpuComparisonChart.Plot.YLabel("CPU %");
            TabHelpers.LockChartVerticalAxis(CpuComparisonChart);
            CpuComparisonChart.Refresh();
        }
        private void SystemHealthFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            ShowFilterPopup(button, columnName, "SystemHealth", _systemHealthFilters,
                args => { },  // Handled in FilterPopup_FilterApplied
                () => { });   // Handled in FilterPopup_FilterCleared
        }

        // ApplySystemHealthFilters removed - grid removed per todo.md #18

        // UpdateSystemHealthFilterButtonStyles removed - grid removed per todo.md #18

        // SystemHealthFilterTextBox_TextChanged removed - grid removed per todo.md #18

        // SystemHealthNumericFilterTextBox_TextChanged removed - grid removed per todo.md #18

        #endregion
    }
}

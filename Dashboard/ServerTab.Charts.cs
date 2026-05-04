/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Controls;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using ScottPlot.WPF;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        // Chart hover tooltips
        private Helpers.ChartHoverHelper? _resourceOverviewCpuHover;
        private Helpers.ChartHoverHelper? _resourceOverviewMemoryHover;
        private Helpers.ChartHoverHelper? _resourceOverviewIoHover;
        private Helpers.ChartHoverHelper? _resourceOverviewWaitHover;
        private Helpers.ChartHoverHelper? _lockWaitStatsHover;
        private Helpers.ChartHoverHelper? _blockingEventsHover;
        private Helpers.ChartHoverHelper? _blockingDurationHover;
        private Helpers.ChartHoverHelper? _deadlocksHover;
        private Helpers.ChartHoverHelper? _deadlockWaitTimeHover;
        private Helpers.ChartHoverHelper? _collectorDurationHover;
        private Helpers.ChartHoverHelper? _currentWaitsDurationHover;
        private Helpers.ChartHoverHelper? _currentWaitsBlockedHover;

        private void OnThemeChanged(string _)
        {
            foreach (var field in GetType().GetFields(
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance))
            {
                if (field.GetValue(this) is ScottPlot.WPF.WpfPlot chart)
                {
                    Helpers.TabHelpers.ApplyThemeToChart(chart);
                    chart.Refresh();
                }
            }
        }

        private void ApplyThemeToChart(ScottPlot.WPF.WpfPlot chart)
        {
            TabHelpers.ApplyThemeToChart(chart);
        }

        private void SetupChartContextMenus()
        {
            // Resource Overview charts
            var overviewCpuMenu = Helpers.TabHelpers.SetupChartContextMenu(ResourceOverviewCpuChart, "CPU_Utilization", "collect.cpu_utilization_stats");
            AddChartDrillDownMenuItem(ResourceOverviewCpuChart, overviewCpuMenu, _resourceOverviewCpuHover, "Show Active Queries at This Time", OnQueryDrillDown);
            var overviewMemMenu = Helpers.TabHelpers.SetupChartContextMenu(ResourceOverviewMemoryChart, "Memory_Utilization", "collect.memory_stats");
            AddChartDrillDownMenuItem(ResourceOverviewMemoryChart, overviewMemMenu, _resourceOverviewMemoryHover, "Show Active Queries at This Time", OnQueryDrillDown);
            var overviewIoMenu = Helpers.TabHelpers.SetupChartContextMenu(ResourceOverviewIoChart, "IO_Latency", "collect.file_io_stats");
            AddChartDrillDownMenuItem(ResourceOverviewIoChart, overviewIoMenu, _resourceOverviewIoHover, "Show Active Queries at This Time", OnQueryDrillDown);
            var overviewWaitMenu = Helpers.TabHelpers.SetupChartContextMenu(ResourceOverviewWaitChart, "Wait_Stats", "collect.wait_stats");
            AddChartDrillDownMenuItem(ResourceOverviewWaitChart, overviewWaitMenu, _resourceOverviewWaitHover, "Show Active Queries at This Time", OnQueryDrillDown);

            // Blocking Stats charts
            var blockingEventsMenu = Helpers.TabHelpers.SetupChartContextMenu(BlockingStatsBlockingEventsChart, "Blocking_Events", "collect.blocking_deadlock_stats");
            AddChartDrillDownMenuItem(BlockingStatsBlockingEventsChart, blockingEventsMenu, _blockingEventsHover, "Show Blocking at This Time", OnBlockingChartDrillDown);
            var blockingDurationMenu = Helpers.TabHelpers.SetupChartContextMenu(BlockingStatsDurationChart, "Blocking_Duration", "collect.blocking_deadlock_stats");
            AddChartDrillDownMenuItem(BlockingStatsDurationChart, blockingDurationMenu, _blockingDurationHover, "Show Blocking at This Time", OnBlockingChartDrillDown);
            var deadlocksMenu = Helpers.TabHelpers.SetupChartContextMenu(BlockingStatsDeadlocksChart, "Deadlocks", "collect.blocking_deadlock_stats");
            AddChartDrillDownMenuItem(BlockingStatsDeadlocksChart, deadlocksMenu, _deadlocksHover, "Show Deadlocks at This Time", OnDeadlockChartDrillDown);
            var deadlockWaitMenu = Helpers.TabHelpers.SetupChartContextMenu(BlockingStatsDeadlockWaitTimeChart, "Deadlock_Wait_Time", "collect.blocking_deadlock_stats");
            AddChartDrillDownMenuItem(BlockingStatsDeadlockWaitTimeChart, deadlockWaitMenu, _deadlockWaitTimeHover, "Show Deadlocks at This Time", OnDeadlockChartDrillDown);

            // Lock Wait Stats chart
            var lockWaitMenu = Helpers.TabHelpers.SetupChartContextMenu(LockWaitStatsChart, "Lock_Wait_Stats", "collect.wait_stats");
            AddChartDrillDownMenuItem(LockWaitStatsChart, lockWaitMenu, _lockWaitStatsHover, "Show Blocking at This Time", OnBlockingChartDrillDown);

            // Current Waits charts
            var cwDurationMenu = Helpers.TabHelpers.SetupChartContextMenu(CurrentWaitsDurationChart, "Current_Waits_Duration", "collect.waiting_tasks");
            AddChartDrillDownMenuItem(CurrentWaitsDurationChart, cwDurationMenu, _currentWaitsDurationHover, "Show Active Queries at This Time", OnQueryDrillDown);
            var cwBlockedMenu = Helpers.TabHelpers.SetupChartContextMenu(CurrentWaitsBlockedChart, "Current_Waits_Blocked", "collect.waiting_tasks");
            AddChartDrillDownMenuItem(CurrentWaitsBlockedChart, cwBlockedMenu, _currentWaitsBlockedHover, "Show Active Queries at This Time", OnQueryDrillDown);

            // Query Performance Trends charts now handled by QueryPerformanceContent UserControl

            // Server Utilization Trends charts now handled by ResourceMetricsContent UserControl

            // System Health charts now handled by SystemEventsContent UserControl
            // Memory Analysis charts now handled by MemoryContent UserControl
        }

        /// <summary>
        /// Locks the vertical axis of a chart so mouse wheel zooming only affects the time (X) axis.
        /// </summary>
        private void LockChartVerticalAxis(WpfPlot chart)
        {
            TabHelpers.LockChartVerticalAxis(chart);
        }

        private void LoadBlockingStatsCharts(List<BlockingDeadlockStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            // Calculate the time range for X-axis limits (use server time, not local time)
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var orderedData = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<BlockingDeadlockStatsItem>();

            // Get all unique time points for consistent X-axis across all charts
            // Blocking Events Chart (raw per-interval count, not delta)
            BlockingStatsBlockingEventsChart.Plot.Clear();
            _blockingEventsHover?.Clear();
            ApplyThemeToChart(BlockingStatsBlockingEventsChart);
            var (blockingXs, blockingYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.BlockingEventCount));
            if (blockingXs.Length > 0)
            {
                var scatter = BlockingStatsBlockingEventsChart.Plot.Add.Scatter(blockingXs, blockingYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[0];
                _blockingEventsHover?.Add(scatter, "Blocking Events");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsBlockingEventsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsBlockingEventsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingStatsBlockingEventsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsBlockingEventsChart.Plot.YLabel("Count");
            LockChartVerticalAxis(BlockingStatsBlockingEventsChart);
            BlockingStatsBlockingEventsChart.Refresh();

            // Blocking Duration Chart (raw per-interval total, not delta)
            BlockingStatsDurationChart.Plot.Clear();
            _blockingDurationHover?.Clear();
            ApplyThemeToChart(BlockingStatsDurationChart);
            var (durationXs, durationYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.TotalBlockingDurationMs));
            if (durationXs.Length > 0)
            {
                var scatter = BlockingStatsDurationChart.Plot.Add.Scatter(durationXs, durationYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[2];
                _blockingDurationHover?.Add(scatter, "Blocking Duration");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsDurationChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingStatsDurationChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsDurationChart.Plot.YLabel("Duration (ms)");
            LockChartVerticalAxis(BlockingStatsDurationChart);
            BlockingStatsDurationChart.Refresh();

            // Deadlock Count Chart (raw per-interval count, not delta)
            BlockingStatsDeadlocksChart.Plot.Clear();
            _deadlocksHover?.Clear();
            ApplyThemeToChart(BlockingStatsDeadlocksChart);
            var (deadlockXs, deadlockYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.DeadlockCount));
            if (deadlockXs.Length > 0)
            {
                var scatter = BlockingStatsDeadlocksChart.Plot.Add.Scatter(deadlockXs, deadlockYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[3];
                _deadlocksHover?.Add(scatter, "Deadlocks");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsDeadlocksChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsDeadlocksChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingStatsDeadlocksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsDeadlocksChart.Plot.YLabel("Count");
            LockChartVerticalAxis(BlockingStatsDeadlocksChart);
            BlockingStatsDeadlocksChart.Refresh();

            // Deadlock Wait Time Chart (raw per-interval total, not delta)
            BlockingStatsDeadlockWaitTimeChart.Plot.Clear();
            _deadlockWaitTimeHover?.Clear();
            ApplyThemeToChart(BlockingStatsDeadlockWaitTimeChart);
            var (deadlockWaitXs, deadlockWaitYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.TotalDeadlockWaitTimeMs));
            if (deadlockWaitXs.Length > 0)
            {
                var scatter = BlockingStatsDeadlockWaitTimeChart.Plot.Add.Scatter(deadlockWaitXs, deadlockWaitYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[4];
                _deadlockWaitTimeHover?.Add(scatter, "Deadlock Wait Time");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsDeadlockWaitTimeChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsDeadlockWaitTimeChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingStatsDeadlockWaitTimeChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsDeadlockWaitTimeChart.Plot.YLabel("Duration (ms)");
            LockChartVerticalAxis(BlockingStatsDeadlockWaitTimeChart);
            BlockingStatsDeadlockWaitTimeChart.Refresh();
        }

        private void UpdateCollectorDurationChart(List<CollectionLogEntry> data)
        {
            if (_legendPanels.TryGetValue(CollectorDurationChart, out var existingPanel) && existingPanel != null)
            {
                CollectorDurationChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[CollectorDurationChart] = null;
            }
            CollectorDurationChart.Plot.Clear();
            _collectorDurationHover?.Clear();
            ApplyThemeToChart(CollectorDurationChart);

            if (data.Count == 0) { CollectorDurationChart.Refresh(); return; }

            var groups = data
                .Where(d => d.CollectorName != "scheduled_master_collector")
                .GroupBy(d => d.CollectorName)
                .OrderBy(g => g.Key)
                .ToList();

            var colors = TabHelpers.ChartColors;
            int colorIndex = 0;
            foreach (var group in groups)
            {
                var points = group.OrderBy(d => d.CollectionTime).ToList();
                if (points.Count < 2) continue;

                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    points.Select(d => d.CollectionTime),
                    points.Select(d => (double)d.DurationMs));

                var scatter = CollectorDurationChart.Plot.Add.Scatter(xs, ys);
                scatter.LegendText = group.Key;
                scatter.Color = colors[colorIndex % colors.Length];
                scatter.LineWidth = 2;
                scatter.MarkerSize = 0;
                _collectorDurationHover?.Add(scatter, group.Key);
                colorIndex++;
            }

            CollectorDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TabHelpers.ReapplyAxisColors(CollectorDurationChart);
            CollectorDurationChart.Plot.YLabel("Duration (ms)");
            CollectorDurationChart.Plot.Axes.AutoScale();
            _legendPanels[CollectorDurationChart] = CollectorDurationChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            CollectorDurationChart.Plot.Legend.FontSize = 12;
            LockChartVerticalAxis(CollectorDurationChart);
            CollectorDurationChart.Refresh();
        }

        private void LoadLockWaitStatsChart(List<LockWaitStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            // Calculate the time range for X-axis limits (use server time, not local time)
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(LockWaitStatsChart, out var existingPanel) && existingPanel != null)
            {
                LockWaitStatsChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[LockWaitStatsChart] = null;
            }
            LockWaitStatsChart.Plot.Clear();
            _lockWaitStatsHover?.Clear();
            ApplyThemeToChart(LockWaitStatsChart);

            // Get all unique time points across all wait types for gap filling
            // Group by wait type and plot each as a separate series
            var waitTypes = data.Select(d => d.WaitType).Distinct().OrderBy(w => w).ToList();
            var colors = TabHelpers.ChartColors;

            int colorIndex = 0;
            foreach (var waitType in waitTypes)
            {
                var waitTypeData = data.Where(d => d.WaitType == waitType).OrderBy(d => d.CollectionTime).ToList();
                if (waitTypeData.Count > 0)
                {
                    // Fill gaps with zeros so lines are continuous
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        waitTypeData.Select(d => d.CollectionTime),
                        waitTypeData.Select(d => (double)d.WaitTimeMsPerSecond));

                    var scatter = LockWaitStatsChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];
                    var lockLabel = waitType.Replace("LCK_M_", "").Replace("LCK_", "");
                    scatter.LegendText = lockLabel;
                    _lockWaitStatsHover?.Add(scatter, lockLabel);
                    colorIndex++;
                }
            }

            if (data.Count == 0)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = LockWaitStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            LockWaitStatsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            LockWaitStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            LockWaitStatsChart.Plot.YLabel("Wait Time (ms/sec)");
            _legendPanels[LockWaitStatsChart] = LockWaitStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            LockWaitStatsChart.Plot.Legend.FontSize = 12;
            LockChartVerticalAxis(LockWaitStatsChart);
            LockWaitStatsChart.Refresh();
        }

        private void LoadCurrentWaitsDurationChart(List<WaitingTaskTrendItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(CurrentWaitsDurationChart, out var existingPanel) && existingPanel != null)
            {
                CurrentWaitsDurationChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[CurrentWaitsDurationChart] = null;
            }
            CurrentWaitsDurationChart.Plot.Clear();
            _currentWaitsDurationHover?.Clear();
            ApplyThemeToChart(CurrentWaitsDurationChart);

            var waitTypes = data.Select(d => d.WaitType).Distinct().OrderBy(w => w).ToList();
            var colors = TabHelpers.ChartColors;

            int colorIndex = 0;
            foreach (var waitType in waitTypes)
            {
                var waitTypeData = data.Where(d => d.WaitType == waitType).OrderBy(d => d.CollectionTime).ToList();
                if (waitTypeData.Count > 0)
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        waitTypeData.Select(d => d.CollectionTime),
                        waitTypeData.Select(d => (double)d.TotalWaitMs));

                    var scatter = CurrentWaitsDurationChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];
                    scatter.LegendText = waitType;
                    _currentWaitsDurationHover?.Add(scatter, waitType);
                    colorIndex++;
                }
            }

            if (data.Count == 0)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = CurrentWaitsDurationChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            CurrentWaitsDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CurrentWaitsDurationChart.Plot.Axes.SetLimitsX(xMin, xMax);
            CurrentWaitsDurationChart.Plot.YLabel("Total Wait Duration (ms)");
            _legendPanels[CurrentWaitsDurationChart] = CurrentWaitsDurationChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            CurrentWaitsDurationChart.Plot.Legend.FontSize = 12;
            LockChartVerticalAxis(CurrentWaitsDurationChart);
            CurrentWaitsDurationChart.Refresh();
        }

        private void LoadCurrentWaitsBlockedChart(List<BlockedSessionTrendItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(CurrentWaitsBlockedChart, out var existingPanel) && existingPanel != null)
            {
                CurrentWaitsBlockedChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[CurrentWaitsBlockedChart] = null;
            }
            CurrentWaitsBlockedChart.Plot.Clear();
            _currentWaitsBlockedHover?.Clear();
            ApplyThemeToChart(CurrentWaitsBlockedChart);

            var databases = data.Select(d => d.DatabaseName).Distinct().OrderBy(d => d).ToList();
            var colors = TabHelpers.ChartColors;

            int colorIndex = 0;
            foreach (var db in databases)
            {
                var dbData = data.Where(d => d.DatabaseName == db).OrderBy(d => d.CollectionTime).ToList();
                if (dbData.Count > 0)
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        dbData.Select(d => d.CollectionTime),
                        dbData.Select(d => (double)d.BlockedCount));

                    var scatter = CurrentWaitsBlockedChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];
                    scatter.LegendText = db;
                    _currentWaitsBlockedHover?.Add(scatter, db);
                    colorIndex++;
                }
            }

            if (data.Count == 0)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = CurrentWaitsBlockedChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            CurrentWaitsBlockedChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CurrentWaitsBlockedChart.Plot.Axes.SetLimitsX(xMin, xMax);
            CurrentWaitsBlockedChart.Plot.YLabel("Blocked Sessions");
            _legendPanels[CurrentWaitsBlockedChart] = CurrentWaitsBlockedChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            CurrentWaitsBlockedChart.Plot.Legend.FontSize = 12;
            LockChartVerticalAxis(CurrentWaitsBlockedChart);
            CurrentWaitsBlockedChart.Refresh();
        }

        private void LoadResourceOverviewCpuChart(IEnumerable<CpuDataPoint> cpuData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewCpuChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewCpuChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewCpuChart] = null;
            }
            ResourceOverviewCpuChart.Plot.Clear();
            _resourceOverviewCpuHover?.Clear();
            ApplyThemeToChart(ResourceOverviewCpuChart);

            var dataList = cpuData?.OrderBy(d => d.SampleTime).ToList() ?? new List<CpuDataPoint>();

            // Build time series with boundary points for continuous lines
            var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.SampleTime),
                dataList.Select(d => (double)d.SqlServerCpu));

            if (xs.Length > 0)
            {
                var scatter = ResourceOverviewCpuChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[0];
                scatter.LegendText = "SQL CPU %";
                _resourceOverviewCpuHover?.Add(scatter, "SQL CPU %");

                _legendPanels[ResourceOverviewCpuChart] = ResourceOverviewCpuChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewCpuChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewCpuChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewCpuChart.Plot.Axes.DateTimeTicksBottomDateChange();
            ResourceOverviewCpuChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewCpuChart.Plot.Axes.SetLimitsY(0, 100);
            ResourceOverviewCpuChart.Plot.YLabel("CPU %");
            LockChartVerticalAxis(ResourceOverviewCpuChart);
            ResourceOverviewCpuChart.Refresh();
        }

        private void LoadResourceOverviewMemoryChart(IEnumerable<MemoryDataPoint> memoryData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewMemoryChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewMemoryChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewMemoryChart] = null;
            }
            ResourceOverviewMemoryChart.Plot.Clear();
            _resourceOverviewMemoryHover?.Clear();
            ApplyThemeToChart(ResourceOverviewMemoryChart);

            var dataList = memoryData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<MemoryDataPoint>();
            // Buffer Pool series with gap filling
            var (bufferXs, bufferYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.BufferPoolMb));

            // Memory Grants series with gap filling
            var (grantsXs, grantsYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.GrantedMemoryMb));

            if (bufferXs.Length > 0)
            {
                var bufferScatter = ResourceOverviewMemoryChart.Plot.Add.Scatter(bufferXs, bufferYs);
                bufferScatter.LineWidth = 2;
                bufferScatter.MarkerSize = 5;
                bufferScatter.Color = TabHelpers.ChartColors[4];
                bufferScatter.LegendText = "Buffer Pool";
                _resourceOverviewMemoryHover?.Add(bufferScatter, "Buffer Pool");

                var grantsScatter = ResourceOverviewMemoryChart.Plot.Add.Scatter(grantsXs, grantsYs);
                grantsScatter.LineWidth = 2;
                grantsScatter.MarkerSize = 5;
                grantsScatter.Color = TabHelpers.ChartColors[2];
                grantsScatter.LegendText = "Memory Grants";
                _resourceOverviewMemoryHover?.Add(grantsScatter, "Memory Grants");

                _legendPanels[ResourceOverviewMemoryChart] = ResourceOverviewMemoryChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewMemoryChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewMemoryChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewMemoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
            ResourceOverviewMemoryChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewMemoryChart.Plot.YLabel("MB");
            LockChartVerticalAxis(ResourceOverviewMemoryChart);
            ResourceOverviewMemoryChart.Refresh();
        }

        private void LoadResourceOverviewIoChart(IEnumerable<FileIoDataPoint> ioData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewIoChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewIoChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewIoChart] = null;
            }
            ResourceOverviewIoChart.Plot.Clear();
            _resourceOverviewIoHover?.Clear();
            ApplyThemeToChart(ResourceOverviewIoChart);

            var dataList = ioData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<FileIoDataPoint>();
            int bucketMinutes = hoursBack <= 1 ? 1 : hoursBack <= 6 ? 5 : hoursBack <= 24 ? 15 : 60;

            var aggregated = dataList
                .GroupBy(d => new DateTime(
                    d.CollectionTime.Year, d.CollectionTime.Month, d.CollectionTime.Day,
                    d.CollectionTime.Hour, (d.CollectionTime.Minute / bucketMinutes) * bucketMinutes, 0))
                .Select(g => new
                {
                    BucketTime = g.Key,
                    AvgReadLatency = g.Average(x => (double)x.AvgReadLatencyMs),
                    AvgWriteLatency = g.Average(x => (double)x.AvgWriteLatencyMs)
                })
                .OrderBy(x => x.BucketTime)
                .ToList();

            // Read latency series with gap filling
            var (readXs, readYs) = TabHelpers.FillTimeSeriesGaps(
                aggregated.Select(d => d.BucketTime),
                aggregated.Select(d => d.AvgReadLatency));

            // Write latency series with gap filling
            var (writeXs, writeYs) = TabHelpers.FillTimeSeriesGaps(
                aggregated.Select(d => d.BucketTime),
                aggregated.Select(d => d.AvgWriteLatency));

            if (readXs.Length > 0)
            {
                var readScatter = ResourceOverviewIoChart.Plot.Add.Scatter(readXs, readYs);
                readScatter.LineWidth = 2;
                readScatter.MarkerSize = 5;
                readScatter.Color = TabHelpers.ChartColors[1];
                readScatter.LegendText = "Read ms";
                _resourceOverviewIoHover?.Add(readScatter, "Read ms");

                var writeScatter = ResourceOverviewIoChart.Plot.Add.Scatter(writeXs, writeYs);
                writeScatter.LineWidth = 2;
                writeScatter.MarkerSize = 5;
                writeScatter.Color = TabHelpers.ChartColors[2];
                writeScatter.LegendText = "Write ms";
                _resourceOverviewIoHover?.Add(writeScatter, "Write ms");

                _legendPanels[ResourceOverviewIoChart] = ResourceOverviewIoChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewIoChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewIoChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewIoChart.Plot.Axes.DateTimeTicksBottomDateChange();
            ResourceOverviewIoChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewIoChart.Plot.Axes.AutoScaleY();
            ResourceOverviewIoChart.Plot.YLabel("Latency (ms)");
            LockChartVerticalAxis(ResourceOverviewIoChart);
            ResourceOverviewIoChart.Refresh();
        }

        private void LoadResourceOverviewWaitChart(IEnumerable<WaitStatsDataPoint> waitData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewWaitChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewWaitChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewWaitChart] = null;
            }
            ResourceOverviewWaitChart.Plot.Clear();
            _resourceOverviewWaitHover?.Clear();
            ApplyThemeToChart(ResourceOverviewWaitChart);

            var dataList = waitData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<WaitStatsDataPoint>();

            // Get all unique time points across all wait types for gap filling
            if (dataList.Count > 0)
            {
                var topWaitTypes = dataList
                    .GroupBy(d => d.WaitType)
                    .Select(g => new { WaitType = g.Key, TotalWait = g.Sum(x => x.WaitTimeMsPerSecond) })
                    .OrderByDescending(x => x.TotalWait)
                    .Take(5)
                    .Select(x => x.WaitType)
                    .ToList();

                var colors = TabHelpers.ChartColors;
                int colorIndex = 0;

                foreach (var waitType in topWaitTypes)
                {
                    var waitTypeData = dataList.Where(d => d.WaitType == waitType).ToList();
                    if (waitTypeData.Count < 2) continue;

                    // Fill gaps with zeros so lines are continuous
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        waitTypeData.Select(d => d.CollectionTime),
                        waitTypeData.Select(d => (double)d.WaitTimeMsPerSecond));

                    var scatter = ResourceOverviewWaitChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];
                    var waitLabel = waitType.Length > 15 ? waitType.Substring(0, 15) + "..." : waitType;
                    scatter.LegendText = waitLabel;
                    _resourceOverviewWaitHover?.Add(scatter, waitLabel);
                    colorIndex++;
                }

                _legendPanels[ResourceOverviewWaitChart] = ResourceOverviewWaitChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewWaitChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewWaitChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewWaitChart.Plot.Axes.DateTimeTicksBottomDateChange();
            ResourceOverviewWaitChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewWaitChart.Plot.Axes.AutoScaleY();
            ResourceOverviewWaitChart.Plot.YLabel("Wait Time (ms/sec)");
            LockChartVerticalAxis(ResourceOverviewWaitChart);
            ResourceOverviewWaitChart.Refresh();
        }
    }
}

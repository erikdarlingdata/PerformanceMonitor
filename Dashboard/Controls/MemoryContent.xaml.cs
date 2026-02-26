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
    /// <summary>
    /// UserControl for the Memory tab content.
    /// Displays memory stats, grants, clerks, and plan cache analysis.
    /// </summary>
    public partial class MemoryContent : UserControl
    {
        private DatabaseService? _databaseService;

        // Memory Stats state
        private int _memoryStatsHoursBack = 24;
        private DateTime? _memoryStatsFromDate;
        private DateTime? _memoryStatsToDate;

        // Memory Grants state
        private int _memoryGrantsHoursBack = 24;
        private DateTime? _memoryGrantsFromDate;
        private DateTime? _memoryGrantsToDate;

        // Memory Clerks state
        private int _memoryClerksHoursBack = 24;
        private DateTime? _memoryClerksFromDate;
        private DateTime? _memoryClerksToDate;

        // Plan Cache state
        private int _planCacheHoursBack = 24;
        private DateTime? _planCacheFromDate;
        private DateTime? _planCacheToDate;

        // Memory Pressure Events state
        private int _memoryPressureEventsHoursBack = 24;
        private DateTime? _memoryPressureEventsFromDate;
        private DateTime? _memoryPressureEventsToDate;

        // Filter state dictionaries removed - no more grids with filters in this control

        // Legend panel references for edge-based legends (ScottPlot issue #4717 workaround)
        private Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();

        // Memory Clerks picker state
        private List<SelectableItem> _memoryClerkItems = new();
        private bool _isUpdatingMemoryClerkSelection;

        // Chart hover tooltips
        private Helpers.ChartHoverHelper? _memoryStatsOverviewHover;
        private Helpers.ChartHoverHelper? _memoryGrantSizingHover;
        private Helpers.ChartHoverHelper? _memoryGrantActivityHover;
        private Helpers.ChartHoverHelper? _memoryClerksHover;
        private Helpers.ChartHoverHelper? _planCacheHover;
        private Helpers.ChartHoverHelper? _memoryPressureEventsHover;

        // No DataGrids with filters - all tabs are chart-only

        public MemoryContent()
        {
            InitializeComponent();
            SetupChartContextMenus();
            Loaded += OnLoaded;

            _memoryStatsOverviewHover = new Helpers.ChartHoverHelper(MemoryStatsOverviewChart, "MB");
            _memoryGrantSizingHover = new Helpers.ChartHoverHelper(MemoryGrantSizingChart, "MB");
            _memoryGrantActivityHover = new Helpers.ChartHoverHelper(MemoryGrantActivityChart, "count");
            _memoryClerksHover = new Helpers.ChartHoverHelper(MemoryClerksChart, "MB");
            _planCacheHover = new Helpers.ChartHoverHelper(PlanCacheChart, "MB");
            _memoryPressureEventsHover = new Helpers.ChartHoverHelper(MemoryPressureEventsChart, "events");
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            // No grids to configure - all tabs are chart-only now
        }

        private void SetupChartContextMenus()
        {
            // Memory Stats Overview chart
            TabHelpers.SetupChartContextMenu(MemoryStatsOverviewChart, "Memory_Stats_Overview", "collect.memory_stats");

            // Memory Grant charts
            TabHelpers.SetupChartContextMenu(MemoryGrantSizingChart, "Memory_Grant_Sizing", "collect.memory_grant_stats");
            TabHelpers.SetupChartContextMenu(MemoryGrantActivityChart, "Memory_Grant_Activity", "collect.memory_grant_stats");

            // Memory Clerks chart
            TabHelpers.SetupChartContextMenu(MemoryClerksChart, "Memory_Clerks", "collect.memory_clerks_stats");

            // Plan Cache chart
            TabHelpers.SetupChartContextMenu(PlanCacheChart, "Plan_Cache", "collect.plan_cache_stats");

            // Memory Pressure Events chart
            TabHelpers.SetupChartContextMenu(MemoryPressureEventsChart, "Memory_Pressure_Events", "collect.memory_pressure_events");
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
        }

        /// <summary>
        /// Sets the time range for all memory sub-tabs.
        /// </summary>
        public void SetTimeRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            _memoryStatsHoursBack = hoursBack;
            _memoryStatsFromDate = fromDate;
            _memoryStatsToDate = toDate;

            _memoryGrantsHoursBack = hoursBack;
            _memoryGrantsFromDate = fromDate;
            _memoryGrantsToDate = toDate;

            _memoryClerksHoursBack = hoursBack;
            _memoryClerksFromDate = fromDate;
            _memoryClerksToDate = toDate;

            _planCacheHoursBack = hoursBack;
            _planCacheFromDate = fromDate;
            _planCacheToDate = toDate;

            _memoryPressureEventsHoursBack = hoursBack;
            _memoryPressureEventsFromDate = fromDate;
            _memoryPressureEventsToDate = toDate;
        }

        /// <summary>
        /// Refreshes all memory data. Can be called from parent control.
        /// </summary>
        public async Task RefreshAllDataAsync()
        {
            try
            {
                using var _ = Helpers.MethodProfiler.StartTiming("Memory");

                // Run all independent refreshes in parallel for better performance
                await Task.WhenAll(
                    RefreshMemoryStatsAsync(),
                    RefreshMemoryGrantsAsync(),
                    RefreshMemoryClerksAsync(),
                    RefreshPlanCacheAsync(),
                    RefreshMemoryPressureEventsAsync()
                );
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Memory data: {ex.Message}", ex);
            }
        }

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
            TabHelpers.ApplyDarkModeToChart(MemoryStatsOverviewChart);

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

            MemoryStatsOverviewChart.Plot.Axes.DateTimeTicksBottom();
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
            TabHelpers.ApplyDarkModeToChart(MemoryGrantSizingChart);

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

            MemoryGrantSizingChart.Plot.Axes.DateTimeTicksBottom();
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
            TabHelpers.ApplyDarkModeToChart(MemoryGrantActivityChart);

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

            MemoryGrantActivityChart.Plot.Axes.DateTimeTicksBottom();
            MemoryGrantActivityChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryGrantActivityChart.Plot.YLabel("Count");
            MemoryGrantActivityChart.Plot.Axes.AutoScaleY();
            var limits = MemoryGrantActivityChart.Plot.Axes.GetLimits();
            MemoryGrantActivityChart.Plot.Axes.SetLimitsY(0, limits.Top * 1.05);
            TabHelpers.LockChartVerticalAxis(MemoryGrantActivityChart);
            MemoryGrantActivityChart.Refresh();
        }

        #endregion

        #region Memory Clerks

        private async System.Threading.Tasks.Task RefreshMemoryClerksAsync()
        {
            if (_databaseService == null) return;

            try
            {
                if (!MemoryClerksChart.Plot.GetPlottables().Any())
                {
                    MemoryClerksLoading.IsLoading = true;
                    MemoryClerksNoDataMessage.Visibility = Visibility.Collapsed;
                }

                var clerkTypes = await _databaseService.GetDistinctMemoryClerkTypesAsync(_memoryClerksHoursBack, _memoryClerksFromDate, _memoryClerksToDate);
                PopulateMemoryClerkPicker(clerkTypes);
                await UpdateMemoryClerksChartFromPickerAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory clerks: {ex.Message}");
            }
            finally
            {
                MemoryClerksLoading.IsLoading = false;
            }
        }

        private void PopulateMemoryClerkPicker(List<string> clerkTypes)
        {
            var previouslySelected = new HashSet<string>(_memoryClerkItems.Where(i => i.IsSelected).Select(i => i.DisplayName));
            var topClerks = previouslySelected.Count == 0 ? new HashSet<string>(clerkTypes.Take(5)) : null;
            _memoryClerkItems = clerkTypes.Select(c => new SelectableItem
            {
                DisplayName = c,
                IsSelected = previouslySelected.Contains(c) || (topClerks != null && topClerks.Contains(c))
            }).ToList();
            RefreshMemoryClerkListOrder();
        }

        private void RefreshMemoryClerkListOrder()
        {
            if (_memoryClerkItems == null) return;
            _memoryClerkItems = _memoryClerkItems
                .OrderByDescending(x => x.IsSelected)
                .ThenBy(x => x.DisplayName)
                .ToList();
            ApplyMemoryClerkFilter();
            UpdateMemoryClerkCount();
        }

        private void UpdateMemoryClerkCount()
        {
            if (_memoryClerkItems == null || MemoryClerkCountText == null) return;
            int count = _memoryClerkItems.Count(x => x.IsSelected);
            MemoryClerkCountText.Text = $"{count} selected";
        }

        private void ApplyMemoryClerkFilter()
        {
            var search = MemoryClerkSearchBox?.Text?.Trim() ?? "";
            MemoryClerksList.ItemsSource = null;
            if (string.IsNullOrEmpty(search))
                MemoryClerksList.ItemsSource = _memoryClerkItems;
            else
                MemoryClerksList.ItemsSource = _memoryClerkItems.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
        }

        private void MemoryClerkSearch_TextChanged(object sender, TextChangedEventArgs e) => ApplyMemoryClerkFilter();

        private void MemoryClerkSelectTop_Click(object sender, RoutedEventArgs e)
        {
            _isUpdatingMemoryClerkSelection = true;
            var topClerks = new HashSet<string>(_memoryClerkItems.Take(5).Select(x => x.DisplayName));
            foreach (var item in _memoryClerkItems)
                item.IsSelected = topClerks.Contains(item.DisplayName);
            _isUpdatingMemoryClerkSelection = false;
            RefreshMemoryClerkListOrder();
            _ = UpdateMemoryClerksChartFromPickerAsync();
        }

        private void MemoryClerkClearAll_Click(object sender, RoutedEventArgs e)
        {
            _isUpdatingMemoryClerkSelection = true;
            var visible = (MemoryClerksList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _memoryClerkItems;
            foreach (var item in visible) item.IsSelected = false;
            _isUpdatingMemoryClerkSelection = false;
            RefreshMemoryClerkListOrder();
            _ = UpdateMemoryClerksChartFromPickerAsync();
        }

        private void MemoryClerk_CheckChanged(object sender, RoutedEventArgs e)
        {
            if (_isUpdatingMemoryClerkSelection) return;
            RefreshMemoryClerkListOrder();
            _ = UpdateMemoryClerksChartFromPickerAsync();
        }

        private async System.Threading.Tasks.Task UpdateMemoryClerksChartFromPickerAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var selected = _memoryClerkItems.Where(i => i.IsSelected).Take(20).ToList();

                if (_legendPanels.TryGetValue(MemoryClerksChart, out var existingPanel) && existingPanel != null)
                {
                    MemoryClerksChart.Plot.Axes.Remove(existingPanel);
                    _legendPanels[MemoryClerksChart] = null;
                }
                MemoryClerksChart.Plot.Clear();
                _memoryClerksHover?.Clear();
                TabHelpers.ApplyDarkModeToChart(MemoryClerksChart);

                DateTime rangeEnd = _memoryClerksToDate ?? Helpers.ServerTimeHelper.ServerNow;
                DateTime rangeStart = _memoryClerksFromDate ?? rangeEnd.AddHours(-_memoryClerksHoursBack);
                double xMin = rangeStart.ToOADate();
                double xMax = rangeEnd.ToOADate();

                if (selected.Count > 0)
                {
                    var selectedTypes = selected.Select(s => s.DisplayName).ToList();
                    var data = await _databaseService.GetMemoryClerksByTypesAsync(selectedTypes, _memoryClerksHoursBack, _memoryClerksFromDate, _memoryClerksToDate);
                    var dataList = data.ToList();

                    MemoryClerksNoDataMessage.Visibility = dataList.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                    if (dataList.Count > 0)
                    {
                        var colors = TabHelpers.ChartColors;
                        int colorIndex = 0;

                        foreach (var clerkType in selectedTypes)
                        {
                            var clerkData = dataList.Where(d => d.ClerkType == clerkType)
                                .OrderBy(d => d.CollectionTime)
                                .ToList();

                            if (clerkData.Count >= 1)
                            {
                                var timePoints = clerkData.Select(d => d.CollectionTime);
                                var values = clerkData.Select(d => (double)d.PagesMb);
                                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                                var scatter = MemoryClerksChart.Plot.Add.Scatter(xs, ys);
                                scatter.LineWidth = 2;
                                scatter.MarkerSize = 5;
                                scatter.Color = colors[colorIndex % colors.Length];
                                var label = clerkType.Length > 20 ? clerkType.Substring(0, 20) + "..." : clerkType;
                                scatter.LegendText = label;
                                _memoryClerksHover?.Add(scatter, label);
                                colorIndex++;
                            }
                        }

                        _legendPanels[MemoryClerksChart] = MemoryClerksChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                        MemoryClerksChart.Plot.Legend.FontSize = 12;
                    }

                    UpdateMemoryClerksSummaryPanel(dataList);
                }
                else
                {
                    MemoryClerksNoDataMessage.Visibility = Visibility.Collapsed;
                    MemoryClerksTotalText.Text = "N/A";
                    MemoryClerksTopText.Text = "N/A";
                }

                MemoryClerksChart.Plot.Axes.DateTimeTicksBottom();
                MemoryClerksChart.Plot.Axes.SetLimitsX(xMin, xMax);
                MemoryClerksChart.Plot.YLabel("MB");
                MemoryClerksChart.Plot.Axes.AutoScaleY();
                var clerksLimits = MemoryClerksChart.Plot.Axes.GetLimits();
                MemoryClerksChart.Plot.Axes.SetLimitsY(0, clerksLimits.Top * 1.05);
                TabHelpers.LockChartVerticalAxis(MemoryClerksChart);
                MemoryClerksChart.Refresh();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error updating memory clerks chart: {ex.Message}");
            }
        }

        private void UpdateMemoryClerksSummaryPanel(List<MemoryClerksItem> dataList)
        {
            if (dataList == null || dataList.Count == 0)
            {
                MemoryClerksTotalText.Text = "N/A";
                MemoryClerksTopText.Text = "N/A";
                return;
            }

            var latestTime = dataList.Max(d => d.CollectionTime);
            var latestData = dataList
                .Where(d => d.CollectionTime == latestTime)
                .Where(d => d.ClerkType == null || !d.ClerkType.Contains("BUFFERPOOL", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var totalMb = latestData.Sum(d => d.PagesMb);
            MemoryClerksTotalText.Text = string.Format(CultureInfo.CurrentCulture, "{0:N0} MB", totalMb);

            var topClerk = latestData.OrderByDescending(d => d.PagesMb).FirstOrDefault();
            if (topClerk != null)
            {
                var name = topClerk.ClerkType ?? "Unknown";
                if (name.StartsWith("MEMORYCLERK_", StringComparison.OrdinalIgnoreCase))
                    name = name.Substring(12);
                if (name.Length > 20) name = name.Substring(0, 20) + "...";
                MemoryClerksTopText.Text = string.Format(CultureInfo.CurrentCulture, "{0} ({1:N0} MB)", name, topClerk.PagesMb);
            }
            else
            {
                MemoryClerksTopText.Text = "N/A";
            }
        }

        #endregion

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
            TabHelpers.ApplyDarkModeToChart(PlanCacheChart);

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

            PlanCacheChart.Plot.Axes.DateTimeTicksBottom();
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

        #region Memory Pressure Events

        private async System.Threading.Tasks.Task RefreshMemoryPressureEventsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetMemoryPressureEventsAsync(_memoryPressureEventsHoursBack, _memoryPressureEventsFromDate, _memoryPressureEventsToDate);
                LoadMemoryPressureEventsChart(data.ToList(), _memoryPressureEventsHoursBack, _memoryPressureEventsFromDate, _memoryPressureEventsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory pressure events: {ex.Message}");
            }
        }

        private void LoadMemoryPressureEventsChart(IEnumerable<MemoryPressureEventItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryPressureEventsChart, out var existingPanel) && existingPanel != null)
            {
                MemoryPressureEventsChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryPressureEventsChart] = null;
            }
            MemoryPressureEventsChart.Plot.Clear();
            _memoryPressureEventsHover?.Clear();
            TabHelpers.ApplyDarkModeToChart(MemoryPressureEventsChart);

            // Only chart HIGH severity events
            var dataList = data?.Where(d => d.Severity.Equals("HIGH", StringComparison.OrdinalIgnoreCase))
                .OrderBy(d => d.SampleTime).ToList() ?? new List<MemoryPressureEventItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Group by hour and count HIGH events
                var grouped = dataList
                    .GroupBy(d => new DateTime(d.SampleTime.Year, d.SampleTime.Month, d.SampleTime.Day, d.SampleTime.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var timePoints = grouped.Select(g => g.Key);
                    double[] highCounts = grouped.Select(g => (double)g.Count()).ToArray();

                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, highCounts.Select(c => c));
                    var highScatter = MemoryPressureEventsChart.Plot.Add.Scatter(xs, ys);
                    highScatter.LineWidth = 2;
                    highScatter.MarkerSize = 5;
                    highScatter.Color = TabHelpers.ChartColors[3];
                    highScatter.LegendText = "High Pressure Events";
                    _memoryPressureEventsHover?.Add(highScatter, "High Pressure Events");

                    _legendPanels[MemoryPressureEventsChart] = MemoryPressureEventsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryPressureEventsChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryPressureEventsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryPressureEventsChart.Plot.Axes.DateTimeTicksBottom();
            MemoryPressureEventsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryPressureEventsChart.Plot.YLabel("Event Count");
            // Fixed negative space for legend
            MemoryPressureEventsChart.Plot.Axes.AutoScaleY();
            var pressureLimits = MemoryPressureEventsChart.Plot.Axes.GetLimits();
            MemoryPressureEventsChart.Plot.Axes.SetLimitsY(0, pressureLimits.Top * 1.05);

            TabHelpers.LockChartVerticalAxis(MemoryPressureEventsChart);
            MemoryPressureEventsChart.Refresh();
        }

        #endregion


        #region Context Menu Handlers

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = TabHelpers.GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(cellContent, false);
                    }
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.SelectedItem != null)
                {
                    var rowText = TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem);
                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(rowText, false);
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new StringBuilder();

                    // Add headers
                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                    {
                        if (column is DataGridBoundColumn)
                        {
                            headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                        }
                    }
                    sb.AppendLine(string.Join("\t", headers));

                    // Add all rows
                    foreach (var item in dataGrid.Items)
                    {
                        sb.AppendLine(TabHelpers.GetRowAsText(dataGrid, item));
                    }

                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    string prefix = "memory";


                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"{prefix}_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Add headers
                            var headers = new List<string>();
                            foreach (var column in dataGrid.Columns)
                            {
                                if (column is DataGridBoundColumn)
                                {
                                    headers.Add(TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column), TabHelpers.CsvSeparator));
                                }
                            }
                            sb.AppendLine(string.Join(TabHelpers.CsvSeparator, headers));

                            // Add all rows
                            foreach (var item in dataGrid.Items)
                            {
                                var values = TabHelpers.GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(TabHelpers.CsvSeparator, values.Select(v => TabHelpers.EscapeCsvField(v, TabHelpers.CsvSeparator))));
                            }

                            File.WriteAllText(saveFileDialog.FileName, sb.ToString());
                            MessageBox.Show($"Data exported successfully to:\n{saveFileDialog.FileName}", "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                        }
                        catch (Exception ex)
                        {
                            MessageBox.Show($"Error exporting data:\n\n{ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }

        #endregion
    }
}

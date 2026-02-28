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
    /// <summary>
    /// UserControl for the Resource Metrics tab content.
    /// Displays Latch Stats, Spinlock Stats, TempDB Stats, CPU Spikes, Session Stats,
    /// File I/O Latency, Server Trends, and Perfmon Counters.
    /// </summary>
    public partial class ResourceMetricsContent : UserControl
    {
        private DatabaseService? _databaseService;

        // Latch Stats state
        private int _latchStatsHoursBack = 24;
        private DateTime? _latchStatsFromDate;
        private DateTime? _latchStatsToDate;

        // Spinlock Stats state
        private int _spinlockStatsHoursBack = 24;
        private DateTime? _spinlockStatsFromDate;
        private DateTime? _spinlockStatsToDate;

        // TempDB Stats state
        private int _tempdbStatsHoursBack = 24;
        private DateTime? _tempdbStatsFromDate;
        private DateTime? _tempdbStatsToDate;

        // CPU Spikes state


        // Session Stats state
        private int _sessionStatsHoursBack = 24;
        private DateTime? _sessionStatsFromDate;
        private DateTime? _sessionStatsToDate;

        // File I/O state
        private int _fileIoHoursBack = 24;
        private DateTime? _fileIoFromDate;
        private DateTime? _fileIoToDate;

        // Server Trends state
        private int _serverTrendsHoursBack = 24;
        private DateTime? _serverTrendsFromDate;
        private DateTime? _serverTrendsToDate;

        // Perfmon Counters state
        private int _perfmonCountersHoursBack = 24;
        private DateTime? _perfmonCountersFromDate;
        private DateTime? _perfmonCountersToDate;
        private List<PerfmonStatsItem>? _allPerfmonCountersData;
        private List<PerfmonCounterSelectionItem>? _perfmonCounterItems;

        // Wait Stats Detail state
        private int _waitStatsDetailHoursBack = 24;
        private DateTime? _waitStatsDetailFromDate;
        private DateTime? _waitStatsDetailToDate;
        private List<WaitStatsDataPoint>? _allWaitStatsDetailData;
        private List<WaitTypeSelectionItem>? _waitTypeItems;
        private bool _isUpdatingWaitTypeSelection = false;
        private Helpers.ChartHoverHelper? _sessionStatsHover;
        private Helpers.ChartHoverHelper? _latchStatsHover;
        private Helpers.ChartHoverHelper? _spinlockStatsHover;
        private Helpers.ChartHoverHelper? _fileIoReadHover;
        private Helpers.ChartHoverHelper? _fileIoWriteHover;
        private Helpers.ChartHoverHelper? _fileIoReadThroughputHover;
        private Helpers.ChartHoverHelper? _fileIoWriteThroughputHover;
        private Helpers.ChartHoverHelper? _perfmonHover;
        private Helpers.ChartHoverHelper? _waitStatsHover;
        private Helpers.ChartHoverHelper? _tempdbStatsHover;
        private Helpers.ChartHoverHelper? _tempDbLatencyHover;
        private Helpers.ChartHoverHelper? _serverTrendsCpuHover;
        private Helpers.ChartHoverHelper? _serverTrendsTempdbHover;
        private Helpers.ChartHoverHelper? _serverTrendsMemoryHover;
        private Helpers.ChartHoverHelper? _serverTrendsPerfmonHover;
        // Filter state dictionaries for each DataGrid
        // Legend panel references for edge-based legends (ScottPlot issue #4717 workaround)
        // Must store and remove these by reference before creating new ones
        private Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();


        public ResourceMetricsContent()
        {
            InitializeComponent();
            SetupChartContextMenus();
            Loaded += OnLoaded;
            Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
            Unloaded += (_, _) => Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;

            // Apply dark theme immediately so charts don't flash white before data loads
            TabHelpers.ApplyThemeToChart(LatchStatsChart);
            TabHelpers.ApplyThemeToChart(SpinlockStatsChart);
            TabHelpers.ApplyThemeToChart(TempdbStatsChart);
            TabHelpers.ApplyThemeToChart(TempDbLatencyChart);
            TabHelpers.ApplyThemeToChart(SessionStatsChart);
            TabHelpers.ApplyThemeToChart(UserDbReadLatencyChart);
            TabHelpers.ApplyThemeToChart(UserDbWriteLatencyChart);
            TabHelpers.ApplyThemeToChart(FileIoReadThroughputChart);
            TabHelpers.ApplyThemeToChart(FileIoWriteThroughputChart);
            TabHelpers.ApplyThemeToChart(PerfmonCountersChart);
            TabHelpers.ApplyThemeToChart(WaitStatsDetailChart);
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsCpuChart);
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsTempdbChart);
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsMemoryChart);
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsPerfmonChart);

            _sessionStatsHover = new Helpers.ChartHoverHelper(SessionStatsChart, "sessions");
            _latchStatsHover = new Helpers.ChartHoverHelper(LatchStatsChart, "ms/sec");
            _spinlockStatsHover = new Helpers.ChartHoverHelper(SpinlockStatsChart, "collisions/sec");
            _fileIoReadHover = new Helpers.ChartHoverHelper(UserDbReadLatencyChart, "ms");
            _fileIoWriteHover = new Helpers.ChartHoverHelper(UserDbWriteLatencyChart, "ms");
            _fileIoReadThroughputHover = new Helpers.ChartHoverHelper(FileIoReadThroughputChart, "MB/s");
            _fileIoWriteThroughputHover = new Helpers.ChartHoverHelper(FileIoWriteThroughputChart, "MB/s");
            _perfmonHover = new Helpers.ChartHoverHelper(PerfmonCountersChart, "");
            _waitStatsHover = new Helpers.ChartHoverHelper(WaitStatsDetailChart, "ms/sec");
            _tempdbStatsHover = new Helpers.ChartHoverHelper(TempdbStatsChart, "MB");
            _tempDbLatencyHover = new Helpers.ChartHoverHelper(TempDbLatencyChart, "ms");
            _serverTrendsCpuHover = new Helpers.ChartHoverHelper(ServerUtilTrendsCpuChart, "%");
            _serverTrendsTempdbHover = new Helpers.ChartHoverHelper(ServerUtilTrendsTempdbChart, "MB");
            _serverTrendsMemoryHover = new Helpers.ChartHoverHelper(ServerUtilTrendsMemoryChart, "MB");
            _serverTrendsPerfmonHover = new Helpers.ChartHoverHelper(ServerUtilTrendsPerfmonChart, "/sec");
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            // Apply minimum column widths based on header text

            // Freeze identifier columns
        }

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

        private void SetupChartContextMenus()
        {
            // Latch Stats chart
            TabHelpers.SetupChartContextMenu(LatchStatsChart, "Latch_Stats", "collect.latch_stats");

            // Spinlock Stats chart
            TabHelpers.SetupChartContextMenu(SpinlockStatsChart, "Spinlock_Stats", "collect.spinlock_stats");

            // TempDB Stats chart
            TabHelpers.SetupChartContextMenu(TempdbStatsChart, "TempDB_Stats", "collect.tempdb_stats");

            // CPU Spikes chart
            // Session Stats chart
            TabHelpers.SetupChartContextMenu(SessionStatsChart, "Session_Stats", "collect.session_stats");

            // File I/O Latency charts
            TabHelpers.SetupChartContextMenu(UserDbReadLatencyChart, "UserDB_Read_Latency", "collect.file_io_stats");
            TabHelpers.SetupChartContextMenu(UserDbWriteLatencyChart, "UserDB_Write_Latency", "collect.file_io_stats");

            // File I/O Throughput charts
            TabHelpers.SetupChartContextMenu(FileIoReadThroughputChart, "UserDB_Read_Throughput", "collect.file_io_stats");
            TabHelpers.SetupChartContextMenu(FileIoWriteThroughputChart, "UserDB_Write_Throughput", "collect.file_io_stats");
            TabHelpers.SetupChartContextMenu(TempDbLatencyChart, "TempDB_Latency", "collect.file_io_stats");

            // Server Utilization Trends charts
            TabHelpers.SetupChartContextMenu(ServerUtilTrendsCpuChart, "Server_CPU_Trends", "collect.cpu_utilization_stats");
            TabHelpers.SetupChartContextMenu(ServerUtilTrendsTempdbChart, "Server_TempDB_Trends", "collect.tempdb_stats");
            TabHelpers.SetupChartContextMenu(ServerUtilTrendsMemoryChart, "Server_Memory_Trends", "collect.memory_stats");
            TabHelpers.SetupChartContextMenu(ServerUtilTrendsPerfmonChart, "Server_Perfmon_Trends", "collect.perfmon_stats");

            // Perfmon Counters chart
            TabHelpers.SetupChartContextMenu(PerfmonCountersChart, "Perfmon_Counters", "collect.perfmon_stats");

            // Wait Stats Detail chart
            TabHelpers.SetupChartContextMenu(WaitStatsDetailChart, "Wait_Stats_Detail", "collect.wait_stats");
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
        }

        /// <summary>
        /// Sets the time range for all resource metrics sub-tabs.
        /// </summary>
        public void SetTimeRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            _latchStatsHoursBack = hoursBack;
            _latchStatsFromDate = fromDate;
            _latchStatsToDate = toDate;

            _spinlockStatsHoursBack = hoursBack;
            _spinlockStatsFromDate = fromDate;
            _spinlockStatsToDate = toDate;

            _tempdbStatsHoursBack = hoursBack;
            _tempdbStatsFromDate = fromDate;
            _tempdbStatsToDate = toDate;


            _sessionStatsHoursBack = hoursBack;
            _sessionStatsFromDate = fromDate;
            _sessionStatsToDate = toDate;

            _fileIoHoursBack = hoursBack;
            _fileIoFromDate = fromDate;
            _fileIoToDate = toDate;

            _serverTrendsHoursBack = hoursBack;
            _serverTrendsFromDate = fromDate;
            _serverTrendsToDate = toDate;

            _perfmonCountersHoursBack = hoursBack;
            _perfmonCountersFromDate = fromDate;
            _perfmonCountersToDate = toDate;

            _waitStatsDetailHoursBack = hoursBack;
            _waitStatsDetailFromDate = fromDate;
            _waitStatsDetailToDate = toDate;
        }

        /// <summary>
        /// Refreshes all resource metrics data. Can be called from parent control.
        /// </summary>
        public async Task RefreshAllDataAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("ResourceMetrics");
            if (_databaseService == null) return;

            try
            {
                // Run all independent refreshes in parallel for better performance
                await Task.WhenAll(
                    RefreshLatchStatsAsync(),
                    RefreshSpinlockStatsAsync(),
                    RefreshTempdbStatsAsync(),
                    RefreshSessionStatsAsync(),
                    LoadFileIoLatencyChartsAsync(),
                    LoadFileIoThroughputChartsAsync(),
                    RefreshServerTrendsAsync(),
                    RefreshPerfmonCountersTabAsync(),
                    RefreshWaitStatsDetailTabAsync()
                );
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing resource metrics data: {ex.Message}", ex);
            }
        }

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

            LatchStatsChart.Plot.Axes.DateTimeTicksBottom();
            LatchStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(LatchStatsChart);
            LatchStatsChart.Plot.YLabel("Wait Time (ms/sec)");
            TabHelpers.LockChartVerticalAxis(LatchStatsChart);
            LatchStatsChart.Refresh();
        }

        #endregion

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

            SpinlockStatsChart.Plot.Axes.DateTimeTicksBottom();
            SpinlockStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(SpinlockStatsChart);
            SpinlockStatsChart.Plot.YLabel("Collisions/sec");
            TabHelpers.LockChartVerticalAxis(SpinlockStatsChart);
            SpinlockStatsChart.Refresh();
        }

        #endregion

        #region TempDB Stats Tab

        private async Task RefreshTempdbStatsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Load TempDB usage stats
                var data = await _databaseService.GetTempdbStatsAsync(_tempdbStatsHoursBack, _tempdbStatsFromDate, _tempdbStatsToDate);
                LoadTempdbStatsChart(data, _tempdbStatsHoursBack, _tempdbStatsFromDate, _tempdbStatsToDate);

                // Load TempDB latency charts (moved from File I/O Latency tab)
                await LoadTempdbLatencyChartsAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading tempdb stats: {ex.Message}", ex);
            }
        }

        private async Task LoadTempdbLatencyChartsAsync()
        {
            if (_databaseService == null) return;

            DateTime rangeEnd = _tempdbStatsToDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = _tempdbStatsFromDate ?? rangeEnd.AddHours(-_tempdbStatsHoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var tempDbData = await _databaseService.GetFileIoLatencyTimeSeriesAsync(isTempDb: true, _tempdbStatsHoursBack, _tempdbStatsFromDate, _tempdbStatsToDate);
            LoadCombinedTempDbLatencyChart(tempDbData, xMin, xMax);
        }

        private void LoadCombinedTempDbLatencyChart(List<FileIoLatencyTimeSeriesItem> data, double xMin, double xMax)
        {
            DateTime rangeStart = DateTime.FromOADate(xMin);
            DateTime rangeEnd = DateTime.FromOADate(xMax);

            // Remove previously stored legend panel by reference (ScottPlot issue #4717)
            if (_legendPanels.TryGetValue(TempDbLatencyChart, out var existingPanel) && existingPanel != null)
            {
                TempDbLatencyChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[TempDbLatencyChart] = null;
            }
            TempDbLatencyChart.Plot.Clear();
            _tempDbLatencyHover?.Clear();
            TabHelpers.ApplyThemeToChart(TempDbLatencyChart);

            if (data != null && data.Count > 0)
            {
                // Aggregate all TempDB files into single read/write latency values per time point
                var aggregated = data
                    .GroupBy(d => d.CollectionTime)
                    .OrderBy(g => g.Key)
                    .Select(g => new
                    {
                        Time = g.Key,
                        AvgReadLatency = g.Average(x => (double)x.ReadLatencyMs),
                        AvgWriteLatency = g.Average(x => (double)x.WriteLatencyMs)
                    })
                    .ToList();

                // Read Latency series
                var (readXs, readYs) = TabHelpers.FillTimeSeriesGaps(
                    aggregated.Select(d => d.Time),
                    aggregated.Select(d => d.AvgReadLatency));
                var readScatter = TempDbLatencyChart.Plot.Add.Scatter(readXs, readYs);
                readScatter.LineWidth = 2;
                readScatter.MarkerSize = 5;
                readScatter.Color = TabHelpers.ChartColors[0];
                readScatter.LegendText = "Read Latency";
                _tempDbLatencyHover?.Add(readScatter, "Read Latency");

                // Write Latency series
                var (writeXs, writeYs) = TabHelpers.FillTimeSeriesGaps(
                    aggregated.Select(d => d.Time),
                    aggregated.Select(d => d.AvgWriteLatency));
                var writeScatter = TempDbLatencyChart.Plot.Add.Scatter(writeXs, writeYs);
                writeScatter.LineWidth = 2;
                writeScatter.MarkerSize = 5;
                writeScatter.Color = TabHelpers.ChartColors[2];
                writeScatter.LegendText = "Write Latency";
                _tempDbLatencyHover?.Add(writeScatter, "Write Latency");

                // Store legend panel reference for removal on refresh (ScottPlot issue #4717)
                _legendPanels[TempDbLatencyChart] = TempDbLatencyChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                TempDbLatencyChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = TempDbLatencyChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            TempDbLatencyChart.Plot.Axes.DateTimeTicksBottom();
            TempDbLatencyChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(TempDbLatencyChart);
            TempDbLatencyChart.Plot.YLabel("Latency (ms)");
            TabHelpers.LockChartVerticalAxis(TempDbLatencyChart);
            TempDbLatencyChart.Refresh();
        }

        private void LoadTempdbStatsChart(IEnumerable<TempdbStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(TempdbStatsChart, out var existingTempdbPanel) && existingTempdbPanel != null)
            {
                TempdbStatsChart.Plot.Axes.Remove(existingTempdbPanel);
                _legendPanels[TempdbStatsChart] = null;
            }
            TempdbStatsChart.Plot.Clear();
            _tempdbStatsHover?.Clear();
            TabHelpers.ApplyThemeToChart(TempdbStatsChart);

            var dataList = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<TempdbStatsItem>();
            if (dataList.Count > 0)
            {
                // User Objects series
                var (userXs, userYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.UserObjectReservedMb));
                var userScatter = TempdbStatsChart.Plot.Add.Scatter(userXs, userYs);
                userScatter.LineWidth = 2;
                userScatter.MarkerSize = 5;
                userScatter.Color = TabHelpers.ChartColors[0];
                userScatter.LegendText = "User Objects";
                _tempdbStatsHover?.Add(userScatter, "User Objects");

                // Version Store series
                var (versionXs, versionYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.VersionStoreReservedMb));
                var versionScatter = TempdbStatsChart.Plot.Add.Scatter(versionXs, versionYs);
                versionScatter.LineWidth = 2;
                versionScatter.MarkerSize = 5;
                versionScatter.Color = TabHelpers.ChartColors[1];
                versionScatter.LegendText = "Version Store";
                _tempdbStatsHover?.Add(versionScatter, "Version Store");

                // Internal Objects series
                var (internalXs, internalYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.InternalObjectReservedMb));
                var internalScatter = TempdbStatsChart.Plot.Add.Scatter(internalXs, internalYs);
                internalScatter.LineWidth = 2;
                internalScatter.MarkerSize = 5;
                internalScatter.Color = TabHelpers.ChartColors[2];
                internalScatter.LegendText = "Internal Objects";
                _tempdbStatsHover?.Add(internalScatter, "Internal Objects");

                // Unallocated (free space) series
                var (unallocXs, unallocYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => (double)d.UnallocatedMb));
                if (unallocYs.Any(y => y > 0))
                {
                    var unallocScatter = TempdbStatsChart.Plot.Add.Scatter(unallocXs, unallocYs);
                    unallocScatter.LineWidth = 2;
                    unallocScatter.MarkerSize = 5;
                    unallocScatter.Color = TabHelpers.ChartColors[9];
                    unallocScatter.LegendText = "Unallocated";
                    _tempdbStatsHover?.Add(unallocScatter, "Unallocated");
                }

                // Top Task Total MB series (worst session's usage)
                var topTaskValues = dataList.Select(d => (double)(d.TopTaskTotalMb ?? 0)).ToArray();
                if (topTaskValues.Any(v => v > 0))
                {
                    var (topTaskXs, topTaskYs) = TabHelpers.FillTimeSeriesGaps(
                        dataList.Select(d => d.CollectionTime),
                        topTaskValues);
                    var topTaskScatter = TempdbStatsChart.Plot.Add.Scatter(topTaskXs, topTaskYs);
                    topTaskScatter.LineWidth = 2;
                    topTaskScatter.MarkerSize = 5;
                    topTaskScatter.Color = TabHelpers.ChartColors[3];
                    topTaskScatter.LegendText = "Top Task";
                }

                // Update summary panel with latest data point
                var latestData = dataList.LastOrDefault();
                UpdateTempdbStatsSummary(latestData);

                _legendPanels[TempdbStatsChart] = TempdbStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                TempdbStatsChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                UpdateTempdbStatsSummary(null);
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = TempdbStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            TempdbStatsChart.Plot.Axes.DateTimeTicksBottom();
            TempdbStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TempdbStatsChart.Plot.Axes.AutoScaleY();
            TempdbStatsChart.Plot.YLabel("MB");
            TabHelpers.LockChartVerticalAxis(TempdbStatsChart);
            TempdbStatsChart.Refresh();
        }

        private void UpdateTempdbStatsSummary(TempdbStatsItem? data)
        {
            if (data != null)
            {
                TempdbSessionsText.Text = $"{data.TotalSessionsUsingTempdb} ({data.SessionsWithUserObjects} user, {data.SessionsWithInternalObjects} internal)";
                
                var warnings = new System.Collections.Generic.List<string>();
                if (data.VersionStoreHighWarning) warnings.Add("Version Store High");
                if (data.AllocationContentionWarning) warnings.Add("Allocation Contention");
                TempdbWarningsText.Text = warnings.Count > 0 ? string.Join(", ", warnings) : "None";
                TempdbWarningsText.Foreground = warnings.Count > 0 
                    ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.OrangeRed)
                    : (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            }
            else
            {
                TempdbSessionsText.Text = "N/A";
                TempdbWarningsText.Text = "N/A";
                TempdbWarningsText.Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush");
            }
        }

        #endregion

        #region Session Stats Tab

        private async Task RefreshSessionStatsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetSessionStatsAsync(_sessionStatsHoursBack, _sessionStatsFromDate, _sessionStatsToDate);
                LoadSessionStatsChart(data, _sessionStatsHoursBack, _sessionStatsFromDate, _sessionStatsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading session stats: {ex.Message}", ex);
            }
        }

        private void LoadSessionStatsChart(IEnumerable<SessionStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(SessionStatsChart, out var existingSessionPanel) && existingSessionPanel != null)
            {
                SessionStatsChart.Plot.Axes.Remove(existingSessionPanel);
                _legendPanels[SessionStatsChart] = null;
            }
            SessionStatsChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(SessionStatsChart);
            _sessionStatsHover?.Clear();

            var dataList = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<SessionStatsItem>();
            if (dataList.Count > 0)
            {
                var timePoints = dataList.Select(d => d.CollectionTime);
                double[] totalCounts = dataList.Select(d => (double)d.TotalSessions).ToArray();
                double[] runningCounts = dataList.Select(d => (double)d.RunningSessions).ToArray();
                double[] sleepingCounts = dataList.Select(d => (double)d.SleepingSessions).ToArray();

                if (totalCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, totalCounts.Select(c => c));
                    var totalScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    totalScatter.LineWidth = 2;
                    totalScatter.MarkerSize = 5;
                    totalScatter.Color = TabHelpers.ChartColors[0];
                    totalScatter.LegendText = "Total";
                    _sessionStatsHover?.Add(totalScatter, "Total");
                }

                if (runningCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, runningCounts.Select(c => c));
                    var runningScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    runningScatter.LineWidth = 2;
                    runningScatter.MarkerSize = 5;
                    runningScatter.Color = TabHelpers.ChartColors[1];
                    runningScatter.LegendText = "Running";
                    _sessionStatsHover?.Add(runningScatter, "Running");
                }

                if (sleepingCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, sleepingCounts.Select(c => c));
                    var sleepingScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    sleepingScatter.LineWidth = 2;
                    sleepingScatter.MarkerSize = 5;
                    sleepingScatter.Color = TabHelpers.ChartColors[2];
                    sleepingScatter.LegendText = "Sleeping";
                    _sessionStatsHover?.Add(sleepingScatter, "Sleeping");
                }

                double[] backgroundCounts = dataList.Select(d => (double)d.BackgroundSessions).ToArray();
                if (backgroundCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, backgroundCounts.Select(c => c));
                    var backgroundScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    backgroundScatter.LineWidth = 2;
                    backgroundScatter.MarkerSize = 5;
                    backgroundScatter.Color = TabHelpers.ChartColors[4];
                    backgroundScatter.LegendText = "Background";
                    _sessionStatsHover?.Add(backgroundScatter, "Background");
                }

                double[] dormantCounts = dataList.Select(d => (double)d.DormantSessions).ToArray();
                if (dormantCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, dormantCounts.Select(c => c));
                    var dormantScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    dormantScatter.LineWidth = 2;
                    dormantScatter.MarkerSize = 5;
                    dormantScatter.Color = TabHelpers.ChartColors[5];
                    dormantScatter.LegendText = "Dormant";
                    _sessionStatsHover?.Add(dormantScatter, "Dormant");
                }

                double[] idleOver30MinCounts = dataList.Select(d => (double)d.IdleSessionsOver30Min).ToArray();
                if (idleOver30MinCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, idleOver30MinCounts.Select(c => c));
                    var idleScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    idleScatter.LineWidth = 2;
                    idleScatter.MarkerSize = 5;
                    idleScatter.Color = TabHelpers.ChartColors[9];
                    idleScatter.LegendText = "Idle >30m";
                    _sessionStatsHover?.Add(idleScatter, "Idle >30m");
                }

                double[] waitingForMemoryCounts = dataList.Select(d => (double)d.SessionsWaitingForMemory).ToArray();
                if (waitingForMemoryCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, waitingForMemoryCounts.Select(c => c));
                    var waitingScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    waitingScatter.LineWidth = 2;
                    waitingScatter.MarkerSize = 5;
                    waitingScatter.Color = TabHelpers.ChartColors[3];
                    waitingScatter.LegendText = "Waiting for Memory";
                    _sessionStatsHover?.Add(waitingScatter, "Waiting for Memory");
                }

                // Update summary panel with latest data point
                var latestData = dataList.LastOrDefault();
                UpdateSessionStatsSummary(latestData);

                _legendPanels[SessionStatsChart] = SessionStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                SessionStatsChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SessionStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                UpdateSessionStatsSummary(null);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            SessionStatsChart.Plot.Axes.DateTimeTicksBottom();
            SessionStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(SessionStatsChart);
            SessionStatsChart.Plot.YLabel("Session Count");
            TabHelpers.LockChartVerticalAxis(SessionStatsChart);
            SessionStatsChart.Refresh();
        }

        private void UpdateSessionStatsSummary(SessionStatsItem? data)
        {
            if (data != null)
            {
                SessionStatsTopAppText.Text = !string.IsNullOrEmpty(data.TopApplicationName) 
                    ? $"{data.TopApplicationName} ({data.TopApplicationConnections ?? 0})" 
                    : "N/A";
                SessionStatsTopHostText.Text = !string.IsNullOrEmpty(data.TopHostName) 
                    ? $"{data.TopHostName} ({data.TopHostConnections ?? 0})" 
                    : "N/A";
                SessionStatsDatabasesText.Text = data.DatabasesWithConnections.ToString(CultureInfo.CurrentCulture);
            }
            else
            {
                SessionStatsTopAppText.Text = "N/A";
                SessionStatsTopHostText.Text = "N/A";
                SessionStatsDatabasesText.Text = "N/A";
            }
        }

        #endregion

        #region File I/O Latency Tab

        private async Task LoadFileIoLatencyChartsAsync()
        {
            if (_databaseService == null) return;

            DateTime rangeEnd = _fileIoToDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = _fileIoFromDate ?? rangeEnd.AddHours(-_fileIoHoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var colors = TabHelpers.ChartColors;

            // Load User DB data only - TempDB latency moved to TempDB Stats tab
            var userDbData = await _databaseService.GetFileIoLatencyTimeSeriesAsync(isTempDb: false, _fileIoHoursBack, _fileIoFromDate, _fileIoToDate);
            LoadFileIoChart(UserDbReadLatencyChart, userDbData, d => d.ReadLatencyMs, "Read Latency (ms)", colors, xMin, xMax, _fileIoReadHover, d => d.ReadQueuedLatencyMs);
            LoadFileIoChart(UserDbWriteLatencyChart, userDbData, d => d.WriteLatencyMs, "Write Latency (ms)", colors, xMin, xMax, _fileIoWriteHover, d => d.WriteQueuedLatencyMs);
        }

        private void LoadFileIoChart(ScottPlot.WPF.WpfPlot chart, List<FileIoLatencyTimeSeriesItem> data, Func<FileIoLatencyTimeSeriesItem, decimal> latencySelector, string yLabel, ScottPlot.Color[] colors, double xMin, double xMax, Helpers.ChartHoverHelper? hover = null, Func<FileIoLatencyTimeSeriesItem, decimal>? queuedSelector = null)
        {
            DateTime rangeStart = DateTime.FromOADate(xMin);
            DateTime rangeEnd = DateTime.FromOADate(xMax);

            // Remove previously stored legend panel by reference (ScottPlot issue #4717)
            if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
            {
                chart.Plot.Axes.Remove(existingPanel);
                _legendPanels[chart] = null;
            }
            chart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(chart);
            hover?.Clear();

            // Check if any queued data exists (only render overlay if there's real data)
            bool hasQueuedData = queuedSelector != null && data != null && data.Any(d => queuedSelector(d) > 0);

            if (data != null && data.Count > 0)
            {
                // Get all unique time points for gap filling
                // Group by file (database + filename)
                var fileGroups = data.GroupBy(d => $"{d.DatabaseName}.{d.FileName}")
                    .Where(g => g.Any(x => latencySelector(x) > 0))
                    .OrderByDescending(g => g.Average(x => (double)latencySelector(x)))
                    .Take(10)
                    .ToList();

                int colorIndex = 0;
                foreach (var group in fileGroups)
                {
                    var fileData = group.OrderBy(d => d.CollectionTime).ToList();
                    if (fileData.Count >= 1)
                    {
                        var timePoints = fileData.Select(d => d.CollectionTime);
                        var values = fileData.Select(d => (double)latencySelector(d));
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                        var scatter = chart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        var color = colors[colorIndex % colors.Length];
                        scatter.Color = color;

                        // Use just the filename for legend (not database.filename which is redundant)
                        var fileName = fileData.First().FileName;
                        scatter.LegendText = fileName;
                        hover?.Add(scatter, fileName);

                        // Add queued I/O overlay as dashed line with same color
                        if (hasQueuedData)
                        {
                            var queuedValues = fileData.Select(d => (double)queuedSelector!(d));
                            if (queuedValues.Any(v => v > 0))
                            {
                                var (qxs, qys) = TabHelpers.FillTimeSeriesGaps(timePoints, queuedValues);
                                var queuedScatter = chart.Plot.Add.Scatter(qxs, qys);
                                queuedScatter.LineWidth = 2;
                                queuedScatter.MarkerSize = 0;
                                queuedScatter.Color = color;
                                queuedScatter.LinePattern = ScottPlot.LinePattern.Dashed;
                                queuedScatter.LegendText = $"{fileName} (queued)";
                                hover?.Add(queuedScatter, $"{fileName} (queued)");
                            }
                        }

                        colorIndex++;
                    }
                }

                if (fileGroups.Count > 0)
                {
                    // Store legend panel reference for removal on refresh (ScottPlot issue #4717)
                    _legendPanels[chart] = chart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    chart.Plot.Legend.FontSize = 12;
                }
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = chart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            chart.Plot.Axes.DateTimeTicksBottom();
            chart.Plot.Axes.SetLimitsX(xMin, xMax);
            chart.Plot.YLabel(yLabel);
            TabHelpers.LockChartVerticalAxis(chart);
            chart.Refresh();
        }

        private async Task LoadFileIoThroughputChartsAsync()
        {
            if (_databaseService == null) return;

            DateTime rangeEnd = _fileIoToDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = _fileIoFromDate ?? rangeEnd.AddHours(-_fileIoHoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var colors = TabHelpers.ChartColors;

            var throughputData = await _databaseService.GetFileIoThroughputTimeSeriesAsync(isTempDb: false, _fileIoHoursBack, _fileIoFromDate, _fileIoToDate);
            LoadFileIoChart(FileIoReadThroughputChart, throughputData, d => d.ReadThroughputMbPerSec, "Read Throughput (MB/s)", colors, xMin, xMax, _fileIoReadThroughputHover);
            LoadFileIoChart(FileIoWriteThroughputChart, throughputData, d => d.WriteThroughputMbPerSec, "Write Throughput (MB/s)", colors, xMin, xMax, _fileIoWriteThroughputHover);
        }

        #endregion

        #region Server Trends Tab

        private async Task RefreshServerTrendsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var cpuTask = _databaseService.GetCpuSpikesAsync(_serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);
                var tempdbTask = _databaseService.GetTempdbStatsAsync(_serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);
                var memoryTask = _databaseService.GetMemoryStatsAsync(_serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);
                var perfmonTask = _databaseService.GetPerfmonStatsFilteredAsync(
                    new[] { "Batch Requests/sec", "SQL Compilations/sec", "SQL Re-Compilations/sec", "Optimizer Statistics" },
                    _serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);

                await Task.WhenAll(cpuTask, tempdbTask, memoryTask, perfmonTask);

                LoadServerTrendsCpuChart(await cpuTask, _serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);
                LoadServerTrendsTempdbChart(await tempdbTask, _serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);
                LoadServerTrendsMemoryChart(await memoryTask, _serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);
                LoadServerTrendsPerfmonChart(await perfmonTask, _serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate);

                try
                {
                    var pressure = await _databaseService.GetCpuPressureAsync();
                    UpdateCpuSchedulerStatus(pressure);
                }
                catch (Exception pressureEx)
                {
                    Logger.Error($"Error loading CPU scheduler pressure: {pressureEx.Message}", pressureEx);
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading server trends: {ex.Message}", ex);
            }
        }

        private void UpdateCpuSchedulerStatus(CpuPressureItem? pressure)
        {
            if (pressure == null)
            {
                CpuSchedulerStatusText.Text = "";
                return;
            }

            CpuSchedulerStatusText.Inlines.Clear();

            var summary = $"Schedulers: {pressure.TotalSchedulers} | " +
                          $"Workers: {pressure.TotalWorkers:N0}/{pressure.MaxWorkers:N0} ({pressure.WorkerUtilizationPercent:F1}%) | " +
                          $"Runnable: {pressure.TotalRunnableTasks} ({pressure.AvgRunnableTasksPerScheduler:F2}/sched) | " +
                          $"Active: {pressure.TotalActiveRequests} | " +
                          $"Queued: {pressure.TotalQueuedRequests} | ";

            CpuSchedulerStatusText.Inlines.Add(new Run(summary));

            var levelText = pressure.PressureLevel;
            var levelRun = new Run(levelText);

            if (levelText.Contains("CRITICAL") || levelText.Contains("HIGH"))
            {
                levelRun.Foreground = new SolidColorBrush(Color.FromRgb(0xFF, 0x44, 0x44));
                levelRun.FontWeight = FontWeights.Bold;
            }
            else if (levelText.Contains("MEDIUM"))
            {
                levelRun.Foreground = new SolidColorBrush(Color.FromRgb(0xFF, 0xA5, 0x00));
                levelRun.FontWeight = FontWeights.Bold;
            }

            CpuSchedulerStatusText.Inlines.Add(levelRun);
        }

        private void LoadServerTrendsCpuChart(IEnumerable<CpuSpikeItem> cpuData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ServerUtilTrendsCpuChart, out var existingCpuPanel) && existingCpuPanel != null)
            {
                ServerUtilTrendsCpuChart.Plot.Axes.Remove(existingCpuPanel);
                _legendPanels[ServerUtilTrendsCpuChart] = null;
            }
            ServerUtilTrendsCpuChart.Plot.Clear();
            _serverTrendsCpuHover?.Clear();
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsCpuChart);

            var dataList = cpuData?.OrderBy(d => d.EventTime).ToList() ?? new List<CpuSpikeItem>();
            if (dataList.Count > 0)
            {
                // SQL CPU series
                var (sqlXs, sqlYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.EventTime),
                    dataList.Select(d => (double)d.SqlServerCpu));
                var sqlScatter = ServerUtilTrendsCpuChart.Plot.Add.Scatter(sqlXs, sqlYs);
                sqlScatter.LineWidth = 2;
                sqlScatter.MarkerSize = 5;
                sqlScatter.Color = TabHelpers.ChartColors[0];
                sqlScatter.LegendText = "SQL CPU";
                _serverTrendsCpuHover?.Add(sqlScatter, "SQL CPU");

                // Other CPU series
                var (otherXs, otherYs) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.EventTime),
                    dataList.Select(d => (double)d.OtherProcessCpu));
                var otherScatter = ServerUtilTrendsCpuChart.Plot.Add.Scatter(otherXs, otherYs);
                otherScatter.LineWidth = 2;
                otherScatter.MarkerSize = 5;
                otherScatter.Color = TabHelpers.ChartColors[2];
                otherScatter.LegendText = "Other CPU";
                _serverTrendsCpuHover?.Add(otherScatter, "Other CPU");

                _legendPanels[ServerUtilTrendsCpuChart] = ServerUtilTrendsCpuChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ServerUtilTrendsCpuChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ServerUtilTrendsCpuChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ServerUtilTrendsCpuChart.Plot.Axes.DateTimeTicksBottom();
            ServerUtilTrendsCpuChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(ServerUtilTrendsCpuChart);
            ServerUtilTrendsCpuChart.Plot.YLabel("CPU %");
            TabHelpers.LockChartVerticalAxis(ServerUtilTrendsCpuChart);
            ServerUtilTrendsCpuChart.Refresh();
        }

        private void LoadServerTrendsTempdbChart(IEnumerable<TempdbStatsItem> tempdbData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ServerUtilTrendsTempdbChart, out var existingTrendsTempdbPanel) && existingTrendsTempdbPanel != null)
            {
                ServerUtilTrendsTempdbChart.Plot.Axes.Remove(existingTrendsTempdbPanel);
                _legendPanels[ServerUtilTrendsTempdbChart] = null;
            }
            ServerUtilTrendsTempdbChart.Plot.Clear();
            _serverTrendsTempdbHover?.Clear();
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsTempdbChart);

            var dataList = tempdbData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<TempdbStatsItem>();
            if (dataList.Count >= 1)
            {
                var userTimePoints = dataList.Select(d => d.CollectionTime);
                var userValues = dataList.Select(d => (double)(d.UserObjectReservedPageCount * 8 / 1024));
                var (userXs, userYs) = TabHelpers.FillTimeSeriesGaps(userTimePoints, userValues);

                var versionTimePoints = dataList.Select(d => d.CollectionTime);
                var versionValues = dataList.Select(d => (double)(d.VersionStoreReservedPageCount * 8 / 1024));
                var (versionXs, versionYs) = TabHelpers.FillTimeSeriesGaps(versionTimePoints, versionValues);

                var userScatter = ServerUtilTrendsTempdbChart.Plot.Add.Scatter(userXs, userYs);
                userScatter.LineWidth = 2;
                userScatter.MarkerSize = 5;
                userScatter.Color = TabHelpers.ChartColors[1];
                userScatter.LegendText = "User Objects";
                _serverTrendsTempdbHover?.Add(userScatter, "User Objects");

                var versionScatter = ServerUtilTrendsTempdbChart.Plot.Add.Scatter(versionXs, versionYs);
                versionScatter.LineWidth = 2;
                versionScatter.MarkerSize = 5;
                versionScatter.Color = TabHelpers.ChartColors[2];
                versionScatter.LegendText = "Version Store";
                _serverTrendsTempdbHover?.Add(versionScatter, "Version Store");

                _legendPanels[ServerUtilTrendsTempdbChart] = ServerUtilTrendsTempdbChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ServerUtilTrendsTempdbChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ServerUtilTrendsTempdbChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ServerUtilTrendsTempdbChart.Plot.Axes.DateTimeTicksBottom();
            ServerUtilTrendsTempdbChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(ServerUtilTrendsTempdbChart);
            ServerUtilTrendsTempdbChart.Plot.YLabel("MB");
            TabHelpers.LockChartVerticalAxis(ServerUtilTrendsTempdbChart);
            ServerUtilTrendsTempdbChart.Refresh();
        }

        private void LoadServerTrendsMemoryChart(IEnumerable<MemoryStatsItem> memoryData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ServerUtilTrendsMemoryChart, out var existingMemoryPanel) && existingMemoryPanel != null)
            {
                ServerUtilTrendsMemoryChart.Plot.Axes.Remove(existingMemoryPanel);
                _legendPanels[ServerUtilTrendsMemoryChart] = null;
            }
            ServerUtilTrendsMemoryChart.Plot.Clear();
            _serverTrendsMemoryHover?.Clear();
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsMemoryChart);

            var dataList = memoryData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<MemoryStatsItem>();
            if (dataList.Count >= 1)
            {
                var bufferTimePoints = dataList.Select(d => d.CollectionTime);
                var bufferValues = dataList.Select(d => (double)d.BufferPoolMb);
                var (bufferXs, bufferYs) = TabHelpers.FillTimeSeriesGaps(bufferTimePoints, bufferValues);

                var cacheTimePoints = dataList.Select(d => d.CollectionTime);
                var cacheValues = dataList.Select(d => (double)d.PlanCacheMb);
                var (cacheXs, cacheYs) = TabHelpers.FillTimeSeriesGaps(cacheTimePoints, cacheValues);

                // DEBUG: Log the last data points to diagnose the "drop to 0" issue
                if (bufferXs.Length > 0)
                {
                    var lastTime = DateTime.FromOADate(bufferXs[^1]);
                    var lastValue = bufferYs[^1];
                    Logger.Info($"Memory chart: Last buffer point = {lastTime:yyyy-MM-dd HH:mm:ss}, Value = {lastValue}. Array length = {bufferXs.Length}");
                    Logger.Info($"Memory chart: rangeStart = {rangeStart:yyyy-MM-dd HH:mm:ss}, rangeEnd = {rangeEnd:yyyy-MM-dd HH:mm:ss}");

                    // Check for any zero values in the array
                    var zeroIndices = bufferYs.Select((v, i) => new { Value = v, Index = i })
                        .Where(x => x.Value == 0)
                        .Select(x => x.Index)
                        .ToList();
                    if (zeroIndices.Count > 0)
                    {
                        Logger.Warning($"Memory chart: Found {zeroIndices.Count} zero values at indices: {string.Join(", ", zeroIndices.Take(10))}");
                        foreach (var idx in zeroIndices.Take(5))
                        {
                            var zeroTime = DateTime.FromOADate(bufferXs[idx]);
                            Logger.Warning($"  Zero at index {idx}: Time = {zeroTime:yyyy-MM-dd HH:mm:ss}");
                        }
                    }

                    // Log first and last 3 points
                    Logger.Info($"Memory chart: First 3 points:");
                    for (int i = 0; i < Math.Min(3, bufferXs.Length); i++)
                    {
                        Logger.Info($"  [{i}] Time = {DateTime.FromOADate(bufferXs[i]):HH:mm:ss}, Value = {bufferYs[i]}");
                    }
                    Logger.Info($"Memory chart: Last 3 points:");
                    for (int i = Math.Max(0, bufferXs.Length - 3); i < bufferXs.Length; i++)
                    {
                        Logger.Info($"  [{i}] Time = {DateTime.FromOADate(bufferXs[i]):HH:mm:ss}, Value = {bufferYs[i]}");
                    }
                }

                var bufferScatter = ServerUtilTrendsMemoryChart.Plot.Add.Scatter(bufferXs, bufferYs);
                bufferScatter.LineWidth = 2;
                bufferScatter.MarkerSize = 5;
                bufferScatter.Color = TabHelpers.ChartColors[4];
                bufferScatter.LegendText = "Buffer Pool";
                _serverTrendsMemoryHover?.Add(bufferScatter, "Buffer Pool");

                var cacheScatter = ServerUtilTrendsMemoryChart.Plot.Add.Scatter(cacheXs, cacheYs);
                cacheScatter.LineWidth = 2;
                cacheScatter.MarkerSize = 5;
                cacheScatter.Color = TabHelpers.ChartColors[5];
                cacheScatter.LegendText = "Plan Cache";
                _serverTrendsMemoryHover?.Add(cacheScatter, "Plan Cache");

                _legendPanels[ServerUtilTrendsMemoryChart] = ServerUtilTrendsMemoryChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ServerUtilTrendsMemoryChart.Plot.Legend.FontSize = 12;

                // Limit X-axis to actual data range to prevent ScottPlot from extrapolating beyond data
                if (bufferXs.Length > 0)
                {
                    var oldXMax = xMax;
                    xMax = Math.Min(xMax, bufferXs[^1]);
                    Logger.Info($"Memory chart: xMax changed from {DateTime.FromOADate(oldXMax):HH:mm:ss} to {DateTime.FromOADate(xMax):HH:mm:ss}");
                }
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ServerUtilTrendsMemoryChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ServerUtilTrendsMemoryChart.Plot.Axes.DateTimeTicksBottom();
            ServerUtilTrendsMemoryChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(ServerUtilTrendsMemoryChart);
            ServerUtilTrendsMemoryChart.Plot.YLabel("MB");
            TabHelpers.LockChartVerticalAxis(ServerUtilTrendsMemoryChart);
            ServerUtilTrendsMemoryChart.Refresh();
        }

        private void LoadServerTrendsPerfmonChart(IEnumerable<PerfmonStatsItem> perfmonData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ServerUtilTrendsPerfmonChart, out var existingPerfmonTrendsPanel) && existingPerfmonTrendsPanel != null)
            {
                ServerUtilTrendsPerfmonChart.Plot.Axes.Remove(existingPerfmonTrendsPanel);
                _legendPanels[ServerUtilTrendsPerfmonChart] = null;
            }
            ServerUtilTrendsPerfmonChart.Plot.Clear();
            _serverTrendsPerfmonHover?.Clear();
            TabHelpers.ApplyThemeToChart(ServerUtilTrendsPerfmonChart);

            var allData = (perfmonData ?? Enumerable.Empty<PerfmonStatsItem>()).ToList();

            // Counters to display
            var countersToShow = new[] {
                ("Batch Requests/sec", TabHelpers.ChartColors[0]),
                ("SQL Compilations/sec", TabHelpers.ChartColors[2]),
                ("SQL Re-Compilations/sec", TabHelpers.ChartColors[3]),
                ("Optimizer Statistics", TabHelpers.ChartColors[1])
            };

            // Get all time points across all counters for gap filling
            int linesAdded = 0;
            foreach (var (counterName, color) in countersToShow)
            {
                var counterData = allData
                    .Where(d => d.CounterName == counterName)
                    .GroupBy(d => d.CollectionTime)
                    .Select(g => new { Time = g.Key, Value = g.Sum(x => x.CntrValuePerSecond ?? x.CntrValueDelta ?? x.CntrValue) })
                    .OrderBy(d => d.Time)
                    .ToList();

                if (counterData.Count >= 1)
                {
                    var timePoints = counterData.Select(d => d.Time);
                    var values = counterData.Select(d => (double)d.Value);
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                    var scatter = ServerUtilTrendsPerfmonChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = color;
                    scatter.LegendText = counterName.Replace("/sec", "", StringComparison.Ordinal);
                    _serverTrendsPerfmonHover?.Add(scatter, counterName);
                    linesAdded++;
                }
            }

            if (linesAdded > 0)
            {
                _legendPanels[ServerUtilTrendsPerfmonChart] = ServerUtilTrendsPerfmonChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ServerUtilTrendsPerfmonChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ServerUtilTrendsPerfmonChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ServerUtilTrendsPerfmonChart.Plot.Axes.DateTimeTicksBottom();
            ServerUtilTrendsPerfmonChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(ServerUtilTrendsPerfmonChart);
            ServerUtilTrendsPerfmonChart.Plot.YLabel("Per Second");
            TabHelpers.LockChartVerticalAxis(ServerUtilTrendsPerfmonChart);
            ServerUtilTrendsPerfmonChart.Refresh();
        }

        #endregion

        #region Context Menu Handlers

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid && grid.CurrentCell.Column != null)
                {
                    var cellContent = TabHelpers.GetCellContent(grid, grid.CurrentCell);
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
                if (contextMenu.PlacementTarget is DataGrid grid && grid.SelectedItem != null)
                {
                    var rowText = TabHelpers.GetRowAsText(grid, grid.SelectedItem);
                    if (!string.IsNullOrEmpty(rowText))
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(rowText, false);
                    }
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var sb = new StringBuilder();

                    var headers = grid.Columns.Select(c => Helpers.DataGridClipboardBehavior.GetHeaderText(c));
                    sb.AppendLine(string.Join("\t", headers));

                    foreach (var item in grid.Items)
                    {
                        var values = new List<string>();
                        foreach (var column in grid.Columns)
                        {
                            var binding = (column as DataGridBoundColumn)?.Binding as System.Windows.Data.Binding;
                            if (binding != null)
                            {
                                var prop = item.GetType().GetProperty(binding.Path.Path);
                                var value = prop?.GetValue(item)?.ToString() ?? string.Empty;
                                values.Add(value);
                            }
                        }
                        sb.AppendLine(string.Join("\t", values));
                    }

                    if (sb.Length > 0)
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(sb.ToString(), false);
                    }
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                if (contextMenu.PlacementTarget is DataGrid grid)
                {
                    var dialog = new SaveFileDialog
                    {
                        Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                        DefaultExt = ".csv",
                        FileName = $"ResourceMetrics_Export_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
                    };

                    if (dialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            var sep = TabHelpers.CsvSeparator;
                            var headers = grid.Columns.Select(c => TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(c), sep));
                            sb.AppendLine(string.Join(sep, headers));

                            foreach (var item in grid.Items)
                            {
                                var values = new List<string>();
                                foreach (var column in grid.Columns)
                                {
                                    var binding = (column as DataGridBoundColumn)?.Binding as System.Windows.Data.Binding;
                                    if (binding != null)
                                    {
                                        var prop = item.GetType().GetProperty(binding.Path.Path);
                                        values.Add(TabHelpers.EscapeCsvField(TabHelpers.FormatForExport(prop?.GetValue(item)), sep));
                                    }
                                }
                                sb.AppendLine(string.Join(sep, values));
                            }

                            File.WriteAllText(dialog.FileName, sb.ToString());
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Error exporting to CSV: {ex.Message}", ex);
                            MessageBox.Show($"Error exporting to CSV: {ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }

        #endregion

        #region Perfmon Counters Tab

        private bool _isUpdatingPerfmonSelection = false;

        private void PerfmonCountersList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            // Not used - we handle via checkbox changes instead
        }

        private async void PerfmonCounter_CheckChanged(object sender, RoutedEventArgs e)
        {
            if (_isUpdatingPerfmonSelection) return;
            RefreshPerfmonCounterListOrder();
            await UpdatePerfmonCountersChartAsync();
        }

        private void RefreshPerfmonCounterListOrder()
        {
            if (_perfmonCounterItems == null) return;
            // Sort: checked items first, then alphabetically
            var sorted = _perfmonCounterItems
                .OrderByDescending(x => x.IsSelected)
                .ThenBy(x => x.CounterName)
                .ToList();
            _perfmonCounterItems = sorted;
            ApplyPerfmonCounterSearchFilter();
        }

        private void PerfmonCounterSearch_TextChanged(object sender, TextChangedEventArgs e)
        {
            ApplyPerfmonCounterSearchFilter();
        }

        private void ApplyPerfmonCounterSearchFilter()
        {
            if (_perfmonCounterItems == null)
            {
                PerfmonCountersList.ItemsSource = null;
                return;
            }

            var searchText = PerfmonCounterSearchBox?.Text?.Trim() ?? string.Empty;
            if (string.IsNullOrEmpty(searchText))
            {
                PerfmonCountersList.ItemsSource = null;
                PerfmonCountersList.ItemsSource = _perfmonCounterItems;
            }
            else
            {
                var filtered = _perfmonCounterItems
                    .Where(c => c.CounterName.Contains(searchText, StringComparison.OrdinalIgnoreCase) ||
                                c.ObjectName.Contains(searchText, StringComparison.OrdinalIgnoreCase))
                    .ToList();
                PerfmonCountersList.ItemsSource = null;
                PerfmonCountersList.ItemsSource = filtered;
            }
        }

        private async void PerfmonCounters_SelectAll_Click(object sender, RoutedEventArgs e)
        {
            if (_perfmonCounterItems == null) return;
            _isUpdatingPerfmonSelection = true;
            foreach (var item in _perfmonCounterItems)
            {
                item.IsSelected = true;
            }
            _isUpdatingPerfmonSelection = false;
            await UpdatePerfmonCountersChartAsync();
        }

        private async void PerfmonCounters_ClearAll_Click(object sender, RoutedEventArgs e)
        {
            if (_perfmonCounterItems == null) return;
            _isUpdatingPerfmonSelection = true;
            foreach (var item in _perfmonCounterItems)
            {
                item.IsSelected = false;
            }
            _isUpdatingPerfmonSelection = false;
            await UpdatePerfmonCountersChartAsync();
        }

        private async void PerfmonPack_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_perfmonCounterItems == null || _perfmonCounterItems.Count == 0) return;
            if (PerfmonPackCombo.SelectedItem is not string pack) return;

            _isUpdatingPerfmonSelection = true;

            /* Clear search so all counters are visible */
            if (PerfmonCounterSearchBox != null)
                PerfmonCounterSearchBox.Text = "";

            /* Uncheck everything first */
            foreach (var item in _perfmonCounterItems)
                item.IsSelected = false;

            if (pack == PerfmonPacks.AllCounters)
            {
                /* "All Counters" selects the General Throughput defaults */
                var defaultSet = new HashSet<string>(PerfmonPacks.Packs["General Throughput"], StringComparer.OrdinalIgnoreCase);
                foreach (var item in _perfmonCounterItems)
                {
                    if (defaultSet.Contains(item.CounterName))
                        item.IsSelected = true;
                }
            }
            else if (PerfmonPacks.Packs.TryGetValue(pack, out var packCounters))
            {
                var packSet = new HashSet<string>(packCounters, StringComparer.OrdinalIgnoreCase);
                int count = 0;
                foreach (var item in _perfmonCounterItems)
                {
                    if (count >= 12) break;
                    if (packSet.Contains(item.CounterName))
                    {
                        item.IsSelected = true;
                        count++;
                    }
                }
            }

            _isUpdatingPerfmonSelection = false;
            RefreshPerfmonCounterListOrder();
            await UpdatePerfmonCountersChartAsync();
        }

        private async void PerfmonCounters_Refresh_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                await RefreshPerfmonCountersTabAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing perfmon counters: {ex.Message}", ex);
            }
        }

        private async Task RefreshPerfmonCountersTabAsync()
        {
            if (_databaseService == null) return;

            /* Initialize pack ComboBox once */
            if (PerfmonPackCombo.Items.Count == 0)
            {
                PerfmonPackCombo.ItemsSource = PerfmonPacks.PackNames;
                PerfmonPackCombo.SelectedItem = "General Throughput";
            }

            try
            {
                // Lightweight query: get only distinct counter names for the picker
                var counterNames = await _databaseService.GetPerfmonCounterNamesAsync(_perfmonCountersHoursBack, _perfmonCountersFromDate, _perfmonCountersToDate);

                // Remember previously selected counters
                var previouslySelected = _perfmonCounterItems?.Where(x => x.IsSelected).Select(x => x.FullName).ToHashSet() ?? new HashSet<string>();

                // Build unique counter list from lightweight query
                var counters = counterNames
                    .OrderBy(c => c.ObjectName)
                    .ThenBy(c => c.CounterName)
                    .Select(c => new PerfmonCounterSelectionItem
                    {
                        ObjectName = c.ObjectName,
                        CounterName = c.CounterName,
                        IsSelected = previouslySelected.Contains($"{c.ObjectName} - {c.CounterName}")
                    })
                    .ToList();

                // If nothing was previously selected, default select General Throughput pack
                if (!counters.Any(c => c.IsSelected))
                {
                    var defaultCounters = PerfmonPacks.Packs["General Throughput"];
                    var defaultSet = new HashSet<string>(defaultCounters, StringComparer.OrdinalIgnoreCase);
                    foreach (var item in counters.Where(c => defaultSet.Contains(c.CounterName)))
                    {
                        item.IsSelected = true;
                    }
                }

                _perfmonCounterItems = counters;
                // Sort so checked items appear at top
                RefreshPerfmonCounterListOrder();

                // Fetch data only for selected counters
                await UpdatePerfmonCountersChartAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading perfmon counters: {ex.Message}");
            }
        }

        private async Task UpdatePerfmonCountersChartAsync()
        {
            if (_databaseService == null || _perfmonCounterItems == null) return;

            var selectedCounterNames = _perfmonCounterItems
                .Where(x => x.IsSelected)
                .Select(x => x.CounterName)
                .Distinct()
                .ToArray();

            if (selectedCounterNames.Length == 0)
            {
                _allPerfmonCountersData = new List<PerfmonStatsItem>();
            }
            else
            {
                var data = await _databaseService.GetPerfmonStatsFilteredAsync(
                    selectedCounterNames, _perfmonCountersHoursBack, _perfmonCountersFromDate, _perfmonCountersToDate);
                _allPerfmonCountersData = data?.ToList() ?? new List<PerfmonStatsItem>();
            }

            LoadPerfmonCountersChart(_allPerfmonCountersData, _perfmonCountersHoursBack, _perfmonCountersFromDate, _perfmonCountersToDate);
        }

        private void LoadPerfmonCountersChart(List<PerfmonStatsItem>? data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(PerfmonCountersChart, out var existingPerfmonPanel) && existingPerfmonPanel != null)
            {
                PerfmonCountersChart.Plot.Axes.Remove(existingPerfmonPanel);
                _legendPanels[PerfmonCountersChart] = null;
            }
            PerfmonCountersChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(PerfmonCountersChart);
            _perfmonHover?.Clear();

            if (data == null || data.Count == 0 || _perfmonCounterItems == null)
            {
                PerfmonCountersChart.Refresh();
                return;
            }

            // Get selected counters
            var selectedCounters = _perfmonCounterItems.Where(x => x.IsSelected).ToList();
            if (selectedCounters.Count == 0)
            {
                PerfmonCountersChart.Refresh();
                return;
            }

            var colors = TabHelpers.ChartColors;

            // Get all time points across all counters for gap filling
            int colorIndex = 0;
            foreach (var counter in selectedCounters.Take(12)) // Limit to 12 counters
            {
                // Get data for this counter (aggregated across all instances)
                var counterData = data
                    .Where(d => d.ObjectName == counter.ObjectName && d.CounterName == counter.CounterName)
                    .GroupBy(d => d.CollectionTime)
                    .Select(g => new {
                        CollectionTime = g.Key,
                        Value = g.Sum(x => x.CntrValuePerSecond ?? x.CntrValueDelta ?? x.CntrValue)
                    })
                    .OrderBy(d => d.CollectionTime)
                    .ToList();

                if (counterData.Count >= 1)
                {
                    var timePoints = counterData.Select(d => d.CollectionTime);
                    var values = counterData.Select(d => (double)d.Value);
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                    var scatter = PerfmonCountersChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5; // Show small markers to ensure visibility
                    scatter.Color = colors[colorIndex % colors.Length];
                    scatter.LegendText = counter.CounterName;
                    _perfmonHover?.Add(scatter, counter.CounterName);

                    colorIndex++;
                }
            }

            if (colorIndex > 0)
            {
                _legendPanels[PerfmonCountersChart] = PerfmonCountersChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                PerfmonCountersChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = PerfmonCountersChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            PerfmonCountersChart.Plot.Axes.DateTimeTicksBottom();
            PerfmonCountersChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(PerfmonCountersChart);
            PerfmonCountersChart.Plot.YLabel("Value/sec");
            TabHelpers.LockChartVerticalAxis(PerfmonCountersChart);
            PerfmonCountersChart.Refresh();
        }

        #endregion

        #region Wait Stats Detail Tab

        private void WaitTypesList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            // Not used - we handle via checkbox changes instead
        }

        private async void WaitType_CheckChanged(object sender, RoutedEventArgs e)
        {
            if (_isUpdatingWaitTypeSelection) return;
            RefreshWaitTypeListOrder();
            await UpdateWaitStatsDetailChartAsync();
        }

        private void WaitStatsMetric_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_allWaitStatsDetailData != null)
                LoadWaitStatsDetailChart(_allWaitStatsDetailData, _waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);
        }

        private void RefreshWaitTypeListOrder()
        {
            if (_waitTypeItems == null) return;
            // Sort: checked items first, then alphabetically
            var sorted = _waitTypeItems
                .OrderByDescending(x => x.IsSelected)
                .ThenBy(x => x.WaitType)
                .ToList();
            _waitTypeItems = sorted;
            ApplyWaitTypeSearchFilter();
            UpdateWaitTypeCount();
        }

        private void UpdateWaitTypeCount()
        {
            if (_waitTypeItems == null || WaitTypeCountText == null) return;
            int count = _waitTypeItems.Count(x => x.IsSelected);
            WaitTypeCountText.Text = $"{count} / 30 selected";
            WaitTypeCountText.Foreground = count >= 30
                ? new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#E57373")!)
                : (System.Windows.Media.Brush)FindResource("ForegroundBrush");
        }

        private void WaitTypeSearch_TextChanged(object sender, TextChangedEventArgs e)
        {
            ApplyWaitTypeSearchFilter();
        }

        private void ApplyWaitTypeSearchFilter()
        {
            if (_waitTypeItems == null)
            {
                WaitTypesList.ItemsSource = null;
                return;
            }

            var searchText = WaitTypeSearchBox?.Text?.Trim() ?? string.Empty;
            if (string.IsNullOrEmpty(searchText))
            {
                WaitTypesList.ItemsSource = null;
                WaitTypesList.ItemsSource = _waitTypeItems;
            }
            else
            {
                var filtered = _waitTypeItems
                    .Where(c => c.WaitType.Contains(searchText, StringComparison.OrdinalIgnoreCase))
                    .ToList();
                WaitTypesList.ItemsSource = null;
                WaitTypesList.ItemsSource = filtered;
            }
        }

        private async void WaitTypes_SelectAll_Click(object sender, RoutedEventArgs e)
        {
            if (_waitTypeItems == null) return;
            _isUpdatingWaitTypeSelection = true;
            var topWaits = TabHelpers.GetDefaultWaitTypes(_waitTypeItems.Select(x => x.WaitType).ToList());
            foreach (var item in _waitTypeItems)
            {
                item.IsSelected = topWaits.Contains(item.WaitType);
            }
            _isUpdatingWaitTypeSelection = false;
            RefreshWaitTypeListOrder();
            await UpdateWaitStatsDetailChartAsync();
        }

        private async void WaitTypes_ClearAll_Click(object sender, RoutedEventArgs e)
        {
            if (_waitTypeItems == null) return;
            _isUpdatingWaitTypeSelection = true;
            foreach (var item in _waitTypeItems)
            {
                item.IsSelected = false;
            }
            _isUpdatingWaitTypeSelection = false;
            RefreshWaitTypeListOrder();
            await UpdateWaitStatsDetailChartAsync();
        }

        private async void WaitStatsDetail_Refresh_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                await RefreshWaitStatsDetailTabAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing wait stats detail: {ex.Message}", ex);
            }
        }

        private async Task RefreshWaitStatsDetailTabAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Lightweight query: get only distinct wait type names for the picker
                var waitTypeNames = await _databaseService.GetWaitTypeNamesAsync(_waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);

                // Remember previously selected wait types
                var previouslySelected = _waitTypeItems?.Where(x => x.IsSelected).Select(x => x.WaitType).ToHashSet() ?? new HashSet<string>();

                // Build unique wait type list, sorted by total wait time descending
                var waitTypes = waitTypeNames
                    .Select(w => new WaitTypeSelectionItem
                    {
                        WaitType = w.WaitType,
                        IsSelected = previouslySelected.Contains(w.WaitType)
                    })
                    .ToList();

                // Ensure poison waits are always in the picker even if they have no collected data
                foreach (var poisonWait in TabHelpers.PoisonWaits)
                {
                    if (!waitTypes.Any(w => string.Equals(w.WaitType, poisonWait, StringComparison.OrdinalIgnoreCase)))
                    {
                        waitTypes.Add(new WaitTypeSelectionItem
                        {
                            WaitType = poisonWait,
                            IsSelected = previouslySelected.Contains(poisonWait)
                        });
                    }
                }

                // If nothing was previously selected, apply poison waits + usual suspects + top 10
                if (!waitTypes.Any(w => w.IsSelected))
                {
                    var topWaits = TabHelpers.GetDefaultWaitTypes(waitTypes.Select(w => w.WaitType).ToList());
                    foreach (var item in waitTypes.Where(w => topWaits.Contains(w.WaitType)))
                    {
                        item.IsSelected = true;
                    }
                }

                _waitTypeItems = waitTypes;
                // Sort so checked items appear at top
                RefreshWaitTypeListOrder();

                // Fetch data only for selected wait types
                await UpdateWaitStatsDetailChartAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading wait stats detail: {ex.Message}");
            }
        }

        private async Task UpdateWaitStatsDetailChartAsync()
        {
            if (_databaseService == null || _waitTypeItems == null) return;

            var selectedWaitTypes = _waitTypeItems
                .Where(x => x.IsSelected)
                .Select(x => x.WaitType)
                .ToArray();

            if (selectedWaitTypes.Length == 0)
            {
                _allWaitStatsDetailData = new List<WaitStatsDataPoint>();
            }
            else
            {
                var data = await _databaseService.GetWaitStatsDataForTypesAsync(
                    selectedWaitTypes, _waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);
                _allWaitStatsDetailData = data?.ToList() ?? new List<WaitStatsDataPoint>();
            }

            LoadWaitStatsDetailChart(_allWaitStatsDetailData, _waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);
        }

        private void LoadWaitStatsDetailChart(List<WaitStatsDataPoint>? data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(WaitStatsDetailChart, out var existingWaitStatsPanel) && existingWaitStatsPanel != null)
            {
                WaitStatsDetailChart.Plot.Axes.Remove(existingWaitStatsPanel);
                _legendPanels[WaitStatsDetailChart] = null;
            }
            bool useAvgPerWait = WaitStatsMetricCombo?.SelectedIndex == 1;

            WaitStatsDetailChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(WaitStatsDetailChart);
            _waitStatsHover?.Clear();
            if (_waitStatsHover != null) _waitStatsHover.Unit = useAvgPerWait ? "ms/wait" : "ms/sec";

            if (data == null || data.Count == 0 || _waitTypeItems == null)
            {
                WaitStatsDetailChart.Refresh();
                return;
            }

            // Get selected wait types
            var selectedWaitTypes = _waitTypeItems.Where(x => x.IsSelected).ToList();
            if (selectedWaitTypes.Count == 0)
            {
                WaitStatsDetailChart.Refresh();
                return;
            }
            var colors = TabHelpers.ChartColors;

            // Get all time points across all wait types for gap filling
            int colorIndex = 0;
            foreach (var waitType in selectedWaitTypes.Take(20)) // Limit to 20 wait types
            {
                // Get data for this wait type
                var waitTypeData = data
                    .Where(d => d.WaitType == waitType.WaitType)
                    .GroupBy(d => d.CollectionTime)
                    .Select(g => new {
                        CollectionTime = g.Key,
                        WaitTimeMsPerSecond = g.Sum(x => x.WaitTimeMsPerSecond),
                        AvgMsPerWait = g.Average(x => x.AvgMsPerWait)
                    })
                    .OrderBy(d => d.CollectionTime)
                    .ToList();

                if (waitTypeData.Count >= 1)
                {
                    var timePoints = waitTypeData.Select(d => d.CollectionTime);
                    var values = useAvgPerWait
                        ? waitTypeData.Select(d => (double)d.AvgMsPerWait)
                        : waitTypeData.Select(d => (double)d.WaitTimeMsPerSecond);
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                    var scatter = WaitStatsDetailChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];

                    // Truncate legend text if too long
                    string legendText = waitType.WaitType;
                    if (legendText.Length > 25)
                        legendText = legendText.Substring(0, 22) + "...";
                    scatter.LegendText = legendText;
                    _waitStatsHover?.Add(scatter, waitType.WaitType);

                    colorIndex++;
                }
            }

            if (colorIndex > 0)
            {
                _legendPanels[WaitStatsDetailChart] = WaitStatsDetailChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                WaitStatsDetailChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = WaitStatsDetailChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            WaitStatsDetailChart.Plot.Axes.DateTimeTicksBottom();
            WaitStatsDetailChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(WaitStatsDetailChart);
            WaitStatsDetailChart.Plot.YLabel(useAvgPerWait ? "Avg Wait Time (ms/wait)" : "Wait Time (ms/sec)");
            TabHelpers.LockChartVerticalAxis(WaitStatsDetailChart);
            WaitStatsDetailChart.Refresh();
        }

        #endregion
    }

    /// <summary>
    /// Model for perfmon counter selection in the UI.
    /// </summary>
    public class PerfmonCounterSelectionItem : System.ComponentModel.INotifyPropertyChanged
    {
        private bool _isSelected;
        public string ObjectName { get; set; } = string.Empty;
        public string CounterName { get; set; } = string.Empty;
        public string DisplayName => $"{CounterName}";
        public string FullName => $"{ObjectName} - {CounterName}";

        public bool IsSelected
        {
            get => _isSelected;
            set
            {
                if (_isSelected != value)
                {
                    _isSelected = value;
                    PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(nameof(IsSelected)));
                }
            }
        }

        public event System.ComponentModel.PropertyChangedEventHandler? PropertyChanged;
    }

    /// <summary>
    /// Model for wait type selection in the UI.
    /// </summary>
    public class WaitTypeSelectionItem : System.ComponentModel.INotifyPropertyChanged
    {
        private bool _isSelected;
        public string WaitType { get; set; } = string.Empty;
        public string DisplayName => WaitType;

        public bool IsSelected
        {
            get => _isSelected;
            set
            {
                if (_isSelected != value)
                {
                    _isSelected = value;
                    PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(nameof(IsSelected)));
                }
            }
        }

        public event System.ComponentModel.PropertyChangedEventHandler? PropertyChanged;
    }
}

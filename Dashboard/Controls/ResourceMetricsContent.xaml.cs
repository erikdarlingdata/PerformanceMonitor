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
using PerformanceMonitor.Ui;

namespace PerformanceMonitorDashboard.Controls
{
    /// <summary>
    /// UserControl for the Resource Metrics tab content.
    /// Displays Latch Stats, Spinlock Stats, TempDB Stats, CPU Spikes, Session Stats,
    /// File I/O Latency, Server Trends, and Perfmon Counters.
    /// </summary>
    public partial class ResourceMetricsContent : UserControl
    {
        /// <summary>Raised when user drills down on a chart point. Args: (chartType, serverLocalTime)</summary>
        public event Action<string, DateTime>? ChartDrillDownRequested;

        private void AddDrillDown(ScottPlot.WPF.WpfPlot chart, ContextMenu menu,
            Func<ChartHoverHelper?> hoverGetter, string label, string chartType)
        {
            menu.Items.Insert(0, new Separator());
            var item = new MenuItem { Header = label };
            menu.Items.Insert(0, item);

            menu.Opened += (s, _) =>
            {
                var pos = System.Windows.Input.Mouse.GetPosition(chart);
                var nearest = hoverGetter()?.GetNearestSeries(pos);
                item.Tag = nearest?.Time;
                item.IsEnabled = nearest.HasValue;
            };

            item.Click += (s, _) =>
            {
                if (item.Tag is DateTime time)
                    ChartDrillDownRequested?.Invoke(chartType, time);
            };
        }

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
        private ChartHoverHelper? _sessionStatsHover;
        private ChartHoverHelper? _latchStatsHover;
        private ChartHoverHelper? _spinlockStatsHover;
        private ChartHoverHelper? _fileIoReadHover;
        private ChartHoverHelper? _fileIoWriteHover;
        private ChartHoverHelper? _fileIoReadThroughputHover;
        private ChartHoverHelper? _fileIoWriteThroughputHover;
        private ChartHoverHelper? _perfmonHover;
        private ChartHoverHelper? _waitStatsHover;
        private ChartHoverHelper? _tempdbStatsHover;
        private ChartHoverHelper? _tempdbSizeHover;
        private ChartHoverHelper? _tempDbLatencyHover;
        // Filter state dictionaries for each DataGrid
        // Legend panel references for edge-based legends (ScottPlot issue #4717 workaround)
        // Must store and remove these by reference before creating new ones
        private Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();


        public ResourceMetricsContent()
        {
            InitializeComponent();
            SetupChartContextMenus();
            Loaded += OnLoaded;
            ThemeManager.ThemeChanged += OnThemeChanged;
            /* WPF fires Unloaded on every TabControl tab switch, not just on destruction.
               Tearing down chart hover helpers here unsubscribes their MouseMove handlers
               and they are never re-registered when the user returns — this is the
               root cause of #916. Final disposal happens via ServerTab.CleanupOnClose. */

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

            _sessionStatsHover = new ChartHoverHelper(SessionStatsChart, "sessions");
            _latchStatsHover = new ChartHoverHelper(LatchStatsChart, "ms/sec");
            _spinlockStatsHover = new ChartHoverHelper(SpinlockStatsChart, "collisions/sec");
            _fileIoReadHover = new ChartHoverHelper(UserDbReadLatencyChart, "ms");
            _fileIoWriteHover = new ChartHoverHelper(UserDbWriteLatencyChart, "ms");
            _fileIoReadThroughputHover = new ChartHoverHelper(FileIoReadThroughputChart, "MB/s");
            _fileIoWriteThroughputHover = new ChartHoverHelper(FileIoWriteThroughputChart, "MB/s");
            _perfmonHover = new ChartHoverHelper(PerfmonCountersChart, "");
            _waitStatsHover = new ChartHoverHelper(WaitStatsDetailChart, "ms/sec");
            _tempdbStatsHover = new ChartHoverHelper(TempdbStatsChart, "MB");
            _tempdbSizeHover = new ChartHoverHelper(TempdbSizeChart, "MB");
            _tempDbLatencyHover = new ChartHoverHelper(TempDbLatencyChart, "ms");
        }

        public void DisposeChartHelpers()
        {
            _sessionStatsHover?.Dispose();
            _latchStatsHover?.Dispose();
            _spinlockStatsHover?.Dispose();
            _fileIoReadHover?.Dispose();
            _fileIoWriteHover?.Dispose();
            _fileIoReadThroughputHover?.Dispose();
            _fileIoWriteThroughputHover?.Dispose();
            _perfmonHover?.Dispose();
            _waitStatsHover?.Dispose();
            _tempdbStatsHover?.Dispose();
            _tempdbSizeHover?.Dispose();
            _tempDbLatencyHover?.Dispose();
            ThemeManager.ThemeChanged -= OnThemeChanged;
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
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
            CorrelatedLanes.ReapplyTheme();
        }

        private void SetupChartContextMenus()
        {
            // Latch Stats chart
            var latchMenu = TabHelpers.SetupChartContextMenu(LatchStatsChart, "Latch_Stats", "collect.latch_stats");
            AddDrillDown(LatchStatsChart, latchMenu, () => _latchStatsHover, "Show Active Queries at This Time", "Latch");

            // Spinlock Stats chart
            var spinlockMenu = TabHelpers.SetupChartContextMenu(SpinlockStatsChart, "Spinlock_Stats", "collect.spinlock_stats");
            AddDrillDown(SpinlockStatsChart, spinlockMenu, () => _spinlockStatsHover, "Show Active Queries at This Time", "Spinlock");

            // TempDB Stats chart
            var tempdbStatsMenu = TabHelpers.SetupChartContextMenu(TempdbStatsChart, "TempDB_Stats", "collect.tempdb_stats");
            AddDrillDown(TempdbStatsChart, tempdbStatsMenu, () => _tempdbStatsHover, "Show Active Queries at This Time", "TempdbStats");

            // TempDB Allocated Size chart
            var tempdbSizeMenu = TabHelpers.SetupChartContextMenu(TempdbSizeChart, "TempDB_Allocated_Size", "collect.tempdb_stats");
            AddDrillDown(TempdbSizeChart, tempdbSizeMenu, () => _tempdbSizeHover, "Show Active Queries at This Time", "TempdbSize");

            // Session Stats chart
            var sessionMenu = TabHelpers.SetupChartContextMenu(SessionStatsChart, "Session_Stats", "collect.session_stats");
            AddDrillDown(SessionStatsChart, sessionMenu, () => _sessionStatsHover, "Show Active Queries at This Time", "SessionStats");

            // File I/O Latency charts
            var userReadLatencyMenu = TabHelpers.SetupChartContextMenu(UserDbReadLatencyChart, "UserDB_Read_Latency", "collect.file_io_stats");
            AddDrillDown(UserDbReadLatencyChart, userReadLatencyMenu, () => _fileIoReadHover, "Show Active Queries at This Time", "FileIoReadLatency");
            var userWriteLatencyMenu = TabHelpers.SetupChartContextMenu(UserDbWriteLatencyChart, "UserDB_Write_Latency", "collect.file_io_stats");
            AddDrillDown(UserDbWriteLatencyChart, userWriteLatencyMenu, () => _fileIoWriteHover, "Show Active Queries at This Time", "FileIoWriteLatency");

            // File I/O Throughput charts
            var readThroughputMenu = TabHelpers.SetupChartContextMenu(FileIoReadThroughputChart, "UserDB_Read_Throughput", "collect.file_io_stats");
            AddDrillDown(FileIoReadThroughputChart, readThroughputMenu, () => _fileIoReadThroughputHover, "Show Active Queries at This Time", "FileIoReadThroughput");
            var writeThroughputMenu = TabHelpers.SetupChartContextMenu(FileIoWriteThroughputChart, "UserDB_Write_Throughput", "collect.file_io_stats");
            AddDrillDown(FileIoWriteThroughputChart, writeThroughputMenu, () => _fileIoWriteThroughputHover, "Show Active Queries at This Time", "FileIoWriteThroughput");
            var tempDbLatencyMenu = TabHelpers.SetupChartContextMenu(TempDbLatencyChart, "TempDB_Latency", "collect.file_io_stats");
            AddDrillDown(TempDbLatencyChart, tempDbLatencyMenu, () => _tempDbLatencyHover, "Show Active Queries at This Time", "TempDbLatency");

            // Perfmon Counters chart
            var perfmonMenu = TabHelpers.SetupChartContextMenu(PerfmonCountersChart, "Perfmon_Counters", "collect.perfmon_stats");
            AddDrillDown(PerfmonCountersChart, perfmonMenu, () => _perfmonHover, "Show Active Queries at This Time", "Perfmon");

            // Wait Stats Detail chart
            var waitStatsMenu = TabHelpers.SetupChartContextMenu(WaitStatsDetailChart, "Wait_Stats_Detail", "collect.wait_stats");
            AddWaitDrillDownMenuItem(WaitStatsDetailChart, waitStatsMenu);
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        public void Initialize(DatabaseService databaseService,
            Analysis.SqlServerBaselineProvider? baselineProvider = null)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
            CorrelatedLanes.Initialize(databaseService, baselineProvider);
            // Forward the Overview lanes' right-click drill-down to the host (-> Active Queries).
            CorrelatedLanes.ShowActiveQueriesRequested += t => ChartDrillDownRequested?.Invoke("CorrelatedLanes", t);
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
        /// Refreshes resource metrics data. When fullRefresh is false, only the visible sub-tab is refreshed.
        /// </summary>
        public async Task RefreshAllDataAsync(bool fullRefresh = true)
        {
            using var _ = Helpers.MethodProfiler.StartTiming("ResourceMetrics");
            if (_databaseService == null) return;

            try
            {
                if (fullRefresh)
                {
                    // Run all independent refreshes in parallel for initial load / manual refresh
                    await Task.WhenAll(
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.LatchStats", () => RefreshLatchStatsAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.SpinlockStats", () => RefreshSpinlockStatsAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.TempdbStats", () => RefreshTempdbStatsAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.SessionStats", () => RefreshSessionStatsAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.FileIoLatency", () => LoadFileIoLatencyChartsAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.FileIoThroughput", () => LoadFileIoThroughputChartsAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.ServerTrends", () => RefreshServerTrendsAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.PerfmonCounters", () => RefreshPerfmonCountersTabAsync()),
                        Helpers.MethodProfiler.TimeAsync("ResourceMetrics.WaitStatsDetail", () => RefreshWaitStatsDetailTabAsync())
                    );
                }
                else
                {
                    // Only refresh the visible sub-tab
                    switch (SubTabControl.SelectedIndex)
                    {
                        case 0: await RefreshServerTrendsAsync(); break;
                        case 1: await RefreshWaitStatsDetailTabAsync(); break;
                        case 2: await RefreshTempdbStatsAsync(); break;
                        case 3: await Task.WhenAll(LoadFileIoLatencyChartsAsync(), LoadFileIoThroughputChartsAsync()); break;
                        case 4: await RefreshPerfmonCountersTabAsync(); break;
                        case 5: await RefreshSessionStatsAsync(); break;
                        case 6: await RefreshLatchStatsAsync(); break;
                        case 7: await RefreshSpinlockStatsAsync(); break;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing resource metrics data: {ex.Message}", ex);
            }
        }

        #region Server Trends Tab

        private (DateTime From, DateTime To)? ComparisonRange { get; set; }

        /// <summary>
        /// Sets the comparison range from the global Compare dropdown and refreshes Server Trends.
        /// </summary>
        public async Task SetComparisonRangeAsync((DateTime From, DateTime To)? range)
        {
            ComparisonRange = range;
            await RefreshServerTrendsAsync();
        }

        private async Task RefreshServerTrendsAsync()
        {
            if (_databaseService == null) return;
            try
            {
                await CorrelatedLanes.RefreshAsync(_serverTrendsHoursBack, _serverTrendsFromDate, _serverTrendsToDate, ComparisonRange);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading server trends: {ex.Message}", ex);
            }
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

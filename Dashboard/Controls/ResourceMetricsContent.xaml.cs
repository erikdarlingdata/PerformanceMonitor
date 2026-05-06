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
        /// <summary>Raised when user drills down on a chart point. Args: (chartType, serverLocalTime)</summary>
        public event Action<string, DateTime>? ChartDrillDownRequested;

        private void AddDrillDown(ScottPlot.WPF.WpfPlot chart, ContextMenu menu,
            Func<Helpers.ChartHoverHelper?> hoverGetter, string label, string chartType)
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
            _tempDbLatencyHover?.Dispose();
            Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
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

            // Perfmon Counters chart
            TabHelpers.SetupChartContextMenu(PerfmonCountersChart, "Perfmon_Counters", "collect.perfmon_stats");

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

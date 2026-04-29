/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Input;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using ScottPlot.WPF;

namespace PerformanceMonitorDashboard.Controls
{
    /// <summary>
    /// UserControl for the Query Performance tab content.
    /// Contains Active Queries, Query Stats, Procedure Stats,
    /// Query Store, Query Store Regressions, Query Trace Patterns, and Performance Trends.
    /// </summary>
    public partial class QueryPerformanceContent : UserControl
    {
        private DatabaseService? _databaseService;
        private Action<string>? _statusCallback;
        internal bool IsRefreshing { get; set; }

        private static (DateTime start, DateTime end) GetSlicerTimeRange(
            int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            if (fromDate.HasValue && toDate.HasValue)
                return (fromDate.Value, toDate.Value);
            var serverNow = Helpers.ServerTimeHelper.ServerNow;
            return (serverNow.AddHours(-hoursBack), serverNow);
        }

        /// <summary>Raised when user wants to view a plan in the Plan Viewer tab. Args: (planXml, label, queryText)</summary>
        public event Action<string, string, string?>? ViewPlanRequested;

        /// <summary>Raised when actual plan execution starts. Arg: label for the plan tab.</summary>
        public event Action<string>? ActualPlanStarted;

        /// <summary>Raised when actual plan execution finishes (success or failure).</summary>
        public event Action? ActualPlanFinished;

        /// <summary>Raised when a drill-down needs the parent to set custom time pickers. Args: (fromUtc, toUtc)</summary>
        public event Action<DateTime, DateTime>? DrillDownTimeRangeRequested;

        /// <summary>Fired when the Queries sub-tab changes, so the global Compare dropdown can update.</summary>
        public event Action? SubTabChanged;

        private CancellationTokenSource? _actualPlanCts;

        /// <summary>Cancels the in-flight actual plan execution, if any.</summary>
        public void CancelActualPlan() => _actualPlanCts?.Cancel();

        // Active Queries state
        private int _activeQueriesHoursBack = 1;
        private DateTime? _activeQueriesFromDate;
        private DateTime? _activeQueriesToDate;
        private bool _isDrillDownActive;

        // Query Stats state
        private int _queryStatsHoursBack = 24;
        private DateTime? _queryStatsFromDate;
        private DateTime? _queryStatsToDate;

        // Procedure Stats state
        private int _procStatsHoursBack = 24;
        private DateTime? _procStatsFromDate;
        private DateTime? _procStatsToDate;

        // Query Store state
        private int _queryStoreHoursBack = 24;
        private DateTime? _queryStoreFromDate;
        private DateTime? _queryStoreToDate;

        // Query Store Regressions state
        private int _qsRegressionsHoursBack = 24;
        private DateTime? _qsRegressionsFromDate;
        private DateTime? _qsRegressionsToDate;

        // Long Running Query Patterns state
        private int _lrqPatternsHoursBack = 24;
        private DateTime? _lrqPatternsFromDate;
        private DateTime? _lrqPatternsToDate;

        // Performance Trends state
        private int _perfTrendsHoursBack = 24;
        private DateTime? _perfTrendsFromDate;
        private DateTime? _perfTrendsToDate;

        // Legend panel references for edge-based legends (ScottPlot issue #4717 workaround)
        private Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();

        // Chart hover tooltips
        private Helpers.ChartHoverHelper? _queryDurationHover;
        private Helpers.ChartHoverHelper? _procDurationHover;
        private Helpers.ChartHoverHelper? _qsDurationHover;
        private Helpers.ChartHoverHelper? _execTrendsHover;

        // Query heatmap
        private HeatmapResult? _lastHeatmapResult;
        private ScottPlot.Plottables.Heatmap? _heatmapPlottable;
        private int _heatmapHoursBack = 24;
        private DateTime? _heatmapFromDate;
        private DateTime? _heatmapToDate;
        private Popup? _heatmapPopup;
        private System.Windows.Controls.TextBlock? _heatmapPopupText;
        private DateTime _lastHeatmapHoverUpdate;

        public QueryPerformanceContent()
        {
            InitializeComponent();
            SetupChartSaveMenus();
            Loaded += OnLoaded;
            Unloaded += OnUnloaded;
            SubTabControl.SelectionChanged += (s, e) =>
            {
                if (e.Source == SubTabControl)
                {
                    _isDrillDownActive = false;
                    SubTabChanged?.Invoke();
                }
            };
            Helpers.ThemeManager.ThemeChanged += OnThemeChanged;

            _queryDurationHover = new Helpers.ChartHoverHelper(QueryPerfTrendsQueryChart, "ms/sec");
            _procDurationHover = new Helpers.ChartHoverHelper(QueryPerfTrendsProcChart, "ms/sec");
            _qsDurationHover = new Helpers.ChartHoverHelper(QueryPerfTrendsQsChart, "ms/sec");
            _execTrendsHover = new Helpers.ChartHoverHelper(QueryPerfTrendsExecChart, "/sec");

            // Heatmap popup tooltip
            _heatmapPopupText = new System.Windows.Controls.TextBlock
            {
                Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xE0, 0xE0, 0xE0)),
                FontSize = 13,
                MaxWidth = 450,
                TextTrimming = TextTrimming.CharacterEllipsis
            };
            _heatmapPopup = new Popup
            {
                PlacementTarget = QueryHeatmapChart,
                Placement = PlacementMode.Relative,
                IsHitTestVisible = false,
                AllowsTransparency = true,
                Child = new System.Windows.Controls.Border
                {
                    Background = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0x33, 0x33, 0x33)),
                    BorderBrush = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0x55, 0x55, 0x55)),
                    BorderThickness = new Thickness(1),
                    CornerRadius = new CornerRadius(3),
                    Padding = new Thickness(8, 4, 8, 4),
                    Child = _heatmapPopupText
                }
            };
            TabHelpers.ApplyThemeToChart(QueryHeatmapChart);

            // Heatmap right-click drill-down
            var heatmapMenu = TabHelpers.SetupChartContextMenu(QueryHeatmapChart, "Query_Heatmap");
            var heatmapDrillDown = new MenuItem { Header = "Show Active Queries at This Time" };
            heatmapMenu.Items.Insert(0, heatmapDrillDown);
            heatmapMenu.Items.Insert(1, new Separator());
            heatmapMenu.Opened += (s, _) =>
            {
                if (_lastHeatmapResult == null || _heatmapPlottable == null || _lastHeatmapResult.TimeBuckets.Length == 0)
                {
                    heatmapDrillDown.IsEnabled = false;
                    return;
                }
                var mpos = Mouse.GetPosition(QueryHeatmapChart);
                var mdpi = System.Windows.Media.VisualTreeHelper.GetDpi(QueryHeatmapChart);
                var mpixel = new ScottPlot.Pixel((float)(mpos.X * mdpi.DpiScaleX), (float)(mpos.Y * mdpi.DpiScaleY));
                var mcoords = QueryHeatmapChart.Plot.GetCoordinates(mpixel);
                var (mCol, _) = _heatmapPlottable.GetIndexes(mcoords);
                if (mCol >= 0 && mCol < _lastHeatmapResult.TimeBuckets.Length)
                {
                    heatmapDrillDown.Tag = _lastHeatmapResult.TimeBuckets[mCol];
                    heatmapDrillDown.IsEnabled = true;
                }
                else
                {
                    heatmapDrillDown.IsEnabled = false;
                }
            };
            heatmapDrillDown.Click += async (s, _) =>
            {
                if (heatmapDrillDown.Tag is DateTime bucketTime)
                {
                    // bucketTime is already server time (SQL Server stores collection_time in server local time)
                    var serverFrom = bucketTime.AddMinutes(-5);
                    var serverTo = bucketTime.AddMinutes(10);
                    DrillDownTimeRangeRequested?.Invoke(serverFrom, serverTo);

                    // Query also uses server time (same as collection_time in SQL Server)
                    var queryFrom = serverFrom;
                    var queryTo = serverTo;
                    SubTabControl.SelectedIndex = 1; // Active Queries
                    await RefreshActiveQueriesWithRangeAsync(queryFrom, queryTo);
                }
            };
        }

        private void OnUnloaded(object sender, RoutedEventArgs e)
        {
            /* Unsubscribe from filter popup events to prevent memory leaks */
            if (_filterPopupContent != null)
            {
                _filterPopupContent.FilterApplied -= ActiveQueriesFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= ActiveQueriesFilterPopup_FilterCleared;
                _filterPopupContent.FilterApplied -= QueryStatsFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= QueryStatsFilterPopup_FilterCleared;
                _filterPopupContent.FilterApplied -= ProcStatsFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= ProcStatsFilterPopup_FilterCleared;
                _filterPopupContent.FilterApplied -= QueryStoreFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= QueryStoreFilterPopup_FilterCleared;
                _filterPopupContent.FilterApplied -= CurrentActiveFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= CurrentActiveFilterPopup_FilterCleared;
                _filterPopupContent.FilterApplied -= QsRegressionsFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= QsRegressionsFilterPopup_FilterCleared;
                _filterPopupContent.FilterApplied -= LrqPatternsFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= LrqPatternsFilterPopup_FilterCleared;
            }

            /* Clear large data collections to free memory */
            _currentActiveUnfilteredData = null;
            _activeQueriesUnfilteredData = null;
            _queryStatsUnfilteredData = null;
            _procStatsUnfilteredData = null;
            _queryStoreUnfilteredData = null;
            _qsRegressionsUnfilteredData = null;
            _lrqPatternsUnfilteredData = null;

            DisposeChartHelpers();
            Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
        }

        public void DisposeChartHelpers()
        {
            _queryDurationHover?.Dispose();
            _procDurationHover?.Dispose();
            _qsDurationHover?.Dispose();
            _execTrendsHover?.Dispose();
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

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            // Initialize charts with dark mode immediately (before data is loaded)
            TabHelpers.ApplyThemeToChart(QueryPerfTrendsQueryChart);
            TabHelpers.ApplyThemeToChart(QueryPerfTrendsProcChart);
            TabHelpers.ApplyThemeToChart(QueryPerfTrendsQsChart);
            TabHelpers.ApplyThemeToChart(QueryPerfTrendsExecChart);
            QueryPerfTrendsQueryChart.Refresh();
            QueryPerfTrendsProcChart.Refresh();
            QueryPerfTrendsQsChart.Refresh();
            QueryPerfTrendsExecChart.Refresh();

            // Apply minimum column widths based on header text to all DataGrids
            TabHelpers.AutoSizeColumnMinWidths(ActiveQueriesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(CurrentActiveQueriesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(QueryStatsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(ProcStatsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(QueryStoreDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(QueryStoreRegressionsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(LongRunningQueryPatternsDataGrid);

            // Freeze first columns for easier horizontal scrolling
            TabHelpers.FreezeColumns(ActiveQueriesDataGrid, 2);
            TabHelpers.FreezeColumns(CurrentActiveQueriesDataGrid, 2);
            TabHelpers.FreezeColumns(QueryStatsDataGrid, 2);
            TabHelpers.FreezeColumns(ProcStatsDataGrid, 2);
            TabHelpers.FreezeColumns(QueryStoreDataGrid, 2);
            TabHelpers.FreezeColumns(QueryStoreRegressionsDataGrid, 2);
            TabHelpers.FreezeColumns(LongRunningQueryPatternsDataGrid, 2);
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        public void Initialize(DatabaseService databaseService, Action<string>? statusCallback = null)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
            _statusCallback = statusCallback;
            ActiveQueriesSlicer.RangeChanged += OnActiveQueriesSlicerChanged;
            QueryStatsSlicer.RangeChanged += OnQueryStatsSlicerChanged;
            ProcStatsSlicer.RangeChanged += OnProcStatsSlicerChanged;
            QueryStoreSlicer.RangeChanged += OnQueryStoreSlicerChanged;
        }

        private void QueryStatsDataGrid_Sorting(object sender, DataGridSortingEventArgs e)
        {
            if (_queryStatsSlicerData == null || _queryStatsSlicerData.Count == 0) return;

            var col = e.Column.SortMemberPath ?? "";
            if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc && bc.Binding is System.Windows.Data.Binding b)
                col = b.Path.Path;

            var (metric, label) = col switch
            {
                "TotalWorkerTimeMs" => ("TotalCpu", "Total CPU (ms)"),
                "AvgWorkerTimeMs" => ("AvgCpu", "Avg CPU (ms)"),
                "TotalElapsedTimeMs" => ("TotalElapsed", "Total Duration (ms)"),
                "AvgElapsedTimeMs" => ("AvgElapsed", "Avg Duration (ms)"),
                "TotalLogicalReads" or "AvgLogicalReads" => ("TotalReads", "Total Reads"),
                "TotalLogicalWrites" => ("TotalWrites", "Total Writes"),
                "TotalPhysicalReads" => ("TotalReads", "Total Physical Reads"),
                "IntervalExecutions" => ("Sessions", "Executions"),
                _ => ("TotalCpu", "Total CPU (ms)"),
            };

            if (metric == _queryStatsSlicerMetric) return;
            _queryStatsSlicerMetric = metric;

            foreach (var bucket in _queryStatsSlicerData)
            {
                var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
                bucket.Value = metric switch
                {
                    "TotalCpu" => bucket.TotalCpu,
                    "AvgCpu" => bucket.TotalCpu / n,
                    "TotalElapsed" => bucket.TotalElapsed,
                    "AvgElapsed" => bucket.TotalElapsed / n,
                    "TotalReads" => bucket.TotalReads,
                    "TotalWrites" => bucket.TotalWrites,
                    "Sessions" => bucket.SessionCount,
                    _ => bucket.TotalCpu,
                };
            }

            QueryStatsSlicer.UpdateMetric(label);

            if (QueryStatsDataGrid.SelectedItem != null)
                QueryStatsDataGrid_SelectionChanged(QueryStatsDataGrid, null!);
        }

        private void ProcStatsDataGrid_Sorting(object sender, DataGridSortingEventArgs e)
        {
            if (_procStatsSlicerData == null || _procStatsSlicerData.Count == 0) return;

            var col = e.Column.SortMemberPath ?? "";
            if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc2 && bc2.Binding is System.Windows.Data.Binding b2)
                col = b2.Path.Path;

            var (metric, label) = col switch
            {
                "TotalWorkerTimeMs" => ("TotalCpu", "Total CPU (ms)"),
                "AvgWorkerTimeMs" => ("AvgCpu", "Avg CPU (ms)"),
                "TotalElapsedTimeMs" => ("TotalElapsed", "Total Duration (ms)"),
                "AvgElapsedTimeMs" => ("AvgElapsed", "Avg Duration (ms)"),
                "TotalLogicalReads" => ("TotalReads", "Total Reads"),
                "TotalLogicalWrites" => ("TotalWrites", "Total Writes"),
                "TotalPhysicalReads" => ("TotalReads", "Total Physical Reads"),
                "IntervalExecutions" => ("Sessions", "Executions"),
                _ => ("TotalCpu", "Total CPU (ms)"),
            };

            if (metric == _procStatsSlicerMetric) return;
            _procStatsSlicerMetric = metric;

            foreach (var bucket in _procStatsSlicerData)
            {
                var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
                bucket.Value = metric switch
                {
                    "TotalCpu" => bucket.TotalCpu,
                    "AvgCpu" => bucket.TotalCpu / n,
                    "TotalElapsed" => bucket.TotalElapsed,
                    "AvgElapsed" => bucket.TotalElapsed / n,
                    "TotalReads" => bucket.TotalReads,
                    "TotalWrites" => bucket.TotalWrites,
                    "Sessions" => bucket.SessionCount,
                    _ => bucket.TotalCpu,
                };
            }

            ProcStatsSlicer.UpdateMetric(label);

            if (ProcStatsDataGrid.SelectedItem != null)
                ProcStatsDataGrid_SelectionChanged(ProcStatsDataGrid, null!);
        }

        private void QueryStoreDataGrid_Sorting(object sender, DataGridSortingEventArgs e)
        {
            if (_queryStoreSlicerData == null || _queryStoreSlicerData.Count == 0) return;

            var col = e.Column.SortMemberPath ?? "";
            if (string.IsNullOrEmpty(col) && e.Column is DataGridBoundColumn bc3 && bc3.Binding is System.Windows.Data.Binding b3)
                col = b3.Path.Path;

            var (metric, label) = col switch
            {
                "AvgCpuTimeMs" => ("AvgCpu", "Avg CPU (ms)"),
                "AvgDurationMs" => ("AvgElapsed", "Avg Duration (ms)"),
                "AvgLogicalReads" => ("TotalReads", "Avg Reads"),
                "AvgLogicalWrites" => ("TotalWrites", "Avg Writes"),
                "AvgPhysicalReads" => ("TotalReads", "Avg Physical Reads"),
                "ExecutionCount" => ("Sessions", "Executions"),
                _ => ("TotalCpu", "Total CPU (ms)"),
            };

            if (metric == _queryStoreSlicerMetric) return;
            _queryStoreSlicerMetric = metric;

            foreach (var bucket in _queryStoreSlicerData)
            {
                var n = bucket.SessionCount > 0 ? bucket.SessionCount : 1;
                bucket.Value = metric switch
                {
                    "TotalCpu" => bucket.TotalCpu,
                    "AvgCpu" => bucket.TotalCpu / n,
                    "TotalElapsed" => bucket.TotalElapsed,
                    "AvgElapsed" => bucket.TotalElapsed / n,
                    "TotalReads" => bucket.TotalReads,
                    "TotalWrites" => bucket.TotalWrites,
                    "Sessions" => bucket.SessionCount,
                    _ => bucket.TotalCpu,
                };
            }

            QueryStoreSlicer.UpdateMetric(label);

            if (QueryStoreDataGrid.SelectedItem != null)
                QueryStoreDataGrid_SelectionChanged(QueryStoreDataGrid, null!);
        }

        // ── Grid → Slicer Overlay (#683) ──

        private async void QueryStatsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_databaseService == null) return;
            if (QueryStatsDataGrid.SelectedItem is not Models.QueryStatsItem row || string.IsNullOrEmpty(row.QueryHash))
            {
                if (!IsRefreshing) QueryStatsSlicer.ClearOverlay();
                return;
            }

            try
            {
                var history = await _databaseService.GetQueryStatsHistoryAsync(
                    row.DatabaseName, row.QueryHash, _queryStatsHoursBack, _queryStatsFromDate, _queryStatsToDate);

                Func<Models.QueryStatsHistoryItem, long?> selector = _queryStatsSlicerMetric switch
                {
                    "TotalCpu" or "AvgCpu" => h => h.TotalWorkerTimeDelta,
                    "TotalReads" or "AvgReads" => h => h.TotalLogicalReadsDelta,
                    "TotalWrites" => h => h.TotalLogicalWritesDelta,
                    "TotalPhysReads" => h => h.TotalPhysicalReadsDelta,
                    _ => h => h.TotalElapsedTimeDelta,
                };
                bool isMicroseconds = _queryStatsSlicerMetric is "TotalCpu" or "AvgCpu" or "TotalElapsed" or "AvgElapsed";

                var points = history
                    .Where(h => (selector(h) ?? 0) > 0)
                    .Select(h => (h.CollectionTime, isMicroseconds ? (selector(h) ?? 0) / 1000.0 : (double)(selector(h) ?? 0)))
                    .ToList();

                QueryStatsSlicer.SetOverlay(points, row.QueryHash);
            }
            catch { QueryStatsSlicer.ClearOverlay(); }
        }

        private async void ProcStatsDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_databaseService == null) return;
            if (ProcStatsDataGrid.SelectedItem is not Models.ProcedureStatsItem row || string.IsNullOrEmpty(row.ProcedureName))
            {
                if (!IsRefreshing) ProcStatsSlicer.ClearOverlay();
                return;
            }

            try
            {
                var history = await _databaseService.GetProcedureStatsHistoryAsync(
                    row.DatabaseName, row.SchemaName ?? "dbo", row.ProcedureName, _procStatsHoursBack, _procStatsFromDate, _procStatsToDate);

                Func<Models.ProcedureExecutionHistoryItem, long?> selector = _procStatsSlicerMetric switch
                {
                    "TotalCpu" or "AvgCpu" => h => h.TotalWorkerTimeDelta,
                    "TotalReads" or "AvgReads" => h => h.TotalLogicalReadsDelta,
                    "TotalWrites" => h => h.TotalLogicalWritesDelta,
                    "TotalPhysReads" => h => h.TotalPhysicalReadsDelta,
                    _ => h => h.TotalElapsedTimeDelta,
                };
                bool isMicroseconds = _procStatsSlicerMetric is "TotalCpu" or "AvgCpu" or "TotalElapsed" or "AvgElapsed";

                var points = history
                    .Where(h => (selector(h) ?? 0) > 0)
                    .Select(h => (h.CollectionTime, isMicroseconds ? (selector(h) ?? 0) / 1000.0 : (double)(selector(h) ?? 0)))
                    .ToList();

                var label = (row.ProcedureName?.Length ?? 0) > 30 ? row.ProcedureName![..30] + "..." : row.ProcedureName ?? "";
                ProcStatsSlicer.SetOverlay(points, label);
            }
            catch { ProcStatsSlicer.ClearOverlay(); }
        }

        private async void QueryStoreDataGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_databaseService == null) return;
            if (QueryStoreDataGrid.SelectedItem is not Models.QueryStoreItem row)
            {
                if (!IsRefreshing) QueryStoreSlicer.ClearOverlay();
                return;
            }

            try
            {
                var history = await _databaseService.GetQueryStoreHistoryAsync(
                    row.DatabaseName, row.QueryId, _queryStoreHoursBack, _queryStoreFromDate, _queryStoreToDate);

                Func<Models.QueryExecutionHistoryItem, double> selector = _queryStoreSlicerMetric switch
                {
                    "TotalCpu" or "AvgCpu" => h => h.AvgCpuTimeMs * h.CountExecutions,
                    "TotalReads" or "AvgReads" => h => h.AvgLogicalReads * (double)h.CountExecutions,
                    _ => h => h.AvgDurationMs * h.CountExecutions,
                };

                var points = history
                    .Where(h => selector(h) > 0)
                    .Select(h => (h.CollectionTime, selector(h)))
                    .ToList();

                var qsLabel = !string.IsNullOrWhiteSpace(row.ModuleName)
                    ? row.ModuleName
                    : $"Query {row.QueryId}";
                QueryStoreSlicer.SetOverlay(points, qsLabel);
            }
            catch { QueryStoreSlicer.ClearOverlay(); }
        }

        public void RefreshGridBindings()
        {
            QueryStatsDataGrid.Items.Refresh();
            ProcStatsDataGrid.Items.Refresh();
            QueryStoreDataGrid.Items.Refresh();
            QueryStoreRegressionsDataGrid.Items.Refresh();
            ActiveQueriesDataGrid.Items.Refresh();
            CurrentActiveQueriesDataGrid.Items.Refresh();
            LongRunningQueryPatternsDataGrid.Items.Refresh();
            ActiveQueriesSlicer.Redraw();
            QueryStatsSlicer.Redraw();
            ProcStatsSlicer.Redraw();
            QueryStoreSlicer.Redraw();
        }

        /// <summary>
        /// Sets the time range for all sub-tabs.
        /// </summary>
        public void SelectSubTab(int index) => SubTabControl.SelectedIndex = index;

        public void SetTimeRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            _isDrillDownActive = false;
            _activeQueriesHoursBack = hoursBack;
            _activeQueriesFromDate = fromDate;
            _activeQueriesToDate = toDate;

            _queryStatsHoursBack = hoursBack;
            _queryStatsFromDate = fromDate;
            _queryStatsToDate = toDate;

            _procStatsHoursBack = hoursBack;
            _procStatsFromDate = fromDate;
            _procStatsToDate = toDate;

            _queryStoreHoursBack = hoursBack;
            _queryStoreFromDate = fromDate;
            _queryStoreToDate = toDate;

            _qsRegressionsHoursBack = hoursBack;
            _qsRegressionsFromDate = fromDate;
            _qsRegressionsToDate = toDate;

            _lrqPatternsHoursBack = hoursBack;
            _lrqPatternsFromDate = fromDate;
            _lrqPatternsToDate = toDate;

            _perfTrendsHoursBack = hoursBack;
            _perfTrendsFromDate = fromDate;
            _perfTrendsToDate = toDate;

            _heatmapHoursBack = hoursBack;
            _heatmapFromDate = fromDate;
            _heatmapToDate = toDate;
        }

        /// <summary>
        /// Refreshes query performance data. When fullRefresh is false, only the visible sub-tab is refreshed.
        /// </summary>
        public async Task RefreshAllDataAsync(bool fullRefresh = true)
        {
            try
            {
                using var _ = Helpers.MethodProfiler.StartTiming("QueryPerformance");

                if (_databaseService == null) return;

                if (!fullRefresh)
                {
                    // Only refresh the visible sub-tab
                    switch (SubTabControl.SelectedIndex)
                    {
                        case 0: await RefreshPerformanceTrendsAsync(); break;
                        case 1: await RefreshActiveQueriesAsync(); break;
                        case 2: break; // Current Active Queries — manual refresh only
                        case 3: await RefreshQueryStatsGridAsync(); break;
                        case 4: await RefreshProcStatsGridAsync(); break;
                        case 5: await RefreshQueryStoreGridAsync(); break;
                        case 6: await RefreshQueryStoreRegressionsAsync(); break;
                        case 7: await RefreshLongRunningPatternsAsync(); break;
                        case 8: await RefreshQueryHeatmapAsync(); break;
                    }
                    return;
                }

                // Full refresh — all sub-tabs in parallel

                // Only show loading overlay on initial load (no existing data)
                if (QueryStatsDataGrid.ItemsSource == null)
                {
                    QueryStatsLoading.IsLoading = true;
                    QueryStatsNoDataMessage.Visibility = Visibility.Collapsed;
                }

                // Fetch grid data (summary views aggregated per query/procedure)
                var queryStatsTask = _databaseService.GetQueryStatsAsync(_queryStatsHoursBack, _queryStatsFromDate, _queryStatsToDate);
                var procStatsTask = _databaseService.GetProcedureStatsAsync(_procStatsHoursBack, _procStatsFromDate, _procStatsToDate);
                var queryStoreTask = _databaseService.GetQueryStoreDataAsync(_queryStoreHoursBack, _queryStoreFromDate, _queryStoreToDate);

                // Fetch chart data (time-series aggregated per collection_time)
                var queryDurationTrendsTask = _databaseService.GetQueryDurationTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
                var procDurationTrendsTask = _databaseService.GetProcedureDurationTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
                var qsDurationTrendsTask = _databaseService.GetQueryStoreDurationTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
                var execTrendsTask = _databaseService.GetExecutionTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);

                // Fetch grid-only data in parallel
                var activeTask = RefreshActiveQueriesAsync();
                var regressionsTask = RefreshQueryStoreRegressionsAsync();
                var patternsTask = RefreshLongRunningPatternsAsync();

                // Wait for all fetches to complete
                await Task.WhenAll(
                    queryStatsTask, procStatsTask, queryStoreTask,
                    queryDurationTrendsTask, procDurationTrendsTask, qsDurationTrendsTask, execTrendsTask,
                    activeTask, regressionsTask, patternsTask
                );

                // Populate grids from summary data
                // If slicer is narrowed, re-query with slicer dates instead of global range
                if (QueryStatsSlicer.HasNarrowedSelection)
                {
                    var slicerData = await _databaseService.GetQueryStatsAsync(0, QueryStatsSlicer.SelectionStart, QueryStatsSlicer.SelectionEnd, fromSlicer: true);
                    PopulateQueryStatsGrid(slicerData);
                }
                else
                {
                    PopulateQueryStatsGrid(await queryStatsTask);
                }
                LoadQueryStatsSlicerAsync().ConfigureAwait(false);
                if (ProcStatsSlicer.HasNarrowedSelection)
                {
                    var slicerProcData = await _databaseService.GetProcedureStatsAsync(0, ProcStatsSlicer.SelectionStart, ProcStatsSlicer.SelectionEnd, fromSlicer: true);
                    PopulateProcStatsGrid(slicerProcData);
                }
                else
                {
                    PopulateProcStatsGrid(await procStatsTask);
                }
                LoadProcStatsSlicerAsync().ConfigureAwait(false);
                if (QueryStoreSlicer.HasNarrowedSelection)
                {
                    var slicerQsData = await _databaseService.GetQueryStoreDataAsync(0, QueryStoreSlicer.SelectionStart, QueryStoreSlicer.SelectionEnd, fromSlicer: true);
                    PopulateQueryStoreGrid(slicerQsData);
                }
                else
                {
                    PopulateQueryStoreGrid(await queryStoreTask);
                }
                LoadQueryStoreSlicerAsync().ConfigureAwait(false);

                // Populate charts from time-series data
                LoadDurationChart(QueryPerfTrendsQueryChart, await queryDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[0], _queryDurationHover);
                LoadDurationChart(QueryPerfTrendsProcChart, await procDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[1], _procDurationHover);
                LoadDurationChart(QueryPerfTrendsQsChart, await qsDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[4], _qsDurationHover);
                LoadExecChart(await execTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);

                // Heatmap
                await RefreshQueryHeatmapAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing QueryPerformance data: {ex.Message}", ex);
            }
            finally
            {
                QueryStatsLoading.IsLoading = false;
            }
        }

        private async Task RefreshPerformanceTrendsAsync()
        {
            if (_databaseService == null) return;

            var queryDurationTrendsTask = _databaseService.GetQueryDurationTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
            var procDurationTrendsTask = _databaseService.GetProcedureDurationTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
            var qsDurationTrendsTask = _databaseService.GetQueryStoreDurationTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
            var execTrendsTask = _databaseService.GetExecutionTrendsAsync(_perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);

            await Task.WhenAll(queryDurationTrendsTask, procDurationTrendsTask, qsDurationTrendsTask, execTrendsTask);

            LoadDurationChart(QueryPerfTrendsQueryChart, await queryDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[0], _queryDurationHover);
            LoadDurationChart(QueryPerfTrendsProcChart, await procDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[1], _procDurationHover);
            LoadDurationChart(QueryPerfTrendsQsChart, await qsDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[4], _qsDurationHover);
            LoadExecChart(await execTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
        }

        private async Task RefreshQueryStatsGridAsync()
        {
            if (_databaseService == null) return;
            List<QueryStatsItem> data;
            if (QueryStatsSlicer.HasNarrowedSelection)
                data = await _databaseService.GetQueryStatsAsync(0, QueryStatsSlicer.SelectionStart, QueryStatsSlicer.SelectionEnd, fromSlicer: true);
            else
                data = await _databaseService.GetQueryStatsAsync(_queryStatsHoursBack, _queryStatsFromDate, _queryStatsToDate);
            PopulateQueryStatsGrid(data);
            LoadQueryStatsSlicerAsync().ConfigureAwait(false);
        }

        private async Task RefreshProcStatsGridAsync()
        {
            if (_databaseService == null) return;
            List<ProcedureStatsItem> data;
            if (ProcStatsSlicer.HasNarrowedSelection)
                data = await _databaseService.GetProcedureStatsAsync(0, ProcStatsSlicer.SelectionStart, ProcStatsSlicer.SelectionEnd, fromSlicer: true);
            else
                data = await _databaseService.GetProcedureStatsAsync(_procStatsHoursBack, _procStatsFromDate, _procStatsToDate);
            PopulateProcStatsGrid(data);
            LoadProcStatsSlicerAsync().ConfigureAwait(false);
        }

        private async Task RefreshQueryStoreGridAsync()
        {
            if (_databaseService == null) return;
            List<QueryStoreItem> data;
            if (QueryStoreSlicer.HasNarrowedSelection)
                data = await _databaseService.GetQueryStoreDataAsync(0, QueryStoreSlicer.SelectionStart, QueryStoreSlicer.SelectionEnd, fromSlicer: true);
            else
                data = await _databaseService.GetQueryStoreDataAsync(_queryStoreHoursBack, _queryStoreFromDate, _queryStoreToDate);
            PopulateQueryStoreGrid(data);
            LoadQueryStoreSlicerAsync().ConfigureAwait(false);
        }

        private void PopulateQueryStatsGrid(List<QueryStatsItem> data)
        {
            SetItemsSourcePreservingSort(QueryStatsDataGrid, data, "AvgCpuTimeMs", ListSortDirection.Descending);
            QueryStatsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }

        private void PopulateProcStatsGrid(List<ProcedureStatsItem> data)
        {
            SetItemsSourcePreservingSort(ProcStatsDataGrid, data, "AvgCpuTimeMs", ListSortDirection.Descending);
            ProcStatsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }

        private void PopulateQueryStoreGrid(List<QueryStoreItem> data)
        {
            SetItemsSourcePreservingSort(QueryStoreDataGrid, data, "AvgCpuTimeMs", ListSortDirection.Descending);
            QueryStoreNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }

        private void SetStatus(string message)
        {
            _statusCallback?.Invoke(message);
        }

        private static void SetItemsSourcePreservingSort(
            DataGrid grid, System.Collections.IEnumerable? newSource,
            string? defaultSortProperty = null,
            ListSortDirection defaultDirection = ListSortDirection.Descending)
        {
            var savedSorts = grid.Items.SortDescriptions.ToList();

            grid.ItemsSource = newSource;

            if (savedSorts.Count > 0)
            {
                foreach (var sort in savedSorts)
                    grid.Items.SortDescriptions.Add(sort);

                foreach (var column in grid.Columns)
                {
                    if (column is DataGridBoundColumn bc &&
                        bc.Binding is Binding b)
                    {
                        var match = savedSorts.FirstOrDefault(s => s.PropertyName == b.Path.Path);
                        column.SortDirection = match.PropertyName != null ? match.Direction : null;
                    }
                }
            }
            else if (defaultSortProperty != null)
            {
                grid.Items.SortDescriptions.Add(new SortDescription(defaultSortProperty, defaultDirection));
                foreach (var column in grid.Columns)
                {
                    if (column is DataGridBoundColumn bc &&
                        bc.Binding is Binding b &&
                        b.Path.Path == defaultSortProperty)
                    {
                        column.SortDirection = defaultDirection;
                        return;
                    }
                }
            }
        }

        private void SetupChartSaveMenus()
        {
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsQueryChart, "Query_Durations", "report.query_stats_summary");
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsProcChart, "Procedure_Durations", "report.procedure_stats_summary");
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsQsChart, "QueryStore_Durations", "report.query_store_summary");
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsExecChart, "Execution_Counts", "collect.query_stats");
        }

        // ── Active Queries refresh ──

        private async Task RefreshActiveQueriesAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("QueryPerf-ActiveQueries");
            if (_databaseService == null) return;
            if (_isDrillDownActive) return;

            try
            {
                // Only show loading overlay on initial load (no existing data)
                if (ActiveQueriesDataGrid.ItemsSource == null)
                {
                    ActiveQueriesLoading.IsLoading = true;
                    ActiveQueriesNoDataMessage.Visibility = Visibility.Collapsed;
                }
                SetStatus("Loading active queries...");

                // If user has narrowed the slicer, use slicer dates for the grid
                List<QuerySnapshotItem> data;
                if (ActiveQueriesSlicer.HasNarrowedSelection)
                {
                    data = await _databaseService.GetQuerySnapshotsAsync(0, ActiveQueriesSlicer.SelectionStart, ActiveQueriesSlicer.SelectionEnd);
                }
                else
                {
                    data = await _databaseService.GetQuerySnapshotsAsync(_activeQueriesHoursBack, _activeQueriesFromDate, _activeQueriesToDate);
                }

                SetItemsSourcePreservingSort(ActiveQueriesDataGrid, data);
                ActiveQueriesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                SetStatus($"Loaded {data.Count} query snapshots");
                LoadActiveQueriesSlicerAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading active queries: {ex.Message}");
                SetStatus("Error loading active queries");
            }
            finally
            {
                ActiveQueriesLoading.IsLoading = false;
            }
        }

        // ── Current Active Queries refresh ──

        private async void CurrentActiveRefresh_Click(object sender, RoutedEventArgs e)
        {
            await RefreshCurrentActiveQueriesAsync();
        }

        private async Task RefreshCurrentActiveQueriesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                CurrentActiveRefreshButton.IsEnabled = false;

                if (CurrentActiveQueriesDataGrid.ItemsSource == null)
                {
                    CurrentActiveLoading.IsLoading = true;
                    CurrentActiveNoDataMessage.Visibility = Visibility.Collapsed;
                }
                SetStatus("Loading current active queries...");

                var data = await _databaseService.GetCurrentActiveQueriesAsync();

                _currentActiveUnfilteredData = data;
                SetItemsSourcePreservingSort(CurrentActiveQueriesDataGrid, data);
                CurrentActiveNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                CurrentActiveTimestamp.Text = $"Last refreshed: {DateTime.Now:HH:mm:ss} — {data.Count} queries";

                if (_currentActiveFilters.Count > 0)
                    ApplyCurrentActiveFilters();

                SetStatus($"Loaded {data.Count} current active queries");
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading current active queries: {ex.Message}");
                CurrentActiveTimestamp.Text = $"Error: {ex.Message}";
                SetStatus("Error loading current active queries");
            }
            finally
            {
                CurrentActiveLoading.IsLoading = false;
                CurrentActiveRefreshButton.IsEnabled = true;
            }
        }

        // ── DataGrid double-click history windows ──

        private void QueryStoreDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            if (_databaseService == null) return;

            if (QueryStoreDataGrid.SelectedItem is QueryStoreItem item)
            {
                // Ensure we have a valid database name and query ID
                if (string.IsNullOrEmpty(item.DatabaseName) || item.QueryId <= 0)
                {
                    MessageBox.Show(
                        "Unable to show history: missing database name or query ID.",
                        "Information",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var historyWindow = new QueryExecutionHistoryWindow(
                    _databaseService,
                    item.DatabaseName,
                    item.QueryId,
                    "Query Store",
                    _queryStoreHoursBack,
                    _queryStoreFromDate,
                    _queryStoreToDate
                );
                historyWindow.Owner = Window.GetWindow(this);
                historyWindow.ShowDialog();
            }
        }

        private void QueryStoreRegressionsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            if (_databaseService == null) return;

            if (QueryStoreRegressionsDataGrid.SelectedItem is QueryStoreRegressionItem item)
            {
                if (string.IsNullOrEmpty(item.DatabaseName) || item.QueryId <= 0)
                {
                    MessageBox.Show(
                        "Unable to show history: missing database name or query ID.",
                        "Information",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var historyWindow = new QueryExecutionHistoryWindow(
                    _databaseService,
                    item.DatabaseName,
                    item.QueryId,
                    "Query Store",
                    _queryStoreHoursBack,
                    _queryStoreFromDate,
                    _queryStoreToDate
                );
                historyWindow.Owner = Window.GetWindow(this);
                historyWindow.ShowDialog();
            }
        }

        private void ProcStatsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            if (_databaseService == null) return;

            if (ProcStatsDataGrid.SelectedItem is ProcedureStatsItem item)
            {
                // Ensure we have a valid database name and object ID
                if (string.IsNullOrEmpty(item.DatabaseName) || item.ObjectId <= 0)
                {
                    MessageBox.Show(
                        "Unable to show history: missing database name or object ID.",
                        "Information",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var historyWindow = new ProcedureHistoryWindow(
                    _databaseService,
                    item.DatabaseName,
                    item.ObjectId,
                    item.FullObjectName ?? item.ObjectName ?? $"ObjectId_{item.ObjectId}",
                    _procStatsHoursBack,
                    _procStatsFromDate,
                    _procStatsToDate,
                    item.SchemaName,
                    item.ProcedureName
                );
                historyWindow.Owner = Window.GetWindow(this);
                historyWindow.ShowDialog();
            }
        }

        private void QueryStatsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            if (_databaseService == null) return;

            if (QueryStatsDataGrid.SelectedItem is QueryStatsItem item)
            {
                // Ensure we have a valid database name and query hash
                if (string.IsNullOrEmpty(item.DatabaseName) || string.IsNullOrEmpty(item.QueryHash))
                {
                    MessageBox.Show(
                        "Unable to show history: missing database name or query hash.",
                        "Information",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var historyWindow = new QueryStatsHistoryWindow(
                    _databaseService,
                    item.DatabaseName,
                    item.QueryHash,
                    _queryStatsHoursBack,
                    _queryStatsFromDate,
                    _queryStatsToDate
                );
                historyWindow.Owner = Window.GetWindow(this);
                historyWindow.ShowDialog();
            }
        }

        // ── Query Store Regressions refresh ──

        private async Task RefreshQueryStoreRegressionsAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("QueryPerf-QueryStoreRegressions");
            if (_databaseService == null) return;

            try
            {
                SetStatus("Loading query store regressions...");
                var data = await _databaseService.GetQueryStoreRegressionsAsync(_qsRegressionsHoursBack, _qsRegressionsFromDate, _qsRegressionsToDate);
                SetItemsSourcePreservingSort(QueryStoreRegressionsDataGrid, data, "DurationRegressionPercent", ListSortDirection.Descending);
                QueryStoreRegressionsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                SetStatus($"Loaded {data.Count} query store regression records");
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading query store regressions: {ex.Message}");
                SetStatus("Error loading query store regressions");
                QueryStoreRegressionsDataGrid.ItemsSource = null;
                QueryStoreRegressionsNoDataMessage.Visibility = Visibility.Visible;
            }
        }

        // ── Long Running Patterns refresh ──

        private async Task RefreshLongRunningPatternsAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("QueryPerf-LongRunningPatterns");
            if (_databaseService == null) return;

            try
            {
                SetStatus("Loading long running query patterns...");
                var data = await _databaseService.GetLongRunningQueryPatternsAsync(_lrqPatternsHoursBack, _lrqPatternsFromDate, _lrqPatternsToDate);
                SetItemsSourcePreservingSort(LongRunningQueryPatternsDataGrid, data, "AvgDurationSec", ListSortDirection.Descending);
                LongRunningQueryPatternsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                SetStatus($"Loaded {data.Count} long running query pattern records");
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading long running query patterns: {ex.Message}");
                SetStatus("Error loading long running query patterns");
            }
        }

        private void LongRunningQueryPatternsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            if (_databaseService == null) return;

            if (LongRunningQueryPatternsDataGrid.SelectedItem is LongRunningQueryPatternItem item)
            {
                if (string.IsNullOrEmpty(item.DatabaseName) || string.IsNullOrEmpty(item.QueryPattern))
                {
                    MessageBox.Show(
                        "Unable to show history: missing database name or query pattern.",
                        "Information",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var historyWindow = new TracePatternHistoryWindow(
                    _databaseService,
                    item.DatabaseName,
                    item.QueryPattern,
                    _lrqPatternsHoursBack,
                    _lrqPatternsFromDate,
                    _lrqPatternsToDate
                );
                historyWindow.Owner = Window.GetWindow(this);
                historyWindow.ShowDialog();
            }
        }

        // ── Performance Trends charts ──

        /// <summary>
        /// Renders a duration trend chart from time-series data (per-collection_time aggregation).
        /// Replaces the old per-query-summary approach that produced too few data points.
        /// </summary>
        private void LoadDurationChart(WpfPlot chart, IEnumerable<DurationTrendItem> trendData, int hoursBack, DateTime? fromDate, DateTime? toDate, string legendText, ScottPlot.Color color, Helpers.ChartHoverHelper? hover = null)
        {
            try
            {
                DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
                DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
                double xMin = rangeStart.ToOADate();
                double xMax = rangeEnd.ToOADate();

                if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
                {
                    chart.Plot.Axes.Remove(existingPanel);
                    _legendPanels[chart] = null;
                }
                chart.Plot.Clear();
                hover?.Clear();
                TabHelpers.ApplyThemeToChart(chart);

                var dataList = (trendData ?? Enumerable.Empty<DurationTrendItem>())
                    .OrderBy(d => d.CollectionTime)
                    .ToList();

                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    dataList.Select(d => d.CollectionTime),
                    dataList.Select(d => d.AvgDurationMs));

                var scatter = chart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = color;
                scatter.LegendText = legendText;
                hover?.Add(scatter, legendText);

                if (xs.Length == 0)
                {
                    double xCenter = xMin + (xMax - xMin) / 2;
                    var noDataText = chart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                    noDataText.LabelFontSize = 14;
                    noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                    noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
                }

                _legendPanels[chart] = chart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                chart.Plot.Legend.FontSize = 12;

                chart.Plot.Axes.DateTimeTicksBottomDateChange();
                chart.Plot.Axes.SetLimitsX(xMin, xMax);
                chart.Plot.YLabel("Duration (ms/sec)");
                TabHelpers.LockChartVerticalAxis(chart);
                chart.Refresh();
            }
            catch (Exception ex)
            {
                Logger.Error($"LoadDurationChart failed: {ex.Message}", ex);
            }
        }

        private void LoadExecChart(IEnumerable<ExecutionTrendItem> execTrends, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(QueryPerfTrendsExecChart, out var existingPanel) && existingPanel != null)
            {
                QueryPerfTrendsExecChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[QueryPerfTrendsExecChart] = null;
            }
            QueryPerfTrendsExecChart.Plot.Clear();
            _execTrendsHover?.Clear();
            TabHelpers.ApplyThemeToChart(QueryPerfTrendsExecChart);

            var dataList = (execTrends ?? Enumerable.Empty<ExecutionTrendItem>())
                .OrderBy(d => d.CollectionTime)
                .ToList();

            var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.ExecutionsPerSecond));

            var scatter = QueryPerfTrendsExecChart.Plot.Add.Scatter(xs, ys);
            scatter.LineWidth = 2;
            scatter.MarkerSize = 5;
            scatter.Color = TabHelpers.ChartColors[0];
            scatter.LegendText = "Executions/sec";
            _execTrendsHover?.Add(scatter, "Executions/sec");

            if (xs.Length == 0)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = QueryPerfTrendsExecChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            _legendPanels[QueryPerfTrendsExecChart] = QueryPerfTrendsExecChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            QueryPerfTrendsExecChart.Plot.Legend.FontSize = 12;

            QueryPerfTrendsExecChart.Plot.Axes.DateTimeTicksBottomDateChange();
            QueryPerfTrendsExecChart.Plot.Axes.SetLimitsX(xMin, xMax);
            QueryPerfTrendsExecChart.Plot.YLabel("Executions/sec");
            TabHelpers.LockChartVerticalAxis(QueryPerfTrendsExecChart);
            QueryPerfTrendsExecChart.Refresh();
        }

        private async Task RefreshActiveQueriesWithRangeAsync(DateTime from, DateTime to)
        {
            if (_databaseService == null) return;
            _isDrillDownActive = true;

            // Update active queries state so slicer loads matching data
            _activeQueriesHoursBack = 0;
            _activeQueriesFromDate = from;
            _activeQueriesToDate = to;

            var snapshots = await _databaseService.GetQuerySnapshotsAsync(0, from, to);
            SetItemsSourcePreservingSort(ActiveQueriesDataGrid, snapshots);
            ActiveQueriesNoDataMessage.Visibility = snapshots.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            LoadActiveQueriesSlicerAsync().ConfigureAwait(false);
        }
    }
}

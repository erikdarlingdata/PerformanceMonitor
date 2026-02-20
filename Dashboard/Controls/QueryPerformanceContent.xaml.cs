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
using System.Windows.Input;
using Microsoft.Win32;
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

        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        // Active Queries filter state
        private Dictionary<string, Models.ColumnFilterState> _activeQueriesFilters = new();
        private List<QuerySnapshotItem>? _activeQueriesUnfilteredData;

        // Query Stats filter state
        private Dictionary<string, Models.ColumnFilterState> _queryStatsFilters = new();
        private List<QueryStatsItem>? _queryStatsUnfilteredData;

        // Procedure Stats filter state
        private Dictionary<string, Models.ColumnFilterState> _procStatsFilters = new();
        private List<ProcedureStatsItem>? _procStatsUnfilteredData;

        // Query Store filter state
        private Dictionary<string, Models.ColumnFilterState> _queryStoreFilters = new();
        private List<QueryStoreItem>? _queryStoreUnfilteredData;

        // Active Queries state
        private int _activeQueriesHoursBack = 1;
        private DateTime? _activeQueriesFromDate;
        private DateTime? _activeQueriesToDate;

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

        public QueryPerformanceContent()
        {
            InitializeComponent();
            SetupChartSaveMenus();
            Loaded += OnLoaded;
            Unloaded += OnUnloaded;

            _queryDurationHover = new Helpers.ChartHoverHelper(QueryPerfTrendsQueryChart, "ms/sec");
            _procDurationHover = new Helpers.ChartHoverHelper(QueryPerfTrendsProcChart, "ms/sec");
            _qsDurationHover = new Helpers.ChartHoverHelper(QueryPerfTrendsQsChart, "ms/sec");
            _execTrendsHover = new Helpers.ChartHoverHelper(QueryPerfTrendsExecChart, "/sec");
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
            }

            /* Clear large data collections to free memory */
            _activeQueriesUnfilteredData = null;
            _queryStatsUnfilteredData = null;
            _procStatsUnfilteredData = null;
            _queryStoreUnfilteredData = null;
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            // Initialize charts with dark mode immediately (before data is loaded)
            TabHelpers.ApplyDarkModeToChart(QueryPerfTrendsQueryChart);
            TabHelpers.ApplyDarkModeToChart(QueryPerfTrendsProcChart);
            TabHelpers.ApplyDarkModeToChart(QueryPerfTrendsQsChart);
            TabHelpers.ApplyDarkModeToChart(QueryPerfTrendsExecChart);
            QueryPerfTrendsQueryChart.Refresh();
            QueryPerfTrendsProcChart.Refresh();
            QueryPerfTrendsQsChart.Refresh();
            QueryPerfTrendsExecChart.Refresh();

            // Apply minimum column widths based on header text to all DataGrids
            TabHelpers.AutoSizeColumnMinWidths(ActiveQueriesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(QueryStatsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(ProcStatsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(QueryStoreDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(QueryStoreRegressionsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(LongRunningQueryPatternsDataGrid);

            // Freeze first columns for easier horizontal scrolling
            TabHelpers.FreezeColumns(ActiveQueriesDataGrid, 2);
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
        }

        /// <summary>
        /// Sets the time range for all sub-tabs.
        /// </summary>
        public void SetTimeRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
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
        }

        /// <summary>
        /// Refreshes all data for all sub-tabs.
        /// </summary>
        public async Task RefreshAllDataAsync()
        {
            try
            {
                using var _ = Helpers.MethodProfiler.StartTiming("QueryPerformance");

                if (_databaseService == null) return;

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
                var queryStats = await queryStatsTask;
                QueryStatsDataGrid.ItemsSource = queryStats;
                QueryStatsNoDataMessage.Visibility = queryStats.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                var procStats = await procStatsTask;
                ProcStatsDataGrid.ItemsSource = procStats;
                ProcStatsNoDataMessage.Visibility = procStats.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                var queryStore = await queryStoreTask;
                QueryStoreDataGrid.ItemsSource = queryStore;
                QueryStoreNoDataMessage.Visibility = queryStore.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                // Populate charts from time-series data
                LoadDurationChart(QueryPerfTrendsQueryChart, await queryDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[0], _queryDurationHover);
                LoadDurationChart(QueryPerfTrendsProcChart, await procDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[1], _procDurationHover);
                LoadDurationChart(QueryPerfTrendsQsChart, await qsDurationTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate, "Duration (ms/sec)", TabHelpers.ChartColors[4], _qsDurationHover);
                LoadExecChart(await execTrendsTask, _perfTrendsHoursBack, _perfTrendsFromDate, _perfTrendsToDate);
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

        private void SetStatus(string message)
        {
            _statusCallback?.Invoke(message);
        }

        private void SetupChartSaveMenus()
        {
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsQueryChart, "Query_Durations", "report.query_stats_summary");
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsProcChart, "Procedure_Durations", "report.procedure_stats_summary");
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsQsChart, "QueryStore_Durations", "report.query_store_summary");
            TabHelpers.SetupChartContextMenu(QueryPerfTrendsExecChart, "Execution_Counts", "collect.query_stats");
        }

        // Filtering logic moved to DataGridFilterService.ApplyFilter()

        #region Filtering

        /// <summary>
        /// Generic method to update filter button styles for any DataGrid by traversing column headers
        /// </summary>
        private void UpdateDataGridFilterButtonStyles(DataGrid dataGrid, Dictionary<string, Models.ColumnFilterState> filters)
        {
            foreach (var column in dataGrid.Columns)
            {
                // Get the header content - it's either a StackPanel containing a Button, or a direct element
                if (column.Header is StackPanel headerPanel)
                {
                    // Find the filter button in the header
                    var filterButton = headerPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        // Create a TextBlock with the filter icon - gold when active, white when inactive
                        var textBlock = new System.Windows.Controls.TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
                        };
                        filterButton.Content = textBlock;

                        // Update tooltip to show current filter
                        filterButton.ToolTip = hasActiveFilter && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                    }
                }
            }
        }

        private void EnsureFilterPopup()
        {
            if (_filterPopup == null)
            {
                _filterPopupContent = new ColumnFilterPopup();

                _filterPopup = new Popup
                {
                    Child = _filterPopupContent,
                    StaysOpen = false,
                    Placement = PlacementMode.Bottom,
                    AllowsTransparency = true
                };
            }
        }

        private void RewireFilterPopupEvents(
            EventHandler<FilterAppliedEventArgs> filterAppliedHandler,
            EventHandler filterClearedHandler)
        {
            if (_filterPopupContent == null) return;

            // Remove all possible handlers first
            _filterPopupContent.FilterApplied -= ActiveQueriesFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= ActiveQueriesFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= QueryStatsFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= QueryStatsFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= ProcStatsFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= ProcStatsFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= QueryStoreFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= QueryStoreFilterPopup_FilterCleared;

            // Add the new handlers
            _filterPopupContent.FilterApplied += filterAppliedHandler;
            _filterPopupContent.FilterCleared += filterClearedHandler;
        }


        #endregion

        #region Active Queries

        private async Task RefreshActiveQueriesAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("QueryPerf-ActiveQueries");
            if (_databaseService == null) return;

            try
            {
                // Only show loading overlay on initial load (no existing data)
                if (ActiveQueriesDataGrid.ItemsSource == null)
                {
                    ActiveQueriesLoading.IsLoading = true;
                    ActiveQueriesNoDataMessage.Visibility = Visibility.Collapsed;
                }
                SetStatus("Loading active queries...");

                var data = await _databaseService.GetQuerySnapshotsAsync(_activeQueriesHoursBack, _activeQueriesFromDate, _activeQueriesToDate);
                ActiveQueriesDataGrid.ItemsSource = data;
                ActiveQueriesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                SetStatus($"Loaded {data.Count} query snapshots");
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

        private void ActiveQueriesFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                ActiveQueriesFilterPopup_FilterApplied,
                ActiveQueriesFilterPopup_FilterCleared);

            _activeQueriesFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void ActiveQueriesFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _activeQueriesFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _activeQueriesFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyActiveQueriesFilters();
            UpdateDataGridFilterButtonStyles(ActiveQueriesDataGrid, _activeQueriesFilters);
        }

        private void ActiveQueriesFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyActiveQueriesFilters()
        {
            if (_activeQueriesUnfilteredData == null)
            {
                // Capture the unfiltered data on first filter application
                _activeQueriesUnfilteredData = ActiveQueriesDataGrid.ItemsSource as List<QuerySnapshotItem>;
                if (_activeQueriesUnfilteredData == null && ActiveQueriesDataGrid.ItemsSource != null)
                {
                    _activeQueriesUnfilteredData = (ActiveQueriesDataGrid.ItemsSource as IEnumerable<QuerySnapshotItem>)?.ToList();
                }
            }

            if (_activeQueriesUnfilteredData == null) return;

            if (_activeQueriesFilters.Count == 0)
            {
                ActiveQueriesDataGrid.ItemsSource = _activeQueriesUnfilteredData;
                return;
            }

            var filteredData = _activeQueriesUnfilteredData.Where(item =>
            {
                foreach (var filter in _activeQueriesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            ActiveQueriesDataGrid.ItemsSource = filteredData;
        }

        private async void DownloadActiveQueryPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is QuerySnapshotItem item && _databaseService != null)
            {
                try
                {
                    SetStatus("Fetching query plan...");

                    // Fetch the plan on-demand (not loaded with grid data for performance)
                    var queryPlan = await _databaseService.GetQuerySnapshotPlanAsync(item.CollectionTime, item.SessionId);

                    if (string.IsNullOrWhiteSpace(queryPlan))
                    {
                        MessageBox.Show("No query plan available.", "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
                        SetStatus("Ready");
                        return;
                    }

                    var rowNumber = ActiveQueriesDataGrid.Items.IndexOf(item) + 1;
                    var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                    var defaultFileName = $"active_query_plan_{item.SessionId}_{rowNumber}_{timestamp}.sqlplan";

                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = defaultFileName,
                        DefaultExt = ".sqlplan",
                        Filter = "SQL Plan (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                        Title = "Save Query Plan"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        File.WriteAllText(saveFileDialog.FileName, queryPlan);
                        MessageBox.Show($"Query plan saved to:\n{saveFileDialog.FileName}", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                    }

                    SetStatus("Ready");
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Error fetching/saving query plan:\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    SetStatus("Error fetching query plan");
                }
            }
        }

        #endregion

        #region Query Stats

        private void QueryStatsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                QueryStatsFilterPopup_FilterApplied,
                QueryStatsFilterPopup_FilterCleared);

            _queryStatsFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void QueryStatsFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _queryStatsFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _queryStatsFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyQueryStatsFilters();
            UpdateDataGridFilterButtonStyles(QueryStatsDataGrid, _queryStatsFilters);
        }

        private void QueryStatsFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyQueryStatsFilters()
        {
            if (_queryStatsUnfilteredData == null)
            {
                _queryStatsUnfilteredData = QueryStatsDataGrid.ItemsSource as List<QueryStatsItem>;
                if (_queryStatsUnfilteredData == null && QueryStatsDataGrid.ItemsSource != null)
                {
                    _queryStatsUnfilteredData = (QueryStatsDataGrid.ItemsSource as IEnumerable<QueryStatsItem>)?.ToList();
                }
            }

            if (_queryStatsUnfilteredData == null) return;

            if (_queryStatsFilters.Count == 0)
            {
                QueryStatsDataGrid.ItemsSource = _queryStatsUnfilteredData;
                return;
            }

            var filteredData = _queryStatsUnfilteredData.Where(item =>
            {
                foreach (var filter in _queryStatsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            QueryStatsDataGrid.ItemsSource = filteredData;
        }


        #endregion

        #region Procedure Stats

        private void ProcStatsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                ProcStatsFilterPopup_FilterApplied,
                ProcStatsFilterPopup_FilterCleared);

            _procStatsFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void ProcStatsFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _procStatsFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _procStatsFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyProcStatsFilters();
            UpdateDataGridFilterButtonStyles(ProcStatsDataGrid, _procStatsFilters);
        }

        private void ProcStatsFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyProcStatsFilters()
        {
            if (_procStatsUnfilteredData == null)
            {
                _procStatsUnfilteredData = ProcStatsDataGrid.ItemsSource as List<ProcedureStatsItem>;
                if (_procStatsUnfilteredData == null && ProcStatsDataGrid.ItemsSource != null)
                {
                    _procStatsUnfilteredData = (ProcStatsDataGrid.ItemsSource as IEnumerable<ProcedureStatsItem>)?.ToList();
                }
            }

            if (_procStatsUnfilteredData == null) return;

            if (_procStatsFilters.Count == 0)
            {
                ProcStatsDataGrid.ItemsSource = _procStatsUnfilteredData;
                return;
            }

            var filteredData = _procStatsUnfilteredData.Where(item =>
            {
                foreach (var filter in _procStatsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            ProcStatsDataGrid.ItemsSource = filteredData;
        }


        #endregion

        #region Query Store

        private void QueryStoreFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                QueryStoreFilterPopup_FilterApplied,
                QueryStoreFilterPopup_FilterCleared);

            _queryStoreFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void QueryStoreFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _queryStoreFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _queryStoreFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyQueryStoreFilters();
            UpdateDataGridFilterButtonStyles(QueryStoreDataGrid, _queryStoreFilters);
        }

        private void QueryStoreFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyQueryStoreFilters()
        {
            if (_queryStoreUnfilteredData == null)
            {
                _queryStoreUnfilteredData = QueryStoreDataGrid.ItemsSource as List<QueryStoreItem>;
                if (_queryStoreUnfilteredData == null && QueryStoreDataGrid.ItemsSource != null)
                {
                    _queryStoreUnfilteredData = (QueryStoreDataGrid.ItemsSource as IEnumerable<QueryStoreItem>)?.ToList();
                }
            }

            if (_queryStoreUnfilteredData == null) return;

            if (_queryStoreFilters.Count == 0)
            {
                QueryStoreDataGrid.ItemsSource = _queryStoreUnfilteredData;
                return;
            }

            var filteredData = _queryStoreUnfilteredData.Where(item =>
            {
                foreach (var filter in _queryStoreFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            QueryStoreDataGrid.ItemsSource = filteredData;
        }

        private void QueryStoreDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
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

        private void ProcStatsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
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
                    _procStatsToDate
                );
                historyWindow.Owner = Window.GetWindow(this);
                historyWindow.ShowDialog();
            }
        }

        private void QueryStatsDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
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

        #endregion

        #region Query Store Regressions

        private async Task RefreshQueryStoreRegressionsAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("QueryPerf-QueryStoreRegressions");
            if (_databaseService == null) return;

            try
            {
                SetStatus("Loading query store regressions...");
                var data = await _databaseService.GetQueryStoreRegressionsAsync(_qsRegressionsHoursBack, _qsRegressionsFromDate, _qsRegressionsToDate);
                QueryStoreRegressionsDataGrid.ItemsSource = data;
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

        private void QueryStoreRegressionsFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(QueryStoreRegressionsDataGrid, sender as TextBox);
        }

        private void QueryStoreRegressionsNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(QueryStoreRegressionsDataGrid, sender as TextBox);
        }

        #endregion

        #region Query Trace Patterns

        private async Task RefreshLongRunningPatternsAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("QueryPerf-LongRunningPatterns");
            if (_databaseService == null) return;

            try
            {
                SetStatus("Loading long running query patterns...");
                var data = await _databaseService.GetLongRunningQueryPatternsAsync(_lrqPatternsHoursBack, _lrqPatternsFromDate, _lrqPatternsToDate);
                LongRunningQueryPatternsDataGrid.ItemsSource = data;
                LongRunningQueryPatternsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                SetStatus($"Loaded {data.Count} long running query pattern records");
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading long running query patterns: {ex.Message}");
                SetStatus("Error loading long running query patterns");
            }
        }

        private void LongRunningQueryPatternsFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(LongRunningQueryPatternsDataGrid, sender as TextBox);
        }

        private void LongRunningQueryPatternsNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(LongRunningQueryPatternsDataGrid, sender as TextBox);
        }

        #endregion

        #region Performance Trends

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
                TabHelpers.ApplyDarkModeToChart(chart);

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

                chart.Plot.Axes.DateTimeTicksBottom();
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
            TabHelpers.ApplyDarkModeToChart(QueryPerfTrendsExecChart);

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

            QueryPerfTrendsExecChart.Plot.Axes.DateTimeTicksBottom();
            QueryPerfTrendsExecChart.Plot.Axes.SetLimitsX(xMin, xMax);
            QueryPerfTrendsExecChart.Plot.YLabel("Executions/sec");
            TabHelpers.LockChartVerticalAxis(QueryPerfTrendsExecChart);
            QueryPerfTrendsExecChart.Refresh();
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

                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                    {
                        if (column is DataGridBoundColumn)
                        {
                            headers.Add(TabHelpers.GetColumnHeader(column));
                        }
                    }
                    sb.AppendLine(string.Join("\t", headers));

                    foreach (var item in dataGrid.Items)
                    {
                        sb.AppendLine(TabHelpers.GetRowAsText(dataGrid, item));
                    }

                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void CopyReproScript_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not MenuItem menuItem || menuItem.Parent is not ContextMenu contextMenu) return;

            var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
            if (dataGrid?.SelectedItem == null) return;

            var item = dataGrid.SelectedItem;
            string? queryText = null;
            string? databaseName = null;
            string? planXml = null;
            string source = "Query";

            /* Extract data based on item type */
            switch (item)
            {
                case QuerySnapshotItem qs:
                    queryText = qs.QueryText;
                    databaseName = qs.DatabaseName;
                    planXml = qs.QueryPlan;
                    source = "Active Queries";
                    break;
                case QueryStatsItem qst:
                    queryText = qst.QueryText;
                    databaseName = qst.DatabaseName;
                    planXml = qst.QueryPlanXml;
                    source = "Query Stats";
                    break;
                case QueryStoreItem qsi:
                    queryText = qsi.QueryText;
                    databaseName = qsi.DatabaseName;
                    planXml = qsi.QueryPlanXml;
                    source = "Query Store";
                    break;
                case ProcedureStatsItem ps:
                    queryText = ps.ObjectName;
                    databaseName = ps.DatabaseName;
                    planXml = null; /* Procedures don't have plan XML in the model */
                    source = "Procedure Stats";
                    break;
                default:
                    MessageBox.Show("Copy Repro Script is not available for this data type.", "Not Available", MessageBoxButton.OK, MessageBoxImage.Information);
                    return;
            }

            if (string.IsNullOrWhiteSpace(queryText))
            {
                MessageBox.Show("No query text available for this row.", "No Query Text", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var script = ReproScriptBuilder.BuildReproScript(queryText, databaseName, planXml, isolationLevel: null, source);

            try
            {
                Clipboard.SetDataObject(script, false);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to copy to clipboard: {ex.Message}", "Clipboard Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"query_performance_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            var headers = new List<string>();
                            foreach (var column in dataGrid.Columns)
                            {
                                if (column is DataGridBoundColumn)
                                {
                                    headers.Add(TabHelpers.EscapeCsvField(TabHelpers.GetColumnHeader(column)));
                                }
                            }
                            sb.AppendLine(string.Join(",", headers));

                            foreach (var item in dataGrid.Items)
                            {
                                var values = TabHelpers.GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(",", values.Select(v => TabHelpers.EscapeCsvField(v))));
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

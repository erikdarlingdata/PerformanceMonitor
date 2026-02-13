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
    /// UserControl for the System Events tab content.
    /// Displays HealthParser data including system health, errors, I/O issues, scheduler issues, etc.
    /// </summary>
    public partial class SystemEventsContent : UserControl
    {
        private DatabaseService? _databaseService;

        // System Health state
        private int _systemHealthHoursBack = 24;
        private DateTime? _systemHealthFromDate;
        private DateTime? _systemHealthToDate;

        // Severe Errors state
        private int _severeErrorsHoursBack = 24;
        private DateTime? _severeErrorsFromDate;
        private DateTime? _severeErrorsToDate;

        // IO Issues state
        private int _ioIssuesHoursBack = 24;
        private DateTime? _ioIssuesFromDate;
        private DateTime? _ioIssuesToDate;

        // Scheduler Issues state
        private int _schedulerIssuesHoursBack = 24;
        private DateTime? _schedulerIssuesFromDate;
        private DateTime? _schedulerIssuesToDate;

        // Memory Conditions state
        private int _memoryConditionsHoursBack = 24;
        private DateTime? _memoryConditionsFromDate;
        private DateTime? _memoryConditionsToDate;

        // CPU Tasks state
        private int _cpuTasksHoursBack = 24;
        private DateTime? _cpuTasksFromDate;
        private DateTime? _cpuTasksToDate;

        // Memory Broker state
        private int _memoryBrokerHoursBack = 24;
        private DateTime? _memoryBrokerFromDate;
        private DateTime? _memoryBrokerToDate;

        // Memory Node OOM state
        private int _memoryNodeOOMHoursBack = 24;
        private DateTime? _memoryNodeOOMFromDate;
        private DateTime? _memoryNodeOOMToDate;

        // Filter state dictionaries for each DataGrid
        private Dictionary<string, ColumnFilterState> _systemHealthFilters = new();
        private Dictionary<string, ColumnFilterState> _severeErrorsFilters = new();
        private Dictionary<string, ColumnFilterState> _ioIssuesFilters = new();
        // Scheduler Issues filter removed - grid removed per todo.md #13
        // Memory Conditions filter removed - grid removed per todo.md #14
        // CPU Tasks filter removed - grid removed per todo.md #15
        private Dictionary<string, ColumnFilterState> _memoryBrokerFilters = new();
        private Dictionary<string, ColumnFilterState> _memoryNodeOOMFilters = new();

        // Unfiltered data caches
        private List<HealthParserSystemHealthItem>? _systemHealthUnfilteredData;
        private List<HealthParserSevereErrorItem>? _severeErrorsUnfilteredData;
        private List<HealthParserIOIssueItem>? _ioIssuesUnfilteredData;
        // Scheduler Issues unfiltered data cache removed - grid removed per todo.md #13
        // Memory Conditions unfiltered data cache removed - grid removed per todo.md #14
        // CPU Tasks unfiltered data cache removed - grid removed per todo.md #15
        private List<HealthParserMemoryBrokerItem>? _memoryBrokerUnfilteredData;
        private List<HealthParserMemoryNodeOOMItem>? _memoryNodeOOMUnfilteredData;

        // Shared popup controls
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        // Track which DataGrid the popup is for
        private string _currentFilterTarget = string.Empty;

        // Legend panel references for edge-based legends (ScottPlot issue #4717 workaround)
        private Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();

        public SystemEventsContent()
        {
            InitializeComponent();
            SetupChartContextMenus();
            Loaded += OnLoaded;
            Unloaded += OnUnloaded;
        }

        private void OnUnloaded(object sender, RoutedEventArgs e)
        {
            /* Unsubscribe from filter popup events to prevent memory leaks */
            if (_filterPopupContent != null)
            {
                _filterPopupContent.FilterApplied -= FilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= FilterPopup_FilterCleared;
            }

            /* Clear large data collections to free memory */
            _systemHealthUnfilteredData = null;
            _severeErrorsUnfilteredData = null;
            _ioIssuesUnfilteredData = null;
            _memoryBrokerUnfilteredData = null;
            _memoryNodeOOMUnfilteredData = null;
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            // Apply minimum column widths based on header text
            // SystemHealthDataGrid removed - chart only per todo.md #18
            TabHelpers.AutoSizeColumnMinWidths(SevereErrorsDataGrid);
            // IOIssuesDataGrid removed - chart only per todo.md #19
            // SchedulerIssuesDataGrid removed - chart + summary only per todo.md #13
            // MemoryConditionsDataGrid removed - chart only per todo.md #14
            // CPUTasksDataGrid AutoSizeColumnMinWidths removed - chart + summary only per todo.md #15
            TabHelpers.AutoSizeColumnMinWidths(MemoryBrokerDataGrid);
            // MemoryNodeOOMDataGrid removed - chart only per GitHub issue #13

            // Freeze time column for easier horizontal scrolling
            // SystemHealthDataGrid FreezeColumns removed - chart only per todo.md #18
            TabHelpers.FreezeColumns(SevereErrorsDataGrid, 1);
            // IOIssuesDataGrid FreezeColumns removed - chart only per todo.md #19
            // SchedulerIssuesDataGrid FreezeColumns removed - chart + summary only per todo.md #13

            // CPUTasksDataGrid FreezeColumns removed - chart + summary only per todo.md #15
            TabHelpers.FreezeColumns(MemoryBrokerDataGrid, 1);
            // MemoryNodeOOMDataGrid FreezeColumns removed - chart only per GitHub issue #13
        }

        private void SetupChartContextMenus()
        {
            // Corruption Events charts
            TabHelpers.SetupChartContextMenu(BadPagesChart, "Bad_Pages", "collect.HealthParser_SystemHealth");
            TabHelpers.SetupChartContextMenu(DumpRequestsChart, "Dump_Requests", "collect.HealthParser_SystemHealth");
            TabHelpers.SetupChartContextMenu(AccessViolationsChart, "Access_Violations", "collect.HealthParser_SystemHealth");
            TabHelpers.SetupChartContextMenu(WriteAccessViolationsChart, "Write_Access_Violations", "collect.HealthParser_SystemHealth");

            // Contention Events charts
            TabHelpers.SetupChartContextMenu(NonYieldingTasksChart, "NonYielding_Tasks", "collect.HealthParser_SystemHealth");
            TabHelpers.SetupChartContextMenu(LatchWarningsChart, "Latch_Warnings", "collect.HealthParser_SystemHealth");
            TabHelpers.SetupChartContextMenu(SickSpinlocksChart, "Sick_Spinlocks", "collect.HealthParser_SystemHealth");
            TabHelpers.SetupChartContextMenu(CpuComparisonChart, "CPU_Comparison", "collect.HealthParser_SystemHealth");

            // Severe Errors chart
            TabHelpers.SetupChartContextMenu(SevereErrorsChart, "Severe_Errors", "collect.HealthParser_SevereErrors");

            // I/O Issues charts
            TabHelpers.SetupChartContextMenu(IOIssuesChart, "IO_Issues", "collect.HealthParser_IOIssues");
            TabHelpers.SetupChartContextMenu(LongestPendingIOChart, "Longest_Pending_IO", "collect.HealthParser_IOIssues");

            // Scheduler Issues chart
            TabHelpers.SetupChartContextMenu(SchedulerIssuesChart, "Scheduler_Issues", "collect.HealthParser_SchedulerIssues");

            // Memory Conditions chart
            TabHelpers.SetupChartContextMenu(MemoryConditionsChart, "Memory_Conditions", "collect.HealthParser_MemoryConditions");

            // CPU Tasks chart
            TabHelpers.SetupChartContextMenu(CPUTasksChart, "CPU_Tasks", "collect.HealthParser_CPUTasks");

            // Memory Broker chart
            TabHelpers.SetupChartContextMenu(MemoryBrokerChart, "Memory_Broker", "collect.HealthParser_MemoryBroker");

            // Memory Node OOM chart
            TabHelpers.SetupChartContextMenu(MemoryNodeOOMChart, "Memory_Node_OOM", "collect.HealthParser_MemoryNodeOOM");
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
        }

        /// <summary>
        /// Sets the time range for all system events sub-tabs.
        /// </summary>
        public void SetTimeRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            _systemHealthHoursBack = hoursBack;
            _systemHealthFromDate = fromDate;
            _systemHealthToDate = toDate;

            _severeErrorsHoursBack = hoursBack;
            _severeErrorsFromDate = fromDate;
            _severeErrorsToDate = toDate;

            _ioIssuesHoursBack = hoursBack;
            _ioIssuesFromDate = fromDate;
            _ioIssuesToDate = toDate;

            _schedulerIssuesHoursBack = hoursBack;
            _schedulerIssuesFromDate = fromDate;
            _schedulerIssuesToDate = toDate;

            _memoryConditionsHoursBack = hoursBack;
            _memoryConditionsFromDate = fromDate;
            _memoryConditionsToDate = toDate;

            _cpuTasksHoursBack = hoursBack;
            _cpuTasksFromDate = fromDate;
            _cpuTasksToDate = toDate;

            _memoryBrokerHoursBack = hoursBack;
            _memoryBrokerFromDate = fromDate;
            _memoryBrokerToDate = toDate;

            _memoryNodeOOMHoursBack = hoursBack;
            _memoryNodeOOMFromDate = fromDate;
            _memoryNodeOOMToDate = toDate;
        }

        /// <summary>
        /// Refreshes all system events data. Can be called from parent control.
        /// </summary>
        public async Task RefreshAllDataAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("SystemEvents");
            if (_databaseService == null) return;

            try
            {
                // Run all independent refreshes in parallel for better performance
                await Task.WhenAll(
                    RefreshSystemHealthAsync(),
                    RefreshSevereErrorsAsync(),
                    RefreshIOIssuesAsync(),
                    RefreshSchedulerIssuesAsync(),
                    RefreshMemoryConditionsAsync(),
                    RefreshCPUTasksAsync(),
                    RefreshMemoryBrokerAsync(),
                    RefreshMemoryNodeOOMAsync()
                );
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing system events data: {ex.Message}", ex);
            }
        }

        #region Shared Filter Popup Methods

        private void ShowFilterPopup(Button button, string columnName, string targetGrid,
            Dictionary<string, ColumnFilterState> filters,
            Action<FilterAppliedEventArgs> onApplied,
            Action onCleared)
        {
            // Create popup if needed
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

            // Disconnect previous event handlers
            _filterPopupContent!.FilterApplied -= FilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= FilterPopup_FilterCleared;

            // Set up current target and reconnect handlers
            _currentFilterTarget = targetGrid;
            _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

            // Initialize with current filter state
            filters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent.Initialize(columnName, existingFilter);

            // Position and show
            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            switch (_currentFilterTarget)
            {
                // SystemHealth case removed - grid removed per todo.md #18
                case "SevereErrors":
                    UpdateFilterState(_severeErrorsFilters, e.FilterState);
                    ApplySevereErrorsFilters();
                    UpdateSevereErrorsFilterButtonStyles();
                    break;
                // IOIssues case removed - grid removed per todo.md #19
                // SchedulerIssues case removed - grid removed per todo.md #13
                // MemoryConditions case removed - grid removed per todo.md #14
                // CPUTasks case removed - grid removed per todo.md #15
                case "MemoryBroker":
                    UpdateFilterState(_memoryBrokerFilters, e.FilterState);
                    ApplyMemoryBrokerFilters();
                    UpdateMemoryBrokerFilterButtonStyles();
                    break;
                // MemoryNodeOOM case removed - DataGrid removed per GitHub issue #13
            }
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void UpdateFilterState(Dictionary<string, ColumnFilterState> filters, ColumnFilterState filterState)
        {
            if (filterState.IsActive)
            {
                filters[filterState.ColumnName] = filterState;
            }
            else
            {
                filters.Remove(filterState.ColumnName);
            }
        }

        private void UpdateFilterButtonStyle(DataGrid dataGrid, string columnName, Dictionary<string, ColumnFilterState> filters)
        {
            // Find the button in the column header
            foreach (var column in dataGrid.Columns)
            {
                if (column.Header is StackPanel stackPanel)
                {
                    var button = stackPanel.Children.OfType<Button>().FirstOrDefault();
                    if (button != null && button.Tag is string tag && tag == columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        // Create a TextBlock with the filter icon - gold when active, light gray when inactive
                        var textBlock = new TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
                        };
                        button.Content = textBlock;

                        button.ToolTip = hasActiveFilter && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                        break;
                    }
                }
            }
        }

        #endregion

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
            TabHelpers.ApplyDarkModeToChart(BadPagesChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.BadPagesDetected ?? 0)));
                var scatter = BadPagesChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[3];
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BadPagesChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BadPagesChart.Plot.Axes.DateTimeTicksBottom();
            BadPagesChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BadPagesChart.Plot.YLabel("Count");
            BadPagesChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(BadPagesChart);
            BadPagesChart.Refresh();

            // Interval Dump Requests Chart
            DumpRequestsChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(DumpRequestsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.IntervalDumpRequests ?? 0)));
                var scatter = DumpRequestsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[2];
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = DumpRequestsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            DumpRequestsChart.Plot.Axes.DateTimeTicksBottom();
            DumpRequestsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            DumpRequestsChart.Plot.YLabel("Count");
            DumpRequestsChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(DumpRequestsChart);
            DumpRequestsChart.Refresh();

            // Access Violations Chart
            AccessViolationsChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(AccessViolationsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.IsAccessViolationOccurred ?? 0)));
                var scatter = AccessViolationsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[4];
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = AccessViolationsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            AccessViolationsChart.Plot.Axes.DateTimeTicksBottom();
            AccessViolationsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            AccessViolationsChart.Plot.YLabel("Count");
            AccessViolationsChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(AccessViolationsChart);
            AccessViolationsChart.Refresh();

            // Write Access Violations Chart
            WriteAccessViolationsChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(WriteAccessViolationsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.WriteAccessViolationCount ?? 0)));
                var scatter = WriteAccessViolationsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[0];
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = WriteAccessViolationsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            WriteAccessViolationsChart.Plot.Axes.DateTimeTicksBottom();
            WriteAccessViolationsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            WriteAccessViolationsChart.Plot.YLabel("Count");
            WriteAccessViolationsChart.Plot.HideGrid();
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
            TabHelpers.ApplyDarkModeToChart(NonYieldingTasksChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.NonYieldingTasksReported ?? 0)));
                var scatter = NonYieldingTasksChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[3];
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = NonYieldingTasksChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            NonYieldingTasksChart.Plot.Axes.DateTimeTicksBottom();
            NonYieldingTasksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            NonYieldingTasksChart.Plot.YLabel("Count");
            NonYieldingTasksChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(NonYieldingTasksChart);
            NonYieldingTasksChart.Refresh();

            // Latch Warnings Chart
            LatchWarningsChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(LatchWarningsChart);
            if (hasData)
            {
                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.LatchWarnings ?? 0)));
                var scatter = LatchWarningsChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[2];
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = LatchWarningsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            LatchWarningsChart.Plot.Axes.DateTimeTicksBottom();
            LatchWarningsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            LatchWarningsChart.Plot.YLabel("Count");
            LatchWarningsChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(LatchWarningsChart);
            LatchWarningsChart.Refresh();

            // Sick Spinlocks by Type Chart (multi-series with legend)
            if (_legendPanels.TryGetValue(SickSpinlocksChart, out var existingSickSpinlocksPanel) && existingSickSpinlocksPanel != null)
            {
                SickSpinlocksChart.Plot.Axes.Remove(existingSickSpinlocksPanel);
                _legendPanels[SickSpinlocksChart] = null;
            }
            SickSpinlocksChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(SickSpinlocksChart);
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
            SickSpinlocksChart.Plot.Axes.DateTimeTicksBottom();
            SickSpinlocksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            SickSpinlocksChart.Plot.YLabel("Backoffs");
            SickSpinlocksChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(SickSpinlocksChart);
            SickSpinlocksChart.Refresh();

            // CPU Comparison Chart (SQL CPU vs System CPU)
            if (_legendPanels.TryGetValue(CpuComparisonChart, out var existingCpuComparisonPanel) && existingCpuComparisonPanel != null)
            {
                CpuComparisonChart.Plot.Axes.Remove(existingCpuComparisonPanel);
                _legendPanels[CpuComparisonChart] = null;
            }
            CpuComparisonChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(CpuComparisonChart);
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

                // SQL CPU series
                var (sqlXs, sqlYs) = TabHelpers.FillTimeSeriesGaps(
                    orderedData.Select(d => d.CollectionTime),
                    orderedData.Select(d => (double)(d.SqlCpuUtilization ?? 0)));
                var sqlScatter = CpuComparisonChart.Plot.Add.Scatter(sqlXs, sqlYs);
                sqlScatter.LineWidth = 2;
                sqlScatter.MarkerSize = 5;
                sqlScatter.Color = TabHelpers.ChartColors[1];
                sqlScatter.LegendText = "SQL CPU %";

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
            CpuComparisonChart.Plot.Axes.DateTimeTicksBottom();
            CpuComparisonChart.Plot.Axes.SetLimitsX(xMin, xMax);
            CpuComparisonChart.Plot.Axes.SetLimitsY(0, 100); // Fixed Y-axis for CPU percentage
            CpuComparisonChart.Plot.YLabel("CPU %");
            CpuComparisonChart.Plot.HideGrid();
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

        #region Severe Errors Tab

        private async System.Threading.Tasks.Task RefreshSevereErrorsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserSevereErrorsAsync(_severeErrorsHoursBack, _severeErrorsFromDate, _severeErrorsToDate);
                _severeErrorsUnfilteredData = data;
                SevereErrorsDataGrid.ItemsSource = data;
                SevereErrorsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                LoadSevereErrorsChart(data, _severeErrorsHoursBack, _severeErrorsFromDate, _severeErrorsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading severe errors: {ex.Message}", ex);
            }
        }

        private void LoadSevereErrorsChart(IEnumerable<HealthParserSevereErrorItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(SevereErrorsChart, out var existingSevereErrorsPanel) && existingSevereErrorsPanel != null)
            {
                SevereErrorsChart.Plot.Axes.Remove(existingSevereErrorsPanel);
                _legendPanels[SevereErrorsChart] = null;
            }
            SevereErrorsChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(SevereErrorsChart);

            var dataList = data?.ToList() ?? new List<HealthParserSevereErrorItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Group by hour and count events
                var grouped = dataList
                    .Where(d => d.EventTime.HasValue)
                    .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        grouped.Select(g => g.Key),
                        grouped.Select(g => (double)g.Count()));

                    var scatter = SevereErrorsChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[3];
                    scatter.LegendText = "Error Count";

                    _legendPanels[SevereErrorsChart] = SevereErrorsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    SevereErrorsChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SevereErrorsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            SevereErrorsChart.Plot.Axes.DateTimeTicksBottom();
            SevereErrorsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            SevereErrorsChart.Plot.YLabel("Event Count");
            SevereErrorsChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(SevereErrorsChart);
            SevereErrorsChart.Refresh();
        }

        private void SevereErrorsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            ShowFilterPopup(button, columnName, "SevereErrors", _severeErrorsFilters,
                args => { },
                () => { });
        }

        private void ApplySevereErrorsFilters()
        {
            if (_severeErrorsUnfilteredData == null)
            {
                _severeErrorsUnfilteredData = SevereErrorsDataGrid.ItemsSource as List<HealthParserSevereErrorItem>;
                if (_severeErrorsUnfilteredData == null && SevereErrorsDataGrid.ItemsSource != null)
                {
                    _severeErrorsUnfilteredData = (SevereErrorsDataGrid.ItemsSource as IEnumerable<HealthParserSevereErrorItem>)?.ToList();
                }
            }

            if (_severeErrorsUnfilteredData == null) return;

            if (_severeErrorsFilters.Count == 0)
            {
                SevereErrorsDataGrid.ItemsSource = _severeErrorsUnfilteredData;
                return;
            }

            var filteredData = _severeErrorsUnfilteredData.Where(item =>
            {
                foreach (var filter in _severeErrorsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            SevereErrorsDataGrid.ItemsSource = filteredData;
        }

        private void UpdateSevereErrorsFilterButtonStyles()
        {
            foreach (var columnName in new[] { "CollectionTime", "EventTime", "ErrorNumber", "Severity", "State", "DatabaseName", "Message" })
            {
                UpdateFilterButtonStyle(SevereErrorsDataGrid, columnName, _severeErrorsFilters);
            }
        }

        private void SevereErrorsFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(SevereErrorsDataGrid, sender as TextBox);
        }

        private void SevereErrorsNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(SevereErrorsDataGrid, sender as TextBox);
        }

        #endregion

        #region IO Issues Tab

        private async System.Threading.Tasks.Task RefreshIOIssuesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserIOIssuesAsync(_ioIssuesHoursBack, _ioIssuesFromDate, _ioIssuesToDate);
                _ioIssuesUnfilteredData = data;
                // IOIssuesDataGrid removed - chart only per todo.md #19
                LoadIOIssuesChart(data, _ioIssuesHoursBack, _ioIssuesFromDate, _ioIssuesToDate);
                LoadLongestPendingIOChart(data, _ioIssuesHoursBack, _ioIssuesFromDate, _ioIssuesToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading IO issues: {ex.Message}", ex);
            }
        }

        private void LoadIOIssuesChart(IEnumerable<HealthParserIOIssueItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(IOIssuesChart, out var existingIOIssuesPanel) && existingIOIssuesPanel != null)
            {
                IOIssuesChart.Plot.Axes.Remove(existingIOIssuesPanel);
                _legendPanels[IOIssuesChart] = null;
            }
            IOIssuesChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(IOIssuesChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).OrderBy(d => d.EventTime).ToList() ?? new List<HealthParserIOIssueItem>();
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
                    double[] latchTimeouts = grouped.Select(g => (double)g.Sum(i => i.IoLatchTimeouts ?? 0)).ToArray();
                    double[] longIos = grouped.Select(g => (double)g.Sum(i => i.IntervalLongIos ?? 0)).ToArray();

                    if (latchTimeouts.Any(c => c > 0))
                    {
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, latchTimeouts.Select(c => c));
                        var scatter = IOIssuesChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = TabHelpers.ChartColors[3];
                        scatter.LegendText = "Latch Timeouts";
                    }

                    if (longIos.Any(c => c > 0))
                    {
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, longIos.Select(c => c));
                        var scatter = IOIssuesChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = TabHelpers.ChartColors[2];
                        scatter.LegendText = "Long IOs";
                    }

                    _legendPanels[IOIssuesChart] = IOIssuesChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    IOIssuesChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = IOIssuesChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            IOIssuesChart.Plot.Axes.DateTimeTicksBottom();
            IOIssuesChart.Plot.Axes.SetLimitsX(xMin, xMax);
            IOIssuesChart.Plot.YLabel("Count");
            IOIssuesChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(IOIssuesChart);
            IOIssuesChart.Refresh();
        }

        // IOIssuesFilter_Click removed - grid removed per todo.md #19
        private void LoadLongestPendingIOChart(IEnumerable<HealthParserIOIssueItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(LongestPendingIOChart, out var existingLongestPendingIOPanel) && existingLongestPendingIOPanel != null)
            {
                LongestPendingIOChart.Plot.Axes.Remove(existingLongestPendingIOPanel);
                _legendPanels[LongestPendingIOChart] = null;
            }
            LongestPendingIOChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(LongestPendingIOChart);

            var dataList = data?.Where(d => d.EventTime.HasValue && !string.IsNullOrEmpty(d.LongestPendingRequestsFilePath)).ToList() ?? new List<HealthParserIOIssueItem>();
            bool hasData = false;

            if (dataList.Count > 0)
            {
                // Get distinct file paths (top 5 by total duration)
                var filePathGroups = dataList
                    .GroupBy(d => d.LongestPendingRequestsFilePath)
                    .Select(g => new
                    {
                        FilePath = g.Key,
                        TotalDuration = g.Sum(d => double.TryParse(d.LongestPendingRequestsDurationMs, out var ms) ? ms : 0),
                        Items = g.ToList()
                    })
                    .OrderByDescending(g => g.TotalDuration)
                    .Take(5)
                    .ToList();

                if (filePathGroups.Count > 0)
                {
                    hasData = true;
                    var colors = TabHelpers.ChartColors;
                    int colorIndex = 0;

                    foreach (var group in filePathGroups)
                    {
                        // Extract just the filename from the path for the legend
                        var fileName = System.IO.Path.GetFileName(group.FilePath);
                        if (string.IsNullOrEmpty(fileName))
                            fileName = group.FilePath.Length > 30 ? "..." + group.FilePath.Substring(group.FilePath.Length - 27) : group.FilePath;

                        // Group by hour
                        var hourlyData = group.Items
                            .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                            .OrderBy(g => g.Key)
                            .ToList();

                        if (hourlyData.Count > 0)
                        {
                            var timePoints = hourlyData.Select(g => g.Key);
                            var durations = hourlyData.Select(g => g.Max(d => double.TryParse(d.LongestPendingRequestsDurationMs, out var ms) ? ms : 0)).ToArray();

                            var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                                timePoints,
                                durations.Select(d => d));

                            var scatter = LongestPendingIOChart.Plot.Add.Scatter(xs, ys);
                            scatter.LineWidth = 2;
                            scatter.MarkerSize = 5;
                            scatter.Color = colors[colorIndex % colors.Length];
                            scatter.LegendText = fileName;
                            colorIndex++;
                        }
                    }

                    _legendPanels[LongestPendingIOChart] = LongestPendingIOChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    LongestPendingIOChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = LongestPendingIOChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            LongestPendingIOChart.Plot.Axes.DateTimeTicksBottom();
            LongestPendingIOChart.Plot.Axes.SetLimitsX(xMin, xMax);
            LongestPendingIOChart.Plot.YLabel("Duration (ms)");
            LongestPendingIOChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(LongestPendingIOChart);
            LongestPendingIOChart.Refresh();
        }



        // ApplyIOIssuesFilters removed - grid removed per todo.md #19

        // UpdateIOIssuesFilterButtonStyles removed - grid removed per todo.md #19

        // IOIssuesFilterTextBox_TextChanged removed - grid removed per todo.md #19
        // IOIssuesNumericFilterTextBox_TextChanged removed - grid removed per todo.md #19

        #endregion

        #region Scheduler Issues Tab

        private async System.Threading.Tasks.Task RefreshSchedulerIssuesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserSchedulerIssuesAsync(_schedulerIssuesHoursBack, _schedulerIssuesFromDate, _schedulerIssuesToDate);
                // Grid removed per todo.md #13 - chart + summary only
                LoadSchedulerIssuesChart(data, _schedulerIssuesHoursBack, _schedulerIssuesFromDate, _schedulerIssuesToDate);
                UpdateSchedulerIssuesSummaryPanel(data);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading scheduler issues: {ex.Message}", ex);
            }
        }

        private void LoadSchedulerIssuesChart(IEnumerable<HealthParserSchedulerIssueItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(SchedulerIssuesChart, out var existingSchedulerIssuesPanel) && existingSchedulerIssuesPanel != null)
            {
                SchedulerIssuesChart.Plot.Axes.Remove(existingSchedulerIssuesPanel);
                _legendPanels[SchedulerIssuesChart] = null;
            }
            SchedulerIssuesChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(SchedulerIssuesChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserSchedulerIssueItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Helper to parse NonYieldingTimeMs (it's a string)
                long ParseNonYield(string? value)
                {
                    if (string.IsNullOrEmpty(value)) return 0;
                    var numericPart = new string(value.Where(c => char.IsDigit(c) || c == '-').ToArray());
                    return long.TryParse(numericPart, out var result) ? result : 0;
                }

                // Group by hour and sum non-yielding time
                var grouped = dataList
                    .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        grouped.Select(g => g.Key),
                        grouped.Select(g => (double)g.Sum(i => ParseNonYield(i.NonYieldingTimeMs))));

                    var scatter = SchedulerIssuesChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[2];
                    scatter.LegendText = "Total Non-Yield Time";

                    _legendPanels[SchedulerIssuesChart] = SchedulerIssuesChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    SchedulerIssuesChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SchedulerIssuesChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            SchedulerIssuesChart.Plot.Axes.DateTimeTicksBottom();
            SchedulerIssuesChart.Plot.Axes.SetLimitsX(xMin, xMax);
            SchedulerIssuesChart.Plot.YLabel("Total Non-Yield Time (ms)");
            SchedulerIssuesChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(SchedulerIssuesChart);
            SchedulerIssuesChart.Refresh();
        }

        private void UpdateSchedulerIssuesSummaryPanel(List<HealthParserSchedulerIssueItem> dataList)
        {
            if (dataList == null || dataList.Count == 0)
            {
                SchedulerIssuesTotalText.Text = "0";
                SchedulerIssuesTotalNonYieldText.Text = "0 ms";
                SchedulerIssuesMaxNonYieldText.Text = "0 ms";
                SchedulerIssuesSchedulersText.Text = "0";
                SchedulerIssuesOfflineText.Text = "0";
                SchedulerIssuesOfflineText.Foreground = new System.Windows.Media.SolidColorBrush(
                    (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));
                return;
            }

            // Total issues count
            SchedulerIssuesTotalText.Text = dataList.Count.ToString("N0", CultureInfo.CurrentCulture);

            // Total and Max non-yield time (NonYieldingTimeMs is a string, need to parse)
            long ParseNonYieldTime(string? value)
            {
                if (string.IsNullOrEmpty(value)) return 0;
                // Remove any non-numeric characters and parse
                var numericPart = new string(value.Where(c => char.IsDigit(c) || c == '-').ToArray());
                return long.TryParse(numericPart, CultureInfo.InvariantCulture, out var result) ? result : 0;
            }
            var totalNonYield = dataList.Sum(d => ParseNonYieldTime(d.NonYieldingTimeMs));
            var maxNonYield = dataList.Max(d => ParseNonYieldTime(d.NonYieldingTimeMs));
            SchedulerIssuesTotalNonYieldText.Text = string.Format(CultureInfo.CurrentCulture, "{0:N0} ms", totalNonYield);
            SchedulerIssuesMaxNonYieldText.Text = string.Format(CultureInfo.CurrentCulture, "{0:N0} ms", maxNonYield);

            // Distinct schedulers affected
            var schedulersAffected = dataList.Select(d => d.SchedulerId).Distinct().Count();
            SchedulerIssuesSchedulersText.Text = schedulersAffected.ToString("N0", CultureInfo.CurrentCulture);

            // Offline events (IsOnline = false)
            var offlineCount = dataList.Count(d => d.IsOnline == false);
            SchedulerIssuesOfflineText.Text = offlineCount.ToString("N0", CultureInfo.CurrentCulture);

            // Color offline count red if > 0
            if (offlineCount > 0)
            {
                SchedulerIssuesOfflineText.Foreground = new System.Windows.Media.SolidColorBrush(
                    System.Windows.Media.Colors.Red);
            }
            else
            {
                SchedulerIssuesOfflineText.Foreground = new System.Windows.Media.SolidColorBrush(
                    (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));
            }
        }

        // SchedulerIssuesFilter_Click removed - grid removed per todo.md #13

        // ApplySchedulerIssuesFilters removed - grid removed per todo.md #13

        // UpdateSchedulerIssuesFilterButtonStyles removed - grid removed per todo.md #13

        // SchedulerIssuesFilterTextBox_TextChanged removed - grid removed per todo.md #13

        // SchedulerIssuesNumericFilterTextBox_TextChanged removed - grid removed per todo.md #13

        // SchedulerIssuesBoolFilter_Changed removed - grid removed per todo.md #13

        // ApplySchedulerIssuesFilter removed - grid removed per todo.md #13

        #endregion

        #region Memory Conditions Tab

        private async System.Threading.Tasks.Task RefreshMemoryConditionsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserMemoryConditionsAsync(_memoryConditionsHoursBack, _memoryConditionsFromDate, _memoryConditionsToDate);
                // Grid removed per todo.md #14 - chart only
                LoadMemoryConditionsChart(data, _memoryConditionsHoursBack, _memoryConditionsFromDate, _memoryConditionsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory conditions: {ex.Message}", ex);
            }
        }

        private void LoadMemoryConditionsChart(IEnumerable<HealthParserMemoryConditionItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryConditionsChart, out var existingPanel) && existingPanel != null)
            {
                MemoryConditionsChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryConditionsChart] = null;
            }
            MemoryConditionsChart.Plot.Clear();
            TabHelpers.ApplyDarkModeToChart(MemoryConditionsChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserMemoryConditionItem>();
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
                    double[] oomCounts = grouped.Select(g => (double)g.Sum(i => i.OutOfMemoryExceptions ?? 0)).ToArray();

                    if (oomCounts.Any(c => c > 0))
                    {
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, oomCounts.Select(c => c));
                        var scatter = MemoryConditionsChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = TabHelpers.ChartColors[3];
                        scatter.LegendText = "OOM Exceptions";
                        hasData = true;

                        _legendPanels[MemoryConditionsChart] = MemoryConditionsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                        MemoryConditionsChart.Plot.Legend.FontSize = 12;
                    }
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryConditionsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryConditionsChart.Plot.Axes.DateTimeTicksBottom();
            MemoryConditionsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryConditionsChart.Plot.YLabel("Count");
            MemoryConditionsChart.Plot.HideGrid();
            TabHelpers.LockChartVerticalAxis(MemoryConditionsChart);
            MemoryConditionsChart.Refresh();
        }

        // MemoryConditionsFilter_Click removed - grid removed per todo.md #14

        // ApplyMemoryConditionsFilters removed - grid removed per todo.md #14

        // UpdateMemoryConditionsFilterButtonStyles removed - grid removed per todo.md #14

        // MemoryConditionsFilterTextBox_TextChanged removed - grid removed per todo.md #14







        #endregion

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
            TabHelpers.ApplyDarkModeToChart(CPUTasksChart);

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

            CPUTasksChart.Plot.Axes.DateTimeTicksBottom();
            CPUTasksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            CPUTasksChart.Plot.YLabel("Workers");
            CPUTasksChart.Plot.HideGrid();
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

        #region Memory Broker Tab

        private async System.Threading.Tasks.Task RefreshMemoryBrokerAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserMemoryBrokerAsync(_memoryBrokerHoursBack, _memoryBrokerFromDate, _memoryBrokerToDate);
                _memoryBrokerUnfilteredData = data;
                MemoryBrokerDataGrid.ItemsSource = data;
                MemoryBrokerNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                LoadMemoryBrokerChart(data, _memoryBrokerHoursBack, _memoryBrokerFromDate, _memoryBrokerToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory broker: {ex.Message}", ex);
            }
        }

        private void LoadMemoryBrokerChart(IEnumerable<HealthParserMemoryBrokerItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            /* Clear both charts */
            foreach (var chart in new[] { MemoryBrokerChart, MemoryBrokerRatioChart })
            {
                if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
                {
                    chart.Plot.Axes.Remove(existingPanel);
                    _legendPanels[chart] = null;
                }
                chart.Plot.Clear();
                TabHelpers.ApplyDarkModeToChart(chart);
            }

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserMemoryBrokerItem>();
            bool hasAllocatedData = false;
            bool hasRatioData = false;

            if (dataList.Count > 0)
            {
                var colors = TabHelpers.ChartColors;

                /* Chart 1: Currently Allocated by Broker */
                var brokerGroups = dataList
                    .Where(d => d.CurrentlyAllocated.HasValue && !string.IsNullOrEmpty(d.Broker))
                    .GroupBy(d => d.Broker)
                    .ToList();

                int colorIndex = 0;
                foreach (var brokerGroup in brokerGroups)
                {
                    var brokerData = brokerGroup.OrderBy(d => d.EventTime!.Value).ToList();
                    if (brokerData.Count >= 1)
                    {
                        hasAllocatedData = true;
                        var timePoints = brokerData.Select(d => d.EventTime!.Value);
                        var values = brokerData.Select(d => (double)(d.CurrentlyAllocated ?? 0));
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                        var scatter = MemoryBrokerChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = colors[colorIndex % colors.Length];
                        scatter.LegendText = brokerGroup.Key.Length > 25 ? brokerGroup.Key.Substring(0, 25) + "..." : brokerGroup.Key;
                        colorIndex++;
                    }
                }

                if (hasAllocatedData)
                {
                    _legendPanels[MemoryBrokerChart] = MemoryBrokerChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryBrokerChart.Plot.Legend.FontSize = 12;
                }

                /* Chart 2: Memory Ratio and Overall over time */
                var ratioData = dataList.Where(d => d.MemoryRatio.HasValue).OrderBy(d => d.EventTime!.Value).ToList();
                var overallData = dataList.Where(d => d.Overall.HasValue).OrderBy(d => d.EventTime!.Value).ToList();

                if (ratioData.Count >= 1)
                {
                    hasRatioData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        ratioData.Select(d => d.EventTime!.Value),
                        ratioData.Select(d => (double)(d.MemoryRatio ?? 0)));

                    var scatter = MemoryBrokerRatioChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[0];
                    scatter.LegendText = "Memory Ratio";
                }

                if (overallData.Count >= 1)
                {
                    hasRatioData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        overallData.Select(d => d.EventTime!.Value),
                        overallData.Select(d => (double)(d.Overall ?? 0)));

                    var scatter = MemoryBrokerRatioChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[2];
                    scatter.LegendText = "Overall";
                }

                if (hasRatioData)
                {
                    _legendPanels[MemoryBrokerRatioChart] = MemoryBrokerRatioChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryBrokerRatioChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasAllocatedData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryBrokerChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            if (!hasRatioData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryBrokerRatioChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            /* Finalize both charts */
            foreach (var chart in new[] { MemoryBrokerChart, MemoryBrokerRatioChart })
            {
                chart.Plot.Axes.DateTimeTicksBottom();
                chart.Plot.Axes.SetLimitsX(xMin, xMax);
                chart.Plot.HideGrid();
                TabHelpers.LockChartVerticalAxis(chart);
                chart.Refresh();
            }

            MemoryBrokerChart.Plot.YLabel("Currently Allocated");
            MemoryBrokerRatioChart.Plot.YLabel("Value");
        }

        private void MemoryBrokerFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            ShowFilterPopup(button, columnName, "MemoryBroker", _memoryBrokerFilters,
                args => { },
                () => { });
        }

        private void ApplyMemoryBrokerFilters()
        {
            if (_memoryBrokerUnfilteredData == null)
            {
                _memoryBrokerUnfilteredData = MemoryBrokerDataGrid.ItemsSource as List<HealthParserMemoryBrokerItem>;
                if (_memoryBrokerUnfilteredData == null && MemoryBrokerDataGrid.ItemsSource != null)
                {
                    _memoryBrokerUnfilteredData = (MemoryBrokerDataGrid.ItemsSource as IEnumerable<HealthParserMemoryBrokerItem>)?.ToList();
                }
            }

            if (_memoryBrokerUnfilteredData == null) return;

            if (_memoryBrokerFilters.Count == 0)
            {
                MemoryBrokerDataGrid.ItemsSource = _memoryBrokerUnfilteredData;
                return;
            }

            var filteredData = _memoryBrokerUnfilteredData.Where(item =>
            {
                foreach (var filter in _memoryBrokerFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            MemoryBrokerDataGrid.ItemsSource = filteredData;
        }

        private void UpdateMemoryBrokerFilterButtonStyles()
        {
            foreach (var columnName in new[] { "CollectionTime", "Broker", "Notification", "MemoryRatio",
                "CurrentlyAllocated", "PreviouslyAllocated", "NewTarget", "Overall", "Rate", "DeltaTime", "BrokerId" })
            {
                UpdateFilterButtonStyle(MemoryBrokerDataGrid, columnName, _memoryBrokerFilters);
            }
        }

        private void MemoryBrokerFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(MemoryBrokerDataGrid, sender as TextBox);
        }

        private void MemoryBrokerNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(MemoryBrokerDataGrid, sender as TextBox);
        }

        #endregion

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
            TabHelpers.ApplyDarkModeToChart(MemoryNodeOOMChart);

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

            MemoryNodeOOMChart.Plot.Axes.DateTimeTicksBottom();
            MemoryNodeOOMChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryNodeOOMChart.Plot.YLabel("Event Count");
            MemoryNodeOOMChart.Plot.HideGrid();
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
            TabHelpers.ApplyDarkModeToChart(MemoryNodeOOMUtilChart);

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

            MemoryNodeOOMUtilChart.Plot.Axes.DateTimeTicksBottom();
            MemoryNodeOOMUtilChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryNodeOOMUtilChart.Plot.HideGrid();
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
            TabHelpers.ApplyDarkModeToChart(MemoryNodeOOMMemoryChart);

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

            MemoryNodeOOMMemoryChart.Plot.Axes.DateTimeTicksBottom();
            MemoryNodeOOMMemoryChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryNodeOOMMemoryChart.Plot.HideGrid();
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

        // Filtering logic moved to DataGridFilterService.ApplyFilter()

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

                    // Header row
                    var headers = grid.Columns.Select(c => c.Header?.ToString() ?? string.Empty);
                    sb.AppendLine(string.Join("\t", headers));

                    // Data rows
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
                        FileName = $"SystemEvents_Export_{DateTime.Now:yyyyMMdd_HHmmss}.csv"
                    };

                    if (dialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Header row
                            var headers = grid.Columns.Select(c => TabHelpers.EscapeCsvField(c.Header?.ToString() ?? string.Empty));
                            sb.AppendLine(string.Join(",", headers));

                            // Data rows
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
                                        values.Add(TabHelpers.EscapeCsvField(value));
                                    }
                                }
                                sb.AppendLine(string.Join(",", values));
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
    }
}

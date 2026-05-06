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

        // Chart hover tooltips
        private Helpers.ChartHoverHelper? _badPagesHover;
        private Helpers.ChartHoverHelper? _dumpRequestsHover;
        private Helpers.ChartHoverHelper? _accessViolationsHover;
        private Helpers.ChartHoverHelper? _writeAccessViolationsHover;
        private Helpers.ChartHoverHelper? _nonYieldingTasksHover;
        private Helpers.ChartHoverHelper? _latchWarningsHover;
        private Helpers.ChartHoverHelper? _sickSpinlocksHover;
        private Helpers.ChartHoverHelper? _cpuComparisonHover;
        private Helpers.ChartHoverHelper? _severeErrorsHover;
        private Helpers.ChartHoverHelper? _ioIssuesHover;
        private Helpers.ChartHoverHelper? _longestPendingIoHover;
        private Helpers.ChartHoverHelper? _schedulerIssuesHover;
        private Helpers.ChartHoverHelper? _memoryConditionsHover;
        private Helpers.ChartHoverHelper? _cpuTasksHover;
        private Helpers.ChartHoverHelper? _memoryBrokerHover;
        private Helpers.ChartHoverHelper? _memoryBrokerRatioHover;
        private Helpers.ChartHoverHelper? _memoryNodeOomHover;
        private Helpers.ChartHoverHelper? _memoryNodeOomUtilHover;
        private Helpers.ChartHoverHelper? _memoryNodeOomMemoryHover;

        public SystemEventsContent()
        {
            InitializeComponent();
            SetupChartContextMenus();
            Loaded += OnLoaded;
            Unloaded += OnUnloaded;
            Helpers.ThemeManager.ThemeChanged += OnThemeChanged;

            // Apply dark theme immediately so charts don't flash white before data loads
            TabHelpers.ApplyThemeToChart(BadPagesChart);
            TabHelpers.ApplyThemeToChart(DumpRequestsChart);
            TabHelpers.ApplyThemeToChart(AccessViolationsChart);
            TabHelpers.ApplyThemeToChart(WriteAccessViolationsChart);
            TabHelpers.ApplyThemeToChart(NonYieldingTasksChart);
            TabHelpers.ApplyThemeToChart(LatchWarningsChart);
            TabHelpers.ApplyThemeToChart(SickSpinlocksChart);
            TabHelpers.ApplyThemeToChart(CpuComparisonChart);
            TabHelpers.ApplyThemeToChart(SevereErrorsChart);
            TabHelpers.ApplyThemeToChart(IOIssuesChart);
            TabHelpers.ApplyThemeToChart(LongestPendingIOChart);
            TabHelpers.ApplyThemeToChart(SchedulerIssuesChart);
            TabHelpers.ApplyThemeToChart(MemoryConditionsChart);
            TabHelpers.ApplyThemeToChart(CPUTasksChart);
            TabHelpers.ApplyThemeToChart(MemoryBrokerChart);
            TabHelpers.ApplyThemeToChart(MemoryBrokerRatioChart);
            TabHelpers.ApplyThemeToChart(MemoryNodeOOMChart);
            TabHelpers.ApplyThemeToChart(MemoryNodeOOMUtilChart);
            TabHelpers.ApplyThemeToChart(MemoryNodeOOMMemoryChart);

            _badPagesHover = new Helpers.ChartHoverHelper(BadPagesChart, "events");
            _dumpRequestsHover = new Helpers.ChartHoverHelper(DumpRequestsChart, "events");
            _accessViolationsHover = new Helpers.ChartHoverHelper(AccessViolationsChart, "events");
            _writeAccessViolationsHover = new Helpers.ChartHoverHelper(WriteAccessViolationsChart, "events");
            _nonYieldingTasksHover = new Helpers.ChartHoverHelper(NonYieldingTasksChart, "events");
            _latchWarningsHover = new Helpers.ChartHoverHelper(LatchWarningsChart, "events");
            _sickSpinlocksHover = new Helpers.ChartHoverHelper(SickSpinlocksChart, "backoffs");
            _cpuComparisonHover = new Helpers.ChartHoverHelper(CpuComparisonChart, "%");
            _severeErrorsHover = new Helpers.ChartHoverHelper(SevereErrorsChart, "events");
            _ioIssuesHover = new Helpers.ChartHoverHelper(IOIssuesChart, "events");
            _longestPendingIoHover = new Helpers.ChartHoverHelper(LongestPendingIOChart, "ms");
            _schedulerIssuesHover = new Helpers.ChartHoverHelper(SchedulerIssuesChart, "ms");
            _memoryConditionsHover = new Helpers.ChartHoverHelper(MemoryConditionsChart, "events");
            _cpuTasksHover = new Helpers.ChartHoverHelper(CPUTasksChart, "workers");
            _memoryBrokerHover = new Helpers.ChartHoverHelper(MemoryBrokerChart, "");
            _memoryBrokerRatioHover = new Helpers.ChartHoverHelper(MemoryBrokerRatioChart, "");
            _memoryNodeOomHover = new Helpers.ChartHoverHelper(MemoryNodeOOMChart, "events");
            _memoryNodeOomUtilHover = new Helpers.ChartHoverHelper(MemoryNodeOOMUtilChart, "%");
            _memoryNodeOomMemoryHover = new Helpers.ChartHoverHelper(MemoryNodeOOMMemoryChart, "MB");
        }

        private void OnUnloaded(object sender, RoutedEventArgs e)
        {
            /* WPF fires Unloaded on every TabControl tab switch, not just on destruction.
               Unsubscribing ThemeManager or filter-popup events here breaks them on
               return to the tab (#916 family). Final cleanup happens via
               ServerTab.CleanupOnClose → DisposeChartHelpers. */
        }

        public void DisposeChartHelpers()
        {
            _badPagesHover?.Dispose();
            _dumpRequestsHover?.Dispose();
            _accessViolationsHover?.Dispose();
            _writeAccessViolationsHover?.Dispose();
            _nonYieldingTasksHover?.Dispose();
            _latchWarningsHover?.Dispose();
            _sickSpinlocksHover?.Dispose();
            _cpuComparisonHover?.Dispose();
            _severeErrorsHover?.Dispose();
            _ioIssuesHover?.Dispose();
            _longestPendingIoHover?.Dispose();
            _schedulerIssuesHover?.Dispose();
            _memoryConditionsHover?.Dispose();
            _cpuTasksHover?.Dispose();
            _memoryBrokerHover?.Dispose();
            _memoryBrokerRatioHover?.Dispose();
            _memoryNodeOomHover?.Dispose();
            _memoryNodeOomUtilHover?.Dispose();
            _memoryNodeOomMemoryHover?.Dispose();

            if (_filterPopupContent != null)
            {
                _filterPopupContent.FilterApplied -= FilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared -= FilterPopup_FilterCleared;
            }

            Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
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
        /// Refreshes system events data. When fullRefresh is false, only the visible sub-tab is refreshed.
        /// </summary>
        public async Task RefreshAllDataAsync(bool fullRefresh = true)
        {
            using var _ = Helpers.MethodProfiler.StartTiming("SystemEvents");
            if (_databaseService == null) return;

            try
            {
                if (fullRefresh)
                {
                    // Run all independent refreshes in parallel for initial load / manual refresh
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
                else
                {
                    // Only refresh the visible sub-tab
                    switch (SubTabControl.SelectedIndex)
                    {
                        case 0: // Corruption Events
                        case 1: // Contention Events — same data source
                            await RefreshSystemHealthAsync(); break;
                        case 2: await RefreshSevereErrorsAsync(); break;
                        case 3: await RefreshIOIssuesAsync(); break;
                        case 4: await RefreshSchedulerIssuesAsync(); break;
                        case 5: await RefreshMemoryConditionsAsync(); break;
                        case 6: await RefreshCPUTasksAsync(); break;
                        case 7: await RefreshMemoryBrokerAsync(); break;
                        case 8: await RefreshMemoryNodeOOMAsync(); break;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing system events data: {ex.Message}", ex);
            }
        }

    }
}

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
            Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
            Unloaded += (_, _) =>
            {
                Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
                DisposeChartHelpers();
            };

            // Apply dark theme immediately so charts don't flash white before data loads
            TabHelpers.ApplyThemeToChart(MemoryStatsOverviewChart);
            TabHelpers.ApplyThemeToChart(MemoryGrantSizingChart);
            TabHelpers.ApplyThemeToChart(MemoryGrantActivityChart);
            TabHelpers.ApplyThemeToChart(MemoryClerksChart);
            TabHelpers.ApplyThemeToChart(PlanCacheChart);
            TabHelpers.ApplyThemeToChart(MemoryPressureEventsChart);

            _memoryStatsOverviewHover = new Helpers.ChartHoverHelper(MemoryStatsOverviewChart, "MB");
            _memoryGrantSizingHover = new Helpers.ChartHoverHelper(MemoryGrantSizingChart, "MB");
            _memoryGrantActivityHover = new Helpers.ChartHoverHelper(MemoryGrantActivityChart, "count");
            _memoryClerksHover = new Helpers.ChartHoverHelper(MemoryClerksChart, "MB");
            _planCacheHover = new Helpers.ChartHoverHelper(PlanCacheChart, "MB");
            _memoryPressureEventsHover = new Helpers.ChartHoverHelper(MemoryPressureEventsChart, "events");
        }

        public void DisposeChartHelpers()
        {
            _memoryStatsOverviewHover?.Dispose();
            _memoryGrantSizingHover?.Dispose();
            _memoryGrantActivityHover?.Dispose();
            _memoryClerksHover?.Dispose();
            _planCacheHover?.Dispose();
            _memoryPressureEventsHover?.Dispose();
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            // No grids to configure - all tabs are chart-only now
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
            // Memory Stats Overview chart
            var memOverviewMenu = TabHelpers.SetupChartContextMenu(MemoryStatsOverviewChart, "Memory_Stats_Overview", "collect.memory_stats");
            AddDrillDown(MemoryStatsOverviewChart, memOverviewMenu, () => _memoryStatsOverviewHover, "Show Active Queries at This Time", "Memory");

            // Memory Grant charts
            var grantSizingMenu = TabHelpers.SetupChartContextMenu(MemoryGrantSizingChart, "Memory_Grant_Sizing", "collect.memory_grant_stats");
            AddDrillDown(MemoryGrantSizingChart, grantSizingMenu, () => _memoryGrantSizingHover, "Show Active Queries at This Time", "MemoryGrant");
            var grantActivityMenu = TabHelpers.SetupChartContextMenu(MemoryGrantActivityChart, "Memory_Grant_Activity", "collect.memory_grant_stats");
            AddDrillDown(MemoryGrantActivityChart, grantActivityMenu, () => _memoryGrantActivityHover, "Show Active Queries at This Time", "MemoryGrant");

            // Memory Clerks chart
            var clerksMenu = TabHelpers.SetupChartContextMenu(MemoryClerksChart, "Memory_Clerks", "collect.memory_clerks_stats");
            AddDrillDown(MemoryClerksChart, clerksMenu, () => _memoryClerksHover, "Show Active Queries at This Time", "MemoryClerks");

            // Plan Cache chart
            TabHelpers.SetupChartContextMenu(PlanCacheChart, "Plan_Cache", "collect.plan_cache_stats");

            // Memory Pressure Events chart
            var pressureMenu = TabHelpers.SetupChartContextMenu(MemoryPressureEventsChart, "Memory_Pressure_Events", "collect.memory_pressure_events");
            AddDrillDown(MemoryPressureEventsChart, pressureMenu, () => _memoryPressureEventsHover, "Show Active Queries at This Time", "MemoryPressure");
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
        /// Refreshes memory data. When fullRefresh is false, only the visible sub-tab is refreshed.
        /// </summary>
        public async Task RefreshAllDataAsync(bool fullRefresh = true)
        {
            try
            {
                using var _ = Helpers.MethodProfiler.StartTiming("Memory");

                if (fullRefresh)
                {
                    // Run all independent refreshes in parallel for initial load / manual refresh
                    await Task.WhenAll(
                        RefreshMemoryStatsAsync(),
                        RefreshMemoryGrantsAsync(),
                        RefreshMemoryClerksAsync(),
                        RefreshPlanCacheAsync(),
                        RefreshMemoryPressureEventsAsync()
                    );
                }
                else
                {
                    // Only refresh the visible sub-tab
                    switch (SubTabControl.SelectedIndex)
                    {
                        case 0: await RefreshMemoryStatsAsync(); break;
                        case 1: await RefreshMemoryGrantsAsync(); break;
                        case 2: await RefreshMemoryClerksAsync(); break;
                        case 3: await RefreshPlanCacheAsync(); break;
                        case 4: await RefreshMemoryPressureEventsAsync(); break;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Memory data: {ex.Message}", ex);
            }
        }
    }
}

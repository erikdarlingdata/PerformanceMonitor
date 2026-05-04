/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        // Blocking date range filtering state
        private int _blockingHoursBack = 24;
        private DateTime? _blockingFromDate = null;
        private DateTime? _blockingToDate = null;

        // Deadlocks date range filtering state
        private int _deadlocksHoursBack = 24;
        private DateTime? _deadlocksFromDate = null;
        private DateTime? _deadlocksToDate = null;

        // Blocking/Deadlock Stats date range filtering state
        private int _blockingStatsHoursBack = 24;
        private DateTime? _blockingStatsFromDate = null;
        private DateTime? _blockingStatsToDate = null;

        // Collection Health date range filtering state
        private int _collectionHealthHoursBack = 24;
        private DateTime? _collectionHealthFromDate = null;
        private DateTime? _collectionHealthToDate = null;

        // Resource Overview date range filtering state
        private int _resourceOverviewHoursBack = 24;
        private DateTime? _resourceOverviewFromDate = null;
        private DateTime? _resourceOverviewToDate = null;

        /// <summary>
        /// Loads data for the Dashboard. When fullRefresh is true (first load, manual refresh,
        /// Apply to All), all tabs are refreshed in parallel. When false (auto-refresh timer tick),
        /// only the currently visible tab is refreshed to reduce SQL Server load.
        /// </summary>
        private async Task LoadDataAsync(bool fullRefresh = true)
        {
            if (_isRefreshing)
            {
                // If a previous refresh has been running for over 2 minutes, it's stuck — allow a new one
                if ((DateTime.UtcNow - _refreshStartedUtc).TotalMinutes < 2) return;
                Logger.Error($"Previous refresh appears stuck (started {_refreshStartedUtc:HH:mm:ss}), allowing new refresh");
            }
            _isRefreshing = true;
            _refreshStartedUtc = DateTime.UtcNow;

            using var _ = Helpers.MethodProfiler.StartTiming("ServerTab");
            try
            {
                StatusText.Text = GetLoadingMessage();
                RefreshButton.IsEnabled = false;

                bool connected = await _databaseService.TestConnectionAsync();
                if (!connected)
                {
                    StatusText.Text = $"Failed to connect to {_serverConnection.DisplayName}";
                    if (fullRefresh)
                    {
                        MessageBox.Show(
                            $"Could not connect to SQL Server: {_serverConnection.ServerName}\n\nCheck connection settings",
                            "Connection Error",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error
                        );
                    }
                    else
                    {
                        Logger.Error($"Auto-refresh connection failed for {_serverConnection.DisplayName}");
                    }
                    return;
                }

                StatusText.Text = GetLoadingMessage();

                if (fullRefresh)
                {
                    // Full refresh: query all tabs in parallel (first load, manual refresh, Apply to All)
                    await RefreshAllTabsAsync();
                }
                else
                {
                    // Timer tick: only refresh the currently visible tab
                    await RefreshVisibleTabAsync();
                }

                StatusText.Text = "Ready";
                FooterText.Text = $"Last refresh: {DateTime.Now:yyyy-MM-dd HH:mm:ss} | Server: {_serverConnection.DisplayName}";
            }
            catch (Exception ex)
            {
                StatusText.Text = "Error loading data";
                if (fullRefresh)
                {
                    MessageBox.Show(
                        $"Error loading data:\n\n{ex.Message}",
                        "Error",
                        MessageBoxButton.OK,
                        MessageBoxImage.Error
                    );
                }
                else
                {
                    Logger.Error($"Auto-refresh error for {_serverConnection.DisplayName}: {ex.Message}", ex);
                }
            }
            finally
            {
                RefreshButton.IsEnabled = true;
                _isRefreshing = false;
            }
        }

        // ====================================================================
        // Per-Tab Refresh Methods
        // ====================================================================

        /// <summary>
        /// Refreshes all tabs in parallel — used on first load, manual refresh, and Apply to All.
        /// </summary>
        private async Task RefreshAllTabsAsync()
        {
            var overviewTask = RefreshOverviewTabAsync();
            var queriesTask = RefreshQueriesTabAsync();
            var resourceMetricsTask = RefreshResourceMetricsTabAsync();
            var memoryTask = RefreshMemoryTabAsync();
            var lockingTask = RefreshLockingTabAsync();
            var systemEventsTask = RefreshSystemEventsTabAsync();

            await Task.WhenAll(overviewTask, queriesTask, resourceMetricsTask, memoryTask, lockingTask, systemEventsTask);
        }

        /// <summary>
        /// Refreshes only the currently visible tab. On first visit to a tab,
        /// does a full refresh so all sub-tabs are populated. Subsequent visits
        /// only refresh the active sub-tab for speed.
        /// </summary>
        private async Task RefreshVisibleTabAsync()
        {
            var selectedTab = DataTabControl.SelectedItem as TabItem;
            if (selectedTab == null) return;

            var tabHeader = GetTabHeaderText(selectedTab);
            bool firstVisit = _initializedTabs.Add(tabHeader);

            switch (tabHeader)
            {
                case "Overview":
                    await RefreshOverviewTabAsync();
                    break;
                case "Queries":
                    await RefreshQueriesTabAsync(fullRefresh: firstVisit);
                    break;
                case "Resource Metrics":
                    await RefreshResourceMetricsTabAsync(fullRefresh: firstVisit);
                    break;
                case "Memory":
                    await RefreshMemoryTabAsync(fullRefresh: firstVisit);
                    break;
                case "Locking":
                    await RefreshLockingTabAsync();
                    break;
                case "System Events":
                    await RefreshSystemEventsTabAsync(fullRefresh: firstVisit);
                    break;
                // Plan Viewer has no data to refresh
            }
        }

        /// <summary>
        /// Refreshes the Overview tab: Collection Health, Duration Trends, Daily Summary,
        /// Critical Issues, Default Trace, Current Config, Config Changes, Resource Overview, Running Jobs.
        /// </summary>
        private async Task RefreshOverviewTabAsync()
        {
            try
            {
                var healthTask = _databaseService.GetCollectionHealthAsync();
                var durationLogsTask = _databaseService.GetCollectionDurationLogsAsync();
                var resourceOverviewTask = RefreshResourceOverviewAsync();
                var runningJobsTask = RefreshRunningJobsAsync();
                var dailySummaryTask = DailySummaryTab.RefreshDataAsync();
                var criticalIssuesTask = CriticalIssuesTab.RefreshDataAsync();
                var defaultTraceTask = DefaultTraceTab.RefreshAllDataAsync();
                var currentConfigTask = CurrentConfigTab.RefreshAllDataAsync();
                var configChangesTask = ConfigChangesTab.RefreshAllDataAsync();

                await Task.WhenAll(healthTask, durationLogsTask, resourceOverviewTask, runningJobsTask,
                    dailySummaryTask, criticalIssuesTask, defaultTraceTask, currentConfigTask, configChangesTask);

                var healthData = await healthTask;
                HealthDataGrid.ItemsSource = healthData;
                UpdateDataGridFilterButtonStyles(HealthDataGrid, _collectionHealthFilters);
                HealthNoDataMessage.Visibility = healthData.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                var durationLogs = await durationLogsTask;
                UpdateCollectorDurationChart(durationLogs);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Overview tab: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Refreshes the Queries tab (delegated to QueryPerformanceContent UserControl).
        /// </summary>
        private async Task RefreshQueriesTabAsync(bool fullRefresh = true)
        {
            try
            {
                PerformanceTab.IsRefreshing = true;
                try { await PerformanceTab.RefreshAllDataAsync(fullRefresh); }
                finally { PerformanceTab.IsRefreshing = false; }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Queries tab: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Refreshes the Resource Metrics tab (delegated to ResourceMetricsContent UserControl).
        /// </summary>
        private async Task RefreshResourceMetricsTabAsync(bool fullRefresh = true)
        {
            try
            {
                await ResourceMetricsContent.RefreshAllDataAsync(fullRefresh);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Resource Metrics tab: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Refreshes the Memory tab (delegated to MemoryContent UserControl).
        /// </summary>
        private async Task RefreshMemoryTabAsync(bool fullRefresh = true)
        {
            try
            {
                await MemoryTab.RefreshAllDataAsync(fullRefresh);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Memory tab: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Refreshes the Locking tab: Blocking events, deadlocks, blocking/deadlock stats,
        /// lock wait stats, current waits duration, and current waits blocked sessions.
        /// </summary>
        private async Task RefreshLockingTabAsync()
        {
            try
            {
                var blockingEventsTask = _databaseService.GetBlockingEventsAsync();
                var deadlocksTask = _databaseService.GetDeadlocksAsync();
                var blockingStatsTask = _databaseService.GetBlockingDeadlockStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var lockWaitStatsTask = _databaseService.GetLockWaitStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var currentWaitsDurationTask = _databaseService.GetWaitingTaskTrendAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var currentWaitsBlockedTask = _databaseService.GetBlockedSessionTrendAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);

                await Task.WhenAll(blockingEventsTask, deadlocksTask, blockingStatsTask, lockWaitStatsTask, currentWaitsDurationTask, currentWaitsBlockedTask);

                try
                {
                    var blockingEvents = await blockingEventsTask;
                    BlockingEventsDataGrid.ItemsSource = blockingEvents;
                    UpdateDataGridFilterButtonStyles(BlockingEventsDataGrid, _blockingEventsFilters);
                    BlockingEventsNoDataMessage.Visibility = blockingEvents.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                }
                catch (Exception blockingEx)
                {
                    Logger.Warning($"Could not load blocking events: {blockingEx.Message}");
                }

                try
                {
                    var deadlocks = await deadlocksTask;
                    DeadlocksDataGrid.ItemsSource = deadlocks;
                    UpdateDataGridFilterButtonStyles(DeadlocksDataGrid, _deadlocksFilters);
                    DeadlocksNoDataMessage.Visibility = deadlocks.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                }
                catch (Exception deadlockEx)
                {
                    Logger.Warning($"Could not load deadlocks: {deadlockEx.Message}");
                }

                _ = LoadBlockingSlicerAsync();
                _ = LoadDeadlockSlicerAsync();

                try
                {
                    var blockingStats = await blockingStatsTask;
                    var lockWaitStats = await lockWaitStatsTask;
                    LoadBlockingStatsCharts(blockingStats, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                    LoadLockWaitStatsChart(lockWaitStats, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                    var currentWaitsDuration = await currentWaitsDurationTask;
                    var currentWaitsBlocked = await currentWaitsBlockedTask;
                    LoadCurrentWaitsDurationChart(currentWaitsDuration, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                    LoadCurrentWaitsBlockedChart(currentWaitsBlocked, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                }
                catch (Exception blockingStatsEx)
                {
                    Logger.Warning($"Could not load blocking/deadlock stats: {blockingStatsEx.Message}");
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Locking tab: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Refreshes the System Events tab (delegated to SystemEventsContent UserControl).
        /// </summary>
        private async Task RefreshSystemEventsTabAsync(bool fullRefresh = true)
        {
            try
            {
                await SystemEventsContent.RefreshAllDataAsync(fullRefresh);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing System Events tab: {ex.Message}", ex);
            }
        }

        private async Task RefreshResourceOverviewAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Load all four charts in parallel
                var cpuTask = _databaseService.GetCpuDataAsync(_resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
                var memoryTask = _databaseService.GetMemoryDataAsync(_resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
                var ioTask = _databaseService.GetFileIoDataAsync(_resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
                var waitTask = _databaseService.GetWaitStatsDataAsync(_resourceOverviewHoursBack, 5, _resourceOverviewFromDate, _resourceOverviewToDate);

                await Task.WhenAll(cpuTask, memoryTask, ioTask, waitTask);

                // Load CPU chart
                LoadResourceOverviewCpuChart(await cpuTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);

                // Load Memory chart
                LoadResourceOverviewMemoryChart(await memoryTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);

                // Load I/O chart
                LoadResourceOverviewIoChart(await ioTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);

                // Load Wait Stats chart
                LoadResourceOverviewWaitChart(await waitTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
            }
            catch (Exception ex)
            {
                Logger.Error("Error refreshing Resource Overview charts", ex);
            }
        }

        private async Task RefreshRunningJobsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var runningJobs = await _databaseService.GetRunningJobsAsync();
                RunningJobsDataGrid.ItemsSource = runningJobs;
                RunningJobsNoDataMessage.Visibility = runningJobs.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Warning($"Could not load running jobs: {ex.Message}");
                RunningJobsNoDataMessage.Visibility = Visibility.Visible;
            }
        }
    }
}

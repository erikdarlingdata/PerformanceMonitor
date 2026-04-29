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
        // ── Blocking Slicer ──

        private async Task LoadBlockingSlicerAsync()
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetBlockingSlicerDataAsync(
                    _blockingHoursBack, _blockingFromDate, _blockingToDate);
                var (start, end) = GetLockingSlicerTimeRange(_blockingHoursBack, _blockingFromDate, _blockingToDate);
                if (data.Count > 0)
                    BlockingSlicer.LoadData(data, "Blocking Events", start, end);
            }
            catch { }
        }

        private async void OnBlockingSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetBlockingEventsAsync(0, e.Start, e.End);
                BlockingEventsDataGrid.ItemsSource = data;
                UpdateDataGridFilterButtonStyles(BlockingEventsDataGrid, _blockingEventsFilters);
                BlockingEventsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch { }
        }

        // ── Deadlock Slicer ──

        private async Task LoadDeadlockSlicerAsync()
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetDeadlockSlicerDataAsync(
                    _deadlocksHoursBack, _deadlocksFromDate, _deadlocksToDate);
                var (start, end) = GetLockingSlicerTimeRange(_deadlocksHoursBack, _deadlocksFromDate, _deadlocksToDate);
                if (data.Count > 0)
                    DeadlockSlicer.LoadData(data, "Deadlocks", start, end);
            }
            catch { }
        }

        private async void OnDeadlockSlicerChanged(object? sender, Controls.SlicerRangeEventArgs e)
        {
            if (_databaseService == null) return;
            try
            {
                var data = await _databaseService.GetDeadlocksAsync(0, e.Start, e.End);
                DeadlocksDataGrid.ItemsSource = data;
                UpdateDataGridFilterButtonStyles(DeadlocksDataGrid, _deadlocksFilters);
                DeadlocksNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch { }
        }

        private static (DateTime start, DateTime end) GetLockingSlicerTimeRange(
            int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            if (fromDate.HasValue && toDate.HasValue)
                return (fromDate.Value, toDate.Value);
            var serverNow = Helpers.ServerTimeHelper.ServerNow;
            return (serverNow.AddHours(-hoursBack), serverNow);
        }

        // ====================================================================
        // Deadlocks Date Range Filtering
        // ====================================================================

        private async void Deadlocks_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = GetLoadingMessage();
                var deadlocks = await _databaseService.GetDeadlocksAsync(_deadlocksHoursBack, _deadlocksFromDate, _deadlocksToDate);
                DeadlocksDataGrid.ItemsSource = deadlocks;
                DeadlocksNoDataMessage.Visibility = deadlocks.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = $"Loaded {deadlocks.Count} deadlocks";
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error refreshing deadlocks:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                StatusText.Text = "Error refreshing deadlocks";
            }
        }

        // ====================================================================
        // Blocking/Deadlock Stats Tab Handlers
        // ====================================================================

        private async void BlockingStats_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = GetLoadingMessage();

                var blockingStatsTask = _databaseService.GetBlockingDeadlockStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var lockWaitStatsTask = _databaseService.GetLockWaitStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var currentWaitsDurationTask = _databaseService.GetWaitingTaskTrendAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var currentWaitsBlockedTask = _databaseService.GetBlockedSessionTrendAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                await Task.WhenAll(blockingStatsTask, lockWaitStatsTask, currentWaitsDurationTask, currentWaitsBlockedTask);

                var data = await blockingStatsTask;
                var lockWaitStats = await lockWaitStatsTask;

                // Load charts with explicit time range for proper axis scaling
                LoadBlockingStatsCharts(data, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                LoadLockWaitStatsChart(lockWaitStats, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                LoadCurrentWaitsDurationChart(await currentWaitsDurationTask, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                LoadCurrentWaitsBlockedChart(await currentWaitsBlockedTask, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                StatusText.Text = $"Loaded {data.Count} blocking/deadlock stats records";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading blocking/deadlock stats: {ex.Message}");
                StatusText.Text = $"Error loading blocking/deadlock stats";
            }
        }

        // ====================================================================
        // Blocking Refresh Handler
        // ====================================================================

        private async void Blocking_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Refreshing blocking events...";
                var blocking = await _databaseService.GetBlockingEventsAsync(_blockingHoursBack, _blockingFromDate, _blockingToDate);
                BlockingEventsDataGrid.ItemsSource = blocking;
                BlockingEventsNoDataMessage.Visibility = blocking.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = $"Loaded {blocking.Count} blocking events";
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error refreshing blocking events:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                StatusText.Text = "Error refreshing blocking events";
            }
        }

        // ====================================================================
        // Collection Health
        // ====================================================================

        private async void CollectionHealth_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Refreshing collection health...";
                var healthData = await _databaseService.GetCollectionHealthAsync();
                HealthDataGrid.ItemsSource = healthData;
                HealthNoDataMessage.Visibility = healthData.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = "Ready";
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error refreshing collection health:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                StatusText.Text = "Error";
            }
        }
    }
}

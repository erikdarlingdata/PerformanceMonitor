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
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class DefaultTraceContent : UserControl
    {
        private DatabaseService? _databaseService;

        private int _defaultTraceEventsHoursBack = 24;
        private DateTime? _defaultTraceEventsFromDate;
        private DateTime? _defaultTraceEventsToDate;
        private string? _defaultTraceEventsFilter;

        private int _traceAnalysisHoursBack = 24;
        private DateTime? _traceAnalysisFromDate;
        private DateTime? _traceAnalysisToDate;

        public DefaultTraceContent()
        {
            InitializeComponent();
        }

        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService;
        }

        public void SetTimeRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            _defaultTraceEventsHoursBack = hoursBack;
            _defaultTraceEventsFromDate = fromDate;
            _defaultTraceEventsToDate = toDate;

            _traceAnalysisHoursBack = hoursBack;
            _traceAnalysisFromDate = fromDate;
            _traceAnalysisToDate = toDate;
        }

        public async Task RefreshAllDataAsync()
        {
            if (_databaseService == null) return;

            await Task.WhenAll(
                RefreshDefaultTraceEventsAsync(),
                RefreshTraceAnalysisAsync()
            );
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(DefaultTraceEventsDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(TraceAnalysisDataGrid);
            TabHelpers.FreezeColumns(DefaultTraceEventsDataGrid, 1);
            TabHelpers.FreezeColumns(TraceAnalysisDataGrid, 1);
        }

        #region Default Trace Events

        private void DefaultTraceEventsFilter_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (DefaultTraceEventsFilterCombo.SelectedItem is ComboBoxItem selected)
            {
                _defaultTraceEventsFilter = selected.Tag?.ToString();
                if (string.IsNullOrEmpty(_defaultTraceEventsFilter))
                {
                    _defaultTraceEventsFilter = null;
                }
                DefaultTraceEvents_Refresh_Click(sender, new RoutedEventArgs());
            }
        }

        private async void DefaultTraceEvents_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                await RefreshDefaultTraceEventsAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Default Trace Events data: {ex.Message}", ex);
            }
        }

        private async Task RefreshDefaultTraceEventsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetDefaultTraceEventsAsync(_defaultTraceEventsHoursBack, _defaultTraceEventsFromDate, _defaultTraceEventsToDate, _defaultTraceEventsFilter);
                DefaultTraceEventsDataGrid.ItemsSource = data;
                DefaultTraceEventsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading default trace events: {ex.Message}");
            }
        }

        private void DefaultTraceEventsFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(DefaultTraceEventsDataGrid, sender as TextBox);
        }

        private void DefaultTraceEventsNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(DefaultTraceEventsDataGrid, sender as TextBox);
        }

        #endregion

        #region Trace Analysis

        private async Task RefreshTraceAnalysisAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetTraceAnalysisAsync(_traceAnalysisHoursBack, _traceAnalysisFromDate, _traceAnalysisToDate);
                TraceAnalysisDataGrid.ItemsSource = data;
                TraceAnalysisNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading trace analysis: {ex.Message}");
            }
        }

        private void TraceAnalysisFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(TraceAnalysisDataGrid, sender as TextBox);
        }

        private void TraceAnalysisNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(TraceAnalysisDataGrid, sender as TextBox);
        }

        #endregion
    }
}

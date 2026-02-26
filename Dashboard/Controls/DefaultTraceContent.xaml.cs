/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
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

        // Popup filter state
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        private readonly Dictionary<string, ColumnFilterState> _defaultTraceFilters = new();
        private List<DefaultTraceEventItem>? _defaultTraceUnfilteredData;

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
        }

        public async Task RefreshAllDataAsync()
        {
            if (_databaseService == null) return;

            await RefreshDefaultTraceEventsAsync();
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(DefaultTraceEventsDataGrid);
            TabHelpers.FreezeColumns(DefaultTraceEventsDataGrid, 1);
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
                _defaultTraceUnfilteredData = data;
                _defaultTraceFilters.Clear();
                DefaultTraceEventsDataGrid.ItemsSource = data;
                DefaultTraceEventsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                UpdateFilterButtonStyles(DefaultTraceEventsDataGrid, _defaultTraceFilters);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading default trace events: {ex.Message}");
            }
        }

        #endregion

        #region Popup Filter Infrastructure

        private void DefaultTraceFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.Tag is string columnName)
                ShowFilterPopup(button, columnName, _defaultTraceFilters);
        }

        private void ShowFilterPopup(Button button, string columnName, Dictionary<string, ColumnFilterState> filters)
        {
            if (_filterPopup == null)
            {
                _filterPopupContent = new ColumnFilterPopup();
                _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

                _filterPopup = new Popup
                {
                    Child = _filterPopupContent,
                    StaysOpen = false,
                    Placement = PlacementMode.Bottom,
                    AllowsTransparency = true
                };
            }

            filters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);
            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
                _defaultTraceFilters[e.FilterState.ColumnName] = e.FilterState;
            else
                _defaultTraceFilters.Remove(e.FilterState.ColumnName);
            ApplyFilters(_defaultTraceFilters, _defaultTraceUnfilteredData, DefaultTraceEventsDataGrid, DefaultTraceEventsNoDataMessage);
            UpdateFilterButtonStyles(DefaultTraceEventsDataGrid, _defaultTraceFilters);
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private static void ApplyFilters<T>(Dictionary<string, ColumnFilterState> filters, List<T>? unfilteredData, DataGrid grid, TextBlock noDataMessage)
        {
            if (unfilteredData == null) return;

            if (filters.Count == 0)
            {
                grid.ItemsSource = unfilteredData;
                noDataMessage.Visibility = unfilteredData.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                return;
            }

            var filtered = unfilteredData.Where(item =>
            {
                foreach (var filter in filters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item!, filter))
                        return false;
                }
                return true;
            }).ToList();

            grid.ItemsSource = filtered;
            noDataMessage.Visibility = filtered.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }

        private void UpdateFilterButtonStyles(DataGrid grid, Dictionary<string, ColumnFilterState> filters)
        {
            foreach (var column in grid.Columns)
            {
                if (column.Header is StackPanel stackPanel)
                {
                    var filterButton = stackPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        var textBlock = new TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00))
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF))
                        };
                        filterButton.Content = textBlock;
                        filterButton.ToolTip = hasActiveFilter && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                    }
                }
            }
        }

        #endregion
    }
}

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
    public partial class ConfigChangesContent : UserControl
    {
        private DatabaseService? _databaseService;

        private int _hoursBack = 24;
        private DateTime? _fromDate;
        private DateTime? _toDate;

        // Popup filter state
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;
        private string? _activeFilterGrid;

        private readonly Dictionary<string, ColumnFilterState> _serverConfigChangesFilters = new();
        private List<ServerConfigChangeItem>? _serverConfigChangesUnfilteredData;

        private readonly Dictionary<string, ColumnFilterState> _dbConfigChangesFilters = new();
        private List<DatabaseConfigChangeItem>? _dbConfigChangesUnfilteredData;

        private readonly Dictionary<string, ColumnFilterState> _traceFlagChangesFilters = new();
        private List<TraceFlagChangeItem>? _traceFlagChangesUnfilteredData;

        public ConfigChangesContent()
        {
            InitializeComponent();
        }

        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService;
        }

        public void SetTimeRange(int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            _hoursBack = hoursBack;
            _fromDate = fromDate;
            _toDate = toDate;
        }

        public async Task RefreshAllDataAsync()
        {
            if (_databaseService == null) return;

            await Task.WhenAll(
                RefreshServerConfigChangesAsync(),
                RefreshDbConfigChangesAsync(),
                RefreshTraceFlagChangesAsync()
            );
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(ServerConfigChangesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(DatabaseConfigChangesDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(TraceFlagChangesDataGrid);
            TabHelpers.FreezeColumns(ServerConfigChangesDataGrid, 1);
            TabHelpers.FreezeColumns(DatabaseConfigChangesDataGrid, 2);
            TabHelpers.FreezeColumns(TraceFlagChangesDataGrid, 1);
        }

        #region Server Configuration Changes

        private async Task RefreshServerConfigChangesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetServerConfigChangesAsync(_hoursBack, _fromDate, _toDate);
                _serverConfigChangesUnfilteredData = data;
                _serverConfigChangesFilters.Clear();
                ServerConfigChangesDataGrid.ItemsSource = data;
                ServerConfigChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                UpdateFilterButtonStyles(ServerConfigChangesDataGrid, _serverConfigChangesFilters);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading server configuration changes: {ex.Message}");
            }
        }

        private void ServerConfigChangesBoolFilter_Changed(object sender, SelectionChangedEventArgs e)
        {
            ApplyServerConfigChangesFilters();
        }

        private void ApplyServerConfigChangesFilters()
        {
            if (_serverConfigChangesUnfilteredData == null) return;

            var restartFilter = (ServerConfigChangesRestartFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();
            var dynamicFilter = (ServerConfigChangesDynamicFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();
            var advancedFilter = (ServerConfigChangesAdvancedFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();

            if (_serverConfigChangesFilters.Count == 0 && restartFilter == "All" && dynamicFilter == "All" && advancedFilter == "All")
            {
                ServerConfigChangesDataGrid.ItemsSource = _serverConfigChangesUnfilteredData;
                return;
            }

            var filteredData = _serverConfigChangesUnfilteredData.Where(item =>
            {
                foreach (var filter in _serverConfigChangesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                        return false;
                }

                if (restartFilter != "All" && restartFilter != null)
                {
                    bool expected = restartFilter == "True";
                    if (item.RequiresRestart != expected) return false;
                }
                if (dynamicFilter != "All" && dynamicFilter != null)
                {
                    bool expected = dynamicFilter == "True";
                    if (item.IsDynamic != expected) return false;
                }
                if (advancedFilter != "All" && advancedFilter != null)
                {
                    bool expected = advancedFilter == "True";
                    if (item.IsAdvanced != expected) return false;
                }

                return true;
            }).ToList();

            ServerConfigChangesDataGrid.ItemsSource = filteredData;
        }

        #endregion

        #region Database Configuration Changes

        private async Task RefreshDbConfigChangesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetDatabaseConfigChangesAsync(_hoursBack, _fromDate, _toDate);
                _dbConfigChangesUnfilteredData = data;
                _dbConfigChangesFilters.Clear();
                DatabaseConfigChangesDataGrid.ItemsSource = data;
                DatabaseConfigChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                UpdateFilterButtonStyles(DatabaseConfigChangesDataGrid, _dbConfigChangesFilters);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database configuration changes: {ex.Message}");
            }
        }

        private void ApplyDbConfigChangesFilters()
        {
            if (_dbConfigChangesUnfilteredData == null) return;

            if (_dbConfigChangesFilters.Count == 0)
            {
                DatabaseConfigChangesDataGrid.ItemsSource = _dbConfigChangesUnfilteredData;
                return;
            }

            var filteredData = _dbConfigChangesUnfilteredData.Where(item =>
            {
                foreach (var filter in _dbConfigChangesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                        return false;
                }
                return true;
            }).ToList();

            DatabaseConfigChangesDataGrid.ItemsSource = filteredData;
        }

        #endregion

        #region Trace Flag Changes

        private async Task RefreshTraceFlagChangesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetTraceFlagChangesAsync(_hoursBack, _fromDate, _toDate);
                _traceFlagChangesUnfilteredData = data;
                _traceFlagChangesFilters.Clear();
                TraceFlagChangesDataGrid.ItemsSource = data;
                TraceFlagChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                UpdateFilterButtonStyles(TraceFlagChangesDataGrid, _traceFlagChangesFilters);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading trace flag changes: {ex.Message}");
            }
        }

        private void TraceFlagChangesBoolFilter_Changed(object sender, SelectionChangedEventArgs e)
        {
            ApplyTraceFlagChangesFilters();
        }

        private void ApplyTraceFlagChangesFilters()
        {
            if (_traceFlagChangesUnfilteredData == null) return;

            var globalFilter = (TraceFlagChangesGlobalFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();
            var sessionFilter = (TraceFlagChangesSessionFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();

            if (_traceFlagChangesFilters.Count == 0 && globalFilter == "All" && sessionFilter == "All")
            {
                TraceFlagChangesDataGrid.ItemsSource = _traceFlagChangesUnfilteredData;
                return;
            }

            var filteredData = _traceFlagChangesUnfilteredData.Where(item =>
            {
                foreach (var filter in _traceFlagChangesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                        return false;
                }

                if (globalFilter != "All" && globalFilter != null)
                {
                    bool expected = globalFilter == "True";
                    if (item.IsGlobal != expected) return false;
                }
                if (sessionFilter != "All" && sessionFilter != null)
                {
                    bool expected = sessionFilter == "True";
                    if (item.IsSession != expected) return false;
                }

                return true;
            }).ToList();

            TraceFlagChangesDataGrid.ItemsSource = filteredData;
        }

        #endregion

        #region Popup Filter Infrastructure

        private void ServerConfigChangesFilter_Click(object sender, RoutedEventArgs e)
        {
            _activeFilterGrid = "ServerConfigChanges";
            if (sender is Button button && button.Tag is string columnName)
                ShowFilterPopup(button, columnName, _serverConfigChangesFilters);
        }

        private void DbConfigChangesFilter_Click(object sender, RoutedEventArgs e)
        {
            _activeFilterGrid = "DbConfigChanges";
            if (sender is Button button && button.Tag is string columnName)
                ShowFilterPopup(button, columnName, _dbConfigChangesFilters);
        }

        private void TraceFlagChangesFilter_Click(object sender, RoutedEventArgs e)
        {
            _activeFilterGrid = "TraceFlagChanges";
            if (sender is Button button && button.Tag is string columnName)
                ShowFilterPopup(button, columnName, _traceFlagChangesFilters);
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

            switch (_activeFilterGrid)
            {
                case "ServerConfigChanges":
                    if (e.FilterState.IsActive)
                        _serverConfigChangesFilters[e.FilterState.ColumnName] = e.FilterState;
                    else
                        _serverConfigChangesFilters.Remove(e.FilterState.ColumnName);
                    ApplyServerConfigChangesFilters();
                    UpdateFilterButtonStyles(ServerConfigChangesDataGrid, _serverConfigChangesFilters);
                    break;

                case "DbConfigChanges":
                    if (e.FilterState.IsActive)
                        _dbConfigChangesFilters[e.FilterState.ColumnName] = e.FilterState;
                    else
                        _dbConfigChangesFilters.Remove(e.FilterState.ColumnName);
                    ApplyDbConfigChangesFilters();
                    UpdateFilterButtonStyles(DatabaseConfigChangesDataGrid, _dbConfigChangesFilters);
                    break;

                case "TraceFlagChanges":
                    if (e.FilterState.IsActive)
                        _traceFlagChangesFilters[e.FilterState.ColumnName] = e.FilterState;
                    else
                        _traceFlagChangesFilters.Remove(e.FilterState.ColumnName);
                    ApplyTraceFlagChangesFilters();
                    UpdateFilterButtonStyles(TraceFlagChangesDataGrid, _traceFlagChangesFilters);
                    break;
            }
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
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

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
    public partial class CurrentConfigContent : UserControl
    {
        private DatabaseService? _databaseService;

        // Popup filter state (shared popup, per-grid filter dictionaries)
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;
        private string? _activeFilterGrid;

        private readonly Dictionary<string, ColumnFilterState> _serverConfigFilters = new();
        private List<CurrentServerConfigItem>? _serverConfigUnfilteredData;

        private readonly Dictionary<string, ColumnFilterState> _databaseConfigFilters = new();
        private List<CurrentDatabaseConfigItem>? _databaseConfigUnfilteredData;

        private readonly Dictionary<string, ColumnFilterState> _traceFlagsFilters = new();
        private List<CurrentTraceFlagItem>? _traceFlagsUnfilteredData;

        public CurrentConfigContent()
        {
            InitializeComponent();
        }

        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService;
        }

        public async Task RefreshAllDataAsync()
        {
            if (_databaseService == null) return;

            await Task.WhenAll(
                RefreshServerConfigAsync(),
                RefreshDatabaseConfigAsync(),
                RefreshTraceFlagsAsync()
            );
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(ServerConfigDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(DatabaseConfigDataGrid);
            TabHelpers.AutoSizeColumnMinWidths(TraceFlagsDataGrid);
            TabHelpers.FreezeColumns(ServerConfigDataGrid, 1);
            TabHelpers.FreezeColumns(DatabaseConfigDataGrid, 1);
        }

        #region Server Configuration

        private async Task RefreshServerConfigAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetCurrentServerConfigAsync();
                _serverConfigUnfilteredData = data;
                _serverConfigFilters.Clear();
                ServerConfigDataGrid.ItemsSource = data;
                ServerConfigNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                UpdateFilterButtonStyles(ServerConfigDataGrid, _serverConfigFilters);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading server configuration: {ex.Message}");
            }
        }

        #endregion

        #region Database Configuration

        private async Task RefreshDatabaseConfigAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetCurrentDatabaseConfigAsync();
                _databaseConfigUnfilteredData = data;
                _databaseConfigFilters.Clear();
                DatabaseConfigDataGrid.ItemsSource = data;
                DatabaseConfigNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                UpdateFilterButtonStyles(DatabaseConfigDataGrid, _databaseConfigFilters);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database configuration: {ex.Message}");
            }
        }

        #endregion

        #region Trace Flags

        private async Task RefreshTraceFlagsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetCurrentTraceFlagsAsync();
                _traceFlagsUnfilteredData = data;
                _traceFlagsFilters.Clear();
                TraceFlagsDataGrid.ItemsSource = data;
                TraceFlagsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                UpdateFilterButtonStyles(TraceFlagsDataGrid, _traceFlagsFilters);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading trace flags: {ex.Message}");
            }
        }

        #endregion

        #region Popup Filter Infrastructure

        private void ServerConfigFilter_Click(object sender, RoutedEventArgs e)
        {
            _activeFilterGrid = "ServerConfig";
            if (sender is Button button && button.Tag is string columnName)
                ShowFilterPopup(button, columnName, _serverConfigFilters);
        }

        private void DatabaseConfigFilter_Click(object sender, RoutedEventArgs e)
        {
            _activeFilterGrid = "DatabaseConfig";
            if (sender is Button button && button.Tag is string columnName)
                ShowFilterPopup(button, columnName, _databaseConfigFilters);
        }

        private void TraceFlagsFilter_Click(object sender, RoutedEventArgs e)
        {
            _activeFilterGrid = "TraceFlags";
            if (sender is Button button && button.Tag is string columnName)
                ShowFilterPopup(button, columnName, _traceFlagsFilters);
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
                case "ServerConfig":
                    if (e.FilterState.IsActive)
                        _serverConfigFilters[e.FilterState.ColumnName] = e.FilterState;
                    else
                        _serverConfigFilters.Remove(e.FilterState.ColumnName);
                    ApplyFilters(_serverConfigFilters, _serverConfigUnfilteredData, ServerConfigDataGrid, ServerConfigNoDataMessage);
                    UpdateFilterButtonStyles(ServerConfigDataGrid, _serverConfigFilters);
                    break;

                case "DatabaseConfig":
                    if (e.FilterState.IsActive)
                        _databaseConfigFilters[e.FilterState.ColumnName] = e.FilterState;
                    else
                        _databaseConfigFilters.Remove(e.FilterState.ColumnName);
                    ApplyFilters(_databaseConfigFilters, _databaseConfigUnfilteredData, DatabaseConfigDataGrid, DatabaseConfigNoDataMessage);
                    UpdateFilterButtonStyles(DatabaseConfigDataGrid, _databaseConfigFilters);
                    break;

                case "TraceFlags":
                    if (e.FilterState.IsActive)
                        _traceFlagsFilters[e.FilterState.ColumnName] = e.FilterState;
                    else
                        _traceFlagsFilters.Remove(e.FilterState.ColumnName);
                    ApplyFilters(_traceFlagsFilters, _traceFlagsUnfilteredData, TraceFlagsDataGrid, TraceFlagsNoDataMessage);
                    UpdateFilterButtonStyles(TraceFlagsDataGrid, _traceFlagsFilters);
                    break;
            }
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
                        Clipboard.SetDataObject(cellContent, false);
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid?.SelectedItem != null)
                    Clipboard.SetDataObject(TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem), false);
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new System.Text.StringBuilder();
                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                        headers.Add(DataGridClipboardBehavior.GetHeaderText(column));
                    sb.AppendLine(string.Join("\t", headers));
                    foreach (var item in dataGrid.Items)
                        sb.AppendLine(TabHelpers.GetRowAsText(dataGrid, item));
                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var dialog = new Microsoft.Win32.SaveFileDialog
                    {
                        FileName = $"config_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };
                    if (dialog.ShowDialog() == true)
                    {
                        var sb = new System.Text.StringBuilder();
                        var headers = new List<string>();
                        foreach (var column in dataGrid.Columns)
                            headers.Add(TabHelpers.EscapeCsvField(DataGridClipboardBehavior.GetHeaderText(column)));
                        sb.AppendLine(string.Join(",", headers));
                        foreach (var item in dataGrid.Items)
                        {
                            var values = TabHelpers.GetRowValues(dataGrid, item);
                            sb.AppendLine(string.Join(",", values.Select(v => TabHelpers.EscapeCsvField(v))));
                        }
                        System.IO.File.WriteAllText(dialog.FileName, sb.ToString());
                    }
                }
            }
        }

        #endregion
    }
}

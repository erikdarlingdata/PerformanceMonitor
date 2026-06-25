/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class FinOpsContent : UserControl
    {

        private void DatabaseSizesFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            if (_dbSizeFilterPopup == null)
            {
                _dbSizeFilterPopupContent = new ColumnFilterPopup();
                _dbSizeFilterPopupContent.FilterApplied += FilterPopup_DbSizeFilterApplied;
                _dbSizeFilterPopupContent.FilterCleared += FilterPopup_DbSizeFilterCleared;
                _dbSizeFilterPopup = new Popup
                {
                    Child = _dbSizeFilterPopupContent,
                    StaysOpen = false,
                    Placement = PlacementMode.Bottom,
                    AllowsTransparency = true
                };
            }

            _dbSizesFilterMgr!.Filters.TryGetValue(columnName, out var existingFilter);
            _dbSizeFilterPopupContent!.Initialize(columnName, existingFilter);
            _dbSizeFilterPopup.PlacementTarget = button;
            _dbSizeFilterPopup.IsOpen = true;
        }

        private void FilterPopup_DbSizeFilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_dbSizeFilterPopup != null)
                _dbSizeFilterPopup.IsOpen = false;

            _dbSizesFilterMgr!.SetFilter(e.FilterState);
            UpdateDbSizeCountUI();
        }

        private void FilterPopup_DbSizeFilterCleared(object? sender, EventArgs e)
        {
            if (_dbSizeFilterPopup != null)
                _dbSizeFilterPopup.IsOpen = false;
        }

        // ============================================
        // Column Filtering
        // ============================================

        #region Column Filtering

        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;
        private DataGrid? _currentFilterGrid;
        private readonly Dictionary<DataGrid, Dictionary<string, ColumnFilterState>> _gridFilters = new();
        private readonly Dictionary<DataGrid, System.Collections.IEnumerable?> _gridUnfilteredData = new();

        private void EnsureFinOpsFilterPopup()
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

        private void FinOpsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            var dataGrid = TabHelpers.FindParent<DataGrid>(button);
            if (dataGrid == null) return;

            EnsureFinOpsFilterPopup();

            // Rewire events — remove then add to avoid double-firing
            _filterPopupContent!.FilterApplied -= FinOpsFilterPopup_Applied;
            _filterPopupContent.FilterCleared -= FinOpsFilterPopup_Cleared;
            _filterPopupContent.FilterApplied += FinOpsFilterPopup_Applied;
            _filterPopupContent.FilterCleared += FinOpsFilterPopup_Cleared;

            _currentFilterGrid = dataGrid;

            if (!_gridFilters.ContainsKey(dataGrid))
                _gridFilters[dataGrid] = new Dictionary<string, ColumnFilterState>();

            _gridFilters[dataGrid].TryGetValue(columnName, out var existing);
            _filterPopupContent.Initialize(columnName, existing);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FinOpsFilterPopup_Applied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (_currentFilterGrid == null) return;

            if (!_gridFilters.ContainsKey(_currentFilterGrid))
                _gridFilters[_currentFilterGrid] = new Dictionary<string, ColumnFilterState>();

            if (e.FilterState.IsActive)
            {
                _gridFilters[_currentFilterGrid][e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _gridFilters[_currentFilterGrid].Remove(e.FilterState.ColumnName);
            }

            ApplyFinOpsFilters(_currentFilterGrid);
            UpdateFinOpsFilterButtonStyles(_currentFilterGrid);
        }

        private void FinOpsFilterPopup_Cleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyFinOpsFilters(DataGrid dataGrid)
        {
            // Capture unfiltered data on first filter application
            if (!_gridUnfilteredData.TryGetValue(dataGrid, out var cached) || cached == null)
            {
                cached = dataGrid.ItemsSource;
                _gridUnfilteredData[dataGrid] = cached;
            }

            var unfilteredData = cached;
            if (unfilteredData == null) return;

            if (!_gridFilters.TryGetValue(dataGrid, out var filters) || filters.Count == 0)
            {
                dataGrid.ItemsSource = unfilteredData;
                return;
            }

            // Generic filtering: cast to IEnumerable, filter each item using reflection-based MatchesFilter
            var sourceList = unfilteredData.Cast<object>().ToList();
            var filteredData = sourceList.Where(item =>
            {
                foreach (var filter in filters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            dataGrid.ItemsSource = filteredData;
        }

        /// <summary>
        /// Updates filter button styles (gold when active, white when inactive) for any FinOps DataGrid.
        /// </summary>
        private void UpdateFinOpsFilterButtonStyles(DataGrid dataGrid)
        {
            if (!_gridFilters.TryGetValue(dataGrid, out var filters))
                filters = new Dictionary<string, ColumnFilterState>();

            foreach (var column in dataGrid.Columns)
            {
                if (column.Header is StackPanel headerPanel)
                {
                    var filterButton = headerPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        var textBlock = new System.Windows.Controls.TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                                ? new SolidColorBrush(Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new SolidColorBrush(Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
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

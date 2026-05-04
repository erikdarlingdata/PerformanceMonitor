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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        // Filter state dictionaries for each DataGrid

        private Dictionary<string, ColumnFilterState> _collectionHealthFilters = new();
        private List<CollectionHealthItem>? _collectionHealthUnfilteredData;

        private Dictionary<string, ColumnFilterState> _blockingEventsFilters = new();
        private List<BlockingEventItem>? _blockingEventsUnfilteredData;

        private Dictionary<string, ColumnFilterState> _deadlocksFilters = new();
        private List<DeadlockItem>? _deadlocksUnfilteredData;

        // Shared filter popup
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;
        private string _currentFilterDataGrid = string.Empty;
        private Button? _currentFilterButton;

        // ====================================================================
        // Column Filter Popup Infrastructure
        // ====================================================================

        #region Filter Popup Infrastructure

        private void ShowFilterPopup(Button button, string columnName, string dataGridName)
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

            _currentFilterDataGrid = dataGridName;
            _currentFilterButton = button;

            // Get existing filter state
            ColumnFilterState? existingFilter = null;
            switch (dataGridName)
            {
                case "CollectionHealth":
                    _collectionHealthFilters.TryGetValue(columnName, out existingFilter);
                    break;
                case "BlockingEvents":
                    _blockingEventsFilters.TryGetValue(columnName, out existingFilter);
                    break;
                case "Deadlocks":
                    _deadlocksFilters.TryGetValue(columnName, out existingFilter);
                    break;
            }

            _filterPopupContent!.Initialize(columnName, existingFilter);
            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            switch (_currentFilterDataGrid)
            {
                case "CollectionHealth":
                    UpdateFilterState(_collectionHealthFilters, e.FilterState);
                    ApplyCollectionHealthFilters();
                    UpdateDataGridFilterButtonStyles(HealthDataGrid, _collectionHealthFilters);
                    break;
                case "BlockingEvents":
                    UpdateFilterState(_blockingEventsFilters, e.FilterState);
                    ApplyBlockingEventsFilters();
                    UpdateDataGridFilterButtonStyles(BlockingEventsDataGrid, _blockingEventsFilters);
                    break;
                case "Deadlocks":
                    UpdateFilterState(_deadlocksFilters, e.FilterState);
                    ApplyDeadlocksFilters();
                    UpdateDataGridFilterButtonStyles(DeadlocksDataGrid, _deadlocksFilters);
                    break;
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

        private void UpdateFilterButtonVisual(Button? button, ColumnFilterState filterState)
        {
            if (button == null) return;

            bool isActive = filterState.IsActive;

            // Create a TextBlock with the filter icon - gold when active, white when inactive
            var textBlock = new System.Windows.Controls.TextBlock
            {
                Text = "",
                FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                Foreground = isActive
                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                    : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
            };
            button.Content = textBlock;

            // Update tooltip to show current filter
            button.ToolTip = isActive
                ? $"Filter: {filterState.DisplayText}\n(Click to modify)"
                : "Click to filter";
        }


        private void UpdateDataGridFilterButtonStyles(DataGrid dataGrid, Dictionary<string, ColumnFilterState> filters)
        {
            foreach (var column in dataGrid.Columns)
            {
                if (column.Header is StackPanel headerPanel)
                {
                    var filterButton = headerPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        // Create a TextBlock with the filter icon - gold when active, white when inactive
                        var textBlock = new System.Windows.Controls.TextBlock
                        {
                            Text = "",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                            ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
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

        // ====================================================================
        // Collection Health Filter Handlers
        // ====================================================================

        #region Collection Health Filters

        private void CollectionHealthFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "CollectionHealth");
        }

        private void ApplyCollectionHealthFilters()
        {
            if (_collectionHealthUnfilteredData == null)
            {
                _collectionHealthUnfilteredData = HealthDataGrid.ItemsSource as List<CollectionHealthItem>;
                if (_collectionHealthUnfilteredData == null && HealthDataGrid.ItemsSource != null)
                {
                    _collectionHealthUnfilteredData = (HealthDataGrid.ItemsSource as IEnumerable<CollectionHealthItem>)?.ToList();
                }
            }

            if (_collectionHealthUnfilteredData == null) return;

            if (_collectionHealthFilters.Count == 0)
            {
                HealthDataGrid.ItemsSource = _collectionHealthUnfilteredData;
                return;
            }

            var filteredData = _collectionHealthUnfilteredData.Where(item =>
            {
                foreach (var filter in _collectionHealthFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            HealthDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================
        // Blocking Events Filter Handlers
        // ====================================================================

        #region Blocking Events Filters

        private void BlockingEventsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "BlockingEvents");
        }

        private void ApplyBlockingEventsFilters()
        {
            if (_blockingEventsUnfilteredData == null)
            {
                _blockingEventsUnfilteredData = BlockingEventsDataGrid.ItemsSource as List<BlockingEventItem>;
                if (_blockingEventsUnfilteredData == null && BlockingEventsDataGrid.ItemsSource != null)
                {
                    _blockingEventsUnfilteredData = (BlockingEventsDataGrid.ItemsSource as IEnumerable<BlockingEventItem>)?.ToList();
                }
            }

            if (_blockingEventsUnfilteredData == null) return;

            if (_blockingEventsFilters.Count == 0)
            {
                BlockingEventsDataGrid.ItemsSource = _blockingEventsUnfilteredData;
                return;
            }

            var filteredData = _blockingEventsUnfilteredData.Where(item =>
            {
                foreach (var filter in _blockingEventsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            BlockingEventsDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================
        // Deadlocks Filter Handlers
        // ====================================================================

        #region Deadlocks Filters

        private void DeadlocksFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "Deadlocks");
        }

        private void ApplyDeadlocksFilters()
        {
            if (_deadlocksUnfilteredData == null)
            {
                _deadlocksUnfilteredData = DeadlocksDataGrid.ItemsSource as List<DeadlockItem>;
                if (_deadlocksUnfilteredData == null && DeadlocksDataGrid.ItemsSource != null)
                {
                    _deadlocksUnfilteredData = (DeadlocksDataGrid.ItemsSource as IEnumerable<DeadlockItem>)?.ToList();
                }
            }

            if (_deadlocksUnfilteredData == null) return;

            if (_deadlocksFilters.Count == 0)
            {
                DeadlocksDataGrid.ItemsSource = _deadlocksUnfilteredData;
                return;
            }

            var filteredData = _deadlocksUnfilteredData.Where(item =>
            {
                foreach (var filter in _deadlocksFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            DeadlocksDataGrid.ItemsSource = filteredData;
        }

        #endregion
    }
}

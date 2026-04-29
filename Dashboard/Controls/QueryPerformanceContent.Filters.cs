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
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class QueryPerformanceContent : UserControl
    {
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        // Active Queries filter state
        private Dictionary<string, Models.ColumnFilterState> _activeQueriesFilters = new();
        private List<QuerySnapshotItem>? _activeQueriesUnfilteredData;

        // Current Active Queries filter state
        private Dictionary<string, Models.ColumnFilterState> _currentActiveFilters = new();
        private List<LiveQueryItem>? _currentActiveUnfilteredData;

        // Query Stats filter state
        private Dictionary<string, Models.ColumnFilterState> _queryStatsFilters = new();
        private List<QueryStatsItem>? _queryStatsUnfilteredData;

        // Procedure Stats filter state
        private Dictionary<string, Models.ColumnFilterState> _procStatsFilters = new();
        private List<ProcedureStatsItem>? _procStatsUnfilteredData;

        // Query Store filter state
        private Dictionary<string, Models.ColumnFilterState> _queryStoreFilters = new();
        private List<QueryStoreItem>? _queryStoreUnfilteredData;

        // Query Store Regressions filter state
        private Dictionary<string, Models.ColumnFilterState> _qsRegressionsFilters = new();
        private List<QueryStoreRegressionItem>? _qsRegressionsUnfilteredData;

        // Query Trace Patterns filter state
        private Dictionary<string, Models.ColumnFilterState> _lrqPatternsFilters = new();
        private List<LongRunningQueryPatternItem>? _lrqPatternsUnfilteredData;

        /// <summary>
        /// Generic method to update filter button styles for any DataGrid by traversing column headers
        /// </summary>
        private void UpdateDataGridFilterButtonStyles(DataGrid dataGrid, Dictionary<string, Models.ColumnFilterState> filters)
        {
            foreach (var column in dataGrid.Columns)
            {
                // Get the header content - it's either a StackPanel containing a Button, or a direct element
                if (column.Header is StackPanel headerPanel)
                {
                    // Find the filter button in the header
                    var filterButton = headerPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        // Create a TextBlock with the filter icon - gold when active, white when inactive
                        var textBlock = new System.Windows.Controls.TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
                        };
                        filterButton.Content = textBlock;

                        // Update tooltip to show current filter
                        filterButton.ToolTip = hasActiveFilter && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                    }
                }
            }
        }

        private void EnsureFilterPopup()
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

        private void RewireFilterPopupEvents(
            EventHandler<FilterAppliedEventArgs> filterAppliedHandler,
            EventHandler filterClearedHandler)
        {
            if (_filterPopupContent == null) return;

            // Remove all possible handlers first
            _filterPopupContent.FilterApplied -= ActiveQueriesFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= ActiveQueriesFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= QueryStatsFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= QueryStatsFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= ProcStatsFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= ProcStatsFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= QueryStoreFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= QueryStoreFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= QsRegressionsFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= QsRegressionsFilterPopup_FilterCleared;
            _filterPopupContent.FilterApplied -= LrqPatternsFilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= LrqPatternsFilterPopup_FilterCleared;

            // Add the new handlers
            _filterPopupContent.FilterApplied += filterAppliedHandler;
            _filterPopupContent.FilterCleared += filterClearedHandler;
        }

        // ── Active Queries Filters ──

        private void ActiveQueriesFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                ActiveQueriesFilterPopup_FilterApplied,
                ActiveQueriesFilterPopup_FilterCleared);

            _activeQueriesFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void ActiveQueriesFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _activeQueriesFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _activeQueriesFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyActiveQueriesFilters();
            UpdateDataGridFilterButtonStyles(ActiveQueriesDataGrid, _activeQueriesFilters);
        }

        private void ActiveQueriesFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyActiveQueriesFilters()
        {
            if (_activeQueriesUnfilteredData == null)
            {
                // Capture the unfiltered data on first filter application
                _activeQueriesUnfilteredData = ActiveQueriesDataGrid.ItemsSource as List<QuerySnapshotItem>;
                if (_activeQueriesUnfilteredData == null && ActiveQueriesDataGrid.ItemsSource != null)
                {
                    _activeQueriesUnfilteredData = (ActiveQueriesDataGrid.ItemsSource as IEnumerable<QuerySnapshotItem>)?.ToList();
                }
            }

            if (_activeQueriesUnfilteredData == null) return;

            if (_activeQueriesFilters.Count == 0)
            {
                ActiveQueriesDataGrid.ItemsSource = _activeQueriesUnfilteredData;
                return;
            }

            var filteredData = _activeQueriesUnfilteredData.Where(item =>
            {
                foreach (var filter in _activeQueriesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            ActiveQueriesDataGrid.ItemsSource = filteredData;
        }

        // ── Current Active Queries Filters ──

        private void CurrentActiveFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                CurrentActiveFilterPopup_FilterApplied,
                CurrentActiveFilterPopup_FilterCleared);

            _currentActiveFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void CurrentActiveFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _currentActiveFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _currentActiveFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyCurrentActiveFilters();
            UpdateDataGridFilterButtonStyles(CurrentActiveQueriesDataGrid, _currentActiveFilters);
        }

        private void CurrentActiveFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyCurrentActiveFilters()
        {
            if (_currentActiveUnfilteredData == null) return;

            if (_currentActiveFilters.Count == 0)
            {
                CurrentActiveQueriesDataGrid.ItemsSource = _currentActiveUnfilteredData;
                return;
            }

            var filteredData = _currentActiveUnfilteredData.Where(item =>
            {
                foreach (var filter in _currentActiveFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            CurrentActiveQueriesDataGrid.ItemsSource = filteredData;
        }

        // ── Query Stats Filters ──

        private void QueryStatsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                QueryStatsFilterPopup_FilterApplied,
                QueryStatsFilterPopup_FilterCleared);

            _queryStatsFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void QueryStatsFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _queryStatsFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _queryStatsFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyQueryStatsFilters();
            UpdateDataGridFilterButtonStyles(QueryStatsDataGrid, _queryStatsFilters);
        }

        private void QueryStatsFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyQueryStatsFilters()
        {
            if (_queryStatsUnfilteredData == null)
            {
                _queryStatsUnfilteredData = QueryStatsDataGrid.ItemsSource as List<QueryStatsItem>;
                if (_queryStatsUnfilteredData == null && QueryStatsDataGrid.ItemsSource != null)
                {
                    _queryStatsUnfilteredData = (QueryStatsDataGrid.ItemsSource as IEnumerable<QueryStatsItem>)?.ToList();
                }
            }

            if (_queryStatsUnfilteredData == null) return;

            if (_queryStatsFilters.Count == 0)
            {
                QueryStatsDataGrid.ItemsSource = _queryStatsUnfilteredData;
                return;
            }

            var filteredData = _queryStatsUnfilteredData.Where(item =>
            {
                foreach (var filter in _queryStatsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            QueryStatsDataGrid.ItemsSource = filteredData;
        }

        // ── Procedure Stats Filters ──

        private void ProcStatsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                ProcStatsFilterPopup_FilterApplied,
                ProcStatsFilterPopup_FilterCleared);

            _procStatsFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void ProcStatsFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _procStatsFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _procStatsFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyProcStatsFilters();
            UpdateDataGridFilterButtonStyles(ProcStatsDataGrid, _procStatsFilters);
        }

        private void ProcStatsFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyProcStatsFilters()
        {
            if (_procStatsUnfilteredData == null)
            {
                _procStatsUnfilteredData = ProcStatsDataGrid.ItemsSource as List<ProcedureStatsItem>;
                if (_procStatsUnfilteredData == null && ProcStatsDataGrid.ItemsSource != null)
                {
                    _procStatsUnfilteredData = (ProcStatsDataGrid.ItemsSource as IEnumerable<ProcedureStatsItem>)?.ToList();
                }
            }

            if (_procStatsUnfilteredData == null) return;

            if (_procStatsFilters.Count == 0)
            {
                ProcStatsDataGrid.ItemsSource = _procStatsUnfilteredData;
                return;
            }

            var filteredData = _procStatsUnfilteredData.Where(item =>
            {
                foreach (var filter in _procStatsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            ProcStatsDataGrid.ItemsSource = filteredData;
        }

        // ── Query Store Filters ──

        private void QueryStoreFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                QueryStoreFilterPopup_FilterApplied,
                QueryStoreFilterPopup_FilterCleared);

            _queryStoreFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void QueryStoreFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _queryStoreFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _queryStoreFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyQueryStoreFilters();
            UpdateDataGridFilterButtonStyles(QueryStoreDataGrid, _queryStoreFilters);
        }

        private void QueryStoreFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyQueryStoreFilters()
        {
            if (_queryStoreUnfilteredData == null)
            {
                _queryStoreUnfilteredData = QueryStoreDataGrid.ItemsSource as List<QueryStoreItem>;
                if (_queryStoreUnfilteredData == null && QueryStoreDataGrid.ItemsSource != null)
                {
                    _queryStoreUnfilteredData = (QueryStoreDataGrid.ItemsSource as IEnumerable<QueryStoreItem>)?.ToList();
                }
            }

            if (_queryStoreUnfilteredData == null) return;

            if (_queryStoreFilters.Count == 0)
            {
                QueryStoreDataGrid.ItemsSource = _queryStoreUnfilteredData;
                return;
            }

            var filteredData = _queryStoreUnfilteredData.Where(item =>
            {
                foreach (var filter in _queryStoreFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            QueryStoreDataGrid.ItemsSource = filteredData;
        }

        // ── Query Store Regressions Filters ──

        private void QsRegressionsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                QsRegressionsFilterPopup_FilterApplied,
                QsRegressionsFilterPopup_FilterCleared);

            _qsRegressionsFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void QsRegressionsFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _qsRegressionsFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _qsRegressionsFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyQsRegressionsFilters();
            UpdateDataGridFilterButtonStyles(QueryStoreRegressionsDataGrid, _qsRegressionsFilters);
        }

        private void QsRegressionsFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyQsRegressionsFilters()
        {
            if (_qsRegressionsUnfilteredData == null)
            {
                _qsRegressionsUnfilteredData = QueryStoreRegressionsDataGrid.ItemsSource as List<QueryStoreRegressionItem>;
                if (_qsRegressionsUnfilteredData == null && QueryStoreRegressionsDataGrid.ItemsSource != null)
                {
                    _qsRegressionsUnfilteredData = (QueryStoreRegressionsDataGrid.ItemsSource as IEnumerable<QueryStoreRegressionItem>)?.ToList();
                }
            }

            if (_qsRegressionsUnfilteredData == null) return;

            if (_qsRegressionsFilters.Count == 0)
            {
                QueryStoreRegressionsDataGrid.ItemsSource = _qsRegressionsUnfilteredData;
                return;
            }

            var filteredData = _qsRegressionsUnfilteredData.Where(item =>
            {
                foreach (var filter in _qsRegressionsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            QueryStoreRegressionsDataGrid.ItemsSource = filteredData;
        }

        // ── Long Running Query Patterns Filters ──

        private void LrqPatternsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            EnsureFilterPopup();
            RewireFilterPopupEvents(
                LrqPatternsFilterPopup_FilterApplied,
                LrqPatternsFilterPopup_FilterCleared);

            _lrqPatternsFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup!.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void LrqPatternsFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _lrqPatternsFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _lrqPatternsFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyLrqPatternsFilters();
            UpdateDataGridFilterButtonStyles(LongRunningQueryPatternsDataGrid, _lrqPatternsFilters);
        }

        private void LrqPatternsFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyLrqPatternsFilters()
        {
            if (_lrqPatternsUnfilteredData == null)
            {
                _lrqPatternsUnfilteredData = LongRunningQueryPatternsDataGrid.ItemsSource as List<LongRunningQueryPatternItem>;
                if (_lrqPatternsUnfilteredData == null && LongRunningQueryPatternsDataGrid.ItemsSource != null)
                {
                    _lrqPatternsUnfilteredData = (LongRunningQueryPatternsDataGrid.ItemsSource as IEnumerable<LongRunningQueryPatternItem>)?.ToList();
                }
            }

            if (_lrqPatternsUnfilteredData == null) return;

            if (_lrqPatternsFilters.Count == 0)
            {
                LongRunningQueryPatternsDataGrid.ItemsSource = _lrqPatternsUnfilteredData;
                return;
            }

            var filteredData = _lrqPatternsUnfilteredData.Where(item =>
            {
                foreach (var filter in _lrqPatternsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            LongRunningQueryPatternsDataGrid.ItemsSource = filteredData;
        }
    }
}

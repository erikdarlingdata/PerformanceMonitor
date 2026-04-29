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
    public partial class SystemEventsContent : UserControl
    {
        #region Shared Filter Popup Methods

        private void ShowFilterPopup(Button button, string columnName, string targetGrid,
            Dictionary<string, ColumnFilterState> filters,
            Action<FilterAppliedEventArgs> onApplied,
            Action onCleared)
        {
            // Create popup if needed
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

            // Disconnect previous event handlers
            _filterPopupContent!.FilterApplied -= FilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared -= FilterPopup_FilterCleared;

            // Set up current target and reconnect handlers
            _currentFilterTarget = targetGrid;
            _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
            _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

            // Initialize with current filter state
            filters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent.Initialize(columnName, existingFilter);

            // Position and show
            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            switch (_currentFilterTarget)
            {
                // SystemHealth case removed - grid removed per todo.md #18
                case "SevereErrors":
                    UpdateFilterState(_severeErrorsFilters, e.FilterState);
                    ApplySevereErrorsFilters();
                    UpdateSevereErrorsFilterButtonStyles();
                    break;
                // IOIssues case removed - grid removed per todo.md #19
                // SchedulerIssues case removed - grid removed per todo.md #13
                // MemoryConditions case removed - grid removed per todo.md #14
                // CPUTasks case removed - grid removed per todo.md #15
                case "MemoryBroker":
                    UpdateFilterState(_memoryBrokerFilters, e.FilterState);
                    ApplyMemoryBrokerFilters();
                    UpdateMemoryBrokerFilterButtonStyles();
                    break;
                // MemoryNodeOOM case removed - DataGrid removed per GitHub issue #13
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

        private void UpdateFilterButtonStyle(DataGrid dataGrid, string columnName, Dictionary<string, ColumnFilterState> filters)
        {
            // Find the button in the column header
            foreach (var column in dataGrid.Columns)
            {
                if (column.Header is StackPanel stackPanel)
                {
                    var button = stackPanel.Children.OfType<Button>().FirstOrDefault();
                    if (button != null && button.Tag is string tag && tag == columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        // Create a TextBlock with the filter icon - gold when active, light gray when inactive
                        var textBlock = new TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
                        };
                        button.Content = textBlock;

                        button.ToolTip = hasActiveFilter && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                        break;
                    }
                }
            }
        }

        #endregion
    }
}

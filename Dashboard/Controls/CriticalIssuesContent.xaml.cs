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
    /// <summary>
    /// UserControl for the Critical Issues sub-tab content.
    /// Displays detected critical issues with filtering by severity.
    /// </summary>
    public partial class CriticalIssuesContent : UserControl
    {
        private DatabaseService? _databaseService;

        // Time range state
        private int _criticalIssuesHoursBack = 24;
        private DateTime? _criticalIssuesFromDate = null;
        private DateTime? _criticalIssuesToDate = null;

        // Filter state for popup-based column filtering
        private Dictionary<string, ColumnFilterState> _criticalIssuesFilters = new();
        private List<CriticalIssueItem>? _criticalIssuesUnfilteredData;
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        public CriticalIssuesContent()
        {
            InitializeComponent();
            Loaded += OnLoaded;
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(CriticalIssuesDataGrid);
            TabHelpers.FreezeColumns(CriticalIssuesDataGrid, 2);
        }

        /// <summary>
        /// Initializes the control with required dependencies.
        /// </summary>
        /// <param name="databaseService">The database service for data retrieval.</param>
        public void Initialize(DatabaseService databaseService)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
        }

        /// <summary>
        /// Sets the time range for data retrieval.
        /// </summary>
        public void SetTimeRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
        {
            _criticalIssuesHoursBack = hoursBack;
            _criticalIssuesFromDate = fromDate;
            _criticalIssuesToDate = toDate;
        }

        /// <summary>
        /// Refreshes the critical issues data. Can be called from parent control.
        /// </summary>
        public async System.Threading.Tasks.Task RefreshDataAsync()
        {
            try
            {
                await LoadCriticalIssuesAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Critical Issues data: {ex.Message}", ex);
            }
        }

        private async System.Threading.Tasks.Task LoadCriticalIssuesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Only show loading overlay on initial load (no existing data)
                if (CriticalIssuesDataGrid.ItemsSource == null)
                {
                    CriticalIssuesLoading.IsLoading = true;
                    CriticalIssuesNoDataMessage.Visibility = Visibility.Collapsed;
                }

                var data = await _databaseService.GetCriticalIssuesAsync(
                    _criticalIssuesHoursBack,
                    _criticalIssuesFromDate,
                    _criticalIssuesToDate);

                // Store unfiltered data and reset filters when new data is loaded
                _criticalIssuesUnfilteredData = data;
                _criticalIssuesFilters.Clear();

                CriticalIssuesDataGrid.ItemsSource = data;
                CriticalIssuesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                UpdateCriticalIssuesFilterButtonStyles();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading critical issues: {ex.Message}");
            }
            finally
            {
                CriticalIssuesLoading.IsLoading = false;
            }
        }

        #region Filter Popup Handlers

        private void CriticalIssuesFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            // Create popup if needed
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

            // Initialize with current filter state
            _criticalIssuesFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            // Position and show
            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _criticalIssuesFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _criticalIssuesFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyCriticalIssuesFilters();
            UpdateCriticalIssuesFilterButtonStyles();
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyCriticalIssuesFilters()
        {
            if (_criticalIssuesUnfilteredData == null)
            {
                // Capture the unfiltered data on first filter application
                _criticalIssuesUnfilteredData = CriticalIssuesDataGrid.ItemsSource as List<CriticalIssueItem>;
                if (_criticalIssuesUnfilteredData == null && CriticalIssuesDataGrid.ItemsSource != null)
                {
                    _criticalIssuesUnfilteredData = (CriticalIssuesDataGrid.ItemsSource as IEnumerable<CriticalIssueItem>)?.ToList();
                }
            }

            if (_criticalIssuesUnfilteredData == null) return;

            if (_criticalIssuesFilters.Count == 0)
            {
                CriticalIssuesDataGrid.ItemsSource = _criticalIssuesUnfilteredData;
                CriticalIssuesNoDataMessage.Visibility = _criticalIssuesUnfilteredData.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                return;
            }

            var filteredData = _criticalIssuesUnfilteredData.Where(item =>
            {
                foreach (var filter in _criticalIssuesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            CriticalIssuesDataGrid.ItemsSource = filteredData;
            CriticalIssuesNoDataMessage.Visibility = filteredData.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }

        private void UpdateCriticalIssuesFilterButtonStyles()
        {
            var activeStyle = TryFindResource("ColumnFilterButtonActiveStyle") as Style;
            var normalStyle = TryFindResource("ColumnFilterButtonStyle") as Style;

            // Find all filter buttons in the DataGrid headers and update their styles
            foreach (var column in CriticalIssuesDataGrid.Columns)
            {
                if (column.Header is StackPanel stackPanel)
                {
                    var filterButton = stackPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        UpdateFilterButtonStyle(filterButton, columnName, activeStyle, normalStyle);
                    }
                }
            }
        }

        private void UpdateFilterButtonStyle(Button? button, string columnName, Style? activeStyle, Style? normalStyle)
        {
            if (button == null) return;

            bool hasActiveFilter = _criticalIssuesFilters.TryGetValue(columnName, out var filter) && filter.IsActive;

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

            // Update tooltip to show current filter
            button.ToolTip = hasActiveFilter && filter != null
                ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                : "Click to filter";
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
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(cellContent, false);
                    }
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.SelectedItem != null)
                {
                    var rowText = TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem);
                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(rowText, false);
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new StringBuilder();

                    // Add headers
                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                    {
                        if (column is DataGridBoundColumn)
                        {
                            headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                        }
                    }
                    sb.AppendLine(string.Join("\t", headers));

                    // Add all rows
                    foreach (var item in dataGrid.Items)
                    {
                        sb.AppendLine(TabHelpers.GetRowAsText(dataGrid, item));
                    }

                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
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
                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"critical_issues_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Add headers
                            var headers = new List<string>();
                            foreach (var column in dataGrid.Columns)
                            {
                                if (column is DataGridBoundColumn)
                                {
                                    headers.Add(TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column), TabHelpers.CsvSeparator));
                                }
                            }
                            sb.AppendLine(string.Join(TabHelpers.CsvSeparator, headers));

                            // Add all rows
                            foreach (var item in dataGrid.Items)
                            {
                                var values = TabHelpers.GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(TabHelpers.CsvSeparator, values.Select(v => TabHelpers.EscapeCsvField(v, TabHelpers.CsvSeparator))));
                            }

                            File.WriteAllText(saveFileDialog.FileName, sb.ToString());
                            MessageBox.Show($"Data exported successfully to:\n{saveFileDialog.FileName}", "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                        }
                        catch (Exception ex)
                        {
                            MessageBox.Show($"Error exporting data:\n\n{ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }

        #endregion
    }
}

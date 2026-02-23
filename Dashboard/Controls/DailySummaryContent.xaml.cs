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
    /// UserControl for the Daily Summary sub-tab content.
    /// Displays aggregated daily health metrics with date selection.
    /// </summary>
    public partial class DailySummaryContent : UserControl
    {
        private DatabaseService? _databaseService;
        private DateTime? _dailySummaryDate = null; // null means today

        // Daily Summary filter state
        private Dictionary<string, ColumnFilterState> _dailySummaryFilters = new();
        private List<DailySummaryItem>? _dailySummaryUnfilteredData;
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        public DailySummaryContent()
        {
            InitializeComponent();
            Loaded += OnLoaded;
        }

        private void OnLoaded(object sender, RoutedEventArgs e)
        {
            TabHelpers.AutoSizeColumnMinWidths(DailySummaryDataGrid);
            TabHelpers.FreezeColumns(DailySummaryDataGrid, 1);
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
        /// Refreshes the daily summary data. Can be called from parent control.
        /// </summary>
        public async System.Threading.Tasks.Task RefreshDataAsync()
        {
            try
            {
                await LoadDailySummaryAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Daily Summary data: {ex.Message}", ex);
            }
        }

        private async System.Threading.Tasks.Task LoadDailySummaryAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Only show loading overlay on initial load (no existing data)
                if (DailySummaryDataGrid.ItemsSource == null)
                {
                    DailySummaryLoading.IsLoading = true;
                    DailySummaryNoDataMessage.Visibility = Visibility.Collapsed;
                }

                var data = await _databaseService.GetDailySummaryAsync(_dailySummaryDate);

                // Store unfiltered data and reset filters when new data is loaded
                _dailySummaryUnfilteredData = data;

                // Apply existing filters if any
                if (_dailySummaryFilters.Count > 0)
                {
                    ApplyDailySummaryFilters();
                }
                else
                {
                    DailySummaryDataGrid.ItemsSource = data;
                }

                DailySummaryNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading daily summary: {ex.Message}");
            }
            finally
            {
                DailySummaryLoading.IsLoading = false;
            }
        }

        #region Event Handlers

        private void DailySummaryToday_Click(object sender, RoutedEventArgs e)
        {
            _dailySummaryDate = null;
            DailySummaryDatePicker.SelectedDate = null;
            DailySummaryTodayButton.FontWeight = FontWeights.Bold;
            DailySummaryIndicator.Text = "Showing: Today";
            DailySummary_Refresh_Click(sender, e);
        }

        private void DailySummaryDate_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (DailySummaryDatePicker.SelectedDate.HasValue)
            {
                _dailySummaryDate = DailySummaryDatePicker.SelectedDate.Value.Date;
                DailySummaryTodayButton.FontWeight = FontWeights.Normal;
                DailySummaryIndicator.Text = $"Showing: {_dailySummaryDate.Value:MMM d, yyyy}";
            }
        }

        private void DatePicker_CalendarOpened(object sender, RoutedEventArgs e)
        {
            if (sender is DatePicker datePicker)
            {
                // Use BeginInvoke to ensure visual tree is ready
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    // Get the Popup and Calendar from the DatePicker template
                    var popup = datePicker.Template.FindName("PART_Popup", datePicker) as System.Windows.Controls.Primitives.Popup;
                    if (popup?.Child is System.Windows.Controls.Calendar calendar)
                    {
                        TabHelpers.ApplyDarkThemeToCalendar(calendar);
                    }
                    else
                    {
                        // Fallback: search visual tree
                        var calendar2 = FindVisualChild<System.Windows.Controls.Calendar>(datePicker);
                        if (calendar2 != null)
                        {
                            TabHelpers.ApplyDarkThemeToCalendar(calendar2);
                        }
                    }
                }), System.Windows.Threading.DispatcherPriority.Loaded);
            }
        }

        private static T? FindVisualChild<T>(DependencyObject parent) where T : DependencyObject
        {
            for (int i = 0; i < System.Windows.Media.VisualTreeHelper.GetChildrenCount(parent); i++)
            {
                var child = System.Windows.Media.VisualTreeHelper.GetChild(parent, i);
                if (child is T typedChild)
                    return typedChild;
                var result = FindVisualChild<T>(child);
                if (result != null)
                    return result;
            }
            return null;
        }

        private void DailySummaryFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(DailySummaryDataGrid, sender as TextBox);
        }

        private void DailySummaryNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(DailySummaryDataGrid, sender as TextBox);
        }

        private async void DailySummary_Refresh_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                await LoadDailySummaryAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing Daily Summary data: {ex.Message}", ex);
            }
        }

        #endregion

        #region Filter Popup Handlers

        private void DailySummaryFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            ShowFilterPopup(button, columnName);
        }

        private void ShowFilterPopup(Button button, string columnName)
        {
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
            _dailySummaryFilters.TryGetValue(columnName, out var existingFilter);
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
                _dailySummaryFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _dailySummaryFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyDailySummaryFilters();
            UpdateFilterButtonStyles();
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyDailySummaryFilters()
        {
            if (_dailySummaryUnfilteredData == null)
            {
                // Capture the unfiltered data on first filter application
                _dailySummaryUnfilteredData = DailySummaryDataGrid.ItemsSource as List<DailySummaryItem>;
                if (_dailySummaryUnfilteredData == null && DailySummaryDataGrid.ItemsSource != null)
                {
                    _dailySummaryUnfilteredData = (DailySummaryDataGrid.ItemsSource as IEnumerable<DailySummaryItem>)?.ToList();
                }
            }

            if (_dailySummaryUnfilteredData == null) return;

            if (_dailySummaryFilters.Count == 0)
            {
                DailySummaryDataGrid.ItemsSource = _dailySummaryUnfilteredData;
                return;
            }

            var filteredData = _dailySummaryUnfilteredData.Where(item =>
            {
                foreach (var filter in _dailySummaryFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            DailySummaryDataGrid.ItemsSource = filteredData;
        }

        private void UpdateFilterButtonStyles()
        {
            var activeStyle = FindResource("ColumnFilterButtonActiveStyle") as Style;
            var normalStyle = FindResource("ColumnFilterButtonStyle") as Style;

            // Find and update filter buttons in the DataGrid column headers
            foreach (var column in DailySummaryDataGrid.Columns)
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

            bool hasActiveFilter = _dailySummaryFilters.TryGetValue(columnName, out var filter) && filter.IsActive;

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
                        FileName = $"daily_summary_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
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

        // Filtering logic moved to DataGridFilterService.ApplyFilter()
    }
}

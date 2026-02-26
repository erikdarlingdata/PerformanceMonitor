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
using System.Windows.Media;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class CollectionLogWindow : Window
    {
        private readonly string _collectorName;
        private readonly DatabaseService _databaseService;

        // Filter state
        private Dictionary<string, ColumnFilterState> _logFilters = new();
        private List<CollectionLogEntry>? _unfilteredLogData;
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        public CollectionLogWindow(string collectorName, DatabaseService databaseService)
        {
            InitializeComponent();

            _collectorName = collectorName;
            _databaseService = databaseService;

            CollectorNameText.Text = $"Collection History: {collectorName}";

            Loaded += CollectionLogWindow_Loaded;
        }

        private async void CollectionLogWindow_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadCollectionLogAsync();
        }

        private async Task LoadCollectionLogAsync()
        {
            try
            {
                var logs = await _databaseService.GetCollectionLogAsync(_collectorName);
                _unfilteredLogData = logs;
                LogDataGrid.ItemsSource = logs;

                if (logs.Count > 0)
                {
                    var successCount = logs.Count(l => l.CollectionStatus == "SUCCESS");
                    var errorCount = logs.Count(l => l.CollectionStatus == "ERROR");
                    var avgDuration = logs.Where(l => l.CollectionStatus == "SUCCESS")
                                           .Select(l => l.DurationMs)
                                           .DefaultIfEmpty(0)
                                           .Average();

                    SummaryText.Text = $"Total Runs: {logs.Count} | Success: {successCount} | Errors: {errorCount} | Avg Duration: {avgDuration:F0} ms";
                }
                else
                {
                    SummaryText.Text = "No collection history found for this collector.";
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load collection history:\n\n{ex.Message}",
                    "Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
        }

        private void LogFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            // Create popup if needed
            if (_filterPopup == null)
            {
                _filterPopupContent = new ColumnFilterPopup();
                _filterPopupContent.FilterApplied += LogFilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared += LogFilterPopup_FilterCleared;

                _filterPopup = new Popup
                {
                    Child = _filterPopupContent,
                    StaysOpen = false,
                    Placement = PlacementMode.Bottom,
                    AllowsTransparency = true
                };
            }

            // Initialize with current filter state
            _logFilters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            // Position and show
            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void LogFilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
            {
                _logFilters[e.FilterState.ColumnName] = e.FilterState;
            }
            else
            {
                _logFilters.Remove(e.FilterState.ColumnName);
            }

            ApplyLogFilters();
            UpdateLogFilterButtonStyles();
        }

        private void LogFilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void ApplyLogFilters()
        {
            if (_unfilteredLogData == null) return;

            if (_logFilters.Count == 0)
            {
                LogDataGrid.ItemsSource = _unfilteredLogData;
                return;
            }

            var filtered = _unfilteredLogData.Where(item =>
            {
                foreach (var filter in _logFilters.Values)
                {
                    if (!DataGridFilterService.MatchesFilter(item, filter))
                        return false;
                }
                return true;
            }).ToList();

            LogDataGrid.ItemsSource = filtered;
        }

        private void UpdateLogFilterButtonStyles()
        {
            foreach (var column in LogDataGrid.Columns)
            {
                if (column.Header is StackPanel stackPanel)
                {
                    var filterButton = stackPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        var hasActiveFilter = _logFilters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        // Create a TextBlock with the filter icon - gold when active, white when inactive
                        var textBlock = new TextBlock
                        {
                            Text = "",
                            FontFamily = new FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                                ? new SolidColorBrush(Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new SolidColorBrush(Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
                        };
                        filterButton.Content = textBlock;

                        // Update tooltip
                        filterButton.ToolTip = hasActiveFilter && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                    }
                }
            }
        }

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = Helpers.TabHelpers.GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (!string.IsNullOrEmpty(cellContent))
                        Clipboard.SetDataObject(cellContent, false);
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid?.SelectedItem != null)
                    Clipboard.SetDataObject(Helpers.TabHelpers.GetRowAsText(dataGrid, dataGrid.SelectedItem), false);
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new System.Text.StringBuilder();
                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                        headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                    sb.AppendLine(string.Join("\t", headers));
                    foreach (var item in dataGrid.Items)
                        sb.AppendLine(Helpers.TabHelpers.GetRowAsText(dataGrid, item));
                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = Helpers.TabHelpers.FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var dialog = new Microsoft.Win32.SaveFileDialog
                    {
                        FileName = $"collection_log_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };
                    if (dialog.ShowDialog() == true)
                    {
                        var sb = new System.Text.StringBuilder();
                        var headers = new List<string>();
                        foreach (var column in dataGrid.Columns)
                            headers.Add(Helpers.TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column)));
                        sb.AppendLine(string.Join(",", headers));
                        foreach (var item in dataGrid.Items)
                        {
                            var values = Helpers.TabHelpers.GetRowValues(dataGrid, item);
                            sb.AppendLine(string.Join(",", values.Select(v => Helpers.TabHelpers.EscapeCsvField(v))));
                        }
                        System.IO.File.WriteAllText(dialog.FileName, sb.ToString());
                    }
                }
            }
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }
}

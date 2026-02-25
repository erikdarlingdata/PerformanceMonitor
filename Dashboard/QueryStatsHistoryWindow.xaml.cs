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
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using ScottPlot;

namespace PerformanceMonitorDashboard
{
    public partial class QueryStatsHistoryWindow : Window
    {
        private readonly DatabaseService _databaseService;
        private readonly string _databaseName;
        private readonly string _queryHash;
        private readonly int _hoursBack;
        private readonly DateTime? _fromDate;
        private readonly DateTime? _toDate;
        private List<QueryStatsHistoryItem> _historyData = new();

        // Filter state
        private Dictionary<string, ColumnFilterState> _filters = new();
        private List<QueryStatsHistoryItem>? _unfilteredData;
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;

        public QueryStatsHistoryWindow(
            DatabaseService databaseService,
            string databaseName,
            string queryHash,
            int hoursBack = 24,
            DateTime? fromDate = null,
            DateTime? toDate = null)
        {
            InitializeComponent();

            _databaseService = databaseService;
            _databaseName = databaseName;
            _queryHash = queryHash;
            _hoursBack = hoursBack;
            _fromDate = fromDate;
            _toDate = toDate;

            QueryIdentifierText.Text = $"Query Stats History: {queryHash} in [{databaseName}]";

            ApplyDarkModeToChart();
            Loaded += QueryStatsHistoryWindow_Loaded;
        }

        private void ApplyDarkModeToChart()
        {
            Helpers.TabHelpers.ApplyDarkModeToChart(HistoryChart);
        }

        private async void QueryStatsHistoryWindow_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                await LoadHistoryAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load query stats history:\n\n{ex.Message}",
                    "Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
        }

        private async Task LoadHistoryAsync()
        {
            try
            {
                _historyData = await _databaseService.GetQueryStatsHistoryAsync(_databaseName, _queryHash, _hoursBack, _fromDate, _toDate);

                // Compute per-interval executions. DMV counters are cumulative and reset on plan
                // eviction, so we walk oldest→newest, detecting lifetime boundaries by CreationTime.
                // Data arrives sorted by CollectionTime DESC, so walk from end to start.
                for (int i = _historyData.Count - 1; i >= 0; i--)
                {
                    var item = _historyData[i];
                    if (i == _historyData.Count - 1)
                    {
                        // Oldest row in window: credit all executions (no prior reference)
                        item.IntervalExecutions = item.ExecutionCount;
                    }
                    else
                    {
                        var olderItem = _historyData[i + 1];
                        if (item.CreationTime != olderItem.CreationTime)
                        {
                            // New plan lifetime (eviction + re-cache): credit all executions
                            item.IntervalExecutions = item.ExecutionCount;
                        }
                        else
                        {
                            // Same lifetime: delta from previous snapshot
                            item.IntervalExecutions = Math.Max(0, item.ExecutionCount - olderItem.ExecutionCount);
                        }
                    }
                }

                _unfilteredData = _historyData;
                _filters.Clear();
                HistoryDataGrid.ItemsSource = _historyData;
                UpdateFilterButtonStyles();

                if (_historyData.Count > 0)
                {
                    var totalExecutions = _historyData.Sum(h => h.IntervalExecutions);
                    var avgCpu = _historyData.Where(h => h.AvgWorkerTimeMs.HasValue).Select(h => h.AvgWorkerTimeMs ?? 0).DefaultIfEmpty(0).Average();
                    var avgDuration = _historyData.Where(h => h.AvgElapsedTimeMs.HasValue).Select(h => h.AvgElapsedTimeMs ?? 0).DefaultIfEmpty(0).Average();
                    var firstSample = _historyData.Min(h => h.CollectionTime);
                    var lastSample = _historyData.Max(h => h.CollectionTime);

                    SummaryText.Text = string.Format(CultureInfo.CurrentCulture,
                        "Samples: {0} | First: {1:yyyy-MM-dd HH:mm} | Last: {2:yyyy-MM-dd HH:mm} | Executions: {3:N0} | Avg CPU: {4:N2} ms | Avg Duration: {5:N2} ms",
                        _historyData.Count, firstSample, lastSample, totalExecutions, avgCpu, avgDuration);

                    UpdateChart();
                }
                else
                {
                    SummaryText.Text = "No historical data found for this query.";
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load query stats history:\n\n{ex.Message}",
                    "Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
        }

        private void MetricSelector_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_historyData.Count > 0)
            {
                UpdateChart();
            }
        }

        private void UpdateChart()
        {
            if (_historyData == null || _historyData.Count == 0)
                return;

            HistoryChart.Plot.Clear();

            var selectedItem = MetricSelector.SelectedItem as ComboBoxItem;
            var metricTag = selectedItem?.Tag?.ToString() ?? "AvgElapsedTimeMs";
            var metricLabel = selectedItem?.Content?.ToString() ?? "Avg Duration (ms)";

            var orderedData = _historyData.OrderBy(h => h.CollectionTime).ToList();

            var dates = orderedData.Select(h => h.CollectionTime.ToOADate()).ToArray();
            var values = orderedData.Select(h => GetMetricValue(h, metricTag)).ToArray();

            var color = ScottPlot.Color.FromHex("#4FC3F7");
            var scatter = HistoryChart.Plot.Add.Scatter(dates, values);
            scatter.Color = color;
            scatter.LineWidth = 2;
            scatter.MarkerSize = 6;

            HistoryChart.Plot.Axes.DateTimeTicksBottom();
            Helpers.TabHelpers.ReapplyAxisColors(HistoryChart);
            HistoryChart.Plot.YLabel(metricLabel);
            HistoryChart.Plot.XLabel("Collection Time");

            HistoryChart.Refresh();
        }

        private static double GetMetricValue(QueryStatsHistoryItem item, string metricTag)
        {
            return metricTag switch
            {
                "AvgElapsedTimeMs" => item.AvgElapsedTimeMs ?? 0,
                "AvgWorkerTimeMs" => item.AvgWorkerTimeMs ?? 0,
                "AvgLogicalReads" => item.AvgLogicalReads ?? 0,
                "AvgLogicalWrites" => item.AvgLogicalWrites ?? 0,
                "AvgPhysicalReads" => item.AvgPhysicalReads ?? 0,
                "AvgRows" => item.AvgRows ?? 0,
                "IntervalExecutions" => item.IntervalExecutions,
                _ => item.AvgElapsedTimeMs ?? 0
            };
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private async void DownloadPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is QueryStatsHistoryItem item)
            {
                try
                {
                    button.IsEnabled = false;
                    button.Content = "Loading...";

                    var planXml = await _databaseService.GetQueryStatsPlanXmlByCollectionIdAsync(item.CollectionId);

                    if (string.IsNullOrWhiteSpace(planXml))
                    {
                        MessageBox.Show("No query plan available for this collection.", "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
                        return;
                    }

                    var timestamp = item.CollectionTime.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                    var safeQueryHash = _queryHash.Replace("0x", "").Substring(0, Math.Min(8, _queryHash.Replace("0x", "").Length));
                    var defaultFileName = $"query_stats_history_{safeQueryHash}_{timestamp}.sqlplan";

                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = defaultFileName,
                        Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|XML files (*.xml)|*.xml|All files (*.*)|*.*",
                        DefaultExt = ".sqlplan"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        File.WriteAllText(saveFileDialog.FileName, planXml);
                        MessageBox.Show($"Query plan saved to:\n{saveFileDialog.FileName}", "Plan Saved", MessageBoxButton.OK, MessageBoxImage.Information);
                    }
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Failed to download query plan:\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }
                finally
                {
                    button.IsEnabled = true;
                    button.Content = "Download Plan";
                }
            }
        }

        #region Column Filter Popup

        private void Filter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

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

            _filters.TryGetValue(columnName, out var existingFilter);
            _filterPopupContent!.Initialize(columnName, existingFilter);

            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null) _filterPopup.IsOpen = false;

            if (e.FilterState.IsActive)
                _filters[e.FilterState.ColumnName] = e.FilterState;
            else
                _filters.Remove(e.FilterState.ColumnName);

            ApplyFilters();
            UpdateFilterButtonStyles();
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null) _filterPopup.IsOpen = false;
        }

        private void ApplyFilters()
        {
            if (_unfilteredData == null) return;

            if (_filters.Count == 0)
            {
                HistoryDataGrid.ItemsSource = _unfilteredData;
                return;
            }

            var filtered = _unfilteredData.Where(item =>
            {
                foreach (var filter in _filters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                        return false;
                }
                return true;
            }).ToList();

            HistoryDataGrid.ItemsSource = filtered;
        }

        private void UpdateFilterButtonStyles()
        {
            foreach (var column in HistoryDataGrid.Columns)
            {
                if (column.Header is StackPanel stackPanel)
                {
                    var filterButton = stackPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton?.Tag is string columnName)
                    {
                        bool hasActive = _filters.TryGetValue(columnName, out var filter) && filter.IsActive;
                        filterButton.Content = new System.Windows.Controls.TextBlock
                        {
                            Text = "\uE71C",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActive
                                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00))
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF))
                        };
                        filterButton.ToolTip = hasActive && filter != null
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
                    var sb = new StringBuilder();
                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                    {
                        if (column is DataGridBoundColumn)
                            headers.Add(Helpers.DataGridClipboardBehavior.GetHeaderText(column));
                    }
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
                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"query_stats_history_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();
                            var headers = new List<string>();
                            foreach (var column in dataGrid.Columns)
                            {
                                if (column is DataGridBoundColumn)
                                    headers.Add(TabHelpers.EscapeCsvField(Helpers.DataGridClipboardBehavior.GetHeaderText(column)));
                            }
                            sb.AppendLine(string.Join(",", headers));
                            foreach (var item in dataGrid.Items)
                            {
                                var values = TabHelpers.GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(",", values.Select(v => TabHelpers.EscapeCsvField(v))));
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

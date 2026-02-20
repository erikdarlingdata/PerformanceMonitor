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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using ScottPlot;

namespace PerformanceMonitorDashboard
{
    public partial class QueryExecutionHistoryWindow : Window
    {
        private readonly DatabaseService _databaseService;
        private readonly string _databaseName;
        private readonly long _queryId;
        private readonly string _sourceType;
        private readonly int _hoursBack;
        private readonly DateTime? _fromDate;
        private readonly DateTime? _toDate;
        private List<QueryExecutionHistoryItem> _historyData = new();
        private ScottPlot.IPanel? _legendPanel;

        public QueryExecutionHistoryWindow(
            DatabaseService databaseService,
            string databaseName,
            long queryId,
            string sourceType = "Query Store",
            int hoursBack = 24,
            DateTime? fromDate = null,
            DateTime? toDate = null)
        {
            InitializeComponent();

            _databaseService = databaseService;
            _databaseName = databaseName;
            _queryId = queryId;
            _sourceType = sourceType;
            _hoursBack = hoursBack;
            _fromDate = fromDate;
            _toDate = toDate;

            QueryIdentifierText.Text = $"Query Execution History: Query {queryId} in [{databaseName}]";

            ApplyDarkModeToChart();
            Loaded += QueryExecutionHistoryWindow_Loaded;
        }

        private void ApplyDarkModeToChart()
        {
            // Use TabHelpers pattern for consistency with other charts
            Helpers.TabHelpers.ApplyDarkModeToChart(HistoryChart);
        }

        private async void QueryExecutionHistoryWindow_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                await LoadHistoryAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load query execution history:\n\n{ex.Message}",
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
                if (_sourceType == "Query Store")
                {
                    _historyData = await _databaseService.GetQueryStoreHistoryAsync(_databaseName, _queryId, _hoursBack, _fromDate, _toDate);
                }
                else
                {
                    _historyData = new List<QueryExecutionHistoryItem>();
                }

                HistoryDataGrid.ItemsSource = _historyData;

                if (_historyData.Count > 0)
                {
                    var totalExecutions = _historyData.Sum(h => h.CountExecutions);
                    var avgDuration = _historyData.Average(h => h.AvgDurationMs);
                    var avgCpu = _historyData.Average(h => h.AvgCpuTimeMs);
                    var avgReads = _historyData.Average(h => (double)h.AvgLogicalReads);
                    var firstSample = _historyData.Min(h => h.CollectionTime);
                    var lastSample = _historyData.Max(h => h.CollectionTime);

                    SummaryText.Text = string.Format(CultureInfo.CurrentCulture,
                        "Samples: {0} | First: {1:yyyy-MM-dd HH:mm} | Last: {2:yyyy-MM-dd HH:mm} | Total Executions: {3:N0} | Avg Duration: {4:N2} ms | Avg CPU: {5:N2} ms | Avg Reads: {6:N0}",
                        _historyData.Count, firstSample, lastSample, totalExecutions, avgDuration, avgCpu, avgReads);

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
                    $"Failed to load query execution history:\n\n{ex.Message}",
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

            if (_legendPanel != null)
            {
                HistoryChart.Plot.Axes.Remove(_legendPanel);
                _legendPanel = null;
            }
            HistoryChart.Plot.Clear();

            var selectedItem = MetricSelector.SelectedItem as ComboBoxItem;
            var metricTag = selectedItem?.Tag?.ToString() ?? "AvgDurationMs";
            var metricLabel = selectedItem?.Content?.ToString() ?? "Avg Duration (ms)";

            // Group data by plan_id
            var planGroups = _historyData
                .GroupBy(h => h.PlanId)
                .OrderBy(g => g.Key)
                .ToList();

            // Color palette for different plans
            var colors = new[]
            {
                ScottPlot.Color.FromHex("#4FC3F7"),
                ScottPlot.Color.FromHex("#81C784"),
                ScottPlot.Color.FromHex("#FFB74D"),
                ScottPlot.Color.FromHex("#F06292"),
                ScottPlot.Color.FromHex("#BA68C8"),
                ScottPlot.Color.FromHex("#4DB6AC"),
                ScottPlot.Color.FromHex("#FF8A65"),
                ScottPlot.Color.FromHex("#A1887F")
            };

            var legendParts = new List<string>();
            int colorIndex = 0;

            foreach (var planGroup in planGroups)
            {
                var orderedData = planGroup.OrderBy(h => h.CollectionTime).ToList();

                if (orderedData.Count == 0)
                    continue;

                var dates = orderedData.Select(h => h.CollectionTime.ToOADate()).ToArray();
                var values = orderedData.Select(h => GetMetricValue(h, metricTag)).ToArray();

                var color = colors[colorIndex % colors.Length];
                var scatter = HistoryChart.Plot.Add.Scatter(dates, values);
                scatter.Color = color;
                scatter.LineWidth = 2;
                scatter.MarkerSize = 6;
                scatter.LegendText = $"Plan {planGroup.Key}";

                legendParts.Add($"Plan {planGroup.Key}");
                colorIndex++;
            }

            HistoryChart.Plot.Axes.DateTimeTicksBottom();
            Helpers.TabHelpers.ReapplyAxisColors(HistoryChart);
            HistoryChart.Plot.YLabel(metricLabel);
            HistoryChart.Plot.XLabel("Collection Time");
            _legendPanel = HistoryChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            HistoryChart.Plot.Legend.FontSize = 12;

            // Update legend text
            ChartLegendText.Text = planGroups.Count > 1
                ? $"{planGroups.Count} different plans detected"
                : "Single plan";

            HistoryChart.Refresh();
        }

        private static double GetMetricValue(QueryExecutionHistoryItem item, string metricTag)
        {
            return metricTag switch
            {
                "AvgDurationMs" => item.AvgDurationMs,
                "AvgCpuTimeMs" => item.AvgCpuTimeMs,
                "AvgLogicalReads" => item.AvgLogicalReads,
                "AvgLogicalWrites" => item.AvgLogicalWrites,
                "AvgPhysicalReads" => item.AvgPhysicalReads,
                "AvgMemoryMb" => item.AvgMemoryMb,
                "AvgRowcount" => item.AvgRowcount,
                "CountExecutions" => item.CountExecutions,
                _ => item.AvgDurationMs
            };
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private async void DownloadPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is QueryExecutionHistoryItem item)
            {
                try
                {
                    button.IsEnabled = false;
                    button.Content = "Loading...";

                    var planXml = await _databaseService.GetQueryStorePlanXmlByCollectionIdAsync(item.CollectionId);

                    if (string.IsNullOrWhiteSpace(planXml))
                    {
                        MessageBox.Show("No query plan available for this collection.", "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
                        return;
                    }

                    var timestamp = item.CollectionTime.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                    var defaultFileName = $"querystore_history_{_queryId}_{timestamp}.sqlplan";

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
    }
}

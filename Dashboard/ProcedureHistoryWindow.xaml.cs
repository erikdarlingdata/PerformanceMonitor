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
    public partial class ProcedureHistoryWindow : Window
    {
        private readonly DatabaseService _databaseService;
        private readonly string _databaseName;
        private readonly int _objectId;
        private readonly string _objectName;
        private readonly int _hoursBack;
        private readonly DateTime? _fromDate;
        private readonly DateTime? _toDate;
        private List<ProcedureExecutionHistoryItem> _historyData = new();

        public ProcedureHistoryWindow(
            DatabaseService databaseService,
            string databaseName,
            int objectId,
            string objectName,
            int hoursBack = 24,
            DateTime? fromDate = null,
            DateTime? toDate = null)
        {
            InitializeComponent();

            _databaseService = databaseService;
            _databaseName = databaseName;
            _objectId = objectId;
            _objectName = objectName;
            _hoursBack = hoursBack;
            _fromDate = fromDate;
            _toDate = toDate;

            ProcedureIdentifierText.Text = $"Procedure Execution History: {objectName} in [{databaseName}]";

            ApplyDarkModeToChart();
            Loaded += ProcedureHistoryWindow_Loaded;
        }

        private void ApplyDarkModeToChart()
        {
            Helpers.TabHelpers.ApplyDarkModeToChart(HistoryChart);
        }

        private async void ProcedureHistoryWindow_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                await LoadHistoryAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load procedure execution history:\n\n{ex.Message}",
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
                _historyData = await _databaseService.GetProcedureStatsHistoryAsync(_databaseName, _objectId, _hoursBack, _fromDate, _toDate);

                HistoryDataGrid.ItemsSource = _historyData;

                if (_historyData.Count > 0)
                {
                    var totalExecutions = _historyData.Max(h => h.ExecutionCount);
                    var latestExecDelta = _historyData.FirstOrDefault()?.ExecutionCountDelta ?? 0;
                    var avgCpu = _historyData.Where(h => h.AvgWorkerTimeMs.HasValue).Select(h => h.AvgWorkerTimeMs ?? 0).DefaultIfEmpty(0).Average();
                    var avgDuration = _historyData.Where(h => h.AvgElapsedTimeMs.HasValue).Select(h => h.AvgElapsedTimeMs ?? 0).DefaultIfEmpty(0).Average();
                    var firstSample = _historyData.Min(h => h.CollectionTime);
                    var lastSample = _historyData.Max(h => h.CollectionTime);

                    SummaryText.Text = string.Format(CultureInfo.CurrentCulture,
                        "Samples: {0} | First: {1:yyyy-MM-dd HH:mm} | Last: {2:yyyy-MM-dd HH:mm} | Total Executions: {3:N0} | Avg CPU: {4:N2} ms | Avg Duration: {5:N2} ms",
                        _historyData.Count, firstSample, lastSample, totalExecutions, avgCpu, avgDuration);

                    UpdateChart();
                }
                else
                {
                    SummaryText.Text = "No historical data found for this procedure.";
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load procedure execution history:\n\n{ex.Message}",
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

        private static double GetMetricValue(ProcedureExecutionHistoryItem item, string metricTag)
        {
            return metricTag switch
            {
                "AvgElapsedTimeMs" => item.AvgElapsedTimeMs ?? 0,
                "AvgWorkerTimeMs" => item.AvgWorkerTimeMs ?? 0,
                "AvgLogicalReads" => item.AvgLogicalReads ?? 0,
                "AvgLogicalWrites" => item.AvgLogicalWrites ?? 0,
                "AvgPhysicalReads" => item.AvgPhysicalReads ?? 0,
                "AvgSpills" => item.AvgSpills ?? 0,
                "ExecutionCountDelta" => item.ExecutionCountDelta ?? 0,
                _ => item.AvgElapsedTimeMs ?? 0
            };
        }

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private async void DownloadPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is ProcedureExecutionHistoryItem item)
            {
                try
                {
                    button.IsEnabled = false;
                    button.Content = "Loading...";

                    var planXml = await _databaseService.GetProcedureStatsPlanXmlByCollectionIdAsync(item.CollectionId);

                    if (string.IsNullOrWhiteSpace(planXml))
                    {
                        MessageBox.Show("No query plan available for this collection.", "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
                        return;
                    }

                    var timestamp = item.CollectionTime.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                    var safeObjectName = string.Join("_", _objectName.Split(Path.GetInvalidFileNameChars()));
                    var defaultFileName = $"procedure_history_{safeObjectName}_{timestamp}.sqlplan";

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

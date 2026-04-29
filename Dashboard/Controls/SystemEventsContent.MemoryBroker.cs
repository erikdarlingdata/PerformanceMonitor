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
        #region Memory Broker Tab

        private async System.Threading.Tasks.Task RefreshMemoryBrokerAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserMemoryBrokerAsync(_memoryBrokerHoursBack, _memoryBrokerFromDate, _memoryBrokerToDate);
                _memoryBrokerUnfilteredData = data;
                MemoryBrokerDataGrid.ItemsSource = data;
                MemoryBrokerNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                LoadMemoryBrokerChart(data, _memoryBrokerHoursBack, _memoryBrokerFromDate, _memoryBrokerToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory broker: {ex.Message}", ex);
            }
        }

        private void LoadMemoryBrokerChart(IEnumerable<HealthParserMemoryBrokerItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            /* Clear both charts */
            _memoryBrokerHover?.Clear();
            _memoryBrokerRatioHover?.Clear();
            foreach (var chart in new[] { MemoryBrokerChart, MemoryBrokerRatioChart })
            {
                if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
                {
                    chart.Plot.Axes.Remove(existingPanel);
                    _legendPanels[chart] = null;
                }
                chart.Plot.Clear();
                TabHelpers.ApplyThemeToChart(chart);
            }

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserMemoryBrokerItem>();
            bool hasAllocatedData = false;
            bool hasRatioData = false;

            if (dataList.Count > 0)
            {
                var colors = TabHelpers.ChartColors;

                /* Chart 1: Currently Allocated by Broker */
                var brokerGroups = dataList
                    .Where(d => d.CurrentlyAllocated.HasValue && !string.IsNullOrEmpty(d.Broker))
                    .GroupBy(d => d.Broker)
                    .ToList();

                int colorIndex = 0;
                foreach (var brokerGroup in brokerGroups)
                {
                    var brokerData = brokerGroup.OrderBy(d => d.EventTime!.Value).ToList();
                    if (brokerData.Count >= 1)
                    {
                        hasAllocatedData = true;
                        var timePoints = brokerData.Select(d => d.EventTime!.Value);
                        var values = brokerData.Select(d => (double)(d.CurrentlyAllocated ?? 0));
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                        var scatter = MemoryBrokerChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = colors[colorIndex % colors.Length];
                        var brokerLabel = brokerGroup.Key.Length > 25 ? brokerGroup.Key.Substring(0, 25) + "..." : brokerGroup.Key;
                        scatter.LegendText = brokerLabel;
                        _memoryBrokerHover?.Add(scatter, brokerLabel);
                        colorIndex++;
                    }
                }

                if (hasAllocatedData)
                {
                    _legendPanels[MemoryBrokerChart] = MemoryBrokerChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryBrokerChart.Plot.Legend.FontSize = 12;
                }

                /* Chart 2: Memory Ratio and Overall over time */
                var ratioData = dataList.Where(d => d.MemoryRatio.HasValue).OrderBy(d => d.EventTime!.Value).ToList();
                var overallData = dataList.Where(d => d.Overall.HasValue).OrderBy(d => d.EventTime!.Value).ToList();

                if (ratioData.Count >= 1)
                {
                    hasRatioData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        ratioData.Select(d => d.EventTime!.Value),
                        ratioData.Select(d => (double)(d.MemoryRatio ?? 0)));

                    var scatter = MemoryBrokerRatioChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[0];
                    scatter.LegendText = "Memory Ratio";
                    _memoryBrokerRatioHover?.Add(scatter, "Memory Ratio");
                }

                if (overallData.Count >= 1)
                {
                    hasRatioData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        overallData.Select(d => d.EventTime!.Value),
                        overallData.Select(d => (double)(d.Overall ?? 0)));

                    var scatter = MemoryBrokerRatioChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[2];
                    scatter.LegendText = "Overall";
                    _memoryBrokerRatioHover?.Add(scatter, "Overall");
                }

                if (hasRatioData)
                {
                    _legendPanels[MemoryBrokerRatioChart] = MemoryBrokerRatioChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryBrokerRatioChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasAllocatedData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryBrokerChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            if (!hasRatioData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryBrokerRatioChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            /* Finalize both charts */
            foreach (var chart in new[] { MemoryBrokerChart, MemoryBrokerRatioChart })
            {
                chart.Plot.Axes.DateTimeTicksBottomDateChange();
                chart.Plot.Axes.SetLimitsX(xMin, xMax);
                TabHelpers.LockChartVerticalAxis(chart);
                chart.Refresh();
            }

            MemoryBrokerChart.Plot.YLabel("Currently Allocated");
            MemoryBrokerRatioChart.Plot.YLabel("Value");
        }

        private void MemoryBrokerFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            ShowFilterPopup(button, columnName, "MemoryBroker", _memoryBrokerFilters,
                args => { },
                () => { });
        }

        private void ApplyMemoryBrokerFilters()
        {
            if (_memoryBrokerUnfilteredData == null)
            {
                _memoryBrokerUnfilteredData = MemoryBrokerDataGrid.ItemsSource as List<HealthParserMemoryBrokerItem>;
                if (_memoryBrokerUnfilteredData == null && MemoryBrokerDataGrid.ItemsSource != null)
                {
                    _memoryBrokerUnfilteredData = (MemoryBrokerDataGrid.ItemsSource as IEnumerable<HealthParserMemoryBrokerItem>)?.ToList();
                }
            }

            if (_memoryBrokerUnfilteredData == null) return;

            if (_memoryBrokerFilters.Count == 0)
            {
                MemoryBrokerDataGrid.ItemsSource = _memoryBrokerUnfilteredData;
                return;
            }

            var filteredData = _memoryBrokerUnfilteredData.Where(item =>
            {
                foreach (var filter in _memoryBrokerFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            MemoryBrokerDataGrid.ItemsSource = filteredData;
        }

        private void UpdateMemoryBrokerFilterButtonStyles()
        {
            foreach (var columnName in new[] { "CollectionTime", "Broker", "Notification", "MemoryRatio",
                "CurrentlyAllocated", "PreviouslyAllocated", "NewTarget", "Overall", "Rate", "DeltaTime", "BrokerId" })
            {
                UpdateFilterButtonStyle(MemoryBrokerDataGrid, columnName, _memoryBrokerFilters);
            }
        }

        private void MemoryBrokerFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(MemoryBrokerDataGrid, sender as TextBox);
        }

        private void MemoryBrokerNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(MemoryBrokerDataGrid, sender as TextBox);
        }

        #endregion
    }
}

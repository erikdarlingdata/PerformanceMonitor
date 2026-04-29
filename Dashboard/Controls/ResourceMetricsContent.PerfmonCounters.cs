/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Windows.Data;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitorDashboard.Helpers;
using ScottPlot.WPF;


namespace PerformanceMonitorDashboard.Controls
{
    public partial class ResourceMetricsContent : UserControl
    {
        #region Perfmon Counters Tab

        private bool _isUpdatingPerfmonSelection = false;

        private void PerfmonCountersList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            // Not used - we handle via checkbox changes instead
        }

        private async void PerfmonCounter_CheckChanged(object sender, RoutedEventArgs e)
        {
            if (_isUpdatingPerfmonSelection) return;
            RefreshPerfmonCounterListOrder();
            await UpdatePerfmonCountersChartAsync();
        }

        private void RefreshPerfmonCounterListOrder()
        {
            if (_perfmonCounterItems == null) return;
            // Sort: checked items first, then alphabetically
            var sorted = _perfmonCounterItems
                .OrderByDescending(x => x.IsSelected)
                .ThenBy(x => x.CounterName)
                .ToList();
            _perfmonCounterItems = sorted;
            ApplyPerfmonCounterSearchFilter();
        }

        private void PerfmonCounterSearch_TextChanged(object sender, TextChangedEventArgs e)
        {
            ApplyPerfmonCounterSearchFilter();
        }

        private void ApplyPerfmonCounterSearchFilter()
        {
            if (_perfmonCounterItems == null)
            {
                PerfmonCountersList.ItemsSource = null;
                return;
            }

            var searchText = PerfmonCounterSearchBox?.Text?.Trim() ?? string.Empty;
            if (string.IsNullOrEmpty(searchText))
            {
                PerfmonCountersList.ItemsSource = null;
                PerfmonCountersList.ItemsSource = _perfmonCounterItems;
            }
            else
            {
                var filtered = _perfmonCounterItems
                    .Where(c => c.CounterName.Contains(searchText, StringComparison.OrdinalIgnoreCase) ||
                                c.ObjectName.Contains(searchText, StringComparison.OrdinalIgnoreCase))
                    .ToList();
                PerfmonCountersList.ItemsSource = null;
                PerfmonCountersList.ItemsSource = filtered;
            }
        }

        private async void PerfmonCounters_SelectAll_Click(object sender, RoutedEventArgs e)
        {
            if (_perfmonCounterItems == null) return;
            _isUpdatingPerfmonSelection = true;
            foreach (var item in _perfmonCounterItems)
            {
                item.IsSelected = true;
            }
            _isUpdatingPerfmonSelection = false;
            await UpdatePerfmonCountersChartAsync();
        }

        private async void PerfmonCounters_ClearAll_Click(object sender, RoutedEventArgs e)
        {
            if (_perfmonCounterItems == null) return;
            _isUpdatingPerfmonSelection = true;
            foreach (var item in _perfmonCounterItems)
            {
                item.IsSelected = false;
            }
            _isUpdatingPerfmonSelection = false;
            await UpdatePerfmonCountersChartAsync();
        }

        private async void PerfmonPack_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_perfmonCounterItems == null || _perfmonCounterItems.Count == 0) return;
            if (PerfmonPackCombo.SelectedItem is not string pack) return;

            _isUpdatingPerfmonSelection = true;

            /* Clear search so all counters are visible */
            if (PerfmonCounterSearchBox != null)
                PerfmonCounterSearchBox.Text = "";

            /* Uncheck everything first */
            foreach (var item in _perfmonCounterItems)
                item.IsSelected = false;

            if (pack == PerfmonPacks.AllCounters)
            {
                /* "All Counters" selects the General Throughput defaults */
                var defaultSet = new HashSet<string>(PerfmonPacks.Packs["General Throughput"], StringComparer.OrdinalIgnoreCase);
                foreach (var item in _perfmonCounterItems)
                {
                    if (defaultSet.Contains(item.CounterName))
                        item.IsSelected = true;
                }
            }
            else if (PerfmonPacks.Packs.TryGetValue(pack, out var packCounters))
            {
                var packSet = new HashSet<string>(packCounters, StringComparer.OrdinalIgnoreCase);
                int count = 0;
                foreach (var item in _perfmonCounterItems)
                {
                    if (count >= 12) break;
                    if (packSet.Contains(item.CounterName))
                    {
                        item.IsSelected = true;
                        count++;
                    }
                }
            }

            _isUpdatingPerfmonSelection = false;
            RefreshPerfmonCounterListOrder();
            await UpdatePerfmonCountersChartAsync();
        }

        private async void PerfmonCounters_Refresh_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                await RefreshPerfmonCountersTabAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing perfmon counters: {ex.Message}", ex);
            }
        }

        private async Task RefreshPerfmonCountersTabAsync()
        {
            if (_databaseService == null) return;

            /* Initialize pack ComboBox once */
            if (PerfmonPackCombo.Items.Count == 0)
            {
                PerfmonPackCombo.ItemsSource = PerfmonPacks.PackNames;
                PerfmonPackCombo.SelectedItem = "General Throughput";
            }

            try
            {
                // Lightweight query: get only distinct counter names for the picker
                var counterNames = await _databaseService.GetPerfmonCounterNamesAsync(_perfmonCountersHoursBack, _perfmonCountersFromDate, _perfmonCountersToDate);

                // Remember previously selected counters
                var previouslySelected = _perfmonCounterItems?.Where(x => x.IsSelected).Select(x => x.FullName).ToHashSet() ?? new HashSet<string>();

                // Build unique counter list from lightweight query
                var counters = counterNames
                    .OrderBy(c => c.ObjectName)
                    .ThenBy(c => c.CounterName)
                    .Select(c => new PerfmonCounterSelectionItem
                    {
                        ObjectName = c.ObjectName,
                        CounterName = c.CounterName,
                        IsSelected = previouslySelected.Contains($"{c.ObjectName} - {c.CounterName}")
                    })
                    .ToList();

                // If nothing was previously selected, default select General Throughput pack
                if (!counters.Any(c => c.IsSelected))
                {
                    var defaultCounters = PerfmonPacks.Packs["General Throughput"];
                    var defaultSet = new HashSet<string>(defaultCounters, StringComparer.OrdinalIgnoreCase);
                    foreach (var item in counters.Where(c => defaultSet.Contains(c.CounterName)))
                    {
                        item.IsSelected = true;
                    }
                }

                _perfmonCounterItems = counters;
                // Sort so checked items appear at top
                RefreshPerfmonCounterListOrder();

                // Fetch data only for selected counters
                await UpdatePerfmonCountersChartAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading perfmon counters: {ex.Message}");
            }
        }

        private async Task UpdatePerfmonCountersChartAsync()
        {
            if (_databaseService == null || _perfmonCounterItems == null) return;

            var selectedCounterNames = _perfmonCounterItems
                .Where(x => x.IsSelected)
                .Select(x => x.CounterName)
                .Distinct()
                .ToArray();

            if (selectedCounterNames.Length == 0)
            {
                _allPerfmonCountersData = new List<PerfmonStatsItem>();
            }
            else
            {
                var data = await _databaseService.GetPerfmonStatsFilteredAsync(
                    selectedCounterNames, _perfmonCountersHoursBack, _perfmonCountersFromDate, _perfmonCountersToDate);
                _allPerfmonCountersData = data?.ToList() ?? new List<PerfmonStatsItem>();
            }

            LoadPerfmonCountersChart(_allPerfmonCountersData, _perfmonCountersHoursBack, _perfmonCountersFromDate, _perfmonCountersToDate);
        }

        private void LoadPerfmonCountersChart(List<PerfmonStatsItem>? data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(PerfmonCountersChart, out var existingPerfmonPanel) && existingPerfmonPanel != null)
            {
                PerfmonCountersChart.Plot.Axes.Remove(existingPerfmonPanel);
                _legendPanels[PerfmonCountersChart] = null;
            }
            PerfmonCountersChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(PerfmonCountersChart);
            _perfmonHover?.Clear();

            if (data == null || data.Count == 0 || _perfmonCounterItems == null)
            {
                PerfmonCountersChart.Refresh();
                return;
            }

            // Get selected counters
            var selectedCounters = _perfmonCounterItems.Where(x => x.IsSelected).ToList();
            if (selectedCounters.Count == 0)
            {
                PerfmonCountersChart.Refresh();
                return;
            }

            var colors = TabHelpers.ChartColors;

            // Get all time points across all counters for gap filling
            int colorIndex = 0;
            foreach (var counter in selectedCounters.Take(12)) // Limit to 12 counters
            {
                // Get data for this counter (aggregated across all instances)
                var counterData = data
                    .Where(d => d.ObjectName == counter.ObjectName && d.CounterName == counter.CounterName)
                    .GroupBy(d => d.CollectionTime)
                    .Select(g => new {
                        CollectionTime = g.Key,
                        Value = g.Sum(x => x.CntrValuePerSecond ?? x.CntrValueDelta ?? x.CntrValue)
                    })
                    .OrderBy(d => d.CollectionTime)
                    .ToList();

                if (counterData.Count >= 1)
                {
                    var timePoints = counterData.Select(d => d.CollectionTime);
                    var values = counterData.Select(d => (double)d.Value);
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                    var scatter = PerfmonCountersChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5; // Show small markers to ensure visibility
                    scatter.Color = colors[colorIndex % colors.Length];
                    scatter.LegendText = counter.CounterName;
                    _perfmonHover?.Add(scatter, counter.CounterName);

                    colorIndex++;
                }
            }

            if (colorIndex > 0)
            {
                _legendPanels[PerfmonCountersChart] = PerfmonCountersChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                PerfmonCountersChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = PerfmonCountersChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            PerfmonCountersChart.Plot.Axes.DateTimeTicksBottomDateChange();
            PerfmonCountersChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(PerfmonCountersChart);
            PerfmonCountersChart.Plot.YLabel("Value/sec");
            TabHelpers.LockChartVerticalAxis(PerfmonCountersChart);
            PerfmonCountersChart.Refresh();
        }

        #endregion
    }
}

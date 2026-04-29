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
        #region Wait Stats Detail Tab

        private void WaitTypesList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            // Not used - we handle via checkbox changes instead
        }

        private async void WaitType_CheckChanged(object sender, RoutedEventArgs e)
        {
            if (_isUpdatingWaitTypeSelection) return;
            RefreshWaitTypeListOrder();
            await UpdateWaitStatsDetailChartAsync();
        }

        private void AddWaitDrillDownMenuItem(ScottPlot.WPF.WpfPlot chart, ContextMenu contextMenu)
        {
            contextMenu.Items.Insert(0, new Separator());
            var drillDownItem = new MenuItem { Header = "Show Queries With This Wait" };
            drillDownItem.Click += ShowQueriesForWaitType_Click;
            contextMenu.Items.Insert(0, drillDownItem);

            contextMenu.Opened += (s, _) =>
            {
                var pos = System.Windows.Input.Mouse.GetPosition(chart);
                var nearest = _waitStatsHover?.GetNearestSeries(pos);
                if (nearest.HasValue)
                {
                    drillDownItem.Tag = (nearest.Value.Label, nearest.Value.Time);
                    drillDownItem.Header = $"Show Queries With {nearest.Value.Label.Replace("_", "__")}";
                    drillDownItem.IsEnabled = true;
                }
                else
                {
                    drillDownItem.Tag = null;
                    drillDownItem.Header = "Show Queries With This Wait";
                    drillDownItem.IsEnabled = false;
                }
            };
        }

        private void ShowQueriesForWaitType_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not MenuItem menuItem) return;
            if (menuItem.Tag is not ValueTuple<string, DateTime> tag) return;
            if (_databaseService == null) return;

            // ±15 minute window around the clicked point
            var fromDate = tag.Item2.AddMinutes(-15);
            var toDate = tag.Item2.AddMinutes(15);

            var window = new WaitDrillDownWindow(
                _databaseService, tag.Item1, 1, fromDate, toDate);
            window.Owner = Window.GetWindow(this);
            window.ShowDialog();
        }

        private void WaitStatsMetric_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (_allWaitStatsDetailData != null)
                LoadWaitStatsDetailChart(_allWaitStatsDetailData, _waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);
        }

        private void RefreshWaitTypeListOrder()
        {
            if (_waitTypeItems == null) return;
            // Sort: checked items first, then alphabetically
            var sorted = _waitTypeItems
                .OrderByDescending(x => x.IsSelected)
                .ThenBy(x => x.WaitType)
                .ToList();
            _waitTypeItems = sorted;
            ApplyWaitTypeSearchFilter();
            UpdateWaitTypeCount();
        }

        private void UpdateWaitTypeCount()
        {
            if (_waitTypeItems == null || WaitTypeCountText == null) return;
            int count = _waitTypeItems.Count(x => x.IsSelected);
            WaitTypeCountText.Text = $"{count} / 30 selected";
            WaitTypeCountText.Foreground = count >= 30
                ? new System.Windows.Media.SolidColorBrush((System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#E57373")!)
                : (System.Windows.Media.Brush)FindResource("ForegroundBrush");
        }

        private void WaitTypeSearch_TextChanged(object sender, TextChangedEventArgs e)
        {
            ApplyWaitTypeSearchFilter();
        }

        private void ApplyWaitTypeSearchFilter()
        {
            if (_waitTypeItems == null)
            {
                WaitTypesList.ItemsSource = null;
                return;
            }

            var searchText = WaitTypeSearchBox?.Text?.Trim() ?? string.Empty;
            if (string.IsNullOrEmpty(searchText))
            {
                WaitTypesList.ItemsSource = null;
                WaitTypesList.ItemsSource = _waitTypeItems;
            }
            else
            {
                var filtered = _waitTypeItems
                    .Where(c => c.WaitType.Contains(searchText, StringComparison.OrdinalIgnoreCase))
                    .ToList();
                WaitTypesList.ItemsSource = null;
                WaitTypesList.ItemsSource = filtered;
            }
        }

        private async void WaitTypes_SelectAll_Click(object sender, RoutedEventArgs e)
        {
            if (_waitTypeItems == null) return;
            _isUpdatingWaitTypeSelection = true;
            var topWaits = TabHelpers.GetDefaultWaitTypes(_waitTypeItems.Select(x => x.WaitType).ToList());
            foreach (var item in _waitTypeItems)
            {
                item.IsSelected = topWaits.Contains(item.WaitType);
            }
            _isUpdatingWaitTypeSelection = false;
            RefreshWaitTypeListOrder();
            await UpdateWaitStatsDetailChartAsync();
        }

        private async void WaitTypes_ClearAll_Click(object sender, RoutedEventArgs e)
        {
            if (_waitTypeItems == null) return;
            _isUpdatingWaitTypeSelection = true;
            foreach (var item in _waitTypeItems)
            {
                item.IsSelected = false;
            }
            _isUpdatingWaitTypeSelection = false;
            RefreshWaitTypeListOrder();
            await UpdateWaitStatsDetailChartAsync();
        }

        private async void WaitStatsDetail_Refresh_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                await RefreshWaitStatsDetailTabAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing wait stats detail: {ex.Message}", ex);
            }
        }

        private async Task RefreshWaitStatsDetailTabAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Lightweight query: get only distinct wait type names for the picker
                var waitTypeNames = await _databaseService.GetWaitTypeNamesAsync(_waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);

                // Remember previously selected wait types
                var previouslySelected = _waitTypeItems?.Where(x => x.IsSelected).Select(x => x.WaitType).ToHashSet() ?? new HashSet<string>();

                // Build unique wait type list, sorted by total wait time descending
                var waitTypes = waitTypeNames
                    .Select(w => new WaitTypeSelectionItem
                    {
                        WaitType = w.WaitType,
                        IsSelected = previouslySelected.Contains(w.WaitType)
                    })
                    .ToList();

                // Ensure poison waits are always in the picker even if they have no collected data
                foreach (var poisonWait in TabHelpers.PoisonWaits)
                {
                    if (!waitTypes.Any(w => string.Equals(w.WaitType, poisonWait, StringComparison.OrdinalIgnoreCase)))
                    {
                        waitTypes.Add(new WaitTypeSelectionItem
                        {
                            WaitType = poisonWait,
                            IsSelected = previouslySelected.Contains(poisonWait)
                        });
                    }
                }

                // If nothing was previously selected, apply poison waits + usual suspects + top 10
                if (!waitTypes.Any(w => w.IsSelected))
                {
                    var topWaits = TabHelpers.GetDefaultWaitTypes(waitTypes.Select(w => w.WaitType).ToList());
                    foreach (var item in waitTypes.Where(w => topWaits.Contains(w.WaitType)))
                    {
                        item.IsSelected = true;
                    }
                }

                _waitTypeItems = waitTypes;
                // Sort so checked items appear at top
                RefreshWaitTypeListOrder();

                // Fetch data only for selected wait types
                await UpdateWaitStatsDetailChartAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading wait stats detail: {ex.Message}");
            }
        }

        private async Task UpdateWaitStatsDetailChartAsync()
        {
            if (_databaseService == null || _waitTypeItems == null) return;

            var selectedWaitTypes = _waitTypeItems
                .Where(x => x.IsSelected)
                .Select(x => x.WaitType)
                .ToArray();

            if (selectedWaitTypes.Length == 0)
            {
                _allWaitStatsDetailData = new List<WaitStatsDataPoint>();
            }
            else
            {
                var data = await _databaseService.GetWaitStatsDataForTypesAsync(
                    selectedWaitTypes, _waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);
                _allWaitStatsDetailData = data?.ToList() ?? new List<WaitStatsDataPoint>();
            }

            LoadWaitStatsDetailChart(_allWaitStatsDetailData, _waitStatsDetailHoursBack, _waitStatsDetailFromDate, _waitStatsDetailToDate);
        }

        private void LoadWaitStatsDetailChart(List<WaitStatsDataPoint>? data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(WaitStatsDetailChart, out var existingWaitStatsPanel) && existingWaitStatsPanel != null)
            {
                WaitStatsDetailChart.Plot.Axes.Remove(existingWaitStatsPanel);
                _legendPanels[WaitStatsDetailChart] = null;
            }
            bool useAvgPerWait = WaitStatsMetricCombo?.SelectedIndex == 1;

            WaitStatsDetailChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(WaitStatsDetailChart);
            _waitStatsHover?.Clear();
            if (_waitStatsHover != null) _waitStatsHover.Unit = useAvgPerWait ? "ms/wait" : "ms/sec";

            if (data == null || data.Count == 0 || _waitTypeItems == null)
            {
                WaitStatsDetailChart.Refresh();
                return;
            }

            // Get selected wait types
            var selectedWaitTypes = _waitTypeItems.Where(x => x.IsSelected).ToList();
            if (selectedWaitTypes.Count == 0)
            {
                WaitStatsDetailChart.Refresh();
                return;
            }
            var colors = TabHelpers.ChartColors;

            // Get all time points across all wait types for gap filling
            int colorIndex = 0;
            foreach (var waitType in selectedWaitTypes.Take(20)) // Limit to 20 wait types
            {
                // Get data for this wait type
                var waitTypeData = data
                    .Where(d => d.WaitType == waitType.WaitType)
                    .GroupBy(d => d.CollectionTime)
                    .Select(g => new {
                        CollectionTime = g.Key,
                        WaitTimeMsPerSecond = g.Sum(x => x.WaitTimeMsPerSecond),
                        AvgMsPerWait = g.Average(x => x.AvgMsPerWait)
                    })
                    .OrderBy(d => d.CollectionTime)
                    .ToList();

                if (waitTypeData.Count >= 1)
                {
                    var timePoints = waitTypeData.Select(d => d.CollectionTime);
                    var values = useAvgPerWait
                        ? waitTypeData.Select(d => (double)d.AvgMsPerWait)
                        : waitTypeData.Select(d => (double)d.WaitTimeMsPerSecond);
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                    var scatter = WaitStatsDetailChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];

                    // Truncate legend text if too long
                    string legendText = waitType.WaitType;
                    if (legendText.Length > 25)
                        legendText = legendText.Substring(0, 22) + "...";
                    scatter.LegendText = legendText;
                    _waitStatsHover?.Add(scatter, waitType.WaitType);

                    colorIndex++;
                }
            }

            if (colorIndex > 0)
            {
                _legendPanels[WaitStatsDetailChart] = WaitStatsDetailChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                WaitStatsDetailChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = WaitStatsDetailChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            WaitStatsDetailChart.Plot.Axes.DateTimeTicksBottomDateChange();
            WaitStatsDetailChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(WaitStatsDetailChart);
            WaitStatsDetailChart.Plot.YLabel(useAvgPerWait ? "Avg Wait Time (ms/wait)" : "Wait Time (ms/sec)");
            TabHelpers.LockChartVerticalAxis(WaitStatsDetailChart);
            WaitStatsDetailChart.Refresh();
        }

        #endregion
    }
}

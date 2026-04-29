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
    public partial class MemoryContent : UserControl
    {
        #region Memory Clerks

        private async System.Threading.Tasks.Task RefreshMemoryClerksAsync()
        {
            if (_databaseService == null) return;

            try
            {
                if (!MemoryClerksChart.Plot.GetPlottables().Any())
                {
                    MemoryClerksLoading.IsLoading = true;
                    MemoryClerksNoDataMessage.Visibility = Visibility.Collapsed;
                }

                var clerkTypes = await _databaseService.GetDistinctMemoryClerkTypesAsync(_memoryClerksHoursBack, _memoryClerksFromDate, _memoryClerksToDate);
                PopulateMemoryClerkPicker(clerkTypes);
                await UpdateMemoryClerksChartFromPickerAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory clerks: {ex.Message}");
            }
            finally
            {
                MemoryClerksLoading.IsLoading = false;
            }
        }

        private void PopulateMemoryClerkPicker(List<string> clerkTypes)
        {
            var previouslySelected = new HashSet<string>(_memoryClerkItems.Where(i => i.IsSelected).Select(i => i.DisplayName));
            var topClerks = previouslySelected.Count == 0 ? new HashSet<string>(clerkTypes.Take(5)) : null;
            _memoryClerkItems = clerkTypes.Select(c => new SelectableItem
            {
                DisplayName = c,
                IsSelected = previouslySelected.Contains(c) || (topClerks != null && topClerks.Contains(c))
            }).ToList();
            RefreshMemoryClerkListOrder();
        }

        private void RefreshMemoryClerkListOrder()
        {
            if (_memoryClerkItems == null) return;
            _memoryClerkItems = _memoryClerkItems
                .OrderByDescending(x => x.IsSelected)
                .ThenBy(x => x.DisplayName)
                .ToList();
            ApplyMemoryClerkFilter();
            UpdateMemoryClerkCount();
        }

        private void UpdateMemoryClerkCount()
        {
            if (_memoryClerkItems == null || MemoryClerkCountText == null) return;
            int count = _memoryClerkItems.Count(x => x.IsSelected);
            MemoryClerkCountText.Text = $"{count} selected";
        }

        private void ApplyMemoryClerkFilter()
        {
            var search = MemoryClerkSearchBox?.Text?.Trim() ?? "";
            MemoryClerksList.ItemsSource = null;
            if (string.IsNullOrEmpty(search))
                MemoryClerksList.ItemsSource = _memoryClerkItems;
            else
                MemoryClerksList.ItemsSource = _memoryClerkItems.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
        }

        private void MemoryClerkSearch_TextChanged(object sender, TextChangedEventArgs e) => ApplyMemoryClerkFilter();

        private void MemoryClerkSelectTop_Click(object sender, RoutedEventArgs e)
        {
            _isUpdatingMemoryClerkSelection = true;
            var topClerks = new HashSet<string>(_memoryClerkItems.Take(5).Select(x => x.DisplayName));
            foreach (var item in _memoryClerkItems)
                item.IsSelected = topClerks.Contains(item.DisplayName);
            _isUpdatingMemoryClerkSelection = false;
            RefreshMemoryClerkListOrder();
            _ = UpdateMemoryClerksChartFromPickerAsync();
        }

        private void MemoryClerkClearAll_Click(object sender, RoutedEventArgs e)
        {
            _isUpdatingMemoryClerkSelection = true;
            var visible = (MemoryClerksList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _memoryClerkItems;
            foreach (var item in visible) item.IsSelected = false;
            _isUpdatingMemoryClerkSelection = false;
            RefreshMemoryClerkListOrder();
            _ = UpdateMemoryClerksChartFromPickerAsync();
        }

        private void MemoryClerk_CheckChanged(object sender, RoutedEventArgs e)
        {
            if (_isUpdatingMemoryClerkSelection) return;
            RefreshMemoryClerkListOrder();
            _ = UpdateMemoryClerksChartFromPickerAsync();
        }

        private async System.Threading.Tasks.Task UpdateMemoryClerksChartFromPickerAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var selected = _memoryClerkItems.Where(i => i.IsSelected).Take(20).ToList();

                if (_legendPanels.TryGetValue(MemoryClerksChart, out var existingPanel) && existingPanel != null)
                {
                    MemoryClerksChart.Plot.Axes.Remove(existingPanel);
                    _legendPanels[MemoryClerksChart] = null;
                }
                MemoryClerksChart.Plot.Clear();
                _memoryClerksHover?.Clear();
                TabHelpers.ApplyThemeToChart(MemoryClerksChart);

                DateTime rangeEnd = _memoryClerksToDate ?? Helpers.ServerTimeHelper.ServerNow;
                DateTime rangeStart = _memoryClerksFromDate ?? rangeEnd.AddHours(-_memoryClerksHoursBack);
                double xMin = rangeStart.ToOADate();
                double xMax = rangeEnd.ToOADate();

                if (selected.Count > 0)
                {
                    var selectedTypes = selected.Select(s => s.DisplayName).ToList();
                    var data = await _databaseService.GetMemoryClerksByTypesAsync(selectedTypes, _memoryClerksHoursBack, _memoryClerksFromDate, _memoryClerksToDate);
                    var dataList = data.ToList();

                    MemoryClerksNoDataMessage.Visibility = dataList.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                    if (dataList.Count > 0)
                    {
                        var colors = TabHelpers.ChartColors;
                        int colorIndex = 0;

                        foreach (var clerkType in selectedTypes)
                        {
                            var clerkData = dataList.Where(d => d.ClerkType == clerkType)
                                .OrderBy(d => d.CollectionTime)
                                .ToList();

                            if (clerkData.Count >= 1)
                            {
                                var timePoints = clerkData.Select(d => d.CollectionTime);
                                var values = clerkData.Select(d => (double)d.PagesMb);
                                var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, values);

                                var scatter = MemoryClerksChart.Plot.Add.Scatter(xs, ys);
                                scatter.LineWidth = 2;
                                scatter.MarkerSize = 5;
                                scatter.Color = colors[colorIndex % colors.Length];
                                var label = clerkType.Length > 20 ? clerkType.Substring(0, 20) + "..." : clerkType;
                                scatter.LegendText = label;
                                _memoryClerksHover?.Add(scatter, label);
                                colorIndex++;
                            }
                        }

                        _legendPanels[MemoryClerksChart] = MemoryClerksChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                        MemoryClerksChart.Plot.Legend.FontSize = 12;
                    }

                    UpdateMemoryClerksSummaryPanel(dataList);
                }
                else
                {
                    MemoryClerksNoDataMessage.Visibility = Visibility.Collapsed;
                    MemoryClerksTotalText.Text = "N/A";
                    MemoryClerksTopText.Text = "N/A";
                }

                MemoryClerksChart.Plot.Axes.DateTimeTicksBottomDateChange();
                MemoryClerksChart.Plot.Axes.SetLimitsX(xMin, xMax);
                MemoryClerksChart.Plot.YLabel("MB");
                MemoryClerksChart.Plot.Axes.AutoScaleY();
                var clerksLimits = MemoryClerksChart.Plot.Axes.GetLimits();
                MemoryClerksChart.Plot.Axes.SetLimitsY(0, clerksLimits.Top * 1.05);
                TabHelpers.LockChartVerticalAxis(MemoryClerksChart);
                MemoryClerksChart.Refresh();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error updating memory clerks chart: {ex.Message}");
            }
        }

        private void UpdateMemoryClerksSummaryPanel(List<MemoryClerksItem> dataList)
        {
            if (dataList == null || dataList.Count == 0)
            {
                MemoryClerksTotalText.Text = "N/A";
                MemoryClerksTopText.Text = "N/A";
                return;
            }

            var latestTime = dataList.Max(d => d.CollectionTime);
            var latestData = dataList
                .Where(d => d.CollectionTime == latestTime)
                .Where(d => d.ClerkType == null || !d.ClerkType.Contains("BUFFERPOOL", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var totalMb = latestData.Sum(d => d.PagesMb);
            MemoryClerksTotalText.Text = string.Format(CultureInfo.CurrentCulture, "{0:N0} MB", totalMb);

            var topClerk = latestData.OrderByDescending(d => d.PagesMb).FirstOrDefault();
            if (topClerk != null)
            {
                var name = topClerk.ClerkType ?? "Unknown";
                if (name.StartsWith("MEMORYCLERK_", StringComparison.OrdinalIgnoreCase))
                    name = name.Substring(12);
                if (name.Length > 20) name = name.Substring(0, 20) + "...";
                MemoryClerksTopText.Text = string.Format(CultureInfo.CurrentCulture, "{0} ({1:N0} MB)", name, topClerk.PagesMb);
            }
            else
            {
                MemoryClerksTopText.Text = "N/A";
            }
        }

        #endregion
    }
}

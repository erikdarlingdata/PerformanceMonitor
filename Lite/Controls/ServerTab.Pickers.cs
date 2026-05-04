/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private static readonly HashSet<string> _defaultPerfmonCounters = new(
        Helpers.PerfmonPacks.Packs["General Throughput"],
        StringComparer.OrdinalIgnoreCase);

    /* ========== Wait Stats Picker ========== */

    private static readonly string[] PoisonWaits = { "THREADPOOL", "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE" };
    private static readonly string[] UsualSuspectWaits = { "SOS_SCHEDULER_YIELD", "CXPACKET", "CXCONSUMER", "PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "WRITELOG" };
    private static readonly string[] UsualSuspectPrefixes = { "PAGELATCH_" };

    private static HashSet<string> GetDefaultWaitTypes(List<string> availableWaitTypes)
    {
        var defaults = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var w in PoisonWaits)
            if (availableWaitTypes.Contains(w)) defaults.Add(w);
        foreach (var w in UsualSuspectWaits)
            if (availableWaitTypes.Contains(w)) defaults.Add(w);
        foreach (var prefix in UsualSuspectPrefixes)
            foreach (var w in availableWaitTypes)
                if (w.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    defaults.Add(w);
        int added = 0;
        foreach (var w in availableWaitTypes)
        {
            if (defaults.Count >= 30) break;
            if (added >= 10) break;
            if (defaults.Add(w)) { added++; }
        }
        return defaults;
    }

    private bool _isUpdatingWaitTypeSelection;

    private void PopulateWaitTypePicker(List<string> waitTypes)
    {
        var previouslySelected = new HashSet<string>(_waitTypeItems.Where(i => i.IsSelected).Select(i => i.DisplayName));
        var topWaits = previouslySelected.Count == 0 ? GetDefaultWaitTypes(waitTypes) : null;
        _waitTypeItems = waitTypes.Select(w => new SelectableItem
        {
            DisplayName = w,
            IsSelected = previouslySelected.Contains(w) || (topWaits != null && topWaits.Contains(w))
        }).ToList();
        /* Sort checked items to top, then preserve original order (by total wait time desc) */
        RefreshWaitTypeListOrder();
    }

    private void RefreshWaitTypeListOrder()
    {
        if (_waitTypeItems == null) return;
        _waitTypeItems = _waitTypeItems
            .OrderByDescending(x => x.IsSelected)
            .ThenBy(x => x.DisplayName)
            .ToList();
        ApplyWaitTypeFilter();
        UpdateWaitTypeCount();
    }

    private void UpdateWaitTypeCount()
    {
        if (_waitTypeItems == null || WaitTypeCountText == null) return;
        int count = _waitTypeItems.Count(x => x.IsSelected);
        WaitTypeCountText.Text = $"{count} / 30 selected";
        WaitTypeCountText.Foreground = count >= 30
            ? new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#E57373")!)
            : (System.Windows.Media.Brush)FindResource("ForegroundBrush");
    }

    private void ApplyWaitTypeFilter()
    {
        var search = WaitTypeSearchBox?.Text?.Trim() ?? "";
        WaitTypesList.ItemsSource = null;
        if (string.IsNullOrEmpty(search))
            WaitTypesList.ItemsSource = _waitTypeItems;
        else
            WaitTypesList.ItemsSource = _waitTypeItems.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
    }

    private void WaitTypeSearch_TextChanged(object sender, TextChangedEventArgs e) => ApplyWaitTypeFilter();

    private void WaitTypeSelectAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingWaitTypeSelection = true;
        var topWaits = GetDefaultWaitTypes(_waitTypeItems.Select(x => x.DisplayName).ToList());
        foreach (var item in _waitTypeItems)
        {
            item.IsSelected = topWaits.Contains(item.DisplayName);
        }
        _isUpdatingWaitTypeSelection = false;
        RefreshWaitTypeListOrder();
        _ = UpdateWaitStatsChartFromPickerAsync();
    }

    private void WaitTypeClearAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingWaitTypeSelection = true;
        var visible = (WaitTypesList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _waitTypeItems;
        foreach (var item in visible) item.IsSelected = false;
        _isUpdatingWaitTypeSelection = false;
        RefreshWaitTypeListOrder();
        _ = UpdateWaitStatsChartFromPickerAsync();
    }

    private void WaitStatsMetric_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        _ = UpdateWaitStatsChartFromPickerAsync();
    }

    private void WaitType_CheckChanged(object sender, RoutedEventArgs e)
    {
        if (_isUpdatingWaitTypeSelection) return;
        RefreshWaitTypeListOrder();
        _ = UpdateWaitStatsChartFromPickerAsync();
    }

    private async System.Threading.Tasks.Task UpdateWaitStatsChartFromPickerAsync()
    {
        try
        {
            var selected = _waitTypeItems.Where(i => i.IsSelected).Take(20).ToList();

            ClearChart(WaitStatsChart);
            ApplyTheme(WaitStatsChart);
            _waitStatsHover?.Clear();

            if (selected.Count == 0) { WaitStatsChart.Refresh(); return; }

            bool useAvgPerWait = WaitStatsMetricCombo?.SelectedIndex == 1;
            if (_waitStatsHover != null) _waitStatsHover.Unit = useAvgPerWait ? "ms/wait" : "ms/sec";

            var hoursBack = GetHoursBack();
            DateTime? fromDate = null;
            DateTime? toDate = null;
            if (IsCustomRange)
            {
                var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
                var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
                if (fromLocal.HasValue && toLocal.HasValue)
                {
                    fromDate = ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                    toDate = ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                }
            }
            double globalMax = 0;

            for (int i = 0; i < selected.Count; i++)
            {
                var trend = await _dataService.GetWaitStatsTrendAsync(_serverId, selected[i].DisplayName, hoursBack, fromDate, toDate);
                if (trend.Count == 0) continue;

                var times = trend.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
                var values = useAvgPerWait
                    ? trend.Select(t => t.AvgMsPerWait).ToArray()
                    : trend.Select(t => t.WaitTimeMsPerSecond).ToArray();

                var plot = WaitStatsChart.Plot.Add.Scatter(times, values);
                plot.LegendText = selected[i].DisplayName;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                _waitStatsHover?.Add(plot, selected[i].DisplayName);

                if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
            }

            WaitStatsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            DateTime rangeStart, rangeEnd;
            if (IsCustomRange && fromDate.HasValue && toDate.HasValue)
            {
                rangeStart = fromDate.Value;
                rangeEnd = toDate.Value;
            }
            else
            {
                rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
                rangeStart = rangeEnd.AddHours(-hoursBack);
            }
            WaitStatsChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(WaitStatsChart);
            WaitStatsChart.Plot.YLabel(useAvgPerWait ? "Avg Wait Time (ms/wait)" : "Wait Time (ms/sec)");
            SetChartYLimitsWithLegendPadding(WaitStatsChart, 0, globalMax > 0 ? globalMax : 100);
            ShowChartLegend(WaitStatsChart);
            WaitStatsChart.Refresh();
        }
        catch
        {
            /* Ignore chart update errors */
        }
    }

    /* ========== Memory Clerks Picker ========== */

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
        {
            item.IsSelected = topClerks.Contains(item.DisplayName);
        }
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
        try
        {
            var selected = _memoryClerkItems.Where(i => i.IsSelected).Take(20).ToList();

            ClearChart(MemoryClerksChart);
            ApplyTheme(MemoryClerksChart);
            _memoryClerksHover?.Clear();

            if (selected.Count == 0)
            {
                MemoryClerksTotalText.Text = "--";
                MemoryClerksTopText.Text = "--";
                MemoryClerksChart.Refresh();
                return;
            }

            var hoursBack = GetHoursBack();
            DateTime? fromDate = null;
            DateTime? toDate = null;
            if (IsCustomRange)
            {
                var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
                var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
                if (fromLocal.HasValue && toLocal.HasValue)
                {
                    fromDate = ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                    toDate = ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                }
            }

            double globalMax = 0;
            double nonBpTotal = 0;
            string topNonBpClerk = "";
            double topNonBpMb = 0;

            for (int i = 0; i < selected.Count; i++)
            {
                var trend = await _dataService.GetMemoryClerkTrendAsync(_serverId, selected[i].DisplayName, hoursBack, fromDate, toDate);
                if (trend.Count == 0) continue;

                var times = trend.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
                var values = trend.Select(t => t.MemoryMb).ToArray();

                var plot = MemoryClerksChart.Plot.Add.Scatter(times, values);
                plot.LegendText = selected[i].DisplayName;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                _memoryClerksHover?.Add(plot, selected[i].DisplayName);

                if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());

                /* Summary: use latest value, exclude buffer pool */
                var latestMb = values.Last();
                if (!selected[i].DisplayName.Contains("BUFFERPOOL", StringComparison.OrdinalIgnoreCase))
                {
                    nonBpTotal += latestMb;
                    if (latestMb > topNonBpMb)
                    {
                        topNonBpMb = latestMb;
                        topNonBpClerk = selected[i].DisplayName;
                    }
                }
            }

            MemoryClerksChart.Plot.Axes.DateTimeTicksBottomDateChange();
            ReapplyAxisColors(MemoryClerksChart);
            MemoryClerksChart.Plot.YLabel("Memory (MB)");
            SetChartYLimitsWithLegendPadding(MemoryClerksChart, 0, globalMax > 0 ? globalMax : 100);
            ShowChartLegend(MemoryClerksChart);
            MemoryClerksChart.Refresh();

            /* Update summary panel */
            MemoryClerksTotalText.Text = nonBpTotal >= 1024 ? $"{nonBpTotal / 1024:F1} GB" : $"{nonBpTotal:N0} MB";
            if (!string.IsNullOrEmpty(topNonBpClerk))
            {
                var name = topNonBpClerk;
                if (name.StartsWith("MEMORYCLERK_", StringComparison.OrdinalIgnoreCase))
                    name = name.Substring(12);
                MemoryClerksTopText.Text = topNonBpMb >= 1024 ? $"{name} ({topNonBpMb / 1024:F1} GB)" : $"{name} ({topNonBpMb:N0} MB)";
            }
            else
            {
                MemoryClerksTopText.Text = "--";
            }
        }
        catch
        {
            /* Ignore chart update errors */
        }
    }

    /* ========== Perfmon Picker ========== */

    private bool _isUpdatingPerfmonSelection;

    private void PopulatePerfmonPicker(List<string> counters)
    {
        /* Initialize pack ComboBox once */
        if (PerfmonPackCombo.Items.Count == 0)
        {
            PerfmonPackCombo.ItemsSource = Helpers.PerfmonPacks.PackNames;
            PerfmonPackCombo.SelectedItem = "General Throughput";
        }

        var previouslySelected = new HashSet<string>(_perfmonCounterItems.Where(i => i.IsSelected).Select(i => i.DisplayName));
        _perfmonCounterItems = counters.Select(c => new SelectableItem
        {
            DisplayName = c,
            IsSelected = previouslySelected.Contains(c)
                || (previouslySelected.Count == 0 && _defaultPerfmonCounters.Contains(c))
        }).ToList();
        RefreshPerfmonListOrder();
    }

    private void PerfmonPack_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_perfmonCounterItems == null || _perfmonCounterItems.Count == 0) return;
        if (PerfmonPackCombo.SelectedItem is not string pack) return;

        _isUpdatingPerfmonSelection = true;

        /* Clear search so all counters are visible */
        if (PerfmonSearchBox != null)
            PerfmonSearchBox.Text = "";

        /* Uncheck everything first */
        foreach (var item in _perfmonCounterItems)
            item.IsSelected = false;

        if (pack == Helpers.PerfmonPacks.AllCounters)
        {
            /* "All Counters" selects the General Throughput defaults */
            foreach (var item in _perfmonCounterItems)
            {
                if (_defaultPerfmonCounters.Contains(item.DisplayName))
                    item.IsSelected = true;
            }
        }
        else if (Helpers.PerfmonPacks.Packs.TryGetValue(pack, out var packCounters))
        {
            var packSet = new HashSet<string>(packCounters, StringComparer.OrdinalIgnoreCase);
            int count = 0;
            foreach (var item in _perfmonCounterItems)
            {
                if (count >= 12) break;
                if (packSet.Contains(item.DisplayName))
                {
                    item.IsSelected = true;
                    count++;
                }
            }
        }

        _isUpdatingPerfmonSelection = false;
        RefreshPerfmonListOrder();
        _ = UpdatePerfmonChartFromPickerAsync();
    }

    private void RefreshPerfmonListOrder()
    {
        if (_perfmonCounterItems == null) return;
        _perfmonCounterItems = _perfmonCounterItems
            .OrderByDescending(x => x.IsSelected)
            .ThenBy(x => _perfmonCounterItems.IndexOf(x))
            .ToList();
        ApplyPerfmonFilter();
    }

    private void ApplyPerfmonFilter()
    {
        var search = PerfmonSearchBox?.Text?.Trim() ?? "";
        PerfmonCountersList.ItemsSource = null;
        if (string.IsNullOrEmpty(search))
            PerfmonCountersList.ItemsSource = _perfmonCounterItems;
        else
            PerfmonCountersList.ItemsSource = _perfmonCounterItems.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
    }

    private void PerfmonSearch_TextChanged(object sender, TextChangedEventArgs e) => ApplyPerfmonFilter();

    private void PerfmonSelectAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingPerfmonSelection = true;
        var visible = (PerfmonCountersList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _perfmonCounterItems;
        int count = visible.Count(i => i.IsSelected);
        foreach (var item in visible)
        {
            if (!item.IsSelected && count < 12)
            {
                item.IsSelected = true;
                count++;
            }
        }
        _isUpdatingPerfmonSelection = false;
        RefreshPerfmonListOrder();
        _ = UpdatePerfmonChartFromPickerAsync();
    }

    private void PerfmonClearAll_Click(object sender, RoutedEventArgs e)
    {
        _isUpdatingPerfmonSelection = true;
        var visible = (PerfmonCountersList.ItemsSource as IEnumerable<SelectableItem>)?.ToList() ?? _perfmonCounterItems;
        foreach (var item in visible) item.IsSelected = false;
        _isUpdatingPerfmonSelection = false;
        RefreshPerfmonListOrder();
        _ = UpdatePerfmonChartFromPickerAsync();
    }

    private void PerfmonCounter_CheckChanged(object sender, RoutedEventArgs e)
    {
        if (_isUpdatingPerfmonSelection) return;
        RefreshPerfmonListOrder();
        _ = UpdatePerfmonChartFromPickerAsync();
    }

    private async System.Threading.Tasks.Task UpdatePerfmonChartFromPickerAsync()
    {
        try
        {
            var selected = _perfmonCounterItems.Where(i => i.IsSelected).Take(12).ToList();

            ClearChart(PerfmonChart);
            _perfmonHover?.Clear();
            ApplyTheme(PerfmonChart);

            if (selected.Count == 0) { PerfmonChart.Refresh(); return; }

            var hoursBack = GetHoursBack();
            DateTime? fromDate = null;
            DateTime? toDate = null;
            if (IsCustomRange)
            {
                var fromLocal = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
                var toLocal = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
                if (fromLocal.HasValue && toLocal.HasValue)
                {
                    fromDate = ServerTimeHelper.DisplayTimeToServerTime(fromLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                    toDate = ServerTimeHelper.DisplayTimeToServerTime(toLocal.Value, ServerTimeHelper.CurrentDisplayMode);
                }
            }
            double globalMax = 0;

            for (int i = 0; i < selected.Count; i++)
            {
                var trend = await _dataService.GetPerfmonTrendAsync(_serverId, selected[i].DisplayName, hoursBack, fromDate, toDate);
                if (trend.Count == 0) continue;

                var times = trend.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
                var values = trend.Select(t => (double)t.DeltaValue).ToArray();

                var plot = PerfmonChart.Plot.Add.Scatter(times, values);
                plot.LegendText = selected[i].DisplayName;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                _perfmonHover?.Add(plot, selected[i].DisplayName);

                if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
            }

            PerfmonChart.Plot.Axes.DateTimeTicksBottomDateChange();
            DateTime rangeStart, rangeEnd;
            if (IsCustomRange && fromDate.HasValue && toDate.HasValue)
            {
                rangeStart = fromDate.Value;
                rangeEnd = toDate.Value;
            }
            else
            {
                rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
                rangeStart = rangeEnd.AddHours(-hoursBack);
            }
            PerfmonChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(PerfmonChart);
            PerfmonChart.Plot.YLabel("Value");
            SetChartYLimitsWithLegendPadding(PerfmonChart, 0, globalMax > 0 ? globalMax : 100);
            ShowChartLegend(PerfmonChart);
            PerfmonChart.Refresh();
        }
        catch
        {
            /* Ignore chart update errors */
        }
    }
}

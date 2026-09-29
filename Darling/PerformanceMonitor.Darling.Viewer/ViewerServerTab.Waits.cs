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
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Wait Stats inner tab — a COPY of Lite's wait-stats picker + chart (ServerTab.Pickers.cs
/// lines 30-221), reads rewired to Postgres and windowed on the per-server toolbar's settable range
/// (preset or custom From/To). The picker behaviors are preserved verbatim: the poison /
/// usual-suspect / PAGELATCH_ default selection with a 30-type cap, checked-to-top ordering, the
/// search filter, "Top Waits" / "Clear All" (Clear All clears only the FILTERED visible set), the
/// reentrancy guard on programmatic checkbox writes, and the generation-guarded batched chart
/// update with its 20-series cap and ms/sec ↔ ms/wait metric swap. Series ride the shared cycling
/// <see cref="ChartPalette"/> colors and <see cref="ChartStyle.StyleScatter"/> line polish, and the
/// hover helper carries the current unit.
/// </summary>
public partial class ViewerServerTab
{
    /* ========== Wait Stats Picker ========== */

    /* SeriesColors (the shared cycling palette) is declared once in ViewerServerTab.Charts.cs. */

    private static readonly string[] PoisonWaits = { "THREADPOOL", "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE" };
    private static readonly string[] UsualSuspectWaits = { "SOS_SCHEDULER_YIELD", "CXPACKET", "CXCONSUMER", "PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "WRITELOG" };
    private static readonly string[] UsualSuspectPrefixes = { "PAGELATCH_" };

    private List<SelectableItem> _waitTypeItems = new();
    private bool _isUpdatingWaitTypeSelection;
    private int _waitStatsPickerGen;
    private ChartHoverHelper? _waitStatsHover;

    internal static HashSet<string> GetDefaultWaitTypes(List<string> availableWaitTypes)
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

    /// <summary>
    /// The Wait Stats tab load (mirrors Lite's <c>RefreshWaitStatsAsync</c>): fetch the distinct wait
    /// types over the toolbar's settable window, (re)populate the picker preserving the current selection,
    /// and redraw. The hover helper is created lazily on first load so the picker's chart carries the
    /// same tooltip + click-to-isolate behavior Lite's does.
    /// </summary>
    private async Task LoadWaitStatsAsync()
    {
        _waitStatsHover ??= new ChartHoverHelper(WaitStatsChart, "ms/sec", displayZone: ViewerTimeHelper.CurrentDisplayZone);

        var (startUtc, endUtc) = GetWindowUtc();
        var waitTypes = await _dataService.GetDistinctWaitTypesAsync(_server.ServerId, startUtc, endUtc);
        PopulateWaitTypePicker(waitTypes);
        await UpdateWaitStatsChartFromPickerAsync();
    }

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

    private async Task UpdateWaitStatsChartFromPickerAsync()
    {
        /* Bump a generation on entry; after each (now genuinely async) query, bail if a newer
           invocation has started — rapid checkbox toggling otherwise interleaves two runs and
           double-plots the series. */
        var gen = ++_waitStatsPickerGen;
        try
        {
            var selected = _waitTypeItems.Where(i => i.IsSelected).Take(20).ToList();

            ClearChart(WaitStatsChart);
            ApplyTheme(WaitStatsChart);
            _waitStatsHover?.Clear();

            if (selected.Count == 0) { WaitStatsChart.Refresh(); return; }

            bool useAvgPerWait = WaitStatsMetricCombo?.SelectedIndex == 1;
            if (_waitStatsHover != null) _waitStatsHover.Unit = useAvgPerWait ? "ms/wait" : "ms/sec";

            /* The per-server toolbar's settable window (preset or custom From/To). The store is naive-UTC and the chart
               plots it as is; the labels are drawn in ViewerTimeHelper.CurrentDisplayZone. */
            var (startUtc, endUtc) = GetWindowUtc();
            double globalMax = 0;

            // Batched fetch: one query for all selected wait types (mirrors Lite's batched read).
            var trendsByType = await _dataService.GetWaitStatsTrendsByTypesAsync(
                _server.ServerId, selected.Select(s => s.DisplayName).ToList(), startUtc, endUtc);
            if (gen != _waitStatsPickerGen) return;

            for (int i = 0; i < selected.Count; i++)
            {
                if (!trendsByType.TryGetValue(selected[i].DisplayName, out var trend) || trend.Count == 0) continue;

                var times = trend.Select(t => t.CollectionTime.ToOADate()).ToArray();
                var values = useAvgPerWait
                    ? trend.Select(t => t.AvgMsPerWait).ToArray()
                    : trend.Select(t => t.WaitTimeMsPerSecond).ToArray();

                var plot = WaitStatsChart.Plot.Add.TimeSeries(times, values);
                plot.LegendText = selected[i].DisplayName;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
                _waitStatsHover?.Add(plot, selected[i].DisplayName);

                if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
            }

            WaitStatsChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
            var rangeStart = startUtc;
            var rangeEnd = endUtc;
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

    /// <summary>
    /// Tears down the wait-stats hover helper (mirrors Lite's <c>DisposeChartHelpers</c>) so the
    /// tooltip popup and its chart event handlers don't outlive a closed server tab. Called from
    /// MainWindow's tab-close path.
    /// </summary>
    public void DisposeChartHelpers()
    {
        _waitStatsHover?.Dispose();
    }
}

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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Perfmon inner tab — a COPY of Lite's perfmon picker + chart (ServerTab.Pickers.cs lines
/// 396-593 plus the <c>_defaultPerfmonCounters</c> seed at line 26), reads rewired to Postgres and
/// windowed on the per-server toolbar's settable range (preset or custom From/To). The picker behaviors are
/// preserved verbatim: the pack ComboBox sourced from the SHARED <see cref="PerfmonPacks"/> (the same
/// packs Lite/Dashboard use), the General-Throughput default selection, the 12-counter cap on pack
/// fills / "Select All", the checked-to-top ordering, the search filter, "Clear All" clearing only the
/// FILTERED visible set, the reentrancy guard on programmatic checkbox writes, and the
/// generation-guarded batched chart update with its 12-series cap plotting <c>DeltaValue</c> — THROUGH
/// the row's stored interval since #3653 A7 (see <see cref="UpdatePerfmonChartFromPickerAsync"/>). Series
/// ride the shared cycling <see cref="ChartPalette"/> colors (via <c>SeriesColors</c>, declared in
/// ViewerServerTab.Charts.cs) and the <see cref="ChartStyle.StyleScatter"/> line polish, and the hover
/// helper carries Lite's empty unit. Lite's per-chart drill-down context menu is intentionally NOT
/// ported (the viewer has no drill-down surfaces yet).
/// </summary>
public partial class ViewerServerTab
{
    /* The default counter set is the shared "General Throughput" pack (mirrors Lite's
       _defaultPerfmonCounters). */
    private static readonly HashSet<string> _defaultPerfmonCounters = new(
        PerfmonPacks.Packs["General Throughput"],
        StringComparer.OrdinalIgnoreCase);

    private List<SelectableItem> _perfmonCounterItems = new();
    private bool _isUpdatingPerfmonSelection;
    private int _perfmonPickerGen;
    private ChartHoverHelper? _perfmonHover;

    /// <summary>
    /// The Perfmon tab load (mirrors Lite's <c>RefreshPerfmonAsync</c>): fetch the distinct counters
    /// over the toolbar's settable window, (re)populate the picker preserving the current selection, and
    /// redraw. The hover helper is created lazily on first load so the picker's chart carries the same
    /// tooltip behavior Lite's does.
    /// </summary>
    private async Task LoadPerfmonAsync()
    {
        _perfmonHover ??= new ChartHoverHelper(PerfmonChart, "");

        var (startUtc, endUtc) = GetWindowUtc();
        var counters = await _dataService.GetDistinctPerfmonCountersAsync(_server.ServerId, startUtc, endUtc);
        PopulatePerfmonPicker(counters);
        await UpdatePerfmonChartFromPickerAsync();
    }

    private void PopulatePerfmonPicker(List<string> counters)
    {
        /* Initialize pack ComboBox once */
        if (PerfmonPackCombo.Items.Count == 0)
        {
            PerfmonPackCombo.ItemsSource = PerfmonPacks.PackNames;
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

        if (pack == PerfmonPacks.AllCounters)
        {
            /* "All Counters" selects the General Throughput defaults */
            foreach (var item in _perfmonCounterItems)
            {
                if (_defaultPerfmonCounters.Contains(item.DisplayName))
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

    /// <summary>
    /// Redraws the picker's selected counters (mirrors Lite's method of the same name). Each series goes
    /// through <see cref="DeltaSeriesShaping"/> before it reaches ScottPlot (#3653 A7): the reader has
    /// fetched <c>MAX(sample_interval_seconds)</c> onto every row since #2234 and this chart plotted the raw
    /// delta beside it under a Y axis that said "Value" — <c>Batch Requests/sec</c> drew batches-per-sweep
    /// (a ~300 s sweep on the measured fleet, so ~300x the number its name promises), and a restart's
    /// fabricated (0, 0) drew as a real trough. Each series is classified by its STORED <c>cntr_type</c>
    /// (V132, #3653 A7): a rate counter plots delta / interval; a gauge plots its raw value as the level it is
    /// (the store held it all along; only the type was missing); everything else plots its raw delta with
    /// " (Δ/interval)" on its legend entry. The #3702 name-suffix proxy (<c>/sec</c> = rate) is the fallback
    /// for a series with no stored type — rows written before the rung, or a counter whose instances mix
    /// types — and never says "gauge", so history renders as it did before the rung until a row carries the
    /// type. A counter's type does not change, so the series takes ANY point's non-null type: a gauge's whole
    /// window plots as a level the morning after the upgrade. A stored interval of 0 plots NaN on a delta
    /// series, which ScottPlot draws as a line break — the restart reads as absence, composing with #1944's
    /// cadence gap rule that <c>Add.TimeSeries</c> already applies on X; a level ignores the interval. The Y
    /// label is composed from the bases actually plotted, so a mixed selection is labelled as mixed rather
    /// than under one unit. The Y ceiling ignores the NaNs (an all-restart window still gets an axis).
    /// </summary>
    private async Task UpdatePerfmonChartFromPickerAsync()
    {
        /* Bump a generation on entry; after each (now genuinely async) query, bail if a newer
           invocation has started — rapid checkbox toggling otherwise interleaves two runs and
           double-plots the series. */
        var gen = ++_perfmonPickerGen;
        try
        {
            var selected = _perfmonCounterItems.Where(i => i.IsSelected).Take(12).ToList();

            ClearChart(PerfmonChart);
            _perfmonHover?.Clear();
            ApplyTheme(PerfmonChart);

            if (selected.Count == 0) { PerfmonChart.Refresh(); return; }

            /* The per-server toolbar's settable window (preset or custom From/To). The store is naive-UTC;
               display converts via ViewerTimeHelper.ForDisplay. */
            var (startUtc, endUtc) = GetWindowUtc();
            double globalMax = 0;
            var plottedBases = new List<DeltaBasis>();

            // Batched fetch: one query for all selected counters (mirrors Lite's batched read).
            var trendsByCounter = await _dataService.GetPerfmonTrendsByCountersAsync(
                _server.ServerId, selected.Select(s => s.DisplayName).ToList(), startUtc, endUtc);
            if (gen != _perfmonPickerGen) return;

            for (int i = 0; i < selected.Count; i++)
            {
                if (!trendsByCounter.TryGetValue(selected[i].DisplayName, out var trend) || trend.Count == 0) continue;

                var counterName = selected[i].DisplayName;
                /* The series' type is any point's non-null type — a counter's type does not change, and the
                   read reports one only where the point's instance rows agree. Null on every point means
                   pre-rung rows or a mixed-type family: the name proxy decides, as it did before V132. */
                var seriesType = trend.Select(t => t.CntrType).LastOrDefault(t => t.HasValue);
                var basis = DeltaSeriesShaping.BasisFor(counterName, seriesType);
                var times = trend.Select(t => ViewerTimeHelper.ForDisplay(t.CollectionTime).ToOADate()).ToArray();
                var values = DeltaSeriesShaping.Shape(
                    trend.Select(t => new DeltaSample(t.CollectionTime, t.DeltaValue, t.SampleIntervalSeconds, t.Value)).ToList(),
                    basis);
                var label = DeltaSeriesShaping.LegendLabel(counterName, basis);

                var plot = PerfmonChart.Plot.Add.TimeSeries(times, values);
                plot.LegendText = label;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
                _perfmonHover?.Add(plot, label);
                plottedBases.Add(basis);

                globalMax = Math.Max(globalMax, DeltaSeriesShaping.MaxFinite(values, 0));
            }

            PerfmonChart.Plot.Axes.DateTimeTicksBottomDateChange();
            var rangeStart = ViewerTimeHelper.ForDisplay(startUtc);
            var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc);
            PerfmonChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(PerfmonChart);
            PerfmonChart.Plot.YLabel(DeltaSeriesShaping.YAxisLabel(plottedBases));
            SetChartYLimitsWithLegendPadding(PerfmonChart, 0, globalMax > 0 ? globalMax : 100);
            ShowChartLegend(PerfmonChart);
            PerfmonChart.Refresh();
        }
        catch
        {
            /* Ignore chart update errors */
        }
    }

    /// <summary>
    /// Tears down the perfmon hover helper (mirrors Lite's <c>DisposeChartHelpers</c>) so the tooltip
    /// popup and its chart event handlers don't outlive a closed server tab. Called from the tab's
    /// single Dispose path in ViewerServerTab.Charts.cs.
    /// </summary>
    public void DisposePerfmonHelpers()
    {
        _perfmonHover?.Dispose();
    }
}

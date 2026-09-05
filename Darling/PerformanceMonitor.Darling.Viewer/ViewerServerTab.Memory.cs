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
/// The Memory inner tab — a COPY of Lite's Memory tab (four sub-tabs: Overview, Memory Clerks, Memory
/// Grants, Memory Pressure Events), with the summary + chart bodies from Lite's <c>ServerTab.Charts.cs</c>
/// (<c>UpdateMemorySummary</c> / <c>UpdateMemoryChart</c> / <c>UpdateMemoryGrantCharts</c> /
/// <c>UpdateMemoryPressureEventsChart</c>) and the clerk picker from Lite's <c>ServerTab.Pickers.cs</c>
/// (<c>PopulateMemoryClerkPicker</c> … <c>UpdateMemoryClerksChartFromPickerAsync</c>), reads rewired to
/// <see cref="ViewerDataService"/> Postgres. The only render-body changes: the time axis runs every point
/// through <see cref="ViewerTimeHelper.ForDisplay"/> (where Lite shifts by its per-server
/// <c>UtcOffsetMinutes</c>), and Lite's per-chart context menu / "Show Active Queries at This Time"
/// drill-down + <c>Task.Run</c> query wrappers are dropped (the viewer's reads are genuinely async and it
/// has no drill-down surfaces yet); the hover tooltips are kept. The clerk picker mirrors the wait/perfmon
/// pickers' reentrancy guard + generation-guarded chart update. Chart chrome/legend/line polish flow
/// through the shared <see cref="ChartStyle"/> / <see cref="ChartPalette"/> and the
/// <c>ViewerServerTab.ChartHelpers.cs</c> bridge, and the per-pool / per-clerk series ride the shared
/// cycling <c>SeriesColors</c> (declared in ViewerServerTab.Charts.cs).
///
/// <para>First cut: all four sub-tabs (re)load on Memory-tab activation (Lite's full-refresh branch);
/// Lite's sub-tab-only timer optimization is deferred, so the sub-TabControl has no SelectionChanged
/// handler. <b>Pressure-event time:</b> unlike CPU's <c>sample_time</c>, which is the monitored server's
/// LOCAL wall clock and needs the per-batch de-skew (#1262), the pressure <c>sample_time</c> is naive UTC —
/// <c>MemoryPressureEventsCollector</c> stamps it from <c>SYSUTCDATETIME()</c> and
/// <c>CollectorTimestampFrameTests</c> pins that it must. So the chart windows AND plots on raw
/// <c>sample_time</c> through <see cref="ViewerTimeHelper.ForDisplay"/>, which takes naive-UTC input, and
/// needs no de-skew of its own: the CPU read's correction assumes the newest sample is ~1 minute old, true
/// for the dense CPU ring buffer and not for the sparse resource-monitor one, and would be wrong here even
/// if the frame called for it.</para>
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _memoryHover;
    private ChartHoverHelper? _memoryClerksHover;
    private ChartHoverHelper? _memoryGrantSizingHover;
    private ChartHoverHelper? _memoryGrantActivityHover;
    private ChartHoverHelper? _memoryPressureEventsHover;

    /* Clerk picker state (mirrors the perfmon picker in ViewerServerTab.Perfmon.cs): the checkbox-list
       items, the reentrancy guard on programmatic checkbox writes, and the generation guard the batched
       chart update bumps on entry to discard a stale in-flight run. */
    private List<SelectableItem> _memoryClerkItems = new();
    private bool _isUpdatingMemoryClerkSelection;
    private int _memoryClerksPickerGen;

    /// <summary>
    /// Applies the shared chrome to the five Memory charts and wires their hover tooltips (Lite's units).
    /// Called from the constructor after <c>InitializeComponent</c>; the theme is applied up front so the
    /// charts don't flash white before the Memory tab's first load, matching Lite's ServerTab.
    /// </summary>
    private void InitializeMemoryCharts()
    {
        ApplyTheme(MemoryChart);
        MemoryChart.Refresh();
        ApplyTheme(MemoryClerksChart);
        MemoryClerksChart.Refresh();
        ApplyTheme(MemoryGrantSizingChart);
        MemoryGrantSizingChart.Refresh();
        ApplyTheme(MemoryGrantActivityChart);
        MemoryGrantActivityChart.Refresh();
        ApplyTheme(MemoryPressureEventsChart);
        MemoryPressureEventsChart.Refresh();

        _memoryHover = new ChartHoverHelper(MemoryChart, "GB");
        _memoryClerksHover = new ChartHoverHelper(MemoryClerksChart, "MB");
        _memoryGrantSizingHover = new ChartHoverHelper(MemoryGrantSizingChart, "MB");
        _memoryGrantActivityHover = new ChartHoverHelper(MemoryGrantActivityChart, "");
        _memoryPressureEventsHover = new ChartHoverHelper(MemoryPressureEventsChart, "events");
    }

    /// <summary>
    /// Loads the Memory tab (Lite's <c>RefreshMemoryAsync</c> full-refresh branch): the six feeds read
    /// concurrently over the toolbar's settable window, then the summary + four charts render and the clerk
    /// picker is populated + drawn. The reads are genuinely async (no <c>Task.Run</c>), so a single
    /// <see cref="Task.WhenAll"/> pools a connection per read.
    /// </summary>
    private async Task LoadMemoryAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        var latestTask = _dataService.GetLatestMemoryStatsAsync(_server.ServerId);
        var trendTask = _dataService.GetMemoryTrendAsync(_server.ServerId, startUtc, endUtc);
        var grantTrendTask = _dataService.GetMemoryGrantTrendAsync(_server.ServerId, startUtc, endUtc);
        var clerkTypesTask = _dataService.GetDistinctMemoryClerkTypesAsync(_server.ServerId, startUtc, endUtc);
        var grantChartTask = _dataService.GetMemoryGrantChartDataAsync(_server.ServerId, startUtc, endUtc);
        var pressureTask = _dataService.GetMemoryPressureEventsAsync(_server.ServerId, startUtc, endUtc);

        await Task.WhenAll(latestTask, trendTask, grantTrendTask, clerkTypesTask, grantChartTask, pressureTask);

        RenderMemorySummary(latestTask.Result);
        RenderMemoryChart(trendTask.Result, grantTrendTask.Result, startUtc, endUtc);
        RenderMemoryGrantCharts(grantChartTask.Result, startUtc, endUtc);
        RenderMemoryPressureEventsChart(pressureTask.Result);
        PopulateMemoryClerkPicker(clerkTypesTask.Result);
        await UpdateMemoryClerksChartFromPickerAsync();

        /* Plan Cache sub-tab (under Memory, matching the Dashboard's Memory > Plan Cache): loaded in the
           same pass as the other Memory sub-tabs (the Memory tab's full-refresh branch). */
        await LoadPlanCacheAsync();
    }

    /// <summary>The Overview summary strip — Lite's <c>UpdateMemorySummary</c> verbatim.</summary>
    private void RenderMemorySummary(MemoryStatsRow? stats)
    {
        if (stats == null)
        {
            PhysicalMemoryText.Text = "--";
            AvailablePhysicalMemoryText.Text = "--";
            TotalServerMemoryText.Text = "--";
            TargetServerMemoryText.Text = "--";
            BufferPoolText.Text = "--";
            PlanCacheText.Text = "--";
            TotalPageFileText.Text = "--";
            AvailablePageFileText.Text = "--";
            MemoryStateText.Text = "--";
            SqlMemoryModelText.Text = "--";
            return;
        }

        PhysicalMemoryText.Text = FormatMb(stats.TotalPhysicalMemoryMb);
        AvailablePhysicalMemoryText.Text = FormatMb(stats.AvailablePhysicalMemoryMb);
        TotalServerMemoryText.Text = FormatMb(stats.TotalServerMemoryMb);
        TargetServerMemoryText.Text = FormatMb(stats.TargetServerMemoryMb);
        BufferPoolText.Text = FormatMb(stats.BufferPoolMb);
        PlanCacheText.Text = FormatMb(stats.PlanCacheMb);
        TotalPageFileText.Text = FormatMb(stats.TotalPageFileMb);
        AvailablePageFileText.Text = FormatMb(stats.AvailablePageFileMb);
        MemoryStateText.Text = stats.SystemMemoryState;
        SqlMemoryModelText.Text = stats.SqlMemoryModel;
    }

    private static string FormatMb(double mb)
    {
        return mb >= 1024 ? $"{mb / 1024:F1} GB" : $"{mb:F0} MB";
    }

    /// <summary>
    /// The Overview memory trend — Lite's <c>UpdateMemoryChart</c>: Total Server Memory, Target Memory
    /// (dashed), Buffer Pool, and the Memory Grants overlay, all MB→GB. The grant overlay draws a flat
    /// zero line when no grant data exists. Times run through <see cref="ViewerTimeHelper.ForDisplay"/>.
    /// </summary>
    private void RenderMemoryChart(List<MemoryTrendPoint> data, List<MemoryGrantTrendPoint> grantData, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(MemoryChart);
        _memoryHover?.Clear();
        ApplyTheme(MemoryChart);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        if (data.Count == 0)
        {
            MemoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
            ReapplyAxisColors(MemoryChart);
            MemoryChart.Refresh();
            return;
        }

        var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
        var totalMem = data.Select(d => d.TotalServerMemoryMb / 1024.0).ToArray();
        var targetMem = data.Select(d => d.TargetServerMemoryMb / 1024.0).ToArray();
        var bufferPool = data.Select(d => d.BufferPoolMb / 1024.0).ToArray();

        var totalPlot = MemoryChart.Plot.Add.TimeSeries(times, totalMem);
        totalPlot.LegendText = "Total Server Memory";
        totalPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("TotalServerMemory"));
        ChartStyle.StyleScatter(totalPlot);
        _memoryHover?.Add(totalPlot, "Total Server Memory");

        var targetPlot = MemoryChart.Plot.Add.TimeSeries(times, targetMem);
        targetPlot.LegendText = "Target Memory";
        targetPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("TargetMemory"));
        ChartStyle.StyleScatter(targetPlot);
        targetPlot.LineStyle.Pattern = ScottPlot.LinePattern.Dashed;
        _memoryHover?.Add(targetPlot, "Target Memory");

        var bpPlot = MemoryChart.Plot.Add.TimeSeries(times, bufferPool);
        bpPlot.LegendText = "Buffer Pool";
        bpPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("BufferPool"));
        ChartStyle.StyleScatter(bpPlot);
        _memoryHover?.Add(bpPlot, "Buffer Pool");

        /* Memory grants trend line — show zero line when no grant data */
        double[] grantTimes, grantMb;
        if (grantData.Count > 0)
        {
            grantTimes = grantData.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
            grantMb = grantData.Select(d => d.TotalGrantedMb / 1024.0).ToArray();
        }
        else
        {
            grantTimes = new[] { times.First(), times.Last() };
            grantMb = new[] { 0.0, 0.0 };
        }

        var grantPlot = MemoryChart.Plot.Add.TimeSeries(grantTimes, grantMb);
        grantPlot.LegendText = "Memory Grants";
        grantPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("MemoryGrants"));
        ChartStyle.StyleScatter(grantPlot);
        _memoryHover?.Add(grantPlot, "Memory Grants");

        MemoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
        MemoryChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(MemoryChart);
        MemoryChart.Plot.YLabel("Memory (GB)");

        var maxVal = totalMem.Max();
        SetChartYLimitsWithLegendPadding(MemoryChart, 0, maxVal);

        ShowChartLegend(MemoryChart);
        MemoryChart.Refresh();
    }

    /// <summary>
    /// The Memory Grants sub-tab's two charts — Lite's <c>UpdateMemoryGrantCharts</c>: sizing
    /// (available/granted/used MB) and activity (grantees/waiters/timeouts/forced grants) per resource
    /// pool, each pool×metric on a cycling <c>SeriesColors</c> color. Times run through
    /// <see cref="ViewerTimeHelper.ForDisplay"/>.
    /// </summary>
    private void RenderMemoryGrantCharts(List<MemoryGrantChartPoint> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(MemoryGrantSizingChart);
        ClearChart(MemoryGrantActivityChart);
        _memoryGrantSizingHover?.Clear();
        _memoryGrantActivityHover?.Clear();
        ApplyTheme(MemoryGrantSizingChart);
        ApplyTheme(MemoryGrantActivityChart);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        if (data.Count == 0)
        {
            foreach (var c in new[] { MemoryGrantSizingChart, MemoryGrantActivityChart })
            {
                c.Plot.Axes.DateTimeTicksBottomDateChange();
                c.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
                ReapplyAxisColors(c);
                c.Refresh();
            }
            return;
        }

        var poolIds = data.Select(d => d.PoolId).Distinct().OrderBy(p => p).ToList();
        int colorIndex = 0;

        /* Chart 1: Memory Grant Sizing — Available, Granted, Used MB per pool, plus the workspace-memory
           ceiling (Target / Max Target MB) as dashed lines so the granted-vs-ceiling headroom is visible
           (the resource-semaphore signal the Dashboard's get_resource_semaphore exposes). IsCeiling marks
           the two dashed ceiling series. */
        double sizingMax = 0;
        var sizingMetrics = new (string Name, Func<MemoryGrantChartPoint, double> Selector, bool IsCeiling)[]
        {
            ("Available MB", d => d.AvailableMemoryMb, false),
            ("Granted MB", d => d.GrantedMemoryMb, false),
            ("Used MB", d => d.UsedMemoryMb, false),
            ("Target MB", d => d.TargetMemoryMb, true),
            ("Max Target MB", d => d.MaxTargetMemoryMb, true)
        };

        foreach (var poolId in poolIds)
        {
            var poolData = data.Where(d => d.PoolId == poolId).OrderBy(d => d.CollectionTime).ToList();
            var times = poolData.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();

            foreach (var metric in sizingMetrics)
            {
                var values = poolData.Select(d => metric.Selector(d)).ToArray();
                var plot = MemoryGrantSizingChart.Plot.Add.TimeSeries(times, values);
                var label = $"Pool {poolId}: {metric.Name}";
                plot.LegendText = label;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[colorIndex % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
                /* Ceiling lines dashed so they read as a limit, not another measured series (mirrors the
                   Memory Overview chart's dashed Target Memory line). */
                if (metric.IsCeiling) plot.LineStyle.Pattern = ScottPlot.LinePattern.Dashed;
                _memoryGrantSizingHover?.Add(plot, label);
                if (values.Length > 0) sizingMax = Math.Max(sizingMax, values.Max());
                colorIndex++;
            }
        }

        MemoryGrantSizingChart.Plot.Axes.DateTimeTicksBottomDateChange();
        MemoryGrantSizingChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(MemoryGrantSizingChart);
        MemoryGrantSizingChart.Plot.YLabel("Memory (MB)");
        SetChartYLimitsWithLegendPadding(MemoryGrantSizingChart, 0, sizingMax > 0 ? sizingMax : 100);
        ShowChartLegend(MemoryGrantSizingChart);
        MemoryGrantSizingChart.Refresh();

        /* Chart 2: Memory Grant Activity — Grantees, Waiters, Timeouts, Forced per pool */
        double activityMax = 0;
        colorIndex = 0;
        var activityMetrics = new (string Name, Func<MemoryGrantChartPoint, double> Selector)[]
        {
            ("Grantees", d => d.GranteeCount),
            ("Waiters", d => d.WaiterCount),
            ("Timeouts", d => d.TimeoutErrorCountDelta),
            ("Forced Grants", d => d.ForcedGrantCountDelta)
        };

        foreach (var poolId in poolIds)
        {
            var poolData = data.Where(d => d.PoolId == poolId).OrderBy(d => d.CollectionTime).ToList();
            var times = poolData.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();

            foreach (var metric in activityMetrics)
            {
                var values = poolData.Select(d => metric.Selector(d)).ToArray();
                var plot = MemoryGrantActivityChart.Plot.Add.TimeSeries(times, values);
                var label = $"Pool {poolId}: {metric.Name}";
                plot.LegendText = label;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[colorIndex % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
                _memoryGrantActivityHover?.Add(plot, label);
                if (values.Length > 0) activityMax = Math.Max(activityMax, values.Max());
                colorIndex++;
            }
        }

        MemoryGrantActivityChart.Plot.Axes.DateTimeTicksBottomDateChange();
        MemoryGrantActivityChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(MemoryGrantActivityChart);
        MemoryGrantActivityChart.Plot.YLabel("Count");
        SetChartYLimitsWithLegendPadding(MemoryGrantActivityChart, 0, activityMax > 0 ? activityMax : 10);
        ShowChartLegend(MemoryGrantActivityChart);
        MemoryGrantActivityChart.Refresh();
    }

    /// <summary>
    /// The Memory Pressure Events sub-tab — Lite's <c>UpdateMemoryPressureEventsChart</c>: hour-bucketed
    /// stacked bars of pressure events, SQL Server (process) vs OS (system) side-by-side and stacked by
    /// severity (medium = indicator 2, severe = indicator ≥ 3). Bar geometry copied verbatim from Lite;
    /// the X range is the toolbar's settable window and every bar/bound runs through
    /// <see cref="ViewerTimeHelper.ForDisplay"/> (see the class remarks on pressure sample_time).
    /// </summary>
    private void RenderMemoryPressureEventsChart(List<MemoryPressureEventRow> data)
    {
        ClearChart(MemoryPressureEventsChart);
        _memoryPressureEventsHover?.Clear();
        ApplyTheme(MemoryPressureEventsChart);

        var (startUtc, endUtc) = GetWindowUtc();
        double xMin = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        double xMax = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        /* Only count rows where SQL Server reported actual pressure (indicator >= 2 matches sp_pressuredetector). */
        var pressureRows = data
            .Where(d => d.MemoryIndicatorsProcess >= 2 || d.MemoryIndicatorsSystem >= 2)
            .OrderBy(d => d.SampleTime)
            .ToList();

        bool hasData = false;
        int maxBarCount = 0;

        if (pressureRows.Count > 0)
        {
            var grouped = pressureRows
                .GroupBy(d => new DateTime(d.SampleTime.Year, d.SampleTime.Month, d.SampleTime.Day, d.SampleTime.Hour, 0, 0))
                .OrderBy(g => g.Key)
                .ToList();

            double hourWidth = 1.0 / 24.0;
            double barSize = hourWidth * 0.4;
            double barOffset = hourWidth * 0.22;

            var sqlMediumColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SqlPressureMedium"));
            var sqlSevereColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SqlPressureSevere"));
            var osMediumColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("OsPressureMedium"));
            var osSevereColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("OsPressureSevere"));

            var sqlMediumBars = new List<ScottPlot.Bar>();
            var sqlSevereBars = new List<ScottPlot.Bar>();
            var osMediumBars = new List<ScottPlot.Bar>();
            var osSevereBars = new List<ScottPlot.Bar>();

            foreach (var g in grouped)
            {
                int sqlMedium = g.Count(d => d.MemoryIndicatorsProcess == 2);
                int sqlSevere = g.Count(d => d.MemoryIndicatorsProcess >= 3);
                int osMedium = g.Count(d => d.MemoryIndicatorsSystem == 2);
                int osSevere = g.Count(d => d.MemoryIndicatorsSystem >= 3);
                double x = ViewerTimeHelper.ForDisplay(g.Key).ToOADate();

                if (sqlMedium > 0)
                    sqlMediumBars.Add(new ScottPlot.Bar { Position = x - barOffset, ValueBase = 0, Value = sqlMedium, Size = barSize, FillColor = sqlMediumColor, LineWidth = 0 });
                if (sqlSevere > 0)
                    sqlSevereBars.Add(new ScottPlot.Bar { Position = x - barOffset, ValueBase = sqlMedium, Value = sqlMedium + sqlSevere, Size = barSize, FillColor = sqlSevereColor, LineWidth = 0 });
                if (osMedium > 0)
                    osMediumBars.Add(new ScottPlot.Bar { Position = x + barOffset, ValueBase = 0, Value = osMedium, Size = barSize, FillColor = osMediumColor, LineWidth = 0 });
                if (osSevere > 0)
                    osSevereBars.Add(new ScottPlot.Bar { Position = x + barOffset, ValueBase = osMedium, Value = osMedium + osSevere, Size = barSize, FillColor = osSevereColor, LineWidth = 0 });

                int sqlTotal = sqlMedium + sqlSevere;
                int osTotal = osMedium + osSevere;
                if (sqlTotal > maxBarCount) maxBarCount = sqlTotal;
                if (osTotal > maxBarCount) maxBarCount = osTotal;
            }

            if (sqlMediumBars.Count > 0 || sqlSevereBars.Count > 0 || osMediumBars.Count > 0 || osSevereBars.Count > 0)
            {
                hasData = true;

                if (sqlMediumBars.Count > 0)
                {
                    var bp = MemoryPressureEventsChart.Plot.Add.Bars(sqlMediumBars);
                    bp.LegendText = "SQL Server (medium)";
                    _memoryPressureEventsHover?.Add(bp, "SQL Server (medium)");
                }
                if (sqlSevereBars.Count > 0)
                {
                    var bp = MemoryPressureEventsChart.Plot.Add.Bars(sqlSevereBars);
                    bp.LegendText = "SQL Server (severe)";
                    _memoryPressureEventsHover?.Add(bp, "SQL Server (severe)");
                }
                if (osMediumBars.Count > 0)
                {
                    var bp = MemoryPressureEventsChart.Plot.Add.Bars(osMediumBars);
                    bp.LegendText = "Operating System (medium)";
                    _memoryPressureEventsHover?.Add(bp, "Operating System (medium)");
                }
                if (osSevereBars.Count > 0)
                {
                    var bp = MemoryPressureEventsChart.Plot.Add.Bars(osSevereBars);
                    bp.LegendText = "Operating System (severe)";
                    _memoryPressureEventsHover?.Add(bp, "Operating System (severe)");
                }
            }
        }

        MemoryPressureEventsChart.Plot.Axes.DateTimeTicksBottomDateChange();
        MemoryPressureEventsChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(MemoryPressureEventsChart);
        MemoryPressureEventsChart.Plot.YLabel("Pressure Events per Hour");
        SetChartYLimitsWithLegendPadding(MemoryPressureEventsChart, 0, Math.Max(maxBarCount, 5));

        if (hasData)
        {
            ShowChartLegend(MemoryPressureEventsChart);
        }

        MemoryPressureEventsChart.Refresh();
    }

    /* ========== Memory Clerks Picker (mirrors ViewerServerTab.Perfmon.cs) ========== */

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

    /// <summary>
    /// Redraws the Memory Clerks chart from the picker selection (Lite's
    /// <c>UpdateMemoryClerksChartFromPickerAsync</c>): the batched trend for up to 20 selected clerks in
    /// one query, each on a cycling <c>SeriesColors</c> color, plus the non-buffer-pool total + top-clerk
    /// summary. The generation guard discards a stale run when the selection changes mid-query; the
    /// toolbar's settable window matches the tab load (Lite's custom-range branch is dropped).
    /// </summary>
    private async Task UpdateMemoryClerksChartFromPickerAsync()
    {
        var gen = ++_memoryClerksPickerGen;
        try
        {
            var selected = _memoryClerkItems.Where(i => i.IsSelected).Take(20).ToList();

            ClearChart(MemoryClerksChart);
            ApplyTheme(MemoryClerksChart);
            _memoryClerksHover?.Clear();

            var (startUtc, endUtc) = GetWindowUtc();
            double xMin = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
            double xMax = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

            if (selected.Count == 0)
            {
                MemoryClerksTotalText.Text = "--";
                MemoryClerksTopText.Text = "--";
                MemoryClerksChart.Plot.Axes.DateTimeTicksBottomDateChange();
                MemoryClerksChart.Plot.Axes.SetLimitsX(xMin, xMax);
                ReapplyAxisColors(MemoryClerksChart);
                MemoryClerksChart.Refresh();
                return;
            }

            double globalMax = 0;
            double nonBpTotal = 0;
            string topNonBpClerk = "";
            double topNonBpMb = 0;

            // Batched fetch: one query for all selected clerk types (was an N+1 query-per-clerk loop).
            var trendsByType = await _dataService.GetMemoryClerkTrendsByTypesAsync(
                _server.ServerId, selected.Select(s => s.DisplayName).ToList(), startUtc, endUtc);
            if (gen != _memoryClerksPickerGen) return;

            for (int i = 0; i < selected.Count; i++)
            {
                if (!trendsByType.TryGetValue(selected[i].DisplayName, out var trend) || trend.Count == 0) continue;

                var times = trend.Select(t => ViewerTimeHelper.ForDisplay(t.CollectionTime).ToOADate()).ToArray();
                var values = trend.Select(t => t.MemoryMb).ToArray();

                var plot = MemoryClerksChart.Plot.Add.TimeSeries(times, values);
                plot.LegendText = selected[i].DisplayName;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
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
            MemoryClerksChart.Plot.Axes.SetLimitsX(xMin, xMax);
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

    /// <summary>
    /// Tears down the five Memory hover helpers (mirrors Lite's dispose) so the tooltip popups and their
    /// chart event handlers don't outlive a closed server tab. Called from the tab's single Dispose path
    /// in ViewerServerTab.Charts.cs.
    /// </summary>
    public void DisposeMemoryHelpers()
    {
        _memoryHover?.Dispose();
        _memoryClerksHover?.Dispose();
        _memoryGrantSizingHover?.Dispose();
        _memoryGrantActivityHover?.Dispose();
        _memoryPressureEventsHover?.Dispose();
    }
}

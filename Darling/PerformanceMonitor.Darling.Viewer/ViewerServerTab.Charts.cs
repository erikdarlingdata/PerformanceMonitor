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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The CPU and tempdb inner-tab charts, copied from Lite's <c>ServerTab.Charts.cs</c>
/// (<c>UpdateCpuChart</c> and the three <c>UpdateTempDb*Chart</c> bodies) with the data layer rewired
/// to <see cref="ViewerDataService"/> Postgres reads. The only render-body change is the time axis:
/// where Lite shifts by its per-server <c>UtcOffsetMinutes</c> (CPU plots the raw server-local
/// sample_time directly), the viewer has no server-offset concept, so every point runs through
/// <see cref="ViewerTimeHelper.ForDisplay"/> — the naive-UTC-to-viewer-local convention the shell's
/// Overview charts already use. The hover tooltips are kept, and Lite's per-chart "Show Active Queries at
/// This Time" drill-down IS now ported (the right-click menu is wired in <c>ViewerServerTab.DrillDown.cs</c>;
/// Lite's ContextMenuHelper save/export chrome remains Lite-only). Chart chrome/legend/line polish flow
/// through the shared <see cref="ChartStyle"/>
/// / <see cref="ChartPalette"/> and the <c>ViewerServerTab.ChartHelpers.cs</c> bridge, exactly as in
/// Lite.
/// </summary>
public partial class ViewerServerTab : IDisposable
{
    private ChartHoverHelper? _cpuHover;
    private ChartHoverHelper? _tempDbHover;
    private ChartHoverHelper? _tempDbSizeHover;
    private ChartHoverHelper? _tempDbFileIoHover;

    /* Cycling palette sourced from the shared ChartPalette so the per-file tempdb I/O series cycle the
       SAME colors in the same order as Lite and Dashboard (mirrors Lite's ServerTab.SeriesColors). */
    private static readonly string[] SeriesColors = ChartPalette.CyclingPalette.ToArray();

    /// <summary>
    /// Applies the shared chrome to the CPU/tempdb charts and wires their hover tooltips. Called from
    /// the constructor after <c>InitializeComponent</c>; the theme is applied up front so the charts
    /// don't flash white before their tab's first load, matching Lite's ServerTab.
    /// </summary>
    private void InitializeCpuTempDbCharts()
    {
        ApplyTheme(CpuChart);
        CpuChart.Refresh();
        ApplyTheme(TempDbChart);
        TempDbChart.Refresh();
        ApplyTheme(TempDbSizeChart);
        TempDbSizeChart.Refresh();
        ApplyTheme(TempDbFileIoChart);
        TempDbFileIoChart.Refresh();

        /* Hover tooltips with Lite's per-chart units ("%", "MB", "MB", "ms"). */
        _cpuHover = new ChartHoverHelper(CpuChart, "%");
        _tempDbHover = new ChartHoverHelper(TempDbChart, "MB");
        _tempDbSizeHover = new ChartHoverHelper(TempDbSizeChart, "MB");
        _tempDbFileIoHover = new ChartHoverHelper(TempDbFileIoChart, "ms");
    }

    /// <summary>Tears down the hover helpers (unhooks their chart event handlers). Called from
    /// MainWindow when the server tab closes.</summary>
    public void Dispose()
    {
        /* Stop the toolbar's auto-refresh timer so a closed tab's timer can't fire against a torn-down UI. */
        if (_autoRefreshTimer != null)
        {
            _autoRefreshTimer.Stop();
            _autoRefreshTimer.Tick -= OnAutoRefreshTick;
        }

        _cpuHover?.Dispose();
        _tempDbHover?.Dispose();
        _tempDbSizeHover?.Dispose();
        _tempDbFileIoHover?.Dispose();
        /* Each partial owns its own hover fields; the single Dispose() forwards to their teardowns
           so the whole tab tears down through one path. */
        DisposeChartHelpers();
        DisposeMemoryHelpers();
        DisposePlanCacheHelpers();
        DisposeCpuSchedulerHelpers();
        DisposeLatchSpinlockHelpers();
        DisposeSessionStatsHelpers();
        DisposeFileIoHelpers();
        DisposeBlockingHelpers();
        DisposePerfmonHelpers();
        DisposeCollectionHealthHelpers();
        DisposeQueriesTabHelpers();
        DisposePlanHelpers();
        DisposeSystemHealthChartHelpers();
    }

    /// <summary>Loads the CPU tab: raw per-sample CPU utilization over the window.</summary>
    private async Task LoadCpuAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var samples = await _dataService.GetCpuUtilizationAsync(_server.ServerId, startUtc);
        /* The read is start-only server-side; bound the end for a custom range so a window that ends in
           the past doesn't trail to the newest sample. sample_time is de-skewed to naive UTC (matching endUtc). */
        if (IsCustomRange)
        {
            samples = samples.Where(s => s.SampleTime <= endUtc).ToList();
        }
        RenderCpuChart(samples, startUtc, endUtc);

        /* The CPU tab is a sub-TabControl (CPU Utilization + CPU Scheduler); load the scheduler sub-tab in
           the same pass — the CPU tab's full-refresh branch, mirroring the Memory tab's all-sub-tabs load. */
        await LoadCpuSchedulerAsync();
    }

    /// <summary>Loads the tempdb tab: usage/size trend and per-file I/O latency, read concurrently.</summary>
    private async Task LoadTempDbAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        /* Both reads run concurrently — NpgsqlDataSource pools a connection for each. */
        using var readFanOut = ViewerReadFanOut.Of(2);
        var trendTask = _dataService.GetTempDbTrendAsync(_server.ServerId, startUtc);
        var fileIoTask = _dataService.GetTempDbFileIoTrendAsync(_server.ServerId, startUtc);
        var trend = await trendTask;
        var fileIo = await fileIoTask;

        /* Start-only reads; bound the end for a custom range (collection_time is naive UTC). */
        if (IsCustomRange)
        {
            trend = trend.Where(t => t.CollectionTime <= endUtc).ToList();
            fileIo = fileIo.Where(f => f.CollectionTime <= endUtc).ToList();
        }

        RenderTempDbUsageChart(trend, startUtc, endUtc);
        RenderTempDbSizeChart(trend, startUtc, endUtc);
        RenderTempDbFileIoChart(fileIo, startUtc, endUtc);
    }

    private void RenderCpuChart(List<CpuUtilizationSample> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(CpuChart);
        _cpuHover?.Clear();
        ApplyTheme(CpuChart);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        if (data.Count == 0)
        {
            CpuChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CpuChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
            ReapplyAxisColors(CpuChart);
            CpuChart.Refresh();
            return;
        }

        /* sample_time is the monitored server's LOCAL wall clock in the store, so GetCpuUtilizationAsync
           de-skews it to naive UTC in SQL (#1262); it then runs through ViewerTimeHelper.ForDisplay like
           every other Darling chart. (The SQL de-skew — recovering the offset from the collection batch —
           is a data correction independent of the Server/Local/UTC display mode ForDisplay then applies;
           Lite instead shifts the raw server-local sample_time by its per-server ServerTimeHelper offset.) */
        var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.SampleTime).ToOADate()).ToArray();
        var sqlCpu = data.Select(d => (double)d.SqlServerCpu).ToArray();
        var otherCpu = data.Select(d => (double)d.OtherProcessCpu).ToArray();

        var sqlPlot = CpuChart.Plot.Add.TimeSeries(times, sqlCpu);
        sqlPlot.LegendText = "SQL Server";
        sqlPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SqlCpu"));
        ChartStyle.StyleScatter(sqlPlot);
        _cpuHover?.Add(sqlPlot, "SQL Server");

        var otherPlot = CpuChart.Plot.Add.TimeSeries(times, otherCpu);
        otherPlot.LegendText = "Other";
        otherPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("OtherCpu"));
        ChartStyle.StyleScatter(otherPlot);
        _cpuHover?.Add(otherPlot, "Other");

        CpuChart.Plot.Axes.DateTimeTicksBottomDateChange();
        CpuChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(CpuChart);
        CpuChart.Plot.YLabel("CPU %");
        CpuChart.Plot.Axes.SetLimitsY(0, 105);

        ShowChartLegend(CpuChart);
        CpuChart.Refresh();
    }

    private void RenderTempDbUsageChart(List<TempDbSample> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(TempDbChart);
        _tempDbHover?.Clear();
        ApplyTheme(TempDbChart);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        if (data.Count == 0)
        {
            TempDbChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempDbChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
            ReapplyAxisColors(TempDbChart);
            TempDbChart.Refresh();
            return;
        }

        var times = data.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
        var userObj = data.Select(d => d.UserObjectReservedMb).ToArray();
        var internalObj = data.Select(d => d.InternalObjectReservedMb).ToArray();
        var versionStore = data.Select(d => d.VersionStoreReservedMb).ToArray();

        var userPlot = TempDbChart.Plot.Add.TimeSeries(times, userObj);
        userPlot.LegendText = "User Objects";
        userPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("UserObjects"));
        ChartStyle.StyleScatter(userPlot);
        _tempDbHover?.Add(userPlot, "User Objects");

        var internalPlot = TempDbChart.Plot.Add.TimeSeries(times, internalObj);
        internalPlot.LegendText = "Internal Objects";
        internalPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("InternalObjects"));
        ChartStyle.StyleScatter(internalPlot);
        _tempDbHover?.Add(internalPlot, "Internal Objects");

        var vsPlot = TempDbChart.Plot.Add.TimeSeries(times, versionStore);
        vsPlot.LegendText = "Version Store";
        vsPlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("VersionStore"));
        ChartStyle.StyleScatter(vsPlot);
        _tempDbHover?.Add(vsPlot, "Version Store");

        TempDbChart.Plot.Axes.DateTimeTicksBottomDateChange();
        TempDbChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(TempDbChart);
        TempDbChart.Plot.YLabel("MB");

        var maxVal = new[] { userObj.Max(), internalObj.Max(), versionStore.Max() }.Max();
        SetChartYLimitsWithLegendPadding(TempDbChart, 0, maxVal);

        ShowChartLegend(TempDbChart);
        TempDbChart.Refresh();
    }

    // Dedicated chart for tempdb TOTAL allocated size (used + unallocated free space) over time — the
    // growth trend, on its own scale (AutoScaleY) so it doesn't flatten the usage series above. Mirror
    // of Lite's UpdateTempDbSizeChart (single series, no legend).
    private void RenderTempDbSizeChart(List<TempDbSample> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(TempDbSizeChart);
        ApplyTheme(TempDbSizeChart);
        _tempDbSizeHover?.Clear();

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        if (data.Count == 0)
        {
            TempDbSizeChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempDbSizeChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
            ReapplyAxisColors(TempDbSizeChart);
            TempDbSizeChart.Refresh();
            return;
        }

        var sorted = data.OrderBy(d => d.CollectionTime).ToList();
        var times = sorted.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
        var totals = sorted.Select(d => d.TotalReservedMb + d.UnallocatedMb).ToArray();

        var sizePlot = TempDbSizeChart.Plot.Add.TimeSeries(times, totals);
        sizePlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("UnallocatedTempdb"));
        ChartStyle.StyleScatter(sizePlot);
        _tempDbSizeHover?.Add(sizePlot, "Allocated MB");

        TempDbSizeChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(TempDbSizeChart);
        TempDbSizeChart.Plot.YLabel("Allocated MB");
        TempDbSizeChart.Plot.Axes.AutoScaleY();
        TempDbSizeChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        TempDbSizeChart.Refresh();
    }

    private void RenderTempDbFileIoChart(List<TempDbFileIoSample> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(TempDbFileIoChart);
        _tempDbFileIoHover?.Clear();
        ApplyTheme(TempDbFileIoChart);

        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        if (data.Count == 0)
        {
            TempDbFileIoChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempDbFileIoChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
            ReapplyAxisColors(TempDbFileIoChart);
            TempDbFileIoChart.Refresh();
            return;
        }

        /* One series per tempdb data file (Lite groups on the file-name column). */
        var files = data
            .GroupBy(d => d.FileName)
            .OrderByDescending(g => g.Sum(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs))
            .Take(12)
            .ToList();

        double maxLatency = 0;
        int colorIdx = 0;

        foreach (var fileGroup in files)
        {
            var points = fileGroup.OrderBy(d => d.CollectionTime).ToList();
            var times = points.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
            var latency = points.Select(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (latency.Length > 0)
            {
                var plot = TempDbFileIoChart.Plot.Add.TimeSeries(times, latency);
                plot.LegendText = fileGroup.Key;
                plot.Color = color;
                ChartStyle.StyleScatter(plot);
                _tempDbFileIoHover?.Add(plot, fileGroup.Key);
                maxLatency = Math.Max(maxLatency, latency.Max());
            }
        }

        TempDbFileIoChart.Plot.Axes.DateTimeTicksBottomDateChange();
        TempDbFileIoChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(TempDbFileIoChart);
        TempDbFileIoChart.Plot.YLabel("tempdb File I/O Latency (ms)");
        SetChartYLimitsWithLegendPadding(TempDbFileIoChart, 0, maxLatency > 0 ? maxLatency : 10);
        ShowChartLegend(TempDbFileIoChart);
        TempDbFileIoChart.Refresh();
    }
}

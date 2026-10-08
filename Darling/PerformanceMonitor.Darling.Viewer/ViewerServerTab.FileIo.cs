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
using System.Windows.Controls;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The File I/O inner tab — a COPY of Lite's File I/O tab (ServerTab.xaml:1373-1409 + the
/// <c>UpdateFileIoCharts</c> / <c>UpdateFileIoThroughputCharts</c> bodies in ServerTab.Charts.cs),
/// reads rewired to <see cref="ViewerDataService"/> Postgres. Two nested sub-tabs — "File I/O Latency"
/// (stacked read + write latency charts, with the queued-I/O dashed overlay) and "File I/O Throughput"
/// (stacked read + write MB/s charts) — each plotting the top 10 files as its own cycling-colored
/// series. The only render-body change is the time axis: Lite shifts by its per-server
/// <c>UtcOffsetMinutes</c>, the viewer plots every point at its naive-UTC instant and draws the labels in
/// <see cref="ViewerTimeHelper.CurrentDisplayZone"/> (the convention every Darling chart uses). Series colors ride the shared
/// cycling <see cref="ChartPalette"/> and <see cref="ChartStyle.StyleScatter"/> line polish; hover
/// tooltips carry the per-chart unit. Lite's per-chart context menu / save-image are NOT ported.
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _fileIoReadHover;
    private ChartHoverHelper? _fileIoWriteHover;
    private ChartHoverHelper? _fileIoReadThroughputHover;
    private ChartHoverHelper? _fileIoWriteThroughputHover;

    /// <summary>
    /// Applies the shared chrome to the four File I/O charts and wires their hover tooltips (Lite's
    /// per-chart units: "ms" for latency, "MB/s" for throughput). Called from the constructor after
    /// <c>InitializeComponent</c> so the charts don't flash white before the tab's first load.
    /// </summary>
    private void InitializeFileIoCharts()
    {
        ApplyTheme(FileIoReadChart);
        FileIoReadChart.Refresh();
        ApplyTheme(FileIoWriteChart);
        FileIoWriteChart.Refresh();
        ApplyTheme(FileIoReadThroughputChart);
        FileIoReadThroughputChart.Refresh();
        ApplyTheme(FileIoWriteThroughputChart);
        FileIoWriteThroughputChart.Refresh();

        _fileIoReadHover = new ChartHoverHelper(FileIoReadChart, "ms", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _fileIoWriteHover = new ChartHoverHelper(FileIoWriteChart, "ms", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _fileIoReadThroughputHover = new ChartHoverHelper(FileIoReadThroughputChart, "MB/s", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _fileIoWriteThroughputHover = new ChartHoverHelper(FileIoWriteThroughputChart, "MB/s", displayZone: ViewerTimeHelper.CurrentDisplayZone);
    }

    /// <summary>
    /// A File I/O sub-tab switch reloads through the shell's overlap-guarded
    /// <see cref="RefreshActiveInnerTabAsync"/> (the File I/O tab is the active inner tab whenever its
    /// sub-tabs are visible, so the loader dispatches to <see cref="LoadFileIoAsync"/> and loads only the
    /// newly-visible sub-tab). Gated on <see cref="System.Windows.FrameworkElement.IsLoaded"/> and the
    /// sub-TabControl's own selection so the build-time selection and bubbled inner selections are ignored.
    /// </summary>
    private async void FileIoSubTabs_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!ReferenceEquals(e.OriginalSource, FileIoSubTabs) || !IsLoaded)
        {
            return;
        }

        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Loads the File I/O tab's ACTIVE sub-tab only (mirrors Lite's subTabOnly gating): the Latency
    /// sub-tab reads the per-file latency trend, the Throughput sub-tab reads the per-file MB/s trend.
    /// Both window on the toolbar's settable range like every other viewer surface.
    /// </summary>
    private async Task LoadFileIoAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        switch (FileIoSubTabs.SelectedIndex)
        {
            case 1: // File I/O Throughput
                var throughput = await _dataService.GetFileIoThroughputTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
                RenderFileIoThroughputCharts(throughput, startUtc, endUtc);
                break;
            case 0: // File I/O Latency
            default:
                var latency = await _dataService.GetFileIoLatencyTrendAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
                RenderFileIoLatencyCharts(latency, startUtc, endUtc);
                break;
        }
    }

    private void RenderFileIoLatencyCharts(List<FileIoLatencyPoint> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(FileIoReadChart);
        ClearChart(FileIoWriteChart);
        _fileIoReadHover?.Clear();
        _fileIoWriteHover?.Clear();
        ApplyTheme(FileIoReadChart);
        ApplyTheme(FileIoWriteChart);

        var rangeStart = startUtc.ToOADate();
        var rangeEnd = endUtc.ToOADate();

        if (data.Count == 0)
        {
            foreach (var c in new[] { FileIoReadChart, FileIoWriteChart })
            {
                c.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
                c.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
                ReapplyAxisColors(c);
                c.Refresh();
            }
            return;
        }

        /* Group by file, limit to top 10 by total stall */
        var databases = data
            .GroupBy(d => $"{d.DatabaseName}.{d.FileName}")
            .OrderByDescending(g => g.Sum(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs))
            .Take(10)
            .ToList();

        double readMax = 0, writeMax = 0;
        int colorIdx = 0;

        bool hasQueuedData = data.Any(d => d.AvgQueuedReadLatencyMs > 0 || d.AvgQueuedWriteLatencyMs > 0);

        foreach (var dbGroup in databases)
        {
            var points = dbGroup.OrderBy(d => d.CollectionTime).ToList();
            var times = points.Select(d => d.CollectionTime.ToOADate()).ToArray();
            var readLatency = points.Select(d => d.AvgReadLatencyMs).ToArray();
            var writeLatency = points.Select(d => d.AvgWriteLatencyMs).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (readLatency.Length > 0)
            {
                var readPlot = FileIoReadChart.Plot.Add.TimeSeries(times, readLatency);
                readPlot.LegendText = dbGroup.Key;
                readPlot.Color = color;
                ChartStyle.StyleScatter(readPlot);
                _fileIoReadHover?.Add(readPlot, dbGroup.Key);
                readMax = Math.Max(readMax, readLatency.Max());
            }

            if (writeLatency.Length > 0)
            {
                var writePlot = FileIoWriteChart.Plot.Add.TimeSeries(times, writeLatency);
                writePlot.LegendText = dbGroup.Key;
                writePlot.Color = color;
                ChartStyle.StyleScatter(writePlot);
                _fileIoWriteHover?.Add(writePlot, dbGroup.Key);
                writeMax = Math.Max(writeMax, writeLatency.Max());
            }

            /* Queued I/O overlay — dashed lines showing queue wait portion of latency */
            if (hasQueuedData)
            {
                var queuedReadLatency = points.Select(d => d.AvgQueuedReadLatencyMs).ToArray();
                var queuedWriteLatency = points.Select(d => d.AvgQueuedWriteLatencyMs).ToArray();

                if (queuedReadLatency.Any(v => v > 0))
                {
                    var qReadPlot = FileIoReadChart.Plot.Add.TimeSeries(times, queuedReadLatency);
                    qReadPlot.LegendText = $"{dbGroup.Key} (queued)";
                    qReadPlot.Color = color;
                    ChartStyle.StyleScatter(qReadPlot);
                    qReadPlot.LinePattern = ScottPlot.LinePattern.Dashed;
                    _fileIoReadHover?.Add(qReadPlot, $"{dbGroup.Key} (queued)");
                }

                if (queuedWriteLatency.Any(v => v > 0))
                {
                    var qWritePlot = FileIoWriteChart.Plot.Add.TimeSeries(times, queuedWriteLatency);
                    qWritePlot.LegendText = $"{dbGroup.Key} (queued)";
                    qWritePlot.Color = color;
                    ChartStyle.StyleScatter(qWritePlot);
                    qWritePlot.LinePattern = ScottPlot.LinePattern.Dashed;
                    _fileIoWriteHover?.Add(qWritePlot, $"{dbGroup.Key} (queued)");
                }
            }
        }

        FileIoReadChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        FileIoReadChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(FileIoReadChart);
        FileIoReadChart.Plot.YLabel("Read Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoReadChart, 0, readMax > 0 ? readMax : 10);
        ShowChartLegend(FileIoReadChart);
        FileIoReadChart.Refresh();

        FileIoWriteChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        FileIoWriteChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(FileIoWriteChart);
        FileIoWriteChart.Plot.YLabel("Write Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoWriteChart, 0, writeMax > 0 ? writeMax : 10);
        ShowChartLegend(FileIoWriteChart);
        FileIoWriteChart.Refresh();
    }

    private void RenderFileIoThroughputCharts(List<FileIoThroughputPoint> data, DateTime startUtc, DateTime endUtc)
    {
        ClearChart(FileIoReadThroughputChart);
        ClearChart(FileIoWriteThroughputChart);
        _fileIoReadThroughputHover?.Clear();
        _fileIoWriteThroughputHover?.Clear();
        ApplyTheme(FileIoReadThroughputChart);
        ApplyTheme(FileIoWriteThroughputChart);

        var rangeStart = startUtc.ToOADate();
        var rangeEnd = endUtc.ToOADate();

        if (data.Count == 0)
        {
            foreach (var c in new[] { FileIoReadThroughputChart, FileIoWriteThroughputChart })
            {
                c.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
                c.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
                ReapplyAxisColors(c);
                c.Refresh();
            }
            return;
        }

        /* Group by file label, limit to top 10 by total throughput */
        var files = data
            .GroupBy(d => d.FileLabel)
            .OrderByDescending(g => g.Sum(d => d.ReadMbPerSec + d.WriteMbPerSec))
            .Take(10)
            .ToList();

        double readMax = 0, writeMax = 0;
        int colorIdx = 0;

        foreach (var fileGroup in files)
        {
            var points = fileGroup.OrderBy(d => d.CollectionTime).ToList();
            var times = points.Select(d => d.CollectionTime.ToOADate()).ToArray();
            var readThroughput = points.Select(d => d.ReadMbPerSec).ToArray();
            var writeThroughput = points.Select(d => d.WriteMbPerSec).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (readThroughput.Length > 0)
            {
                var readPlot = FileIoReadThroughputChart.Plot.Add.TimeSeries(times, readThroughput);
                readPlot.LegendText = fileGroup.Key;
                readPlot.Color = color;
                ChartStyle.StyleScatter(readPlot);
                _fileIoReadThroughputHover?.Add(readPlot, fileGroup.Key);
                readMax = Math.Max(readMax, readThroughput.Max());
            }

            if (writeThroughput.Length > 0)
            {
                var writePlot = FileIoWriteThroughputChart.Plot.Add.TimeSeries(times, writeThroughput);
                writePlot.LegendText = fileGroup.Key;
                writePlot.Color = color;
                ChartStyle.StyleScatter(writePlot);
                _fileIoWriteThroughputHover?.Add(writePlot, fileGroup.Key);
                writeMax = Math.Max(writeMax, writeThroughput.Max());
            }
        }

        FileIoReadThroughputChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        FileIoReadThroughputChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(FileIoReadThroughputChart);
        FileIoReadThroughputChart.Plot.YLabel("Read Throughput (MB/s)");
        SetChartYLimitsWithLegendPadding(FileIoReadThroughputChart, 0, readMax > 0 ? readMax : 1);
        ShowChartLegend(FileIoReadThroughputChart);
        FileIoReadThroughputChart.Refresh();

        FileIoWriteThroughputChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        FileIoWriteThroughputChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ReapplyAxisColors(FileIoWriteThroughputChart);
        FileIoWriteThroughputChart.Plot.YLabel("Write Throughput (MB/s)");
        SetChartYLimitsWithLegendPadding(FileIoWriteThroughputChart, 0, writeMax > 0 ? writeMax : 1);
        ShowChartLegend(FileIoWriteThroughputChart);
        FileIoWriteThroughputChart.Refresh();
    }

    /// <summary>Tears down the File I/O hover helpers; called from the single Dispose() in Charts.cs.</summary>
    private void DisposeFileIoHelpers()
    {
        _fileIoReadHover?.Dispose();
        _fileIoWriteHover?.Dispose();
        _fileIoReadThroughputHover?.Dispose();
        _fileIoWriteThroughputHover?.Dispose();
    }
}

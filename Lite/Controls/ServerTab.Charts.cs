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
    private static readonly string[] SeriesColors = new[]
    {
        "#4FC3F7", "#E57373", "#81C784", "#FFD54F", "#BA68C8",
        "#FFB74D", "#4DD0E1", "#F06292", "#AED581", "#7986CB",
        "#FFF176", "#A1887F", "#FF7043", "#80DEEA", "#FFE082",
        "#CE93D8", "#EF9A9A", "#C5E1A5", "#FFCC80", "#B0BEC5"
    };

    private void UpdateMemorySummary(MemoryStatsRow? stats)
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


    private void UpdateCpuChart(List<CpuUtilizationRow> data)
    {
        ClearChart(CpuChart);
        _cpuHover?.Clear();
        ApplyTheme(CpuChart);

        if (data.Count == 0) { CpuChart.Refresh(); return; }

        var times = data.Select(d => d.SampleTime.ToOADate()).ToArray();
        var sqlCpu = data.Select(d => (double)d.SqlServerCpu).ToArray();
        var otherCpu = data.Select(d => (double)d.OtherProcessCpu).ToArray();

        var sqlPlot = CpuChart.Plot.Add.Scatter(times, sqlCpu);
        sqlPlot.LegendText = "SQL Server";
        sqlPlot.Color = ScottPlot.Color.FromHex("#4FC3F7");
        _cpuHover?.Add(sqlPlot, "SQL Server");

        var otherPlot = CpuChart.Plot.Add.Scatter(times, otherCpu);
        otherPlot.LegendText = "Other";
        otherPlot.Color = ScottPlot.Color.FromHex("#E57373");
        _cpuHover?.Add(otherPlot, "Other");

        CpuChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(CpuChart);
        CpuChart.Plot.YLabel("CPU %");
        CpuChart.Plot.Axes.SetLimitsY(0, 105);

        ShowChartLegend(CpuChart);
        CpuChart.Refresh();
    }

    private void UpdateMemoryChart(List<MemoryTrendPoint> data, List<MemoryTrendPoint> grantData)
    {
        ClearChart(MemoryChart);
        _memoryHover?.Clear();
        ApplyTheme(MemoryChart);

        if (data.Count == 0) { MemoryChart.Refresh(); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var totalMem = data.Select(d => d.TotalServerMemoryMb / 1024.0).ToArray();
        var targetMem = data.Select(d => d.TargetServerMemoryMb / 1024.0).ToArray();
        var bufferPool = data.Select(d => d.BufferPoolMb / 1024.0).ToArray();

        var totalPlot = MemoryChart.Plot.Add.Scatter(times, totalMem);
        totalPlot.LegendText = "Total Server Memory";
        totalPlot.Color = ScottPlot.Color.FromHex("#4FC3F7");
        _memoryHover?.Add(totalPlot, "Total Server Memory");

        var targetPlot = MemoryChart.Plot.Add.Scatter(times, targetMem);
        targetPlot.LegendText = "Target Memory";
        targetPlot.Color = ScottPlot.Colors.Gray;
        targetPlot.LineStyle.Pattern = LinePattern.Dashed;
        _memoryHover?.Add(targetPlot, "Target Memory");

        var bpPlot = MemoryChart.Plot.Add.Scatter(times, bufferPool);
        bpPlot.LegendText = "Buffer Pool";
        bpPlot.Color = ScottPlot.Color.FromHex("#81C784");
        _memoryHover?.Add(bpPlot, "Buffer Pool");

        /* Memory grants trend line — show zero line when no grant data */
        double[] grantTimes, grantMb;
        if (grantData.Count > 0)
        {
            grantTimes = grantData.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            grantMb = grantData.Select(d => d.TotalGrantedMb / 1024.0).ToArray();
        }
        else
        {
            grantTimes = new[] { times.First(), times.Last() };
            grantMb = new[] { 0.0, 0.0 };
        }

        var grantPlot = MemoryChart.Plot.Add.Scatter(grantTimes, grantMb);
        grantPlot.LegendText = "Memory Grants";
        grantPlot.Color = ScottPlot.Color.FromHex("#FFB74D");
        _memoryHover?.Add(grantPlot, "Memory Grants");

        MemoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(MemoryChart);
        MemoryChart.Plot.YLabel("Memory (GB)");

        var maxVal = totalMem.Max();
        SetChartYLimitsWithLegendPadding(MemoryChart, 0, maxVal);

        ShowChartLegend(MemoryChart);
        MemoryChart.Refresh();
    }

    private void UpdateMemoryGrantCharts(List<MemoryGrantChartPoint> data)
    {
        ClearChart(MemoryGrantSizingChart);
        ClearChart(MemoryGrantActivityChart);
        _memoryGrantSizingHover?.Clear();
        _memoryGrantActivityHover?.Clear();
        ApplyTheme(MemoryGrantSizingChart);
        ApplyTheme(MemoryGrantActivityChart);

        if (data.Count == 0)
        {
            MemoryGrantSizingChart.Refresh();
            MemoryGrantActivityChart.Refresh();
            return;
        }

        var poolIds = data.Select(d => d.PoolId).Distinct().OrderBy(p => p).ToList();
        int colorIndex = 0;

        /* Chart 1: Memory Grant Sizing — Available, Granted, Used MB per pool */
        double sizingMax = 0;
        var sizingMetrics = new (string Name, Func<MemoryGrantChartPoint, double> Selector)[]
        {
            ("Available MB", d => d.AvailableMemoryMb),
            ("Granted MB", d => d.GrantedMemoryMb),
            ("Used MB", d => d.UsedMemoryMb)
        };

        foreach (var poolId in poolIds)
        {
            var poolData = data.Where(d => d.PoolId == poolId).OrderBy(d => d.CollectionTime).ToList();
            var times = poolData.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();

            foreach (var metric in sizingMetrics)
            {
                var values = poolData.Select(d => metric.Selector(d)).ToArray();
                var plot = MemoryGrantSizingChart.Plot.Add.Scatter(times, values);
                var label = $"Pool {poolId}: {metric.Name}";
                plot.LegendText = label;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[colorIndex % SeriesColors.Length]);
                _memoryGrantSizingHover?.Add(plot, label);
                if (values.Length > 0) sizingMax = Math.Max(sizingMax, values.Max());
                colorIndex++;
            }
        }

        MemoryGrantSizingChart.Plot.Axes.DateTimeTicksBottomDateChange();
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
            var times = poolData.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();

            foreach (var metric in activityMetrics)
            {
                var values = poolData.Select(d => metric.Selector(d)).ToArray();
                var plot = MemoryGrantActivityChart.Plot.Add.Scatter(times, values);
                var label = $"Pool {poolId}: {metric.Name}";
                plot.LegendText = label;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[colorIndex % SeriesColors.Length]);
                _memoryGrantActivityHover?.Add(plot, label);
                if (values.Length > 0) activityMax = Math.Max(activityMax, values.Max());
                colorIndex++;
            }
        }

        MemoryGrantActivityChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(MemoryGrantActivityChart);
        MemoryGrantActivityChart.Plot.YLabel("Count");
        SetChartYLimitsWithLegendPadding(MemoryGrantActivityChart, 0, activityMax > 0 ? activityMax : 10);
        ShowChartLegend(MemoryGrantActivityChart);
        MemoryGrantActivityChart.Refresh();
    }

    /// <summary>
    /// Stacked bar chart of memory pressure events per hour, split by SQL Server (process) vs
    /// Operating System (system) and stacked by severity (medium=indicator 2, severe=indicator >= 3).
    /// </summary>
    private void UpdateMemoryPressureEventsChart(List<MemoryPressureEventRow> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(MemoryPressureEventsChart);
        _memoryPressureEventsHover?.Clear();
        ApplyTheme(MemoryPressureEventsChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

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

            var sqlMediumColor = ScottPlot.Color.FromHex("#FFB74D"); // orange 300
            var sqlSevereColor = ScottPlot.Color.FromHex("#E65100"); // orange 900
            var osMediumColor = ScottPlot.Color.FromHex("#E57373");  // red 300
            var osSevereColor = ScottPlot.Color.FromHex("#B71C1C");  // red 900

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
                double x = g.Key.AddMinutes(UtcOffsetMinutes).ToOADate();

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

    private void UpdateTempDbChart(List<TempDbRow> data)
    {
        ClearChart(TempDbChart);
        _tempDbHover?.Clear();
        ApplyTheme(TempDbChart);

        if (data.Count == 0) { TempDbChart.Refresh(); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var userObj = data.Select(d => d.UserObjectReservedMb).ToArray();
        var internalObj = data.Select(d => d.InternalObjectReservedMb).ToArray();
        var versionStore = data.Select(d => d.VersionStoreReservedMb).ToArray();

        var userPlot = TempDbChart.Plot.Add.Scatter(times, userObj);
        userPlot.LegendText = "User Objects";
        userPlot.Color = ScottPlot.Color.FromHex("#4FC3F7");
        _tempDbHover?.Add(userPlot, "User Objects");

        var internalPlot = TempDbChart.Plot.Add.Scatter(times, internalObj);
        internalPlot.LegendText = "Internal Objects";
        internalPlot.Color = ScottPlot.Color.FromHex("#FFD54F");
        _tempDbHover?.Add(internalPlot, "Internal Objects");

        var vsPlot = TempDbChart.Plot.Add.Scatter(times, versionStore);
        vsPlot.LegendText = "Version Store";
        vsPlot.Color = ScottPlot.Color.FromHex("#81C784");
        _tempDbHover?.Add(vsPlot, "Version Store");

        TempDbChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(TempDbChart);
        TempDbChart.Plot.YLabel("MB");

        var maxVal = new[] { userObj.Max(), internalObj.Max(), versionStore.Max() }.Max();
        SetChartYLimitsWithLegendPadding(TempDbChart, 0, maxVal);

        ShowChartLegend(TempDbChart);
        TempDbChart.Refresh();
    }

    private void UpdateTempDbFileIoChart(List<FileIoTrendPoint> data)
    {
        ClearChart(TempDbFileIoChart);
        _tempDbFileIoHover?.Clear();
        ApplyTheme(TempDbFileIoChart);

        if (data.Count == 0) { TempDbFileIoChart.Refresh(); return; }

        var files = data
            .GroupBy(d => d.DatabaseName)
            .OrderByDescending(g => g.Sum(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs))
            .Take(12)
            .ToList();

        double maxLatency = 0;
        int colorIdx = 0;

        foreach (var fileGroup in files)
        {
            var points = fileGroup.OrderBy(d => d.CollectionTime).ToList();
            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var latency = points.Select(d => d.AvgReadLatencyMs + d.AvgWriteLatencyMs).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (latency.Length > 0)
            {
                var plot = TempDbFileIoChart.Plot.Add.Scatter(times, latency);
                plot.LegendText = fileGroup.Key;
                plot.Color = color;
                _tempDbFileIoHover?.Add(plot, fileGroup.Key);
                maxLatency = Math.Max(maxLatency, latency.Max());
            }
        }

        TempDbFileIoChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(TempDbFileIoChart);
        TempDbFileIoChart.Plot.YLabel("TempDB File I/O Latency (ms)");
        SetChartYLimitsWithLegendPadding(TempDbFileIoChart, 0, maxLatency > 0 ? maxLatency : 10);
        ShowChartLegend(TempDbFileIoChart);
        TempDbFileIoChart.Refresh();
    }

    private void UpdateFileIoCharts(List<FileIoTrendPoint> data)
    {
        ClearChart(FileIoReadChart);
        ClearChart(FileIoWriteChart);
        _fileIoReadHover?.Clear();
        _fileIoWriteHover?.Clear();
        ApplyTheme(FileIoReadChart);
        ApplyTheme(FileIoWriteChart);

        if (data.Count == 0) { FileIoReadChart.Refresh(); FileIoWriteChart.Refresh(); return; }

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
            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var readLatency = points.Select(d => d.AvgReadLatencyMs).ToArray();
            var writeLatency = points.Select(d => d.AvgWriteLatencyMs).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (readLatency.Length > 0)
            {
                var readPlot = FileIoReadChart.Plot.Add.Scatter(times, readLatency);
                readPlot.LegendText = dbGroup.Key;
                readPlot.Color = color;
                _fileIoReadHover?.Add(readPlot, dbGroup.Key);
                readMax = Math.Max(readMax, readLatency.Max());
            }

            if (writeLatency.Length > 0)
            {
                var writePlot = FileIoWriteChart.Plot.Add.Scatter(times, writeLatency);
                writePlot.LegendText = dbGroup.Key;
                writePlot.Color = color;
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
                    var qReadPlot = FileIoReadChart.Plot.Add.Scatter(times, queuedReadLatency);
                    qReadPlot.LegendText = $"{dbGroup.Key} (queued)";
                    qReadPlot.Color = color;
                    qReadPlot.LinePattern = ScottPlot.LinePattern.Dashed;
                    _fileIoReadHover?.Add(qReadPlot, $"{dbGroup.Key} (queued)");
                }

                if (queuedWriteLatency.Any(v => v > 0))
                {
                    var qWritePlot = FileIoWriteChart.Plot.Add.Scatter(times, queuedWriteLatency);
                    qWritePlot.LegendText = $"{dbGroup.Key} (queued)";
                    qWritePlot.Color = color;
                    qWritePlot.LinePattern = ScottPlot.LinePattern.Dashed;
                    _fileIoWriteHover?.Add(qWritePlot, $"{dbGroup.Key} (queued)");
                }
            }
        }

        FileIoReadChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(FileIoReadChart);
        FileIoReadChart.Plot.YLabel("Read Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoReadChart, 0, readMax > 0 ? readMax : 10);
        ShowChartLegend(FileIoReadChart);
        FileIoReadChart.Refresh();

        FileIoWriteChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(FileIoWriteChart);
        FileIoWriteChart.Plot.YLabel("Write Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoWriteChart, 0, writeMax > 0 ? writeMax : 10);
        ShowChartLegend(FileIoWriteChart);
        FileIoWriteChart.Refresh();
    }

    private void UpdateFileIoThroughputCharts(List<FileIoThroughputPoint> data)
    {
        ClearChart(FileIoReadThroughputChart);
        ClearChart(FileIoWriteThroughputChart);
        _fileIoReadThroughputHover?.Clear();
        _fileIoWriteThroughputHover?.Clear();
        ApplyTheme(FileIoReadThroughputChart);
        ApplyTheme(FileIoWriteThroughputChart);

        if (data.Count == 0) { FileIoReadThroughputChart.Refresh(); FileIoWriteThroughputChart.Refresh(); return; }

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
            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var readThroughput = points.Select(d => d.ReadMbPerSec).ToArray();
            var writeThroughput = points.Select(d => d.WriteMbPerSec).ToArray();
            var color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            colorIdx++;

            if (readThroughput.Length > 0)
            {
                var readPlot = FileIoReadThroughputChart.Plot.Add.Scatter(times, readThroughput);
                readPlot.LegendText = fileGroup.Key;
                readPlot.Color = color;
                _fileIoReadThroughputHover?.Add(readPlot, fileGroup.Key);
                readMax = Math.Max(readMax, readThroughput.Max());
            }

            if (writeThroughput.Length > 0)
            {
                var writePlot = FileIoWriteThroughputChart.Plot.Add.Scatter(times, writeThroughput);
                writePlot.LegendText = fileGroup.Key;
                writePlot.Color = color;
                _fileIoWriteThroughputHover?.Add(writePlot, fileGroup.Key);
                writeMax = Math.Max(writeMax, writeThroughput.Max());
            }
        }

        FileIoReadThroughputChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(FileIoReadThroughputChart);
        FileIoReadThroughputChart.Plot.YLabel("Read Throughput (MB/s)");
        SetChartYLimitsWithLegendPadding(FileIoReadThroughputChart, 0, readMax > 0 ? readMax : 1);
        ShowChartLegend(FileIoReadThroughputChart);
        FileIoReadThroughputChart.Refresh();

        FileIoWriteThroughputChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(FileIoWriteThroughputChart);
        FileIoWriteThroughputChart.Plot.YLabel("Write Throughput (MB/s)");
        SetChartYLimitsWithLegendPadding(FileIoWriteThroughputChart, 0, writeMax > 0 ? writeMax : 1);
        ShowChartLegend(FileIoWriteThroughputChart);
        FileIoWriteThroughputChart.Refresh();
    }

    /* ========== Blocking/Deadlock Trend Charts ========== */

    private void UpdateLockWaitTrendChart(List<LockWaitTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(LockWaitTrendChart);
        ApplyTheme(LockWaitTrendChart);

        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        _lockWaitTrendHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = LockWaitTrendChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Lock Waits";
            zeroLine.Color = ScottPlot.Color.FromHex("#4FC3F7");
            zeroLine.MarkerSize = 0;
            LockWaitTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
            LockWaitTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(LockWaitTrendChart);
            LockWaitTrendChart.Plot.YLabel("Lock Wait Time (ms/sec)");
            SetChartYLimitsWithLegendPadding(LockWaitTrendChart, 0, 1);
            ShowChartLegend(LockWaitTrendChart);
            LockWaitTrendChart.Refresh();
            return;
        }

        var grouped = data.GroupBy(d => d.WaitType).ToList();
        double globalMax = 0;

        for (int i = 0; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var times = group.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var values = group.Select(t => t.WaitTimeMsPerSecond).ToArray();

            var plot = LockWaitTrendChart.Plot.Add.Scatter(times, values);
            plot.LegendText = group.Key;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            _lockWaitTrendHover?.Add(plot, group.Key);

            if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
        }

        LockWaitTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        LockWaitTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(LockWaitTrendChart);
        LockWaitTrendChart.Plot.YLabel("Lock Wait Time (ms/sec)");
        SetChartYLimitsWithLegendPadding(LockWaitTrendChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(LockWaitTrendChart);
        LockWaitTrendChart.Refresh();
    }

    private void UpdateBlockingTrendChart(List<TrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(BlockingTrendChart);
        ApplyTheme(BlockingTrendChart);

        /* Calculate X-axis range based on selected time window */
        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        _blockingTrendHover?.Clear();
        if (data.Count == 0)
        {
            /* No blocking events — show a flat line at zero so the chart looks active */
            var zeroLine = BlockingTrendChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Blocking Incidents";
            zeroLine.Color = ScottPlot.Color.FromHex("#E57373");
            zeroLine.MarkerSize = 0;
            BlockingTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
            BlockingTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(BlockingTrendChart);
            BlockingTrendChart.Plot.YLabel("Blocking Incidents");
            SetChartYLimitsWithLegendPadding(BlockingTrendChart, 0, 1);
            ShowChartLegend(BlockingTrendChart);
            BlockingTrendChart.Refresh();
            return;
        }

        /* Build arrays with zero baseline between data points for spike effect */
        var expandedTimes = new List<double>();
        var expandedCounts = new List<double>();

        /* Add zero at start */
        expandedTimes.Add(rangeStart.ToOADate());
        expandedCounts.Add(0);

        foreach (var point in data.OrderBy(d => d.Time))
        {
            var time = point.Time.AddMinutes(UtcOffsetMinutes).ToOADate();
            /* Go to zero just before the spike */
            expandedTimes.Add(time - 0.0001);
            expandedCounts.Add(0);
            /* Spike up */
            expandedTimes.Add(time);
            expandedCounts.Add(point.Count);
            /* Back to zero just after */
            expandedTimes.Add(time + 0.0001);
            expandedCounts.Add(0);
        }

        /* Add zero at end */
        expandedTimes.Add(rangeEnd.ToOADate());
        expandedCounts.Add(0);

        var plot = BlockingTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray());
        plot.LegendText = "Blocking Incidents";
        plot.Color = ScottPlot.Color.FromHex("#E57373");
        plot.MarkerSize = 0; /* No markers, just lines */
        _blockingTrendHover?.Add(plot, "Blocking Incidents");

        BlockingTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        BlockingTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(BlockingTrendChart);
        BlockingTrendChart.Plot.YLabel("Blocking Incidents");
        SetChartYLimitsWithLegendPadding(BlockingTrendChart, 0, data.Max(d => d.Count));
        ShowChartLegend(BlockingTrendChart);
        BlockingTrendChart.Refresh();
    }

    private void UpdateDeadlockTrendChart(List<TrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(DeadlockTrendChart);
        ApplyTheme(DeadlockTrendChart);

        /* Calculate X-axis range based on selected time window */
        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        _deadlockTrendHover?.Clear();
        if (data.Count == 0)
        {
            /* No deadlocks — show a flat line at zero so the chart looks active */
            var zeroLine = DeadlockTrendChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Deadlocks";
            zeroLine.Color = ScottPlot.Color.FromHex("#FFB74D");
            zeroLine.MarkerSize = 0;
            DeadlockTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
            DeadlockTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(DeadlockTrendChart);
            DeadlockTrendChart.Plot.YLabel("Deadlocks");
            SetChartYLimitsWithLegendPadding(DeadlockTrendChart, 0, 1);
            ShowChartLegend(DeadlockTrendChart);
            DeadlockTrendChart.Refresh();
            return;
        }

        /* Build arrays with zero baseline between data points for spike effect */
        var expandedTimes = new List<double>();
        var expandedCounts = new List<double>();

        /* Add zero at start */
        expandedTimes.Add(rangeStart.ToOADate());
        expandedCounts.Add(0);

        foreach (var point in data.OrderBy(d => d.Time))
        {
            var time = point.Time.AddMinutes(UtcOffsetMinutes).ToOADate();
            /* Go to zero just before the spike */
            expandedTimes.Add(time - 0.0001);
            expandedCounts.Add(0);
            /* Spike up */
            expandedTimes.Add(time);
            expandedCounts.Add(point.Count);
            /* Back to zero just after */
            expandedTimes.Add(time + 0.0001);
            expandedCounts.Add(0);
        }

        /* Add zero at end */
        expandedTimes.Add(rangeEnd.ToOADate());
        expandedCounts.Add(0);

        var plot = DeadlockTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray());
        plot.LegendText = "Deadlocks";
        plot.Color = ScottPlot.Color.FromHex("#FFB74D");
        plot.MarkerSize = 0; /* No markers, just lines */
        _deadlockTrendHover?.Add(plot, "Deadlocks");

        DeadlockTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        DeadlockTrendChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(DeadlockTrendChart);
        DeadlockTrendChart.Plot.YLabel("Deadlocks");
        SetChartYLimitsWithLegendPadding(DeadlockTrendChart, 0, data.Max(d => d.Count));
        ShowChartLegend(DeadlockTrendChart);
        DeadlockTrendChart.Refresh();
    }

    /* ========== Current Waits Charts ========== */

    private void UpdateCurrentWaitsDurationChart(List<WaitingTaskTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(CurrentWaitsDurationChart);
        ApplyTheme(CurrentWaitsDurationChart);

        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        _currentWaitsDurationHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = CurrentWaitsDurationChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Current Waits";
            zeroLine.Color = ScottPlot.Color.FromHex("#4FC3F7");
            zeroLine.MarkerSize = 0;
            CurrentWaitsDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CurrentWaitsDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(CurrentWaitsDurationChart);
            CurrentWaitsDurationChart.Plot.YLabel("Total Wait Duration (ms)");
            SetChartYLimitsWithLegendPadding(CurrentWaitsDurationChart, 0, 1);
            ShowChartLegend(CurrentWaitsDurationChart);
            CurrentWaitsDurationChart.Refresh();
            return;
        }

        var grouped = data.GroupBy(d => d.WaitType).OrderBy(g => g.Key).ToList();
        double globalMax = 0;

        for (int i = 0; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var ordered = group.OrderBy(t => t.CollectionTime).ToList();
            var times = ordered.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var values = ordered.Select(t => (double)t.TotalWaitMs).ToArray();

            var plot = CurrentWaitsDurationChart.Plot.Add.Scatter(times, values);
            plot.LegendText = group.Key;
            plot.LineWidth = 2;
            plot.MarkerSize = 5;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            _currentWaitsDurationHover?.Add(plot, group.Key);

            if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
        }

        CurrentWaitsDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
        CurrentWaitsDurationChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(CurrentWaitsDurationChart);
        CurrentWaitsDurationChart.Plot.YLabel("Total Wait Duration (ms)");
        SetChartYLimitsWithLegendPadding(CurrentWaitsDurationChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(CurrentWaitsDurationChart);
        CurrentWaitsDurationChart.Refresh();
    }

    private void UpdateCurrentWaitsBlockedChart(List<BlockedSessionTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(CurrentWaitsBlockedChart);
        ApplyTheme(CurrentWaitsBlockedChart);

        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        _currentWaitsBlockedHover?.Clear();
        if (data.Count == 0)
        {
            var zeroLine = CurrentWaitsBlockedChart.Plot.Add.Scatter(
                new[] { rangeStart.ToOADate(), rangeEnd.ToOADate() },
                new[] { 0.0, 0.0 });
            zeroLine.LegendText = "Blocked Sessions";
            zeroLine.Color = ScottPlot.Color.FromHex("#E57373");
            zeroLine.MarkerSize = 0;
            CurrentWaitsBlockedChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CurrentWaitsBlockedChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
            ReapplyAxisColors(CurrentWaitsBlockedChart);
            CurrentWaitsBlockedChart.Plot.YLabel("Blocked Sessions");
            SetChartYLimitsWithLegendPadding(CurrentWaitsBlockedChart, 0, 1);
            ShowChartLegend(CurrentWaitsBlockedChart);
            CurrentWaitsBlockedChart.Refresh();
            return;
        }

        var grouped = data.GroupBy(d => d.DatabaseName).OrderBy(g => g.Key).ToList();
        double globalMax = 0;

        for (int i = 0; i < grouped.Count; i++)
        {
            var group = grouped[i];
            var ordered = group.OrderBy(t => t.CollectionTime).ToList();
            var times = ordered.Select(t => t.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var values = ordered.Select(t => (double)t.BlockedCount).ToArray();

            var plot = CurrentWaitsBlockedChart.Plot.Add.Scatter(times, values);
            plot.LegendText = group.Key;
            plot.LineWidth = 2;
            plot.MarkerSize = 5;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            _currentWaitsBlockedHover?.Add(plot, group.Key);

            if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
        }

        CurrentWaitsBlockedChart.Plot.Axes.DateTimeTicksBottomDateChange();
        CurrentWaitsBlockedChart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        ReapplyAxisColors(CurrentWaitsBlockedChart);
        CurrentWaitsBlockedChart.Plot.YLabel("Blocked Sessions");
        SetChartYLimitsWithLegendPadding(CurrentWaitsBlockedChart, 0, globalMax > 0 ? globalMax : 1);
        ShowChartLegend(CurrentWaitsBlockedChart);
        CurrentWaitsBlockedChart.Refresh();
    }

    /* ========== Performance Trend Charts ========== */

    private void UpdateQueryDurationTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(QueryDurationTrendChart);
        ApplyTheme(QueryDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryDurationTrendChart, "Query Duration", "Duration (ms/sec)"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _queryDurationTrendHover?.Clear();
        var plot = QueryDurationTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Query Duration";
        plot.Color = ScottPlot.Color.FromHex("#4FC3F7");
        _queryDurationTrendHover?.Add(plot, "Query Duration");

        QueryDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(QueryDurationTrendChart);
        QueryDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryDurationTrendChart);
        QueryDurationTrendChart.Refresh();
    }

    private void UpdateProcDurationTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(ProcDurationTrendChart);
        ApplyTheme(ProcDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ProcDurationTrendChart, "Procedure Duration", "Duration (ms/sec)"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _procDurationTrendHover?.Clear();
        var plot = ProcDurationTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Procedure Duration";
        plot.Color = ScottPlot.Color.FromHex("#81C784");
        _procDurationTrendHover?.Add(plot, "Procedure Duration");

        ProcDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(ProcDurationTrendChart);
        ProcDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(ProcDurationTrendChart, 0, values.Max());
        ShowChartLegend(ProcDurationTrendChart);
        ProcDurationTrendChart.Refresh();
    }

    private void UpdateQueryStoreDurationTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(QueryStoreDurationTrendChart);
        ApplyTheme(QueryStoreDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryStoreDurationTrendChart, "Query Store Duration", "Duration (ms/sec)"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _queryStoreDurationTrendHover?.Clear();
        var plot = QueryStoreDurationTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Query Store Duration";
        plot.Color = ScottPlot.Color.FromHex("#FFB74D");
        _queryStoreDurationTrendHover?.Add(plot, "Query Store Duration");

        QueryStoreDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryStoreDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Refresh();
    }

    private void UpdateExecutionCountTrendChart(List<QueryTrendPoint> data)
    {
        ClearChart(ExecutionCountTrendChart);
        ApplyTheme(ExecutionCountTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ExecutionCountTrendChart, "Executions", "Executions/sec"); return; }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _executionCountTrendHover?.Clear();
        var plot = ExecutionCountTrendChart.Plot.Add.Scatter(times, values);
        plot.LegendText = "Executions";
        plot.Color = ScottPlot.Color.FromHex("#BA68C8");
        _executionCountTrendHover?.Add(plot, "Executions");

        ExecutionCountTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Plot.YLabel("Executions/sec");
        SetChartYLimitsWithLegendPadding(ExecutionCountTrendChart, 0, values.Max());
        ShowChartLegend(ExecutionCountTrendChart);
        ExecutionCountTrendChart.Refresh();
    }

    /* ========== Query Heatmap ========== */

    private void UpdateQueryHeatmapChart(HeatmapResult result)
    {
        AppLogger.Info("ServerTab", $"[{_server.DisplayName}] UpdateQueryHeatmapChart called: TimeBuckets={result.TimeBuckets.Length}, Grid={result.Intensities.GetLength(0)}x{result.Intensities.GetLength(1)}, BucketLabels={result.BucketLabels.Length}");
        ClearChart(QueryHeatmapChart);
        ApplyTheme(QueryHeatmapChart);

        _lastHeatmapResult = result;

        if (result.TimeBuckets.Length == 0 || result.BucketLabels.Length == 0)
        {
            RefreshEmptyChart(QueryHeatmapChart, "Query Heatmap", "");
            return;
        }

        int numRows = result.Intensities.GetLength(0);
        int numCols = result.Intensities.GetLength(1);

        // Log1p scaling; NaN for empty cells so they render as background.
        var scaled = new double[numRows, numCols];
        for (int r = 0; r < numRows; r++)
        {
            for (int c = 0; c < numCols; c++)
            {
                scaled[r, c] = result.Intensities[r, c] > 0
                    ? Math.Log(1 + result.Intensities[r, c])
                    : double.NaN;
            }
        }

        var heatmap = QueryHeatmapChart.Plot.Add.Heatmap(scaled);
        _heatmapPlottable = heatmap;
        heatmap.FlipVertically = true; // row 0 ("0-1ms") at bottom, row 6 (">100s") at top
        heatmap.Colormap = new ScottPlot.Colormaps.Viridis();
        heatmap.NaNCellColor = QueryHeatmapChart.Plot.DataBackground.Color;

        // Let ScottPlot use default extent (0..numCols, 0..numRows).
        // No custom Position — avoids cell-centering offset issues.
        // Use manual tick labels for both axes instead.
        ReapplyAxisColors(QueryHeatmapChart);

        // X-axis: time labels at column positions
        var xTicks = new ScottPlot.TickGenerators.NumericManual();
        int xStep = Math.Max(1, numCols / 12); // ~12 labels max
        for (int i = 0; i < numCols; i += xStep)
        {
            var t = result.TimeBuckets[i].AddMinutes(UtcOffsetMinutes);
            xTicks.AddMajor(i, t.ToString("M/d\nh:mm tt"));
        }
        QueryHeatmapChart.Plot.Axes.Bottom.TickGenerator = xTicks;
        QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Left.TickLabelStyle.ForeColor;

        // Y-axis: bucket labels
        var yTicks = new ScottPlot.TickGenerators.NumericManual();
        for (int i = 0; i < result.BucketLabels.Length; i++)
        {
            yTicks.AddMajor(i, result.BucketLabels[i]);
        }
        QueryHeatmapChart.Plot.Axes.Left.TickGenerator = yTicks;

        // Axis limits match default heatmap extent
        QueryHeatmapChart.Plot.Axes.SetLimitsX(-0.5, numCols - 0.5);
        QueryHeatmapChart.Plot.Axes.SetLimitsY(-0.5, numRows - 0.5);

        // Colorbar with real query counts (undo log1p for tick labels)
        var colorBar = new ScottPlot.Panels.ColorBar(heatmap, ScottPlot.Edge.Right);
        colorBar.Label = "Query Count";
        colorBar.LabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;
        colorBar.Axis.TickLabelStyle.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;
        double maxRaw = 0;
        for (int r = 0; r < numRows; r++)
            for (int c = 0; c < numCols; c++)
                if (result.Intensities[r, c] > maxRaw) maxRaw = result.Intensities[r, c];
        var cbTicks = new ScottPlot.TickGenerators.NumericManual();
        cbTicks.AddMajor(0, "0");
        int[] niceValues = { 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000 };
        foreach (var n in niceValues)
        {
            if (n > maxRaw) break;
            cbTicks.AddMajor(Math.Log(1 + n), n.ToString("N0"));
        }
        cbTicks.AddMajor(Math.Log(1 + maxRaw), ((int)maxRaw).ToString("N0"));
        colorBar.Axis.TickGenerator = cbTicks;
        QueryHeatmapChart.Plot.Axes.AddPanel(colorBar);
        _legendPanels[QueryHeatmapChart] = colorBar;

        var metricName = ((ComboBoxItem)HeatmapMetricCombo.SelectedItem).Content?.ToString() ?? "Duration (ms)";
        QueryHeatmapChart.Plot.Title($"Query Distribution by {metricName}");
        QueryHeatmapChart.Plot.Axes.Title.Label.ForeColor = QueryHeatmapChart.Plot.Axes.Bottom.TickLabelStyle.ForeColor;

        QueryHeatmapChart.Refresh();
    }

    private DateTime _lastHeatmapHoverUpdate;

    private void HeatmapChart_MouseLeave(object sender, System.Windows.Input.MouseEventArgs e)
    {
        if (_heatmapPopup != null) _heatmapPopup.IsOpen = false;
    }

    private void HeatmapChart_MouseMove(object sender, System.Windows.Input.MouseEventArgs e)
    {
        if (_heatmapPopup == null || _heatmapPopupText == null || _heatmapPlottable == null) return;
        if (_lastHeatmapResult == null || _lastHeatmapResult.TimeBuckets.Length == 0) return;

        var now = DateTime.UtcNow;
        if ((now - _lastHeatmapHoverUpdate).TotalMilliseconds < 50) return;
        _lastHeatmapHoverUpdate = now;

        var pos = e.GetPosition(QueryHeatmapChart);
        var dpi = VisualTreeHelper.GetDpi(QueryHeatmapChart);
        var pixel = new ScottPlot.Pixel(
            (float)(pos.X * dpi.DpiScaleX),
            (float)(pos.Y * dpi.DpiScaleY));
        var coords = QueryHeatmapChart.Plot.GetCoordinates(pixel);

        int numRows = _lastHeatmapResult.Intensities.GetLength(0);
        int numCols = _lastHeatmapResult.Intensities.GetLength(1);

        // Default heatmap extent (no custom Position): cols = 0..numCols, rows = 0..numRows.
        // GetIndexes returns bitmap indices. With FlipVertically=true, flip row for data index.
        var (col, rowIdx) = _heatmapPlottable.GetIndexes(coords);
        int row = (numRows - 1) - rowIdx;

        if (row < 0 || row >= numRows || col < 0 || col >= numCols)
        {
            _heatmapPopup.IsOpen = false;
            return;
        }

        long count = (long)_lastHeatmapResult.Intensities[row, col];
        if (count == 0)
        {
            _heatmapPopup.IsOpen = false;
            return;
        }

        var cell = _lastHeatmapResult.CellDetails[row, col];
        var time = ServerTimeHelper.ConvertForDisplay(
            _lastHeatmapResult.TimeBuckets[col].AddMinutes(UtcOffsetMinutes),
            ServerTimeHelper.CurrentDisplayMode);
        var bucketLabel = row < _lastHeatmapResult.BucketLabels.Length
            ? _lastHeatmapResult.BucketLabels[row]
            : "?";

        var tipText = $"{time:HH:mm:ss}  |  {bucketLabel}  |  {count:N0} queries";
        if (cell != null && !string.IsNullOrEmpty(cell.TopQueryText))
        {
            // Single line, collapse whitespace, truncate
            var flat = System.Text.RegularExpressions.Regex.Replace(cell.TopQueryText, @"\s+", " ").Trim();
            if (flat.Length > 60) flat = flat[..60] + "...";
            tipText += $"\n{flat}";
        }
        _heatmapPopupText.Text = tipText;

        _heatmapPopup.HorizontalOffset = pos.X + 15;
        _heatmapPopup.VerticalOffset = pos.Y + 15;
        _heatmapPopup.IsOpen = true;
    }

    private async void HeatmapMetric_SelectionChanged(object sender, System.Windows.Controls.SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        try
        {
            var hoursBack = GetHoursBack();
            DateTime? fromDate = null, toDate = null;
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
            var metric = (HeatmapMetric)HeatmapMetricCombo.SelectedIndex;
            var result = await _dataService.GetQueryHeatmapAsync(_serverId, metric, hoursBack, fromDate, toDate);
            UpdateQueryHeatmapChart(result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] HeatmapMetric_SelectionChanged failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Clears a chart and removes any existing legend panel to prevent duplication.
    /// </summary>
    private void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
        {
            chart.Plot.Axes.Remove(existingPanel);
            _legendPanels[chart] = null;
        }

        /* Reset fully — Plot.Clear() leaves stale DateTime axes behind,
           and DateTimeTicksBottom() replaces the axis object entirely.
           Resetting the plot object avoids tick generator type mismatches. */
        chart.Reset();
        chart.Plot.Clear();
    }

    /// <summary>
    /// Sets up an empty chart with dark theme, Y-axis label, legend, and "No Data" annotation.
    /// Matches Full Dashboard behavior for consistent UX.
    /// </summary>
    private void RefreshEmptyChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel)
    {
        ReapplyAxisColors(chart);

        /* Add invisible scatter to create legend entry (matches data chart layout) */
        var placeholder = chart.Plot.Add.Scatter(new double[] { 0 }, new double[] { 0 });
        placeholder.LegendText = legendText;
        placeholder.Color = ScottPlot.Color.FromHex("#888888");
        placeholder.MarkerSize = 0;
        placeholder.LineWidth = 0;

        /* Add centered "No Data" text */
        var text = chart.Plot.Add.Text($"{legendText}\nNo Data", 0, 0);
        text.LabelFontColor = ScottPlot.Color.FromHex("#888888");
        text.LabelFontSize = 14;
        text.LabelAlignment = ScottPlot.Alignment.MiddleCenter;

        /* Configure axes */
        chart.Plot.HideGrid();
        chart.Plot.Axes.SetLimitsX(-1, 1);
        chart.Plot.Axes.SetLimitsY(-1, 1);
        chart.Plot.Axes.Bottom.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.Axes.Left.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.YLabel(yAxisLabel);

        /* Show legend to match data chart layout */
        ShowChartLegend(chart);
        chart.Refresh();
    }

    /// <summary>
    /// Shows legend on chart and tracks it for proper cleanup on next refresh.
    /// </summary>
    private void ShowChartLegend(ScottPlot.WPF.WpfPlot chart)
    {
        _legendPanels[chart] = chart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
        chart.Plot.Legend.FontSize = 13;
    }

    /// <summary>
    /// Applies the Darling Data dark theme to a ScottPlot chart.
    /// Matches Dashboard TabHelpers.ApplyThemeToChart exactly.
    /// </summary>
    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart)
    {
        ScottPlot.Color figureBackground, dataBackground, textColor, gridColor, legendBg, legendFg, legendOutline;

        if (Helpers.ThemeManager.CurrentTheme == "CoolBreeze")
        {
            figureBackground = ScottPlot.Color.FromHex("#EEF4FA");
            dataBackground   = ScottPlot.Color.FromHex("#DAE6F0");
            textColor        = ScottPlot.Color.FromHex("#1A2A3A");
            gridColor        = ScottPlot.Color.FromHex("#A8BDD0").WithAlpha(120);
            legendBg         = ScottPlot.Color.FromHex("#EEF4FA");
            legendFg         = ScottPlot.Color.FromHex("#1A2A3A");
            legendOutline    = ScottPlot.Color.FromHex("#A8BDD0");
        }
        else if (Helpers.ThemeManager.HasLightBackground)
        {
            figureBackground = ScottPlot.Color.FromHex("#FFFFFF");
            dataBackground   = ScottPlot.Color.FromHex("#F5F7FA");
            textColor        = ScottPlot.Color.FromHex("#1A1D23");
            gridColor        = ScottPlot.Colors.Black.WithAlpha(20);
            legendBg         = ScottPlot.Color.FromHex("#FFFFFF");
            legendFg         = ScottPlot.Color.FromHex("#1A1D23");
            legendOutline    = ScottPlot.Color.FromHex("#DEE2E6");
        }
        else
        {
            figureBackground = ScottPlot.Color.FromHex("#22252b");
            dataBackground   = ScottPlot.Color.FromHex("#111217");
            textColor        = ScottPlot.Color.FromHex("#E4E6EB");
            gridColor        = ScottPlot.Colors.White.WithAlpha(40);
            legendBg         = ScottPlot.Color.FromHex("#22252b");
            legendFg         = ScottPlot.Color.FromHex("#E4E6EB");
            legendOutline    = ScottPlot.Color.FromHex("#2a2d35");
        }

        chart.Plot.FigureBackground.Color = figureBackground;
        chart.Plot.DataBackground.Color = dataBackground;
        chart.Plot.Axes.Color(textColor);
        chart.Plot.Grid.MajorLineColor = gridColor;
        chart.Plot.Legend.BackgroundColor = legendBg;
        chart.Plot.Legend.FontColor = legendFg;
        chart.Plot.Legend.OutlineColor = legendOutline;
        chart.Plot.Legend.Alignment = ScottPlot.Alignment.LowerCenter;
        chart.Plot.Legend.Orientation = ScottPlot.Orientation.Horizontal;
        chart.Plot.Axes.Margins(bottom: 0); /* No bottom margin - SetChartYLimitsWithLegendPadding handles Y-axis */

        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
        chart.Plot.Axes.Left.Label.ForeColor = textColor;
        chart.Plot.Axes.Bottom.TickLabelStyle.FontSize = 13;
        chart.Plot.Axes.Left.TickLabelStyle.FontSize = 13;

        // Set the WPF control Background to match so no white flash appears before ScottPlot's render loop fires
        chart.Background = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(figureBackground.R, figureBackground.G, figureBackground.B));

        // Ensure ScottPlot renders with the correct colors the very first time it gets pixel dimensions.
        chart.Loaded -= HandleChartFirstLoaded;
        if (!chart.IsLoaded)
            chart.Loaded += HandleChartFirstLoaded;
    }

    private static void HandleChartFirstLoaded(object sender, RoutedEventArgs e)
    {
        var chart = (ScottPlot.WPF.WpfPlot)sender;
        chart.Loaded -= HandleChartFirstLoaded;
        chart.Refresh();
    }

    private void OnThemeChanged(string _)
    {
        foreach (var field in GetType().GetFields(
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance))
        {
            if (field.GetValue(this) is ScottPlot.WPF.WpfPlot chart)
            {
                ApplyTheme(chart);
                chart.Refresh();
            }
        }

        CorrelatedLanes.ReapplyTheme();
    }

    private static IEnumerable<ScottPlot.WPF.WpfPlot> GetAllCharts(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            if (child is ScottPlot.WPF.WpfPlot plot)
                yield return plot;
            foreach (var nested in GetAllCharts(child))
                yield return nested;
        }
    }

    /// <summary>
    /// Reapplies theme-appropriate text colors and font sizes after DateTimeTicksBottom() resets them.
    /// </summary>
    private static void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart)
    {
        var textColor = Helpers.ThemeManager.CurrentTheme == "CoolBreeze"
            ? ScottPlot.Color.FromHex("#1A2A3A")
            : Helpers.ThemeManager.HasLightBackground
                ? ScottPlot.Color.FromHex("#1A1D23")
                : ScottPlot.Color.FromHex("#E4E6EB");
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = textColor;
        chart.Plot.Axes.Bottom.Label.ForeColor = textColor;
        chart.Plot.Axes.Left.Label.ForeColor = textColor;
        chart.Plot.Axes.Bottom.TickLabelStyle.FontSize = 13;
        chart.Plot.Axes.Left.TickLabelStyle.FontSize = 13;
    }

    /// <summary>
    /// Sets Y-axis limits with padding for bottom legend and top breathing room.
    /// </summary>
    private static void SetChartYLimitsWithLegendPadding(ScottPlot.WPF.WpfPlot chart, double dataYMin = 0, double dataYMax = 0)
    {
        if (dataYMin == 0 && dataYMax == 0)
        {
            var limits = chart.Plot.Axes.GetLimits();
            dataYMin = limits.Bottom;
            dataYMax = limits.Top;
        }
        if (dataYMax <= dataYMin) dataYMax = dataYMin + 1;

        double range = dataYMax - dataYMin;
        double topPadding = range * 0.05;

        /* Add small bottom margin when dataYMin is zero so flat lines at Y=0 are visible above the axis */
        double yMin = dataYMin > 0 ? 0 : dataYMin == 0 ? -(range * 0.05) : dataYMin - (range * 0.10);
        double yMax = dataYMax + topPadding;

        chart.Plot.Axes.SetLimitsY(yMin, yMax);
    }

    /* ========== Collection Health ========== */

    private void UpdateCollectorDurationChart(List<CollectionLogRow> data)
    {
        ClearChart(CollectorDurationChart);
        ApplyTheme(CollectorDurationChart);

        if (data.Count == 0) { CollectorDurationChart.Refresh(); return; }

        /* Group by collector, plot each as a separate series */
        var groups = data
            .Where(d => d.DurationMs.HasValue && d.Status == "SUCCESS")
            .GroupBy(d => d.CollectorName)
            .OrderBy(g => g.Key)
            .ToList();

        _collectorDurationHover?.Clear();
        int colorIdx = 0;
        foreach (var group in groups)
        {
            var points = group.OrderBy(d => d.CollectionTime).ToList();
            if (points.Count < 2) continue;

            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
            var durations = points.Select(d => (double)d.DurationMs!.Value).ToArray();

            var scatter = CollectorDurationChart.Plot.Add.Scatter(times, durations);
            scatter.LegendText = group.Key;
            scatter.Color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            scatter.LineWidth = 2;
            scatter.MarkerSize = 0;
            _collectorDurationHover?.Add(scatter, group.Key);
            colorIdx++;
        }

        CollectorDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(CollectorDurationChart);
        CollectorDurationChart.Plot.YLabel("Duration (ms)");
        CollectorDurationChart.Plot.Axes.AutoScale();
        ShowChartLegend(CollectorDurationChart);
        CollectorDurationChart.Refresh();
    }
}

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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    // Cycling palette sourced from the shared ChartPalette so Lite and Dashboard cycle through the
    // SAME colors in the same order (Lite's old array was reordered, breaking cross-app parity).
    private static readonly string[] SeriesColors = ChartPalette.CyclingPalette.ToArray();

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


    private void UpdateCpuChart(List<CpuUtilizationRow> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(CpuChart);
        _cpuHover?.Clear();
        ApplyTheme(CpuChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            CpuChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CpuChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ReapplyAxisColors(CpuChart);
            CpuChart.Refresh();
            return;
        }

        var times = data.Select(d => d.SampleTime.ToOADate()).ToArray();
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
        CpuChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(CpuChart);
        CpuChart.Plot.YLabel("CPU %");
        CpuChart.Plot.Axes.SetLimitsY(0, 105);

        ShowChartLegend(CpuChart);
        CpuChart.Refresh();
    }

    private void UpdateMemoryChart(List<MemoryTrendPoint> data, List<MemoryTrendPoint> grantData, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(MemoryChart);
        _memoryHover?.Clear();
        ApplyTheme(MemoryChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            MemoryChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ReapplyAxisColors(MemoryChart);
            MemoryChart.Refresh();
            return;
        }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
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
        targetPlot.LineStyle.Pattern = LinePattern.Dashed;
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
            grantTimes = grantData.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
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
        MemoryChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(MemoryChart);
        MemoryChart.Plot.YLabel("Memory (GB)");

        var maxVal = totalMem.Max();
        SetChartYLimitsWithLegendPadding(MemoryChart, 0, maxVal);

        ShowChartLegend(MemoryChart);
        MemoryChart.Refresh();
    }

    private void UpdateMemoryGrantCharts(List<MemoryGrantChartPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(MemoryGrantSizingChart);
        ClearChart(MemoryGrantActivityChart);
        _memoryGrantSizingHover?.Clear();
        _memoryGrantActivityHover?.Clear();
        ApplyTheme(MemoryGrantSizingChart);
        ApplyTheme(MemoryGrantActivityChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            foreach (var c in new[] { MemoryGrantSizingChart, MemoryGrantActivityChart })
            {
                c.Plot.Axes.DateTimeTicksBottomDateChange();
                c.Plot.Axes.SetLimitsX(xMin, xMax);
                ReapplyAxisColors(c);
                c.Refresh();
            }
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
                var plot = MemoryGrantSizingChart.Plot.Add.TimeSeries(times, values);
                var label = $"Pool {poolId}: {metric.Name}";
                plot.LegendText = label;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[colorIndex % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
                _memoryGrantSizingHover?.Add(plot, label);
                if (values.Length > 0) sizingMax = Math.Max(sizingMax, values.Max());
                colorIndex++;
            }
        }

        MemoryGrantSizingChart.Plot.Axes.DateTimeTicksBottomDateChange();
        MemoryGrantSizingChart.Plot.Axes.SetLimitsX(xMin, xMax);
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
        MemoryGrantActivityChart.Plot.Axes.SetLimitsX(xMin, xMax);
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

    private void UpdateTempDbChart(List<TempDbRow> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(TempDbChart);
        _tempDbHover?.Clear();
        ApplyTheme(TempDbChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            TempDbChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempDbChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ReapplyAxisColors(TempDbChart);
            TempDbChart.Refresh();
            return;
        }

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
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
        TempDbChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(TempDbChart);
        TempDbChart.Plot.YLabel("MB");

        var maxVal = new[] { userObj.Max(), internalObj.Max(), versionStore.Max() }.Max();
        SetChartYLimitsWithLegendPadding(TempDbChart, 0, maxVal);

        ShowChartLegend(TempDbChart);
        TempDbChart.Refresh();
    }

    // Dedicated chart for tempdb TOTAL allocated size (used + unallocated free space) over time — the
    // growth trend, on its own scale so it doesn't flatten the usage series above. Mirror of Dashboard.
    private void UpdateTempDbSizeChart(List<TempDbRow> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(TempDbSizeChart);
        ApplyTheme(TempDbSizeChart);
        _tempDbSizeHover?.Clear();

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            TempDbSizeChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempDbSizeChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ReapplyAxisColors(TempDbSizeChart);
            TempDbSizeChart.Refresh();
            return;
        }

        var sorted = data.OrderBy(d => d.CollectionTime).ToList();
        var times = sorted.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var totals = sorted.Select(d => d.TotalReservedMb + d.UnallocatedMb).ToArray();

        var sizePlot = TempDbSizeChart.Plot.Add.TimeSeries(times, totals);
        sizePlot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("UnallocatedTempdb"));
        ChartStyle.StyleScatter(sizePlot);
        _tempDbSizeHover?.Add(sizePlot, "Allocated MB");

        TempDbSizeChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(TempDbSizeChart);
        TempDbSizeChart.Plot.YLabel("Allocated MB");
        TempDbSizeChart.Plot.Axes.AutoScaleY();
        TempDbSizeChart.Plot.Axes.SetLimitsX(xMin, xMax);
        TempDbSizeChart.Refresh();
    }

    private void UpdateTempDbFileIoChart(List<FileIoTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(TempDbFileIoChart);
        _tempDbFileIoHover?.Clear();
        ApplyTheme(TempDbFileIoChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            TempDbFileIoChart.Plot.Axes.DateTimeTicksBottomDateChange();
            TempDbFileIoChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ReapplyAxisColors(TempDbFileIoChart);
            TempDbFileIoChart.Refresh();
            return;
        }

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
                var plot = TempDbFileIoChart.Plot.Add.TimeSeries(times, latency);
                plot.LegendText = fileGroup.Key;
                plot.Color = color;
                ChartStyle.StyleScatter(plot);
                _tempDbFileIoHover?.Add(plot, fileGroup.Key);
                maxLatency = Math.Max(maxLatency, latency.Max());
            }
        }

        TempDbFileIoChart.Plot.Axes.DateTimeTicksBottomDateChange();
        TempDbFileIoChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(TempDbFileIoChart);
        TempDbFileIoChart.Plot.YLabel("tempdb File I/O Latency (ms)");
        SetChartYLimitsWithLegendPadding(TempDbFileIoChart, 0, maxLatency > 0 ? maxLatency : 10);
        ShowChartLegend(TempDbFileIoChart);
        TempDbFileIoChart.Refresh();
    }

    private void UpdateFileIoCharts(List<FileIoTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(FileIoReadChart);
        ClearChart(FileIoWriteChart);
        _fileIoReadHover?.Clear();
        _fileIoWriteHover?.Clear();
        ApplyTheme(FileIoReadChart);
        ApplyTheme(FileIoWriteChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            foreach (var c in new[] { FileIoReadChart, FileIoWriteChart })
            {
                c.Plot.Axes.DateTimeTicksBottomDateChange();
                c.Plot.Axes.SetLimitsX(xMin, xMax);
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
            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
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

        FileIoReadChart.Plot.Axes.DateTimeTicksBottomDateChange();
        FileIoReadChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(FileIoReadChart);
        FileIoReadChart.Plot.YLabel("Read Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoReadChart, 0, readMax > 0 ? readMax : 10);
        ShowChartLegend(FileIoReadChart);
        FileIoReadChart.Refresh();

        FileIoWriteChart.Plot.Axes.DateTimeTicksBottomDateChange();
        FileIoWriteChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(FileIoWriteChart);
        FileIoWriteChart.Plot.YLabel("Write Latency (ms)");
        SetChartYLimitsWithLegendPadding(FileIoWriteChart, 0, writeMax > 0 ? writeMax : 10);
        ShowChartLegend(FileIoWriteChart);
        FileIoWriteChart.Refresh();
    }

    private void UpdateFileIoThroughputCharts(List<FileIoThroughputPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(FileIoReadThroughputChart);
        ClearChart(FileIoWriteThroughputChart);
        _fileIoReadThroughputHover?.Clear();
        _fileIoWriteThroughputHover?.Clear();
        ApplyTheme(FileIoReadThroughputChart);
        ApplyTheme(FileIoWriteThroughputChart);

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            foreach (var c in new[] { FileIoReadThroughputChart, FileIoWriteThroughputChart })
            {
                c.Plot.Axes.DateTimeTicksBottomDateChange();
                c.Plot.Axes.SetLimitsX(xMin, xMax);
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
            var times = points.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
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

        FileIoReadThroughputChart.Plot.Axes.DateTimeTicksBottomDateChange();
        FileIoReadThroughputChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(FileIoReadThroughputChart);
        FileIoReadThroughputChart.Plot.YLabel("Read Throughput (MB/s)");
        SetChartYLimitsWithLegendPadding(FileIoReadThroughputChart, 0, readMax > 0 ? readMax : 1);
        ShowChartLegend(FileIoReadThroughputChart);
        FileIoReadThroughputChart.Refresh();

        FileIoWriteThroughputChart.Plot.Axes.DateTimeTicksBottomDateChange();
        FileIoWriteThroughputChart.Plot.Axes.SetLimitsX(xMin, xMax);
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
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("LockWaits"));
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

            var plot = LockWaitTrendChart.Plot.Add.TimeSeries(times, values);
            plot.LegendText = group.Key;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            ChartStyle.StyleScatter(plot);
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
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
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

        var plot = BlockingTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray()); /* synthetic spike baseline (±0.0001d offsets), NOT a cadence series - gap-breaking would lock onto the artificial deltas (#1944 review) */
        plot.LegendText = "Blocking Incidents";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking"));
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
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
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

        var plot = DeadlockTrendChart.Plot.Add.Scatter(expandedTimes.ToArray(), expandedCounts.ToArray()); /* synthetic spike baseline (±0.0001d offsets), NOT a cadence series - gap-breaking would lock onto the artificial deltas (#1944 review) */
        plot.LegendText = "Deadlocks";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks"));
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
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("CurrentWaits"));
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

            var plot = CurrentWaitsDurationChart.Plot.Add.TimeSeries(times, values);
            plot.LegendText = group.Key;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            ChartStyle.StyleScatter(plot);
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
            zeroLine.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("BlockedSessions"));
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

            var plot = CurrentWaitsBlockedChart.Plot.Add.TimeSeries(times, values);
            plot.LegendText = group.Key;
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            ChartStyle.StyleScatter(plot);
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

    private void UpdateQueryDurationTrendChart(List<QueryTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(QueryDurationTrendChart);
        ApplyTheme(QueryDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryDurationTrendChart, "Query Duration", "Duration (ms/sec)"); return; }

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _queryDurationTrendHover?.Clear();
        var plot = QueryDurationTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Query Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("QueryDuration"));
        ChartStyle.StyleScatter(plot);
        _queryDurationTrendHover?.Add(plot, "Query Duration");

        QueryDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        QueryDurationTrendChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(QueryDurationTrendChart);
        QueryDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryDurationTrendChart);
        QueryDurationTrendChart.Refresh();
    }

    private void UpdateProcDurationTrendChart(List<QueryTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(ProcDurationTrendChart);
        ApplyTheme(ProcDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ProcDurationTrendChart, "Procedure Duration", "Duration (ms/sec)"); return; }

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _procDurationTrendHover?.Clear();
        var plot = ProcDurationTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Procedure Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("ProcedureDuration"));
        ChartStyle.StyleScatter(plot);
        _procDurationTrendHover?.Add(plot, "Procedure Duration");

        ProcDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ProcDurationTrendChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(ProcDurationTrendChart);
        ProcDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(ProcDurationTrendChart, 0, values.Max());
        ShowChartLegend(ProcDurationTrendChart);
        ProcDurationTrendChart.Refresh();
    }

    private void UpdateQueryStoreDurationTrendChart(List<QueryTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(QueryStoreDurationTrendChart);
        ApplyTheme(QueryStoreDurationTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(QueryStoreDurationTrendChart, "Query Store Duration", "Duration (ms/sec)"); return; }

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _queryStoreDurationTrendHover?.Clear();
        var plot = QueryStoreDurationTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Query Store Duration";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("QueryStoreDuration"));
        ChartStyle.StyleScatter(plot);
        _queryStoreDurationTrendHover?.Add(plot, "Query Store Duration");

        QueryStoreDurationTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        QueryStoreDurationTrendChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ReapplyAxisColors(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Plot.YLabel("Duration (ms/sec)");
        SetChartYLimitsWithLegendPadding(QueryStoreDurationTrendChart, 0, values.Max());
        ShowChartLegend(QueryStoreDurationTrendChart);
        QueryStoreDurationTrendChart.Refresh();
    }

    private void UpdateExecutionCountTrendChart(List<QueryTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(ExecutionCountTrendChart);
        ApplyTheme(ExecutionCountTrendChart);

        if (data.Count == 0) { RefreshEmptyChart(ExecutionCountTrendChart, "Executions", "Executions/sec"); return; }

        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        var times = data.Select(d => d.CollectionTime.AddMinutes(UtcOffsetMinutes).ToOADate()).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        _executionCountTrendHover?.Clear();
        var plot = ExecutionCountTrendChart.Plot.Add.TimeSeries(times, values);
        plot.LegendText = "Executions";
        plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Executions"));
        ChartStyle.StyleScatter(plot);
        _executionCountTrendHover?.Add(plot, "Executions");

        ExecutionCountTrendChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ExecutionCountTrendChart.Plot.Axes.SetLimitsX(xMin, xMax);
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

        // X-axis: time labels at column positions. #1831: NumericManual labels bypass the shared
        // DateTime formatter, so this axis converts for display itself — matching this same
        // chart's tooltip, which already goes through ConvertForDisplay.
        var xTicks = new ScottPlot.TickGenerators.NumericManual();
        int xStep = Math.Max(1, numCols / 12); // ~12 labels max
        for (int i = 0; i < numCols; i += xStep)
        {
            var t = UiTimeContext.ConvertForDisplay(result.TimeBuckets[i].AddMinutes(UtcOffsetMinutes));
            xTicks.AddMajor(i, t.ToString("M/d\nHH:mm"));
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
        _chartHelper.SetLegendPanel(QueryHeatmapChart, colorBar);

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
            var result = await System.Threading.Tasks.Task.Run(() => _dataService.GetQueryHeatmapAsync(_serverId, metric, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
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
    private void ClearChart(ScottPlot.WPF.WpfPlot chart) => _chartHelper.ClearChart(chart);

    /// <summary>
    /// Sets up an empty chart with dark theme, Y-axis label, legend, and "No Data" annotation.
    /// Matches Full Dashboard behavior for consistent UX.
    /// </summary>
    private void RefreshEmptyChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel)
        => _chartHelper.RefreshEmptyChart(chart, legendText, yAxisLabel);

    /// <summary>
    /// Shows legend on chart and tracks it for proper cleanup on next refresh.
    /// </summary>
    private void ShowChartLegend(ScottPlot.WPF.WpfPlot chart) => _chartHelper.ShowChartLegend(chart);

    /// <summary>
    /// Applies the chrome theme to a ScottPlot chart.
    /// Delegates to the shared <see cref="ChartStyle"/> — single source of truth across apps.
    /// </summary>
    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart) => ChartStyle.ApplyThemeToChart(chart);

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

    /// <summary>
    /// Reapplies theme-appropriate axis text colors/sizes after DateTimeTicksBottom() resets them.
    /// Delegates to the shared <see cref="ChartStyle"/>.
    /// </summary>
    private static void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart) => ChartStyle.ReapplyAxisColors(chart);

    /// <summary>
    /// Sets Y-axis limits with padding for bottom legend and top breathing room.
    /// Delegates to the shared <see cref="ChartStyle"/>.
    /// </summary>
    private static void SetChartYLimitsWithLegendPadding(ScottPlot.WPF.WpfPlot chart, double dataYMin = 0, double dataYMax = 0)
        => ChartStyle.SetChartYLimitsWithLegendPadding(chart, dataYMin, dataYMax);

    /// <summary>
    /// Per-chart Revert / double-click axis reset (passed to <see cref="ContextMenuHelper.SetupChartContextMenu"/>
    /// as its revertAction). Re-pins the X axis to the current settable window (+ auto-fit Y) instead of
    /// AutoScale()'ing to the data range, which would re-introduce ScottPlot's ~10% side dead-space on every
    /// interaction. Clears an active click-isolate first (state parity). The Query Heatmap is the one exception:
    /// its X axis is categorical (bucket columns), not a time window, so it keeps AutoScale.
    /// </summary>
    private void RevertChartAxes(ScottPlot.WPF.WpfPlot chart)
    {
        if (ChartHoverHelper.TryGetForChart(chart, out var h)) h.Restore();

        if (ReferenceEquals(chart, QueryHeatmapChart))
        {
            chart.Plot.Axes.AutoScale();
            chart.Refresh();
            return;
        }

        /* Re-pins the axes onto the window the plotted data was read over, so it takes the same offset
           those reads take — the selected tab's. This runs from a chart on the visible tab, where that
           is this tab. */
        var (hoursBack, fromDate, toDate) = GetCurrentWindow(ServerTimeHelper.UtcOffsetMinutes);
        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        chart.Plot.Axes.SetLimitsX(rangeStart.ToOADate(), rangeEnd.ToOADate());
        chart.Plot.Axes.AutoScaleY();
        chart.Refresh();
    }

    /* ========== Collection Health ========== */

    private void UpdateCollectorDurationChart(List<CollectionLogRow> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        ClearChart(CollectorDurationChart);
        ApplyTheme(CollectorDurationChart);

        /* Pin the X axis to the settable window (the same idiom as the CPU / tempdb-size charts) rather than
           AutoScale()'ing to the data — a bare AutoScale fits X to the data plus ScottPlot's ~10% side margins,
           which reads as symmetric dead space. This is the one chart the window-pin campaign missed. */
        DateTime rangeEnd = toDate ?? DateTime.UtcNow.AddMinutes(UtcOffsetMinutes);
        DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

        if (data.Count == 0)
        {
            CollectorDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CollectorDurationChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ReapplyAxisColors(CollectorDurationChart);
            CollectorDurationChart.Refresh();
            return;
        }

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

            var scatter = CollectorDurationChart.Plot.Add.TimeSeries(times, durations);
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
        CollectorDurationChart.Plot.Axes.AutoScaleY();
        CollectorDurationChart.Plot.Axes.SetLimitsX(xMin, xMax);
        ShowChartLegend(CollectorDurationChart);
        CollectorDurationChart.Refresh();
    }
}

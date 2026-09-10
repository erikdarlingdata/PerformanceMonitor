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
/// The CPU Scheduler sub-tab (under the CPU tab, alongside CPU Utilization) — the Darling-viewer surface
/// for the cpu_scheduler_stats collector (scheduler / worker-thread / runnable-task / NUMA / OS-memory
/// pressure), mirroring the Dashboard's CPU-pressure reporting. A point-in-time collector, so the trend
/// chart plots the runnable / blocked / queued task counts directly over the settable window and the
/// latest snapshot renders as a metric/value grid whose warning rows highlight (the collector's own
/// CASE-computed pressure flags). Loaded from <c>LoadCpuAsync</c> alongside CPU Utilization (the CPU
/// tab's full-refresh branch), so the CPU sub-TabControl needs no SelectionChanged handler. Chart chrome
/// flows through the shared <see cref="ChartStyle"/> / <see cref="ChartPalette"/> and the ChartHelpers
/// bridge, so the Y-floor-at-0 fix applies. The metric/value projection + pressure classification are the
/// shared <see cref="CpuSchedulerMetrics"/> (Common); this file keeps only the per-app data read and the
/// chart render.
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _cpuSchedulerHover;

    /// <summary>Applies the shared chrome + hover to the scheduler chart up front (constructor).</summary>
    private void InitializeCpuSchedulerChart()
    {
        ApplyTheme(CpuSchedulerChart);
        CpuSchedulerChart.Refresh();
        _cpuSchedulerHover = new ChartHoverHelper(CpuSchedulerChart, "tasks");
    }

    /// <summary>
    /// Loads the CPU Scheduler sub-tab over the toolbar's settable window: the pressure trend and the
    /// latest-snapshot metric read fire concurrently, then the chart and metric grid render.
    /// </summary>
    private async Task LoadCpuSchedulerAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(2);

        var trendTask = _dataService.GetCpuSchedulerTrendAsync(_server.ServerId, startUtc, endUtc);
        var snapshotTask = _dataService.GetCpuSchedulerSnapshotAsync(_server.ServerId, startUtc, endUtc);
        await Task.WhenAll(trendTask, snapshotTask);

        RenderCpuSchedulerChart(trendTask.Result);
        CpuSchedulerGrid.ItemsSource = CpuSchedulerMetrics.BuildMetrics(snapshotTask.Result);
    }

    private CpuSchedulerChartRenderer? _cpuSchedRendererField;
    private CpuSchedulerChartRenderer CpuSchedRenderer =>
        _cpuSchedRendererField ??= new CpuSchedulerChartRenderer(_chartHelper, ViewerTimeHelper.ForDisplay);

    private void RenderCpuSchedulerChart(List<CpuSchedulerTrendPoint> data)
    {
        var (startUtc, endUtc) = GetWindowUtc();
        CpuSchedRenderer.Render(CpuSchedulerChart, _cpuSchedulerHover, data,
            ViewerTimeHelper.ForDisplay(startUtc).ToOADate(), ViewerTimeHelper.ForDisplay(endUtc).ToOADate());
    }

    /// <summary>Tears down the scheduler hover helper (mirrors the other tabs' dispose).</summary>
    public void DisposeCpuSchedulerHelpers()
    {
        _cpuSchedulerHover?.Dispose();
    }
}

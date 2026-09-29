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
using System.Windows.Controls;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The CPU Scheduler tab — the Lite port of the Darling viewer's CPU Scheduler surface (itself the
/// Dashboard-parity port of the cpu_scheduler_stats reporting). A point-in-time snapshot collector, so
/// the trend chart plots the runnable / blocked / queued task counts directly over the settable window
/// (no delta math) and the latest snapshot renders as a metric/value grid whose warning rows highlight
/// (the collector's own CASE-computed pressure flags). Loads on tab activation via the
/// RefreshVisibleTabAsync switch (mirroring tempdb / Latches &amp; Spinlocks), so no SelectionChanged
/// handler is needed. The chart rides the shared ChartStyle / ChartHoverHelper + the same SeriesColors +
/// Y-floor-at-0 helpers as Lite's other multi-series trends. The metric/value projection + pressure
/// classification are the shared <see cref="CpuSchedulerMetrics"/> (Common); this file keeps only the
/// per-app data read and the chart render.
/// </summary>
public partial class ServerTab : UserControl
{
    private ChartHoverHelper? _cpuSchedulerHover;

    private CpuSchedulerChartRenderer? _cpuSchedRendererField;
    private CpuSchedulerChartRenderer CpuSchedRenderer =>
        _cpuSchedRendererField ??= new CpuSchedulerChartRenderer(_chartHelper, t => ToServerLocal(t));

    /// <summary>Applies the shared chrome + hover to the scheduler chart up front (constructor), so it
    /// doesn't flash white before the tab's first load — matching the CPU/Memory charts.</summary>
    private void InitializeCpuSchedulerChart()
    {
        ApplyTheme(CpuSchedulerChart);
        CpuSchedulerChart.Refresh();
        _cpuSchedulerHover = new ChartHoverHelper(CpuSchedulerChart, "tasks");
    }

    /// <summary>
    /// Loads the CPU Scheduler tab over the toolbar's settable window: the pressure trend and the
    /// latest-snapshot metric read fire concurrently, then the chart and metric grid render. Mirrors the
    /// tempdb tab's full-refresh branch.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshCpuSchedulerAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var trendTask = Helpers.MethodProfiler.TimeAsync("CpuScheduler.Trend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetCpuSchedulerTrendAsync(_serverId, hoursBack, fromDate, toDate))));
            var snapshotTask = Helpers.MethodProfiler.TimeAsync("CpuScheduler.Snapshot", () => Task.Run(() => _dataService.GetCpuSchedulerSnapshotAsync(_serverId, hoursBack, fromDate, toDate)));

            await System.Threading.Tasks.Task.WhenAll(trendTask, snapshotTask);

            UpdateCpuSchedulerChart(trendTask.Result, hoursBack, fromDate, toDate);
            CpuSchedulerGrid.ItemsSource = CpuSchedulerMetrics.BuildMetrics(snapshotTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshCpuSchedulerAsync failed: {ex.Message}");
        }
    }

    /// <summary>The scheduler pressure trend: three fixed series (Runnable / Blocked / Queued task counts)
    /// plotted directly (point-in-time collector), a flat labelled zero-line window when the range has no
    /// data. Mirrors Darling's RenderCpuSchedulerChart.</summary>
    private void UpdateCpuSchedulerChart(List<CpuSchedulerTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            rangeEnd = ToServerLocal(DateTime.UtcNow);
            rangeStart = rangeEnd.AddHours(-hoursBack);
        }

        CpuSchedRenderer.Render(CpuSchedulerChart, _cpuSchedulerHover, data, rangeStart.ToOADate(), rangeEnd.ToOADate());
    }

    /// <summary>Tears down the scheduler hover helper (mirrors the other tabs' dispose) so its tooltip
    /// popup + chart event handlers don't outlive a closed server tab.</summary>
    public void DisposeCpuSchedulerHelpers()
    {
        _cpuSchedulerHover?.Dispose();
    }
}

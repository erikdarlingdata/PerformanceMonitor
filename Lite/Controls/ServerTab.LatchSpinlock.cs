/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows.Controls;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The Latches &amp; Spinlocks tab — the Lite port of the Darling viewer's consolidated Latch/Spinlock
/// tab (itself the Dashboard-parity port of ResourceMetricsContent). ONE tab: the latch wait-time trend
/// and the spinlock collisions trend for the TOP 5 contenders stack vertically, each above a collapsed
/// Expander holding its per-class latest-snapshot grid. The cumulative-delta tables carry no stored
/// sample_interval_seconds, so the ms/sec and collisions/sec rates are computed in SQL from the
/// per-contender LAG interval (the same idiom the Wait Stats trend uses). Loads on tab activation via
/// the RefreshVisibleTabAsync switch (mirroring tempdb/CPU), so no SelectionChanged handler is needed.
/// Charts ride the shared ChartStyle/ChartHoverHelper and the same SeriesColors + Y-floor-at-0 helpers
/// as the Blocking trend charts, so the look matches Lite's other multi-series trends exactly.
/// </summary>
public partial class ServerTab : UserControl
{
    private ChartHoverHelper? _latchStatsHover;
    private ChartHoverHelper? _spinlockStatsHover;

    /* One shared grouped-trend renderer serves BOTH the latch and spinlock charts; built lazily with
       Lite's UTC-offset display projection (mirrors the CpuScheduler renderer's per-app wiring). */
    private GroupedTrendChartRenderer? _latchSpinlockRendererField;
    private GroupedTrendChartRenderer LatchSpinlockRenderer =>
        _latchSpinlockRendererField ??= new GroupedTrendChartRenderer(_chartHelper, t => ToServerLocal(t));

    /// <summary>Applies the shared chrome + hover to the latch/spinlock charts up front (constructor),
    /// so they don't flash white before the tab's first load — matching the CPU/Memory charts.</summary>
    private void InitializeLatchSpinlockCharts()
    {
        ApplyTheme(LatchStatsChart);
        LatchStatsChart.Refresh();
        ApplyTheme(SpinlockStatsChart);
        SpinlockStatsChart.Refresh();

        _latchStatsHover = new ChartHoverHelper(LatchStatsChart, "ms/sec");
        _spinlockStatsHover = new ChartHoverHelper(SpinlockStatsChart, "/sec");
    }

    /// <summary>
    /// Loads the consolidated tab (latch trend + snapshot, spinlock trend + snapshot) over the toolbar's
    /// settable window: the four reads fire concurrently, then the two charts and their two Expander
    /// grids render. Mirrors the tempdb tab's full-refresh branch.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshLatchSpinlockAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var latchTrendTask = Helpers.MethodProfiler.TimeAsync("LatchSpinlock.LatchTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatchStatsTrendAsync(_serverId, hoursBack, fromDate, toDate))));
            var latchSnapshotTask = Helpers.MethodProfiler.TimeAsync("LatchSpinlock.LatchSnapshot", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetLatchStatsSnapshotAsync(_serverId, hoursBack, fromDate, toDate))));
            var spinlockTrendTask = Helpers.MethodProfiler.TimeAsync("LatchSpinlock.SpinlockTrend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetSpinlockStatsTrendAsync(_serverId, hoursBack, fromDate, toDate))));
            var spinlockSnapshotTask = Helpers.MethodProfiler.TimeAsync("LatchSpinlock.SpinlockSnapshot", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetSpinlockStatsSnapshotAsync(_serverId, hoursBack, fromDate, toDate))));

            await System.Threading.Tasks.Task.WhenAll(latchTrendTask, latchSnapshotTask, spinlockTrendTask, spinlockSnapshotTask);

            UpdateLatchStatsChart(latchTrendTask.Result, hoursBack, fromDate, toDate);
            _latchStatsFilterMgr!.UpdateData(latchSnapshotTask.Result);
            UpdateSpinlockStatsChart(spinlockTrendTask.Result, hoursBack, fromDate, toDate);
            _spinlockStatsFilterMgr!.UpdateData(spinlockSnapshotTask.Result);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshLatchSpinlockAsync failed: {ex.Message}");
        }
    }

    /// <summary>The latch wait-time trend: one ms/sec series per top-5 latch class (heaviest first so it
    /// takes the first palette color), a flat labelled zero-line when the window has no data. Mirrors
    /// <see cref="UpdateLockWaitTrendChart"/>.</summary>
    private void UpdateLatchStatsChart(List<LatchStatsTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
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

        LatchSpinlockRenderer.Render(LatchStatsChart, _latchStatsHover, data,
            d => d.CollectionTime, d => d.LatchClass, d => d.WaitTimeMsPerSecond,
            "Latch Waits", "Wait Time (ms/sec)", rangeStart.ToOADate(), rangeEnd.ToOADate());
    }

    /// <summary>The spinlock collisions trend: one collisions/sec series per top-5 spinlock (heaviest
    /// first), a flat labelled zero-line when the window has no data. Collision analog of
    /// <see cref="UpdateLatchStatsChart"/>.</summary>
    private void UpdateSpinlockStatsChart(List<SpinlockStatsTrendPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
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

        LatchSpinlockRenderer.Render(SpinlockStatsChart, _spinlockStatsHover, data,
            d => d.CollectionTime, d => d.SpinlockName, d => d.CollisionsPerSecond,
            "Spinlock Collisions", "Collisions/sec", rangeStart.ToOADate(), rangeEnd.ToOADate());
    }

    /// <summary>Tears down the latch/spinlock hover helpers (mirrors the other tabs' dispose) so their
    /// tooltip popups + chart event handlers don't outlive a closed server tab.</summary>
    public void DisposeLatchSpinlockHelpers()
    {
        _latchStatsHover?.Dispose();
        _spinlockStatsHover?.Dispose();
    }
}

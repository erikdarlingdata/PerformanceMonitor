/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Latches &amp; Spinlocks inner tab — the Dashboard-parity port of ResourceMetricsContent's Latch
/// Stats / Spinlock Stats into the Darling viewer, consolidated into ONE tab: the latch and spinlock
/// per-second trend charts for the TOP 5 contenders (latch classes by delta wait time, spinlocks by delta
/// collisions) stack vertically, each above a collapsed Expander holding its latest-snapshot grid of the
/// most recent collection in the settable window. The Darling cumulative-delta tables carry no stored
/// <c>sample_interval_seconds</c> (unlike the Dashboard's), so the ms/sec and collisions/sec rates are
/// computed in SQL from the per-contender <c>LAG</c> interval — the same idiom the Wait Stats trend uses.
/// Both charts and grids (re)load together on the parent tab's activation (mirroring the Memory tab's
/// full-refresh branch), so the tab needs no SelectionChanged handler. Chart chrome / legend / line polish
/// flow through the shared <see cref="ChartStyle"/> / <see cref="ChartPalette"/> and the
/// <c>ViewerServerTab.ChartHelpers.cs</c> bridge, so the Y-floor-at-0 fix applies; series ride the cycling
/// <c>SeriesColors</c> (declared in ViewerServerTab.Charts.cs).
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _latchStatsHover;
    private ChartHoverHelper? _spinlockStatsHover;

    /* One shared grouped-trend renderer serves BOTH the latch and spinlock charts; built lazily with the
       viewer's ForDisplay projection (mirrors the CpuScheduler renderer's per-app wiring). */
    private GroupedTrendChartRenderer? _latchSpinlockRendererField;
    private GroupedTrendChartRenderer LatchSpinlockRenderer =>
        _latchSpinlockRendererField ??= new GroupedTrendChartRenderer(_chartHelper, ViewerTimeHelper.ForDisplay);

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
    /// settable window: the four reads (two trends, two snapshots) fire concurrently — NpgsqlDataSource
    /// pools a connection each — then the two charts and their two Expander grids render.
    /// </summary>
    private async Task LoadLatchSpinlockAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(4);

        var latchTrendTask = _dataService.GetLatchStatsTrendAsync(_server.ServerId, startUtc, endUtc);
        var latchSnapshotTask = _dataService.GetLatchStatsSnapshotAsync(_server.ServerId, startUtc, endUtc);
        var spinlockTrendTask = _dataService.GetSpinlockStatsTrendAsync(_server.ServerId, startUtc, endUtc);
        var spinlockSnapshotTask = _dataService.GetSpinlockStatsSnapshotAsync(_server.ServerId, startUtc, endUtc);

        await Task.WhenAll(latchTrendTask, latchSnapshotTask, spinlockTrendTask, spinlockSnapshotTask);

        RenderLatchStatsChart(latchTrendTask.Result);
        LatchStatsGrid.ItemsSource = latchSnapshotTask.Result;
        RenderSpinlockStatsChart(spinlockTrendTask.Result);
        SpinlockStatsGrid.ItemsSource = spinlockSnapshotTask.Result;
    }

    private void RenderLatchStatsChart(List<LatchStatsTrendPoint> data)
    {
        var (startUtc, endUtc) = GetWindowUtc();
        LatchSpinlockRenderer.Render(LatchStatsChart, _latchStatsHover, data,
            d => d.CollectionTime, d => d.LatchClass, d => d.WaitTimeMsPerSecond,
            "Latch Waits", "Wait Time (ms/sec)",
            ViewerTimeHelper.ForDisplay(startUtc).ToOADate(), ViewerTimeHelper.ForDisplay(endUtc).ToOADate());
    }

    private void RenderSpinlockStatsChart(List<SpinlockStatsTrendPoint> data)
    {
        var (startUtc, endUtc) = GetWindowUtc();
        LatchSpinlockRenderer.Render(SpinlockStatsChart, _spinlockStatsHover, data,
            d => d.CollectionTime, d => d.SpinlockName, d => d.CollisionsPerSecond,
            "Spinlock Collisions", "Collisions/sec",
            ViewerTimeHelper.ForDisplay(startUtc).ToOADate(), ViewerTimeHelper.ForDisplay(endUtc).ToOADate());
    }

    /// <summary>Tears down the latch/spinlock hover helpers (mirrors the other tabs' dispose) so their
    /// tooltip popups + chart event handlers don't outlive a closed server tab.</summary>
    public void DisposeLatchSpinlockHelpers()
    {
        _latchStatsHover?.Dispose();
        _spinlockStatsHover?.Dispose();
    }
}

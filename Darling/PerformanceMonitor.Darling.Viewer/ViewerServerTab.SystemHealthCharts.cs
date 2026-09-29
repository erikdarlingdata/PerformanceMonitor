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
/// The System Events tab's two SYSTEM-component counter-chart sub-tabs — Corruption Events and Contention
/// Events. Both sub-tabs read the ONE SYSTEM-component feed (<see cref="ViewerDataService.GetSystemHealthAsync"/>)
/// and select their own columns, so a single load renders all eight charts — exactly as the Dashboard's
/// <c>RefreshSystemHealthAsync</c> feeds both tabs. Unlike the grid categories these plot EVERY collected
/// snapshot over time (no significance filter); counts floor at 0 and the time axis is bound to the toolbar's
/// settable window (<see cref="ViewerServerTab.GetWindowUtc"/>). The chart BODIES are the shared
/// <see cref="SystemHealthChartRenderer"/> (byte-identical to Lite's); this partial keeps the per-app data
/// read, the hover wiring, and the display-time projection it hands the renderer.
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _badPagesHover;
    private ChartHoverHelper? _dumpRequestsHover;
    private ChartHoverHelper? _accessViolationsHover;
    private ChartHoverHelper? _writeAccessViolationsHover;
    private ChartHoverHelper? _nonYieldingTasksHover;
    private ChartHoverHelper? _latchWarningsHover;
    private ChartHoverHelper? _sickSpinlocksHover;
    private ChartHoverHelper? _cpuComparisonHover;

    private SystemHealthChartRenderer? _sysHealthRendererField;
    /// <summary>The shared System Events counter-chart renderer, bound to the viewer's display zone (it plots the UTC instant).</summary>
    private SystemHealthChartRenderer SysHealthRenderer =>
        _sysHealthRendererField ??= new SystemHealthChartRenderer(_chartHelper, static utc => utc, ViewerTimeHelper.CurrentDisplayZone);

    /// <summary>Applies the shared chrome + hover to the eight Corruption/Contention charts up front
    /// (constructor), so they don't flash white before the tab's first load — matching the CPU/Latch charts.
    /// Hover units mirror the Dashboard's (events / backoffs / %).</summary>
    private void InitializeSystemHealthCharts()
    {
        foreach (var chart in new[]
        {
            BadPagesChart, DumpRequestsChart, AccessViolationsChart, WriteAccessViolationsChart,
            NonYieldingTasksChart, LatchWarningsChart, SickSpinlocksChart, CpuComparisonChart,
        })
        {
            ApplyTheme(chart);
            chart.Refresh();
        }

        _badPagesHover = new ChartHoverHelper(BadPagesChart, "events", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _dumpRequestsHover = new ChartHoverHelper(DumpRequestsChart, "events", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _accessViolationsHover = new ChartHoverHelper(AccessViolationsChart, "events", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _writeAccessViolationsHover = new ChartHoverHelper(WriteAccessViolationsChart, "events", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _nonYieldingTasksHover = new ChartHoverHelper(NonYieldingTasksChart, "events", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _latchWarningsHover = new ChartHoverHelper(LatchWarningsChart, "events", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _sickSpinlocksHover = new ChartHoverHelper(SickSpinlocksChart, "backoffs", displayZone: ViewerTimeHelper.CurrentDisplayZone);
        _cpuComparisonHover = new ChartHoverHelper(CpuComparisonChart, "%", displayZone: ViewerTimeHelper.CurrentDisplayZone);
    }

    /// <summary>
    /// Loads both Corruption + Contention chart sub-tabs from a single SYSTEM-component read over the
    /// toolbar's settable window (the reads are already bounded on both ends, so no client-side end-trim
    /// is needed unlike the start-only CPU/tempdb reads). Mirrors the Dashboard's one-refresh-feeds-both.
    /// </summary>
    private async Task LoadSystemHealthChartsAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var data = await _dataService.GetSystemHealthAsync(_server.ServerId, startUtc, endUtc);

        double xMin = startUtc.ToOADate();
        double xMax = endUtc.ToOADate();

        // Corruption Events (2x2).
        SysHealthRenderer.RenderCounterChart(BadPagesChart, _badPagesHover, data, d => d.BadPagesDetected, "Bad Pages",
            ChartPalette.CyclingColor(3), xMin, xMax);
        SysHealthRenderer.RenderCounterChart(DumpRequestsChart, _dumpRequestsHover, data, d => d.IntervalDumpRequests, "Dump Requests",
            ChartPalette.CyclingColor(2), xMin, xMax);
        SysHealthRenderer.RenderCounterChart(AccessViolationsChart, _accessViolationsHover, data, d => d.IsAccessViolationOccurred, "Access Violations",
            ChartPalette.CyclingColor(4), xMin, xMax);
        SysHealthRenderer.RenderCounterChart(WriteAccessViolationsChart, _writeAccessViolationsHover, data, d => d.WriteAccessViolationCount, "Write Access Violations",
            ChartPalette.CyclingColor(0), xMin, xMax);

        // Contention Events (2x2).
        SysHealthRenderer.RenderCounterChart(NonYieldingTasksChart, _nonYieldingTasksHover, data, d => d.NonYieldingTasksReported, "Non-Yielding Tasks",
            ChartPalette.CyclingColor(3), xMin, xMax);
        SysHealthRenderer.RenderCounterChart(LatchWarningsChart, _latchWarningsHover, data, d => d.LatchWarnings, "Latch Warnings",
            ChartPalette.CyclingColor(2), xMin, xMax);
        SysHealthRenderer.RenderSickSpinlocksChart(SickSpinlocksChart, _sickSpinlocksHover, data, xMin, xMax);
        SysHealthRenderer.RenderCpuComparisonChart(CpuComparisonChart, _cpuComparisonHover, data, xMin, xMax);
    }

    /// <summary>Tears down the eight Corruption/Contention hover helpers (mirrors the other tabs' dispose)
    /// so their tooltip popups + chart event handlers don't outlive a closed server tab.</summary>
    public void DisposeSystemHealthChartHelpers()
    {
        _badPagesHover?.Dispose();
        _dumpRequestsHover?.Dispose();
        _accessViolationsHover?.Dispose();
        _writeAccessViolationsHover?.Dispose();
        _nonYieldingTasksHover?.Dispose();
        _latchWarningsHover?.Dispose();
        _sickSpinlocksHover?.Dispose();
        _cpuComparisonHover?.Dispose();
    }
}

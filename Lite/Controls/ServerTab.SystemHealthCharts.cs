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
/// The System Events tab's two SYSTEM-component counter-chart sub-tabs — Corruption Events and Contention
/// Events — the Lite port of the Darling viewer's SystemHealthCharts (itself the Dashboard's Corruption /
/// Contention 2x2 chart grids). Both sub-tabs read the ONE SYSTEM-component feed
/// (<see cref="LocalDataService.GetSystemHealthAsync"/>) and select their own columns, so a single load
/// renders all eight charts. Unlike the grid categories these plot EVERY collected snapshot over time (no
/// significance filter); counts floor at 0 and the time axis is bound to the toolbar's settable window. The
/// chart BODIES are the shared <see cref="SystemHealthChartRenderer"/> (byte-identical to Darling's); this
/// partial keeps the per-app data read, the hover wiring, and the display-time projection it hands the renderer.
/// </summary>
public partial class ServerTab : UserControl
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
    /// <summary>The shared System Events counter-chart renderer, bound to the tab's display zone (<see cref="GetPickerZone"/>).</summary>
    private SystemHealthChartRenderer SysHealthRenderer =>
        _sysHealthRendererField ??= new SystemHealthChartRenderer(_chartHelper, t => t, GetPickerZone);

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

        _badPagesHover = new ChartHoverHelper(BadPagesChart, "events", displayZone: GetPickerZone);
        _dumpRequestsHover = new ChartHoverHelper(DumpRequestsChart, "events", displayZone: GetPickerZone);
        _accessViolationsHover = new ChartHoverHelper(AccessViolationsChart, "events", displayZone: GetPickerZone);
        _writeAccessViolationsHover = new ChartHoverHelper(WriteAccessViolationsChart, "events", displayZone: GetPickerZone);
        _nonYieldingTasksHover = new ChartHoverHelper(NonYieldingTasksChart, "events", displayZone: GetPickerZone);
        _latchWarningsHover = new ChartHoverHelper(LatchWarningsChart, "events", displayZone: GetPickerZone);
        _sickSpinlocksHover = new ChartHoverHelper(SickSpinlocksChart, "backoffs", displayZone: GetPickerZone);
        _cpuComparisonHover = new ChartHoverHelper(CpuComparisonChart, "%", displayZone: GetPickerZone);
    }

    /// <summary>
    /// Loads both Corruption + Contention chart sub-tabs from a single SYSTEM-component read over the
    /// toolbar's settable window. Mirrors the Dashboard's one-refresh-feeds-both.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshSystemHealthChartsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => SafeQueryAsync(() => _dataService.GetSystemHealthAsync(_serverId, hoursBack, fromDate, toDate)));

        DateTime rangeStart, rangeEnd;
        if (fromDate.HasValue && toDate.HasValue)
        {
            rangeStart = fromDate.Value;
            rangeEnd = toDate.Value;
        }
        else
        {
            (rangeStart, rangeEnd) = GetChartWindow(hoursBack, null, null);
        }

        double xMin = rangeStart.ToOADate();
        double xMax = rangeEnd.ToOADate();

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

        ShowSystemHealthChartsNote();
    }

    /// <summary>
    /// The two chart sub-tabs on an Azure SQL Database, where the system_health_events collector does not run: the charts
    /// are hidden and its note shows in their place, since nothing can ever be plotted there. Anywhere else the charts
    /// show and the note stays hidden.
    /// </summary>
    private void ShowSystemHealthChartsNote()
    {
        var gap = SystemHealthGapNote(_server.DisplayName, _isAzureSqlDatabase);
        foreach (var (note, charts) in new (TextBlock, Grid)[]
                 {
                     (CorruptionEventsNoDataMessage, CorruptionEventsCharts),
                     (ContentionEventsNoDataMessage, ContentionEventsCharts),
                 })
        {
            note.Text = gap ?? "";
            note.Visibility = gap is null ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
            charts.Visibility = gap is null ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
        }
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

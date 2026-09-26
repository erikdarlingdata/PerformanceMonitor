/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows.Controls;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The slicer overlay-on-select (#683) — a port of Lite's <c>ServerTab.Grids.cs</c> grid→slicer overlay:
/// selecting a Top Queries / Top Procedures / Query Store row overlays THAT item's execution timeline as a
/// curve on the sub-tab's slicer (<see cref="TimeRangeSlicerControl.SetOverlay"/> / <c>ClearOverlay</c>, which
/// were ported earlier but left dead). The per-item timeline comes from
/// <see cref="ViewerDataService"/> (<c>ItemTimeline</c> partial); the metric shown tracks the slicer's current
/// sort metric via <see cref="ComputeOverlayPoints"/>. One deviation from Lite: Darling's <c>delta_*</c> reads
/// are already per-interval, so no C# row-over-row differencing is needed (Lite diffs cumulative history rows).
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// Wires the three query grids' SelectionChanged to their slicer overlay. Called from the constructor
    /// after InitializeComponent, so the named grids exist. Subscribed for the life of the tab (the grids
    /// are never re-created); no unsubscribe needed.
    /// </summary>
    private void WireSlicerOverlays()
    {
        QueryStatsGrid.SelectionChanged += QueryStatsGrid_SelectionChanged;
        ProcedureStatsGrid.SelectionChanged += ProcedureStatsGrid_SelectionChanged;
        QueryStoreGrid.SelectionChanged += QueryStoreGrid_SelectionChanged;
    }

    private async void QueryStatsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (QueryStatsGrid.SelectedItem is not ViewerQueryStatsRow row || string.IsNullOrEmpty(row.QueryHash))
        {
            /* A refresh momentarily clears the selection (UpdateData rebinds the grid); don't wipe the
               overlay then — only when the user actually deselects. Mirrors Lite's _isRefreshing guard. */
            if (!_refreshInFlight) QueryStatsSlicer.ClearOverlay();
            return;
        }

        try
        {
            var (startUtc, endUtc) = GetWindowUtc();
            var timeline = await _dataService.GetQueryStatsItemTimelineAsync(
                _server.ServerId, row.DatabaseName, row.QueryHash, startUtc, endUtc);
            QueryStatsSlicer.SetOverlay(ComputeOverlayPoints(timeline, _queryStatsSlicerMetric), row.QueryHash);
        }
        catch
        {
            QueryStatsSlicer.ClearOverlay();
        }
    }

    private async void ProcedureStatsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (ProcedureStatsGrid.SelectedItem is not ViewerProcedureStatsRow row || string.IsNullOrEmpty(row.ObjectName))
        {
            if (!_refreshInFlight) ProcStatsSlicer.ClearOverlay();
            return;
        }

        try
        {
            var (startUtc, endUtc) = GetWindowUtc();
            var timeline = await _dataService.GetProcStatsItemTimelineAsync(
                _server.ServerId, row.DatabaseName, row.SchemaName, row.ObjectName, startUtc, endUtc);
            var label = row.ObjectName.Length > 30 ? row.ObjectName[..30] + "..." : row.ObjectName;
            ProcStatsSlicer.SetOverlay(ComputeOverlayPoints(timeline, _procStatsSlicerMetric), label);
        }
        catch
        {
            ProcStatsSlicer.ClearOverlay();
        }
    }

    private async void QueryStoreGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (QueryStoreGrid.SelectedItem is not ViewerQueryStoreRow row)
        {
            if (!_refreshInFlight) QueryStoreSlicer.ClearOverlay();
            return;
        }

        try
        {
            var (startUtc, endUtc) = GetWindowUtc();
            var timeline = await _dataService.GetQueryStoreItemTimelineAsync(
                _server.ServerId, row.DatabaseName, row.QueryId, row.PlanId, startUtc, endUtc,
                literalEndUtc: IsCustomRange ? endUtc : null);
            var label = !string.IsNullOrWhiteSpace(row.ModuleName)
                ? row.ModuleName
                : $"Query {row.QueryId} / Plan {row.PlanId}";
            QueryStoreSlicer.SetOverlay(ComputeOverlayPoints(timeline, _queryStoreSlicerMetric), label);
        }
        catch
        {
            QueryStoreSlicer.ClearOverlay();
        }
    }

    /// <summary>
    /// Projects an item's timeline to the (naive-UTC time, value) points the slicer overlays, picking the
    /// magnitude field that matches the slicer's current sort metric (Lite's ComputeQueryOverlayPoints
    /// selector). Zero-value cycles are dropped so an idle stretch doesn't draw a flat baseline. Pure +
    /// static for unit testing.
    /// </summary>
    internal static List<(DateTime TimeUtc, double Value)> ComputeOverlayPoints(
        IReadOnlyList<ViewerDataService.ItemTimelinePoint> timeline, string slicerMetric)
    {
        Func<ViewerDataService.ItemTimelinePoint, double> selector = slicerMetric switch
        {
            "TotalCpu" or "AvgCpu" => p => p.CpuMs,
            "TotalReads" or "AvgReads" => p => p.Reads,
            "TotalWrites" => p => p.Writes,
            "TotalPhysReads" => p => p.PhysicalReads,
            _ => p => p.ElapsedMs, /* TotalElapsed / AvgElapsed / Sessions / default */
        };

        var points = new List<(DateTime TimeUtc, double Value)>();
        foreach (var p in timeline)
        {
            var value = selector(p);
            if (value > 0)
                points.Add((p.PointTime, value));
        }
        return points;
    }
}

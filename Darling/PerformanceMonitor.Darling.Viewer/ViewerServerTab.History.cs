/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The per-row double-click execution-history drill-downs — a port of Lite's <c>ServerTab.Grids.cs</c>
/// <c>*Grid_MouseDoubleClick</c> handlers: double-clicking a Top Queries / Top Procedures / Query Store row
/// opens that item's history window (<see cref="QueryStatsHistoryWindow"/> / <see cref="ProcedureHistoryWindow"/>
/// / <see cref="QueryStoreHistoryWindow"/>), showing its metric trend + per-collection snapshot grid over the
/// toolbar's current window (<see cref="GetWindowUtc"/>). This is a DIFFERENT event from #1409's grid
/// SelectionChanged slicer overlay (ViewerServerTab.SlicerOverlay.cs) — both are wired on the same three grids
/// and coexist (a double-click also selects the row, so the overlay updates too). The row → window-parameter
/// mapping (which fields identify the double-clicked item, and when a row can't open a window) is factored into
/// pure static <c>Map*HistoryKey</c> methods so it is unit-testable without WPF.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>The fields that identify a double-clicked Top-Queries row's history window (Lite gates on a
    /// non-empty database + query_hash).</summary>
    public sealed record QueryStatsHistoryKey(string DatabaseName, string QueryHash, string QueryText);

    /// <summary>The fields that identify a double-clicked Top-Procedures row's history window (Lite gates on a
    /// non-empty database + object name).</summary>
    public sealed record ProcedureHistoryKey(string DatabaseName, string SchemaName, string ObjectName);

    /// <summary>The fields that identify a double-clicked Query Store row's history window (Lite gates on a
    /// non-empty database + a non-zero query_id; the window is query-scoped, plan_id seeds the launched row).</summary>
    public sealed record QueryStoreHistoryKey(string DatabaseName, long QueryId, long PlanId, string QueryText);

    /// <summary>
    /// Wires the three query grids' MouseDoubleClick to their history windows. Called from the constructor
    /// after InitializeComponent, so the named grids exist. Subscribed for the life of the tab (the grids are
    /// never re-created); no unsubscribe needed — the same lifetime rule <see cref="WireSlicerOverlays"/> uses.
    /// </summary>
    private void WireHistoryDrillDowns()
    {
        QueryStatsGrid.MouseDoubleClick += QueryStatsGrid_MouseDoubleClick;
        ProcedureStatsGrid.MouseDoubleClick += ProcedureStatsGrid_MouseDoubleClick;
        QueryStoreGrid.MouseDoubleClick += QueryStoreGrid_MouseDoubleClick;
        QueryStoreRegressionsGrid.MouseDoubleClick += QueryStoreRegressionsGrid_MouseDoubleClick;
    }

    /// <summary>Maps a Top-Queries grid row to its history-window key, or null when the row can't open one
    /// (missing database or query_hash) — Lite's <c>QueryStatsGrid_MouseDoubleClick</c> guard. Pure + static.</summary>
    internal static QueryStatsHistoryKey? MapQueryStatsHistoryKey(ViewerQueryStatsRow? row)
        => row is null || string.IsNullOrEmpty(row.DatabaseName) || string.IsNullOrEmpty(row.QueryHash)
            ? null
            : new QueryStatsHistoryKey(row.DatabaseName, row.QueryHash, row.QueryText);

    /// <summary>Maps a Top-Procedures grid row to its history-window key, or null when the row can't open one
    /// (missing database or object name) — Lite's <c>ProcedureStatsGrid_MouseDoubleClick</c> guard. Pure + static.</summary>
    internal static ProcedureHistoryKey? MapProcedureHistoryKey(ViewerProcedureStatsRow? row)
        => row is null || string.IsNullOrEmpty(row.DatabaseName) || string.IsNullOrEmpty(row.ObjectName)
            ? null
            : new ProcedureHistoryKey(row.DatabaseName, row.SchemaName, row.ObjectName);

    /// <summary>Maps a Query Store grid row to its history-window key, or null when the row can't open one
    /// (missing database or a zero query_id) — Lite's <c>QueryStoreGrid_MouseDoubleClick</c> guard. Pure + static.</summary>
    internal static QueryStoreHistoryKey? MapQueryStoreHistoryKey(ViewerQueryStoreRow? row)
        => row is null || string.IsNullOrEmpty(row.DatabaseName) || row.QueryId == 0
            ? null
            : new QueryStoreHistoryKey(row.DatabaseName, row.QueryId, row.PlanId, row.QueryText);

    /// <summary>Maps a Query Store Regressions grid row to a Query Store history-window key (the window is
    /// query-scoped), or null when the row can't open one (missing database or a zero query_id) — mirrors the
    /// Dashboard's <c>QueryStoreRegressionsDataGrid_MouseDoubleClick</c> guard. The regression row aggregates
    /// over plans (it carries baseline/recent plan COUNTS, not a single plan_id), so plan_id is 0 — the same
    /// "no specific plan to seed" value the Dashboard passes. Pure + static.</summary>
    internal static QueryStoreHistoryKey? MapQueryStoreRegressionHistoryKey(ViewerQueryStoreRegressionRow? row)
        => row is null || string.IsNullOrEmpty(row.DatabaseName) || row.QueryId == 0
            ? null
            : new QueryStoreHistoryKey(row.DatabaseName, row.QueryId, 0, row.QueryTextSample);

    private void QueryStatsGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (MapQueryStatsHistoryKey(QueryStatsGrid.SelectedItem as ViewerQueryStatsRow) is not { } key) return;

        var (startUtc, endUtc) = GetWindowUtc();
        var window = new QueryStatsHistoryWindow(
            _dataService, _server.ServerId, _server.DisplayName, key.DatabaseName, key.QueryHash, key.QueryText, startUtc, endUtc)
        {
            Owner = Window.GetWindow(this),
        };
        window.ShowDialog();
    }

    private void ProcedureStatsGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (MapProcedureHistoryKey(ProcedureStatsGrid.SelectedItem as ViewerProcedureStatsRow) is not { } key) return;

        var (startUtc, endUtc) = GetWindowUtc();
        var window = new ProcedureHistoryWindow(
            _dataService, _server.ServerId, key.DatabaseName, key.SchemaName, key.ObjectName, startUtc, endUtc)
        {
            Owner = Window.GetWindow(this),
        };
        window.ShowDialog();
    }

    private void QueryStoreGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (MapQueryStoreHistoryKey(QueryStoreGrid.SelectedItem as ViewerQueryStoreRow) is not { } key) return;

        var (startUtc, endUtc) = GetWindowUtc();
        var window = new QueryStoreHistoryWindow(
            _dataService, _server.ServerId, _server.DisplayName, key.DatabaseName, key.QueryId, key.PlanId, key.QueryText, startUtc, endUtc)
        {
            Owner = Window.GetWindow(this),
        };
        window.ShowDialog();
    }

    private void QueryStoreRegressionsGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (MapQueryStoreRegressionHistoryKey(QueryStoreRegressionsGrid.SelectedItem as ViewerQueryStoreRegressionRow) is not { } key) return;

        var (startUtc, endUtc) = GetWindowUtc();
        var window = new QueryStoreHistoryWindow(
            _dataService, _server.ServerId, _server.DisplayName, key.DatabaseName, key.QueryId, key.PlanId, key.QueryText, startUtc, endUtc)
        {
            Owner = Window.GetWindow(this),
        };
        window.ShowDialog();
    }
}

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
/// The Session Stats inner tab — the Dashboard-parity port of ResourceMetricsContent's Session Stats
/// sub-tab (Dashboard/Controls/ResourceMetricsContent.SessionStats.cs) into the Darling viewer. A single
/// trend chart plots the server-wide session-status counts over the settable window — Total / Running /
/// Sleeping / Background / Dormant / Idle &gt;30m / Waiting for Memory — and a summary strip below it shows
/// the non-chartable attribution the latest snapshot carries: the top application and top host by
/// connection count, and the number of distinct databases with connections.
/// <para>
/// The feed is <see cref="ViewerDataService.GetSessionStatsAsync"/> over <c>v_session_summary_stats</c>
/// (the server-wide summary collector, 1:1 with the Dashboard's <c>session_stats</c> table) — NOT the
/// per-application <c>v_session_stats</c> FinOps' Application Connections read uses. The chart BODY is the
/// shared <see cref="SessionStatsChartRenderer"/> (byte-identical to Lite's, each status series on its fixed
/// <see cref="ChartPalette.SeriesColor"/> "Session*" key, all-zero series skipped, Y-floored at 0 with a
/// bottom legend); this partial keeps the per-app data read, the hover wiring, the identity projection and display zone
/// (<c>ViewerTimeHelper.CurrentDisplayZone</c>) it hands the renderer, and the summary strip (shaped by the shared
/// <see cref="SessionStatsSummary"/>). The chart's Copy/Save/CSV right-click menu is wired in
/// <c>ViewerServerTab.ChartContextMenu.cs</c>.
/// </para>
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _sessionStatsHover;

    private SessionStatsChartRenderer? _sessionStatsRendererField;
    /// <summary>The shared Session Stats trend-chart renderer, bound to the viewer's display zone (it plots the UTC instant).</summary>
    private SessionStatsChartRenderer SessionStatsRenderer =>
        _sessionStatsRendererField ??= new SessionStatsChartRenderer(_chartHelper, static utc => utc, ViewerTimeHelper.CurrentDisplayZone);

    /// <summary>Applies the shared chrome + hover to the Session Stats chart up front (constructor), so it
    /// doesn't flash white before the tab's first load — matching the CPU/Memory/latch charts.</summary>
    private void InitializeSessionStatsChart()
    {
        ApplyTheme(SessionStatsChart);
        SessionStatsChart.Refresh();
        _sessionStatsHover = new ChartHoverHelper(SessionStatsChart, "sessions", displayZone: ViewerTimeHelper.CurrentDisplayZone);
    }

    /// <summary>Loads the Session Stats tab: the session-summary snapshots over the toolbar's settable
    /// window, then renders the status-count chart and the top-app / top-host / databases summary.</summary>
    private async Task LoadSessionStatsAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        var data = await _dataService.GetSessionStatsAsync(_server.ServerId, startUtc, endUtc);
        RenderSessionStatsChart(data);
    }

    /// <summary>The session-status trend: hands the settable-window bounds + snapshots to the shared
    /// <see cref="SessionStatsChartRenderer"/>, then updates the summary strip from the latest snapshot.</summary>
    private void RenderSessionStatsChart(List<SessionStatsPoint> data)
    {
        var (startUtc, endUtc) = GetWindowUtc();
        double xMin = startUtc.ToOADate();
        double xMax = endUtc.ToOADate();

        SessionStatsRenderer.Render(SessionStatsChart, _sessionStatsHover, data, xMin, xMax);

        /* Summary panel reflects the latest snapshot in the window (Dashboard uses the last data point). */
        var ordered = data.OrderBy(d => d.CollectionTime).ToList();
        UpdateSessionStatsSummary(ordered.Count > 0 ? ordered[^1] : null);
    }

    /// <summary>Writes the latest snapshot's attribution into the summary strip (Dashboard parity: the
    /// non-chartable Top Application / Top Host / Databases values), delegating the formatting to the shared
    /// <see cref="SessionStatsSummary.Format"/> so the "name (count)" / N/A shaping is unit-tested.</summary>
    private void UpdateSessionStatsSummary(SessionStatsPoint? data)
    {
        var (topApp, topHost, databases) = SessionStatsSummary.Format(data);
        SessionStatsTopAppText.Text = topApp;
        SessionStatsTopHostText.Text = topHost;
        SessionStatsDatabasesText.Text = databases;
        /* #4966: the newest collection in the last bucket (where Top Application and Top Host come from), not the bucket's grid time. */
        SessionStatsCollectedText.Text = HistoryTime.SnapshotCollected(data?.LatestCollectionTime, "N/A");
    }

    /// <summary>Tears down the Session Stats hover helper (mirrors the other tabs' dispose) so its tooltip
    /// popup + chart event handlers don't outlive a closed server tab.</summary>
    public void DisposeSessionStatsHelpers()
    {
        _sessionStatsHover?.Dispose();
    }
}

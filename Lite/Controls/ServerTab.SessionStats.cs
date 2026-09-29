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
/// The Session Stats tab — the Lite port of the Darling viewer's Session Stats surface (itself the
/// Dashboard-parity port of ResourceMetricsContent's Session Stats sub-tab). A single trend chart plots
/// the server-wide session-status counts over the settable window — Total / Running / Sleeping /
/// Background / Dormant / Idle &gt;30m / Waiting for Memory — and a summary strip below shows the
/// non-chartable attribution the latest snapshot carries: the top application and top host by connection
/// count, and the number of distinct databases with connections. The feed is the server-wide
/// v_session_summary_stats (1:1 with the Dashboard's session_stats table) — NOT the per-application
/// v_session_stats FinOps' Application Connections read uses. The chart BODY is the shared
/// <see cref="SessionStatsChartRenderer"/> (byte-identical to Darling's, each series on its own fixed
/// <see cref="ChartPalette"/> "Session*" key, all-zero series skipped); this partial keeps the per-app data
/// read, the hover wiring, the display-time projection it hands the renderer, and the summary strip (shaped
/// by the shared <see cref="SessionStatsSummary"/>). Loads on tab activation via the RefreshVisibleTabAsync switch.
/// </summary>
public partial class ServerTab : UserControl
{
    private ChartHoverHelper? _sessionStatsHover;

    private SessionStatsChartRenderer? _sessionStatsRendererField;
    /// <summary>The shared Session Stats trend-chart renderer, bound to Lite's settable display-time offset.</summary>
    private SessionStatsChartRenderer SessionStatsRenderer =>
        _sessionStatsRendererField ??= new SessionStatsChartRenderer(_chartHelper, t => t, GetPickerZone);

    /// <summary>Applies the shared chrome + hover to the Session Stats chart up front (constructor), so it
    /// doesn't flash white before the tab's first load — matching the CPU/Memory/latch charts.</summary>
    private void InitializeSessionStatsChart()
    {
        ApplyTheme(SessionStatsChart);
        SessionStatsChart.Refresh();
        _sessionStatsHover = new ChartHoverHelper(SessionStatsChart, "sessions", displayZone: GetPickerZone);
    }

    /// <summary>Loads the Session Stats tab over the toolbar's settable window: the session-summary
    /// snapshots, then renders the status-count chart and the top-app / top-host / databases summary.</summary>
    private async System.Threading.Tasks.Task RefreshSessionStatsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            var data = await Helpers.MethodProfiler.TimeAsync("SessionStats.Trend", () => Task.Run(() => SafeQueryAsync(() => _dataService.GetSessionStatsAsync(_serverId, hoursBack, fromDate, toDate))));
            UpdateSessionStatsChart(data, hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshSessionStatsAsync failed: {ex.Message}");
        }
    }

    /// <summary>The session-status trend: hands the settable-window bounds + snapshots to the shared
    /// <see cref="SessionStatsChartRenderer"/>, then updates the summary strip from the latest snapshot.</summary>
    private void UpdateSessionStatsChart(List<SessionStatsPoint> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
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

        SessionStatsRenderer.Render(SessionStatsChart, _sessionStatsHover, data, rangeStart.ToOADate(), rangeEnd.ToOADate());

        /* Summary panel reflects the latest snapshot in the window (Dashboard uses the last point). */
        var ordered = data.OrderBy(d => d.CollectionTime).ToList();
        UpdateSessionStatsSummary(ordered.Count > 0 ? ordered[^1] : null);
    }

    /// <summary>Writes the latest snapshot's attribution into the summary strip (Dashboard parity: the
    /// non-chartable Top Application / Top Host / Databases values), delegating the shaping to the shared
    /// <see cref="SessionStatsSummary.Format"/>.</summary>
    private void UpdateSessionStatsSummary(SessionStatsPoint? data)
    {
        var (topApp, topHost, databases) = SessionStatsSummary.Format(data);
        SessionStatsTopAppText.Text = topApp;
        SessionStatsTopHostText.Text = topHost;
        SessionStatsDatabasesText.Text = databases;
    }

    /// <summary>Tears down the Session Stats hover helper (mirrors the other tabs' dispose) so its tooltip
    /// popup + chart event handlers don't outlive a closed server tab.</summary>
    public void DisposeSessionStatsHelpers()
    {
        _sessionStatsHover?.Dispose();
    }
}

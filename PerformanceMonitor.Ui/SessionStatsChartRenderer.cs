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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Shared renderer for the Session Stats tab's server-wide session-status trend chart. The ScottPlot chart BODY
/// was byte-identical between Lite's <c>ServerTab.SessionStats.cs</c> and Darling's
/// <c>ViewerServerTab.SessionStats.cs</c> apart from the display-time projection, so it lives here once: it plots
/// the seven fixed status series (Total / Running / Sleeping / Background / Dormant / Idle &gt;30m / Waiting for
/// Memory), SKIPPING any series that is all-zero across the window, each on its own shared
/// <see cref="ChartPalette"/> <c>Session*</c> key, Y-floored at 0 with a bottom legend. Each app constructs one
/// with its own projection (Lite adds its settable UTC offset; Darling uses <c>ViewerTimeHelper.ForDisplay</c>)
/// and passes the chart + hover + pre-computed display-time x-bounds; the store-specific data read, the chart
/// init/dispose, and the summary strip (the non-chartable Top App / Top Host / Databases values, shaped by
/// <see cref="SessionStatsSummary"/>) stay per-app. INTERNAL because it takes the internal
/// <see cref="ChartHoverHelper"/> (both apps see it via Ui's InternalsVisibleTo). (The deprecated Full Dashboard
/// keeps its own copy.)
/// </summary>
internal sealed class SessionStatsChartRenderer
{
    /// <summary>One chart series: its legend label, its shared-palette color key, and the accessor that pulls its
    /// status count out of an <see cref="ISessionStatsPoint"/>. Pure data so the mapping is unit-testable without
    /// a WpfPlot; the render loop and the parity pins share this single source of truth.</summary>
    internal readonly record struct SessionSeriesSpec(string Legend, string PaletteKey, Func<ISessionStatsPoint, double> Value);

    /// <summary>The seven session-status series in the Dashboard's order (Total first, then the status
    /// breakdown), each pinned to the shared <c>Session*</c> palette key the Dashboard chart uses.</summary>
    internal static IReadOnlyList<SessionSeriesSpec> SessionSeriesSpecs { get; } = new[]
    {
        new SessionSeriesSpec("Total", "SessionTotal", p => p.TotalSessions),
        new SessionSeriesSpec("Running", "SessionRunning", p => p.RunningSessions),
        new SessionSeriesSpec("Sleeping", "SessionSleeping", p => p.SleepingSessions),
        new SessionSeriesSpec("Background", "SessionBackground", p => p.BackgroundSessions),
        new SessionSeriesSpec("Dormant", "SessionDormant", p => p.DormantSessions),
        new SessionSeriesSpec("Idle >30m", "SessionIdle", p => p.IdleSessionsOver30Min),
        new SessionSeriesSpec("Waiting for Memory", "SessionWaiting", p => p.SessionsWaitingForMemory),
    };

    private readonly ChartRenderHelper _chartHelper;
    private readonly Func<DateTime, DateTime> _project;
    private readonly Func<TimeZoneInfo>? _displayZone;

    /// <param name="chartHelper">The app's chart clear/legend helper.</param>
    /// <param name="project">Turns a stored UTC sample time into the app's plotted X.</param>
    /// <param name="displayZone">
    /// Set by an app whose chart X is the UTC instant (#4766): every sample is then plotted at its own UTC time
    /// (<paramref name="project"/> is not applied) and the bottom axis prints whole wall-clock times of the zone
    /// (<see cref="AxesExtensions.DateTimeTicksBottomUtc"/>). Omitted, the renderer plots <paramref name="project"/>'s
    /// value and draws the axis exactly as it always has.
    /// </param>
    internal SessionStatsChartRenderer(ChartRenderHelper chartHelper, Func<DateTime, DateTime> project, Func<TimeZoneInfo>? displayZone = null)
    {
        _chartHelper = chartHelper;
        _project = project;
        _displayZone = displayZone;
    }

    /// <summary>The X a sample at <paramref name="sampleUtc"/> plots at: its UTC time when a display zone was passed, else the app's projection of it.</summary>
    private double PlotX(DateTime sampleUtc) => _displayZone is null ? _project(sampleUtc).ToOADate() : sampleUtc.ToOADate();

    /// <summary>
    /// Renders the session-status trend: one scatter per status that is non-zero somewhere in the window (each on
    /// its fixed palette key), the date axis bound to the pre-computed <paramref name="xMin"/>/<paramref
    /// name="xMax"/> display-time bounds, Y floored at 0 with legend padding, and a bottom legend. Chart-only —
    /// the caller owns the summary strip from the same ordered data.
    /// </summary>
    internal void Render(
        ScottPlot.WPF.WpfPlot chart, ChartHoverHelper? hover, IReadOnlyList<ISessionStatsPoint> data,
        double xMin, double xMax)
    {
        _chartHelper.ClearChart(chart);
        hover?.Clear();
        ChartStyle.ApplyThemeToChart(chart);

        var ordered = data.OrderBy(d => d.CollectionTime).ToList();

        double globalMax = 0;
        if (ordered.Count > 0)
        {
            var times = ordered.Select(d => PlotX(d.CollectionTime)).ToArray();

            foreach (var spec in SessionSeriesSpecs)
            {
                var values = ordered.Select(spec.Value).ToArray();

                /* Mirror the Dashboard: only draw a series that is non-zero somewhere in the window, so
                   all-zero states (often Background / Dormant / Waiting for Memory) stay out of the legend. */
                if (!values.Any(v => v > 0))
                {
                    continue;
                }

                var plot = chart.Plot.Add.TimeSeries(times, values);
                plot.LegendText = spec.Legend;
                plot.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor(spec.PaletteKey));
                ChartStyle.StyleScatter(plot);
                hover?.Add(plot, spec.Legend);

                globalMax = Math.Max(globalMax, values.Max());
            }
        }

        chart.Plot.Axes.DateTimeTicksBottomFor(_displayZone);
        chart.Plot.Axes.SetLimitsX(xMin, xMax);
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.YLabel("Session Count");
        ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, globalMax > 0 ? globalMax : 10);
        _chartHelper.ShowChartLegend(chart);
        chart.Refresh();
    }
}

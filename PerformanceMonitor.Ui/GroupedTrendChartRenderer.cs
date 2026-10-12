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
/// Shared renderer for a "top-N grouped per-second trend" chart — the shape behind the Latches &amp;
/// Spinlocks tab's two charts (latch wait ms/sec per latch class, spinlock collisions/sec per spinlock).
/// The ScottPlot body was byte-identical across Lite's <c>ServerTab.LatchSpinlock.cs</c> and Darling's
/// <c>ViewerServerTab.LatchSpinlock.cs</c> — and, within each app, between the latch and spinlock render —
/// apart from the chart control, the group-key / value properties, the empty-state legend text, the Y-axis
/// label, and the display-time projection. Those vary via <see cref="Func{T, TResult}"/> accessors rather
/// than an interface, because the two trend point types name their properties differently
/// (<c>LatchClass</c>/<c>WaitTimeMsPerSecond</c> vs <c>SpinlockName</c>/<c>CollisionsPerSecond</c>) and
/// stay per-app. Each app constructs one with its own projection (Lite adds its UTC offset; Darling uses
/// <c>ViewerTimeHelper.ForDisplay</c>) and hands the chart + hover + accessors + pre-computed window
/// bounds; the store-specific trend read stays per-app. INTERNAL because it takes the internal
/// <see cref="ChartHoverHelper"/> (both apps reach it via Ui's InternalsVisibleTo).
/// </summary>
internal sealed class GroupedTrendChartRenderer
{
    private static readonly string[] SeriesColors = ChartPalette.CyclingPalette.ToArray();

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
    internal GroupedTrendChartRenderer(ChartRenderHelper chartHelper, Func<DateTime, DateTime> project, Func<TimeZoneInfo>? displayZone = null)
    {
        _chartHelper = chartHelper;
        _project = project;
        _displayZone = displayZone;
    }

    /// <summary>The X a sample at <paramref name="sampleUtc"/> plots at: its UTC time when a display zone was passed, else the app's projection of it.</summary>
    private double PlotX(DateTime sampleUtc) => _displayZone is null ? _project(sampleUtc).ToOADate() : sampleUtc.ToOADate();

    /// <summary>
    /// Renders one grouped per-second trend: when the window has no data, a flat labelled zero-line across
    /// [xMin,xMax]; otherwise one scatter series per group (ordered by total value descending so the
    /// heaviest contender takes the first palette color), each point's time projected to display space,
    /// legend clipped via <see cref="TruncateName"/>, tracked in the hover. <paramref name="xMin"/> /
    /// <paramref name="xMax"/> are the display-space window bounds as OADate doubles (the caller already
    /// projected them); only the per-point times get re-projected here.
    /// </summary>
    internal void Render<T>(
        ScottPlot.WPF.WpfPlot chart,
        ChartHoverHelper? hover,
        IReadOnlyList<T> data,
        Func<T, DateTime> time,
        Func<T, string> groupKey,
        Func<T, double> value,
        string emptyLegend,
        string yLabel,
        double xMin,
        double xMax)
    {
        _chartHelper.ClearChart(chart);
        hover?.Clear();
        ChartStyle.ApplyThemeToChart(chart);

        if (data.Count == 0)
        {
            var zeroLine = chart.Plot.Add.Scatter(new[] { xMin, xMax }, new[] { 0.0, 0.0 });
            zeroLine.LegendText = emptyLegend;
            zeroLine.Color = ScottPlot.Color.FromHex(SeriesColors[0]);
            zeroLine.MarkerSize = 0;
            chart.Plot.Axes.DateTimeTicksBottomFor(_displayZone);
            chart.Plot.Axes.SetLimitsX(xMin, xMax);
            ChartStyle.ReapplyAxisColors(chart);
            chart.Plot.YLabel(yLabel);
            ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, 1);
            _chartHelper.ShowChartLegend(chart);
            chart.Refresh();
            return;
        }

        /* Order the series by total value desc so the heaviest contender gets the first palette color
           (deterministic, mirrors the Dashboard's re-group-by-total). */
        var byGroup = data
            .GroupBy(groupKey)
            .OrderByDescending(g => g.Sum(value))
            .ToList();

        double globalMax = 0;
        int colorIdx = 0;
        foreach (var group in byGroup)
        {
            var points = group.OrderBy(time).ToList();
            var times = points.Select(d => PlotX(time(d))).ToArray();
            var values = points.Select(value).ToArray();

            var plot = chart.Plot.Add.TimeSeries(times, values);
            plot.LegendText = TruncateName(group.Key);
            plot.Color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            ChartStyle.StyleScatter(plot);
            hover?.Add(plot, group.Key);
            colorIdx++;

            if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
        }

        chart.Plot.Axes.DateTimeTicksBottomFor(_displayZone);
        chart.Plot.Axes.SetLimitsX(xMin, xMax);
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.YLabel(yLabel);
        ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, globalMax > 0 ? globalMax : 1);
        _chartHelper.ShowChartLegend(chart);
        chart.Refresh();
    }

    /// <summary>Legend names for latch classes / spinlock names get long; clip to 20 chars + ellipsis
    /// exactly like the Dashboard's Latch/Spinlock chart legends.</summary>
    private static string TruncateName(string name)
        => name.Length > 20 ? name.Substring(0, 20) + "..." : name;
}

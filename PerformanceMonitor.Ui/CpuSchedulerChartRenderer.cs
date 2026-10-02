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
/// Shared renderer for the CPU Scheduler pressure trend chart — three fixed series (Runnable / Blocked /
/// Queued task counts) over the settable window. The ScottPlot body was byte-identical between Lite's
/// <c>ServerTab.CpuScheduler.cs</c> and Darling's <c>ViewerServerTab.CpuScheduler.cs</c> apart from the
/// display-time projection, so it lives here once. Each app constructs one with its own projection (Lite
/// adds its UTC offset; Darling uses <c>ViewerTimeHelper.ForDisplay</c>) and hands the chart + hover +
/// pre-computed window bounds; the store-specific trend read stays per-app. INTERNAL because it takes the
/// internal <see cref="ChartHoverHelper"/> (both apps reach it via Ui's InternalsVisibleTo).
/// </summary>
internal sealed class CpuSchedulerChartRenderer
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
    internal CpuSchedulerChartRenderer(ChartRenderHelper chartHelper, Func<DateTime, DateTime> project, Func<TimeZoneInfo>? displayZone = null)
    {
        _chartHelper = chartHelper;
        _project = project;
        _displayZone = displayZone;
    }

    /// <summary>The X a sample at <paramref name="sampleUtc"/> plots at: its UTC time when a display zone was passed, else the app's projection of it.</summary>
    private double PlotX(DateTime sampleUtc) => _displayZone is null ? _project(sampleUtc).ToOADate() : sampleUtc.ToOADate();

    internal void Render(ScottPlot.WPF.WpfPlot chart, ChartHoverHelper? hover, IReadOnlyList<ICpuSchedulerTrendPoint> data, double xMin, double xMax)
    {
        _chartHelper.ClearChart(chart);
        hover?.Clear();
        ChartStyle.ApplyThemeToChart(chart);

        double globalMax = 0;
        if (data.Count > 0)
        {
            var ordered = data.OrderBy(d => d.CollectionTime).ToList();
            var times = ordered.Select(d => PlotX(d.CollectionTime)).ToArray();

            var series = new (string Name, Func<ICpuSchedulerTrendPoint, double> Selector)[]
            {
                ("Runnable Tasks", d => d.RunnableTasks),
                ("Blocked Tasks", d => d.BlockedTasks),
                ("Queued Requests", d => d.QueuedRequests),
            };

            int colorIdx = 0;
            foreach (var s in series)
            {
                var values = ordered.Select(s.Selector).ToArray();
                var plot = chart.Plot.Add.TimeSeries(times, values);
                plot.LegendText = s.Name;
                plot.Color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
                ChartStyle.StyleScatter(plot);
                hover?.Add(plot, s.Name);
                colorIdx++;
                if (values.Length > 0) globalMax = Math.Max(globalMax, values.Max());
            }
        }

        chart.Plot.Axes.DateTimeTicksBottomFor(_displayZone);
        chart.Plot.Axes.SetLimitsX(xMin, xMax);
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.YLabel("Task Count");
        ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, globalMax > 0 ? globalMax : 5);
        _chartHelper.ShowChartLegend(chart);
        chart.Refresh();
    }
}

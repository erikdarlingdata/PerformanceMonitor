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
/// Shared renderer for the System Events tab's SYSTEM-component counter charts (the Corruption + Contention
/// 2x2 grids). The ScottPlot chart BODIES were byte-identical between Lite's
/// <c>ServerTab.SystemHealthCharts.cs</c> and Darling's <c>ViewerServerTab.SystemHealthCharts.cs</c> apart
/// from the display-time projection, so they live here once. Each app constructs one with its own projection
/// (Lite adds its settable UTC offset; Darling uses <c>ViewerTimeHelper.ForDisplay</c>) and passes each chart
/// + hover; the store-specific data read and the chart-field / hover wiring stay per-app. INTERNAL because it
/// takes the internal <see cref="ChartHoverHelper"/> (both apps see it via Ui's InternalsVisibleTo). (The
/// deprecated Full Dashboard keeps its own copies.)
/// </summary>
internal sealed class SystemHealthChartRenderer
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
    internal SystemHealthChartRenderer(ChartRenderHelper chartHelper, Func<DateTime, DateTime> project, Func<TimeZoneInfo>? displayZone = null)
    {
        _chartHelper = chartHelper;
        _project = project;
        _displayZone = displayZone;
    }

    /// <summary>The X a sample at <paramref name="sampleUtc"/> plots at: its UTC time when a display zone was passed, else the app's projection of it.</summary>
    private double PlotX(DateTime sampleUtc) => _displayZone is null ? _project(sampleUtc).ToOADate() : sampleUtc.ToOADate();

    /// <summary>
    /// One single-series counter chart (Bad Pages / Dump Requests / Access Violations / Write AV /
    /// Non-Yielding Tasks / Latch Warnings): a scatter of the selected counter over event time, Y floored at
    /// 0, no legend. Empty window shows the centered "No data" text. <paramref name="selector"/>
    /// null-coalesces to 0 so a snapshot missing the attribute plots a zero, never a gap.
    /// </summary>
    internal void RenderCounterChart(
        ScottPlot.WPF.WpfPlot chart, ChartHoverHelper? hover, List<SystemHealthRecord> data,
        Func<SystemHealthRecord, long?> selector, string label, string colorHex, double xMin, double xMax)
    {
        _chartHelper.ClearChart(chart);
        hover?.Clear();
        ChartStyle.ApplyThemeToChart(chart);

        double max = 0;
        if (data.Count == 0)
        {
            AddNoDataText(chart, xMin, xMax);
        }
        else
        {
            var xs = data.Select(d => PlotX(d.EventTime!.Value)).ToArray();
            var ys = data.Select(d => (double)(selector(d) ?? 0)).ToArray();

            var scatter = chart.Plot.Add.TimeSeries(xs, ys);
            scatter.Color = ScottPlot.Color.FromHex(colorHex);
            ChartStyle.StyleScatter(scatter);
            hover?.Add(scatter, label);

            max = ys.Length > 0 ? ys.Max() : 0;
        }

        chart.Plot.Axes.DateTimeTicksBottomFor(_displayZone);
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.Axes.SetLimitsX(xMin, xMax);
        /* The tiles are borderless (no in-panel title), so the descriptive Y-axis label is the chart's
           identifier — the same convention the File I/O charts use ("Read Latency (ms)"). */
        chart.Plot.YLabel(label);
        ChartStyle.UseCountAxis(chart); // #7606: every counter chart counts events
        chart.Plot.Axes.SetLimitsY(0, max > 0 ? max * 1.15 : 1);
        chart.Refresh();
    }

    /// <summary>How many spinlock types the Sick Spinlocks chart draws: the ones with the most backoffs in the window (#8187).</summary>
    internal const int SickSpinlockSeriesLimit = 5;

    /// <summary>
    /// Sick Spinlocks by Type: one series per <c>SickSpinlockType</c> for the <see cref="SickSpinlockSeriesLimit"/> types with the most
    /// backoffs in the window, busiest first, plotting the spinlock backoffs over event time, with a bottom legend (backoffs, or 1 when
    /// absent). The Y label says how many types there were when some were left out.
    /// </summary>
    internal void RenderSickSpinlocksChart(
        ScottPlot.WPF.WpfPlot chart, ChartHoverHelper? hover, List<SystemHealthRecord> data, double xMin, double xMax)
    {
        _chartHelper.ClearChart(chart);
        hover?.Clear();
        ChartStyle.ApplyThemeToChart(chart);

        double max = 0;
        var typeCount = 0;
        var spinlockTypes = new List<string>();
        if (data.Count == 0)
        {
            AddNoDataText(chart, xMin, xMax);
        }
        else
        {
            (spinlockTypes, typeCount) = TopSickSpinlockTypes(data, SickSpinlockSeriesLimit);

            int colorIdx = 0;
            foreach (var spinlockType in spinlockTypes)
            {
                var typeData = data.Where(d => d.SickSpinlockType == spinlockType).ToList();
                if (typeData.Count == 0)
                    continue;

                var xs = typeData.Select(d => PlotX(d.EventTime!.Value)).ToArray();
                var ys = typeData.Select(d => (double)(d.SpinlockBackoffs ?? 1)).ToArray();

                var scatter = chart.Plot.Add.TimeSeries(xs, ys);
                scatter.Color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
                ChartStyle.StyleScatter(scatter);
                scatter.LegendText = spinlockType;
                hover?.Add(scatter, spinlockType);
                colorIdx++;

                if (ys.Length > 0)
                    max = Math.Max(max, ys.Max());
            }

            if (spinlockTypes.Count > 0)
                _chartHelper.ShowChartLegend(chart);
        }

        chart.Plot.Axes.DateTimeTicksBottomFor(_displayZone);
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.Axes.SetLimitsX(xMin, xMax);
        chart.Plot.YLabel(SickSpinlocksYLabel(spinlockTypes.Count, typeCount));
        ChartStyle.UseCountAxis(chart); // #7606: a count, whole-number ticks
        ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, max);
        chart.Refresh();
    }

    /// <summary>
    /// #8187: the spinlock types in <paramref name="data"/> ranked by their total backoffs in the window (a record with no count plots
    /// as 1, and counts as 1 here), busiest first and ties by name, cut to <paramref name="limit"/>; and how many types there were, so
    /// the chart can say when it left some out. Records with no type are not a type.
    /// </summary>
    internal static (List<string> Top, int Total) TopSickSpinlockTypes(IEnumerable<SystemHealthRecord> data, int limit)
    {
        var ranked = data
            .Where(d => !string.IsNullOrEmpty(d.SickSpinlockType))
            .GroupBy(d => d.SickSpinlockType!, StringComparer.Ordinal)
            .OrderByDescending(g => g.Sum(d => (double)(d.SpinlockBackoffs ?? 1)))
            .ThenBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => g.Key)
            .ToList();
        return (ranked.Take(limit).ToList(), ranked.Count);
    }

    /// <summary>The Sick Spinlocks chart's Y label: its name, with "top 5 of N types" when the window had more types than it draws (#8187).</summary>
    internal static string SickSpinlocksYLabel(int shown, int total)
        => total > shown ? $"Sick Spinlocks (backoffs), top {shown} of {total} types" : "Sick Spinlocks (backoffs)";

    /// <summary>
    /// SQL CPU vs System CPU: two percent series over event time on a fixed 0-100 axis with a bottom legend
    /// (System CPU on the cycling accent, SQL CPU on the shared SqlCpu identity color).
    /// </summary>
    internal void RenderCpuComparisonChart(
        ScottPlot.WPF.WpfPlot chart, ChartHoverHelper? hover, List<SystemHealthRecord> data, double xMin, double xMax)
    {
        _chartHelper.ClearChart(chart);
        hover?.Clear();
        ChartStyle.ApplyThemeToChart(chart);

        if (data.Count == 0)
        {
            AddNoDataText(chart, xMin, xMax);
        }
        else
        {
            var xs = data.Select(d => PlotX(d.EventTime!.Value)).ToArray();

            /* #7416 item 1: a reading the server could not give (System CPU on SQL Server on Linux) is a gap, never a flat zero,
               and a series with no reading at all is not drawn. */
            var systemCpu = CpuSeriesValues(data.Select(d => d.SystemCpuUtilization));
            if (systemCpu.Any(v => !double.IsNaN(v)))
            {
                var sysScatter = chart.Plot.Add.TimeSeries(xs, systemCpu);
                sysScatter.Color = ScottPlot.Color.FromHex(ChartPalette.CyclingColor(0));
                ChartStyle.StyleScatter(sysScatter);
                sysScatter.LegendText = "System CPU %";
                hover?.Add(sysScatter, "System CPU %");
            }

            var sqlScatter = chart.Plot.Add.TimeSeries(xs, CpuSeriesValues(data.Select(d => d.SqlCpuUtilization)));
            sqlScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SqlCpu"));
            ChartStyle.StyleScatter(sqlScatter);
            sqlScatter.LegendText = "SQL CPU %";
            hover?.Add(sqlScatter, "SQL CPU %");

            _chartHelper.ShowChartLegend(chart);
        }

        chart.Plot.Axes.DateTimeTicksBottomFor(_displayZone);
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.Axes.SetLimitsX(xMin, xMax);
        chart.Plot.YLabel("CPU %");
        chart.Plot.Axes.SetLimitsY(0, 100);
        chart.Refresh();
    }

    /// <summary>A CPU reading per snapshot as plot values: a missing reading is NaN (a gap), never 0 (#7416 item 1).</summary>
    internal static double[] CpuSeriesValues(IEnumerable<long?> readings) =>
        readings.Select(v => v.HasValue ? (double)v.Value : double.NaN).ToArray();

    /// <summary>Centered "No data for selected time range" annotation for an empty counter chart.</summary>
    private static void AddNoDataText(ScottPlot.WPF.WpfPlot chart, double xMin, double xMax)
    {
        double xCenter = xMin + (xMax - xMin) / 2;
        var text = chart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
        text.LabelFontSize = 14;
        text.LabelFontColor = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));
        text.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
    }
}

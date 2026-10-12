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
        chart.Plot.Axes.SetLimitsY(0, max > 0 ? max * 1.15 : 1);
        chart.Refresh();
    }

    /// <summary>
    /// Sick Spinlocks by Type: one series per distinct <c>SickSpinlockType</c> (top 5, to bound clutter)
    /// plotting the spinlock backoffs over event time, with a bottom legend (backoffs, or 1 when absent).
    /// </summary>
    internal void RenderSickSpinlocksChart(
        ScottPlot.WPF.WpfPlot chart, ChartHoverHelper? hover, List<SystemHealthRecord> data, double xMin, double xMax)
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
            var spinlockTypes = data
                .Where(d => !string.IsNullOrEmpty(d.SickSpinlockType))
                .Select(d => d.SickSpinlockType)
                .Distinct()
                .Take(5)
                .ToList();

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
                scatter.LegendText = spinlockType ?? "Unknown";
                hover?.Add(scatter, spinlockType ?? "Unknown");
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
        chart.Plot.YLabel("Sick Spinlocks (backoffs)");
        ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, max);
        chart.Refresh();
    }

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

            var sysScatter = chart.Plot.Add.TimeSeries(xs, data.Select(d => (double)(d.SystemCpuUtilization ?? 0)).ToArray());
            sysScatter.Color = ScottPlot.Color.FromHex(ChartPalette.CyclingColor(0));
            ChartStyle.StyleScatter(sysScatter);
            sysScatter.LegendText = "System CPU %";
            hover?.Add(sysScatter, "System CPU %");

            var sqlScatter = chart.Plot.Add.TimeSeries(xs, data.Select(d => (double)(d.SqlCpuUtilization ?? 0)).ToArray());
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

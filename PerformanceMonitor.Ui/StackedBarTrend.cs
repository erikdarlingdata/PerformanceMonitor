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
/// #7571: the Current Waits tab's Waiting Tasks and Blocked Sessions charts read one row per collection time and
/// key (wait type, database). Drawn as one line per key, the line joined moments hours apart into a ramp that never
/// happened. A bar at each collection time says "at this moment" and joins nothing; the keys at one moment stack in
/// one column. Both apps draw both charts through this one body.
/// </summary>
internal static class StackedBarTrend
{
    private static readonly string[] SeriesColors = ChartPalette.CyclingPalette.ToArray();

    /// <summary>The widest a bar gets, as a share of the plotted window: wide enough to see when a window holds only a few collection times.</summary>
    private const double MaxBarShareOfWindow = 1.0 / 120;

    /// <summary>The narrowest a bar gets, as a share of the plotted window: about a pixel on a full-size chart, so a dense window still draws.</summary>
    private const double MinBarShareOfWindow = 1.0 / 1000;

    /// <summary>Share of the usual gap between collection times that a bar fills, so neighbours do not touch.</summary>
    private const double GapFill = 0.8;

    /// <summary>
    /// Adds one <see cref="ScottPlot.Plottables.BarPlot"/> per key, ordered by key and coloured from the palette in that
    /// order, with a bar at each collection time stacked on the keys before it. A key's bars are named in the legend and
    /// the hover. Zero and negative values draw nothing. Returns the tallest stack, for the Y limit.
    /// </summary>
    /// <param name="time">The plotted X of a row: the UTC instant, as these charts' bottom axis prints it in the display zone.</param>
    /// <param name="windowStartOa">The plotted window's left edge, as an OADate; with <paramref name="windowEndOa"/> it sets the bar width.</param>
    internal static double Add<T>(
        ScottPlot.Plot plot,
        ChartHoverHelper? hover,
        IReadOnlyList<T> data,
        Func<T, DateTime> time,
        Func<T, string> key,
        Func<T, double> value,
        double windowStartOa,
        double windowEndOa)
    {
        var keys = data.Select(key).Distinct().OrderBy(k => k, StringComparer.Ordinal).ToList();

        /* One total per (time, key): two rows for the same moment and key add up rather than overdraw each other. */
        var totals = data
            .GroupBy(d => (Time: time(d).ToOADate(), Key: key(d)))
            .ToDictionary(g => g.Key, g => g.Sum(value));
        var times = totals.Keys.Select(k => k.Time).Distinct().OrderBy(t => t).ToList();

        var size = BarSize(times, windowEndOa - windowStartOa);
        var running = new Dictionary<double, double>();
        double tallest = 0;

        for (int i = 0; i < keys.Count; i++)
        {
            var color = ScottPlot.Color.FromHex(SeriesColors[i % SeriesColors.Length]);
            var bars = new List<ScottPlot.Bar>();
            foreach (var t in times)
            {
                if (!totals.TryGetValue((t, keys[i]), out var v) || v <= 0) continue;
                running.TryGetValue(t, out var below);
                var top = below + v;
                running[t] = top;
                tallest = Math.Max(tallest, top);
                bars.Add(new ScottPlot.Bar { Position = t, ValueBase = below, Value = top, Size = size, FillColor = color, LineWidth = 0 });
            }

            if (bars.Count == 0) continue;
            var barPlot = plot.Add.Bars(bars);
            barPlot.LegendText = keys[i];
            hover?.Add(barPlot, keys[i]);
        }

        return tallest;
    }

    /// <summary>
    /// A bar's width in days: 80 percent of the usual gap between collection times, never wider than 1/120th of the
    /// window (a window with a few moments hours apart still shows a bar a reader can see) and never narrower than
    /// 1/1000th of it (a dense window still draws).
    /// </summary>
    internal static double BarSize(IReadOnlyList<double> sortedTimes, double windowSpanDays)
    {
        var span = windowSpanDays > 0 ? windowSpanDays : 1.0;
        var widest = span * MaxBarShareOfWindow;
        var narrowest = span * MinBarShareOfWindow;
        if (sortedTimes.Count < 2) return widest;

        var gaps = new List<double>(sortedTimes.Count - 1);
        for (int i = 1; i < sortedTimes.Count; i++) gaps.Add(sortedTimes[i] - sortedTimes[i - 1]);
        gaps.Sort();
        var median = gaps[gaps.Count / 2];
        return Math.Min(Math.Max(median * GapFill, narrowest), widest);
    }
}

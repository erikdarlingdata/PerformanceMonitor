/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using ScottPlot.Plottables;

namespace Darling.Tests;

/// <summary>
/// Reads the above-band marks a Viewer lane drew (#7678). A run of neighbouring points is ONE thicker segment and a lone point is one small dot,
/// so a test of which points are marked reads every mark in the anomaly colour, whatever shape each took.
/// </summary>
internal static class OverviewLaneMarks
{
    private static IEnumerable<Scatter> Marks(ScottPlot.Plot plot)
    {
        var anomaly = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Anomaly"));
        return plot.GetPlottables<Scatter>().Where(s => s.Color == anomaly);
    }

    /// <summary>The X of every point that carries a mark, in order, whether it sits in a segment or is a dot of its own.</summary>
    public static List<double> MarkedX(ScottPlot.Plot plot) =>
        Marks(plot).SelectMany(s => s.Data.GetScatterPoints()).Select(p => p.X).OrderBy(x => x).ToList();

    /// <summary>The number of marked points.</summary>
    public static int MarkedCount(ScottPlot.Plot plot) => Marks(plot).Sum(s => s.Data.GetScatterPoints().Count());

    /// <summary>
    /// True when every anomaly mark sits after the lane's own series line in the plot's plottable list. ScottPlot paints in that order, so a mark
    /// added first is painted over by the 1.5 px line instead of redrawing the line's stretch thicker. False when there is no mark or no series line.
    /// </summary>
    public static bool MarksComeAfterTheSeries(ScottPlot.Plot plot)
    {
        var anomaly = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Anomaly"));
        var list = plot.PlottableList;
        var firstMark = -1;
        var lastSeries = -1;
        for (var i = 0; i < list.Count; i++)
        {
            if (list[i] is not Scatter sc) continue;
            if (sc.Color == anomaly) { if (firstMark < 0) firstMark = i; }
            else lastSeries = i;
        }
        return firstMark >= 0 && lastSeries >= 0 && lastSeries < firstMark;
    }

    /// <summary>One entry per drawn mark: how many points it spans and whether it is the lone-point dot (no line) or a segment.</summary>
    public static List<(int Points, bool IsDot)> Shapes(ScottPlot.Plot plot) =>
        Marks(plot).Select(s => (s.Data.GetScatterPoints().Count(), s.LineWidth == 0)).ToList();
}

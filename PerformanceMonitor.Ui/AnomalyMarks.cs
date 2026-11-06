/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The marks the Overview lanes draw on points outside the baseline band, one routine for the Darling Viewer and Lite so the two cannot drift apart.
/// </summary>
public static class AnomalyMarks
{
    /// <summary>
    /// #7678: a run of neighbouring marked points (above or below the band, the marker predicate marks both) is ONE mark, drawn as a
    /// thicker stretch of the lane's own line in the anomaly colour. A marked point alone is one small dot. A 6 px circle per point turned
    /// a quiet server's lane (a third of its points marked) into a solid row of dots that hid the line.
    /// <paramref name="indices"/> ascend, and neighbours are points one index apart.
    /// <para>
    /// Call this AFTER the lane's own series is added: ScottPlot paints plottables in the order they were added, and the mark has to sit over
    /// the 1.5 px line to read as that stretch redrawn thicker, the way the web strip draws it. The lane's line breaks at a collection gap
    /// (<see cref="TimeSeriesGaps.GapThreshold"/>, 3 times the series' median spacing), so a run splits at the same gap: two marked points either
    /// side of a hole are two marks, never a segment across the hole. <paramref name="times"/> must be the lane's FULL series (the one its line is
    /// added from), which is where the threshold comes from; a run drawn on its own would judge gaps against its own few points.
    /// </para>
    /// </summary>
    public static void Add(ScottPlot.Plot plot, double[] times, double[] values, IReadOnlyList<int> indices)
    {
        var color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Anomaly"));
        var gap = times.Length > 0 ? TimeSeriesGaps.GapThreshold(times) : null;
        var start = 0;
        while (start < indices.Count)
        {
            var end = start;
            while (end + 1 < indices.Count && indices[end + 1] == indices[end] + 1
                && (gap is not double limit || times[indices[end + 1]] - times[indices[end]] <= limit)) end++;
            var count = end - start + 1;
            var runTimes = new double[count];
            var runValues = new double[count];
            Array.Copy(times, indices[start], runTimes, 0, count);
            Array.Copy(values, indices[start], runValues, 0, count);
            // Plain scatter: the run is already split at the lane's gaps, and TimeSeries would judge it against its own few points.
            var mark = plot.Add.Scatter(runTimes, runValues);
            mark.Color = color;
            if (count == 1)
            {
                mark.MarkerSize = 4;
                mark.MarkerShape = ScottPlot.MarkerShape.FilledCircle;
                mark.LineWidth = 0;
            }
            else
            {
                mark.MarkerSize = 0;
                mark.LineWidth = 3.5f;
            }
            start = end + 1;
        }
    }
}

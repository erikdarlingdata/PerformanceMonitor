/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Breaks a time series' line where the data has a collection gap, so charts never draw a line across a
/// window in which nothing was collected (#1944, from discussion #1936: a monitor that was offline rendered
/// a continuous line, which reads as data that never existed).
///
/// <para><b>The threshold derives from the series' own cadence</b> — three times the median positive
/// inter-point spacing — because collectors here run at wildly different frequencies (one-minute counters to
/// daily index stats) and no constant serves both: a fixed threshold either shatters slow series into dots
/// or papers over real outages on fast ones. Median rather than mean so the outage gaps being detected do
/// not inflate the yardstick used to detect them — the same robust-statistics argument as #1743, one layer
/// down.</para>
///
/// <para><b>The break is a <see cref="double.NaN"/> Y injected between the gap's endpoints.</b> ScottPlot 5
/// renders a NaN as a line break (the documented "Scatter with Gaps" behavior), the endpoints keep their
/// markers, and axis limits ignore NaN. Callers hand their already-built OADate/value arrays to
/// <see cref="BreakAtGaps"/> at the point they add the scatter; series too short to have a cadence (fewer
/// than three points) and series with no measurable spacing (all duplicate timestamps) pass through
/// untouched, because claiming a gap needs evidence of a rhythm to have gapped.</para>
/// </summary>
public static class TimeSeriesGaps
{
    /// <summary>Gap threshold multiplier over the median positive inter-point spacing. Three keeps normal
    /// scheduler jitter and a single missed cycle connected, while an outage of two-plus cycles breaks.</summary>
    private const double CadenceMultiplier = 3.0;

    /// <summary>
    /// Returns the series with a NaN break point injected inside every gap wider than
    /// <see cref="CadenceMultiplier"/> times the series' median positive spacing. Inputs must be parallel
    /// arrays with X ascending (the order every chart here already builds); the inputs are never mutated.
    /// Returns the original arrays when no break is needed, so the common no-gap case allocates nothing.
    /// </summary>
    public static (double[] Xs, double[] Ys) BreakAtGaps(double[] xs, double[] ys)
        => BreakAtGaps(xs, ys, null);

    /// <summary>
    /// <see cref="BreakAtGaps(double[], double[])"/> for a series that joins two grains (#7508): the Query Store
    /// Duration Trend is one point per hour before the instant the raw intervals start (<c>routing.raw_from</c>,
    /// <c>QueryStoreTrendRoute.RawStartUtc</c>) and one point per Query Store interval from it. One threshold for
    /// the whole series cannot judge both: once the interval points outnumber the hourly ones the median falls to
    /// minutes and every hourly step reads as a gap, so the older part drew as lone dots. With
    /// <paramref name="seamX"/> (an OADate) that has points on both sides, each side gets its own threshold, so
    /// the hourly side is judged against an hour and the raw side against its interval, and a real outage on either
    /// side still breaks. A pair that crosses the seam is judged by the hourly side, whose step is the larger.
    /// With no seam, or every point on one side of it, this is the plain rule.
    /// </summary>
    public static (double[] Xs, double[] Ys) BreakAtGaps(double[] xs, double[] ys, double? seamX)
    {
        if (xs is null || ys is null || xs.Length != ys.Length || xs.Length < 3)
        {
            return (xs!, ys!);
        }

        var threshold = GapThreshold(xs);
        double? beforeSeam = null;
        double? afterSeam = null;
        var seamed = false;
        if (seamX is double seam && double.IsFinite(seam))
        {
            var first = Array.FindIndex(xs, x => x >= seam);
            if (first > 0)
            {
                seamed = true;
                beforeSeam = GapThreshold(xs[..first]);
                afterSeam = GapThreshold(xs[first..]);
            }
        }

        if (!seamed && threshold is null)
        {
            return (xs, ys);
        }

        List<int>? breakAfter = null;
        for (var i = 0; i < xs.Length - 1; i++)
        {
            var limit = seamed ? (xs[i] < seamX!.Value ? beforeSeam : afterSeam) : threshold;
            if (limit is not null && xs[i + 1] - xs[i] > limit.Value)
            {
                (breakAfter ??= new List<int>()).Add(i);
            }
        }

        if (breakAfter is null)
        {
            return (xs, ys);
        }

        var outXs = new double[xs.Length + breakAfter.Count];
        var outYs = new double[ys.Length + breakAfter.Count];
        var next = 0;
        var write = 0;
        foreach (var b in breakAfter)
        {
            var count = b - next + 1;
            Array.Copy(xs, next, outXs, write, count);
            Array.Copy(ys, next, outYs, write, count);
            write += count;

            /* The break sits mid-gap so neither endpoint's marker moves and hover/crosshair math on the
               real points is unaffected. */
            outXs[write] = xs[b] + (xs[b + 1] - xs[b]) / 2.0;
            outYs[write] = double.NaN;
            write++;
            next = b + 1;
        }

        Array.Copy(xs, next, outXs, write, xs.Length - next);
        Array.Copy(ys, next, outYs, write, ys.Length - next);

        return (outXs, outYs);
    }

    /// <summary>
    /// The gap threshold for an ascending X array: <see cref="CadenceMultiplier"/> x the median POSITIVE
    /// spacing, or null when the series has no measurable cadence (fewer than two distinct timestamps).
    /// Zero spacings — duplicate timestamps, common when several series share one collection cycle — are
    /// excluded from the median rather than dragging it to zero, which would break the line everywhere.
    /// </summary>
    internal static double? GapThreshold(double[] xs)
    {
        var deltas = new List<double>(xs.Length - 1);
        for (var i = 0; i < xs.Length - 1; i++)
        {
            var d = xs[i + 1] - xs[i];
            if (d > 0)
            {
                deltas.Add(d);
            }
        }

        if (deltas.Count < 2)
        {
            return null;
        }

        deltas.Sort();
        var mid = deltas.Count / 2;
        var median = deltas.Count % 2 == 1
            ? deltas[mid]
            : (deltas[mid - 1] + deltas[mid]) / 2.0;

        return median > 0 ? median * CadenceMultiplier : null;
    }
}

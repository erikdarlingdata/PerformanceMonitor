/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using ScottPlot;
using ScottPlot.Plottables;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The gap-aware way to add a TIME series line (#1944): identical to <c>Add.Scatter</c> except the line
/// breaks where the data has a collection gap (see <see cref="TimeSeriesGaps"/>), so an offline window
/// renders as absence rather than as a line claiming data that was never collected.
///
/// <para>Use this for every series whose X axis is time. Non-time scatters (histograms, synthetic
/// zero-lines, pixel-space annotations) keep plain <c>Add.Scatter</c> — a cadence is a property of
/// collection timestamps, and deriving one from arbitrary X data would be noise.</para>
/// </summary>
public static class TimeSeriesPlotExtensions
{
    /// <summary>
    /// Adds a scatter line whose segments break across collection gaps. Drop-in for
    /// <c>Add.Scatter(xs, ys)</c> on ascending time data; returns the same <see cref="Scatter"/> the
    /// caller styles today.
    /// </summary>
    public static Scatter TimeSeries(this PlottableAdder add, double[] xs, double[] ys)
    {
        var (gapXs, gapYs) = TimeSeriesGaps.BreakAtGaps(xs, ys);
        return add.Scatter(gapXs, gapYs);
    }

    /// <summary>
    /// <see cref="TimeSeries(PlottableAdder, double[], double[])"/> for a series that changes grain at
    /// <paramref name="seamX"/> (an OADate; #7508), each side judged against its own spacing. Only the Query
    /// Store Duration Trend passes one.
    /// </summary>
    public static Scatter TimeSeries(this PlottableAdder add, double[] xs, double[] ys, double? seamX)
    {
        var (gapXs, gapYs) = TimeSeriesGaps.BreakAtGaps(xs, ys, seamX);
        return add.Scatter(gapXs, gapYs);
    }
}

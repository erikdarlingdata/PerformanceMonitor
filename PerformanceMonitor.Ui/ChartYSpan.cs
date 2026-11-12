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
/// The least a size chart's Y axis spans. An autoscaled axis fits the series exactly, so a series that moves 0.01 MB around 8,190 MB
/// gets an axis 0.01 MB tall: every tick prints the same number and the noise fills the plot as a field of bars. A floor on the span
/// keeps a near-flat series reading as the flat line it is, with tick labels that differ. A series that moves more than the floor keeps
/// the axis the autoscale gave it.
/// </summary>
public static class ChartYSpan
{
    /// <summary>The floor as a share of the series' magnitude: 2 % of 8,190 MB is about 164 MB.</summary>
    public const double MinimumRelativeSpan = 0.02;

    /// <summary>The floor for a series near zero, in the axis' unit (MB on the size charts).</summary>
    public const double MinimumAbsoluteSpan = 1.0;

    /// <summary>
    /// The axis range for a series spanning <paramref name="low"/>..<paramref name="high"/>: the range itself when it is at least the
    /// floor, otherwise the floor centred on the series. A series at or above zero is never given a negative axis: the range slides up
    /// to start at zero instead.
    /// </summary>
    public static (double Min, double Max) Widen(double low, double high)
    {
        if (!double.IsFinite(low) || !double.IsFinite(high))
        {
            return (low, high);
        }

        if (high < low)
        {
            (low, high) = (high, low);
        }

        var floor = Math.Max(MinimumAbsoluteSpan, MinimumRelativeSpan * Math.Max(Math.Abs(low), Math.Abs(high)));
        var span = high - low;
        if (span >= floor)
        {
            return (low, high);
        }

        var pad = (floor - span) / 2;
        var min = low - pad;
        var max = high + pad;
        if (low >= 0 && min < 0)
        {
            max -= min;
            min = 0;
        }

        return (min, max);
    }

    /// <summary>
    /// After the plot autoscaled its Y axis to <paramref name="values"/>, widens that axis to the floor <see cref="Widen"/> gives when
    /// the series is flatter than the floor. A wider series keeps the autoscaled axis, padding and all.
    /// </summary>
    public static void ApplyMinimum(ScottPlot.Plot plot, IEnumerable<double> values)
    {
        ArgumentNullException.ThrowIfNull(plot);
        ArgumentNullException.ThrowIfNull(values);

        var low = double.PositiveInfinity;
        var high = double.NegativeInfinity;
        foreach (var value in values)
        {
            if (double.IsNaN(value))
            {
                continue;
            }

            low = Math.Min(low, value);
            high = Math.Max(high, value);
        }

        if (high < low)
        {
            return;
        }

        var (min, max) = Widen(low, high);
        if (min != low || max != high)
        {
            plot.Axes.SetLimitsY(min, max);
        }
    }
}

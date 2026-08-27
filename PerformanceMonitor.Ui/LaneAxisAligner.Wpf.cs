/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * The WPF half of LaneAxisAligner (#2533). It is separate from LaneAxisAligner.cs on purpose: the
 * measuring/flooring logic there touches nothing but ScottPlot, so it stays exercisable headlessly on
 * any platform, and everything that needs a laid-out WPF control is confined here.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Ui;

internal static partial class LaneAxisAligner
{
    /// <summary>
    /// WPF entry point: aligns a stack of lane controls at the size they are actually laid out at,
    /// falling back to <see cref="FallbackMeasureHeight"/> before first layout.
    /// </summary>
    /// <returns>The shared gutter width in pixels, or 0 if nothing was aligned.</returns>
    internal static float AlignLeftGutters(IReadOnlyList<ScottPlot.WPF.WpfPlot> lanes)
    {
        if (lanes is null || lanes.Count == 0)
            return 0f;

        /* Measure at the lanes' real device-pixel height rather than a nominal one. Any height at or
           above the real one aligns exactly, so the TALLEST laid-out lane is the safe pick - the lanes
           sit in equal-height grid rows that differ only by a 2 px margin, and rounding that margin the
           wrong way is the one way this could under-reserve. DisplayScale is ScottPlot's own WPF-unit to
           device-pixel factor, so it matches what the control will really render at. */
        double tallest = 0;
        var plots = new List<ScottPlot.Plot>(lanes.Count);
        foreach (var lane in lanes)
        {
            plots.Add(lane.Plot);
            if (lane.ActualHeight > 0)
                tallest = Math.Max(tallest, lane.ActualHeight * lane.DisplayScale);
        }

        return AlignLeftGutters(plots, tallest > 0 ? (float)tallest : FallbackMeasureHeight);
    }
}

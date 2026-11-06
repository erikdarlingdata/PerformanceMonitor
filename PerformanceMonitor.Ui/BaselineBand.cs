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
/// The shaded normal range (mean plus or minus two standard deviations) the Overview lanes draw behind a series.
/// </summary>
/// <remarks>
/// #7433: the lanes used to call <c>Plot.Add.HorizontalSpan(lower, upper)</c>, and in ScottPlot 5 a HorizontalSpan
/// takes a range on the X axis. <c>lower</c> and <c>upper</c> are Y values, so the band sat at dates near 1899 and
/// never drew; only the dashed mean line showed. A <c>VerticalSpan</c> is the Y-axis range. One helper keeps the six
/// lane call sites (three in the Darling Viewer, three in Lite) from drifting apart again.
/// <para>
/// The span is added BEFORE the lane's series so it paints behind them. It does not move the Y axis: every lane sets
/// its Y limits explicitly after the plottables are added, and the lanes drop all pan, zoom and autoscale input, so a
/// band whose <c>upper</c> is far above the series is clipped at the lane's edge and the series stays readable.
/// </para>
/// </remarks>
public static class BaselineBand
{
    /// <summary>Fill opacity of the band, out of 255. Low enough that the series on top reads through it.</summary>
    public const byte FillAlpha = 40;

    /// <summary>Adds the band (Y range <paramref name="lower"/> to <paramref name="upper"/>) in <paramref name="hexColor"/>.</summary>
    public static ScottPlot.Plottables.VerticalSpan Add(ScottPlot.Plot plot, double lower, double upper, string hexColor)
    {
        var band = plot.Add.VerticalSpan(lower, upper);
        band.FillStyle.Color = ScottPlot.Color.FromHex(hexColor).WithAlpha(FillAlpha);
        band.LineStyle.Width = 0;
        return band;
    }

    /// <summary>
    /// #7644: one local hour of a stepped band, on the lane's X axis (OADate days). <c>Lower</c> and <c>Upper</c> are the hour's
    /// mean minus and plus two effective standard deviations (the lower edge not below zero). The step covers
    /// <c>FromX</c> up to, but not including, <c>ToX</c>.
    /// </summary>
    public sealed record HourStep(double FromX, double ToX, double Mean, double Lower, double Upper)
    {
        /// <summary>The step a point at <paramref name="x"/> falls in, or null when its hour has none (no band, no marker).</summary>
        public static HourStep? At(IReadOnlyList<HourStep>? steps, double x)
        {
            if (steps is null)
                return null;

            /* The steps arrive sorted by FromX and do not overlap, so the one step that can hold x is the last whose FromX is at or
               before it. A binary search: a 30-day custom range holds about 720 steps and this runs for every marker point and on every
               crosshair move, where the linear walk it replaces cost the whole list each time. NaN lands before every step, as it
               matched none before. */
            int lo = 0, hi = steps.Count - 1, found = -1;
            while (lo <= hi)
            {
                var mid = lo + ((hi - lo) / 2);
                if (steps[mid].FromX <= x)
                {
                    found = mid;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }

            return found >= 0 && x < steps[found].ToX ? steps[found] : null;
        }
    }

    /// <summary>Builds a step from an hour's bucket: mean plus or minus two effective standard deviations, the lower edge not below zero.</summary>
    public static HourStep MakeStep(double fromX, double toX, double mean, double effectiveStdDev)
        => new(fromX, toX, mean, Math.Max(0, mean - 2 * effectiveStdDev), mean + 2 * effectiveStdDev);

    /// <summary>
    /// #7644: draws the stepped band, one rectangle per hour, then a dashed mean segment per hour. The rectangles are added first
    /// so they paint behind the lane's series; the mean segments follow them. Returns the rectangles.
    /// </summary>
    public static List<ScottPlot.Plottables.Rectangle> AddSteps(ScottPlot.Plot plot, IReadOnlyList<HourStep> steps, string hexColor)
    {
        var rectangles = new List<ScottPlot.Plottables.Rectangle>(steps.Count);
        foreach (var step in steps)
        {
            var rect = plot.Add.Rectangle(step.FromX, step.ToX, step.Lower, step.Upper);
            rect.FillStyle.Color = ScottPlot.Color.FromHex(hexColor).WithAlpha(FillAlpha);
            rect.LineStyle.Width = 0;
            /* #7386: a step is a band, not a collected point. The first one starts at the window's left edge, so unmarked it would stand in
               for "the earliest point drawn" and cancel the trimmed axis start the lanes share. */
            ChartAxisStart.Baseline(rect);
            rectangles.Add(rect);
        }

        foreach (var step in steps)
        {
            var mean = plot.Add.Line(step.FromX, step.Mean, step.ToX, step.Mean);
            mean.Color = ScottPlot.Color.FromHex(hexColor).WithAlpha(60);
            mean.LinePattern = ScottPlot.LinePattern.Dashed;
            mean.LineWidth = 1;
            ChartAxisStart.Baseline(mean);
        }

        return rectangles;
    }
}

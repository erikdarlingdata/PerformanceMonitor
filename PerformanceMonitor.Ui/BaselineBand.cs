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
    /// #8162: draws the band as one translucent fill, not a rectangle per hour. The edges run straight from each hour's value (taken
    /// at the middle of its hour) to the next hour's, so the band has no steps, and its first and last hours hold their value out to
    /// the run's ends. Hours that have no step split the band: each run of neighbouring steps is one fill, and a gap stays empty.
    /// The dashed mean follows the same path. The fill is added first so it paints behind the lane's series. Each hour's own step
    /// still decides which points get a mark: the fill is the picture of the range, not the rule.
    /// </summary>
    public static void AddBand(ScottPlot.Plot plot, IReadOnlyList<HourStep> steps, string hexColor)
    {
        var runs = Runs(steps);
        foreach (var run in runs)
        {
            var upper = Edge(run, step => step.Upper);
            var lower = Edge(run, step => step.Lower);
            var outline = new ScottPlot.Coordinates[upper.Count + lower.Count];
            for (var i = 0; i < upper.Count; i++)
                outline[i] = upper[i];
            for (var i = 0; i < lower.Count; i++)
                outline[upper.Count + i] = lower[lower.Count - 1 - i];

            var fill = plot.Add.Polygon(outline);
            fill.FillColor = ScottPlot.Color.FromHex(hexColor).WithAlpha(FillAlpha);
            fill.LineWidth = 0;
            /* #7386: the band is not a collected point. It starts at the window's left edge, so unmarked it would stand in
               for "the earliest point drawn" and cancel the trimmed axis start the lanes share. */
            ChartAxisStart.Baseline(fill);
        }

        foreach (var run in runs)
        {
            var mean = Edge(run, step => step.Mean);
            var line = plot.Add.Scatter(mean.Select(c => c.X).ToArray(), mean.Select(c => c.Y).ToArray());
            line.Color = ScottPlot.Color.FromHex(hexColor).WithAlpha(60);
            line.MarkerSize = 0;
            line.LinePattern = ScottPlot.LinePattern.Dashed;
            line.LineWidth = 1;
            ChartAxisStart.Baseline(line);
        }
    }

    /// <summary>The steps split where one does not start at the end of the one before (an hour with no step), each run in order.</summary>
    private static List<List<HourStep>> Runs(IReadOnlyList<HourStep> steps)
    {
        const double JoinTolerance = 1.0 / 86400.0; // a second, in days
        var runs = new List<List<HourStep>>();
        foreach (var step in steps)
        {
            if (runs.Count > 0 && Math.Abs(runs[^1][^1].ToX - step.FromX) <= JoinTolerance)
                runs[^1].Add(step);
            else
                runs.Add([step]);
        }

        return runs;
    }

    /// <summary>One edge of a run: the run's start, the middle of each hour at <paramref name="value"/>, the run's end.</summary>
    private static List<ScottPlot.Coordinates> Edge(List<HourStep> run, Func<HourStep, double> value)
    {
        var points = new List<ScottPlot.Coordinates> { new(run[0].FromX, value(run[0])) };
        foreach (var step in run)
            points.Add(new((step.FromX + step.ToX) / 2, value(step)));
        points.Add(new(run[^1].ToX, value(run[^1])));
        return points;
    }
}

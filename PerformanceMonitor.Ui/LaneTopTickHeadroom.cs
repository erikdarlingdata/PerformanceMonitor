/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Keeps a timeline lane's highest Y tick label whole (#8307). A lane's plot is flush with the lane's top edge (no top axis: the
/// lane's name is a caption over the plot), and ScottPlot centers each left-axis tick label on its tick, so a tick at the top of
/// the Y range puts the label's upper half above the plot, where the lane's edge cuts it. The lane keeps its plot where it is and
/// gives the range headroom instead: the top of the Y range is raised until the highest labelled tick sits at least half its
/// label's height below the plot's top edge. A lane whose top tick already has that room is left as it is.
/// </summary>
/// <remarks>
/// The ticks a range gets depend on the plot's pixel height (a taller plot takes denser ticks), so the pass renders the lane in
/// memory at its real height, reads the ticks ScottPlot chose, and sets the limits from them. Raising the top never makes the
/// ticks denser (a wider range at the same height takes the same step or a coarser one), so the highest tick after the raise is
/// the one measured or a lower one, and one pass settles it; the loop is a guard, not a search. The core takes plain
/// <see cref="ScottPlot.Plot"/>s so it renders headlessly, as <see cref="LaneAxisAligner"/> does.
/// </remarks>
internal static class LaneTopTickHeadroom
{
    /// <summary>The pixels a label's top stays inside the plot's edge by, over its own half height.</summary>
    internal const double Margin = 0.5;

    /// <summary>The rounding a measured gap may fall short of the need by and still count as met: the data rectangle is laid out in whole pixels, so the gap a range gives lands within this of the one asked for.</summary>
    internal const double Tolerance = 0.1;

    /// <summary>The figure height a lane not yet laid out is measured at: the plot floor every chart keeps, so the fraction of the range given to headroom is the largest a lane can need.</summary>
    internal const int FallbackMeasureHeight = (int)ChartLayout.MinPlotAreaHeight;

    private const int MaxPasses = 3;

    /// <summary>
    /// Raises the top of the lane's Y range so the highest labelled tick sits at least half its label's height (and
    /// <see cref="Margin"/>) below the plot's top edge, measured at <paramref name="measureHeight"/> figure pixels.
    /// </summary>
    /// <returns>The pixels between the highest labelled tick and the plot's top edge after the pass, or NaN when nothing was measured.</returns>
    internal static double Apply(ScottPlot.Plot plot, float measureHeight)
    {
        int height = (int)Math.Clamp(measureHeight, 60, 4000);
        double gap = double.NaN;
        try
        {
            for (var pass = 0; pass < MaxPasses; pass++)
            {
                plot.RenderInMemory(LaneAxisAligner.MeasureWidth, height);
                var plotHeight = plot.RenderManager.LastRender.DataRect.Height;
                var limits = plot.Axes.GetLimits();
                var span = limits.Top - limits.Bottom;
                if (plotHeight <= 0 || span <= 0 || double.IsNaN(span) || double.IsInfinity(span))
                    return gap;

                var topTick = plot.Axes.Left.TickGenerator.Ticks
                    .Where(t => t.IsMajor && !string.IsNullOrEmpty(t.Label) && t.Position >= limits.Bottom && t.Position <= limits.Top)
                    .OrderByDescending(t => t.Position)
                    .Select(t => (ScottPlot.Tick?)t)
                    .FirstOrDefault();
                if (topTick is null)
                    return gap;

                var need = NeededPixels(plot, topTick.Value.Label);
                gap = plotHeight * (limits.Top - topTick.Value.Position) / span;
                if (gap + Tolerance >= need)
                    return gap;

                /* The tick sits `need` pixels under the new top when (top - tick) / (top - bottom) = need / plotHeight. */
                var fraction = need / plotHeight;
                if (fraction >= 0.5)
                    return gap;
                plot.Axes.SetLimitsY(limits.Bottom, (topTick.Value.Position - limits.Bottom * fraction) / (1 - fraction));
            }

            return gap;
        }
        catch (Exception ex)
        {
            /* A measuring render is not worth losing the Overview over: the lane keeps the limits it had. */
            Debug.WriteLine($"Lane top tick headroom skipped: {ex.Message}");
            return gap;
        }
    }

    /// <summary>
    /// The pixels <paramref name="label"/> needs above its tick on the plot's left axis: half the height ScottPlot lays it out at,
    /// plus <see cref="Margin"/>. ScottPlot lays the plot out in figure pixels divided by the plot's scale factor and measures a
    /// label at its unscaled font size, so this and the gap read from the data rectangle are in the same units at any scale.
    /// </summary>
    internal static double NeededPixels(ScottPlot.Plot plot, string label)
    {
        using var paint = ScottPlot.Paint.NewDisposablePaint();
        return plot.Axes.Left.TickLabelStyle.Measure(label, paint).Height / 2.0 + Margin;
    }

    /// <summary>
    /// WPF entry point: gives every lane the headroom its top tick label needs at the height it is laid out at, or at
    /// <see cref="FallbackMeasureHeight"/> before first layout.
    /// </summary>
    internal static void Apply(IReadOnlyList<ScottPlot.WPF.WpfPlot> lanes)
    {
        foreach (var lane in lanes)
        {
            var height = lane.ActualHeight > 0 ? (float)(lane.ActualHeight * lane.DisplayScale) : FallbackMeasureHeight;
            Apply(lane.Plot, height);
        }
    }
}

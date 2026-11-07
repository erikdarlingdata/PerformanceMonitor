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
using ScottPlot;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #7385: gives charts that are shown together the same plot rectangle, on all four sides.
/// </summary>
/// <remarks>
/// <para>
/// Every ScottPlot chart sizes its own plot from what is left after its caption, tick labels, axis title and legend. Side
/// by side, a chart with a two-line caption and a three-line legend gets a plot half the height of the empty chart next to
/// it, which has no tick labels at all. <see cref="LaneAxisAligner"/> fixes the left gutter of a vertical stack; a block of
/// charts needs every side.
/// </para>
/// <para>
/// Mechanism, the same one <see cref="LaneAxisAligner"/> chose: a FLOOR (<c>MinimumSize</c>) on one panel per edge, so
/// the layout stays automatic and a chart that needs more room than the floor still gets it. Each edge's floor is
/// the group's largest natural total on that edge, less what the chart's other panels there already take. The left, right
/// and bottom floors sit on the axis, the top floor on the title panel. The legend panel is never given a floor: it belongs to
/// <see cref="WrappedLegendLayout"/>, which sizes it for the width it wraps at. One exception (#8086): a chart whose legend is on the
/// right keeps its right-hand spare width in that engine's <see cref="WrappedLegendLayout.Spare"/>, so the blank space is
/// beyond the legend and not between the legend and the plot. A chart that shows only its empty-state sentence has no
/// plot rectangle and is not part of a group at all.
/// </para>
/// <para>
/// This half takes plain <see cref="Plot"/> objects and renders in memory, so it runs headlessly. The WPF half
/// (<c>ChartGroupAligner.Wpf.cs</c>) finds the group, measures at the controls' real size, and debounces.
/// </para>
/// </remarks>
internal static partial class ChartGroupAligner
{
    /// <summary>Largest padding a measuring render may be asked for, guarding a zero or absurd size from layout.</summary>
    private const int MaxPixels = 8000;

    /// <summary>One edge's total padding, in the plot's scaled units, as its last render laid it out.</summary>
    internal readonly record struct Frame(float Left, float Right, float Top, float Bottom)
    {
        internal Frame Max(Frame o) => new(Math.Max(Left, o.Left), Math.Max(Right, o.Right), Math.Max(Top, o.Top), Math.Max(Bottom, o.Bottom));
        internal bool Matches(Frame o, float tolerance) =>
            Math.Abs(Left - o.Left) <= tolerance && Math.Abs(Right - o.Right) <= tolerance
            && Math.Abs(Top - o.Top) <= tolerance && Math.Abs(Bottom - o.Bottom) <= tolerance;
    }

    /// <summary>The padding a chart's last render left around its plot.</summary>
    internal static Frame LastFrame(Plot plot)
    {
        /* From the layout, not RenderDetails.Padding: the padding is the scaled data rectangle taken from the UNSCALED figure,
           so at a display scale above 1 its right and bottom come out wrong. The layout's two rectangles share one unit. */
        var layout = plot.RenderManager.LastRender.Layout;
        return new Frame(
            layout.DataRect.Left - layout.FigureRect.Left,
            layout.FigureRect.Right - layout.DataRect.Right,
            layout.DataRect.Top - layout.FigureRect.Top,
            layout.FigureRect.Bottom - layout.DataRect.Bottom);
    }

    /// <summary>Removes the floors this class sets, so the next render shows the chart's own size.</summary>
    internal static void ClearFloors(Plot plot)
    {
        plot.Axes.Left.MinimumSize = 0f;
        plot.Axes.Right.MinimumSize = AxesExtensions.RightEdgePadFor(plot); // #7606: a time axis' own right gutter is not a floor to clear
        plot.Axes.Bottom.MinimumSize = 0f;
        plot.Axes.Title.MinimumSize = 0f;
        if (plot.Layout.LayoutEngine is WrappedLegendLayout engine) engine.Spare = 0f; // #8086
    }

    /// <summary>
    /// Core form: clears every chart's floors, renders each in memory at its real pixel size, and floors every edge at the
    /// group's largest. Returns the shared frame, or null when nothing was aligned.
    /// </summary>
    /// <param name="charts">The charts and the pixel size each will render at, already carrying their final data.</param>
    internal static Frame? Align(IReadOnlyList<(Plot Plot, int Width, int Height)> charts)
    {
        if (charts is null || charts.Count < 2) return null;

        try
        {
            var natural = new Frame[charts.Count];
            var anchors = new (float Left, float Right, float Bottom, float Top)[charts.Count];
            var target = default(Frame);

            for (var i = 0; i < charts.Count; i++)
            {
                var plot = charts[i].Plot;
                ClearFloors(plot);
                plot.RenderInMemory(Math.Clamp(charts[i].Width, 1, MaxPixels), Math.Clamp(charts[i].Height, 1, MaxPixels));
                natural[i] = LastFrame(plot);
                var sizes = plot.RenderManager.LastRender.Layout.PanelSizes;
                anchors[i] = (Size(sizes, plot.Axes.Left), Size(sizes, plot.Axes.Right), Size(sizes, plot.Axes.Bottom), Size(sizes, plot.Axes.Title));
                target = i == 0 ? natural[i] : target.Max(natural[i]);
            }

            for (var i = 0; i < charts.Count; i++)
            {
                var plot = charts[i].Plot;
                plot.Axes.Left.MinimumSize = Floor(anchors[i].Left, target.Left - natural[i].Left);
                var rightDeficit = target.Right - natural[i].Right;
                if (plot.Layout.LayoutEngine is WrappedLegendLayout { Panel.Edge: Edge.Right } rightLegend)
                {
                    /* #8086: a right legend's chart gives its spare width to the legend panel, the outermost one, so the blank
                       space falls to the right of the legend and not between the plot and the legend. */
                    plot.Axes.Right.MinimumSize = AxesExtensions.RightEdgePadFor(plot);
                    rightLegend.Spare = rightDeficit > 0.5f ? rightDeficit : 0f;
                }
                else
                {
                    plot.Axes.Right.MinimumSize = Math.Max(AxesExtensions.RightEdgePadFor(plot), Floor(anchors[i].Right, rightDeficit));
                }

                plot.Axes.Bottom.MinimumSize = Floor(anchors[i].Bottom, target.Bottom - natural[i].Bottom);
                plot.Axes.Title.MinimumSize = Floor(anchors[i].Top, target.Top - natural[i].Top);
            }

            return target;
        }
        catch (Exception ex)
        {
            /* A measuring render is not worth losing a chart over. Leave every chart un-floored - correct data with
               ragged edges - rather than half-floored, which would misalign in a new way. */
            Debug.WriteLine($"Chart group alignment skipped: {ex.Message}");
            foreach (var (plot, _, _) in charts) ClearFloors(plot);
            return null;
        }
    }

    private static float Size(Dictionary<IPanel, float> sizes, IPanel panel) =>
        sizes.TryGetValue(panel, out var size) ? size : 0f;

    private static float Floor(float natural, float deficit) => deficit > 0.5f ? natural + deficit : 0f;
}

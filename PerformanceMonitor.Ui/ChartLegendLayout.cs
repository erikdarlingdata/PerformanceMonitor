/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using ScottPlot;
using ScottPlot.LayoutEngines;
using ScottPlot.Panels;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #7385: sizes the legend panel for the width it will really be drawn at, in the same render. #8086: a right-edge legend is
/// sized to its own widest entry instead, and <see cref="Spare"/> adds the room a chart group keeps for its neighbors' wider legends.
/// </summary>
/// <remarks>
/// <para>
/// ScottPlot's <c>LegendPanel.Measure</c> reads <c>Legend.LastRenderSize</c>, which is the PREVIOUS render's size, and on the
/// first render it measures the legend with no width limit (one line). A horizontal legend that wraps would then be drawn
/// into a panel one line tall, and grow by about one padding per later render, so the first frames clip it.
/// </para>
/// <para>
/// This engine wraps <see cref="Automatic"/>. The first pass fixes the data rectangle's width, which the legend wraps to and
/// which does not depend on the legend's height. The legend is laid out at that width, the panel is locked to the height that
/// needs, and a second pass places everything. A render whose legend already has its height pays for one pass.
/// </para>
/// </remarks>
internal sealed class WrappedLegendLayout : ILayoutEngine
{
    private readonly Automatic _automatic = new();
    private readonly LegendPanel _panel;

    internal WrappedLegendLayout(LegendPanel panel) => _panel = panel;

    /// <summary>The legend panel this engine sizes.</summary>
    internal LegendPanel Panel => _panel;

    /// <summary>
    /// Extra room added to a RIGHT legend's panel, on the side away from the plot, so charts in a group whose legends differ in
    /// width share one plot rectangle and the spare space falls to the right of the legend (<see cref="ChartGroupAligner"/>).
    /// </summary>
    internal float Spare { get; set; }

    public Layout GetLayout(PixelRect figureRect, Plot plot, Paint paint)
    {
        var layout = _automatic.GetLayout(figureRect, plot, paint);

        var right = _panel.Edge == Edge.Right;
        float need;
        try
        {
            if (right)
            {
                var size = new PixelSize(float.PositiveInfinity, layout.DataRect.Height);
                need = _panel.Legend.GetLayout(size, paint).LegendRect.Width + _panel.Padding.Horizontal + Spare;
            }
            else
            {
                var size = new PixelSize(layout.DataRect.Width, float.PositiveInfinity);
                need = _panel.Legend.GetLayout(size, paint).LegendRect.Height + _panel.Padding.Vertical;
            }
        }
        catch (Exception)
        {
            // A legend with no entries has nothing to lay out; the default measure is right for it.
            return layout;
        }

        if (!(need > 0) || float.IsInfinity(need)) return layout;
        if (layout.PanelSizes.TryGetValue(_panel, out var current) && Math.Abs(current - need) < 0.5f) return layout;

        _panel.MinimumSize = need;
        _panel.MaximumSize = need;
        return _automatic.GetLayout(figureRect, plot, paint);
    }
}

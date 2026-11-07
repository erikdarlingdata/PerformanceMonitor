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
using ScottPlot;
using ScottPlot.LayoutEngines;
using ScottPlot.Panels;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #7385: sizes the legend panel for the width it will really be drawn at, in the same render. #8086: a right-edge legend is
/// sized to its own widest entry instead, and <see cref="Spare"/> adds the room a chart group keeps for its neighbors' wider legends.
/// #8220: the engine also decides, on every size it is asked to lay out, where the legend goes.
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
/// <para>
/// The one legend rule (#8220), applied to the chart's size at layout time, so a window resize moves the legend and the first
/// draw needs no size: (1) the legend sits beside the plot when that leaves the plot at least <see cref="MinSidePlotShare"/> of
/// the chart's width and the whole column fits the plot's height; (2) otherwise it goes under the plot; (3) when under the plot
/// would leave the plot less than <see cref="MinPlotHeight"/> DIPs, the legend shrinks to ONE row of whole entries, ending in a
/// "+N more" entry that says how many were left out (the hover still names every series). An entry is never cut. The choice
/// is remembered per chart size and entry list, so a hover or a pan does not re-decide it.
/// </para>
/// </remarks>
internal sealed class WrappedLegendLayout : ILayoutEngine
{
    /// <summary>The plot keeps at least this many DIPs of height; a legend that would take it below shows as one row.</summary>
    internal const float MinPlotHeight = 120f;

    /// <summary>A legend beside the plot leaves the plot at least this share of the chart's width, else it goes under the plot.</summary>
    internal const float MinSidePlotShare = 0.60f;

    private readonly Automatic _automatic = new();
    private readonly LegendPanel _panel;
    private readonly float _defaultMinimum;
    private readonly float _defaultMaximum;
    private readonly PixelPadding _basePadding;
    private readonly Alignment _baseAlignment;

    private LegendItem[] _all = Array.Empty<LegendItem>();
    private bool _compact;
    private int _more; // 0 "+N more", 1 "+N", 2 no marker (a chart too narrow for an entry and a marker on one row)
    private (Edge Edge, bool Compact, int Fit) _decision;
    private string? _decidedFor;

    internal WrappedLegendLayout(LegendPanel panel)
    {
        _panel = panel;
        _defaultMinimum = panel.MinimumSize;
        _defaultMaximum = panel.MaximumSize;
        _basePadding = panel.Padding;
        _baseAlignment = panel.Alignment;
    }

    /// <summary>The legend panel this engine sizes.</summary>
    internal LegendPanel Panel => _panel;

    /// <summary>
    /// Extra room added to a RIGHT legend's panel, on the side away from the plot, so charts in a group whose legends differ in
    /// width share one plot rectangle and the spare space falls to the right of the legend (<see cref="ChartGroupAligner"/>).
    /// </summary>
    internal float Spare { get; set; }

    /// <summary>
    /// #8220: set by <see cref="ChartGroupAligner"/> when the charts of a group would not agree on a side (one legend beside the plot,
    /// another under it, or a legend with a wide empty strip beside it): every legend of the group goes under its plot, so the plots
    /// share one rectangle and no strip is left empty.
    /// </summary>
    internal bool ForceBottom { get; set; }

    public Layout GetLayout(PixelRect figureRect, Plot plot, Paint paint)
    {
        if (!_compact) _all = plot.Legend.GetItems();
        if (_all.Length == 0) return _automatic.GetLayout(figureRect, plot, paint);

        Layout laid;
        try
        {
            var key = $"{figureRect.Width:0.#}x{figureRect.Height:0.#}|{ForceBottom}|{_all.Length}|{string.Concat(_all.Select(i => i.LabelText.Length.ToString()))}";
            if (_decidedFor != key)
            {
                _decision = Decide(figureRect, plot, paint);
                _decidedFor = key;
            }

            laid = Place(figureRect, plot, paint, _decision.Edge, _decision.Compact, _decision.Fit, Spare).Layout;
        }
        catch (Exception)
        {
            // A legend with no entries has nothing to lay out; the default measure is right for it.
            return _automatic.GetLayout(figureRect, plot, paint);
        }

        return laid;
    }

    private (Edge Edge, bool Compact, int Fit) Decide(PixelRect figureRect, Plot plot, Paint paint)
    {
        /* #8086: a chart at least three times as wide as it is tall keeps its height for the plot and puts the legend beside it, as
           long as that leaves the plot 60% of the width and the column fits. Any other chart keeps its width, unless under the plot
           would leave it under 120 px, when beside it is tried too. */
        var under = Place(figureRect, plot, paint, Edge.Bottom, compact: false, fit: 0, spare: 0f);
        var wide = figureRect.Width >= 3 * figureRect.Height;
        var underFits = under.Layout.DataRect.Height >= MinPlotHeight;
        if (!ForceBottom && (wide || !underFits))
        {
            var side = Place(figureRect, plot, paint, Edge.Right, compact: false, fit: 0, spare: 0f);
            if (side.Layout.DataRect.Width >= MinSidePlotShare * figureRect.Width && side.LegendHeight <= side.Layout.DataRect.Height)
            {
                return (Edge.Right, false, 0);
            }
        }

        if (underFits) return (Edge.Bottom, false, 0);

        /* One row. The data rectangle's width does not depend on the legend's height, so any layout gives the width the row wraps at.
           A legend that already fits one row has nothing to leave out. */
        Apply(Edge.Bottom, compact: false, fit: 0); // the side trial above left the legend on the right
        var width = under.Layout.DataRect.Width;
        var oneRow = RowHeight(width, paint);
        if (under.LegendHeight <= oneRow + _panel.Padding.Vertical + 0.5f) return (Edge.Bottom, false, 0);
        return (Edge.Bottom, true, FitOneRow(width, oneRow, paint));
    }

    /// <summary>The height of one row of the legend: the first entry alone.</summary>
    private float RowHeight(float width, Paint paint)
    {
        var legend = _panel.Legend;
        legend.DisplayPlottableLegendItems = false;
        legend.ManualItems.Clear();
        legend.ManualItems.Add(_all[0]);
        return legend.GetLayout(new PixelSize(width, float.PositiveInfinity), paint).LegendRect.Height;
    }

    /// <summary>
    /// The most whole entries that fit, with the "+N more" entry after them, on one row of the given width. A chart too narrow for even
    /// one entry and "+N more" tries the short "+N", and then the entry alone. At least one entry.
    /// </summary>
    private int FitOneRow(float width, float oneRow, Paint paint)
    {
        for (var more = 0; more <= 2; more++)
        {
            _more = more;
            for (var fit = _all.Length - 1; fit >= 1; fit--)
            {
                Apply(Edge.Bottom, compact: true, fit);
                if (_panel.Legend.GetLayout(new PixelSize(width, float.PositiveInfinity), paint).LegendRect.Height <= oneRow + 0.5f) return fit;
            }
        }

        return 1;
    }

    /// <summary>Puts the legend on <paramref name="edge"/> with all its entries, or with the first <paramref name="fit"/> and a "+N more" entry.</summary>
    private void Apply(Edge edge, bool compact, int fit)
    {
        var legend = _panel.Legend;
        _panel.Edge = edge;
        if (edge == Edge.Right)
        {
            legend.Orientation = Orientation.Vertical;
            _panel.Alignment = Alignment.MiddleLeft;
            _panel.Padding = _basePadding;
        }
        else
        {
            legend.Orientation = Orientation.Horizontal;
            legend.TightHorizontalWrapping = true;
            _panel.Alignment = _baseAlignment;
            /* ScottPlot pads a legend panel 20 px on every side, which is 40 px of blank under the legend (Session Stats at
               800x600: 90 of the chart's 286 px). The entries carry their own spacing, so 10 px above and below is enough and the
               plot gets the rest. */
            _panel.Padding = new PixelPadding(_basePadding.Left, _basePadding.Right, 10, 10);
        }

        _compact = compact && fit < _all.Length;
        if (!_compact)
        {
            legend.DisplayPlottableLegendItems = true;
            legend.ManualItems.Clear();
            return;
        }

        legend.DisplayPlottableLegendItems = false;
        legend.ManualItems.Clear();
        legend.ManualItems.AddRange(_all.Take(fit));
        if (_more < 2) legend.ManualItems.Add(new LegendItem { LabelText = _more == 1 ? $"+{_all.Length - fit}" : $"+{_all.Length - fit} more" });
    }

    /// <summary>One layout of the chart with the legend on <paramref name="edge"/>, and the room its entries take.</summary>
    private (Layout Layout, float LegendHeight) Place(PixelRect figureRect, Plot plot, Paint paint, Edge edge, bool compact, int fit, float spare)
    {
        Apply(edge, compact, fit);
        _panel.MinimumSize = _defaultMinimum;
        _panel.MaximumSize = _defaultMaximum;
        var layout = _automatic.GetLayout(figureRect, plot, paint);

        var right = edge == Edge.Right;
        float need;
        float legendHeight;
        if (right)
        {
            var rect = _panel.Legend.GetLayout(new PixelSize(float.PositiveInfinity, layout.DataRect.Height), paint).LegendRect;
            need = rect.Width + _panel.Padding.Horizontal + spare;
            legendHeight = rect.Height + _panel.Padding.Vertical;
        }
        else
        {
            var rect = _panel.Legend.GetLayout(new PixelSize(layout.DataRect.Width, float.PositiveInfinity), paint).LegendRect;
            need = rect.Height + _panel.Padding.Vertical;
            legendHeight = need;
        }

        if (!(need > 0) || float.IsInfinity(need)) return (layout, legendHeight);
        if (layout.PanelSizes.TryGetValue(_panel, out var current) && Math.Abs(current - need) < 0.5f) return (layout, legendHeight);

        _panel.MinimumSize = need;
        _panel.MaximumSize = need;
        return (_automatic.GetLayout(figureRect, plot, paint), legendHeight);
    }
}

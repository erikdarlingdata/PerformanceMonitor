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
/// <para>
/// #8540: an entry wider than the row under the plot (a sentence, such as the restart marker's, on a narrow chart) is fitted to that row
/// before the rule above places the entries. It is broken across lines at its spaces, so the whole entry shows. When those lines would
/// take the plot under <see cref="MinPlotHeight"/> and one line per entry would not, the entry keeps one line and ends in an ellipsis.
/// A word wider than the row is cut the same way. The hover shows the whole text of an entry that ends in an ellipsis.
/// While an entry is on more than one line the legend's rows take their own heights (<see cref="RowHeightLegendLayout"/>).
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
    private readonly ILegendLayout _stockRows;
    private readonly ILegendLayout _ownRows = new RowHeightLegendLayout();

    private LegendItem[] _all = Array.Empty<LegendItem>();
    private readonly Dictionary<string, string> _fitted = new(StringComparer.Ordinal); // a wide entry's text as its plottable names it, to the text the legend shows for it
    private readonly Dictionary<LegendItem, string> _named = new(ReferenceEqualityComparer.Instance); // the plottable's text of each entry now showing its fitted text
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
        _stockRows = panel.Legend.Layout is RowHeightLegendLayout ? new ScottPlot.LegendLayouts.Wrapping() : panel.Legend.Layout;
    }

    /// <summary>The legend panel this engine sizes.</summary>
    internal LegendPanel Panel => _panel;

    /// <summary>
    /// #8307: a legend under the plot keeps to one row of whole entries and "+N more" even when more rows would fit the chart: for a
    /// chart whose entries are one per file (File I/O), where every row would otherwise be legend.
    /// </summary>
    internal bool OneRow { get; init; }

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

    /// <summary>
    /// #8220: the height of the legend column (padding included) of the last layout that put the legend beside the plot. A row that holds a side
    /// legend never shrinks below the chrome plus this, or the column would no longer fit and the next layout would move the legend (<see cref="ChartLayout.RowMinHeightFor(double, double)"/>).
    /// </summary>
    internal float SideColumnHeight { get; private set; }

    public Layout GetLayout(PixelRect figureRect, Plot plot, Paint paint)
    {
        ShowAsNamed();
        _all = Entries(plot);
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
            else
            {
                ShowFitted();
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
        _fitted.Clear();
        var under = Place(figureRect, plot, paint, Edge.Bottom, compact: false, fit: 0, spare: 0f);
        /* #8540: an entry wider than the row under the plot is fitted to that row first, so every placement tried below works with
           entries that fit. Lines are kept when the plot keeps its floor with them, or when it is under its floor either way. */
        if (FitWideEntries(under.Layout.DataRect.Width, paint, oneLine: false))
        {
            var plain = under.Layout.DataRect.Height;
            under = Place(figureRect, plot, paint, Edge.Bottom, compact: false, fit: 0, spare: 0f);
            if (under.Layout.DataRect.Height < MinPlotHeight && plain >= MinPlotHeight)
            {
                ShowAsNamed();
                _fitted.Clear();
                FitWideEntries(under.Layout.DataRect.Width, paint, oneLine: true);
                under = Place(figureRect, plot, paint, Edge.Bottom, compact: false, fit: 0, spare: 0f);
            }
        }

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

        if (underFits && !OneRow) return (Edge.Bottom, false, 0);

        /* One row. The data rectangle's width does not depend on the legend's height, so any layout gives the width the row wraps at.
           A legend that already fits one row has nothing to leave out. */
        Apply(Edge.Bottom, compact: false, fit: 0); // the side trial above left the legend on the right
        var width = under.Layout.DataRect.Width;
        var oneRow = RowHeight(width, paint);
        if (under.LegendHeight <= oneRow + _panel.Padding.Vertical + 0.5f) return (Edge.Bottom, false, 0);
        return (Edge.Bottom, true, FitOneRow(width, oneRow, paint));
    }

    /// <summary>The entries the plottables give the legend, read behind any list of its own this engine left the legend showing.</summary>
    private static LegendItem[] Entries(Plot plot)
    {
        var legend = plot.Legend;
        if (legend.DisplayPlottableLegendItems) return legend.GetItems();

        var shown = legend.ManualItems.ToArray();
        legend.ManualItems.Clear();
        legend.DisplayPlottableLegendItems = true;
        try
        {
            return legend.GetItems();
        }
        finally
        {
            legend.DisplayPlottableLegendItems = false;
            legend.ManualItems.AddRange(shown);
        }
    }

    /// <summary>
    /// Fits every entry wider than <paramref name="width"/> to it and remembers the fitted text for the layouts that follow. The entry is
    /// broken across lines at its spaces; with <paramref name="oneLine"/> it keeps one line, cut to the width and ending in an ellipsis.
    /// A single word wider than the legend keeps a line of its own, cut to the width and ending in an ellipsis. True when an entry was fitted.
    /// </summary>
    private bool FitWideEntries(float width, Paint paint, bool oneLine)
    {
        foreach (var item in _all)
        {
            var text = item.LabelText;
            if (EntryWidth(item, paint) <= width + 0.5f) continue;

            if (oneLine)
            {
                _fitted[text] = Shorten(item, text, width, paint);
                item.LabelText = text;
                continue;
            }

            var words = text.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var lines = new List<string>();
            var line = words.Length > 0 ? words[0] : text;
            foreach (var word in words.Skip(1))
            {
                item.LabelText = line + " " + word;
                if (EntryWidth(item, paint) <= width + 0.5f)
                {
                    line = item.LabelText;
                    continue;
                }

                lines.Add(line);
                line = word;
            }

            lines.Add(line);

            // A line that is one word wider than the row (a name with no space in it) is cut to the row and ends in an ellipsis.
            for (var i = 0; i < lines.Count; i++)
            {
                item.LabelText = lines[i];
                if (EntryWidth(item, paint) > width + 0.5f) lines[i] = Shorten(item, lines[i], width, paint);
            }

            item.LabelText = text;
            var fitted = string.Join("\n", lines);
            if (!string.Equals(fitted, text, StringComparison.Ordinal)) _fitted[text] = fitted;
        }

        ShowFitted();
        if (_fitted.Count == 0) Apply(Edge.Bottom, compact: false, fit: 0); // measuring left one entry in the legend's list
        return _fitted.Count > 0;
    }

    /// <summary>Ends an entry, or a line of one, that does not show its whole text.</summary>
    internal const string Ellipsis = "\u2026";

    /// <summary>The longest start of <paramref name="text"/> that, with an ellipsis after it, is no wider than <paramref name="width"/> as the entry's label.</summary>
    private string Shorten(LegendItem item, string text, float width, Paint paint)
    {
        var keep = text.Length - 1;
        for (; keep > 1; keep--)
        {
            item.LabelText = text[..keep].TrimEnd() + Ellipsis;
            if (EntryWidth(item, paint) <= width + 0.5f) break;
        }

        return text[..Math.Max(keep, 1)].TrimEnd() + Ellipsis;
    }

    /// <summary>
    /// The text the plottable named <paramref name="item"/> when the legend shows it fitted (broken across lines, or shortened), else null.
    /// The hover reads it to show the whole text of an entry that ends in an ellipsis.
    /// </summary>
    internal string? NamedText(LegendItem item) => _named.TryGetValue(item, out var named) ? named : null;

    /// <summary>The width one entry takes on a row of its own: its symbol and its label.</summary>
    private float EntryWidth(LegendItem item, Paint paint)
    {
        var legend = _panel.Legend;
        legend.DisplayPlottableLegendItems = false;
        legend.ManualItems.Clear();
        legend.ManualItems.Add(item);
        return legend.GetLayout(new PixelSize(float.PositiveInfinity, float.PositiveInfinity), paint).LegendRect.Width;
    }

    /// <summary>Gives each entry that has a fitted text that text.</summary>
    private void ShowFitted()
    {
        foreach (var item in _all)
        {
            if (!_fitted.TryGetValue(item.LabelText, out var fitted)) continue;
            _named[item] = item.LabelText;
            item.LabelText = fitted;
        }
    }

    /// <summary>Puts every fitted entry back to the text its plottable named it, so an entry a plottable hands out again is read as named.</summary>
    private void ShowAsNamed()
    {
        foreach (var (item, text) in _named) item.LabelText = text;
        _named.Clear();
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
        // An entry broken across lines is taller than the rest: each row then takes its own height, not the tallest entry's.
        legend.Layout = _all.Any(i => i.LabelText.Contains('\n')) ? _ownRows : _stockRows;
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
        if (!_compact && _named.Count == 0)
        {
            legend.DisplayPlottableLegendItems = true;
            legend.ManualItems.Clear();
            return;
        }

        // The legend shows this engine's own list: the entries with their fitted text, or the first few and a "+N more" entry.
        legend.DisplayPlottableLegendItems = false;
        legend.ManualItems.Clear();
        if (!_compact)
        {
            legend.ManualItems.AddRange(_all);
            return;
        }

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
            SideColumnHeight = legendHeight;
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

/// <summary>
/// #8540: ScottPlot's wrapping legend gives every row the height of the tallest entry, so one entry on two lines makes every row two
/// lines tall. This layout takes ScottPlot's and gives each row the height of its own tallest entry, moving the rows below it up.
/// A legend whose rows are all one height comes back as ScottPlot laid it out.
/// </summary>
internal sealed class RowHeightLegendLayout : ILegendLayout
{
    private readonly ScottPlot.LegendLayouts.Wrapping _stock = new();

    public LegendLayout GetLayout(Legend legend, LegendItem[] items, PixelSize maxSize, Paint paint)
    {
        if (legend.Orientation != Orientation.Horizontal || items.Length < 2) return _stock.GetLayout(legend, items, maxSize, paint);

        /* ScottPlot's layout cuts an entry's rectangle at the height it is given, and that height is the one this layout's shorter
           rows need. So the rows are laid out with no height limit first, and take their own heights from there. */
        var laid = _stock.GetLayout(legend, items, new PixelSize(maxSize.Width, float.PositiveInfinity), paint);
        if (laid.LegendItems.Length < 2) return _stock.GetLayout(legend, items, maxSize, paint);

        var labels = (PixelRect[])laid.LabelRects.Clone();
        var symbols = (PixelRect[])laid.SymbolRects.Clone();
        var unbounded = new PixelSize(float.PositiveInfinity, float.PositiveInfinity);
        var saved = 0f;
        foreach (var row in Enumerable.Range(0, labels.Length).GroupBy(i => laid.LabelRects[i].Top).OrderBy(g => g.Key))
        {
            // The row's entries laid out alone are one row as tall as the tallest of them.
            var members = row.ToArray();
            var own = _stock.GetLayout(legend, members.Select(i => laid.LegendItems[i]).ToArray(), unbounded, paint).LabelRects.Max(r => r.Height);
            var given = laid.LabelRects[members[0]].Height;
            if (own > given) own = given;
            foreach (var i in members)
            {
                labels[i] = new PixelRect(new Pixel(labels[i].Left, labels[i].Top - saved), new PixelSize(labels[i].Width, own));
                symbols[i] = new PixelRect(new Pixel(symbols[i].Left, symbols[i].Top - saved), new PixelSize(symbols[i].Width, own));
            }

            saved += given - own;
        }

        if (saved < 0.5f) return _stock.GetLayout(legend, items, maxSize, paint);

        var whole = laid.LegendRect;
        return new LegendLayout
        {
            LegendItems = laid.LegendItems,
            LegendRect = new PixelRect(new Pixel(whole.Left, whole.Top), new PixelSize(whole.Width, whole.Height - saved)),
            LabelRects = labels,
            SymbolRects = symbols,
        };
    }
}

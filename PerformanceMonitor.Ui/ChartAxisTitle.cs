/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.CompilerServices;
using ScottPlot.WPF;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #6652: a left-axis title is drawn rotated along the plot's height, so on a short plot (1366x768, three
    /// or four stacked charts) a long title ("Lock Wait Time (ms/sec)") is cut at both ends and the unit is lost.
    /// <see cref="Fit"/> is the pure rule (shrink the font, then trim the name and keep the unit last);
    /// <see cref="Attach"/> applies it on every render, so every chart that goes through
    /// <see cref="ChartStyle.ApplyThemeToChart"/> gains it without touching its call sites.
    /// </summary>
    public static class ChartAxisTitle
    {
        public const double BaseFontSize = 12;
        public const double MinFontSize = 9;

        /// <summary>Scaled units (DIPs) of the figure height the bottom tick labels (two lines for a date), legend and padding take; measured at 250% scaling once the text drew at its real size (#6678). Over-reserving only trims a title earlier.</summary>
        public const double ReservedHeight = 112;

        /// <summary>Average glyph width as a fraction of the font size (sans-serif, mixed case and digits).</summary>
        private const double GlyphWidth = 0.6;

        private const int MaxUnitLength = 12;

        /// <summary>The title and font size that fit <paramref name="availablePx"/>; the title unchanged when nothing is known about the space.</summary>
        public static (string Text, double FontSize) Fit(string title, double availablePx)
        {
            if (string.IsNullOrEmpty(title) || !(availablePx > 0)) return (title, BaseFontSize);

            for (var size = BaseFontSize; size >= MinFontSize; size -= 1)
            {
                if (title.Length * GlyphWidth * size <= availablePx) return (title, size);
            }

            var max = (int)Math.Floor(availablePx / (GlyphWidth * MinFontSize));
            if (max < 4) return (title, MinFontSize);

            // "Name (unit)": trim the name and keep a short unit, the part that says what the numbers are. A long
            // parenthetical is a source note (BlockingSourceLabel), not a unit, so it is trimmed like the rest.
            var open = title.LastIndexOf(" (", StringComparison.Ordinal);
            if (open > 0 && title.EndsWith(')') && title.Length - open - 1 <= MaxUnitLength)
            {
                var unit = title[(open + 1)..];
                var room = max - unit.Length - 2;
                if (room >= 3) return (title[..room].TrimEnd() + "… " + unit, MinFontSize);
                return (title[^max..], MinFontSize);
            }

            return (title[..(max - 1)].TrimEnd() + "…", MinFontSize);
        }

        /// <summary>
        /// #6832: the rotated title is centred on the plot, and the top and bottom tick labels sit at the plot's ends, so a title
        /// longer than the plot minus one tick label's height runs into them (and out of the figure on a plot of 30 px). Scaled
        /// units: a 13 point tick label is about 16 tall, plus a few for its padding.
        /// </summary>
        public const double TickLabelClearance = 20;

        /// <summary>
        /// #6832: the left axis panel is at least this wide (scaled units): the title's height (a 12 point line, about 16), a tick
        /// mark, and a tick label of six characters at 13 points ("398.17", about 48), with some padding.
        /// </summary>
        public const double LeftPanelMinWidth = 80;

        /// <summary>
        /// The room (scaled units) the title may use: the plot's height, less <see cref="TickLabelClearance"/>. The plot's height
        /// is the figure's less <paramref name="chromeHeight"/>, the space the bottom ticks, legend and padding take as last
        /// measured (<see cref="ChartLayout.ChromeHeight"/>); <see cref="ReservedHeight"/> until a render has measured it.
        /// </summary>
        public static double AvailableFor(double figureHeight, double? chromeHeight) =>
            figureHeight - (chromeHeight ?? ReservedHeight) - TickLabelClearance;

        private sealed class State
        {
            public string? Original;
            public string? Applied;
            public double? UsedChrome;
            public bool RefitPending;
        }

        private static readonly ConditionalWeakTable<ScottPlot.Plot, State> s_state = new();

        /// <summary>Fits the chart's left-axis title to its height just before every render. Safe to call more than once per plot.</summary>
        public static void Attach(WpfPlot chart)
        {
            var plot = chart.Plot;
            if (s_state.TryGetValue(plot, out _)) return;
            var state = new State();
            s_state.Add(plot, state);
            plot.RenderManager.RenderStarting += (_, rp) =>
            {
                var label = plot.Axes.Left.Label;
                if (label.Text != state.Applied) state.Original = label.Text;
                if (string.IsNullOrEmpty(state.Original)) { state.Applied = null; return; }
                state.UsedChrome = ChartLayout.ChromeHeight(plot);
                var (text, size) = Fit(state.Original, AvailableFor(rp.ScaledFigureRect.Height, state.UsedChrome));
                label.Text = text;
                label.FontSize = (float)size;
                state.Applied = text;
            };
            // The first render of a chart fits with the estimate. Once it has measured the real chrome (a ten-entry legend is
            // three rows tall), one more render fits the title to the plot that is really there.
            plot.RenderManager.RenderFinished += (_, rp) =>
            {
                var measured = ChartLayout.ChromeHeight(plot);
                if (state.RefitPending || measured is null || string.IsNullOrEmpty(state.Original)) return;
                if (state.UsedChrome is { } used && Math.Abs(used - measured.Value) <= 3) return;
                state.RefitPending = true;
                chart.Dispatcher.BeginInvoke(() =>
                {
                    state.RefitPending = false;
                    if (ReferenceEquals(chart.Plot, plot)) chart.Refresh();
                }, System.Windows.Threading.DispatcherPriority.Background);
            };
        }
    }

    /// <summary>#6652: the stacked-chart layout floor the Viewer's chart tabs share.</summary>
    public static class ChartLayout
    {
        /// <summary>Smallest height a chart row may shrink to; below it the tab scrolls instead (a shorter plot cannot draw its ticks).</summary>
        public const double MinPlotHeight = 200;

        /// <summary>
        /// #6832: smallest height the plot area (the data rectangle, not the chart row) may have. The row minimum above is for a chart
        /// with a one-line legend. A ten-entry legend is three lines and a restart marker adds an entry, so the row grows by what the
        /// legend, the bottom ticks and the padding take, and the tab scrolls instead (<see cref="Attach"/>).
        /// </summary>
        public const double MinPlotAreaHeight = 100;

        /// <summary>The row height that leaves <see cref="MinPlotAreaHeight"/> for the plot when everything but the plot takes <paramref name="chromeHeight"/>.</summary>
        public static double RowMinHeightFor(double chromeHeight) => Math.Max(MinPlotHeight, chromeHeight + MinPlotAreaHeight);

        private sealed class Chrome
        {
            public double Height = double.NaN;
            public float FigureHeight;
            public bool FitPending;
        }

        private static readonly ConditionalWeakTable<ScottPlot.Plot, Chrome> s_chrome = new();
        private static readonly ConditionalWeakTable<System.Windows.Controls.RowDefinition, object> s_baseRowMin = new();
        private static readonly ConditionalWeakTable<System.Windows.Controls.Grid, object> s_baseGridMin = new();

        /// <summary>The height (scaled units) of everything in the figure but the plot area, from the plot's last render; null before the first.</summary>
        public static double? ChromeHeight(ScottPlot.Plot plot) =>
            s_chrome.TryGetValue(plot, out var c) && !double.IsNaN(c.Height) ? c.Height : null;

        /// <summary>
        /// Measures every render of the chart and, when the chart sits in a star row of a Grid whose XAML minimum is
        /// <see cref="MinPlotHeight"/> or more, raises that row's minimum to <see cref="RowMinHeightFor"/> and the Grid's own minimum
        /// to the rows' total, so the tab's scroll viewer scrolls instead of clipping the chart. A chart anywhere else is only measured.
        /// Safe to call more than once per plot. Rendering happens on the UI thread; the row is adjusted after it, at background priority.
        /// </summary>
        public static void Attach(WpfPlot chart)
        {
            var plot = chart.Plot;
            if (s_chrome.TryGetValue(plot, out _)) return;
            var chrome = new Chrome();
            s_chrome.Add(plot, chrome);
            plot.RenderManager.RenderStarting += (_, rp) => chrome.FigureHeight = rp.ScaledFigureRect.Height;
            plot.RenderManager.RenderFinished += (_, rp) =>
            {
                var measured = chrome.FigureHeight - rp.DataRect.Height;
                if (!(measured > 0) || double.IsInfinity(measured)) return;
                var changed = double.IsNaN(chrome.Height) || Math.Abs(chrome.Height - measured) > 0.5;
                chrome.Height = measured;
                if (!changed || chrome.FitPending) return;
                chrome.FitPending = true;
                chart.Dispatcher.BeginInvoke(() =>
                {
                    chrome.FitPending = false;
                    FitRow(chart);
                }, System.Windows.Threading.DispatcherPriority.Background);
            };
        }

        private static void FitRow(WpfPlot chart)
        {
            // WpfPlot -> Border -> Grid is how every chart tab hosts a chart; walk up to the Grid whichever the nesting.
            System.Windows.DependencyObject child = chart;
            System.Windows.Controls.Grid? grid = null;
            while (child is not null)
            {
                var parent = System.Windows.Media.VisualTreeHelper.GetParent(child);
                if (parent is System.Windows.Controls.Grid g) { grid = g; break; }
                child = parent;
            }
            if (grid is null || child is not System.Windows.FrameworkElement host) return;

            var row = System.Windows.Controls.Grid.GetRow(host);
            if (row < 0 || row >= grid.RowDefinitions.Count) return;
            var definition = grid.RowDefinitions[row];
            var baseMin = (double)s_baseRowMin.GetValue(definition, d => ((System.Windows.Controls.RowDefinition)d).MinHeight);
            if (baseMin < MinPlotHeight || !definition.Height.IsStar) return;

            var need = baseMin;
            foreach (System.Windows.UIElement sibling in grid.Children)
            {
                if (System.Windows.Controls.Grid.GetRow(sibling) != row) continue;
                var plot = ((sibling as System.Windows.Controls.Border)?.Child as WpfPlot ?? sibling as WpfPlot)?.Plot;
                // The Border around the chart has a margin between charts, and that is part of the row, not of the chart.
                var margin = sibling is System.Windows.FrameworkElement fe ? fe.Margin.Top + fe.Margin.Bottom : 0;
                if (plot is not null && ChromeHeight(plot) is { } h) need = Math.Max(need, Math.Ceiling(RowMinHeightFor(h) + margin)); // whole pixels: the chart renders at its pixel size, truncated
            }
            if (Math.Abs(definition.MinHeight - need) > 0.5) definition.MinHeight = need;

            var baseGridMin = (double)s_baseGridMin.GetValue(grid, g => ((System.Windows.Controls.Grid)g).MinHeight);
            double total = 0;
            foreach (var d in grid.RowDefinitions)
            {
                if (d.Height.IsStar) total += d.MinHeight;
                else if (d.Height.IsAbsolute) total += d.Height.Value;
                else total += d.ActualHeight;
            }
            var gridMin = Math.Max(baseGridMin, total);
            if (Math.Abs(grid.MinHeight - gridMin) > 0.5) grid.MinHeight = gridMin;
        }

        /// <summary>
        /// A chart inside a vertical <see cref="System.Windows.Controls.ScrollViewer"/> keeps the mouse wheel for its own
        /// zoom: the wheel over the plot never also scrolls the tab (the scroll bar and the area around the plots scroll it).
        /// Runs after ScottPlot's own wheel handler, so the zoom still happens.
        /// </summary>
        public static void KeepWheelOnPlot(params WpfPlot[] charts)
        {
            foreach (var chart in charts)
                chart.MouseWheel += (_, e) => e.Handled = true;
        }
    }
}

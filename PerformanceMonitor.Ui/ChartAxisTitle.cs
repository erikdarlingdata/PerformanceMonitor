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

        /// <summary>Pixels of the figure height the bottom tick labels, bottom title and padding take.</summary>
        public const double ReservedHeight = 60;

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

        private sealed class State
        {
            public string? Original;
            public string? Applied;
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
                var (text, size) = Fit(state.Original, rp.ScaledFigureRect.Height - ReservedHeight);
                label.Text = text;
                label.FontSize = (float)size;
                state.Applied = text;
            };
        }
    }

    /// <summary>#6652: the stacked-chart layout floor the Viewer's chart tabs share.</summary>
    public static class ChartLayout
    {
        /// <summary>Smallest height a chart row may shrink to; below it the tab scrolls instead (a shorter plot cannot draw its ticks).</summary>
        public const double MinPlotHeight = 150;

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

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Media;
using ScottPlot.WPF;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// The per-theme chart CHROME colors (Dark / Light / CoolBreeze). The single source of truth
    /// for these hex values — every chart, in both apps, resolves them through
    /// <see cref="ChartStyle.GetThemeColors"/> so they can never drift between copies again.
    /// </summary>
    public readonly record struct ChartThemeColors(
        ScottPlot.Color FigureBackground,
        ScottPlot.Color DataBackground,
        ScottPlot.Color Text,
        ScottPlot.Color Grid,
        ScottPlot.Color LegendBackground,
        ScottPlot.Color LegendForeground,
        ScottPlot.Color LegendOutline);

    /// <summary>
    /// Shared CHROME styling for ScottPlot charts — the single source of truth for chart theming
    /// across Dashboard and Lite. "Chrome" = figure/data backgrounds, axis + grid + legend colors,
    /// tick label colors/sizes, and axis-mechanics helpers. It deliberately does NOT own series /
    /// category COLOR IDENTITY (that is a separate concern; see the ChartPalette work in A.3).
    ///
    /// Theme colors read <see cref="ThemeManager.CurrentTheme"/> (Dark / Light / CoolBreeze).
    /// </summary>
    public static class ChartStyle
    {
        /// <summary>Resolves the chrome colors for the currently active theme.</summary>
        public static ChartThemeColors GetThemeColors()
        {
            if (ThemeManager.CurrentTheme == "CoolBreeze")
                return new ChartThemeColors(
                    ScottPlot.Color.FromHex("#EEF4FA"),
                    ScottPlot.Color.FromHex("#DAE6F0"),
                    ScottPlot.Color.FromHex("#1A2A3A"),
                    ScottPlot.Color.FromHex("#A8BDD0").WithAlpha(120),
                    ScottPlot.Color.FromHex("#EEF4FA"),
                    ScottPlot.Color.FromHex("#1A2A3A"),
                    ScottPlot.Color.FromHex("#A8BDD0"));

            if (ThemeManager.HasLightBackground)
                return new ChartThemeColors(
                    ScottPlot.Color.FromHex("#FFFFFF"),
                    ScottPlot.Color.FromHex("#F5F7FA"),
                    ScottPlot.Color.FromHex("#1A1D23"),
                    ScottPlot.Colors.Black.WithAlpha(20),
                    ScottPlot.Color.FromHex("#FFFFFF"),
                    ScottPlot.Color.FromHex("#1A1D23"),
                    ScottPlot.Color.FromHex("#DEE2E6"));

            return new ChartThemeColors(
                ScottPlot.Color.FromHex("#22252b"),
                ScottPlot.Color.FromHex("#111217"),
                ScottPlot.Color.FromHex("#E4E6EB"),
                ScottPlot.Colors.White.WithAlpha(40),
                ScottPlot.Color.FromHex("#22252b"),
                ScottPlot.Color.FromHex("#E4E6EB"),
                ScottPlot.Color.FromHex("#2a2d35"));
        }

        /// <summary>
        /// Applies the full chrome theme to a ScottPlot chart (backgrounds, axis/grid/legend
        /// colors, bottom-horizontal legend, tick label colors + 13px font, and a first-render
        /// hook to avoid a white flash). Use for the standard multi-series trend charts.
        /// </summary>
        public static void ApplyThemeToChart(WpfPlot chart)
        {
            var c = GetThemeColors();

            chart.Plot.FigureBackground.Color = c.FigureBackground;
            chart.Plot.DataBackground.Color = c.DataBackground;
            chart.Plot.Axes.Color(c.Text);
            chart.Plot.Grid.MajorLineColor = c.Grid;
            chart.Plot.Legend.BackgroundColor = c.LegendBackground;
            chart.Plot.Legend.FontColor = c.LegendForeground;
            chart.Plot.Legend.OutlineColor = c.LegendOutline;
            chart.Plot.Legend.Alignment = ScottPlot.Alignment.LowerCenter;
            chart.Plot.Legend.Orientation = ScottPlot.Orientation.Horizontal;
            chart.Plot.Axes.Margins(bottom: 0); // No bottom margin - SetChartYLimitsWithLegendPadding handles Y-axis

            // Explicitly set axis tick label colors (needed after DateTimeTicksBottom() is called)
            chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = c.Text;
            chart.Plot.Axes.Left.TickLabelStyle.ForeColor = c.Text;
            chart.Plot.Axes.Bottom.Label.ForeColor = c.Text;
            chart.Plot.Axes.Left.Label.ForeColor = c.Text;
            chart.Plot.Axes.Bottom.TickLabelStyle.FontSize = 13;
            chart.Plot.Axes.Left.TickLabelStyle.FontSize = 13;

            // Set the WPF control Background to match so no white flash appears before ScottPlot's render loop fires
            chart.Background = new SolidColorBrush(Color.FromRgb(c.FigureBackground.R, c.FigureBackground.G, c.FigureBackground.B));

            // Ensure ScottPlot renders with the correct colors the very first time it gets pixel dimensions.
            // Without this, ScottPlot's first auto-render (triggered by SizeChanged) would show a white canvas
            // before our FigureBackground color takes visual effect.
            chart.Loaded -= HandleChartFirstLoaded;
            if (!chart.IsLoaded)
                chart.Loaded += HandleChartFirstLoaded;
        }

        /// <summary>
        /// Minimal chrome for simple single-series charts (e.g. the history dialogs): backgrounds,
        /// axis, grid, and tick label colors only — no legend handling, axis-margin override,
        /// font-size override, background brush, or first-render hook. Sources colors from
        /// <see cref="GetThemeColors"/> so the per-theme hexes can't drift from the full theme.
        /// </summary>
        public static void ApplyMinimalChartTheme(WpfPlot chart)
        {
            var c = GetThemeColors();
            chart.Plot.FigureBackground.Color = c.FigureBackground;
            chart.Plot.DataBackground.Color = c.DataBackground;
            chart.Plot.Axes.Color(c.Text);
            chart.Plot.Grid.MajorLineColor = c.Grid;
            chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = c.Text;
            chart.Plot.Axes.Left.TickLabelStyle.ForeColor = c.Text;
        }

        private static void HandleChartFirstLoaded(object sender, RoutedEventArgs e)
        {
            var chart = (WpfPlot)sender;
            chart.Loaded -= HandleChartFirstLoaded;
            chart.Refresh();
        }

        /// <summary>
        /// Reapplies theme-appropriate text colors (and 13px font) to chart axes.
        /// Call this AFTER DateTimeTicksBottom() or other axis modifications that reset them.
        /// </summary>
        public static void ReapplyAxisColors(WpfPlot chart)
        {
            var text = GetThemeColors().Text;
            chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = text;
            chart.Plot.Axes.Left.TickLabelStyle.ForeColor = text;
            chart.Plot.Axes.Bottom.Label.ForeColor = text;
            chart.Plot.Axes.Left.Label.ForeColor = text;
            chart.Plot.Axes.Bottom.TickLabelStyle.FontSize = 13;
            chart.Plot.Axes.Left.TickLabelStyle.FontSize = 13;
        }

        /// <summary>
        /// Locks the vertical axis so mouse-wheel zoom only affects the time (X) axis.
        /// Also reapplies axis colors (DateTimeTicksBottom() may have reset them).
        /// </summary>
        public static void LockChartVerticalAxis(WpfPlot chart)
        {
            var limits = chart.Plot.Axes.GetLimits();
            var rule = new ScottPlot.AxisRules.LockedVertical(
                chart.Plot.Axes.Left,
                limits.Bottom,
                limits.Top);
            chart.Plot.Axes.Rules.Clear();
            chart.Plot.Axes.Rules.Add(rule);

            ReapplyAxisColors(chart);
        }

        /// <summary>
        /// Sets Y-axis limits with padding for a bottom legend and top breathing room. Floors the axis
        /// at zero for non-negative data (the common metric case) so there is no magnitude-scaled
        /// dead-band below the lines; only genuinely-negative data gets a below-zero margin. Call this
        /// BEFORE LockChartVerticalAxis.
        /// </summary>
        public static void SetChartYLimitsWithLegendPadding(WpfPlot chart, double dataYMin = 0, double dataYMax = 0)
        {
            if (dataYMin == 0 && dataYMax == 0)
            {
                var limits = chart.Plot.Axes.GetLimits();
                dataYMin = limits.Bottom;
                dataYMax = limits.Top;
            }

            var (yMin, yMax) = ComputeYLimitsWithLegendPadding(dataYMin, dataYMax);
            chart.Plot.Axes.SetLimitsY(yMin, yMax);
        }

        /// <summary>
        /// Pure Y-limit math behind <see cref="SetChartYLimitsWithLegendPadding"/>, extracted so it can be
        /// unit-tested without a WPF control. Floors at zero for non-negative data so high-magnitude charts
        /// get no dead-band below the lines; only genuinely-negative data gets a 10%-of-range below-zero
        /// margin. Adds 15%-of-range top breathing room so a series that plateaus at a hard ceiling
        /// (e.g. CPU-scheduler task counts pinned at the scheduler count) does not crowd the top edge
        /// and read as clipped; a momentary peak simply gets a little more air above it.
        /// </summary>
        public static (double YMin, double YMax) ComputeYLimitsWithLegendPadding(double dataYMin, double dataYMax)
        {
            if (dataYMax <= dataYMin) dataYMax = dataYMin + 1;

            double range = dataYMax - dataYMin;
            double topPadding = range * 0.15;

            double yMin = dataYMin >= 0 ? 0 : dataYMin - (range * 0.10);
            double yMax = dataYMax + topPadding;

            return (yMin, yMax);
        }

        /// <summary>
        /// Applies the shared line-series polish to a <see cref="ScottPlot.Plottables.Scatter"/>:
        /// a consistent line width, a slight line transparency so overlapping series read clearly
        /// (markers stay fully opaque so peaks stay crisp), and a marker size scaled to point
        /// density — dense series collapse to a clean line, sparse series keep visible markers.
        /// Call this AFTER setting the scatter's <c>Color</c>. Deliberately typed to Scatter so
        /// Bars / lines / heatmaps can't be mis-fed. Sites that intentionally use MarkerSize 0
        /// (line-only reference lines) or a fixed anomaly marker (6) set their own and must NOT
        /// call this (the density rule would override their intent).
        /// </summary>
        public static void StyleScatter(ScottPlot.Plottables.Scatter scatter)
        {
            var pts = scatter.Data.GetScatterPoints();
            int pointCount = pts.Count;
            var seriesColor = scatter.LineColor;
            scatter.LineWidth = 2;
            scatter.MarkerSize = MarkerSizeForDensity(pointCount);
            // Soften the connecting line; markers stay fully opaque so peaks read clearly.
            scatter.LineColor = seriesColor.WithAlpha(215);
            // Gradient area fill — the "easy on the eye" PerformanceStudio look. Anchored to THIS
            // series' own data range [min, max] and filled to FillYValue = min (NOT zero): the fill is
            // a ribbon hugging the line, fading to transparent at the series' own floor — sensible for
            // both near-zero metrics (CPU) and high-baseline ones (memory), and the fade stops
            // overlapping multi-series fills piling into grey near the floor.
            //
            // CRITICAL: a gradient fill needs a real vertical span. A flat or empty series (e.g. a
            // tempdb version-store stuck at 0) gives a ZERO-HEIGHT fill rect, which collapses the two
            // gradient color stops onto a single pixel and makes ScottPlot throw "number of colors
            // must match the number of color positions" on EVERY render frame — an unhandled-exception
            // flood that crashes the app. So only fill when the data genuinely varies; otherwise leave
            // the line unfilled.
            /* NaN Ys are #1944's injected gap markers, and Enumerable.Min/Max PROPAGATE NaN - one gap
               would read as minY=maxY=NaN and silently kill the gradient fill for the entire series.
               Only real values rank. */
            var realYs = pts.Where(p => !double.IsNaN(p.Y)).Select(p => p.Y).ToList();
            double minY = realYs.Count > 0 ? realYs.Min() : 0.0;
            double maxY = realYs.Count > 0 ? realYs.Max() : 0.0;
            /* #2324: a series carrying gap markers gets NO fill at all — line-only. The earlier reading
               here ("the fill must survive a gap; ScottPlot splits it at the break") is true of the LINE
               and false of the FILL: reproduced headlessly against ScottPlot 5.1.59, one NaN in a
               FillY + ColorPositions series renders the ribbon as OPAQUE BLACK polygons with straight
               chord edges crossing the gap — Scatter.Render's fill path closes its SKPath contours
               through the break, and its fill paint under ColorPositions is hardcoded `Colors.Black`
               with the gradient shader expected to paint over it, which a NaN-bearing series defeats.
               That black buried every other series on 3.4.0's gapped charts (the field report's one
               healthy tab was the one whose data happened to have no gaps; 3.3.0 predates gap markers).
               Lines and markers handle NaN contours correctly, so line-only is the honest degradation:
               a chart showing an outage keeps its break, and continuous data keeps the full ribbon. */
            bool hasGapMarkers = realYs.Count != pointCount;
            bool canFill = pointCount >= 2
                && maxY > minY
                && !hasGapMarkers
                && !double.IsNaN(minY) && !double.IsNaN(maxY)
                && !double.IsInfinity(minY) && !double.IsInfinity(maxY);
            scatter.ColorPositions.Clear();
            if (canFill)
            {
                scatter.FillY = true;
                scatter.FillYValue = minY;
                scatter.AxisGradientDirection = ScottPlot.AxisGradientDirection.Vertical;
                scatter.ColorPositions.Add(new(seriesColor.WithAlpha(0), minY));
                scatter.ColorPositions.Add(new(seriesColor.WithAlpha(135), maxY));
            }
            else
            {
                scatter.FillY = false;
            }
        }

        /// <summary>
        /// Marker size chosen by point density (see <see cref="StyleScatter"/>). Public so callers
        /// that build scatters in a non-standard way can reuse the same density curve.
        /// </summary>
        public static float MarkerSizeForDensity(int pointCount) => pointCount switch
        {
            <= 1   => 7f,   // a lone sample has no line to anchor it — needs a visible dot
            <= 50  => 5f,   // sparse: full markers
            <= 120 => 3f,   // medium: small markers
            _      => 0f,   // dense: line only — markers would be a wall of dots
        };
    }
}

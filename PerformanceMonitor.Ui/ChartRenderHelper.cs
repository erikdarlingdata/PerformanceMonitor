/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The STATEFUL ScottPlot chart-render helpers shared by Lite's and Darling's per-server tabs: legend-panel
/// tracking (so a chart's bottom legend is removed before the next refresh instead of stacking) plus the
/// empty-state "No Data" chart. The stateless theme / axis-color / y-limit shapes stay as thin per-app
/// forwarders to the shared <see cref="ChartStyle"/>. (The deprecated Full Dashboard keeps its own copies.)
/// </summary>
public sealed class ChartRenderHelper
{
    private readonly Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();

    /// <summary>Clears a chart and removes any existing legend panel to prevent duplication.</summary>
    public void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        if (_legendPanels.TryGetValue(chart, out var existingPanel) && existingPanel != null)
        {
            chart.Plot.Axes.Remove(existingPanel);
            _legendPanels[chart] = null;
        }

        /* Reset fully — Plot.Clear() leaves stale DateTime axes behind,
           and DateTimeTicksBottom() replaces the axis object entirely.
           Resetting the plot object avoids tick generator type mismatches. */
        chart.Reset();
        ChartDisplayScale.Apply(chart); // #6678: Reset swaps in a plot at scale 1
        chart.Plot.Clear();
    }

    /// <summary>
    /// Shows the legend on a chart and tracks it for cleanup on the next refresh. #7385: the legend runs horizontally and
    /// wraps only when it does not fit the chart's width, so a chart pays for a second legend line only when it must (the
    /// default is one line per series). <see cref="WrappedLegendLayout"/> gives the panel the wrapped height in the same render.
    /// </summary>
    public void ShowChartLegend(ScottPlot.WPF.WpfPlot chart)
    {
        /* #8086: the edge is chosen from the control's size NOW, so the first render already has the right chrome and no
           extra pass is needed. A control with no size yet (a tab not shown) keeps the legend under the axis. */
        ChartStyle.FloorAllZeroYAxis(chart.Plot);
        var edge = LegendEdgeFor(chart.ActualWidth, chart.ActualHeight, chart.Plot.Legend.GetItems().Length);
        var panel = chart.Plot.ShowLegend(edge);
        _legendPanels[chart] = panel;
        var legend = chart.Plot.Legend;
        legend.FontSize = 13;
        // No frame and no fill: the legend sits on the figure's own background, outside the plot.
        legend.BackgroundColor = ScottPlot.Colors.Transparent;
        legend.OutlineColor = ScottPlot.Colors.Transparent;
        legend.OutlineWidth = 0;
        legend.ShadowColor = ScottPlot.Colors.Transparent;
        if (edge == ScottPlot.Edge.Right)
        {
            legend.Orientation = ScottPlot.Orientation.Vertical;
            panel.Alignment = ScottPlot.Alignment.MiddleLeft;
        }
        else
        {
            legend.Orientation = ScottPlot.Orientation.Horizontal;
            legend.TightHorizontalWrapping = true;
            /* ScottPlot pads a legend panel 20 px on every side, which is 40 px of blank under the legend (Session Stats at
               800x600: 90 of the chart's 286 px). The entries carry their own spacing, so 10 px above and below is enough and the
               plot gets the rest. */
            panel.Padding = new ScottPlot.PixelPadding(panel.Padding.Left, panel.Padding.Right, 10, 10);
        }

        chart.Plot.Layout.LayoutEngine = new WrappedLegendLayout(panel);
    }

    /// <summary>Most series a right-edge legend lists; more than this and the legend goes under the axis.</summary>
    internal const int MaxRightLegendSeries = 12;

    /// <summary>
    /// #8086: the right edge when the chart is at least three times as wide as it is tall and has at most
    /// <see cref="MaxRightLegendSeries"/> series (and the chart is tall enough to list them), else under the axis. Pure, so the
    /// rule can be tested without a control.
    /// </summary>
    internal static ScottPlot.Edge LegendEdgeFor(double width, double height, int series)
    {
        if (width <= 0 || height <= 0 || series < 1 || series > MaxRightLegendSeries) return ScottPlot.Edge.Bottom;
        /* A vertical legend is about 20 px a row at the 13 px font; one taller than the chart would be cut off. */
        if (series * 20.0 + 40 > height) return ScottPlot.Edge.Bottom;
        return width >= 3 * height ? ScottPlot.Edge.Right : ScottPlot.Edge.Bottom;
    }

    /// <summary>
    /// Records a legend panel a caller created directly (e.g. the Query Heatmap color bar) so
    /// <see cref="ClearChart"/> removes it on the next refresh.
    /// </summary>
    public void SetLegendPanel(ScottPlot.WPF.WpfPlot chart, ScottPlot.IPanel? panel) => _legendPanels[chart] = panel;

    /// <summary>
    /// Sets up an empty chart: one centered, muted sentence and nothing else (#8086): no axes, no grid, no data box and no legend.
    /// <see cref="ChartGroupAligner"/> leaves it out of its group, since there is no plot rectangle to share.
    /// </summary>
    public void RefreshEmptyChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel)
    {
        ChartStyle.ReapplyAxisColors(chart);

        var name = string.IsNullOrWhiteSpace(legendText) ? "data" : $"{legendText} data";
        var sentence = $"No {name} in this window";
        /* One sentence; on a narrow chart it breaks before the tail rather than running off the edge. */
        var label = sentence.Length > 36 ? $"No {name}\nin this window" : sentence;
        var text = chart.Plot.Add.Text(label, 0, 0);
        text.LabelFontColor = MutedTextColor();
        text.LabelFontSize = 14;
        text.LabelItalic = false;
        text.LabelAlignment = ScottPlot.Alignment.MiddleCenter;

        chart.Plot.Axes.SetLimitsX(-1, 1);
        chart.Plot.Axes.SetLimitsY(-1, 1);
        chart.Plot.YLabel(yAxisLabel);
        chart.Plot.HideAxesAndGrid();
        // The data area takes the figure's own color, so no box is left behind the sentence.
        chart.Plot.DataBackground.Color = chart.Plot.FigureBackground.Color;
        ChartGroupAligner.MarkEmpty(chart.Plot);

        chart.Refresh();
    }

    /// <summary>The theme's muted text color (the one the XAML empty hints use), or the palette's placeholder grey when no theme is loaded.</summary>
    private static ScottPlot.Color MutedTextColor()
    {
        if (System.Windows.Application.Current?.TryFindResource("ForegroundMutedBrush") is System.Windows.Media.SolidColorBrush brush)
        {
            var c = brush.Color;
            return new ScottPlot.Color(c.R, c.G, c.B, c.A);
        }

        return ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));
    }
}

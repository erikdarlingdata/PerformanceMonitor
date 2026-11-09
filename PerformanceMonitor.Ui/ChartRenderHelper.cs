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
    /// Shows the legend on a chart and tracks it for cleanup on the next refresh. #8220: where the legend goes is not decided here. The
    /// chart's <see cref="WrappedLegendLayout"/> applies the one legend rule on every layout, at the size the chart has then, so a chart
    /// drawn before it had a size, or resized later, still gets the right place (beside the plot, under it, or as one row).
    /// #7385: a legend under the plot wraps only when it does not fit the chart's width, so a chart pays for a second legend line only when it must.
    /// #8307: with <paramref name="oneRow"/> the legend under the plot keeps to one row and "+N more" whatever the chart's height.
    /// </summary>
    public void ShowChartLegend(ScottPlot.WPF.WpfPlot chart, bool oneRow = false)
    {
        ChartStyle.FloorAllZeroYAxis(chart.Plot);
        var panel = chart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
        _legendPanels[chart] = panel;
        var legend = chart.Plot.Legend;
        legend.FontSize = 13;
        // No frame and no fill: the legend sits on the figure's own background, outside the plot.
        legend.BackgroundColor = ScottPlot.Colors.Transparent;
        legend.OutlineColor = ScottPlot.Colors.Transparent;
        legend.OutlineWidth = 0;
        legend.ShadowColor = ScottPlot.Colors.Transparent;
        // The engine sets the edge, the orientation, the padding and the alignment for the side it picks, on every layout.
        chart.Plot.Layout.LayoutEngine = new WrappedLegendLayout(panel) { OneRow = oneRow };
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

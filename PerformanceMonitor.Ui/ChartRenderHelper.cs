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

    /// <summary>Shows the legend on a chart and tracks it for cleanup on the next refresh.</summary>
    public void ShowChartLegend(ScottPlot.WPF.WpfPlot chart)
    {
        _legendPanels[chart] = chart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
        chart.Plot.Legend.FontSize = 13;
    }

    /// <summary>
    /// Records a legend panel a caller created directly (e.g. the Query Heatmap color bar) so
    /// <see cref="ClearChart"/> removes it on the next refresh.
    /// </summary>
    public void SetLegendPanel(ScottPlot.WPF.WpfPlot chart, ScottPlot.IPanel? panel) => _legendPanels[chart] = panel;

    /// <summary>
    /// Sets up an empty chart with dark theme, Y-axis label, legend, and a centered "No Data" annotation.
    /// </summary>
    public void RefreshEmptyChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel)
    {
        ChartStyle.ReapplyAxisColors(chart);

        /* Add invisible scatter to create legend entry (matches data chart layout) */
        var placeholder = chart.Plot.Add.Scatter(new double[] { 0 }, new double[] { 0 });
        placeholder.LegendText = legendText;
        placeholder.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));
        placeholder.MarkerSize = 0;
        placeholder.LineWidth = 0;

        /* Add centered "No Data" text */
        var text = chart.Plot.Add.Text($"{legendText}\nNo Data", 0, 0);
        text.LabelFontColor = ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));
        text.LabelFontSize = 14;
        text.LabelAlignment = ScottPlot.Alignment.MiddleCenter;

        /* Configure axes */
        chart.Plot.HideGrid();
        chart.Plot.Axes.SetLimitsX(-1, 1);
        chart.Plot.Axes.SetLimitsY(-1, 1);
        chart.Plot.Axes.Bottom.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.Axes.Left.TickGenerator = new ScottPlot.TickGenerators.EmptyTickGenerator();
        chart.Plot.YLabel(yAxisLabel);

        /* Show legend to match data chart layout */
        ShowChartLegend(chart);
        chart.Refresh();
    }
}

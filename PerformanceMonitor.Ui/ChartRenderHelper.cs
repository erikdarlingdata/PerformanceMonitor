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
        => RefreshEmptyChart(chart, legendText, yAxisLabel, pinToPlotCenter: false);

    /// <summary>
    /// <see cref="RefreshEmptyChart(ScottPlot.WPF.WpfPlot, string, string)"/>, optionally with the sentence pinned to the plot's center
    /// in pixels (<paramref name="pinToPlotCenter"/>) instead of to the data point (0, 0). A chart whose X limits are set after this
    /// (the Overview lanes share one time ruler, set once every lane has drawn) would carry a data-anchored sentence off the screen.
    /// </summary>
    public void RefreshEmptyChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel, bool pinToPlotCenter)
    {
        var name = string.IsNullOrWhiteSpace(legendText) ? "data" : $"{legendText} data";
        var sentence = $"No {name} in this window";
        /* One sentence; on a narrow chart it breaks before the tail rather than running off the edge. */
        var label = sentence.Length > 36 ? $"No {name}\nin this window" : sentence;
        DrawNote(chart, label, yAxisLabel, pinToPlotCenter);
    }

    /// <summary>The sentence a chart shows between the moment its tab is picked and the moment its first read returns (#8647).</summary>
    public const string LoadingSentence = "Loading...";

    /// <summary>
    /// #8647: the empty look with <see cref="LoadingSentence"/> in place of the empty-state sentence, pinned to the plot's center.
    /// A chart whose first read has not returned has nothing to say about its window yet; "No data in this window" is true only
    /// after a read came back with no rows. The next <see cref="RefreshEmptyChart(ScottPlot.WPF.WpfPlot, string, string)"/> or a
    /// plot that was cleared and drawn replaces it, and <see cref="ResolveLoadingChart"/> turns a loading chart no read ever
    /// replaced into the empty look. <paramref name="legendText"/> and <paramref name="yAxisLabel"/> are what that empty look will use.
    /// </summary>
    public void RefreshLoadingChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel)
    {
        var note = DrawNote(chart, LoadingSentence, yAxisLabel, pinToPlotCenter: true);
        _loading.Remove(chart);
        _loading.Add(chart, new LoadingMark(note!, legendText, yAxisLabel));
    }

    /// <summary>
    /// #8647: the empty look for a chart whose list has nothing ticked ("No counters picked"). Not the "No data in this window"
    /// sentence: a read has not found the window empty, none was asked for.
    /// </summary>
    public void RefreshNothingPickedChart(ScottPlot.WPF.WpfPlot chart, string noun, string yAxisLabel)
        => DrawNote(chart, $"No {noun} picked", yAxisLabel, pinToPlotCenter: false);

    /// <summary>
    /// #8647: the look of a picker-driven chart (Perfmon counters, Wait Stats, Memory Clerks) when nothing is ticked, which is one of
    /// three different facts. The list has not loaded yet: the loading look, since nobody has picked or declined anything. The list
    /// loaded with no entries at all: the empty-state sentence, since there is nothing to pick. The list has entries and none is
    /// ticked: "No <paramref name="noun"/> picked".
    /// </summary>
    public void RefreshNothingToPlotChart(ScottPlot.WPF.WpfPlot chart, bool listLoaded, int listCount, string title, string noun, string yAxisLabel)
    {
        if (!listLoaded)
        {
            RefreshLoadingChart(chart, title, yAxisLabel);
        }
        else if (listCount == 0)
        {
            RefreshEmptyChart(chart, title, yAxisLabel);
        }
        else
        {
            RefreshNothingPickedChart(chart, noun, yAxisLabel);
        }
    }

    /// <summary>True while <paramref name="chart"/> still shows the sentence <see cref="RefreshLoadingChart"/> drew, that is, no read has drawn on it since.</summary>
    public bool IsShowingLoading(ScottPlot.WPF.WpfPlot chart)
        => _loading.TryGetValue(chart, out var mark) && chart.Plot.PlottableList.Contains(mark.Note);

    /// <summary>
    /// #8647: a loading chart that the pass which was meant to fill it ended without drawing on (the read failed, or the collector
    /// has nothing to say on this server) becomes the empty look, so "Loading..." never outlives the read it waited for.
    /// A chart a read drew on, or one that was never made to load, is left alone. Returns true when it changed the chart.
    /// </summary>
    public bool ResolveLoadingChart(ScottPlot.WPF.WpfPlot chart)
    {
        if (!_loading.TryGetValue(chart, out var mark))
        {
            return false;
        }

        if (!chart.Plot.PlottableList.Contains(mark.Note))
        {
            _loading.Remove(chart);
            return false;
        }

        RefreshEmptyChart(chart, mark.Legend, mark.YLabel, pinToPlotCenter: true);
        return true;
    }

    private sealed record LoadingMark(ScottPlot.Plottables.Annotation Note, string Legend, string YLabel);

    private readonly System.Runtime.CompilerServices.ConditionalWeakTable<ScottPlot.WPF.WpfPlot, LoadingMark> _loading = new();

    private ScottPlot.Plottables.Annotation? DrawNote(ScottPlot.WPF.WpfPlot chart, string label, string yAxisLabel, bool pinToPlotCenter)
    {
        ChartStyle.ReapplyAxisColors(chart);

        /* A loading sentence still on the plot goes first, so the two sentences are never drawn over each other. */
        if (_loading.TryGetValue(chart, out var loading))
        {
            chart.Plot.Remove(loading.Note);
            _loading.Remove(chart);
        }

        ScottPlot.Plottables.Annotation? pinned = null;
        if (pinToPlotCenter)
        {
            var note = chart.Plot.Add.Annotation(label, ScottPlot.Alignment.MiddleCenter);
            note.LabelFontSize = 14;
            note.LabelFontColor = MutedTextColor();
            note.LabelBackgroundColor = ScottPlot.Colors.Transparent;
            note.LabelBorderColor = ScottPlot.Colors.Transparent;
            note.LabelBorderWidth = 0;
            note.LabelShadowColor = ScottPlot.Colors.Transparent;
            pinned = note;
        }
        else
        {
            var text = chart.Plot.Add.Text(label, 0, 0);
            text.LabelFontColor = MutedTextColor();
            text.LabelFontSize = 14;
            text.LabelItalic = false;
            text.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
        }

        chart.Plot.Axes.SetLimitsX(-1, 1);
        chart.Plot.Axes.SetLimitsY(-1, 1);
        chart.Plot.YLabel(yAxisLabel);
        chart.Plot.HideAxesAndGrid();
        // The data area takes the figure's own color, so no box is left behind the sentence.
        chart.Plot.DataBackground.Color = chart.Plot.FigureBackground.Color;
        ChartGroupAligner.MarkEmpty(chart.Plot);

        chart.Refresh();
        return pinned;
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

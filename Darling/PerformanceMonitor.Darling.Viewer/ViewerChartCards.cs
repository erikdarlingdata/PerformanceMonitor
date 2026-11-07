/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8151: the Viewer's charts sit in <see cref="ChartPanel"/> cards. The card carries the chart's name, so the Y axis shows only
/// the unit; a chart whose plotted values are all zero says so in the card's header slot instead of drawing a flat line with no
/// explanation.
/// </summary>
internal static class ViewerChartCards
{
    /// <summary>The header mark of a chart whose every plotted value is zero.</summary>
    internal const string ZeroThroughoutText = "0 throughout";

    private static readonly ConditionalWeakTable<ScottPlot.Plot, object> s_attached = new();

    /// <summary>The card a chart sits in, or null for a chart that is not in one.</summary>
    internal static ChartPanel? CardOf(WpfPlot chart)
    {
        for (DependencyObject? up = chart; up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is ChartPanel card) return card;
        }

        return null;
    }

    /// <summary>Starts the "0 throughout" mark on every chart under <paramref name="root"/> that sits in a card. Safe to call more than once.</summary>
    internal static void Attach(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            if (child is WpfPlot chart) AttachChart(chart);
            else Attach(child);
        }
    }

    private static void AttachChart(WpfPlot chart)
    {
        var plot = chart.Plot;
        if (CardOf(chart) is null || s_attached.TryGetValue(plot, out _)) return;
        s_attached.Add(plot, new object());
        plot.RenderManager.RenderFinished += (_, _) => ShowZeroMark(chart);
    }

    /// <summary>
    /// True when the plot draws at least one series and every series' values are zero (the same reading
    /// <see cref="ChartStyle.FloorAllZeroYAxis"/> takes). A chart with nothing drawn is empty, not zero.
    /// </summary>
    internal static bool IsZeroThroughout(ScottPlot.Plot plot)
    {
        var seen = false;
        foreach (var plottable in plot.PlottableList)
        {
            if (!plottable.LegendItems.Any()) continue;
            var limits = plottable.GetAxisLimits();
            if (!double.IsFinite(limits.Bottom) || !double.IsFinite(limits.Top)) continue;
            if (limits.Bottom != 0 || limits.Top != 0) return false;
            seen = true;
        }

        return seen;
    }

    private static void ShowZeroMark(WpfPlot chart)
    {
        if (CardOf(chart) is not { } card) return;
        var zero = IsZeroThroughout(chart.Plot);
        var shown = card.HeaderContent is TextBlock;
        if (zero == shown) return;
        card.HeaderContent = zero
            ? new TextBlock { Text = ZeroThroughoutText, Style = card.TryFindResource("PanelSubtitleText") as Style, VerticalAlignment = VerticalAlignment.Center }
            : null;
    }

    /// <summary>Sets the chart's Y axis title to its unit after a shared renderer drew it with a name, and draws it again.</summary>
    internal static void SetUnit(WpfPlot chart, string unit)
    {
        chart.Plot.YLabel(unit);
        chart.Refresh();
    }

    /// <summary>The card's subtitle (a note about where the numbers came from); empty clears it. A chart in no card has nowhere to show one.</summary>
    internal static void SetSubtitle(WpfPlot chart, string? text)
    {
        if (CardOf(chart) is { } card) card.Subtitle = text ?? string.Empty;
    }
}

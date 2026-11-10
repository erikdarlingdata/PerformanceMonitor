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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;
using ScottPlot;
using ScottPlot.Panels;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8540: the restart marker's legend entry on a Performance Trends chart is never cut. The chart is built the way Lite's trend
/// charts build theirs (one series, the marker with the shared sentence, the legend the app shows) and rendered 360 px wide, no wider than
/// a trend chart in a 900x600 window, at every height from 200 to 380 px. What the legend showed is read from the render.
/// </summary>
public sealed class TrendRestartLegendTests
{
    private const int ChartWidth = 360;
    private const string Ellipsis = "…";

    private static readonly string Sentence =
        BaselineDiscontinuities.Sentence(new DateTime(2026, 11, 8, 12, 40, 0), BaselineDiscontinuities.RestartReason);

    private static WpfPlot TrendChart(string series)
    {
        var chart = new WpfPlot();
        ChartStyle.ApplyThemeToChart(chart);
        var t0 = new DateTime(2026, 11, 8).ToOADate();
        var xs = Enumerable.Range(0, 48).Select(k => t0 + (k / 48.0)).ToArray();
        var line = chart.Plot.Add.Scatter(xs, xs.Select(x => (x - t0) * 100).ToArray());
        line.LegendText = series;
        ChartStyle.AddDiscontinuityMarker(chart, xs[25], Sentence);
        chart.Plot.Axes.DateTimeTicksBottom();
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.YLabel("ms/sec");
        ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, 100);
        new ChartRenderHelper().ShowChartLegend(chart);
        return chart;
    }

    /// <summary>Renders the chart at the height given and returns how the legend showed the marker's entry: "whole", "ellipsis" or "more".
    /// Adds one line to <paramref name="problems"/> for each entry wider than the legend's row, and for a marker entry in no such form.</summary>
    private static string Render(WpfPlot chart, int height, List<string> problems)
    {
        var plot = chart.Plot;
        for (var pass = 0; pass < 2; pass++)
        {
            using var image = plot.GetImage(ChartWidth, height);
        }

        var last = plot.RenderManager.LastRender;
        var panel = plot.Axes.GetPanels().OfType<LegendPanel>().Single();
        using var paint = Paint.NewDisposablePaint();
        var rect = panel.GetPanelRect(last.Layout.DataRect, last.Layout.PanelSizes[panel], last.Layout.PanelOffsets[panel], paint);
        var room = panel.Edge == Edge.Right ? rect.Width - panel.Padding.Horizontal : rect.Width;

        // Each entry's own width: its symbol and its label, laid out with no limit.
        var shown = plot.Legend.GetItems();
        var free = plot.Legend.GetLayout(new PixelSize(float.PositiveInfinity, float.PositiveInfinity), paint);
        for (var i = 0; i < shown.Length; i++)
        {
            var needs = free.SymbolRects[i].Width + free.LabelRects[i].Width;
            if (needs > room + 0.5f)
            {
                problems.Add($"at {ChartWidth}x{height} the legend entry \"{shown[i].LabelText}\" needs {needs:0} px in a {room:0} px legend row, so it is cut");
            }
        }

        var marker = shown.Select(i => i.LabelText.Replace('\n', ' ')).FirstOrDefault(t => t.StartsWith("SQL Server restarted", StringComparison.Ordinal));
        if (marker is null)
        {
            if (!shown.Any(i => i.LabelText.StartsWith('+')))
            {
                problems.Add($"at {ChartWidth}x{height} the legend shows neither the marker's entry nor a \"+N more\" entry: {string.Join(" / ", shown.Select(i => i.LabelText))}");
            }

            return "more";
        }

        if (marker == Sentence)
        {
            return "whole";
        }

        if (marker.EndsWith(Ellipsis, StringComparison.Ordinal) && Sentence.StartsWith(marker[..^Ellipsis.Length], StringComparison.Ordinal))
        {
            return "ellipsis";
        }

        problems.Add($"at {ChartWidth}x{height} the marker's entry reads \"{marker}\", neither the whole sentence nor its start and an ellipsis");
        return "other";
    }

    [Theory]
    [InlineData("Query Duration")]
    [InlineData("Procedure Duration")]
    public void TheRestartSentence_IsNeverCut_OnANarrowTrendChart(string series)
    {
        var (problems, forms) = StaTestThread.Run(() =>
        {
            var found = new List<string>();
            var seen = new SortedSet<string>(StringComparer.Ordinal);
            var chart = TrendChart(series);
            for (var height = 200; height <= 380; height += 2)
            {
                seen.Add(Render(chart, height, found));
            }

            return (found, seen);
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems.Take(5)) + $"{Environment.NewLine}({problems.Count} in all)");
        // With room for its lines the sentence shows whole; where the lines would take the plot under its floor it keeps one line and an ellipsis.
        Assert.Contains("whole", forms);
        Assert.Contains("ellipsis", forms);
    }

    [Theory]
    [InlineData("Query Duration")]
    [InlineData("Procedure Duration")]
    public void TheRestartSentence_ShowsWholeAcrossLines_WhenTheChartHasTheHeight(string series)
    {
        var (problems, entry) = StaTestThread.Run(() =>
        {
            var found = new List<string>();
            var chart = TrendChart(series);
            Render(chart, 380, found);
            var text = chart.Plot.Legend.GetItems().Select(i => i.LabelText).FirstOrDefault(t => t.StartsWith("SQL Server restarted", StringComparison.Ordinal)) ?? "";
            return (found, text);
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        Assert.Contains("\n", entry, StringComparison.Ordinal);
        Assert.Equal(Sentence, entry.Replace('\n', ' '));
    }
}

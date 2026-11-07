/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using ScottPlot;
using ScottPlot.WPF;

namespace Darling.Tests;

/// <summary>
/// #7478: measures every chart on a real server tab at a real window size. Shared by the Viewer and Lite tests (Lite.Tests links this
/// file). The probe finds charts by walking the tree, so a chart added to a tab is measured with no change here. It reads the layout
/// ScottPlot produced for the render the control would have made, never the source.
/// </summary>
internal static class ChartLayoutProbe
{
    /// <summary>Window sizes (DIPs) and display scales the tab is measured at.</summary>
    public static readonly (int Width, int Height, double Dpi)[] Windows =
    {
        (800, 600, 1.0), (1280, 720, 2.0), (1920, 1080, 1.0), (3840, 2160, 2.0),
    };

    /// <summary>The share of the axis a gap between the axis start and the first plotted point may take.</summary>
    internal const double MaxLeadingGapShare = 0.12;

    public static IEnumerable<T> Logical<T>(DependencyObject root) where T : DependencyObject
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            if (child is T hit)
            {
                yield return hit;
            }

            foreach (var inner in Logical<T>(child))
            {
                yield return inner;
            }
        }
    }

    /// <summary>A name the failure message can point at: the x:Name, else the type and the chart's Y label.</summary>
    public static string NameOf(WpfPlot chart) =>
        !string.IsNullOrEmpty(chart.Name) ? chart.Name : $"(unnamed chart, Y label '{chart.Plot.Axes.Left.Label.Text}')";

    /// <summary>The tab items between the element and the root, outermost first.</summary>
    public static List<TabItem> TabPath(DependencyObject element)
    {
        var path = new List<TabItem>();
        for (var at = LogicalTreeHelper.GetParent(element); at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem tab)
            {
                path.Insert(0, tab);
            }
        }

        return path;
    }

    /// <summary>The tab names a failure message uses, e.g. "Memory > Grants".</summary>
    public static string TabLabel(IEnumerable<TabItem> path) =>
        string.Join(" > ", path.Select(t => t.Header as string ?? t.Name)).Trim();

    public static void Reveal(IEnumerable<TabItem> path)
    {
        foreach (var tab in path)
        {
            if (ItemsControl.ItemsControlFromItemContainer(tab) is TabControl owner)
            {
                owner.SelectedItem = tab;
            }
        }
    }

    public static void Layout(Window window)
    {
        for (var pass = 0; pass < 3; pass++)
        {
            window.UpdateLayout();
            Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
        }
    }

    public static void Render(WpfPlot chart)
    {
        var w = (int)(chart.ActualWidth * chart.DisplayScale);
        var h = (int)(chart.ActualHeight * chart.DisplayScale);
        if (w < 1 || h < 1)
        {
            return;
        }

        using var image = chart.Plot.GetImage(w, h);
    }

    /// <summary>The way the app gets there: charts render, the group debounce fires, the charts that changed render again.</summary>
    public static void Settle(Window window, IReadOnlyList<WpfPlot> charts)
    {
        for (var round = 0; round < 3; round++)
        {
            foreach (var chart in charts)
            {
                Render(chart);
            }

            foreach (var group in charts.Select(c => ChartGroupAligner.FindGroup(c)).Where(g => g is not null).Distinct())
            {
                ChartGroupAligner.Check(group!);
            }

            Layout(window);
        }

        foreach (var chart in charts)
        {
            Render(chart);
        }
    }

    public static bool IsShown(WpfPlot chart) =>
        chart.IsVisible && chart.ActualWidth > 1 && chart.ActualHeight > 1;

    /// <summary>The earliest X of the chart's drawn data. Reference lines and spans are left out: ScottPlot reports an unset X for
    /// them (a vertical span, a horizontal line), or their own X (a horizontal span, a vertical line at 0), and none of them is a
    /// plotted point.</summary>
    public static double? FirstPlottedX(Plot plot)
    {
        double? first = null;
        foreach (var plottable in plot.PlottableList)
        {
            if (plottable is not (ScottPlot.Plottables.Scatter or ScottPlot.Plottables.Signal or ScottPlot.Plottables.SignalXY
                or ScottPlot.Plottables.BarPlot or ScottPlot.Plottables.FillY))
            {
                continue;
            }

            var left = plottable.GetAxisLimits().Left;
            if (double.IsFinite(left) && (first is null || left < first))
            {
                first = left;
            }
        }

        return first;
    }

    /// <summary>Measures one chart after the tab settled. Returns one line per defect, empty when the chart is sound.</summary>
    public static List<string> Measure(WpfPlot chart, double? firstDataOa = null)
    {
        var problems = new List<string>();
        var plot = chart.Plot;
        var last = plot.RenderManager.LastRender;
        var data = last.DataRect;
        var figureH = chart.ActualHeight;
        var figureW = chart.ActualWidth;

        // 1. Height: the plot keeps a floor and a healthy share of the chart.
        if (data.Height < ChartLayout.MinPlotAreaHeight - 0.5)
        {
            problems.Add($"plot is {data.Height:0.#} px tall in a {figureH:0.#} px chart, under the {ChartLayout.MinPlotAreaHeight} px floor");
        }
        else if (data.Height < figureH * ChartLayout.MinPlotShare)
        {
            problems.Add($"plot is {data.Height:0.#} px tall, under 55% of its {figureH:0.#} px chart");
        }

        if (data.Width < 60)
        {
            problems.Add($"plot is {data.Width:0.#} px wide in a {figureW:0.#} px chart");
        }

        // 2. Legend: the legend panel holds the whole wrapped legend, and the legend fits the chart.
        var legend = plot.Axes.GetPanels().OfType<ScottPlot.Panels.LegendPanel>().FirstOrDefault();
        if (legend is not null && plot.PlottableList.Any(p => p.LegendItems.Any()) && last.Layout.PanelSizes.TryGetValue(legend, out var legendSize))
        {
            using var paint = ScottPlot.Paint.NewDisposablePaint();
            var needed = plot.Legend.GetLayout(new PixelSize(data.Width, float.PositiveInfinity), paint).LegendRect.Height + legend.Padding.Vertical;
            if (legendSize < needed - 0.5f)
            {
                problems.Add($"legend is cut off: its panel is {legendSize:0.#} px, the wrapped legend needs {needed:0.#} px");
            }

            if (plot.Legend.LastRenderSize.Width > figureW + 0.5)
            {
                problems.Add($"legend is {plot.Legend.LastRenderSize.Width:0.#} px wide in a {figureW:0.#} px chart");
            }
        }

        // 3. Tick labels and axis labels: each axis panel got the room its labels need, and the panels fit the figure.
        foreach (var axis in plot.Axes.GetAxes())
        {
            if (!axis.Edge.IsVertical() && !axis.Edge.IsHorizontal())
            {
                continue;
            }

            if (last.Layout.PanelSizes.TryGetValue(axis, out var given))
            {
                using var paint = ScottPlot.Paint.NewDisposablePaint();
                var needed = axis.Measure(paint);
                if (needed > 0 && given < needed - 0.5f)
                {
                    problems.Add($"{axis.Edge} axis labels are clipped: the panel is {given:0.#} px, the labels need {needed:0.#} px");
                }
            }
        }

        // 4. Dead space: when the data starts late, the axis starts near it, not a long way before.
        if (firstDataOa is not null && plot.Axes.Bottom.TickGenerator is ScottPlot.TickGenerators.DateTimeAutomatic)
        {
            var limits = plot.Axes.GetLimits();
            var span = limits.Right - limits.Left;
            var plotted = FirstPlottedX(plot);
            if (plotted is not null && span > 0)
            {
                var gap = (plotted.Value - limits.Left) / span;
                if (gap > MaxLeadingGapShare)
                {
                    problems.Add($"{gap:P0} of the X axis is empty before the first plotted point (axis starts {DateTime.FromOADate(limits.Left):u}, first point {DateTime.FromOADate(plotted.Value):u})");
                }
            }
        }

        return problems;
    }

    /// <summary>#8220: the plot keeps at least this many DIPs of height whatever the legend does.</summary>
    internal const double LegendRuleMinPlotHeight = 120;

    /// <summary>#8220: a legend on the side leaves the plot at least this share of the chart's width.</summary>
    internal const double LegendRuleMinSidePlotShare = 0.60;

    /// <summary>#8220: no more than this share of the chart's width is left empty beside a side legend.</summary>
    internal const double LegendRuleMaxDeadShare = 0.15;

    /// <summary>What a chart's legend did, read from the render the control made: the chart's size, the plot rectangle, the side the legend
    /// sits on, the width the legend drew and the width its panel was given.</summary>
    public readonly record struct LegendReading(
        string Chart, double FigureWidth, double FigureHeight, double PlotWidth, double PlotHeight, string Edge, int Entries, double PanelSize, double DrawnWidth)
    {
        /// <summary>Empty width beside a side legend, as a share of the chart's width. Zero for a legend under the plot.</summary>
        public double DeadShare => Edge == "Right" ? Math.Max(0, PanelSize - DrawnWidth) / FigureWidth : 0;

        public override string ToString() =>
            $"{Chart}: chart {FigureWidth:0}x{FigureHeight:0}, plot {PlotWidth:0}x{PlotHeight:0} ({PlotWidth / FigureWidth:P0} wide), legend {Edge} ({Entries} entries, panel {PanelSize:0} px, drawn {DrawnWidth:0} px, dead {DeadShare:P0})";
    }

    /// <summary>Reads the legend rule's numbers from a settled chart, or null when it shows no legend.</summary>
    public static LegendReading? ReadLegend(WpfPlot chart)
    {
        var plot = chart.Plot;
        var last = plot.RenderManager.LastRender;
        var legend = plot.Axes.GetPanels().OfType<ScottPlot.Panels.LegendPanel>().FirstOrDefault();
        if (legend is null || !plot.PlottableList.Any(p => p.LegendItems.Any()) || !last.Layout.PanelSizes.TryGetValue(legend, out var panelSize))
        {
            return null;
        }

        return new LegendReading(
            NameOf(chart), chart.ActualWidth, chart.ActualHeight, last.DataRect.Width, last.DataRect.Height, legend.Edge.ToString(),
            plot.Legend.GetItems().Length, panelSize, plot.Legend.LastRenderSize.Width + legend.Padding.Horizontal);
    }

    /// <summary>The legend rule (#8220) for one settled chart: one line per defect.</summary>
    public static List<string> MeasureLegendRule(WpfPlot chart)
    {
        var problems = new List<string>();
        if (ReadLegend(chart) is not { } r)
        {
            return problems;
        }

        if (r.PlotHeight < LegendRuleMinPlotHeight - 0.5)
        {
            problems.Add($"plot is {r.PlotHeight:0.#} px tall, under the {LegendRuleMinPlotHeight} px floor ({r})");
        }

        if (r.Edge == "Right" && r.PlotWidth < LegendRuleMinSidePlotShare * r.FigureWidth - 0.5)
        {
            problems.Add($"a side legend leaves the plot {r.PlotWidth / r.FigureWidth:P0} of the width, under {LegendRuleMinSidePlotShare:P0} ({r})");
        }

        if (r.DeadShare > LegendRuleMaxDeadShare)
        {
            problems.Add($"{r.DeadShare:P0} of the width is empty beside the legend, over {LegendRuleMaxDeadShare:P0} ({r})");
        }

        // A legend never cuts an entry: what it drew fits the panel it was given and the chart it sits in.
        var plot = chart.Plot;
        var legend = plot.Axes.GetPanels().OfType<ScottPlot.Panels.LegendPanel>().First();
        var drawn = plot.Legend.LastRenderSize;
        var panelNeeds = r.Edge == "Right" ? drawn.Width + legend.Padding.Horizontal : drawn.Height + legend.Padding.Vertical;
        if (r.PanelSize < panelNeeds - 0.5)
        {
            problems.Add($"the legend is cut: its panel is {r.PanelSize:0.#} px, what it drew needs {panelNeeds:0.#} px ({r})");
        }

        if (r.Edge == "Right" && drawn.Height + legend.Padding.Vertical > r.PlotHeight + 0.5)
        {
            problems.Add($"the side legend is {drawn.Height + legend.Padding.Vertical:0.#} px tall beside a {r.PlotHeight:0.#} px plot, so entries are cut ({r})");
        }

        return problems;
    }

    /// <summary>The charts named in #8220 whose legends are long or sit beside the plot, with the series each really lists: Wait Stats lists up to 20
    /// wait types, Collection Health's Collector Duration lists 41 collectors.</summary>
    public static readonly IReadOnlyDictionary<string, int> HeavyLegendCharts = new Dictionary<string, int>
    {
        ["WaitStatsChart"] = 20,
        ["MemoryClerksChart"] = 12,
        ["PlanCacheChart"] = 6,
        ["PerfmonChart"] = 5,
        ["LatchStatsChart"] = 5,
        ["SpinlockStatsChart"] = 5,
        ["CollectorDurationChart"] = 41,
        ["SickSpinlocksChart"] = 6,
        ["CpuComparisonChart"] = 2,
    };

    /// <summary>Fills a chart on the tab with <paramref name="series"/> made-up time series that have long names, and gives it the legend
    /// the app does (<see cref="ChartRenderHelper.ShowChartLegend"/>), so a chart whose data cannot be fed through its own render method
    /// (it reads a database) is still measured at its real place in the tab.</summary>
    public static void FillLegendChart(WpfPlot chart, int series)
    {
        var helper = new ChartRenderHelper();
        helper.ClearChart(chart);
        ChartStyle.ApplyThemeToChart(chart);
        var now = DateTime.UtcNow;
        var xs = Enumerable.Range(0, 48).Select(k => now.AddMinutes(k - 48).ToOADate()).ToArray();
        for (var s = 0; s < series; s++)
        {
            var ys = xs.Select((_, k) => 50 + (40 * Math.Sin((k / 6.0) + s))).ToArray();
            var line = chart.Plot.Add.Scatter(xs, ys);
            line.LegendText = $"MEMORYCLERK_SQLQUERY_{s + 1:00}";
        }

        chart.Plot.Axes.DateTimeTicksBottom();
        chart.Plot.YLabel("ms/sec");
        helper.ShowChartLegend(chart);
        chart.Refresh();
    }

    /// <summary>True for a chart <paramref name="heavy"/> names. The Overview's stacked lanes reuse chart names (its own WaitStatsChart) but
    /// never show a legend, so a chart inside a lanes control is not the chart the issue means.</summary>
    private static bool IsHeavy(WpfPlot chart, IReadOnlyDictionary<string, int> heavy)
    {
        if (!heavy.ContainsKey(chart.Name))
        {
            return false;
        }

        for (var at = LogicalTreeHelper.GetParent(chart); at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at.GetType().Name.Contains("CorrelatedTimelineLanes", StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The legend rule (#8220) for the charts <paramref name="heavy"/> names (x:Name to series count), each filled by
    /// <see cref="FillLegendChart"/> and measured at its real place in the tab.</summary>
    public static (List<string> Failures, List<string> Readings) MeasureHeavyLegends(
        Window window, DependencyObject root, IReadOnlyDictionary<string, int> heavy, string product, string windowName)
    {
        var failures = new List<string>();
        var readings = new List<string>();
        var charts = Logical<WpfPlot>(root).ToList();
        var seen = new HashSet<string>();
        foreach (var page in charts.GroupBy(c => TabLabel(TabPath(c))))
        {
            Reveal(TabPath(page.First()));
            Layout(window);
            var shown = page.Where(IsShown).ToList();
            foreach (var chart in shown.Where(c => IsHeavy(c, heavy)))
            {
                FillLegendChart(chart, heavy[chart.Name]);
                seen.Add(chart.Name);
            }

            Settle(window, shown);
            foreach (var chart in shown.Where(c => IsHeavy(c, heavy)))
            {
                if (ReadLegend(chart) is { } reading)
                {
                    readings.Add($"[{page.Key}] {reading}");
                }
                else
                {
                    failures.Add($"[{page.Key}] {NameOf(chart)}: the chart shows no legend after it was filled");
                }

                foreach (var problem in MeasureLegendRule(chart))
                {
                    failures.Add($"[{page.Key}] {NameOf(chart)}: {problem}");
                }
            }

            MaybeSave(product, windowName, shown.Where(c => IsHeavy(c, heavy)));
        }

        foreach (var name in heavy.Keys.Where(n => !seen.Contains(n)))
        {
            failures.Add($"{name}: no such chart is shown on the tab, so the legend rule was not measured");
        }

        return (failures, readings);
    }

    /// <summary>Walks every chart under <paramref name="root"/> the way <see cref="MeasureAll"/> does and applies the legend rule (#8220) to each.
    /// Returns one line per defect and one reading line per chart that shows a legend.</summary>
    public static (List<string> Failures, List<string> Readings) MeasureAllLegends(Window window, DependencyObject root, string product, string windowName)
    {
        var failures = new List<string>();
        var readings = new List<string>();
        var charts = Logical<WpfPlot>(root).ToList();
        foreach (var page in charts.GroupBy(c => TabLabel(TabPath(c))))
        {
            Reveal(TabPath(page.First()));
            Layout(window);
            var shown = page.Where(IsShown).ToList();
            Settle(window, shown);
            foreach (var chart in shown)
            {
                if (ReadLegend(chart) is { } reading)
                {
                    readings.Add($"[{page.Key}] {reading}");
                }

                foreach (var problem in MeasureLegendRule(chart))
                {
                    failures.Add($"[{page.Key}] {NameOf(chart)}: {problem}");
                }
            }

            MaybeSave(product, windowName, shown);
        }

        return (failures, readings);
    }

    /// <summary>The main window around a server tab in both apps: a 280 DIP server list on the left and a status strip under the tab. The tests
    /// host the tab in the same room so a chart gets the width and height a person sees at the minimum window, not the whole window.</summary>
    public static Grid HostLikeMainWindow(UIElement tab, double statusHeight = 36)
    {
        var host = new Grid();
        host.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(280) });
        host.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
        host.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });
        host.RowDefinitions.Add(new RowDefinition { Height = new GridLength(statusHeight) });
        Grid.SetColumn(tab, 1);
        host.Children.Add(tab);
        return host;
    }

    /// <summary>Plots that share a group must share a data rectangle: the same left edge and width, and the same room taken by the
    /// title, legend and ticks (so a chart that is shorter only by its margin has a plot shorter by exactly that).</summary>
    public static List<string> MeasureGroup(IReadOnlyList<WpfPlot> members)
    {
        var problems = new List<string>();
        var shown = members.Where(IsShown).ToList();
        if (shown.Count < 2)
        {
            return problems;
        }

        var first = shown[0].Plot.RenderManager.LastRender.DataRect;
        foreach (var chart in shown.Skip(1))
        {
            var r = chart.Plot.RenderManager.LastRender.DataRect;
            var firstChrome = shown[0].ActualHeight - first.Height;
            var chrome = chart.ActualHeight - r.Height;
            if (Math.Abs(r.Left - first.Left) > 1.5 || Math.Abs(r.Width - first.Width) > 1.5 || Math.Abs(r.Top - first.Top) > 1.5 || Math.Abs(chrome - firstChrome) > 1.5)
            {
                problems.Add($"{NameOf(chart)} has plot rectangle {r.Left:0.#},{r.Top:0.#} {r.Width:0.#}x{r.Height:0.#}, but {NameOf(shown[0])} has {first.Left:0.#},{first.Top:0.#} {first.Width:0.#}x{first.Height:0.#}");
            }
        }

        return problems;
    }

    /// <summary>Walks every chart under <paramref name="root"/>, page by page (revealing each tab it sits under), settles the layout,
    /// and measures each chart and each chart group. <paramref name="dataStartOa"/> is where the made-up data starts. Returns one line
    /// per defect, each naming its tab and chart, and a summary line.</summary>
    public static (List<string> Failures, string Summary) MeasureAll(Window window, DependencyObject root, double dataStartOa, string product, string windowName)
    {
        var failures = new List<string>();
        var unfed = new List<string>();
        var charts = Logical<WpfPlot>(root).ToList();
        var measured = 0;
        var skipped = 0;
        foreach (var page in charts.GroupBy(c => TabLabel(TabPath(c))))
        {
            Reveal(TabPath(page.First()));
            Layout(window);
            var shown = page.Where(IsShown).ToList();
            skipped += page.Count() - shown.Count;
            Settle(window, shown);
            foreach (var chart in shown)
            {
                measured++;
                var fedData = FirstPlottedX(chart.Plot) is not null;
                if (!fedData)
                {
                    unfed.Add(NameOf(chart));
                }

                try
                {
                    foreach (var problem in Measure(chart, fedData ? dataStartOa : null))
                    {
                        failures.Add($"[{page.Key}] {NameOf(chart)}: {problem}");
                    }
                }
                catch (Exception ex)
                {
                    failures.Add($"[{page.Key}] {NameOf(chart)}: measuring threw {ex.GetType().Name}: {ex.Message}");
                }
            }

            foreach (var group in shown.GroupBy(c => ChartGroupAligner.FindGroup(c)).Where(g => g.Key is not null))
            {
                foreach (var problem in MeasureGroup(group.ToList()))
                {
                    failures.Add($"[{page.Key}] group: {problem}");
                }
            }

            MaybeSave(product, windowName, shown);
        }

        return (failures, $"{windowName}: {charts.Count} charts, {measured} measured, {skipped} not shown, {unfed.Count} without plotted data (no seam fed them): {string.Join(", ", unfed)}");
    }

    /// <summary>Saves what each chart drew, for looking at a failure. Set CHART_LAYOUT_PNG_DIR to a folder.</summary>
    public static void MaybeSave(string tabName, string windowName, IEnumerable<WpfPlot> charts)
    {
        var dir = Environment.GetEnvironmentVariable("CHART_LAYOUT_PNG_DIR");
        if (string.IsNullOrEmpty(dir))
        {
            return;
        }

        Directory.CreateDirectory(dir);
        foreach (var chart in charts.Where(IsShown))
        {
            var w = (int)(chart.ActualWidth * chart.DisplayScale);
            var h = (int)(chart.ActualHeight * chart.DisplayScale);
            var name = string.Concat(NameOf(chart).Where(char.IsLetterOrDigit));
            chart.Plot.GetImage(w, h).SavePng(Path.Combine(dir, $"{tabName}-{windowName}-{name}.png"));
        }
    }

    /// <summary>Saves the whole hosted window for looking at it. Set CHART_LAYOUT_PNG_DIR to a folder.</summary>
    public static void SaveWindow(Window window, string name)
    {
        var dir = Environment.GetEnvironmentVariable("CHART_LAYOUT_PNG_DIR");
        if (string.IsNullOrEmpty(dir) || window.Content is not FrameworkElement content)
        {
            return;
        }

        Directory.CreateDirectory(dir);
        var bitmap = new System.Windows.Media.Imaging.RenderTargetBitmap((int)content.ActualWidth, (int)content.ActualHeight, 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(content);
        var encoder = new System.Windows.Media.Imaging.PngBitmapEncoder();
        encoder.Frames.Add(System.Windows.Media.Imaging.BitmapFrame.Create(bitmap));
        using var stream = File.Create(Path.Combine(dir, $"window-{name}.png"));
        encoder.Save(stream);
    }

    /// <summary>#8220: the Query Heatmap's labels on a settled chart that plots <paramref name="columns"/> time buckets and <paramref name="rows"/> value
    /// buckets. An X label stays on one line and keeps 10 px clear of the next; a Y label keeps 4 px clear of the next. One line per defect.</summary>
    public static List<string> MeasureHeatmapLabels(WpfPlot chart, int columns, int rows, out string reading)
    {
        var problems = new List<string>();
        var plot = chart.Plot;
        var rect = plot.RenderManager.LastRender.DataRect;
        using var paint = Paint.NewDisposablePaint();

        var xTicks = plot.Axes.Bottom.TickGenerator.Ticks.Where(t => t.IsMajor).OrderBy(t => t.Position).ToList();
        var xStyle = plot.Axes.Bottom.TickLabelStyle;
        var xPixels = rect.Width / columns;
        var minGap = double.MaxValue;
        for (var i = 0; i < xTicks.Count; i++)
        {
            if (xTicks[i].Label.Contains('\n', StringComparison.Ordinal))
            {
                problems.Add($"the X label '{xTicks[i].Label.Replace("\n", "\\n", StringComparison.Ordinal)}' is on more than one line");
            }

            if (i > 0)
            {
                var gap = ((xTicks[i].Position - xTicks[i - 1].Position) * xPixels)
                    - ((xStyle.Measure(xTicks[i].Label, paint).Width + xStyle.Measure(xTicks[i - 1].Label, paint).Width) / 2);
                minGap = Math.Min(minGap, gap);
                if (gap < 10)
                {
                    problems.Add($"X labels '{xTicks[i - 1].Label}' and '{xTicks[i].Label}' are {gap:0.#} px apart, under 10");
                }
            }
        }

        var yTicks = plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor).OrderBy(t => t.Position).ToList();
        var yStyle = plot.Axes.Left.TickLabelStyle;
        var yPixels = rect.Height / rows;
        var minYGap = double.MaxValue;
        for (var i = 1; i < yTicks.Count; i++)
        {
            var gap = ((yTicks[i].Position - yTicks[i - 1].Position) * yPixels)
                - ((yStyle.Measure(yTicks[i].Label, paint).Height + yStyle.Measure(yTicks[i - 1].Label, paint).Height) / 2);
            minYGap = Math.Min(minYGap, gap);
            if (gap < 4)
            {
                problems.Add($"Y labels '{yTicks[i - 1].Label}' and '{yTicks[i].Label}' are {gap:0.#} px apart, under 4");
            }
        }

        if (rect.Height < LegendRuleMinPlotHeight - 0.5)
        {
            problems.Add($"the plot is {rect.Height:0.#} px tall, under the {LegendRuleMinPlotHeight} px floor");
        }

        reading = $"plot {rect.Width:0}x{rect.Height:0}, {xTicks.Count} X labels (tightest gap {(minGap == double.MaxValue ? 0 : minGap):0} px), {yTicks.Count} Y labels (tightest gap {(minYGap == double.MaxValue ? 0 : minYGap):0} px)";
        return problems;
    }

    // ---- Feeding a chart through its own render method with a made-up reply ----

    /// <summary>Builds a list of made-up rows of <paramref name="rowType"/>: times ramp from <paramref name="from"/> to
    /// <paramref name="to"/>, numbers wave, and a text column makes three series. A row type is built through its longest
    /// constructor, and any settable property the constructor left alone is filled the same way. Null when the type cannot be built.</summary>
    public static object? MakeRows(Type rowType, DateTime from, DateTime to, int keyedSeries = 3)
    {
        var ctor = rowType.GetConstructors().OrderByDescending(c => c.GetParameters().Length).FirstOrDefault();
        if (ctor is null)
        {
            return null;
        }

        var parameters = ctor.GetParameters();
        var properties = rowType.GetProperties().Where(p => p.CanWrite && p.SetMethod!.IsPublic && p.GetIndexParameters().Length == 0).ToArray();
        var series = parameters.Any(p => p.ParameterType == typeof(string)) || properties.Any(p => p.PropertyType == typeof(string)) ? keyedSeries : 1;
        const int steps = 48;
        var list = (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(rowType))!;
        for (var step = 0; step < steps; step++)
        {
            var t = from + TimeSpan.FromTicks((to - from).Ticks * step / (steps - 1));
            for (var s = 0; s < series; s++)
            {
                var args = new object?[parameters.Length];
                for (var i = 0; i < parameters.Length; i++)
                {
                    args[i] = Value(parameters[i].ParameterType, parameters[i].Name ?? string.Empty, t, step, s, parameters[i].HasDefaultValue ? parameters[i].DefaultValue : null);
                }

                try
                {
                    var row = ctor.Invoke(args);
                    foreach (var property in properties)
                    {
                        property.SetValue(row, Value(property.PropertyType, property.Name, t, step, s, property.GetValue(row)));
                    }

                    list.Add(row);
                }
                catch (TargetInvocationException)
                {
                    return null;
                }
            }
        }

        return list;
    }

    private static object? Value(Type declared, string name, DateTime t, int step, int series, object? fallback)
    {
        var type = Nullable.GetUnderlyingType(declared) ?? declared;
        var wave = 50 + (40 * Math.Sin((step / 6.0) + series));
        var isId = name.EndsWith("Id", StringComparison.OrdinalIgnoreCase) || name.EndsWith("Number", StringComparison.OrdinalIgnoreCase);
        if (type == typeof(DateTime))
        {
            return DateTime.SpecifyKind(t, DateTimeKind.Utc);
        }

        if (type == typeof(DateTimeOffset))
        {
            return new DateTimeOffset(DateTime.SpecifyKind(t, DateTimeKind.Utc));
        }

        if (type == typeof(string))
        {
            return $"{name} {series + 1}";
        }

        if (type == typeof(double))
        {
            return wave;
        }

        if (type == typeof(float))
        {
            return (float)wave;
        }

        if (type == typeof(decimal))
        {
            return (decimal)wave;
        }

        if (type == typeof(int))
        {
            return isId ? series + 1 : (int)wave;
        }

        if (type == typeof(long))
        {
            return isId ? series + 1L : (long)wave;
        }

        if (type == typeof(short))
        {
            return (short)(isId ? series + 1 : wave);
        }

        if (type == typeof(byte))
        {
            return (byte)(isId ? series + 1 : wave % 100);
        }

        if (type == typeof(bool))
        {
            return false;
        }

        if (type.IsEnum)
        {
            return Enum.GetValues(type).GetValue(0);
        }

        return fallback;
    }

    /// <summary>Calls the render methods of a tab that <paramref name="isRenderMethod"/> picks, with made-up arguments. <paramref name="argumentFor"/> gets each parameter's type and, for a date, how many dates came before it. Each is called, and the ones that threw are returned by name.</summary>
    public static (List<string> Fed, List<string> Failed) FeedRenderMethods(
        object tab, Func<MethodInfo, bool> isRenderMethod, Func<Type, int, object?> argumentFor)
    {
        var fed = new List<string>();
        var failed = new List<string>();
        const BindingFlags all = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
        foreach (var method in tab.GetType().GetMethods(all).Where(isRenderMethod))
        {
            var parameters = method.GetParameters();
            var args = new object?[parameters.Length];
            var ok = parameters.Length > 0;
            var dates = 0;
            for (var i = 0; ok && i < parameters.Length; i++)
            {
                var isDate = (Nullable.GetUnderlyingType(parameters[i].ParameterType) ?? parameters[i].ParameterType) == typeof(DateTime);
                args[i] = argumentFor(parameters[i].ParameterType, isDate ? dates++ : -1);
                ok = args[i] is not null || Nullable.GetUnderlyingType(parameters[i].ParameterType) is not null;
            }

            if (!ok)
            {
                continue;
            }

            try
            {
                method.Invoke(tab, args);
                fed.Add(method.Name);
            }
            catch (TargetInvocationException ex)
            {
                failed.Add($"{method.Name}: {ex.InnerException?.GetType().Name}: {ex.InnerException?.Message}");
            }
        }

        return (fed, failed);
    }
}

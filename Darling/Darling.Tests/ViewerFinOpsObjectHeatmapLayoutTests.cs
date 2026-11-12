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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using ScottPlot;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8531: the Viewer's FinOps > Storage Growth > a database's Object Growth Heatmap, hosted the way the main window hosts it at
/// 900x600. The plot keeps at least 120 px of height, each object's label has a row of its own, the colour bar's tick labels do
/// not touch, its "Reserved (MB)" label fits the bar, the grid under the chart keeps its space (the page scrolls when both
/// cannot fit), a day with no sample keeps its column on the day axis, and none of the tab's words holds an em dash.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerFinOpsObjectHeatmapLayoutTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const int Days = 30;

    private readonly ITestOutputHelper _output;

    public ViewerFinOpsObjectHeatmapLayoutTests(ITestOutputHelper output) => _output = output;

    [Theory]
    [InlineData("DarkTheme", 13)]
    [InlineData("LightTheme", 13)]
    [InlineData("CoolBreezeTheme", 13)]
    [InlineData("DarkTheme", 20)]
    [InlineData("DarkTheme", 3)]
    public void TheHeatmap_KeepsARowForEachObject_AndAReadableColourBar(string theme, int objects)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, () => OnTheObjectView((tab, window, chart) =>
        {
            var grid = (DataGrid)tab.FindName("FinOpsObjectGrowthDetailGrid");
            grid.ItemsSource = Enumerable.Range(0, objects)
                .Select(i => new ObjectSizeGrowthRow { DatabaseName = "SalesDatabase", SchemaName = "dbo", TableName = $"CustomerOrderHistory_{i:00}" })
                .ToList();

            var matrix = Matrix(objects);
            var handle = FinOpsHeatmapRenderer.Render(chart, matrix, "Reserved (MB)", "SalesDatabase Object Reserved Footprint", null);
            ChartLayoutProbe.Settle(window, new[] { chart });
            ChartLayoutProbe.SaveWindow(window, $"finops-object-heatmap-viewer-{theme}-{objects}");

            var found = ChartLayoutProbe.MeasureHeatmapLabels(chart, Days, objects, out var reading);
            found.AddRange(MeasureColourBar(chart, handle, matrix, out var barReading));

            var origin = grid.TransformToAncestor(window).Transform(new Point(0, 0));
            var hostBottom = ((FrameworkElement)window.Content).ActualHeight - 36;
            _output.WriteLine($"{theme}, {objects} objects: {reading}; {barReading}; chart {chart.ActualWidth:0}x{chart.ActualHeight:0}; grid {grid.ActualHeight:0} tall, bottom {origin.Y + grid.ActualHeight:0} of {hostBottom:0}");
            if (grid.ActualHeight < 120)
            {
                found.Add($"the grid under the chart is {grid.ActualHeight:0} DIPs tall, under 120");
            }

            if (origin.Y + grid.ActualHeight > hostBottom + 1 && FirstScrollViewerAbove(grid) is null)
            {
                found.Add($"the grid ends at {origin.Y + grid.ActualHeight:0}, below the {hostBottom:0} the window shows, and nothing scrolls");
            }

            return found;
        })));

        Assert.True(problems.Count == 0, $"{theme}, {objects} objects:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void ADayWithNoSample_KeepsItsColumn_OnTheDayAxis()
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", () => OnTheObjectView((tab, window, chart) =>
        {
            /* Samples on 10/30 and 11/3 only: nothing was collected on the three days between them. */
            var matrix = new FinOpsHeatmapMatrix
            {
                Intensities = new double[,] { { 120, 150 }, { 40, 55 }, { 900, 1100 } },
                RowLabels = new[] { "dbo.Orders", "dbo.Customers", "dbo.OrderLines" },
                Days = new[] { new DateTime(2026, 10, 30), new DateTime(2026, 11, 3) },
            };
            var handle = FinOpsHeatmapRenderer.Render(chart, matrix, "Reserved (MB)", "SalesDatabase Object Reserved Footprint", null);
            ChartLayoutProbe.Settle(window, new[] { chart });

            var found = new List<string>();
            var expected = new[] { "10/30", "10/31", "11/1", "11/2", "11/3" };
            var labels = chart.Plot.Axes.Bottom.TickGenerator.Ticks.Where(t => t.IsMajor).OrderBy(t => t.Position).Select(t => t.Label).ToArray();
            if (!labels.SequenceEqual(expected))
            {
                found.Add($"the day axis reads {string.Join(", ", labels)}, not {string.Join(", ", expected)}");
            }

            var columns = handle.Plottable?.Intensities.GetLength(1) ?? 0;
            if (columns != expected.Length)
            {
                found.Add($"the heatmap has {columns} columns for the {expected.Length} days from 10/30 to 11/3");
            }

            return found;
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheTabsWords_HoldNoEmDash()
    {
        var problems = StaTestThread.Run(() => WithTheme("DarkTheme", () => OnTheObjectView((tab, window, chart) =>
        {
            var found = new List<string>();
            foreach (var words in WordsOf(tab).Where(w => w.Contains('\u2014', StringComparison.Ordinal)).Distinct())
            {
                found.Add($"the tab's text holds an em dash: \"{words}\"");
            }

            /* A database with no object samples: the chart's own title for the empty heatmap. */
            FinOpsHeatmapRenderer.Render(chart, new FinOpsHeatmapMatrix(), "Reserved (MB)", "SalesDatabase Object Reserved Footprint", null);
            ChartLayoutProbe.Settle(window, new[] { chart });
            var title = chart.Plot.Axes.Title.Label.Text;
            if (title.Contains('\u2014', StringComparison.Ordinal))
            {
                found.Add($"the empty heatmap's title holds an em dash: \"{title}\"");
            }

            return found;
        })));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>The words the tab holds, shown or not yet shown: every text block, string tooltip, string content (a button, a
    /// check box), string header (a tab, a group), chart card title and plain grid column header.</summary>
    private static IEnumerable<string> WordsOf(DependencyObject root)
    {
        foreach (var element in ChartLayoutProbe.Logical<FrameworkElement>(root))
        {
            if (element is TextBlock text) yield return text.Text;
            if (element.ToolTip is string tip) yield return tip;
            if (element is ContentControl { Content: string content }) yield return content;
            if (element is HeaderedContentControl { Header: string header }) yield return header;
            if (element is ChartPanel { Title: string title }) yield return title;
            if (element is DataGrid grid)
            {
                foreach (var column in grid.Columns)
                {
                    if (column.Header is string columnHeader) yield return columnHeader;
                }
            }
        }
    }

    /// <summary>The colour bar beside the heatmap: at least two tick labels on it, neighbours at least 2 px apart, and the bar's
    /// own label no longer than the bar. The bar spans the heatmap's range, which is the log of its smallest and largest cells.</summary>
    private static List<string> MeasureColourBar(WpfPlot chart, FinOpsHeatmapHandle handle, FinOpsHeatmapMatrix matrix, out string reading)
    {
        var problems = new List<string>();
        reading = "no colour bar";
        if (handle.ColorBar is not { } bar)
        {
            problems.Add("the heatmap has no colour bar");
            return problems;
        }

        var cells = matrix.Intensities.Cast<double>().Where(v => v > 0).ToList();
        var low = Math.Log(1 + cells.Min());
        var high = Math.Log(1 + cells.Max());
        var rect = chart.Plot.RenderManager.LastRender.DataRect;
        using var paint = Paint.NewDisposablePaint();
        var style = bar.Axis.TickLabelStyle;
        var ticks = bar.Axis.TickGenerator.Ticks
            .Where(t => t.IsMajor && t.Position >= low - 1e-6 && t.Position <= high + 1e-6)
            .OrderBy(t => t.Position)
            .ToList();
        if (ticks.Count < 2)
        {
            problems.Add($"the colour bar has {ticks.Count} tick labels in its range, under 2");
        }

        var tightest = double.MaxValue;
        for (var i = 1; i < ticks.Count; i++)
        {
            var gap = ((ticks[i].Position - ticks[i - 1].Position) / (high - low) * rect.Height)
                - ((style.Measure(ticks[i].Label, paint).Height + style.Measure(ticks[i - 1].Label, paint).Height) / 2);
            tightest = Math.Min(tightest, gap);
            if (gap < 2)
            {
                problems.Add($"colour-bar labels '{ticks[i - 1].Label}' and '{ticks[i].Label}' are {gap:0.#} px apart, under 2");
            }
        }

        var label = bar.LabelStyle.Measure(bar.Label, paint).Width;
        if (label > rect.Height)
        {
            problems.Add($"the colour bar's label \"{bar.Label}\" is {label:0} px long on a bar {rect.Height:0} px tall, so it is cut off");
        }

        reading = $"colour bar {ticks.Count} labels (tightest gap {(tightest == double.MaxValue ? 0 : tightest):0} px), label {label:0} px on {rect.Height:0} px";
        return problems;
    }

    /// <summary>A heatmap of <paramref name="objects"/> objects over <see cref="Days"/> days: reserved MB from under 1 to tens of
    /// thousands, with a blank cell here and there (an object with no sample that day).</summary>
    private static FinOpsHeatmapMatrix Matrix(int objects)
    {
        var cells = new double[objects, Days];
        for (var r = 0; r < objects; r++)
        {
            for (var c = 0; c < Days; c++)
            {
                cells[r, c] = (r + c) % 7 == 0 ? 0 : 0.4 + ((r + 1) * (r + 1) * 37.0 * (1 + (c / (double)Days)));
            }
        }

        return new FinOpsHeatmapMatrix
        {
            Intensities = cells,
            RowLabels = Enumerable.Range(0, objects).Select(i => $"dbo.CustomerOrderHistory_{i:00}").ToArray(),
            Days = Enumerable.Range(0, Days).Select(d => new DateTime(2026, 10, 1).AddDays(d)).ToArray(),
        };
    }

    /// <summary>Hosts the FinOps tab like the main window at 900x600, opens Storage Growth's object view, and hands the tab, the
    /// window and the heatmap chart to <paramref name="check"/>.</summary>
    private static List<string> OnTheObjectView(Func<FinOpsTab, Window, WpfPlot, List<string>> check)
    {
        var tab = new FinOpsTab();
        var outer = new TabControl();
        outer.Items.Add(new TabItem { Header = "FinOps", Content = tab });
        var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(outer), Width = 900, Height = 600, Left = 0, Top = 0 };
        StaTestThread.ShowOffScreen(window);
        try
        {
            var chart = (WpfPlot)tab.FindName("FinOpsObjectGrowthHeatmapChart");
            for (var node = (DependencyObject)chart; node is not null; node = LogicalTreeHelper.GetParent(node))
            {
                if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
                {
                    tabs.SelectedItem = item;
                }
            }

            /* The drill the tab makes when a database is opened: the object view in place of the per-database grid. */
            var level = typeof(FinOpsTab).GetNestedType("FinOpsStorageDrillLevel", BindingFlags.NonPublic)!;
            typeof(FinOpsTab).GetMethod("ShowFinOpsStorageView", Instance)!.Invoke(tab, new[] { Enum.Parse(level, "Objects") });
            ChartLayoutProbe.Layout(window);

            return check(tab, window, chart);
        }
        finally
        {
            window.Close();
        }
    }

    private static ScrollViewer? FirstScrollViewerAbove(DependencyObject element)
    {
        for (var node = LogicalTreeHelper.GetParent(element); node is not null; node = LogicalTreeHelper.GetParent(node))
        {
            if (node is ScrollViewer sv && sv.VerticalScrollBarVisibility != ScrollBarVisibility.Disabled)
            {
                return sv;
            }
        }

        return null;
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}

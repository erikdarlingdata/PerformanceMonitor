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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using ScottPlot;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7385: charts shown together share one plot rectangle, a long legend wraps at the chart's width instead of stacking one
/// line per series, an empty chart is one sentence with no axes and is left out of the group, and no chart has a legend frame, and a block of charts takes its column count and
/// height from the space it has. Every test drives real renders and a real measure and arrange on an STA thread.
/// </summary>
public sealed class ChartGroupLayoutTests
{
    // A window of the given size (DIPs), less what the tab strip, toolbar and margins take of it.
    public static TheoryData<int, int, double> Windows => new()
    {
        { 1280, 720, 1.0 }, { 1280, 720, 2.0 },
        { 1936, 1156, 1.0 }, { 1936, 1156, 2.0 },
        { 3840, 2160, 1.0 }, { 3840, 2160, 2.0 },
    };

    private static Size Viewport(int windowWidth, int windowHeight) => new(windowWidth - 40, windowHeight - 260);

    private static readonly string[] Names =
    {
        "Heap reads", "Index reads", "TOAST reads", "Temp reads",
    };

    private static WpfPlot DataChart(double dpi, string yLabel, string caption, int series, double magnitude, bool longNames = false) =>
        FillDataChart(new WpfPlot(), dpi, yLabel, caption, series, magnitude, longNames);

    private static WpfPlot FillDataChart(WpfPlot chart, double dpi, string yLabel, string caption, int series, double magnitude, bool longNames = false)
    {
        ChartDisplayScale.Apply(chart, dpi);
        ChartStyle.ApplyThemeToChart(chart);
        var t0 = new DateTime(2026, 11, 1).ToOADate();
        var xs = Enumerable.Range(0, 120).Select(k => t0 + k / 144.0).ToArray();
        for (var s = 0; s < series; s++)
        {
            var scatter = chart.Plot.Add.Scatter(xs, xs.Select(x => (Math.Sin((x - t0) * 20 + s) + 1.2) * magnitude / (s + 1)).ToArray());
            scatter.LegendText = longNames ? $"{Names[s % Names.Length]} from disk (shared buffers)" : Names[s % Names.Length];
        }
        chart.Plot.Axes.DateTimeTicksBottom();
        ChartStyle.ReapplyAxisColors(chart);
        chart.Plot.YLabel(yLabel);
        if (caption.Length > 0) chart.Plot.Title(caption);
        new ChartRenderHelper().ShowChartLegend(chart);
        return chart;
    }

    private static WpfPlot EmptyChart(double dpi) => FillEmptyChart(new WpfPlot(), dpi);

    private static WpfPlot FillEmptyChart(WpfPlot chart, double dpi)
    {
        ChartDisplayScale.Apply(chart, dpi);
        ChartStyle.ApplyThemeToChart(chart);
        new ChartRenderHelper().RefreshEmptyChart(chart, "Execution Count", "Executions/sec");
        return chart;
    }

    private static (ScrollViewer Viewer, ChartGridPanel Panel, WpfPlot[] Charts) Host(int windowWidth, int windowHeight, double dpi, int maxColumns, bool withEmpty = true)
    {
        var charts = new List<WpfPlot>
        {
            DataChart(dpi, "Read Ops/sec", "Reads per second, by relation kind, raw samples (one point per collection) since the start of the window", 4, 120000),
            DataChart(dpi, "Write Ops/sec", "Writes", 3, 0.4),
            DataChart(dpi, "Hit Ratio %", "", 1, 99),
        };
        if (withEmpty) charts.Add(EmptyChart(dpi));
        var panel = new ChartGridPanel { MaxColumns = maxColumns };
        foreach (var c in charts) panel.Children.Add(c);
        var viewer = new ScrollViewer { VerticalScrollBarVisibility = ScrollBarVisibility.Auto, HorizontalScrollBarVisibility = ScrollBarVisibility.Disabled, Content = panel };
        Layout(viewer, Viewport(windowWidth, windowHeight));
        return (viewer, panel, charts.ToArray());
    }

    private static void Layout(ScrollViewer viewer, Size size)
    {
        for (var pass = 0; pass < 4; pass++)
        {
            viewer.Measure(size);
            viewer.Arrange(new Rect(new Point(0, 0), size));
            viewer.UpdateLayout();
            Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
        }
    }

    /// <summary>Renders every chart at the size layout gave it, the way the control does.</summary>
    private static void RenderAll(IEnumerable<WpfPlot> charts)
    {
        foreach (var chart in charts)
        {
            using var image = chart.Plot.GetImage((int)(chart.ActualWidth * chart.DisplayScale), (int)(chart.ActualHeight * chart.DisplayScale));
        }
    }

    /// <summary>The way the app gets there: charts render, the debounce fires, the charts that changed render again.</summary>
    private static void Settle(ChartGridPanel panel, WpfPlot[] charts)
    {
        RenderAll(charts);
        ChartGroupAligner.Check(panel);
        RenderAll(charts);
        ChartGroupAligner.Check(panel);
        RenderAll(charts);
    }

    private static PixelRect Data(WpfPlot chart) => chart.Plot.RenderManager.LastRender.DataRect;

    private static void MaybeSavePng(string name, FrameworkElement panel, WpfPlot[] charts)
    {
        var dir = Environment.GetEnvironmentVariable("CHART_LAYOUT_PNG_DIR");
        if (string.IsNullOrEmpty(dir)) return;
        Directory.CreateDirectory(dir);
        var bitmap = new System.Windows.Media.Imaging.RenderTargetBitmap(
            (int)Math.Max(1, panel.ActualWidth), (int)Math.Max(1, panel.ActualHeight), 96, 96, System.Windows.Media.PixelFormats.Pbgra32);
        var back = new System.Windows.Shapes.Rectangle { Width = panel.ActualWidth, Height = panel.ActualHeight, Fill = System.Windows.Media.Brushes.Black };
        back.Measure(new Size(panel.ActualWidth, panel.ActualHeight));
        back.Arrange(new Rect(0, 0, panel.ActualWidth, panel.ActualHeight));
        bitmap.Render(back);
        // Compose the charts' own renders at their cell positions.
        var visual = new System.Windows.Media.DrawingVisual();
        using (var dc = visual.RenderOpen())
        {
            foreach (var chart in charts)
            {
                var pw = (int)(chart.ActualWidth * chart.DisplayScale);
                var ph = (int)(chart.ActualHeight * chart.DisplayScale);
                var bytes = chart.Plot.GetImage(pw, ph).GetImageBytes();
                var source = new System.Windows.Media.Imaging.BitmapImage();
                source.BeginInit();
                source.StreamSource = new MemoryStream(bytes);
                source.CacheOption = System.Windows.Media.Imaging.BitmapCacheOption.OnLoad;
                source.EndInit();
                var origin = chart.TranslatePoint(new Point(0, 0), panel);
                dc.DrawImage(source, new Rect(origin.X, origin.Y, chart.ActualWidth, chart.ActualHeight));
            }
        }
        bitmap.Render(visual);
        var encoder = new System.Windows.Media.Imaging.PngBitmapEncoder();
        encoder.Frames.Add(System.Windows.Media.Imaging.BitmapFrame.Create(bitmap));
        using var file = File.Create(Path.Combine(dir, name + ".png"));
        encoder.Save(file);
    }

    [Theory]
    [MemberData(nameof(Windows))]
    public void ChartsInAGroup_ShareOnePlotRectangle_ExceptTheEmptyOne(int width, int height, double dpi) =>
        StaTestThread.Run(() =>
        {
            var (_, panel, charts) = Host(width, height, dpi, maxColumns: 2);
            Settle(panel, charts);
            MaybeSavePng($"group-{width}x{height}-x{dpi}", panel, charts);

            var first = Data(charts[0]);
            foreach (var chart in charts.Where(c => !ChartGroupAligner.IsEmptyState(c.Plot)))
            {
                var r = Data(chart);
                Assert.True(Math.Abs(r.Left - first.Left) <= 1.5 && Math.Abs(r.Top - first.Top) <= 1.5
                    && Math.Abs(r.Width - first.Width) <= 1.5 && Math.Abs(r.Height - first.Height) <= 1.5,
                    $"plot rectangles differ: {first.Left:0.#},{first.Top:0.#} {first.Width:0.#}x{first.Height:0.#} vs {r.Left:0.#},{r.Top:0.#} {r.Width:0.#}x{r.Height:0.#}"
                    + " | " + string.Join(" ; ", charts.Select(c => $"{ChartGroupAligner.LastFrame(c.Plot)} floors L{c.Plot.Axes.Left.MinimumSize:0.#} B{c.Plot.Axes.Bottom.MinimumSize:0.#} T{c.Plot.Axes.Title.MinimumSize:0.#} size {c.ActualWidth:0.#}x{c.ActualHeight:0.#}")));
            }
        });

    [Theory]
    [MemberData(nameof(Windows))]
    public void ThePlotKeepsAHealthyShareOfTheChart(int width, int height, double dpi) =>
        StaTestThread.Run(() =>
        {
            var (viewer, panel, charts) = Host(width, height, dpi, maxColumns: 4);
            Settle(panel, charts);
            Layout(viewer, Viewport(width, height)); // the chart chrome raised the panel's floor, as it does in the app
            Settle(panel, charts);

            foreach (var chart in charts)
            {
                var plot = Data(chart).Height;
                var figure = chart.ActualHeight; // the plot's units are DIP-sized ("scaled")
                Assert.True(plot >= ChartLayout.MinPlotAreaHeight - 0.5, $"plot {plot:0.#} px of a {figure:0.#} px chart is under {ChartLayout.MinPlotAreaHeight}");
                Assert.True(plot >= figure * 0.3, $"plot {plot:0.#} px is under 30% of the {figure:0.#} px chart");
            }
        });

    // The shape of the tempdb, File I/O and Lock/Blocking blocks in both apps (a Grid of star rows with a floor, marked as a group, whose
    // height is the viewport's), written the way the XAML writes it and loaded by the XAML parser, so the attached property, the
    // ViewportHeight binding and the x:Static floor are the real ones. One chart has a long legend and caption, one is empty.
    private const string StackedBlockXaml = """
        <ScrollViewer xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
                      xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml"
                      xmlns:ui="clr-namespace:PerformanceMonitor.Ui;assembly=PerformanceMonitor.Ui"
                      xmlns:scottplot="clr-namespace:ScottPlot.WPF;assembly=ScottPlot.WPF"
                      VerticalScrollBarVisibility="Auto" HorizontalScrollBarVisibility="Disabled">
            <Grid x:Name="Block" ui:ChartGroup.IsAligned="True"
                  Height="{Binding ViewportHeight, RelativeSource={RelativeSource AncestorType=ScrollViewer}}" MinHeight="450">
                <Grid.RowDefinitions>
                    <RowDefinition Height="*" MinHeight="{x:Static ui:ChartLayout.MinPlotHeight}"/>
                    <RowDefinition Height="*" MinHeight="{x:Static ui:ChartLayout.MinPlotHeight}"/>
                    <RowDefinition Height="*" MinHeight="{x:Static ui:ChartLayout.MinPlotHeight}"/>
                </Grid.RowDefinitions>
                <Border Grid.Row="0" Margin="0,0,0,4"><scottplot:WpfPlot x:Name="First"/></Border>
                <Border Grid.Row="1" Margin="0,4,0,4"><scottplot:WpfPlot x:Name="Second"/></Border>
                <Border Grid.Row="2" Margin="0,4,0,0"><scottplot:WpfPlot x:Name="Third"/></Border>
            </Grid>
        </ScrollViewer>
        """;

    [Theory]
    [MemberData(nameof(Windows))]
    public void AStackedXamlBlock_SharesOnePlotRectangle_AndFollowsTheWindow(int width, int height, double dpi) =>
        StaTestThread.Run(() =>
        {
            var viewer = (ScrollViewer)XamlReader.Parse(StackedBlockXaml);
            var block = (Grid)viewer.FindName("Block");
            var charts = new[] { (WpfPlot)viewer.FindName("First"), (WpfPlot)viewer.FindName("Second"), (WpfPlot)viewer.FindName("Third") };
            FillDataChart(charts[0], dpi, "Read Ops/sec", "Reads per second, by relation kind, raw samples (one point per collection) since the start of the window", 4, 120000);
            FillDataChart(charts[1], dpi, "Write Ops/sec", "", 2, 0.4);
            FillEmptyChart(charts[2], dpi);
            var size = Viewport(width, height);
            Layout(viewer, size);

            // Render, let the group check run, render what changed, and let the chart chrome raise the row floors.
            RenderAll(charts);
            ChartGroupAligner.Check(block);
            RenderAll(charts);
            Layout(viewer, size);
            RenderAll(charts);
            ChartGroupAligner.Check(block);
            RenderAll(charts);
            ChartGroupAligner.Check(block);
            RenderAll(charts);
            MaybeSavePng($"stacked-{width}x{height}-x{dpi}", block, charts);

            // The block fills the viewport when its floor allows, and is taller than it (so the tab scrolls) when it does not.
            Assert.Equal(Math.Max(size.Height, block.MinHeight), block.ActualHeight, 1);
            Assert.True(block.MinHeight >= 3 * ChartLayout.MinPlotHeight, $"the floor {block.MinHeight} is under three minimum rows");
            if (height == 720) Assert.True(viewer.ScrollableHeight > 0, "a small window should scroll");
            else Assert.True(viewer.ScrollableHeight < 60, $"a window of {height} should nearly fit the block (a plot keeps 55% of its row, so a tall legend can need a little more), scrolls by {viewer.ScrollableHeight}");

            var first = Data(charts[0]);
            foreach (var chart in charts)
            {
                var r = Data(chart);
                if (ChartGroupAligner.IsEmptyState(chart.Plot)) continue; // one sentence, no plot rectangle to share
                Assert.True(Math.Abs(r.Left - first.Left) <= 1.5 && Math.Abs(r.Width - first.Width) <= 1.5 && Math.Abs(r.Height - first.Height) <= 5, // the middle row has 4 px more margin than the outer ones
                    $"stacked plot rectangles differ: {first.Left:0.#},{first.Top:0.#} {first.Width:0.#}x{first.Height:0.#} vs {r.Left:0.#},{r.Top:0.#} {r.Width:0.#}x{r.Height:0.#}");
                Assert.True(r.Height >= ChartLayout.MinPlotAreaHeight - 0.5, $"plot {r.Height:0.#} px is under {ChartLayout.MinPlotAreaHeight}");
            }
        });

    [Fact]
    public void ALongLegend_WrapsAtTheChartWidth_AndIsNotClipped() =>
        StaTestThread.Run(() =>
        {
            var narrow = DataChart(1.0, "Read Ops/sec", "", 4, 100, longNames: true);
            var wide = DataChart(1.0, "Read Ops/sec", "", 4, 100, longNames: true);
            using (narrow.Plot.GetImage(360, 420)) { }
            using (wide.Plot.GetImage(1900, 420)) { }

            var rowHeight = wide.Plot.Legend.LastRenderSize.Height;
            Assert.True(narrow.Plot.Legend.LastRenderSize.Height > rowHeight * 1.5f,
                $"legend {narrow.Plot.Legend.LastRenderSize.Height:0.#} px at 360 px wide should be several rows of {rowHeight:0.#} px");
            // Not clipped: the legend panel in the layout holds the whole wrapped legend, and the legend fits the chart's width.
            var panel = narrow.Plot.Axes.GetPanels().OfType<ScottPlot.Panels.LegendPanel>().Single();
            var size = narrow.Plot.RenderManager.LastRender.Layout.PanelSizes[panel];
            var needed = narrow.Plot.Legend.GetLayout(new PixelSize(Data(narrow).Width, float.PositiveInfinity), ScottPlot.Paint.NewDisposablePaint()).LegendRect.Height + panel.Padding.Vertical;
            Assert.True(size >= needed - 0.5f, $"legend panel {size:0.#} px is under the {needed:0.#} px the wrapped legend needs");
            Assert.True(narrow.Plot.Legend.LastRenderSize.Width <= 360, "legend is wider than the chart");
            // And at a width that holds all four labels the legend is one line.
            Assert.True(rowHeight < 40, $"a wide chart's legend is {rowHeight:0.#} px, more than one line");
        });

    [Fact]
    public void AnEmptyChart_IsOneSentence_WithNoAxesLegendOrBox() =>
        StaTestThread.Run(() =>
        {
            var chart = EmptyChart(1.0);
            using (chart.Plot.GetImage(600, 300)) { }

            Assert.Empty(chart.Plot.Axes.GetPanels().OfType<ScottPlot.Panels.LegendPanel>());
            Assert.Contains(chart.Plot.PlottableList.OfType<ScottPlot.Plottables.Text>(), t => t.LabelText.Contains("No Execution Count data") && !t.LabelItalic);
            Assert.All(chart.Plot.Axes.GetAxes(), a => Assert.False(a.IsVisible));
            Assert.Equal(chart.Plot.FigureBackground.Color, chart.Plot.DataBackground.Color);
        });

    [Fact]
    public void Columns_FollowTheWidth_AndHeight_FollowsTheVisibleHeight() =>
        StaTestThread.Run(() =>
        {
            var small = Host(1280, 720, 1.0, maxColumns: 4).Panel.Chosen;
            var mid = Host(1936, 1156, 1.0, maxColumns: 4).Panel.Chosen;
            var big = Host(3840, 2160, 1.0, maxColumns: 4).Panel.Chosen;

            Assert.True(small.Columns < big.Columns || small.Rows > big.Rows, $"1280 wide: {small.Columns}x{small.Rows}, 3840 wide: {big.Columns}x{big.Rows}");
            Assert.Equal(4, big.Columns);
            Assert.True(big.Height > mid.Height && mid.Height >= small.Height, $"chart heights {small.Height}, {mid.Height}, {big.Height} do not grow with the window");
            Assert.True(small.Height >= 280, "the floor holds on a small window");
        });

    [Fact]
    public void BelowTheFloor_TheTabScrolls_AboveItTheBlockFillsTheWindow() =>
        StaTestThread.Run(() =>
        {
            var (small, _, _) = Host(1280, 720, 1.0, maxColumns: 2);
            var (big, _, _) = Host(3840, 2160, 1.0, maxColumns: 2);

            Assert.True(small.ExtentHeight > small.ViewportHeight + 1, "a small window scrolls");
            Assert.True(big.ExtentHeight <= big.ViewportHeight + 1, $"a large window shows the whole block: extent {big.ExtentHeight:0}, viewport {big.ViewportHeight:0}");
        });

    [Fact]
    public void AnUnboundedWidth_FallsBackToTheFloor_NotInfinity() =>
        StaTestThread.Run(() =>
        {
            var panel = new ChartGridPanel { MaxColumns = 3 };
            for (var i = 0; i < 3; i++) panel.Children.Add(new Border());
            panel.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));

            Assert.True(double.IsFinite(panel.DesiredSize.Width) && double.IsFinite(panel.DesiredSize.Height));
            Assert.True(panel.DesiredSize.Height >= panel.MinChartHeight);
        });

    [Fact]
    public void AViewportBox_TakesAShareOfTheVisibleHeight_WithAFloor() =>
        StaTestThread.Run(() =>
        {
            var boxes = new List<ViewportBox>();
            foreach (var fraction in new[] { 0.3 })
            {
                foreach (var h in new[] { 720, 2160 })
                {
                    var box = new ViewportBox { Fraction = fraction, MinHeight = 180, Child = new Border() };
                    var viewer = new ScrollViewer { VerticalScrollBarVisibility = ScrollBarVisibility.Auto, Content = new StackPanel { Children = { box } } };
                    Layout(viewer, new Size(1200, h - 260));
                    boxes.Add(box);
                }
            }

            Assert.True(boxes[0].ActualHeight >= 180, "the floor holds on a small window");
            Assert.True(boxes[1].ActualHeight > boxes[0].ActualHeight * 1.5, $"a tall window gives taller grids: {boxes[0].ActualHeight} then {boxes[1].ActualHeight}");
        });

    // #8086: a legend goes on the right edge only on a chart at least three times as wide as tall with at most twelve series, else under the axis.
    [Theory]
    [InlineData(1200, 300, 4, true)]
    [InlineData(900, 300, 12, true)]
    [InlineData(1200, 300, 13, false)]
    [InlineData(1200, 400, 4, true)]
    [InlineData(1100, 400, 4, false)]
    [InlineData(700, 350, 4, false)]
    [InlineData(1200, 200, 12, false)]
    [InlineData(0, 0, 4, false)]
    public void TheLegendEdge_FollowsTheChartShape(double width, double height, int series, bool right) =>
        Assert.Equal(right ? Edge.Right : Edge.Bottom, ChartRenderHelper.LegendEdgeFor(width, height, series));

    [Fact]
    public void ARightLegend_GetsItsWholeWidth_InTheFirstRender()
    {
        var plot = new Plot();
        for (var s = 0; s < 4; s++)
        {
            plot.Add.Scatter(new double[] { 1, 2, 3 }, new double[] { s, s + 1, s + 2 }).LegendText = $"Read latency of file number {s}";
        }

        var panel = plot.ShowLegend(Edge.Right);
        plot.Legend.Orientation = ScottPlot.Orientation.Vertical;
        plot.Layout.LayoutEngine = new WrappedLegendLayout(panel);
        using (plot.GetImage(1200, 300)) { }

        var layout = plot.RenderManager.LastRender.Layout;
        using var paint = Paint.NewDisposablePaint();
        var needed = plot.Legend.GetLayout(new PixelSize(float.PositiveInfinity, layout.DataRect.Height), paint).LegendRect.Width + panel.Padding.Horizontal;
        Assert.True(layout.PanelSizes[panel] >= needed - 0.5f, $"legend panel {layout.PanelSizes[panel]:0.#} px is under the {needed:0.#} px its entries need");
        Assert.True(layout.DataRect.Width > 600, "the plot keeps most of the width");
    }
}

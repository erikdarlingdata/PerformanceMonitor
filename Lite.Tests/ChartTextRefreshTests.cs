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
using System.Windows.Media.Imaging;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8224: the text on and around Lite's charts, read back from the realized controls with a theme loaded: a Y-axis unit is one
/// regular-weight size on every chart and the perfmon chart's mixed title fits at that size; the time slicer names its metric in
/// its header and not over the strip, and no hour label sits under the resting start handle; the Query Heatmap has one title, a
/// normal-size colour bar label and a visible colour for a count of one; the Overview's blocking lane labels whole counts; the
/// five Overview lanes fit a 1280x800 window at the shared plot-area floor and their captions hide the gridline behind them.
/// </summary>
[Collection("credential-manager")]
public sealed class ChartTextRefreshTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    [Theory]
    [MemberData(nameof(Themes))]
    public void AYAxisUnit_IsDrawnRegularWeightAtTheBaseSize(string theme)
    {
        var (bold, size) = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var chart = new WpfPlot();
            ChartStyle.ApplyThemeToChart(chart);
            var xs = Enumerable.Range(0, 24).Select(i => DateTime.UtcNow.AddHours(i - 24).ToOADate()).ToArray();
            chart.Plot.Add.TimeSeries(xs, xs.Select((_, i) => 10.0 + i).ToArray());
            chart.Plot.YLabel("MB/s");
            var window = new Window { Content = chart, Width = 480, Height = 320, Left = 0, Top = 0 };
            window.Show();
            try
            {
                ChartLayoutProbe.Settle(window, [chart]);
                Shot(window, $"y-unit-{theme}");
                return (chart.Plot.Axes.Left.Label.Bold, (double)chart.Plot.Axes.Left.Label.FontSize);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.False(bold, "the Y-axis unit is drawn bold");
        Assert.Equal(ChartAxisTitle.BaseFontSize, size, 0.01);
    }

    [Fact]
    public void ThePerfmonMixedAxisTitle_FitsA340PixelChartAtTheBaseSize()
    {
        var pairs = new[]
        {
            new[] { DeltaBasis.PerSecond, DeltaBasis.Level },
            new[] { DeltaBasis.PerSecond, DeltaBasis.PerInterval },
            new[] { DeltaBasis.PerInterval, DeltaBasis.Level },
        };
        var room = ChartAxisTitle.AvailableFor(340, null);
        foreach (var pair in pairs)
        {
            var label = DeltaSeriesShaping.YAxisLabel(pair);
            var (text, size) = ChartAxisTitle.Fit(label, room);
            Assert.True(text == label && Math.Abs(size - ChartAxisTitle.BaseFontSize) < 0.01,
                $"\"{label}\" ({label.Length} characters) is drawn as \"{text}\" at {size} points on a 340 px chart");
        }
    }

    [Fact]
    public void TheSlicersMetricName_IsInItsHeader_AndNoHourLabelSitsUnderTheStartHandle()
    {
        var (onCanvas, inHeader, labelLefts) = StaTestThread.Run(() => WithTheme("Dark", () =>
        {
            var slicer = new TimeRangeSlicerControl();
            var window = new Window { Content = slicer, Width = 700, Height = 220, Left = 0, Top = 0 };
            window.Show();
            try
            {
                var end = new DateTime(2026, 3, 10, 18, 0, 0, DateTimeKind.Utc);
                var buckets = Enumerable.Range(0, 24 * 12)
                    .Select(i => new TimeSliceBucket { BucketTime = end.AddMinutes(-5 * (24 * 12 - 1 - i)), TotalCpu = 100 + (i % 7) * 40, SessionCount = 1 })
                    .ToList();
                slicer.LoadData(buckets, "Total CPU (ms)");
                ClippedTextProbe.Settle(window);
                slicer.Redraw();
                ClippedTextProbe.Settle(window);
                Shot(window, "slicer-Dark");

                var canvas = (Canvas)slicer.FindName("SlicerCanvas");
                var canvasText = canvas.Children.OfType<TextBlock>().ToList();
                var onCanvas = canvasText.Count(t => t.Text == "Total CPU (ms)");
                var inHeader = Descendants<TextBlock>(slicer).Count(t => t.Text == "Total CPU (ms)" && t.IsVisible && !canvasText.Contains(t));
                var lefts = canvasText.Where(t => t.Text.Contains(':')).Select(t => Canvas.GetLeft(t)).ToList();
                return (onCanvas, inHeader, lefts);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(onCanvas == 0, "the metric name is drawn over the strip");
        Assert.True(inHeader == 1, $"the metric name appears {inHeader} times in the header");
        Assert.NotEmpty(labelLefts);
        Assert.All(labelLefts, left => Assert.True(left >= 8, $"an hour label starts at {left:F0}, under the 8 px start handle"));
    }

    [Fact]
    public void TheQueryHeatmap_HasOneTitle_ANormalSizeBarLabel_AndAVisibleColourForOne()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ChartTextRefresh_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var (plotTitle, barBold, barSize, contrast) = StaTestThread.Run(() => WithTheme("Dark", () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "heatmap-test", DisplayName = "heatmap-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    ClippedTextProbe.Settle(window);
                    const int rows = 7, cols = 12;
                    var result = new HeatmapResult
                    {
                        Intensities = new double[rows, cols],
                        TimeBuckets = Enumerable.Range(0, cols).Select(i => new DateTime(2026, 3, 10, i, 0, 0, DateTimeKind.Utc)).ToArray(),
                        BucketLabels = ["0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s"],
                        CellDetails = new HeatmapCell[rows, cols],
                    };
                    for (var r = 0; r < rows; r++)
                        for (var c = 0; c < cols; c++)
                            result.Intensities[r, c] = (r + c) % 3 == 0 ? 0 : 1 + r * 40 + c;
                    typeof(ServerTab).GetMethod("UpdateQueryHeatmapChart", Instance)!.Invoke(tab, [result]);

                    var chart = (WpfPlot)tab.FindName("QueryHeatmapChart");
                    var heatmap = chart.Plot.PlottableList.OfType<ScottPlot.Plottables.Heatmap>().Single();
                    var bar = chart.Plot.Axes.GetPanels().OfType<ScottPlot.Panels.ColorBar>().Single();
                    var values = heatmap.Intensities.Cast<double>().Where(v => !double.IsNaN(v)).ToList();
                    var range = heatmap.ManualRange ?? new ScottPlot.Range(values.Min(), values.Max());
                    var one = Math.Log(2); // a count of one, after the chart's log1p scaling
                    var fraction = (one - range.Min) / (range.Max - range.Min);
                    var cell = heatmap.Colormap.GetColor(Math.Clamp(fraction, 0, 1));
                    return (chart.Plot.Axes.Title.Label.Text, bar.LabelStyle.Bold, (double)bar.LabelStyle.FontSize, Contrast(cell, chart.Plot.DataBackground.Color));
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.True(string.IsNullOrEmpty(plotTitle), $"the plot draws its own title \"{plotTitle}\" under the card's");
            Assert.False(barBold, "the colour bar label is bold");
            Assert.Equal(ChartAxisTitle.BaseFontSize, barSize, 0.01);
            Assert.True(contrast >= 1.8, $"a count of one is drawn at {contrast:F2}:1 against the Dark data background");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Theory]
    [MemberData(nameof(ThemesAndWindows))]
    public void TheQueryHeatmapColourBar_LabelsDoNotTouch_WhenTheMaximumSitsJustPastARoundValue(string theme, int width, int height)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ChartTextRefresh_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var (labels, overlaps) = StaTestThread.Run(() => WithTheme(theme, () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "heatmap-ticks", DisplayName = "heatmap-ticks", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    var chart = (WpfPlot)tab.FindName("QueryHeatmapChart");
                    SelectTabsAbove(chart);
                    ClippedTextProbe.Settle(window);
                    typeof(ServerTab).GetMethod("UpdateQueryHeatmapChart", Instance)!.Invoke(tab, [HeatmapToppingOutAt(252)]);
                    ChartLayoutProbe.Settle(window, [chart]);
                    Shot(window, $"colourbar-{theme}-{width}x{height}");
                    return ColourBarLabels(chart);
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.Contains("252", labels);
            Assert.True(overlaps.Count == 0, $"colour bar labels touch at {width}x{height} in {theme}: " + string.Join("; ", overlaps));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void TheOverviewBlockingLane_LabelsWholeCountsOnly()
    {
        var ticks = StaTestThread.Run(() => WithTheme("Dark", () =>
        {
            var control = new CorrelatedTimelineLanesControl();
            control.Initialize(null!, 1, () => TimeZoneInfo.Utc);
            var window = new Window { Content = control, Width = 1280, Height = 800, Left = 0, Top = 0 };
            window.Show();
            try
            {
                var to = DateTime.UtcNow;
                var from = to.AddHours(-24);
                control.UpdateBlockingLane(Series(from, to, 0, 1), Series(from, to, 0, 1).Take(5).ToList());
                control.SyncXAxes(24, null, null);
                ChartLayoutProbe.Settle(window, [control.BlockingChart]);
                return control.BlockingChart.Plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor).Select(t => t.Position).ToList();
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.NotEmpty(ticks);
        Assert.All(ticks, p => Assert.True(Math.Abs(p - Math.Round(p)) < 1e-9, $"the blocking lane labels {p}"));
    }

    public static TheoryData<string, int, int> ThemesAndWindows => new()
    {
        { "Dark", 1280, 800 }, { "Light", 1280, 800 }, { "CoolBreeze", 1280, 800 },
        { "Dark", 900, 600 }, { "Light", 900, 600 }, { "CoolBreeze", 900, 600 },
    };

    [Theory]
    [MemberData(nameof(ThemesAndWindows))]
    public void TheOverviewLanes_KeepThePlotFloor_FitA1280x800Window_AndTheirCaptionsHideTheGridline(string theme, int width, int height)
    {
        var (scrollable, plotHeights, captionsOffBackground, waitBandAlpha) = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var control = new CorrelatedTimelineLanesControl();
            control.Initialize(null!, 1, () => TimeZoneInfo.Utc);
            var window = new Window { Content = control, Width = width, Height = height, Left = 0, Top = 0 };
            window.Show();
            try
            {
                var to = DateTime.UtcNow;
                var from = to.AddHours(-24);
                var steps = Enumerable.Range(0, 24).Select(i => new BaselineBand.HourStep(from.AddHours(i).ToOADate(), from.AddHours(i + 1).ToOADate(), 450, 300, 600)).ToList();
                control.UpdateCpuLane(Series(from, to, 5, 90), Series(from, to, 8, 95));
                control.UpdateLane(control.WaitStatsChart, "Wait ms/sec", Series(from, to, 150, 900), "#FFB74D", bands: steps, minAnomalyValue: 100);
                control.UpdateBlockingLane(Series(from, to, 1, 6), Series(from, to, 0, 2).Take(5).ToList());
                control.UpdateLane(control.MemoryChart, "Buffer Pool MB", Series(from, to, 2000, 9000), "#4FC3F7");
                control.UpdateLane(control.FileIoChart, "I/O Latency ms", Series(from, to, 1, 40), "#81C784");
                control.SyncXAxes(24, null, null);
                var lanes = new[] { control.CpuChart, control.WaitStatsChart, control.BlockingChart, control.MemoryChart, control.FileIoChart };
                ChartLayoutProbe.Settle(window, lanes);
                Shot(window, $"overview-lanes-{theme}-{width}x{height}");

                var scroll = Descendants<ScrollViewer>(control).First();
                var dataBackground = ChartStyle.GetThemeColors().DataBackground;
                var expected = Color.FromRgb(dataBackground.R, dataBackground.G, dataBackground.B);
                var off = Descendants<TextBlock>(control)
                    .Where(t => t.IsVisible && t.Name.EndsWith("Caption", StringComparison.Ordinal))
                    .Where(t => t.Background is not SolidColorBrush b || b.Color != expected)
                    .Select(t => t.Name)
                    .ToList();
                var band = control.WaitStatsChart.Plot.PlottableList.OfType<ScottPlot.Plottables.Polygon>().Select(p => (int)p.FillColor.A).DefaultIfEmpty(-1).Max();
                return (scroll.ScrollableHeight, lanes.Select(l => (double)l.Plot.RenderManager.LastRender.DataRect.Height).ToList(), off, band);
            }
            finally
            {
                window.Close();
            }
        }));

        if (height >= 800)
            Assert.True(scrollable < 0.5, $"the lanes scroll by {scrollable:F0} px in a {width}x{height} window (plots {string.Join(", ", plotHeights.Select(h => h.ToString("F0")))} px)");
        /* The shared floor every chart on the tab keeps so its ticks can draw; a lane at its minimum, scrolling or not, still keeps it. */
        Assert.All(plotHeights, h => Assert.True(h >= ChartLayout.MinPlotAreaHeight, $"a lane's plot is {h:F0} px tall, under the {ChartLayout.MinPlotAreaHeight} px floor"));
        Assert.True(captionsOffBackground.Count == 0, "these captions let the gridline through: " + string.Join(", ", captionsOffBackground));
        /* 24 of 255: a tint the amber band reads as, not a brown block, on the dark and the Cool Breeze data backgrounds. */
        Assert.True(waitBandAlpha >= 0 && waitBandAlpha <= 24, $"the Wait lane's band is filled at alpha {waitBandAlpha}");
    }

    /// <summary>A 7x12 grid of counts from 1 to 40 with one cell at <paramref name="max"/>: a maximum just past the round 200 on the colour bar.</summary>
    private static HeatmapResult HeatmapToppingOutAt(int max)
    {
        const int rows = 7, cols = 12;
        var result = new HeatmapResult
        {
            Intensities = new double[rows, cols],
            TimeBuckets = Enumerable.Range(0, cols).Select(i => new DateTime(2026, 3, 10, i, 0, 0, DateTimeKind.Utc)).ToArray(),
            BucketLabels = ["0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s"],
            CellDetails = new HeatmapCell[rows, cols],
        };
        for (var r = 0; r < rows; r++)
            for (var c = 0; c < cols; c++)
                result.Intensities[r, c] = (r + c) % 3 == 0 ? 0 : 1 + (r * cols + c) % 40;
        result.Intensities[rows - 1, cols - 1] = max;
        return result;
    }

    /// <summary>Selects every tab on the way up from <paramref name="element"/>, so it is laid out and rendered.</summary>
    private static void SelectTabsAbove(DependencyObject element)
    {
        for (var node = element; node is not null; node = LogicalTreeHelper.GetParent(node))
            if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
                tabs.SelectedItem = item;
    }

    /// <summary>The colour bar's realized major tick labels, bottom to top, and every neighbouring pair drawn closer along the bar than a
    /// label's height (the tick label font size). The bar spans the plot's data area and maps the heatmap's colour range onto it.</summary>
    private static (List<string> Labels, List<string> Overlaps) ColourBarLabels(WpfPlot chart)
    {
        var heatmap = chart.Plot.PlottableList.OfType<ScottPlot.Plottables.Heatmap>().Single();
        var bar = chart.Plot.Axes.GetPanels().OfType<ScottPlot.Panels.ColorBar>().Single();
        var values = heatmap.Intensities.Cast<double>().Where(v => !double.IsNaN(v)).ToList();
        var range = heatmap.ManualRange ?? new ScottPlot.Range(values.Min(), values.Max());
        var barHeight = (double)chart.Plot.RenderManager.LastRender.DataRect.Height;
        var ticks = bar.Axis.TickGenerator.Ticks.Where(t => t.IsMajor).OrderBy(t => t.Position).ToList();
        var labelHeight = (double)bar.Axis.TickLabelStyle.FontSize;
        var overlaps = new List<string>();
        for (var i = 1; i < ticks.Count; i++)
        {
            var px = (ticks[i].Position - ticks[i - 1].Position) / (range.Max - range.Min) * barHeight;
            if (px < labelHeight) overlaps.Add($"\"{ticks[i - 1].Label}\" and \"{ticks[i].Label}\" are {px:F0} px apart on a {barHeight:F0} px bar");
        }

        return (ticks.Select(t => t.Label).ToList(), overlaps);
    }

    private static List<(double Time, double Value)> Series(DateTime from, DateTime to, double lo, double hi) =>
        Enumerable.Range(0, 96).Select(i => (from.AddTicks((to - from).Ticks * i / 95).ToOADate(), lo + (hi - lo) * (i % 12) / 11.0)).ToList();

    /// <summary>WCAG contrast ratio of two opaque colours.</summary>
    private static double Contrast(ScottPlot.Color a, ScottPlot.Color b)
    {
        static double Channel(byte v) { var s = v / 255.0; return s <= 0.03928 ? s / 12.92 : Math.Pow((s + 0.055) / 1.055, 2.4); }
        static double Luminance(ScottPlot.Color c) => 0.2126 * Channel(c.R) + 0.7152 * Channel(c.G) + 0.0722 * Channel(c.B);
        var la = Luminance(a) + 0.05;
        var lb = Luminance(b) + 0.05;
        return Math.Max(la, lb) / Math.Min(la, lb);
    }

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match) yield return match;
            foreach (var deeper in Descendants<T>(child)) yield return deeper;
        }
    }

    /// <summary>Saves the window as a PNG under the directory PM_SHOTS_DIR names, when it names one.</summary>
    private static void Shot(Window window, string name)
    {
        var dir = Environment.GetEnvironmentVariable("PM_SHOTS_DIR");
        if (string.IsNullOrEmpty(dir) || window.Content is not FrameworkElement content || content.ActualWidth <= 0) return;
        try
        {
            Directory.CreateDirectory(dir);
            var bitmap = new RenderTargetBitmap((int)Math.Ceiling(content.ActualWidth), (int)Math.Ceiling(content.ActualHeight), 96, 96, PixelFormats.Pbgra32);
            bitmap.Render(content);
            var encoder = new PngBitmapEncoder();
            encoder.Frames.Add(BitmapFrame.Create(bitmap));
            using var file = File.Create(Path.Combine(dir, name + ".png"));
            encoder.Save(file);
        }
        catch
        {
            /* a screenshot is a convenience for the reader, never the test */
        }
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>
        {
            new() { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml") },
            new() { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") },
        };
        foreach (var dictionary in added) app.Resources.MergedDictionaries.Add(dictionary);
        try
        {
            return body();
        }
        finally
        {
            foreach (var dictionary in added) app.Resources.MergedDictionaries.Remove(dictionary);
        }
    }
}

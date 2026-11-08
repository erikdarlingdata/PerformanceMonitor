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
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's chrome that stayed on the stock Windows look, or on the dark look, after a theme switch: the scroll viewer's
/// corner square, the expander's header circle, the scroll bar thumb, the lanes' crosshair tooltip and the lanes' horizontal
/// gridlines (#8306). Each test renders the real control under each Viewer theme and reads the painted colours back.
/// </summary>
public sealed class ViewerThemeChromeLeftoversRenderTests
{
    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private static T Themed<T>(string theme, Func<Application, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = ThemeUri;
            try
            {
                ThemeManager.Apply(theme);
                return body(app);
            }
            finally
            {
                ThemeManager.Apply(previous);
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in before)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }

                ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
            }
        });

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush solid ? solid.Color : Colors.Magenta;

    private static Color Resource(Application app, string key) => ColorOf((Brush)app.FindResource(key));

    /// <summary>Shows <paramref name="content"/> at the top left of a root Grid, so a render of <see cref="Root"/> has no offset and bounds measured in it line up with its pixels.</summary>
    private static Window Show(FrameworkElement content, double width, double height)
    {
        content.HorizontalAlignment = HorizontalAlignment.Left;
        content.VerticalAlignment = VerticalAlignment.Top;
        var root = new Grid();
        root.Children.Add(content);
        var window = new Window { Content = root, Width = width, Height = height, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
        StaTestThread.ShowOffScreen(window);
        ClippedTextProbe.Settle(window);
        return window;
    }

    private static FrameworkElement Root(Window window) => (FrameworkElement)window.Content;

    private static RenderTargetBitmap Render(FrameworkElement root)
    {
        var bitmap = new RenderTargetBitmap((int)Math.Ceiling(root.ActualWidth), (int)Math.Ceiling(root.ActualHeight), 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(root);
        return bitmap;
    }

    private static Color PixelAt(RenderTargetBitmap bitmap, int x, int y)
    {
        var pixel = new byte[4];
        bitmap.CopyPixels(new Int32Rect(Math.Clamp(x, 0, bitmap.PixelWidth - 1), Math.Clamp(y, 0, bitmap.PixelHeight - 1), 1, 1), pixel, 4, 0);
        return Color.FromArgb(pixel[3], pixel[2], pixel[1], pixel[0]);
    }

    private static Rect BoundsIn(FrameworkElement element, Visual root) =>
        element.TransformToAncestor(root).TransformBounds(new Rect(0, 0, element.ActualWidth, element.ActualHeight));

    private static double Luminance(Color c)
    {
        static double Channel(byte v)
        {
            var d = v / 255.0;
            return d <= 0.03928 ? d / 12.92 : Math.Pow((d + 0.055) / 1.055, 2.4);
        }

        return 0.2126 * Channel(c.R) + 0.7152 * Channel(c.G) + 0.0722 * Channel(c.B);
    }

    private static double Contrast(Color a, Color b)
    {
        var (la, lb) = (Luminance(a), Luminance(b));
        return (Math.Max(la, lb) + 0.05) / (Math.Min(la, lb) + 0.05);
    }

    private static string Hex(Color c) => $"#{c.R:X2}{c.G:X2}{c.B:X2}";

    /// <summary>The Wait Stats wait-type list is a transparent ListBox in a card. Where its two scroll bars meet, the corner is painted in the scroll bars' own track colour, not the system's control grey.</summary>
    [Theory]
    [MemberData(nameof(Themes))]
    public void TheListBoxScrollCorner_IsTheScrollTrackColour(string theme)
    {
        var (corner, track, barsShown) = Themed(theme, app =>
        {
            var list = new ListBox { Background = Brushes.Transparent, BorderThickness = new Thickness(0), ItemsSource = Enumerable.Range(0, 60).Select(i => $"PAGEIOLATCH_SH wait type number {i} with a long enough name to need the horizontal bar").ToList() };
            var card = new Border { Background = (Brush)app.FindResource("BackgroundLightBrush"), Child = list, Width = 220, Height = 200 };
            var window = Show(card, 300, 260);
            try
            {
                var scroll = ClippedTextProbe.Descendants(list).OfType<ScrollViewer>().First();
                var bars = ClippedTextProbe.Descendants(scroll).OfType<ScrollBar>().Count(b => b.Visibility == Visibility.Visible && b.ActualWidth > 0 && b.ActualHeight > 0);
                var bitmap = Render(Root(window));
                var bounds = BoundsIn(scroll, Root(window));
                var pixel = PixelAt(bitmap, (int)(bounds.Right - 4), (int)(bounds.Bottom - 4));
                var trackColour = ColorOf(((Setter)((Style)app.FindResource(typeof(ScrollBar))).Setters.OfType<Setter>().First(s => s.Property == Control.BackgroundProperty)).Value as Brush);
                return (pixel, trackColour, bars);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(2, barsShown);
        Assert.Equal((track.R, track.G, track.B), (corner.R, corner.G, corner.B));
    }

    /// <summary>An expander header draws its chevron in the theme's own colours: nothing in the header is lighter than the theme's lightest of its background and text, or darker than the darkest, so no stock white circle shows on a dark or tinted theme.</summary>
    [Theory]
    [MemberData(nameof(Themes))]
    public void TheExpanderHeader_PaintsOnlyThemeColours_AndShowsAChevron(string theme)
    {
        var (outside, drawn, lo, hi, worst) = Themed(theme, app =>
        {
            var expander = new Expander { Header = "CPU by database", IsExpanded = false, Margin = new Thickness(4, 2, 4, 0), Content = new TextBlock { Text = "body" } };
            var panel = new Border { Background = (Brush)app.FindResource("BackgroundLightBrush"), Child = expander, Width = 300, Height = 80 };
            var window = Show(panel, 340, 120);
            try
            {
                var toggle = ClippedTextProbe.Descendants(expander).OfType<ToggleButton>().First();
                var bitmap = Render(Root(window));
                var bounds = BoundsIn(toggle, Root(window));
                var background = Resource(app, "BackgroundLightBrush");
                var text = Resource(app, "ForegroundBrush");
                var (low, high) = (Math.Min(Luminance(background), Luminance(text)) - 0.02, Math.Max(Luminance(background), Luminance(text)) + 0.02);
                var outsideRange = 0;
                var notBackground = 0;
                var worstPixel = background;
                var worstDistance = 0.0;
                for (var y = (int)bounds.Top; y < (int)bounds.Bottom; y++)
                {
                    for (var x = (int)bounds.Left; x < (int)bounds.Left + 24; x++)
                    {
                        var pixel = PixelAt(bitmap, x, y);
                        var luminance = Luminance(pixel);
                        if ((pixel.R, pixel.G, pixel.B) != (background.R, background.G, background.B)) notBackground++;
                        var distance = Math.Max(low - luminance, luminance - high);
                        if (distance > 0) outsideRange++;
                        if (distance > worstDistance) { worstDistance = distance; worstPixel = pixel; }
                    }
                }

                return (outsideRange, notBackground, low, high, worstPixel);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(drawn > 0, $"{theme}: nothing is drawn where the expander's chevron goes");
        Assert.True(outside == 0, $"{theme}: {outside} pixels of the expander header fall outside the theme's range [{lo:F3}, {hi:F3}], the worst {Hex(worst)}");
    }

    /// <summary>A grid's vertical scroll thumb reads against its track: at least 2:1, so a reader finds it without hunting.</summary>
    [Theory]
    [MemberData(nameof(Themes))]
    public void TheGridScrollThumb_ReadsAgainstItsTrack(string theme)
    {
        var (thumb, track, contrast) = Themed(theme, app =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false, HeadersVisibility = DataGridHeadersVisibility.Column, Width = 400, Height = 300 };
            grid.Columns.Add(new DataGridTextColumn { Header = "Query", Binding = new System.Windows.Data.Binding("."), Width = 200 });
            grid.ItemsSource = Enumerable.Range(0, 200).Select(i => $"query {i}").ToList();
            var window = Show(grid, 440, 340);
            try
            {
                var scroll = (ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid);
                var vertical = (ScrollBar)scroll.Template.FindName("PART_VerticalScrollBar", scroll);
                var thumbElement = ClippedTextProbe.Descendants(vertical).OfType<Thumb>().First(t => t.ActualHeight > 0 && t.ActualWidth > 0);
                var bitmap = Render(Root(window));
                var thumbBounds = BoundsIn(thumbElement, Root(window));
                var trackBounds = BoundsIn(vertical, Root(window));
                var thumbColour = PixelAt(bitmap, (int)(thumbBounds.X + thumbBounds.Width / 2), (int)(thumbBounds.Y + thumbBounds.Height / 2));
                var trackColour = PixelAt(bitmap, (int)(trackBounds.X + trackBounds.Width / 2), (int)(trackBounds.Bottom - 3));
                return (thumbColour, trackColour, Contrast(thumbColour, trackColour));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(contrast >= 2.0, $"{theme}: the thumb {Hex(thumb)} against its track {Hex(track)} has a contrast of {contrast:F2}; the floor is 2.00");
    }

    /// <summary>The Overview lanes' hover tooltip takes the theme's tooltip colours, and follows a theme switched while the app runs.</summary>
    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    [InlineData("Dark")]
    public void TheLanesCrosshairTooltip_TakesTheThemeTooltipColours_AfterARunTimeSwitch(string theme)
    {
        var (frame, text, wantFrame, wantText) = Themed("Dark", app =>
        {
            using var manager = new CorrelatedCrosshairManager();
            ThemeManager.Apply(theme);
            var popup = (Popup)typeof(CorrelatedCrosshairManager).GetField("_tooltip", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!;
            var border = (Border)popup.Child;
            var block = (TextBlock)typeof(CorrelatedCrosshairManager).GetField("_tooltipText", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!;
            var tooltipStyle = (Style)app.FindResource(typeof(ToolTip));
            Color Setter(DependencyProperty property) => ColorOf(tooltipStyle.Setters.OfType<Setter>().First(s => s.Property == property).Value as Brush);
            return (ColorOf(border.Background), ColorOf(block.Foreground), Setter(Control.BackgroundProperty), Setter(Control.ForegroundProperty));
        });

        Assert.Equal(wantFrame, frame);
        Assert.Equal(wantText, text);
    }

    private const double GridlineFloor = 1.45;

    /// <summary>With a day of data plotted, each Overview lane draws horizontal gridlines at its Y ticks that read against the plot, in every theme, in the same ink as its vertical ones.</summary>
    [Theory]
    [MemberData(nameof(Themes))]
    public void TheLanes_DrawHorizontalGridlines_ThatReadAgainstThePlot(string theme)
    {
        var results = Themed("Dark", app =>
        {
            var lanes = new CorrelatedTimelineLanesControl();
            var window = Show(lanes, 900, 600);
            try
            {
                ThemeManager.Apply(theme);
                lanes.ReapplyTheme();
                ClippedTextProbe.Settle(window);
                var start = DateTime.UtcNow.Date.AddDays(-1);
                var cpu = Enumerable.Range(0, 288).Select(i => (start.AddMinutes(5 * i).ToOADate(), 35 + 30 * Math.Sin(i / 20.0))).ToList();
                var total = cpu.Select(p => (p.Item1, Math.Min(100, p.Item2 + 12))).ToList();
                var waits = Enumerable.Range(0, 288).Select(i => (start.AddMinutes(5 * i).ToOADate(), 80 + 70 * Math.Abs(Math.Sin(i / 15.0)))).ToList();
                lanes.UpdateCpuLane(cpu, total);
                lanes.UpdateLane((ScottPlot.WPF.WpfPlot)lanes.FindName("WaitStatsChart"), "Wait ms/sec", waits, "#2EAEF1", 0);
                ClippedTextProbe.Settle(window);
                var measured = new List<(string Lane, double Horizontal, int VerticalColumns, string Background, string Line, int Ticks)>();
                foreach (var name in new[] { "CpuChart", "WaitStatsChart" })
                {
                    var chart = (ScottPlot.WPF.WpfPlot)lanes.FindName(name);
                    var (horizontal, verticalColumns, background, line, ticks) = GridlineScan.Measure(chart.Plot, (int)Math.Max(300, chart.ActualWidth), (int)Math.Max(60, chart.ActualHeight));
                    measured.Add((name, horizontal, verticalColumns, background, line, ticks));
                }

                return measured;
            }
            finally
            {
                window.Close();
            }
        });

        foreach (var (lane, horizontal, verticalColumns, background, line, ticks) in results)
        {
            Assert.True(ticks >= 1, $"{theme} {lane}: no Y tick falls inside the lane, so it can draw no horizontal gridline");
            Assert.True(horizontal >= GridlineFloor, $"{theme} {lane}: the horizontal gridline ({line}) against the plot ({background}) has a contrast of {horizontal:F2}; the floor is {GridlineFloor:F2}");
            Assert.True(verticalColumns >= 1, $"{theme} {lane}: no column of a row near the top carries the horizontal gridlines' ink ({line}); the vertical gridlines are not drawn in it");
        }
    }

    /// <summary>
    /// Renders a lane's plot at its own size and limits and reads its gridlines back. The horizontal lines are read at the rows of the
    /// Y axis' major ticks inside the plot, down a column near the left edge (ahead of the plotted points), as the contrast of their
    /// ink against the plot's background; the vertical ones as how many columns of a row near the top carry that ink, blended over the
    /// background or not, since the plotted series can cross any row but never in the gridlines' colour.
    /// </summary>
    private static class GridlineScan
    {
        public static (double Horizontal, int VerticalColumns, string Background, string Line, int InteriorTicks) Measure(ScottPlot.Plot plot, int width, int height)
        {
            var image = plot.GetImage(width, height);
            var rgb = image.GetArrayRGB();
            var rect = plot.RenderManager.LastRender.DataRect;
            var range = plot.Axes.Left.Range;
            var column = (int)rect.Left + 10;
            var top = (int)rect.Top + 2;
            var bottom = (int)rect.Bottom - 2;
            var columnColours = Enumerable.Range(top, bottom - top).Select(y => Key(rgb, y, column)).ToList();
            var background = columnColours.GroupBy(k => k).OrderByDescending(g => g.Count()).First().Key;
            var backgroundLuminance = Luminance(background);

            var interior = plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor && t.Position > range.Min && t.Position < range.Max).ToList();
            var horizontal = double.MaxValue;
            var line = background;
            foreach (var tick in interior)
            {
                var y = (int)Math.Round(rect.Top + rect.Height * (range.Max - tick.Position) / (range.Max - range.Min));
                var ink = Enumerable.Range(y - 1, 3).Select(row => Key(rgb, Math.Clamp(row, top, bottom - 1), column))
                    .OrderByDescending(k => Math.Abs(Luminance(k) - backgroundLuminance)).First();
                var ratio = Ratio(backgroundLuminance, Luminance(ink));
                if (ratio < horizontal)
                {
                    horizontal = ratio;
                    line = ink;
                }
            }

            var row = (int)(rect.Top + rect.Height * 0.12);
            var verticalColumns = Enumerable.Range((int)rect.Left + 2, (int)rect.Width - 4).Select(x => Key(rgb, row, x)).Count(k => k != background && Between(k, background, line));
            return (interior.Count == 0 ? 0 : horizontal, verticalColumns, Hex(background), Hex(line), interior.Count);
        }

        /// <summary>Whether every channel of <paramref name="k"/> lies between the background's and the ink's (within 6): a gridline blended over the background by anti-aliasing does, a plotted series' colour does not.</summary>
        private static bool Between(int k, int background, int ink)
        {
            for (var shift = 0; shift <= 16; shift += 8)
            {
                var v = (k >> shift) & 0xFF;
                var a = (background >> shift) & 0xFF;
                var b = (ink >> shift) & 0xFF;
                if (v < Math.Min(a, b) - 6 || v > Math.Max(a, b) + 6)
                {
                    return false;
                }
            }

            return true;
        }

        private static int Key(byte[,,] rgb, int y, int x) => (rgb[y, x, 0] << 16) | (rgb[y, x, 1] << 8) | rgb[y, x, 2];

        private static string Hex(int rgb) => $"#{rgb:X6}";

        private static double Ratio(double a, double b) => (Math.Max(a, b) + 0.05) / (Math.Min(a, b) + 0.05);

        private static double Luminance(int rgb) =>
            ViewerThemeChromeLeftoversRenderTests.Luminance(Color.FromRgb((byte)((rgb >> 16) & 0xFF), (byte)((rgb >> 8) & 0xFF), (byte)(rgb & 0xFF)));
    }
}

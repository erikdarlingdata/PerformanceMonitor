/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Models;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The chrome that stayed on the stock Windows look, or on the dark look, after a theme switch: the scroll viewer's corner
/// square, the expander's header circle, the scroll bar thumb and the lanes' crosshair tooltip (#8306). Each test renders the real control under each theme and reads the painted colours back, so a theme
/// that paints a stock white square or a tooltip in another theme's colours fails here. The sidebar rail and the footer are in
/// <see cref="SidebarRailRenderTests"/>.
/// </summary>
[Collection("credential-manager")]
public sealed class ThemeChromeLeftoversRenderTests
{
    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");

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

    /// <summary>A 96 dpi render of <paramref name="root"/>: one pixel per device-independent unit.</summary>
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

    // ── Item 2: the scroll viewer's corner ──────────────────────────────────────────────────────

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

    // ── Item 3: the expander's header ───────────────────────────────────────────────────────────

    /// <summary>An expander header ("CPU by database", the Recommendations bands) draws its chevron in the theme's own colours: nothing in the header is lighter than the theme's lightest of its background and text, or darker than the darkest, so no stock white circle shows on a dark or tinted theme.</summary>
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

    // ── Item 4: the scroll bar thumb ────────────────────────────────────────────────────────────

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

    // ── Item 6: the lanes' crosshair tooltip ────────────────────────────────────────────────────

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
            var host = new Border();
            var window = Show(host, 200, 100);
            try
            {
                manager.FollowThemeWhileLoaded(host);
                ThemeManager.Apply(theme);
                var popup = (Popup)typeof(CorrelatedCrosshairManager).GetField("_tooltip", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!;
                var border = (Border)popup.Child;
                var block = (TextBlock)typeof(CorrelatedCrosshairManager).GetField("_tooltipText", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(manager)!;
                var tooltipStyle = (Style)app.FindResource(typeof(ToolTip));
                Color Setter(DependencyProperty property) => ColorOf(tooltipStyle.Setters.OfType<Setter>().First(s => s.Property == property).Value as Brush);
                return (ColorOf(border.Background), ColorOf(block.Foreground), Setter(Control.BackgroundProperty), Setter(Control.ForegroundProperty));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(wantFrame, frame);
        Assert.Equal(wantText, text);
    }

}

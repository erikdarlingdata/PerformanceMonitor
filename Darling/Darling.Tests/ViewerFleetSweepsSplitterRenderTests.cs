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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The two drag handles on the Viewer's Fleet Sweeps tab, between the timeline, the selected sweep and the watch items, can be seen in
/// every theme (#8306). Each sets no colour of its own, so it paints whatever the theme gives a splitter; the test renders the tab on
/// the tab strip's page colour and reads the handles back.
/// </summary>
public sealed class ViewerFleetSweepsSplitterRenderTests
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

    private static IEnumerable<DependencyObject> Descendants(DependencyObject parent)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);
            yield return child;
            foreach (var grandchild in Descendants(child))
            {
                yield return grandchild;
            }
        }
    }

    private static Color PixelAt(RenderTargetBitmap bitmap, int x, int y)
    {
        var pixel = new byte[4];
        bitmap.CopyPixels(new Int32Rect(Math.Clamp(x, 0, bitmap.PixelWidth - 1), Math.Clamp(y, 0, bitmap.PixelHeight - 1), 1, 1), pixel, 4, 0);
        return Color.FromArgb(pixel[3], pixel[2], pixel[1], pixel[0]);
    }

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

    /// <summary>Each of the tab's two splitters, rendered on the tab strip's page colour, reads against it at 2:1 or better.</summary>
    [Theory]
    [MemberData(nameof(Themes))]
    public void TheFleetSweepsSplitters_ReadAgainstThePage(string theme)
    {
        var (page, splitters) = Themed(theme, app =>
        {
            var tab = new FleetSweepsTab();
            var host = new Border { Background = (Brush)app.FindResource("BackgroundBrush"), Child = tab, Width = 900, Height = 600, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            var root = new Grid();
            root.Children.Add(host);
            var window = new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(window);
            ClippedTextProbe.Settle(window);
            try
            {
                var bitmap = new RenderTargetBitmap((int)Math.Ceiling(root.ActualWidth), (int)Math.Ceiling(root.ActualHeight), 96, 96, PixelFormats.Pbgra32);
                bitmap.Render(root);
                var page = ColorOf(host.Background);
                var found = Descendants(tab).OfType<GridSplitter>().Select(splitter =>
                {
                    var bounds = splitter.TransformToAncestor(root).TransformBounds(new Rect(0, 0, splitter.ActualWidth, splitter.ActualHeight));
                    var ink = PixelAt(bitmap, (int)(bounds.X + bounds.Width / 2), (int)(bounds.Y + bounds.Height / 2));
                    return (Row: Grid.GetRow(splitter), Ink: ink, Contrast: Contrast(ink, page));
                }).ToList();
                return (page, found);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(2, splitters.Count);
        Assert.All(splitters, s => Assert.True(s.Contrast >= 2.0, $"the Fleet Sweeps splitter in row {s.Row} paints {Hex(s.Ink)} on the page's {Hex(page)} under {theme}, a contrast of {s.Contrast:F2}: under 2:1 the drag handle cannot be seen"));
    }
}

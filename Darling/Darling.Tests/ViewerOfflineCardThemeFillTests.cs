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
/// #8304, item 6, the viewer's half: an offline server's Overview card reads the same in Dark, Light and Cool Breeze. Its fill is
/// the card background every card has, with nothing drawn over it, so the red "Offline" status and the Last Collect value sit on
/// the same surface a healthy card's text does and clear the text contrast floor against it. Each theme is applied the way Settings
/// applies it, two cards from the Overview's own template (one offline, one healthy) are rendered at 900x600, and the pixels inside
/// each card's padding are sampled beside the status line, the Last Collect row and the Memory row.
/// </summary>
public sealed class ViewerOfflineCardThemeFillTests
{
    private const double CardWidth = 300;

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private static string Hex(Color c) => $"#{c.R:X2}{c.G:X2}{c.B:X2}";

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush s ? s.Color : Colors.Transparent;

    private static ServerSummaryItem Card(bool online) => new()
    {
        ServerId = online ? 1 : 2,
        ServerName = online ? "calm" : "dark",
        DisplayName = online ? "calm" : "dark",
        IsOnline = online,
        CpuPercent = 20,
        OtherProcessCpuPercent = 3,
        MemoryMb = 32768,
        CollectorCount = 40,
        HealthyCollectorCount = online ? 40 : 0,
        FailedCollectorCount = online ? 0 : 40,
        /* Two days back for the dark card, so its Last Collect carries a date. */
        LastCollectionTime = online ? DateTime.UtcNow.AddMinutes(-1) : DateTime.UtcNow.AddDays(-2),
    };

    /// <summary>What one rendered card shows: the fill sampled beside three rows, and the ink of the two texts.</summary>
    private sealed record Sample(Color FillAtStatus, Color FillAtLastCollect, Color FillAtMemory, Color StatusInk, Color LastCollectInk, string StatusText, string LastCollectText);

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject =>
        ClippedTextProbe.Descendants(root).OfType<T>();

    private static (Sample Offline, Sample Healthy, Color CardBackground) Rendered(string theme) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = name => ThemeUri(name);
            try
            {
                ThemeManager.Apply(theme);
                var template = OverviewCardTemplate();
                var offlineItem = Card(online: false);
                var healthyItem = Card(online: true);
                var offline = new ContentControl { Content = offlineItem, ContentTemplate = template };
                var healthy = new ContentControl { Content = healthyItem, ContentTemplate = template };
                /* The Overview's page surface behind the cards, so the host's own box is what the snapshot sees. */
                var host = new StackPanel { Orientation = Orientation.Horizontal, VerticalAlignment = VerticalAlignment.Top, Background = (Brush)app.FindResource("BackgroundBrush"), Children = { offline, healthy } };
                var window = new Window { Content = host, Width = 900, Height = 600, Left = 0, Top = 0, SizeToContent = SizeToContent.Manual };
                window.Resources["BoolToVisibility"] = new BooleanToVisibilityConverter();
                StaTestThread.ShowOffScreen(window);
                try
                {
                    ClippedTextProbe.Settle(window);
                    var pixels = Snapshot(host);
                    return (SampleCard(host, pixels, offline, offlineItem), SampleCard(host, pixels, healthy, healthyItem), ColorOf((Brush)app.FindResource("BackgroundLightBrush")));
                }
                finally
                {
                    window.Close();
                }
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

    /// <summary>The host as rendered, drawn through a VisualBrush whose viewbox is the host's own layout box, so a bitmap pixel at (x, y)
    /// is the host's layout point (x, y) and nothing is shifted or scaled by where its content happens to start.</summary>
    private static RenderTargetBitmap Snapshot(FrameworkElement host)
    {
        var width = (int)Math.Ceiling(host.ActualWidth);
        var height = (int)Math.Ceiling(host.ActualHeight);
        var drawing = new DrawingVisual();
        using (var context = drawing.RenderOpen())
        {
            var brush = new VisualBrush(host)
            {
                ViewboxUnits = BrushMappingMode.Absolute,
                Viewbox = new Rect(0, 0, width, height),
                Stretch = Stretch.None,
                AlignmentX = AlignmentX.Left,
                AlignmentY = AlignmentY.Top,
            };
            context.DrawRectangle(brush, null, new Rect(0, 0, width, height));
        }

        var bitmap = new RenderTargetBitmap(width, height, 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(drawing);
        return bitmap;
    }

    private static Color Pixel(RenderTargetBitmap bitmap, double x, double y)
    {
        var px = new byte[4];
        bitmap.CopyPixels(new Int32Rect((int)Math.Round(x), (int)Math.Round(y), 1, 1), px, 4, 0);
        return Color.FromArgb(px[3], px[2], px[1], px[0]);
    }

    private static Sample SampleCard(FrameworkElement host, RenderTargetBitmap pixels, FrameworkElement card, ServerSummaryItem item)
    {
        var border = Descendants<Border>(card).First(b => b.Width == CardWidth);
        var status = Descendants<TextBlock>(card).First(t => t.Text == item.StatusDisplay);
        var lastCollectLabel = Descendants<TextBlock>(card).First(t => t.Text == "Last Collect:");
        var lastCollect = Descendants<TextBlock>(card).First(t => t.Parent == lastCollectLabel.Parent && Grid.GetRow(t) == Grid.GetRow(lastCollectLabel) && Grid.GetColumn(t) == 2);
        var memory = Descendants<TextBlock>(card).First(t => t.Text == "Memory:");

        /* The sample column sits inside the card's 2 px border, in the 16 px padding to the right of every row, where nothing is drawn. */
        var borderBox = border.TransformToAncestor(host).TransformBounds(new Rect(border.RenderSize));
        var x = borderBox.Right - 2 - 8;
        Color Beside(FrameworkElement row)
        {
            var box = row.TransformToAncestor(host).TransformBounds(new Rect(row.RenderSize));
            return Pixel(pixels, x, box.Top + box.Height / 2);
        }

        return new Sample(Beside(status), Beside(lastCollect), Beside(memory), ColorOf(status.Foreground), ColorOf(lastCollect.Foreground), status.Text, lastCollect.Text);
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void AnOfflineCard_HasAHealthyCardsFill_AndReadableText(string theme)
    {
        var (offline, healthy, background) = Rendered(theme);

        Assert.Equal("Offline", offline.StatusText);
        Assert.Matches(@"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", offline.LastCollectText);

        /* The fill: the card background, the same as a healthy card's, beside every row sampled. */
        foreach (var (where, dark, calm) in new[]
        {
            ("status line", offline.FillAtStatus, healthy.FillAtStatus),
            ("Last Collect row", offline.FillAtLastCollect, healthy.FillAtLastCollect),
            ("Memory row", offline.FillAtMemory, healthy.FillAtMemory),
        })
        {
            Assert.True(dark == calm, $"{theme}: beside the {where} the offline card is {Hex(dark)} but a healthy card is {Hex(calm)}");
            Assert.True(dark == background, $"{theme}: beside the {where} the offline card is {Hex(dark)}, not the card background {Hex(background)}");
        }

        /* The ink: the red status word and the Last Collect value clear the text floor against what is behind them. */
        var statusRatio = WcagContrast.Ratio(offline.StatusInk, offline.FillAtStatus);
        var lastCollectRatio = WcagContrast.Ratio(offline.LastCollectInk, offline.FillAtLastCollect);
        Assert.True(statusRatio >= WcagContrast.TextMinimum,
            $"{theme}: \"Offline\" in {Hex(offline.StatusInk)} on {Hex(offline.FillAtStatus)} is {WcagContrast.Format(statusRatio)}, under {WcagContrast.TextMinimum}:1");
        Assert.True(lastCollectRatio >= WcagContrast.TextMinimum,
            $"{theme}: \"{offline.LastCollectText}\" in {Hex(offline.LastCollectInk)} on {Hex(offline.FillAtLastCollect)} is {WcagContrast.Format(lastCollectRatio)}, under {WcagContrast.TextMinimum}:1");
    }

    /// <summary>
    /// The card template lives inline in MainWindow.xaml, so the shell is built (never shown) to read it. Its constructor seeds the
    /// process-wide time display mode from the app settings; that is put back so the other classes see what they set.
    /// </summary>
    private static DataTemplate OverviewCardTemplate()
    {
        var mode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            var shell = new MainWindow();
            return ((ItemsControl)shell.FindName("OverviewItemsControl")).ItemTemplate;
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
        }
    }
}

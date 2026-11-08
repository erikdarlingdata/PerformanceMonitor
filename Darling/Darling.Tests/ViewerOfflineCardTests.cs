/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8304, the viewer's half: an offline server's Overview card says Offline once, under its name inside the red border, with no
/// second "Offline" and no red cross drawn over its metric rows; its Blocking and Deadlocks rows read "--" with the unknown grey dot,
/// as CPU does; and a dated Last Collect stamp fits inside the card. The card is the Overview's own template, rendered with the
/// theme loaded and read back.
/// </summary>
public sealed class ViewerOfflineCardTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";
    private const string UnknownGrey = "#888888";
    private const double CardWidth = 300;

    private static string Hex(Brush? brush) =>
        brush is SolidColorBrush { Color: var c } ? $"#{c.R:X2}{c.G:X2}{c.B:X2}" : "(not a solid colour)";

    private static ServerSummaryItem OfflineCard() => new()
    {
        ServerId = 1,
        ServerName = "dark",
        DisplayName = "dark",
        IsOnline = false,
        CpuPercent = 20,
        OtherProcessCpuPercent = 3,
        MemoryMb = 32768,
        BlockingCount = 3,
        DeadlockCount = 2,
        CollectorCount = 40,
        FailedCollectorCount = 40,
        /* Two days back, so the stamp carries its date in every display zone. */
        LastCollectionTime = DateTime.UtcNow.AddDays(-2),
    };

    private static T Shown<T>(ServerSummaryItem item, Func<Border, T> body) =>
        StaTestThread.Run(() => WithTheme(() =>
        {
            var host = new ContentControl
            {
                Content = item,
                ContentTemplate = OverviewCardTemplate(),
                HorizontalAlignment = HorizontalAlignment.Left,
                VerticalAlignment = VerticalAlignment.Top,
            };
            var window = new Window { Content = host, Width = 900, Height = 600, Left = 0, Top = 0 };
            window.Resources["BoolToVisibility"] = new BooleanToVisibilityConverter();
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var card = ClippedTextProbe.Descendants(host).OfType<Border>().First(b => b.Width == CardWidth);
                return body(card);
            }
            finally
            {
                window.Close();
            }
        }));

    /// <summary>The value cell and the dot on the metric row labelled <paramref name="label"/>.</summary>
    private static (TextBlock Value, Ellipse? Dot) Row(Border card, string label)
    {
        var labelBlock = ClippedTextProbe.Descendants(card).OfType<TextBlock>().First(t => t.Text == label);
        var row = Grid.GetRow(labelBlock);
        var value = ClippedTextProbe.Descendants(card).OfType<TextBlock>().First(t => t.Parent == labelBlock.Parent && Grid.GetRow(t) == row && Grid.GetColumn(t) == 2);
        var dot = ClippedTextProbe.Descendants(card).OfType<Ellipse>().FirstOrDefault(e => e.Width == 9 && Grid.GetRow(e) == row && e.Parent == labelBlock.Parent);
        return (value, dot);
    }

    [Fact]
    public void AnOfflineCard_SaysOfflineOnce_AndDrawsNoCrossOverItsRows()
    {
        var found = Shown(OfflineCard(), card =>
        {
            var visible = ClippedTextProbe.Descendants(card).OfType<TextBlock>().Where(t => t.IsVisible).Select(t => t.Text).ToList();
            return (Offline: visible.Count(t => t == "Offline"), Cross: visible.Count(t => t == "✗"));
        });

        Assert.Equal(1, found.Offline);
        Assert.Equal(0, found.Cross);
    }

    [Fact]
    public void AnOfflineCard_ReadsNoBlockingOrDeadlockFigure_AndPaintsThoseDotsGrey()
    {
        var rows = Shown(OfflineCard(), card =>
        {
            var blocking = Row(card, "Blocking:");
            var deadlocks = Row(card, "Deadlocks:");
            return (Blocking: blocking.Value.Text, BlockingDot: Hex(blocking.Dot?.Fill), Deadlocks: deadlocks.Value.Text, DeadlockDot: Hex(deadlocks.Dot?.Fill));
        });

        Assert.Equal("--", rows.Blocking);
        Assert.Equal(UnknownGrey, rows.BlockingDot);
        Assert.Equal("--", rows.Deadlocks);
        Assert.Equal(UnknownGrey, rows.DeadlockDot);
    }

    [Fact]
    public void TheDatedLastCollectValue_FitsInsideTheCard()
    {
        var (text, clipped, rightEdge, cardWidth, slot, width) = Shown(OfflineCard(), card =>
        {
            var value = Row(card, "Last Collect:").Value;
            var right = value.TransformToAncestor(card).TransformBounds(new Rect(value.RenderSize)).Right;
            /* Layout clips a block whose rendered size is wider than the cell it was given; the person sees the cut, not the card edge. */
            var layoutClip = LayoutInformation.GetLayoutClip(value);
            return (value.Text, ClippedTextProbe.Clipped(card, card), right, card.ActualWidth, layoutClip is null ? value.ActualWidth : layoutClip.Bounds.Width, value.ActualWidth);
        });

        Assert.Matches(@"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", text);
        Assert.True(clipped.Count == 0, "cut off: " + string.Join("; ", clipped));
        Assert.True(slot + 0.5 >= width, $"\"{text}\" renders {width:F0} wide in a {slot:F0} wide cell, so its end is cut");
        Assert.True(rightEdge <= cardWidth - 2 + 0.5, $"the Last Collect value ends at {rightEdge:F0}, past the {cardWidth:F0} wide card's border");
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

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("BackgroundLightBrush"))
        {
            added = new ResourceDictionary { Source = new Uri(ThemeUri) };
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

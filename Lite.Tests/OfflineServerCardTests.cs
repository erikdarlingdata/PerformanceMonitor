/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8304: an offline server's Overview card. The card already says Offline under the server name, inside a red border, so it
/// draws no second "Offline" and no red cross over its metric rows; Blocking and Deadlocks read "--" with the card's unknown grey,
/// the way CPU does, instead of a green "0" or an amber count the server cannot have reported; and the Last Collect value, with its
/// date and its state word, fits inside the card at a 900x600 window. Each card is the real control, bound as the Overview binds
/// it, laid out with the theme loaded and read back from what rendered.
/// </summary>
[Collection("credential-manager")]
public sealed class OfflineServerCardTests
{
    private const string UnknownGrey = "#888888";
    private const string Amber = "#FFB74D";
    private const string Red = "#E57373";

    private static readonly Uri LiteTheme = new("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml");
    private static readonly Uri Shared = new("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml");

    private static string Hex(Brush? brush) =>
        brush is SolidColorBrush { Color: var c } ? $"#{c.R:X2}{c.G:X2}{c.B:X2}" : "(not a solid colour)";

    /// <summary>A card with counts in its last hour and a dated Last Collect stamp, reachable or not as the test says.</summary>
    private static ServerSummaryItem Card(bool? isOnline, ServerFreshness freshness) => new()
    {
        DisplayName = "offline-card",
        ServerName = "offline-card",
        IsOnline = isOnline,
        CpuPercent = 20,
        OtherProcessCpuPercent = 5,
        MemoryMb = 32768,
        BlockingCount = 3,
        DeadlockCount = 2,
        /* Two days back, so the stamp carries its date in every display zone. */
        LastCollectionTime = DateTime.UtcNow.AddDays(-2),
        CollectionFreshness = freshness,
    };

    private static T Shown<T>(ServerSummaryItem item, Func<FrameworkElement, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new List<ResourceDictionary> { new() { Source = LiteTheme }, new() { Source = Shared } };
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }

            try
            {
                var card = new ServerOverviewCard { DataContext = item };
                /* The Overview's wrap panel gives a card its own height; a vertical stack does the same here. */
                var host = new StackPanel { Children = { card } };
                var window = new Window { Content = host, Width = 900, Height = 600, Left = 0, Top = 0, SizeToContent = SizeToContent.Manual };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    ClippedTextProbe.Settle(window);
                    return body(card);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Remove(dictionary);
                }
            }
        });

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject =>
        ClippedTextProbe.Descendants(root).OfType<T>();

    private static Border CardBorder(FrameworkElement card) =>
        Descendants<Border>(card).First(b => b.GetType().Name == "OverviewCardBorder");

    /// <summary>The value cell and the dot on the metric row labelled <paramref name="label"/>.</summary>
    private static (TextBlock Value, Ellipse? Dot) Row(FrameworkElement card, string label)
    {
        var labelBlock = Descendants<TextBlock>(card).First(t => t.Text == label);
        var row = Grid.GetRow(labelBlock);
        var value = Descendants<TextBlock>(card).First(t => t.Parent == labelBlock.Parent && Grid.GetRow(t) == row && Grid.GetColumn(t) == 2);
        var dot = Descendants<Ellipse>(card).FirstOrDefault(e => e.Width == 9 && Grid.GetRow(e) == row && e.Parent == labelBlock.Parent);
        return (value, dot);
    }

    private static double NaturalWidth(TextBlock block) => new FormattedText(
        block.Text, CultureInfo.CurrentUICulture, block.FlowDirection,
        new Typeface(block.FontFamily, block.FontStyle, block.FontWeight, block.FontStretch),
        block.FontSize, block.Foreground, VisualTreeHelper.GetDpi(block).PixelsPerDip).WidthIncludingTrailingWhitespace;

    /// <summary>The height <paramref name="block"/>'s whole text needs at the width it was given, wrapping as the block does.</summary>
    private static double HeightNeeded(TextBlock block)
    {
        var text = new FormattedText(
            block.Text, CultureInfo.CurrentUICulture, block.FlowDirection,
            new Typeface(block.FontFamily, block.FontStyle, block.FontWeight, block.FontStretch),
            block.FontSize, block.Foreground, VisualTreeHelper.GetDpi(block).PixelsPerDip);
        if (block.TextWrapping != TextWrapping.NoWrap)
        {
            text.MaxTextWidth = Math.Max(1, block.ActualWidth);
        }

        return text.Height;
    }

    [Fact]
    public void AnOfflineCard_SaysOfflineOnce_AndDrawsNoCrossOverItsRows()
    {
        var found = Shown(Card(isOnline: false, ServerFreshness.Offline), card =>
        {
            var visible = Descendants<TextBlock>(card).Where(t => t.IsVisible).Select(t => t.Text).ToList();
            return (Offline: visible.Count(t => t == "Offline"), Cross: visible.Count(t => t == "✗"));
        });

        Assert.Equal(1, found.Offline);
        Assert.Equal(0, found.Cross);
    }

    [Fact]
    public void AnOfflineCard_ReadsNoBlockingOrDeadlockFigure_AndPaintsThoseDotsGrey()
    {
        var rows = Shown(Card(isOnline: false, ServerFreshness.Offline), card =>
        {
            var blocking = Row(card, "Blocking:");
            var deadlocks = Row(card, "Deadlocks:");
            var cpu = Row(card, "CPU:");
            return (
                Blocking: blocking.Value.Text, BlockingDot: Hex(blocking.Dot?.Fill), BlockingText: Hex(blocking.Value.Foreground),
                Deadlocks: deadlocks.Value.Text, DeadlockDot: Hex(deadlocks.Dot?.Fill), DeadlockText: Hex(deadlocks.Value.Foreground),
                CpuDot: Hex(cpu.Dot?.Fill));
        });

        Assert.Equal("--", rows.Blocking);
        Assert.Equal(UnknownGrey, rows.BlockingDot);
        Assert.Equal(UnknownGrey, rows.BlockingText);
        Assert.Equal("--", rows.Deadlocks);
        Assert.Equal(UnknownGrey, rows.DeadlockDot);
        Assert.Equal(UnknownGrey, rows.DeadlockText);
    }

    [Fact]
    public void AReachableCard_KeepsItsBlockingAndDeadlockCounts_InTheirBandColours()
    {
        var rows = Shown(Card(isOnline: true, ServerFreshness.Fresh), card =>
        {
            var blocking = Row(card, "Blocking:");
            var deadlocks = Row(card, "Deadlocks:");
            return (Blocking: blocking.Value.Text, BlockingDot: Hex(blocking.Dot?.Fill), Deadlocks: deadlocks.Value.Text, DeadlockDot: Hex(deadlocks.Dot?.Fill));
        });

        Assert.Equal("3", rows.Blocking);
        Assert.Equal(Amber, rows.BlockingDot);
        Assert.Equal("2", rows.Deadlocks);
        Assert.Equal(Red, rows.DeadlockDot);
    }

    [Theory]
    [InlineData(ServerFreshness.Offline, "(stopped)")]
    [InlineData(ServerFreshness.Stale, "(stale)")]
    public void TheDatedLastCollectValue_WithItsStateWord_FitsInsideTheCard(ServerFreshness freshness, string stateWord)
    {
        var item = Card(isOnline: false, freshness);
        var (text, clipped, rightEdge, cardWidth, slot, width, height, needed) = Shown(item, card =>
        {
            var border = CardBorder(card);
            var value = Row(card, "Last Collect:").Value;
            var right = value.TransformToAncestor(border).TransformBounds(new Rect(value.RenderSize)).Right;
            /* Layout clips a block whose rendered size is wider than the cell it was given; the person sees the cut, not the card edge. */
            var layoutClip = LayoutInformation.GetLayoutClip(value);
            var slotWidth = layoutClip is null ? value.ActualWidth : layoutClip.Bounds.Width;
            return (value.Text, ClippedTextProbe.Clipped(card, border), right, border.ActualWidth, slotWidth, value.ActualWidth, value.ActualHeight, HeightNeeded(value));
        });

        Assert.EndsWith(stateWord, text, StringComparison.Ordinal);
        Assert.Matches(@"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \(", text);
        Assert.True(clipped.Count == 0, "cut off: " + string.Join("; ", clipped));
        Assert.True(slot + 0.5 >= width, $"\"{text}\" renders {width:F0} wide in a {slot:F0} wide cell, so its end is cut");
        Assert.True(rightEdge <= cardWidth - 2 + 0.5, $"the Last Collect value ends at {rightEdge:F0}, past the {cardWidth:F0} wide card's border");
        Assert.True(height + 0.5 >= needed, $"the Last Collect value is {height:F0} high but its text needs {needed:F0}");
    }
}

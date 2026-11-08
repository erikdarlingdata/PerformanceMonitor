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
using System.Windows.Shapes;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8162: Lite's fleet card takes the Viewer's layout. The card is built as the Overview builds it (its own control, bound to a
/// <see cref="ServerSummaryItem"/>), shown in a window and read back from what it rendered: a 2 px border in the server's health band
/// colour, a severity dot on each metric row that has a band, the CPU detail column, and the name and rows in the shared text styles.
/// </summary>
[Collection("credential-manager")]
public sealed class LiteOverviewCardRenderTests
{
    private static readonly Uri LiteTheme = new("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml");
    private static readonly Uri Shared = new("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml");

    private static string Hex(Brush? brush) =>
        brush is SolidColorBrush { Color: var c } ? $"#{c.R:X2}{c.G:X2}{c.B:X2}" : "(not a solid colour)";

    private static ServerSummaryItem Server(string name, int blocking, int deadlocks) => new()
    {
        DisplayName = name,
        ServerName = name,
        IsOnline = true,
        CpuPercent = 20,
        OtherProcessCpuPercent = 5,
        MemoryMb = 32768,
        BlockingCount = blocking,
        DeadlockCount = deadlocks,
        LastCollectionTime = DateTime.UtcNow,
        CollectionFreshness = ServerFreshness.Fresh,
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
                var type = typeof(ServerSummaryItem).Assembly.GetType("PerformanceMonitorLite.Controls.ServerOverviewCard");
                Assert.True(type is not null, "the Overview card is not its own control yet");
                var card = (FrameworkElement)Activator.CreateInstance(type!)!;
                card.DataContext = item;
                var window = new Window { Content = card, Width = 420, Height = 360, Left = 0, Top = 0, SizeToContent = SizeToContent.Manual };
                window.Show();
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

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var deeper in Descendants<T>(child))
            {
                yield return deeper;
            }
        }
    }

    /// <summary>The fill of the dot on the metric row labelled <paramref name="label"/>, or null when that row has no dot.</summary>
    private static string? DotOf(FrameworkElement card, string label)
    {
        var labelBlock = Descendants<TextBlock>(card).First(t => t.Text == label);
        var row = Grid.GetRow(labelBlock);
        var dot = Descendants<Ellipse>(card).FirstOrDefault(e => e.Width == 9 && Grid.GetRow(e) == row && e.Parent == labelBlock.Parent);
        return dot is null ? null : Hex(dot.Fill);
    }

    [Theory]
    [InlineData(0, 0, "#3A3D45")]   // healthy: the Dark theme's neutral card border (#8221; Light and Cool Breeze have their own, see ThemeLeftoversRenderTests)
    [InlineData(3, 0, "#FFB74D")]   // blocking: amber
    [InlineData(0, 2, "#E57373")]   // a deadlock: red
    public void TheCardBorder_IsTwoPixels_InTheServersBandColour(int blocking, int deadlocks, string expected)
    {
        var (thickness, brush) = Shown(Server("band", blocking, deadlocks), card =>
        {
            var border = Descendants<Border>(card).Concat(card is Border b ? [b] : []).First(x => x.GetType().Name == "OverviewCardBorder");
            return (border.BorderThickness, Hex(border.BorderBrush));
        });

        Assert.Equal(new Thickness(2), thickness);
        Assert.Equal(expected, brush);
    }

    [Theory]
    [InlineData(0, 0, "#81C784", "#81C784")]
    [InlineData(3, 0, "#FFB74D", "#81C784")]
    [InlineData(0, 2, "#81C784", "#E57373")]
    public void EachMetricRowWithABand_HasItsDot_InThatBandsColour(int blocking, int deadlocks, string blockingDot, string deadlockDot)
    {
        var dots = Shown(Server("dots", blocking, deadlocks), card => new
        {
            Cpu = DotOf(card, "CPU:"),
            Blocking = DotOf(card, "Blocking:"),
            Deadlocks = DotOf(card, "Deadlocks:"),
            Memory = DotOf(card, "Memory:"),
        });

        Assert.Equal("#81C784", dots.Cpu);
        Assert.Equal(blockingDot, dots.Blocking);
        Assert.Equal(deadlockDot, dots.Deadlocks);
        // Lite keeps no memory band, so the row has no dot rather than an invented one.
        Assert.Null(dots.Memory);
    }

    [Fact]
    public void TheCpuRow_ShowsItsDetail_InTheColumnAfterTheValue()
    {
        var detail = Shown(Server("detail", 0, 0), card =>
        {
            var value = Descendants<TextBlock>(card).First(t => t.Text.StartsWith("25%", StringComparison.Ordinal));
            return Descendants<TextBlock>(card).FirstOrDefault(t => Grid.GetRow(t) == Grid.GetRow(value) && Grid.GetColumn(t) == Grid.GetColumn(value) + 1)?.Text;
        });

        Assert.Equal("Other: 5%", detail);
    }

    [Fact]
    public void TheCardName_IsAPanelTitle()
    {
        var (size, weight) = Shown(Server("title-check", 0, 0), card =>
        {
            var name = Descendants<TextBlock>(card).First(t => t.Text == "title-check");
            return (name.FontSize, name.FontWeight);
        });

        Assert.Equal(14, size);
        Assert.Equal(FontWeights.SemiBold, weight);
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
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
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared plan viewer and the Viewer's sidebar, as a reader meets them: the plan viewer's empty state names the menu items that
/// exist, its Plan Insights cards stay reachable in a 900 px window (inside the strip or by its scroll bar), and the sidebar dot's
/// tooltip names the server and asks for a gesture every row surface supports.
/// </summary>
public sealed class ViewerLiveScreenTextTests
{
    private static readonly DateTime Now = new(2026, 11, 8, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ThePlanViewerEmptyState_NamesTheMenuItemsThatExist()
    {
        var text = StaTestThread.Run(() => ((TextBlock)new PlanViewerControl().FindName("EmptyStateDetail")).Text);

        Assert.Contains("'View Stored Plan'", text, StringComparison.Ordinal);
        Assert.Contains("'Get Actual Plan (re-run)'", text, StringComparison.Ordinal);
        Assert.DoesNotContain("'View Plan'", text, StringComparison.Ordinal);
    }

    /// <summary>The Server Context card's text is as long as the server's edition string makes it, and the strip's columns size to
    /// their content, so at a 900 px window the cards after it ran past the right edge with nothing to reach them by. Every card is
    /// inside the control's width, or the strip scrolls to it.</summary>
    [Fact]
    public void TheInsightCards_StayReachable_InA900PixelWindow()
    {
        var unreachable = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl
            {
                ServerMetadata = new ServerMetadata
                {
                    ServerName = "SQL01",
                    Edition = "Enterprise Edition: Core-based Licensing " + string.Concat(Enumerable.Repeat("with a very long edition string ", 12)),
                    ProductVersion = "16.0.1000.6",
                    CpuCount = 8,
                    PhysicalMemoryMB = 65536,
                    MaxDop = 4,
                    CostThresholdForParallelism = 50,
                    MaxServerMemoryMB = 49152,
                },
            };
            Invoke(control, "ShowServerContext");
            Invoke(control, "UpdateInsightsHeader");

            var host = new Border { Width = 860, Height = 560, Child = control };
            host.Measure(new Size(860, 560));
            host.Arrange(new Rect(0, 0, 860, 560));
            host.UpdateLayout();

            var strip = control.FindName("InsightsStrip") as ScrollViewer;
            var panel = (Border)control.FindName("InsightsPanel");
            var cards = Descendants(panel).OfType<Grid>().First(g => g.ColumnDefinitions.Count == 5).Children.OfType<Border>().ToList();
            Assert.True(cards.Count >= 4, $"{cards.Count} insight cards found");

            return cards
                .Select(card => card.TransformToAncestor(control).TransformBounds(new Rect(card.RenderSize)))
                .Where(bounds => bounds.Right > control.ActualWidth + 0.5 && (strip is null || strip.ScrollableWidth <= 0))
                .Select(bounds => $"a card ends at {bounds.Right:F0} px in a {control.ActualWidth:F0} px viewer with no way to scroll to it")
                .ToList();
        });

        Assert.True(unreachable.Count == 0, string.Join(Environment.NewLine, unreachable));
    }

    [Fact]
    public void TheRailTooltip_NamesTheServer_AndAsksForAGestureEveryRowSupports()
    {
        var server = new DarlingServer(1, "SQL2022", "Prod", true, 16);
        server.ApplyFreshness(Now, Now);

        var lines = server.DotTooltip.Split('\n');

        Assert.Equal(server.DisplayName, lines[0]);
        Assert.Equal(server.CardStatus.Headline(), lines[1]);
        Assert.Equal("Double-click to open this server's tab", lines[^1]);
        Assert.DoesNotContain(lines, line => line.Contains("the row", StringComparison.Ordinal));
    }

    private static void Invoke(PlanViewerControl control, string method)
    {
        var target = typeof(PlanViewerControl).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.True(target is not null, $"PlanViewerControl.{method} no longer exists under that name");
        target!.Invoke(control, Array.Empty<object>());
    }

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            yield return child;
            foreach (var grandchild in Descendants(child))
                yield return grandchild;
        }
    }
}

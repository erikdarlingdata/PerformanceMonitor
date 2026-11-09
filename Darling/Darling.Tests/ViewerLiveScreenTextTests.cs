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

    /// <summary>The Server Context card's text is as long as the server's edition string makes it. Every card is inside the control's
    /// width or the strip scrolls to it, the Server Context card stops at its 360 px cap, and the Server row's text wraps inside the
    /// card instead of being cut off at its edge.</summary>
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

            var problems = cards
                .Select(card => card.TransformToAncestor(control).TransformBounds(new Rect(card.RenderSize)))
                .Where(bounds => bounds.Right > control.ActualWidth + 0.5 && (strip is null || strip.ScrollableWidth <= 0))
                .Select(bounds => $"a card ends at {bounds.Right:F0} px in a {control.ActualWidth:F0} px viewer with no way to scroll to it")
                .ToList();

            /* The Server row's text is as long as the edition makes it. Inside the card it is shown whole: on one line when the line
               fits the card, else wrapped onto more lines, never cut off where the card ends. A line longer than the card that stays
               one line tall is drawn past the card's edge and clipped there. */
            var serverRow = Descendants(panel).OfType<TextBlock>().Single(t => t.Text.StartsWith("SQL01", StringComparison.Ordinal));
            var oneLine = new TextBlock { Text = serverRow.Text, FontFamily = serverRow.FontFamily, FontSize = serverRow.FontSize, FontWeight = serverRow.FontWeight, FontStyle = serverRow.FontStyle };
            oneLine.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
            var cardWidth = cards[0].ActualWidth;
            var staysOneLine = serverRow.ActualHeight < oneLine.DesiredSize.Height * 1.5;
            if (cardWidth > 360.5)
                problems.Add($"the Server Context card is {cardWidth:F0} px wide; a card stops at 360 px so that its text wraps inside it instead of widening the strip");
            if (oneLine.DesiredSize.Width > cardWidth + 0.5 && staysOneLine)
                problems.Add($"the Server row needs {oneLine.DesiredSize.Width:F0} px on one line in a {cardWidth:F0} px card and does not wrap (text {serverRow.ActualWidth:F0}x{serverRow.ActualHeight:F0} px): its tail is cut off");

            return problems;
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

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
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
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The plan viewer's Plan Insights strip in a wide window. The strip scrolls sideways, so it measures its grid with no width limit,
/// and the Wait Stats card sits in the grid's star column. This fixture's wait rows are wider than any width that column is given,
/// and the four cards in front of it sit at their 360 px caps, so the grid is bounded: as wide as the strip, or as wide as the four
/// content-sized columns plus the Wait Stats column's minimum when the strip is narrower than that, never as wide as the longest
/// wait row. Where the strip holds those minimums it has nothing to scroll and the Wait Stats card ends at the strip's right edge;
/// everywhere, each wait row is trimmed inside its card. A narrow window still scrolls to every card
/// (<see cref="ViewerLiveScreenTextTests.TheInsightCards_StayReachable_InA900PixelWindow"/>).
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class PlanViewerInsightsStripWidthTests
{
    /// <summary>The Wait Stats column's minimum width, which the grid keeps when the strip is narrower than the cards' minimums.</summary>
    private const double WaitStatsMinimum = 280;

    private const double CardCap = 360;

    /// <summary>Tall enough for the insights panel to start expanded (it starts collapsed under 640 px).</summary>
    private const double HostHeight = 720;

    [Theory]
    [InlineData(1280)]
    [InlineData(1600)]
    [InlineData(1920)]
    public void TheWaitStatsCard_FillsTheStrip_InAWideWindow(double width)
    {
        var problems = StaTestThread.Run(() => WithTheme("Dark", () =>
        {
            var control = new PlanViewerControl { ServerMetadata = LongEditionMetadata() };
            try
            {
                RenderInsights(control, LongStatement(), LongWaits());

                /* The CI screen is 1024x768, so the width comes from an explicitly sized host, not from a Window. The host is tall
                   enough (640 px or more) for the insights panel to start expanded; a collapsed panel leaves the layout. */
                var host = new Border { Width = width, Height = HostHeight, Child = control };
                LayOut(host);

                var strip = (ScrollViewer)control.FindName("InsightsStrip");
                var panel = (Border)control.FindName("InsightsPanel");
                var grid = Descendants(panel).OfType<Grid>().First(g => g.ColumnDefinitions.Count == 5);
                var cards = grid.Children.OfType<Border>().ToList();
                var waitCard = cards.Single(card => Grid.GetColumn(card) == 4);
                var found = new List<string>();
                if (!((Expander)control.FindName("InsightsExpander")).IsExpanded)
                    found.Add($"Plan Insights is collapsed in a {HostHeight} px tall viewer; the cards are measured open");

                /* The grid's width is the strip's, or the four content-sized columns plus the Wait Stats column's minimum when the
                   strip is narrower than that; never the Wait Stats card's longest row. */
                var contentColumns = grid.ColumnDefinitions.Take(4).Sum(column => column.ActualWidth);
                var stripWidth = strip.ViewportWidth;
                var bound = Math.Max(stripWidth, contentColumns + WaitStatsMinimum);
                if (Math.Abs(grid.ActualWidth - bound) > 0.5)
                    found.Add($"the insights grid is {grid.ActualWidth:F0} px wide (extent {strip.ExtentWidth:F0} px) in a {stripWidth:F0} px strip whose four content-sized cards take {contentColumns:F0} px: it is bounded at {bound:F0} px (the strip, or those cards plus the Wait Stats column's {WaitStatsMinimum} px minimum), so the Wait Stats card fills the remaining width instead of taking its longest row ({waitCard.ActualWidth:F0} px)");

                /* Where the strip holds its columns' minimums it has nothing to scroll, and the Wait Stats card ends at the strip's own
                   right edge. */
                var columnsFit = contentColumns + WaitStatsMinimum <= stripWidth + 0.5;
                var waitCardRight = waitCard.TransformToAncestor(strip).TransformBounds(new Rect(waitCard.RenderSize)).Right;
                if (columnsFit && (strip.ScrollableWidth > 0 || strip.ComputedHorizontalScrollBarVisibility == Visibility.Visible))
                    found.Add($"the strip scrolls by {strip.ScrollableWidth:F2} px (extent {strip.ExtentWidth:F2} px, scroll bar {strip.ComputedHorizontalScrollBarVisibility}) in a {stripWidth:F2} px strip that holds its columns' minimums ({contentColumns:F0} + {WaitStatsMinimum} px)");
                if (columnsFit && Math.Abs(waitCardRight - stripWidth) > 0.5)
                    found.Add($"the Wait Stats card ends at {waitCardRight:F0} px in a strip {stripWidth:F0} px wide: it does not fill the remaining width");
                if (!columnsFit && Math.Abs(strip.ScrollableWidth - (bound - stripWidth)) > 0.5)
                    found.Add($"the strip scrolls by {strip.ScrollableWidth:F0} px in a {stripWidth:F0} px strip whose columns' minimums need {bound:F0} px: it scrolls by the {bound - stripWidth:F0} px it must, not to the Wait Stats card's longest row");

                /* The wait rows are longer than the Wait Stats column at every width here. Each one is trimmed to the card: its text ends
                   inside the card's right edge, and the duration (the row's flexible text) is drawn narrower than its one-line width. */
                var waitRows = Descendants((Panel)control.FindName("WaitStatsContent")).OfType<TextBlock>().ToList();
                if (waitRows.Count == 0)
                    found.Add("no wait rows were rendered");
                var cardRight = waitCard.ActualWidth;
                foreach (var row in waitRows)
                {
                    var rowBounds = row.TransformToAncestor(waitCard).TransformBounds(new Rect(row.RenderSize));
                    if (rowBounds.Right > cardRight + 0.5)
                        found.Add($"the wait row \"{row.Text}\" ends at {rowBounds.Right:F0} px in a {cardRight:F0} px Wait Stats card");
                }
                var durations = waitRows.Where(row => row.Text.EndsWith(" waits)", StringComparison.Ordinal)).ToList();
                if (durations.Count == 0)
                    found.Add("no wait duration text was rendered");
                foreach (var duration in durations)
                {
                    var oneLine = new TextBlock { Text = duration.Text, FontFamily = duration.FontFamily, FontSize = duration.FontSize, FontWeight = duration.FontWeight };
                    oneLine.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
                    if (duration.ActualWidth > oneLine.DesiredSize.Width - 0.5)
                        found.Add($"the wait duration \"{duration.Text}\" is drawn at its full {duration.ActualWidth:F0} px in a {waitCard.ActualWidth:F0} px Wait Stats card: it does not ellipsize");
                }

                return found;
            }
            finally
            {
                control.Cleanup();
            }
        }));

        Assert.True(problems.Count == 0, $"At {width} px:" + Environment.NewLine + string.Join(Environment.NewLine, problems));
    }

    /// <summary>Runs <paramref name="body"/> with the Viewer's theme applied, since the cards' coloured rows look up theme brushes,
    /// and puts the previous theme back.</summary>
    private static T WithTheme<T>(string theme, Func<T> body)
    {
        _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var previous = ThemeManager.CurrentTheme;
        ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");
        try
        {
            ThemeManager.Apply(theme);
            return body();
        }
        finally
        {
            ThemeManager.Apply(previous);
            ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
        }
    }

    /// <summary>A strip whose width moves by a fraction of a pixel, as it does at a fractional display scale, takes the grid with it:
    /// the grid is exactly as wide as the strip, so a sub-pixel difference never leaves a scroll bar showing.</summary>
    [Fact]
    public void TheInsightsGrid_FollowsAFractionalResize_Exactly()
    {
        var problems = StaTestThread.Run(() => WithTheme("Dark", () =>
        {
            var control = new PlanViewerControl { ServerMetadata = LongEditionMetadata() };
            try
            {
                RenderInsights(control, LongStatement(), LongWaits());

                var host = new Border { Width = 1920.4, Height = HostHeight, Child = control };
                LayOut(host);
                host.Width = 1920;
                LayOut(host);

                var strip = (ScrollViewer)control.FindName("InsightsStrip");
                var panel = (Border)control.FindName("InsightsPanel");
                var grid = Descendants(panel).OfType<Grid>().First(g => g.ColumnDefinitions.Count == 5);
                var found = new List<string>();
                if (!((Expander)control.FindName("InsightsExpander")).IsExpanded)
                    found.Add($"Plan Insights is collapsed in a {HostHeight} px tall viewer; the cards are measured open");
                if (strip.ViewportWidth != 1920)
                    found.Add($"the strip's viewport is {strip.ViewportWidth:F2} px after the host narrowed to 1920 px");
                if (grid.ActualWidth != strip.ViewportWidth)
                    found.Add($"the insights grid is {grid.ActualWidth:F2} px wide in a strip whose viewport is {strip.ViewportWidth:F2} px after the strip narrowed from 1920.4 px to 1920 px: the grid follows the strip exactly");
                if (strip.ScrollableWidth > 0 || strip.ComputedHorizontalScrollBarVisibility == Visibility.Visible)
                    found.Add($"the strip scrolls by {strip.ScrollableWidth:F2} px (extent {strip.ExtentWidth:F2} px, scroll bar {strip.ComputedHorizontalScrollBarVisibility}) after it narrowed from 1920.4 px to 1920 px");
                return found;
            }
            finally
            {
                control.Cleanup();
            }
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>Lays the host out at its own size and lets the layout settle: a width the plan viewer sets from a layout-updated
    /// handler lands on the next pass, and the strip's scroll data refreshes on its own deferred handler, so the layout runs,
    /// the dispatcher's pending work is pumped, and the layout runs again before anything is read.</summary>
    private static void LayOut(Border host)
    {
        host.Measure(new Size(host.Width, host.Height));
        host.Arrange(new Rect(0, 0, host.Width, host.Height));
        host.UpdateLayout();
        PumpDispatcher();
        host.Measure(new Size(host.Width, host.Height));
        host.Arrange(new Rect(0, 0, host.Width, host.Height));
        host.UpdateLayout();
    }

    private static void PumpDispatcher()
    {
        var frame = new System.Windows.Threading.DispatcherFrame();
        System.Windows.Threading.Dispatcher.CurrentDispatcher.BeginInvoke(System.Windows.Threading.DispatcherPriority.ContextIdle, new Action(() => frame.Continue = false));
        System.Windows.Threading.Dispatcher.PushFrame(frame);
    }

    /// <summary>Renders the five insight cards for a statement as the plan viewer does when a statement is drawn, with the wait
    /// rows replaced by <paramref name="waits"/>, and shows the panel.</summary>
    private static void RenderInsights(PlanViewerControl control, PlanStatement statement, List<WaitStatInfo> waits)
    {
        Invoke(control, "ShowMissingIndexes", statement.MissingIndexes);
        Invoke(control, "ShowParameters", statement);
        Invoke(control, "ShowWaitStats", waits, statement.PlanWarnings, true);
        Invoke(control, "ShowRuntimeSummary", statement);
        Invoke(control, "ShowServerContext");
        Invoke(control, "UpdateInsightsHeader");
    }

    /// <summary>The fixture plan's first statement, with a parameter whose value is longer than a card, so the Parameters card sits at
    /// its cap like the Server Context and Missing Index cards.</summary>
    private static PlanStatement LongStatement()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", "memory_grant_wait_plan.sqlplan");
        Assert.True(File.Exists(path), $"Test plan not found: {path}");
        var plan = PlanAnalysisPipeline.Run(ShowPlanParser.Parse(File.ReadAllText(path)));
        Assert.True(plan.ParseError is null, plan.ParseError);
        var statement = PlanStatements.EnumerateAll(plan).First(s => s.RootNode is not null);
        Assert.True(statement.MissingIndexes.Count > 0, "the fixture plan carries a missing index suggestion");
        statement.Parameters.Add(new PlanParameter
        {
            Name = "@SearchText",
            DataType = "nvarchar(4000)",
            CompiledValue = "N'" + string.Concat(Enumerable.Repeat("a compiled value longer than the card ", 8)) + "'",
            RuntimeValue = "N'" + string.Concat(Enumerable.Repeat("a runtime value longer than the card ", 8)) + "'",
        });
        return statement;
    }

    /// <summary>Wait rows whose duration text is as long as a 64-bit count makes it, so the longest row is wider than any width the
    /// Wait Stats column is given.</summary>
    private static List<WaitStatInfo> LongWaits() => new()
    {
        new WaitStatInfo { WaitType = "RESERVED_MEMORY_ALLOCATION_EXT", WaitTimeMs = long.MaxValue / 2, WaitCount = long.MaxValue / 2 },
        new WaitStatInfo { WaitType = "CXCONSUMER", WaitTimeMs = long.MaxValue / 4, WaitCount = long.MaxValue / 3 },
        new WaitStatInfo { WaitType = "PAGEIOLATCH_SH", WaitTimeMs = long.MaxValue / 8, WaitCount = long.MaxValue / 5 },
    };

    private static ServerMetadata LongEditionMetadata() => new()
    {
        ServerName = "SQL01",
        Edition = "Enterprise Edition: Core-based Licensing " + string.Concat(Enumerable.Repeat("with a very long edition string ", 12)),
        ProductVersion = "16.0.1000.6",
        CpuCount = 8,
        PhysicalMemoryMB = 65536,
        MaxDop = 4,
        CostThresholdForParallelism = 50,
        MaxServerMemoryMB = 49152,
    };

    private static void Invoke(PlanViewerControl control, string method, params object[] args)
    {
        var target = typeof(PlanViewerControl).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.True(target is not null, $"PlanViewerControl.{method} no longer exists under that name");
        target!.Invoke(control, args);
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

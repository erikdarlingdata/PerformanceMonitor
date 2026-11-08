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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Darling.Tests;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;
using static PerformanceMonitorLite.Tests.PageLayoutProbe;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8307, the smaller things a walk of Lite at 900x600, at a maximized window and in Light found: a Refresh button styled unlike its
/// siblings, a card subtitle cut short, a footer label parted from its value, an empty-grid time on a clock the tab does not use, grid
/// columns that leave the free width empty, a headline that runs off the page, and a search box with no placeholder. Each fact reads the
/// realized result of the real control.
/// </summary>
[Collection("credential-manager")]
public sealed class PageLayoutNitsTests
{
    private static CredentialResolver Credentials() => new(new CredentialService(), null);

    [Fact]
    public void TheMemoryBrokerRefreshButton_IsStyledLikeTheOtherSystemEventsRefreshButtons()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(1280), 800, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var siblingTab = ChartLayoutProbe.Logical<TabItem>(tab).First(t => t.Header as string == "Scheduler Issues");
            var sibling = ChartLayoutProbe.Logical<Button>(siblingTab).First(b => Equals(b.Content, "Refresh"));
            Reveal(sibling);
            Settle(window);
            var siblingStyle = sibling.Style;

            var brokerTab = ChartLayoutProbe.Logical<TabItem>(tab).First(t => t.Header as string == "Memory Broker");
            Reveal(Find<FrameworkElement>(tab, "MemoryBrokerGrid"));
            Settle(window);
            var refresh = ChartLayoutProbe.Logical<Button>(brokerTab).First(b => Equals(b.Content, "Refresh"));
            if (!ReferenceEquals(refresh.Style, siblingStyle))
            {
                found.Add($"Memory Broker's Refresh has its own style ({refresh.Style?.ToString() ?? "none"}); Scheduler Issues' has {siblingStyle?.ToString() ?? "the theme's"}");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void ThePerfmonPickersSubtitle_FitsItsCard()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            Reveal(Find<FrameworkElement>(tab, "PerfmonCountersList"));
            Settle(window);
            var card = Find<ChartPanel>(tab, "PerfmonPickerCard");
            if (card.Template.FindName("PART_Subtitle", card) is not TextBlock subtitle)
            {
                found.Add("the picker card has no subtitle part");
            }
            else if (ClippedTextProbe.NaturalWidth(subtitle) > subtitle.ActualWidth + 0.5)
            {
                found.Add($"the subtitle '{subtitle.Text}' needs {ClippedTextProbe.NaturalWidth(subtitle):F0} px and has {subtitle.ActualWidth:F0}");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void ThePlanCacheFooter_KeepsCollectedAndItsTimeOnOneLine_WhereverTheFooterWraps()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(1280), 800, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var value = Find<TextBlock>(tab, "PlanCacheCollectedText");
            Reveal(value);
            Find<TextBlock>(tab, "PlanCacheTotalPlansText").Text = "12,345";
            Find<TextBlock>(tab, "PlanCacheOldestPlanText").Text = "3d 4h";
            Find<TextBlock>(tab, "PlanCacheCompiledSingleUseText").Text = "9,876 (80%)";
            Find<TextBlock>(tab, "PlanCacheBloatLevelText").Text = "HIGH";
            Find<TextBlock>(tab, "PlanCacheRecommendationText").Text = "Consider optimize for ad hoc workloads";
            value.Text = "2026-11-07 14:44:10";
            Settle(window);

            var label = Visual<TextBlock>(tab).First(t => t.Text == "Collected:" && t.IsVisible);
            Border? footer = null;
            for (var up = VisualTreeHelper.GetParent(label); up is not null; up = VisualTreeHelper.GetParent(up))
            {
                if (up is Border border)
                {
                    footer = border;
                    break;
                }
            }

            Assert.NotNull(footer);
            /* The footer just wide enough for the label and not for its value: a break must fall somewhere, and the pair must not be parted by it. */
            var chrome = window.Width - footer.ActualWidth;
            Resize(window, (int)Math.Ceiling(Bounds(label, footer).Right + 2 + chrome), 800);
            var labelTop = Bounds(label, tab).Top;
            var valueTop = Bounds(value, tab).Top;
            if (Math.Abs(labelTop - valueTop) > 0.5)
            {
                found.Add($"'Collected:' sits at y={labelTop:F0} and its time at y={valueTop:F0} in a {footer.ActualWidth:F0} px footer");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The Running Jobs empty text names the last good collection the way the tab words its other times. The note comes from
    /// <see cref="RunningJobsCurrency"/> with the tab's own formatter (the Collection Health one, on the tab's server clock), so the
    /// overload that takes a formatter is looked up by its signature, which lets this build where the note still has only its UTC form.
    /// </summary>
    [Fact]
    public void TheRunningJobsEmptyText_WordsTheLastGoodCollectionInTheTabsTimeMode()
    {
        var at = new DateTime(2026, 11, 4, 14, 0, 0, DateTimeKind.Utc);
        var withFormatter = typeof(RunningJobsCurrency).GetMethod("NotCurrentNote", new[] { typeof(DateTime), typeof(Func<DateTime, string>) });
        Assert.True(withFormatter is not null, "the not-current note takes no time formatter, so it can only name the UTC instant");

        var problems = WithServerTab("DarkTheme", PaneWidth(1280), 800, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var clock = typeof(ServerTab).GetField("_serverClock", Instance)!.GetValue(tab);
            var format = typeof(CollectionHealthTime).GetMethod("Format", Instance | System.Reflection.BindingFlags.Static)!;
            Func<DateTime, string> tabsWords = utc => (string)format.Invoke(null, new[] { utc, clock })!;
            var expected = tabsWords(at);
            var note = (string)withFormatter.Invoke(null, new object[] { at, tabsWords })!;
            if (!note.Contains(expected, StringComparison.Ordinal))
            {
                found.Add($"the note reads '{note}'; the tab words that instant as '{expected}'");
            }

            if (note.Contains(" UTC", StringComparison.Ordinal) && !expected.Contains("UTC", StringComparison.Ordinal))
            {
                found.Add($"the note still names the UTC clock: '{note}'");
            }

            return found;
        }, utcOffsetMinutes: -480);

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheConfigurationGrids_ShareTheFreeWidth_AndShowWholeNames_AtAMaximizedWindow()
    {
        const string Database = "CardinalityExtrapolationDemoDatabase";
        const string Setting = "ASYNC_STATS_UPDATE_WAIT_AT_LOW_PRIORITY";
        var problems = WithServerTab("DarkTheme", PaneWidth(1550), 925, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var scoped = Find<DataGrid>(tab, "DatabaseScopedConfigGrid");
            Reveal(scoped);
            scoped.ItemsSource = Enumerable.Range(1, 3).Select(i => new { DatabaseName = Database, ConfigurationName = Setting, Value = $"{i}", ValueForSecondary = "PRIMARY", CaptureTimeLocal = "2026-11-07 14:44:10" }).ToList();
            Settle(window);
            found.AddRange(Unfilled(scoped, "Scoped Configuration"));
            foreach (var cell in Visual<TextBlock>(scoped).Where(t => t.Text == Database || t.Text == Setting))
            {
                if (ClippedTextProbe.NaturalWidth(cell) > cell.ActualWidth + 0.5)
                {
                    found.Add($"Scoped Configuration cuts '{cell.Text}' to {cell.ActualWidth:F0} px");
                }
            }

            var server = Find<DataGrid>(tab, "ServerConfigGrid");
            Reveal(server);
            server.ItemsSource = Enumerable.Range(1, 3).Select(i => new { ConfigurationName = "max degree of parallelism", ValueConfigured = (long)i, ValueInUse = (long)i, DynamicDisplay = "Yes", AdvancedDisplay = "Yes", CaptureTimeLocal = "2026-11-07 14:44:10" }).ToList();
            Settle(window);
            found.AddRange(Unfilled(server, "Server Configuration"));

            var tuning = Find<DataGrid>(tab, "AutomaticTuningGrid");
            Reveal(tuning);
            tuning.ItemsSource = Enumerable.Range(1, 3).Select(i => new { DatabaseName = Database, DesiredState = "ON", ActualState = "ON", Reason = "", CreateIndexState = "OFF", DropIndexState = "OFF", CollectionTimeLocal = "2026-11-07 14:44:10" }).ToList();
            Settle(window);
            found.AddRange(Unfilled(tuning, "Automatic Tuning"));
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>The columns of <paramref name="grid"/> leave none of its width empty.</summary>
    private static IEnumerable<string> Unfilled(DataGrid grid, string name)
    {
        var viewport = InnerScroll(grid).ViewportWidth;
        var columns = grid.Columns.Sum(c => c.ActualWidth);
        if (columns + 1 < viewport)
        {
            yield return $"{name}: the columns take {columns:F0} of {viewport:F0} px and leave the rest empty";
        }
    }

    [Fact]
    public void ARecommendationHeadline_WrapsInsideThePage_InLight_At900x600()
    {
        const string Headline = "SQL Server signalled physical-memory pressure on this host, with the working set trimmed twice in the window and the buffer pool shrunk by a third · WARNING";
        var problems = StaTestThread.Run(() => WithTheme("LightTheme", () =>
        {
            var found = new List<string>();
            var tab = new RecommendationsTab();
            var window = Open(tab, PaneWidth(900), 600);
            try
            {
                Find<ScrollViewer>(tab, "SectionsScroll").Visibility = Visibility.Visible;
                Find<ItemsControl>(tab, "SectionsList").ItemsSource = new[] { new { Header = Headline, IsExpanded = true, Cards = Array.Empty<object>() } };
                Settle(window);
                var block = Visual<TextBlock>(tab).FirstOrDefault(t => t.Text == Headline);
                if (block is null)
                {
                    found.Add("the headline is not rendered");
                    return found;
                }

                var bounds = Bounds(block, tab);
                if (bounds.Right > tab.ActualWidth + 0.5)
                {
                    found.Add($"the headline runs to x={bounds.Right:F0} of a {tab.ActualWidth:F0} px page");
                }

                if (block.ActualHeight < block.FontSize * 2)
                {
                    found.Add($"the headline is one {block.ActualHeight:F0} px line in {block.ActualWidth:F0} px, so its end is out of sight");
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheWaitStatsSearchBox_ShowsAPlaceholderWhileEmpty()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var box = Find<TextBox>(tab, "WaitTypeSearchBox");
            Reveal(box);
            Settle(window);
            if (tab.FindName("WaitTypeSearchPlaceholder") is not TextBlock placeholder)
            {
                found.Add("the wait type search box has no placeholder");
                return found;
            }

            if (placeholder.Visibility != Visibility.Visible || string.IsNullOrWhiteSpace(placeholder.Text))
            {
                found.Add($"the placeholder is {placeholder.Visibility} with '{placeholder.Text}' while the box is empty");
            }

            box.Text = "page";
            Settle(window);
            if (placeholder.Visibility == Visibility.Visible)
            {
                found.Add("the placeholder stays over the text a reader typed");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheTraceFlagsGrid_StartsAtTheLeftEdge_AndFillsItsWidth_WhenEmpty()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var grid = Find<DataGrid>(tab, "TraceFlagsGrid");
            Reveal(grid);
            grid.ItemsSource = Array.Empty<object>();
            Settle(window);
            var header = Visual<System.Windows.Controls.Primitives.DataGridColumnHeader>(grid).Where(h => h.Column is not null).OrderBy(h => Bounds(h, grid).Left).FirstOrDefault();
            if (header is null)
            {
                found.Add("the grid shows no column headers");
            }
            else
            {
                var left = Bounds(header, grid).Left;
                var content = Visual<StackPanel>(header).FirstOrDefault();
                var contentLeft = content is null ? left : Bounds(content, grid).Left;
                if (left > 2 || contentLeft > 12)
                {
                    found.Add($"the first header starts at x={left:F0} and its text at x={contentLeft:F0} of the grid");
                }
            }

            found.AddRange(Unfilled(grid, "Trace Flags"));
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}

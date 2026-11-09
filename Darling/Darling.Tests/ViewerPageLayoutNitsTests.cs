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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.ViewerPageLayoutProbe;

namespace Darling.Tests;

/// <summary>
/// #8307, the Viewer's twins of the smaller things a walk of Lite found: a Refresh button styled unlike its siblings, a card subtitle cut
/// short, a footer label parted from its value, grid columns that leave the free width empty, and a search box with no placeholder. Each
/// fact reads the realized result of the real control.
/// </summary>
public sealed class ViewerPageLayoutNitsTests
{
    private readonly ITestOutputHelper _output;

    public ViewerPageLayoutNitsTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public void TheMemoryBrokerRefreshButton_IsStyledLikeTheOtherSystemEventsRefreshButtons()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(1280), 800, (tab, window) =>
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
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
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
        var problems = WithServerTab("DarkTheme", PaneWidth(1280), 800, (tab, window) =>
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

            var label = Visual<TextBlock>(tab).First(t => t.Text == "Collected:" && t.IsVisible && Bounds(t, tab).Top > Bounds(value, tab).Top - 60);
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
            var chrome = HostWidth(window) - footer.ActualWidth;
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

    [Fact]
    public void TheConfigurationGrids_ShareTheFreeWidth_AndShowWholeNames_AtAMaximizedWindow()
    {
        const string Database = "CardinalityExtrapolationDemoDatabase";
        const string Setting = "ASYNC_STATS_UPDATE_WAIT_AT_LOW_PRIORITY";
        var problems = WithServerTab("DarkTheme", PaneWidth(1550), 925, (tab, window) =>
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
    public void TheWaitStatsSearchBox_ShowsAPlaceholderWhileEmpty()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
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
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
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

    [Fact]
    public void TheSickSpinlocksChart_WithOnlyZeroBackoffs_ShowsTheEmptyState_NotAZeroSeries()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
        {
            var found = new List<string>();
            var chart = Find<ScottPlot.WPF.WpfPlot>(tab, "SickSpinlocksChart");
            Reveal(chart);
            Settle(window);
            var at = new DateTime(2026, 11, 7, 0, 0, 0, DateTimeKind.Utc);
            var data = Enumerable.Range(0, 12).Select(i => new SystemHealthRecord { EventTime = at.AddHours(i), SickSpinlockType = "none", SpinlockBackoffs = 0 }).ToList();
            var renderer = tab.GetType().GetProperty("SysHealthRenderer", Instance)!.GetValue(tab)!;
            Invoke(renderer, "RenderSickSpinlocksChart", chart, null, data, at.ToOADate(), at.AddHours(12).ToOADate());
            Settle(window);
            var legend = chart.Plot.Legend.GetItems().Select(i => i.LabelText).ToList();
            if (legend.Count > 0)
            {
                found.Add($"the all-zero chart has a legend entry: {string.Join(", ", legend)}");
            }

            var limits = chart.Plot.Axes.GetLimits();
            if (limits.Bottom < 0)
            {
                found.Add($"the all-zero chart runs from {limits.Bottom:0.##} to {limits.Top:0.##}");
            }

            if (!chart.Plot.GetPlottables<ScottPlot.Plottables.Text>().Any(t => t.LabelText.StartsWith("No ", StringComparison.Ordinal)))
            {
                found.Add("the all-zero chart does not say it has no data");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    public static TheoryData<string, int, int> ThemesAndWindows => new()
    {
        { "DarkTheme", 900, 600 }, { "LightTheme", 900, 600 }, { "CoolBreezeTheme", 900, 600 },
        { "DarkTheme", 1280, 800 }, { "LightTheme", 1280, 800 }, { "CoolBreezeTheme", 1280, 800 },
    };

    /// <summary>
    /// Item 16: every Overview lane's highest Y tick label is whole, with the plot still flush with the lane's top edge. A left-axis
    /// label is centered on its tick, so a tick on the plot's top edge loses the label's upper half to the lane's edge. The empty
    /// lane is the sharpest case (its 0 to 1 axis puts "1" on the edge), and the CPU lane's "100" sits close to it.
    /// </summary>
    [Theory]
    [MemberData(nameof(ThemesAndWindows))]
    public void TheOverviewLanes_KeepTheirTopTickLabelWhole_WithThePlotFlushWithTheLanesTop(string theme, int width, int height)
    {
        var (measured, problems) = WithServerTab(theme, PaneWidth(width), height, (tab, window) =>
        {
            var lines = new List<string>();
            var found = new List<string>();
            var lanes = Find<CorrelatedTimelineLanesControl>(tab, "OverviewLanes");
            Reveal(lanes);
            Settle(window);
            var t0 = new DateTime(2026, 11, 7, 0, 0, 0, DateTimeKind.Utc).ToOADate();
            List<(double, double)> Series(double lo, double hi) => Enumerable.Range(0, 48).Select(i => (t0 + i / 48.0, lo + (hi - lo) * (i % 12) / 11.0)).ToList();
            lanes.UpdateCpuLane(Series(5, 90), Series(8, 95));
            lanes.UpdateLane(lanes.WaitStatsChart, "Wait ms/sec", [], "#FFB74D");
            lanes.UpdateBlockingLane(Series(1, 6), Series(0, 2).Take(5).ToList());
            lanes.UpdateLane(lanes.MemoryChart, "Buffer Pool MB", Series(2000, 9000), "#4FC3F7");
            lanes.UpdateLane(lanes.FileIoChart, "I/O Latency ms", Series(1, 40), "#81C784");
            lanes.SyncXAxes(24, null, null);
            Settle(window);
            foreach (var (chart, name) in new[] { (lanes.CpuChart, "CPU"), (lanes.WaitStatsChart, "Wait"), (lanes.BlockingChart, "Blocking"), (lanes.MemoryChart, "Memory"), (lanes.FileIoChart, "I/O") })
            {
                var last = chart.Plot.RenderManager.LastRender;
                if (last.DataRect.Top > 2)
                {
                    found.Add($"the {name} lane holds {last.DataRect.Top:0.#} px above its plot");
                }

                var axis = chart.Plot.Axes.Left;
                var drawn = axis.TickGenerator.Ticks
                    .Where(t => t.IsMajor && !string.IsNullOrEmpty(t.Label))
                    .Select(t => (t.Label, Y: axis.GetPixel(t.Position, last.DataRect), Height: TickLabelHeight(axis, t.Label)))
                    .Where(t => t.Y >= last.DataRect.Top - 0.5 && t.Y <= last.DataRect.Bottom + 0.5)
                    .OrderBy(t => t.Y)
                    .ToList();
                if (drawn.Count > 0)
                {
                    var top = drawn[0];
                    lines.Add($"{name}: plot {last.DataRect.Height:0.#} px tall from y={last.DataRect.Top:0.#}; top label \"{top.Label}\" is centered at y={top.Y:0.#} and {top.Height:0.#} px tall, so it spans y={top.Y - top.Height / 2:0.#} to {top.Y + top.Height / 2:0.#}");
                }

                foreach (var (label, y, labelHeight) in drawn)
                {
                    if (y - labelHeight / 2 < -0.5)
                    {
                        found.Add($"the {name} lane's \"{label}\" label is centered {y:0.#} px below the lane's top edge and is {labelHeight:0.#} px tall, so its top is cut");
                    }
                }
            }

            return (lines, found);
        });

        foreach (var line in measured)
        {
            _output.WriteLine($"{theme} {width}x{height} {line}");
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>The height ScottPlot lays the tick label out at, from the axis's own label style.</summary>
    private static float TickLabelHeight(ScottPlot.IAxis axis, string label)
    {
        using var paint = ScottPlot.Paint.NewDisposablePaint();
        return axis.TickLabelStyle.Measure(label, paint).Height;
    }

    /// <summary>
    /// The Running Jobs empty text names the last good collection in the Viewer's selected time mode, like the page's other times: the
    /// page's own text, after the tab shows its not-current note for an instant, carries that wording to the second and not the UTC
    /// form the MCP tools use.
    /// </summary>
    [Fact]
    public void TheRunningJobsEmptyText_WordsTheLastGoodCollectionInTheViewersTimeMode()
    {
        var at = new DateTime(2026, 11, 4, 14, 0, 0);
        var problems = WithServerTab("DarkTheme", PaneWidth(1280), 800, (tab, window) =>
        {
            var found = new List<string>();
            var expected = ViewerTimeHelper.FormatForDisplay(at, "yyyy-MM-dd HH:mm:ss");
            tab.ShowRunningJobsNotCurrent(at);
            var message = Find<TextBlock>(tab, "RunningJobsNoDataMessage");
            if (!message.Text.Contains(expected, StringComparison.Ordinal))
            {
                found.Add($"the page reads '{message.Text}'; the Viewer words that instant as '{expected}'");
            }

            if (message.Visibility != Visibility.Visible)
            {
                found.Add("the note is not shown");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}

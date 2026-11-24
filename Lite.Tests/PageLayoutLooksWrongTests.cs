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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;
using static PerformanceMonitorLite.Tests.PageLayoutProbe;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8307, the pages that hid or crowded data at Lite's 900x600 minimum and at a maximized window. Each fact builds the real server tab on
/// the shared STA thread with the theme loaded, shows it at the width a tab has beside the sidebar, fills the page the way its load does,
/// and reads the realized layout: which scroll viewer has an extent, what a card's share of a page is, how many times a message is said,
/// and how many rows a legend takes.
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
public sealed class PageLayoutLooksWrongTests
{
    private static CredentialResolver Credentials() => new(new CredentialService(), null);

    [Fact]
    public void TheCpuSchedulerGrid_ShowsAllItsRowsWithNoScrollBarOfItsOwn_AndTheChartKeepsItsFloor_At900x600()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var grid = Find<DataGrid>(tab, "CpuSchedulerGrid");
            Reveal(grid);
            grid.ItemsSource = Enumerable.Range(1, 8)
                .Select(i => new { Metric = $"Metric {i}", Value = $"{i * 10}", CollectionTimeLocal = "2026-11-07 10:00:00", IsWarning = i == 3, Hint = $"hint {i}" })
                .ToList();
            Settle(window);

            var rows = InnerScroll(grid);
            if (rows.ScrollableHeight > 0.5)
            {
                found.Add($"the grid scrolls {rows.ScrollableHeight:F0} px of its own rows inside a {grid.ActualHeight:F0} px grid, beside the page's scroll bar");
            }

            var chart = Find<FrameworkElement>(tab, "CpuSchedulerChart");
            if (chart.ActualHeight + 0.5 < ChartLayout.MinPlotHeight - 60)
            {
                found.Add($"the chart is {chart.ActualHeight:F0} px tall");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheCollectionCyclesCard_SitsOnHealthSummaryOnly_AndDurationTrends_HasNoScrollOfItsOwn_At900x600()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            /* The two summaries as a walk with every server offline shows them: a late cycle and one connection failure, both worded in full. */
            Find<TextBlock>(tab, "SkippedCyclesSummary").Text = "Skipped cycles in 24 h: 3 late cycles, 7 slots skipped. Last 2026-11-07 14:44:10: slowest step wait_stats, 41.2 s.";
            var failure = Find<TextBlock>(tab, "ConnectionFailureSummary");
            failure.Text = "One connection failure at 2026-11-07 14:44:10 reached 12 collectors: each missed that one run and has no other error in the last 7 days. Nothing to do unless it repeats.";
            failure.Visibility = Visibility.Visible;

            var chart = Find<FrameworkElement>(tab, "CollectorDurationChart");
            Reveal(chart);
            Settle(window);
            var card = Find<FrameworkElement>(tab, "CollectionHealthCard");
            if (card.IsVisible)
            {
                found.Add($"on Duration Trends the Collection Cycles card takes {card.ActualHeight:F0} px of the page");
            }

            var page = PageOf(chart);
            if (page.ScrollableHeight > 0.5)
            {
                found.Add($"Duration Trends scrolls {page.ScrollableHeight:F0} px inside its {page.ViewportHeight:F0} px viewport");
            }

            var grid = Find<DataGrid>(tab, "CollectionHealthGrid");
            Reveal(grid);
            Settle(window);
            if (!card.IsVisible)
            {
                found.Add("on Health Summary the Collection Cycles card is not shown");
            }
            else if (Bounds(card, tab).Bottom > Bounds(grid, tab).Top + 0.5)
            {
                found.Add("on Health Summary the Collection Cycles card does not sit above the grid");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void LongQueries_SaysTheTraceIsOffOnce_AndItsTitleKeepsToOneLine_At900x600()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var grid = Find<DataGrid>(tab, "LongQueryCompletionsGrid");
            Reveal(grid);
            Settle(window);
            /* The refresh sets the banner and the grid's empty text before its first await; the read itself runs against the empty store. */
            var refresh = (Task)Invoke(tab, "RefreshLongQueriesAsync", 24, null, null)!;
            var banner = Find<TextBlock>(tab, "LongQueriesDisabledWarning");
            var said = new[] { banner.Visibility == Visibility.Visible ? banner.Text : "", EmptyState.GetText(grid) ?? "" }
                .Count(t => t.Contains("trace", StringComparison.OrdinalIgnoreCase) && t.Contains("off", StringComparison.OrdinalIgnoreCase));
            if (said != 1)
            {
                found.Add($"the page says the trace is off {said} times (banner: {banner.Visibility}; grid: '{EmptyState.GetText(grid)}')");
            }

            Settle(window);
            if (tab.FindName("LongQueriesTitle") is not TextBlock title)
            {
                found.Add("the page has no one-line title");
            }
            else if (title.ActualHeight > title.FontSize * 1.8)
            {
                found.Add($"the title wraps: {title.ActualHeight:F0} px tall at {title.FontSize} px");
            }

            Pump(refresh);
            return found;
        }, isLongQueryTraceEnabled: () => false);

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheFileIoLatencyLegends_KeepToOneRow_WithAMoreEntry_AtAMaximizedWindow()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(1550), 925, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var write = Find<ScottPlot.WPF.WpfPlot>(tab, "FileIoWriteChart");
            var read = Find<ScottPlot.WPF.WpfPlot>(tab, "FileIoReadChart");
            Reveal(write);
            Settle(window);
            var at = new DateTime(2026, 11, 7, 0, 0, 0, DateTimeKind.Utc);
            var points = new List<FileIoTrendPoint>();
            for (var file = 1; file <= 8; file++)
            {
                for (var i = 0; i < 12; i++)
                {
                    points.Add(new FileIoTrendPoint
                    {
                        CollectionTime = at.AddHours(i),
                        DatabaseName = "SalesArchive",
                        FileName = $"SalesArchive_Data{file:00}",
                        AvgReadLatencyMs = 4 + file,
                        AvgWriteLatencyMs = 6 + file,
                        AvgQueuedReadLatencyMs = 1,
                        AvgQueuedWriteLatencyMs = 1,
                        Reads = 100,
                        Writes = 100,
                        InReadTen = true,
                        InWriteTen = true,
                    });
                }
            }

            Invoke(tab, "UpdateFileIoCharts", points, 24, null, null);
            Settle(window);
            foreach (var (chart, name) in new[] { (read, "Read Latency"), (write, "Write Latency") })
            {
                var entries = chart.Plot.Legend.ManualItems;
                if (!entries.Any(e => e.LabelText.EndsWith(" more", StringComparison.Ordinal)))
                {
                    found.Add($"{name}: the legend lays out all {chart.Plot.Legend.GetItems().Length} entries under the plot instead of one row and a '+N more' entry");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheOverviewLanes_SayTheyHaveNoData_WhenTheRangeIsEmpty()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var lanes = Find<CorrelatedTimelineLanesControl>(tab, "CorrelatedLanes");
            Reveal(lanes);
            Settle(window);
            var none = new List<(double Time, double Value)>();
            lanes.UpdateCpuLane(none, none);
            lanes.UpdateLane(lanes.WaitStatsChart, "Wait ms/sec", none, "#FFB74D");
            lanes.UpdateBlockingLane(none, none, windowSpanDays: 1);
            lanes.SyncXAxes(24, null, null);
            Settle(window);
            foreach (var (chart, name) in new[] { (lanes.CpuChart, "CPU %"), (lanes.WaitStatsChart, "Wait ms/sec"), (lanes.BlockingChart, "Blocking & Deadlocking") })
            {
                var said = chart.Plot.GetPlottables<ScottPlot.Plottables.Annotation>().Select(a => a.LabelText).FirstOrDefault(t => t.StartsWith("No ", StringComparison.Ordinal));
                if (said is null)
                {
                    found.Add($"{name}: the empty lane draws its axes and says nothing");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheCpuComparisonChart_DrawsSystemAndSqlCpu_InTwoColours()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var chart = Find<ScottPlot.WPF.WpfPlot>(tab, "CpuComparisonChart");
            Reveal(chart);
            Settle(window);
            var at = new DateTime(2026, 11, 7, 0, 0, 0, DateTimeKind.Utc);
            var data = Enumerable.Range(0, 12).Select(i => new SystemHealthRecord { EventTime = at.AddHours(i), SystemCpuUtilization = 40 + i, SqlCpuUtilization = 20 + i }).ToList();
            var renderer = tab.GetType().GetProperty("SysHealthRenderer", Instance)!.GetValue(tab)!;
            Invoke(renderer, "RenderCpuComparisonChart", chart, null, data, at.ToOADate(), at.AddHours(12).ToOADate());
            Settle(window);
            var colours = chart.Plot.GetPlottables<ScottPlot.Plottables.Scatter>().Select(s => s.Color.ToHex()).Distinct().ToList();
            if (colours.Count < 2)
            {
                found.Add($"System CPU % and SQL CPU % share one colour ({string.Join(", ", colours)})");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// A lane whose read failed says only that: the empty sentence would claim the window holds no data, which a read that did not
    /// return cannot establish. The CPU lane fails whole; the Blocking lane draws the series that read (here none) and is then marked.
    /// </summary>
    [Fact]
    public void TheOverviewLanes_SayOnlyThatTheReadFailed_WhenItDid()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var lanes = Find<CorrelatedTimelineLanesControl>(tab, "CorrelatedLanes");
            Reveal(lanes);
            Settle(window);
            var none = new List<(double Time, double Value)>();
            Invoke(lanes, "ShowFailed", lanes.CpuChart, "CPU %");
            lanes.UpdateBlockingLane(none, none, windowSpanDays: 1);
            var markFailed = typeof(CorrelatedTimelineLanesControl).GetMethod("MarkFailed", Instance | System.Reflection.BindingFlags.Static)
                ?? throw new Xunit.Sdk.XunitException("the lanes control has no MarkFailed");
            markFailed.Invoke(lanes, new object[] { lanes.BlockingChart });
            lanes.SyncXAxes(24, null, null);
            Settle(window);
            foreach (var (chart, name) in new[] { (lanes.CpuChart, "CPU %"), (lanes.BlockingChart, "Blocking & Deadlocking") })
            {
                var said = chart.Plot.GetPlottables<ScottPlot.Plottables.Annotation>().Select(a => a.LabelText).ToList();
                if (said.Count != 1 || said[0] != "read failed")
                {
                    found.Add($"{name}: the failed lane says {string.Join(" and ", said.Select(t => $"'{t}'"))}");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The CPU Scheduler page's least height follows its content: the chart's floor over the rows the grid takes. The page scrolls
    /// only when the two do not fit the viewport, and then by exactly what they need; the chart keeps its floor and the grid shows
    /// all its rows either way.
    /// </summary>
    [Fact]
    public void TheCpuSchedulerPage_ScrollsOnlyWhenTheChartFloorAndTheGridsRowsDoNotFit()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var grid = Find<DataGrid>(tab, "CpuSchedulerGrid");
            Reveal(grid);
            Settle(window);
            var page = PageOf(grid);
            var layout = (Grid)page.Content;
            foreach (var (rows, height) in new[] { (3, 600), (8, 600), (8, 450) })
            {
                grid.ItemsSource = Enumerable.Range(1, rows)
                    .Select(i => new { Metric = $"Metric {i}", Value = $"{i * 10}", CollectionTimeLocal = "2026-11-07 10:00:00", IsWarning = i == 3, Hint = $"hint {i}" })
                    .ToList();
                Resize(window, PaneWidth(900), height);
                Settle(window);
                var needed = ChartLayout.MinPlotHeight + grid.ActualHeight + PageScroll.ChartOverGridGap;
                var at = $"{rows} rows at {height} tall ({page.ViewportHeight:F0} px viewport, {grid.ActualHeight:F0} px grid)";
                if (Math.Abs(page.ExtentHeight - Math.Max(page.ViewportHeight, needed)) > 1)
                {
                    found.Add($"{at}: the page's extent is {page.ExtentHeight:F0} px where the chart floor and the grid need {needed:F0}");
                }

                if (needed <= page.ViewportHeight && page.ScrollableHeight > 0.5)
                {
                    found.Add($"{at}: the page scrolls {page.ScrollableHeight:F0} px though its content fits");
                }

                if (layout.RowDefinitions[0].ActualHeight + 0.5 < ChartLayout.MinPlotHeight)
                {
                    found.Add($"{at}: the chart row is {layout.RowDefinitions[0].ActualHeight:F0} px");
                }

                if (InnerScroll(grid).ScrollableHeight > 0.5)
                {
                    found.Add($"{at}: the grid scrolls {InnerScroll(grid).ScrollableHeight:F0} px of its own rows");
                }
            }

            if (page.ScrollableHeight < 0.5)
            {
                found.Add($"8 rows at 450 tall: the page does not scroll in a {page.ViewportHeight:F0} px viewport");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The CPU Scheduler page writes its least height from the grid's size, which is layout written from a layout event. At each size
    /// and row count the page must settle: the dispatcher reaches idle inside the settle budget, the grid raises no further size change
    /// once it has, and the page's minimum is the chart floor plus the grid's height plus the gap.
    /// </summary>
    [Theory]
    [InlineData(900, 600, 3)]
    [InlineData(900, 600, 8)]
    [InlineData(1280, 800, 3)]
    [InlineData(1280, 800, 8)]
    public void TheCpuSchedulerPage_SettlesAfterItsMinimumFollowsTheGrid(int width, int height, int rows)
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(width), height, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            var grid = Find<DataGrid>(tab, "CpuSchedulerGrid");
            Reveal(grid);
            Settle(window);
            grid.ItemsSource = Enumerable.Range(1, rows)
                .Select(i => new { Metric = $"Metric {i}", Value = $"{i * 10}", CollectionTimeLocal = "2026-11-07 10:00:00", IsWarning = i == 3, Hint = $"hint {i}" })
                .ToList();
            ClippedTextProbe.Settle(window);
            var changes = 0;
            grid.SizeChanged += (_, _) => changes++;
            ClippedTextProbe.Settle(window);
            if (changes != 0)
            {
                found.Add($"the grid's size changed {changes} more times after the page had settled");
            }

            var page = Find<Grid>(tab, "CpuSchedulerPage");
            var expected = ChartLayout.MinPlotHeight + grid.ActualHeight + PageScroll.ChartOverGridGap;
            if (Math.Abs(page.MinHeight - expected) > 1)
            {
                found.Add($"the page's minimum is {page.MinHeight:F0} px where the chart floor and the {grid.ActualHeight:F0} px grid need {expected:F0}");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}

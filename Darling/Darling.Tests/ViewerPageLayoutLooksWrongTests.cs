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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.ViewerPageLayoutProbe;

namespace Darling.Tests;

/// <summary>
/// #8307, the Viewer's twins of the Lite pages that hid or crowded data at a 900x600 window and at a maximized one. Each fact builds the
/// real server tab on the shared STA thread with the theme loaded, shows it at the width a tab has beside the sidebar, fills the page the
/// way its load does, and reads the realized layout: which scroll viewer has an extent, what a card's share of a page is, how many times a
/// message is said, and how many rows a legend takes.
/// </summary>
public sealed class ViewerPageLayoutLooksWrongTests
{
    [Fact]
    public void TheCpuSchedulerGrid_ShowsAllItsRowsWithNoScrollBarOfItsOwn_AndTheChartKeepsItsFloor_At900x600()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
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
            if (chart.ActualHeight < ChartLayout.MinPlotHeight - 60)
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
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
        {
            var found = new List<string>();
            Find<TextBlock>(tab, "SkippedCyclesLine").Text = "Skipped cycles in 24 h: 3 late cycles, 7 slots skipped. Last 2026-11-07 14:44:10: slowest step wait_stats, 41.2 s.";
            var failure = Find<TextBlock>(tab, "ConnectionFailureLine");
            failure.Text = "One connection failure at 2026-11-07 14:44:10 reached 12 collectors: each missed that one run and has no other error in the window. Nothing to do unless it repeats.";
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
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
        {
            var found = new List<string>();
            var grid = Find<DataGrid>(tab, "LongQueryCompletionsGrid");
            Reveal(grid);
            Settle(window);
            Find<TextBlock>(tab, "LongQueriesDisabledWarning").Text = "The long-query completion trace is OFF for this server. Turn it on in the collector schedules.";
            /* The trace check answered "off": the note shows and the grid's empty text is set, both before the method's first real await. */
            Pump((Task)Invoke(tab, "ApplyTraceNoteAsync", Task.FromResult(false))!);
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

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheTempdbSizeChart_CarriesALegendLikeItsSiblings_SoTheGroupLeavesNoEmptyBandUnderItsAxis()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
        {
            var found = new List<string>();
            var size = Find<ScottPlot.WPF.WpfPlot>(tab, "TempDbSizeChart");
            Reveal(size);
            Settle(window);
            var at = new DateTime(2026, 11, 7, 0, 0, 0, DateTimeKind.Utc);
            var rows = Enumerable.Range(0, 12).Select(i => new TempDbSample(at.AddHours(i), 100 + i, 50, 10, 160 + i, 840 - i, 3, 51, 12)).ToList();
            Invoke(tab, "RenderTempDbUsageChart", rows, at, at.AddHours(12));
            Invoke(tab, "RenderTempDbSizeChart", rows, at, at.AddHours(12));
            Settle(window);

            /* The shared legend is drawn as a panel outside the plot, not as the inset legend, so the panel is what a legend's presence means. */
            if (!size.Plot.Axes.GetPanels().OfType<ScottPlot.Panels.LegendPanel>().Any() || size.Plot.Legend.GetItems().Length == 0)
            {
                found.Add("the tempdb Allocated Size chart has no legend, so the group's shared bottom leaves an empty band under its axis");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheFileIoLatencyLegends_KeepToOneRow_WithAMoreEntry_AtAMaximizedWindow()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(1550), 925, (tab, window) =>
        {
            var found = new List<string>();
            var write = Find<ScottPlot.WPF.WpfPlot>(tab, "FileIoWriteChart");
            var read = Find<ScottPlot.WPF.WpfPlot>(tab, "FileIoReadChart");
            Reveal(write);
            Settle(window);
            var at = new DateTime(2026, 11, 7, 0, 0, 0, DateTimeKind.Utc);
            var points = new List<FileIoLatencyPoint>();
            for (var file = 1; file <= 8; file++)
            {
                for (var i = 0; i < 12; i++)
                {
                    points.Add(new FileIoLatencyPoint(at.AddHours(i), "SalesArchive", $"SalesArchive_Data{file:00}", 4 + file, 6 + file, 1, 1, 100, 100, true, true));
                }
            }

            Invoke(tab, "RenderFileIoLatencyCharts", points, at, at.AddHours(12));
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
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
        {
            var found = new List<string>();
            var lanes = Find<CorrelatedTimelineLanesControl>(tab, "OverviewLanes");
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
}

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
using System.Reflection;
using System.Windows;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8151: every chart the Viewer shows sits in a titled <see cref="ChartPanel"/> card, and its Y axis names only the unit. The real
/// Viewer server tab and FinOps tab are built off screen; every chart is found by walking the tree and read from the rendered control
/// (the card's Title, the plot's left axis title), so a new chart is checked with no change here.
/// </summary>
public sealed class ViewerChartCardsTests
{
    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    /// <summary>The card a chart sits in, found the way the tab's layout finds it: up the logical tree.</summary>
    private static ChartPanel? CardOf(WpfPlot chart)
    {
        for (DependencyObject? up = chart; up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is ChartPanel card)
            {
                return card;
            }
        }

        return null;
    }

    private static bool IsInCorrelatedLanes(WpfPlot chart)
    {
        for (DependencyObject? up = chart; up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is CorrelatedTimelineLanesControl)
            {
                return true;
            }
        }

        return false;
    }

    private static string Name(WpfPlot chart) => string.IsNullOrEmpty(chart.Name) ? "(unnamed chart)" : chart.Name;

    [Fact]
    public void EveryChartOnTheServerTab_SitsInATitledCard_AndItsYAxisShowsOnlyTheUnit() =>
        StaTestThread.Run(() =>
        {
            var failures = new List<string>();
            WithTheme(() =>
            {
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "chart-cards-test", "chart-cards-test", true, null));
                typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    // The overview's correlated lanes (CorrelatedTimelineLanesControl) are one stack of lane plots that share an axis, named by
                    // their lane captions: they are one timeline, not separate chart cards.
                    var charts = ChartLayoutProbe.Logical<WpfPlot>(tab).Where(c => !IsInCorrelatedLanes(c)).ToList();
                    Assert.NotEmpty(charts);
                    var window24 = (ValueTuple<DateTime, DateTime>)typeof(ViewerServerTab).GetMethod("GetWindowUtc", Instance)!.Invoke(tab, null)!;
                    var dataStart = window24.Item2.AddHours(-6);
                    tab.ServerDataStartUtc = dataStart;
                    var (_, fedFailed) = ChartLayoutProbe.FeedRenderMethods(
                        tab,
                        m => (m.Name.StartsWith("Render", StringComparison.Ordinal) || m.Name.StartsWith("Update", StringComparison.Ordinal)) && m.Name.Contains("Chart", StringComparison.Ordinal),
                        (type, dateIndex) =>
                        {
                            if (type == typeof(DateTime))
                            {
                                return dateIndex == 0 ? window24.Item1 : window24.Item2;
                            }

                            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>)
                                ? ChartLayoutProbe.MakeRows(type.GetGenericArguments()[0], dataStart, window24.Item2)
                                : null;
                        });
                    failures.AddRange(fedFailed.Select(f => $"feeding {f}"));

                    foreach (var chart in charts)
                    {
                        failures.AddRange(Check(chart));
                    }
                }
                finally
                {
                    window.Close();
                }
            });

            Assert.True(failures.Count == 0, $"{failures.Count} chart card defects:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
        });

    [Fact]
    public void EveryChartOnTheFinOpsTab_SitsInATitledCard() =>
        StaTestThread.Run(() =>
        {
            var failures = new List<string>();
            WithTheme(() =>
            {
                var tab = new FinOpsTab();
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var charts = ChartLayoutProbe.Logical<WpfPlot>(tab).ToList();
                    Assert.NotEmpty(charts);
                    foreach (var chart in charts)
                    {
                        failures.AddRange(Check(chart));
                    }
                }
                finally
                {
                    window.Close();
                }
            });

            Assert.True(failures.Count == 0, $"{failures.Count} chart card defects:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
        });

    [Fact]
    public void ACardWhoseChartPlotsOnlyZeros_ShowsZeroThroughoutInItsHeader_AndLosesItWhenDataArrives() =>
        StaTestThread.Run(() =>
        {
            WithTheme(() =>
            {
                var chart = new WpfPlot();
                var card = new ChartPanel { Title = "Zeros", Content = chart };
                var host = new System.Windows.Controls.Grid();
                host.Children.Add(card);
                ViewerChartCards.Attach(host);
                var window = new Window { Content = host, Width = 600, Height = 400, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    ChartLayoutProbe.Layout(window);
                    Assert.Null(card.HeaderContent);

                    chart.Plot.Add.Scatter(new[] { 1.0, 2.0, 3.0 }, new[] { 0.0, 0.0, 0.0 }).LegendText = "none";
                    ChartLayoutProbe.Render(chart);
                    var mark = Assert.IsType<System.Windows.Controls.TextBlock>(card.HeaderContent);
                    Assert.Equal("0 throughout", mark.Text);

                    chart.Plot.Clear();
                    chart.Plot.Add.Scatter(new[] { 1.0, 2.0, 3.0 }, new[] { 0.0, 4.0, 0.0 }).LegendText = "some";
                    ChartLayoutProbe.Render(chart);
                    Assert.Null(card.HeaderContent);
                }
                finally
                {
                    window.Close();
                }
            });
        });

    [Fact]
    public void ACardShowingZeroThroughout_LosesTheMark_WhenTheNextRefreshFindsNoRows() =>
        StaTestThread.Run(() =>
        {
            WithTheme(() =>
            {
                // The real Query Duration card: a refresh clears the chart (which swaps in a new plot), draws, and with no rows shows the "No ... data" sentence.
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "chart-cards-test", "chart-cards-test", true, null));
                typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    var update = typeof(ViewerServerTab).GetMethod("UpdateQueryDurationTrendChart", Instance)!;
                    var none = Array.Empty<PerformanceMonitor.Collectors.BaselineDiscontinuity>();
                    QueryTrendSeries Series(params double[] values) => new(
                        values.Select((v, i) => new QueryTrendPoint { CollectionTime = t0.AddMinutes(i), Value = v, ExecutionCount = 1 }).ToList(),
                        PerformanceMonitor.Darling.Storage.RetentionTier.Raw, t0, false);
                    var chart = tab.QueryDurationTrendChart;
                    var card = ViewerChartCards.CardOf(chart)!;
                    ChartLayoutProbe.Reveal(ChartLayoutProbe.TabPath(chart));
                    ChartLayoutProbe.Layout(window);

                    update.Invoke(tab, new object[] { Series(0, 0, 0), t0, t0.AddMinutes(10), none });
                    ChartLayoutProbe.Render(chart);
                    Assert.Equal("0 throughout", Assert.IsType<System.Windows.Controls.TextBlock>(card.HeaderContent).Text);

                    update.Invoke(tab, new object[] { Series(), t0, t0.AddMinutes(10), none });
                    ChartLayoutProbe.Render(chart);
                    Assert.Null(card.HeaderContent);
                }
                finally
                {
                    window.Close();
                }
            });
        });

    [Fact]
    public void TheBlockingSeverityCards_NameTheAnsweringCollectorInTheSubtitle_OnlyWhenTheyDrawRows() =>
        StaTestThread.Run(() =>
        {
            WithTheme(() =>
            {
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "chart-cards-test", "chart-cards-test", true, null));
                typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var t0 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    var rows = new List<BlockingDurationStatsPoint> { new(t0, 2, 500, 300, 250, "DMV snapshot"), new(t0.AddMinutes(1), 1, 100, 100, 100, "DMV snapshot") };
                    var total = typeof(ViewerServerTab).GetMethod("RenderBlockingTotalDurationChart", Instance)!;

                    tab.RenderBlockingDurationChart(rows);
                    total.Invoke(tab, new object[] { rows });
                    Assert.Equal("Source: DMV snapshot", ViewerChartCards.CardOf(tab.BlockingDurationChart)!.Subtitle);
                    Assert.Equal("Source: DMV snapshot", ViewerChartCards.CardOf(tab.BlockingTotalDurationChart)!.Subtitle);
                    Assert.Equal("ms", tab.BlockingDurationChart.Plot.Axes.Left.Label.Text);

                    tab.RenderBlockingDurationChart(new List<BlockingDurationStatsPoint>());
                    total.Invoke(tab, new object[] { new List<BlockingDurationStatsPoint>() });
                    Assert.Equal(string.Empty, ViewerChartCards.CardOf(tab.BlockingDurationChart)!.Subtitle);
                    Assert.Equal(string.Empty, ViewerChartCards.CardOf(tab.BlockingTotalDurationChart)!.Subtitle);
                }
                finally
                {
                    window.Close();
                }
            });
        });

    /// <summary>
    /// A unit is short (twelve characters at most, the length ChartAxisTitle treats as a unit) and has no spaces ("ms", "MB", "GB", "%", "count", "ms/sec", "reports/min"); a name or a name with its unit
    /// ("Lock wait ms/sec", "Memory (GB)") has spaces or is long. The label is read from the plot's left axis after the render methods ran.
    /// </summary>
    /// <summary>#8226: the chart cards' titles are in one case in both apps: title case, the case most of the two apps' titles already had.</summary>
    [Fact]
    public void EveryChartCardTitle_OnTheServerTab_IsInTitleCase() =>
        StaTestThread.Run(() =>
        {
            var problems = new List<string>();
            WithTheme(() =>
            {
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "chart-cards-test", "chart-cards-test", true, null));
                typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    var cards = ChartLayoutProbe.Logical<ChartPanel>(tab).ToList();
                    Assert.NotEmpty(cards);
                    problems.AddRange(cards.Select(c => c.Title).Where(t => !IsTitleCase(t)).Select(t => $"'{t}' is not in title case"));
                }
                finally
                {
                    window.Close();
                }
            });

            Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        });

    /// <summary>
    /// Title case as the chart cards use it: every word of four or more letters starts with a capital, "tempdb" excepted since
    /// that is the database's name. Short words (and, by, of, vs), acronyms and units are free.
    /// </summary>
    internal static bool IsTitleCase(string title) =>
        title.Split(new[] { ' ', '-', '/' }, StringSplitOptions.RemoveEmptyEntries)
            .All(word => word.Length < 4 || word == "tempdb" || !char.IsLetter(word[0]) || char.IsUpper(word[0]));

    private static List<string> Check(WpfPlot chart)
    {
        var found = new List<string>();
        var card = CardOf(chart);
        if (card is null)
        {
            found.Add($"{Name(chart)} is not inside a ChartPanel card");
        }
        else if (string.IsNullOrWhiteSpace(card.Title))
        {
            found.Add($"{Name(chart)} sits in a ChartPanel with no Title");
        }

        var label = chart.Plot.Axes.Left.Label.Text ?? string.Empty;
        if (label.Length > 12 || label.Contains(' ', StringComparison.Ordinal) || label.EndsWith('…') || label.EndsWith("...", StringComparison.Ordinal))
        {
            found.Add($"{Name(chart)} Y axis title is '{label}', more than a unit");
        }

        return found;
    }

    private static void WithTheme(Action body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary
            {
                Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
            };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            body();
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

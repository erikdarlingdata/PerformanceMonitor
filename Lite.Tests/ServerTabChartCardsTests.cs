/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
using Darling.Tests;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8144: Lite's real server tab, built off screen and fed made-up rows through every chart's own update method. Every chart host the
/// tab holds sits in a <see cref="ChartPanel"/> card with a title (the chart's name), and the Y axis of every chart that drew carries
/// only its unit: a name or a parenthesised unit left in the axis title is the defect the card replaces. Read from the rendered
/// controls, not from the markup.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabChartCardsTests
{
    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Fact]
    public void EveryChartOnTheServerTab_SitsInATitledCard_AndItsYAxisIsOnlyAUnit()
    {
        var failures = WithTab(CheckCardsAndAxes);
        Assert.True(failures.Count == 0, $"{failures.Count} chart card defects:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// The blocking charts used to carry the collector that answered in their Y axis title. The axis is only the unit now, so the card's
    /// subtitle names the collector, and only when the chart drew rows (the empty chart names nothing).
    /// </summary>
    [Fact]
    public void TheBlockingCharts_NameTheCollectorInTheCardSubtitle_OnlyWhenTheyDrawRows()
    {
        var t0 = DateTime.UtcNow.AddHours(-1);
        var problems = WithTab(tab =>
        {
            var found = new List<string>();
            static string Subtitle(WpfPlot chart) => ServerTab.CardOf(chart)!.Subtitle;
            static string AxisOf(WpfPlot chart) => chart.Plot.Axes.Left.Label.Text;

            tab.UpdateBlockingDurationChart(new List<BlockingDurationStatsPoint> { new(t0, 2, 500, 300, 250, "DMV snapshot") }, 24, null, null);
            tab.UpdateBlockingTotalDurationChart(new List<BlockingDurationStatsPoint> { new(t0, 2, 500, 300, 250, "blocked-process-report") }, 24, null, null);
            tab.UpdateBlockingTrendChart(new List<TrendPoint> { new() { Time = t0, Count = 2, Source = "DMV snapshot" } }, 24, null, null);
            Expect(found, "duration subtitle", Subtitle(tab.BlockingDurationChart), "Source: DMV snapshot");
            Expect(found, "total duration subtitle", Subtitle(tab.BlockingTotalDurationChart), "Source: blocked-process-report");
            Expect(found, "duration axis", AxisOf(tab.BlockingDurationChart), "ms");
            Expect(found, "snapshot trend axis", AxisOf(tab.BlockingTrendChart), "snapshots/min");

            tab.UpdateBlockingTrendChart(new List<TrendPoint> { new() { Time = t0, Count = 2, Source = "blocked-process-report" } }, 24, null, null);
            Expect(found, "report trend axis", AxisOf(tab.BlockingTrendChart), "reports/min");

            tab.UpdateBlockingDurationChart(new List<BlockingDurationStatsPoint>(), 24, null, null);
            tab.UpdateBlockingTotalDurationChart(new List<BlockingDurationStatsPoint>(), 24, null, null);
            Expect(found, "empty duration subtitle", Subtitle(tab.BlockingDurationChart), string.Empty);
            Expect(found, "empty total duration subtitle", Subtitle(tab.BlockingTotalDurationChart), string.Empty);
            return found;
        });
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A card whose chart drew only zeros says so in its header, and says nothing once a value is drawn.</summary>
    [Fact]
    public void AChartOfAllZeros_ShowsZeroThroughoutInItsCardHeader()
    {
        var t0 = DateTime.UtcNow.AddHours(-1);
        var problems = WithTab(tab =>
        {
            var found = new List<string>();
            static string Header(WpfPlot chart) => ServerTab.CardOf(chart)!.HeaderContent is TextBlock t ? t.Text : string.Empty;
            static TrendPoint Point(DateTime t, int count) => new() { Time = t, Count = count, Source = "blocked-process-report" };

            tab.UpdateBlockingTrendChart(new List<TrendPoint> { Point(t0, 0), Point(t0.AddMinutes(1), 0) }, 24, null, null);
            Expect(found, "all-zero header", Header(tab.BlockingTrendChart), "0 throughout");

            tab.UpdateBlockingTrendChart(new List<TrendPoint> { Point(t0, 0), Point(t0.AddMinutes(1), 3) }, 24, null, null);
            Expect(found, "non-zero header", Header(tab.BlockingTrendChart), string.Empty);
            return found;
        });
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>#8144: a window with no rows at all is not a window of zeros, so the card drops "0 throughout" when the chart goes empty.</summary>
    [Fact]
    public void AChartThatGoesFromZerosToNoRows_DropsZeroThroughoutFromItsCardHeader()
    {
        var t0 = DateTime.UtcNow.AddHours(-1);
        var problems = WithTab(tab =>
        {
            var found = new List<string>();
            static string Header(WpfPlot chart) => ServerTab.CardOf(chart)!.HeaderContent is TextBlock t ? t.Text : string.Empty;
            var update = typeof(ServerTab).GetMethod("UpdateQueryDurationTrendChart", Instance)!;
            void Draw(List<QueryTrendPoint> data) => update.Invoke(tab, new object?[] { data, 24, null, null, new List<BaselineDiscontinuity>() });
            static QueryTrendPoint Point(DateTime t) => new() { CollectionTime = t, Value = 0 };

            Draw(new List<QueryTrendPoint> { Point(t0), Point(t0.AddMinutes(1)) });
            Expect(found, "all-zero header", Header(tab.QueryDurationTrendChart), "0 throughout");

            Draw(new List<QueryTrendPoint>());
            Expect(found, "empty-window header", Header(tab.QueryDurationTrendChart), string.Empty);
            return found;
        });
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    private static void Expect(List<string> found, string what, string? actual, string expected)
    {
        if (!string.Equals(actual ?? string.Empty, expected, StringComparison.Ordinal))
        {
            found.Add($"{what}: expected '{expected}', got '{actual}'");
        }
    }

    private static bool HasLogicalAncestor<T>(DependencyObject element) where T : DependencyObject
    {
        for (var up = LogicalTreeHelper.GetParent(element); up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is T)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Builds the real server tab in a window on the shared STA thread, themed as Lite themes it, and runs <paramref name="body"/> on it.</summary>
    private static List<string> WithTab(Func<ServerTab, List<string>> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabChartCards_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => Run(duckDb, body));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static List<string> Run(DuckDbInitializer duckDb, Func<ServerTab, List<string>> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "chart-cards-test", DisplayName = "chart-cards-test", IsEnabled = true };
            var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
            typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
            var window = new Window { Content = tab, Width = 1536, Height = 912, Left = 0, Top = 0 };
            window.Show();
            try
            {
                return body(tab);
            }
            finally
            {
                window.Close();
            }
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }

    private static List<string> CheckCardsAndAxes(ServerTab tab)
    {
        var dataStart = DateTime.UtcNow.AddHours(-6);
        tab.ServerDataStartUtc = dataStart;
        var (_, failed) = ChartLayoutProbe.FeedRenderMethods(
            tab,
            m => m.Name.StartsWith("Update", StringComparison.Ordinal) && m.Name.Contains("Chart", StringComparison.Ordinal),
            (type, _) =>
            {
                if (type == typeof(int))
                {
                    return 24;
                }

                if (type == typeof(DateTime?))
                {
                    return null;
                }

                if (type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(List<>) || type.GetGenericTypeDefinition() == typeof(IReadOnlyList<>)))
                {
                    return type.GetGenericTypeDefinition() == typeof(List<>)
                        ? ChartLayoutProbe.MakeRows(type.GetGenericArguments()[0], dataStart, DateTime.UtcNow)
                        : Activator.CreateInstance(typeof(List<>).MakeGenericType(type.GetGenericArguments()[0]));
                }

                return null;
            });

        var problems = failed.Select(f => $"feeding {f}").ToList();
        // The Overview tab's timeline lanes are their own control with their own lane layout, not chart hosts of the server tab's markup.
        var charts = ChartLayoutProbe.Logical<WpfPlot>(tab).Where(c => !HasLogicalAncestor<CorrelatedTimelineLanesControl>(c)).ToList();
        if (charts.Count < 40)
        {
            problems.Add($"only {charts.Count} charts found on the server tab");
        }

        var labelled = 0;
        foreach (var chart in charts)
        {
            var name = ChartLayoutProbe.NameOf(chart);
            var card = ServerTab.CardOf(chart);
            if (card is null)
            {
                problems.Add($"{name}: not inside a ChartPanel card");
            }
            else if (string.IsNullOrWhiteSpace(card.Title))
            {
                problems.Add($"{name}: its card has no title");
            }

            var label = chart.Plot.Axes.Left.Label.Text ?? string.Empty;
            if (label.Length == 0)
            {
                continue;
            }

            labelled++;
            // A unit is one token such as "ms", "MB/s", "%" or "count": a space or a bracket means a name or a second unit is in there.
            if (label.Any(char.IsWhiteSpace) || label.Contains('(') || label.EndsWith("...", StringComparison.Ordinal) || label.EndsWith('…'))
            {
                problems.Add($"{name}: Y axis title '{label}' is more than a unit");
            }
        }

        if (labelled < 20)
        {
            problems.Add($"only {labelled} charts drew a Y axis title, so the unit check proved too little");
        }

        return problems;
    }
}

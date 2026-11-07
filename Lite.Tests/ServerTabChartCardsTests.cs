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
using Darling.Tests;
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
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabChartCards_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var failures = StaTestThread.Run(() => Run(duckDb));
            Assert.True(failures.Count == 0, $"{failures.Count} chart card defects:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
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

    private static List<string> Run(DuckDbInitializer duckDb)
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
                    ChartPanel? card = null;
                    for (var up = LogicalTreeHelper.GetParent(chart); up is not null && card is null; up = LogicalTreeHelper.GetParent(up))
                    {
                        card = up as ChartPanel;
                    }

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
}

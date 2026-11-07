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
                window.Show();
                try
                {
                    var charts = ChartLayoutProbe.Logical<WpfPlot>(tab).ToList();
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
                window.Show();
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

    /// <summary>
    /// A unit is short and has no spaces ("ms", "MB", "GB", "%", "count", "ms/s", "MB/s"); a name or a name with its unit
    /// ("Lock wait ms/s", "Memory (GB)") has spaces or is long. The label is read from the plot's left axis after the render methods ran.
    /// </summary>
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
        if (label.Length > 8 || label.Contains(' ', StringComparison.Ordinal) || label.EndsWith('…') || label.EndsWith("...", StringComparison.Ordinal))
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

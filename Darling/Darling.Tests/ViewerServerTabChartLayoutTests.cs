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
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7478: the real Viewer server tab, built off screen at real window sizes and display scales, with every chart fed a made-up reply
/// through its own render method. Every chart the tab holds is found by walking the tree, so a new chart is measured with no change
/// here. Each failure is reported by tab and chart name.
/// </summary>
public sealed class ViewerServerTabChartLayoutTests
{
    private readonly ITestOutputHelper _output;

    public ViewerServerTabChartLayoutTests(ITestOutputHelper output) => _output = output;

    public static TheoryData<int, int, double> Windows
    {
        get
        {
            var data = new TheoryData<int, int, double>();
            foreach (var (w, h, dpi) in ChartLayoutProbe.Windows)
            {
                data.Add(w, h, dpi);
            }

            return data;
        }
    }

    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    /// <summary>Calls every render method of the tab that takes only made-up rows and the window, the way its load path does with the reply.</summary>
    private static (List<string> Fed, List<string> Failed) FeedCharts(ViewerServerTab tab, DateTime windowStart, DateTime windowEnd, DateTime dataStart) =>
        ChartLayoutProbe.FeedRenderMethods(
            tab,
            m => (m.Name.StartsWith("Render", StringComparison.Ordinal) || m.Name.StartsWith("Update", StringComparison.Ordinal)) && m.Name.Contains("Chart", StringComparison.Ordinal),
            (type, dateIndex) =>
            {
                if (type == typeof(DateTime))
                {
                    return dateIndex == 0 ? windowStart : windowEnd;
                }

                return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>)
                    ? ChartLayoutProbe.MakeRows(type.GetGenericArguments()[0], dataStart, windowEnd)
                    : null;
            });

    [Theory]
    [MemberData(nameof(Windows))]
    public void EveryChartOnTheServerTab_KeepsItsLayout_AtThisWindow(int width, int height, double dpi) =>
        StaTestThread.Run(() =>
        {
            var failures = new List<string>();
            WithTheme(() =>
            {
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "chart-layout-test", "chart-layout-test", true, null));
                // Switching to a tab would start that tab's load against a server that is not there: the refresh loop bounces instead.
                typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    var charts = ChartLayoutProbe.Logical<WpfPlot>(tab).ToList();
                    Assert.NotEmpty(charts);
                    foreach (var chart in charts)
                    {
                        PerformanceMonitor.Ui.ChartDisplayScale.Apply(chart, dpi);
                    }

                    var window24 = (ValueTuple<DateTime, DateTime>)typeof(ViewerServerTab).GetMethod("GetWindowUtc", Instance)!.Invoke(tab, null)!;
                    var dataStart = window24.Item2.AddHours(-6);
                    tab.ServerDataStartUtc = dataStart;
                    var (fed, fedFailed) = FeedCharts(tab, window24.Item1, window24.Item2, dataStart);
                    foreach (var line in fedFailed)
                    {
                        failures.Add($"feeding {line}");
                    }

                    var (found, summary) = ChartLayoutProbe.MeasureAll(window, tab, dataStart.ToOADate(), "viewer", $"{width}x{height}-x{dpi}");
                    failures.AddRange(found);
                    _output.WriteLine($"{summary}; {fed.Count} render methods fed");
                }
                finally
                {
                    window.Close();
                }
            });

            Assert.True(failures.Count == 0, $"{failures.Count} chart layout defects at {width}x{height} (x{dpi}):{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
        });

    /// <summary>#8220: the sizes the app's minimum window and a common laptop screen give, in DIPs.</summary>
    public static TheoryData<int, int> AppWindows => new() { { 900, 600 }, { 1280, 800 } };

    /// <summary>#8220: the legend rule on the Viewer's server tab hosted the way the main window hosts it, every keyed chart fed twelve series (the
    /// most any chart lists). At the minimum window each plot keeps 120 DIPs, a side legend leaves the plot 60% of the width, and no legend leaves
    /// more than 15% of the width empty.</summary>
    [Theory]
    [MemberData(nameof(AppWindows))]
    public void EveryChartLegend_FollowsTheRule_AtTheAppWindow(int width, int height) =>
        StaTestThread.Run(() =>
        {
            var failures = new List<string>();
            WithTheme(() =>
            {
                var tab = NewTab();
                var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = width, Height = height, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    var window24 = (ValueTuple<DateTime, DateTime>)typeof(ViewerServerTab).GetMethod("GetWindowUtc", Instance)!.Invoke(tab, null)!;
                    var dataStart = window24.Item2.AddHours(-6);
                    tab.ServerDataStartUtc = dataStart;
                    var (_, fedFailed) = ChartLayoutProbe.FeedRenderMethods(
                        tab,
                        m => (m.Name.StartsWith("Render", StringComparison.Ordinal) || m.Name.StartsWith("Update", StringComparison.Ordinal)) && m.Name.Contains("Chart", StringComparison.Ordinal),
                        (type, dateIndex) => type == typeof(DateTime)
                            ? (dateIndex == 0 ? window24.Item1 : window24.Item2)
                            : type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>)
                                ? ChartLayoutProbe.MakeRows(type.GetGenericArguments()[0], dataStart, window24.Item2, 12)
                                : null);
                    failures.AddRange(fedFailed.Select(f => $"feeding {f}"));
                    var (found, readings) = ChartLayoutProbe.MeasureAllLegends(window, tab, "viewer-legend", $"{width}x{height}");
                    failures.AddRange(found);
                    foreach (var reading in readings)
                    {
                        _output.WriteLine(reading);
                    }
                }
                finally
                {
                    window.Close();
                }
            });

            Assert.True(failures.Count == 0, $"{failures.Count} legend defects at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
        });

    /// <summary>#8220: the charts whose legend is long or beside the plot, filled with the series they really list, at their real place in the
    /// Viewer's tab hosted the way the main window hosts it. The same floors and shares as Lite's test.</summary>
    [Theory]
    [MemberData(nameof(AppWindows))]
    public void TheLongLegendCharts_FollowTheRule_AtTheAppWindow(int width, int height) =>
        StaTestThread.Run(() =>
        {
            var failures = new List<string>();
            WithTheme(() =>
            {
                var tab = NewTab();
                var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = width, Height = height, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    var (found, readings) = ChartLayoutProbe.MeasureHeavyLegends(window, tab, ChartLayoutProbe.HeavyLegendCharts, "viewer-heavy", $"{width}x{height}");
                    failures.AddRange(found);
                    foreach (var reading in readings)
                    {
                        _output.WriteLine(reading);
                    }
                }
                finally
                {
                    window.Close();
                }
            });

            Assert.True(failures.Count == 0, $"{failures.Count} legend defects at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
        });

    private static ViewerServerTab NewTab()
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "chart-layout-test", "chart-layout-test", true, null));
        // Switching to a tab would start that tab's load against a server that is not there: the refresh loop bounces instead.
        typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
        return tab;
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

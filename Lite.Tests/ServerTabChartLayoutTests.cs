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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7478: Lite's real server tab, built off screen at real window sizes and display scales, with every chart fed a made-up reply
/// through its own update method. Every chart the tab holds is found by walking the tree, so a new chart is measured with no
/// change here. Each failure is reported by tab and chart name.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabChartLayoutTests
{
    private readonly ITestOutputHelper _output;

    public ServerTabChartLayoutTests(ITestOutputHelper output) => _output = output;

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

    [Theory]
    [MemberData(nameof(Windows))]
    public void EveryChartOnTheServerTab_KeepsItsLayout_AtThisWindow(int width, int height, double dpi)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabChartLayout_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var failures = StaTestThread.Run(() => Run(duckDb, width, height, dpi));
            Assert.True(failures.Count == 0, $"{failures.Count} chart layout defects at {width}x{height} (x{dpi}):{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private List<string> Run(DuckDbInitializer duckDb, int width, int height, double dpi)
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
            var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "chart-layout-test", DisplayName = "chart-layout-test", IsEnabled = true };
            var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
            // Switching to a tab would start that tab's load against a server that is not there.
            typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
            var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
            window.Show();
            try
            {
                foreach (var chart in ChartLayoutProbe.Logical<WpfPlot>(tab))
                {
                    PerformanceMonitor.Ui.ChartDisplayScale.Apply(chart, dpi);
                }

                var dataStart = DateTime.UtcNow.AddHours(-6);
                tab.ServerDataStartUtc = dataStart;
                var (fed, failed) = ChartLayoutProbe.FeedRenderMethods(
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
                            var rows = type.GetGenericTypeDefinition() == typeof(List<>)
                                ? ChartLayoutProbe.MakeRows(type.GetGenericArguments()[0], dataStart, DateTime.UtcNow)
                                : Activator.CreateInstance(typeof(List<>).MakeGenericType(type.GetGenericArguments()[0]));
                            return rows;
                        }

                        return null;
                    });

                var (found, summary) = ChartLayoutProbe.MeasureAll(window, tab, dataStart.ToOADate(), "lite", $"{width}x{height}-x{dpi}");
                _output.WriteLine($"{summary}; {fed.Count} update methods fed");
                return failed.Select(f => $"feeding {f}").Concat(found).ToList();
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

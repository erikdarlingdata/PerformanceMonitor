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
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8368: an Overview lane whose read fails after it held data draws as a failed lane and nothing else: one "read failed" note, no
/// series left from the last refresh, and nothing for the crosshair to read. A lane whose read fails again says it once. Each fact
/// builds the real lanes control on the shared STA thread with the theme loaded, shows it, drives the lane through the same calls
/// a refresh makes, and reads back what ScottPlot holds and what the crosshair would say.
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
public sealed class OverviewLanesFailedClearTests
{
    private static List<(double Time, double Value)> Series(DateTime from, DateTime to, double lo, double hi) =>
        Enumerable.Range(0, 48).Select(i => (from.AddMinutes(i * (to - from).TotalMinutes / 48).ToOADate(), lo + (hi - lo) * (i % 7) / 6.0)).ToList();

    private static void ShowFailed(CorrelatedTimelineLanesControl control, WpfPlot chart, string title) =>
        (typeof(CorrelatedTimelineLanesControl).GetMethod("ShowFailed", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new Xunit.Sdk.XunitException("the lanes control has no ShowFailed")).Invoke(control, new object[] { chart, title });

    private static void MarkFailed(CorrelatedTimelineLanesControl control, WpfPlot chart) =>
        (typeof(CorrelatedTimelineLanesControl).GetMethod("MarkFailed", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new Xunit.Sdk.XunitException("the lanes control has no MarkFailed")).Invoke(control, new object[] { chart });

    private static CorrelatedCrosshairManager ManagerOf(CorrelatedTimelineLanesControl control) =>
        (CorrelatedCrosshairManager)typeof(CorrelatedTimelineLanesControl).GetField("_crosshairManager", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(control)!;

    /// <summary>What the lane says (its annotations' text) and what else it still draws (every other plottable but the crosshair's own line).</summary>
    private static (List<string> Said, List<string> Drawn) Holds(WpfPlot chart)
    {
        var plottables = chart.Plot.GetPlottables().ToList();
        var said = plottables.OfType<ScottPlot.Plottables.Annotation>().Select(a => a.LabelText).ToList();
        var drawn = plottables.Where(p => p is not ScottPlot.Plottables.Annotation and not ScottPlot.Plottables.VerticalLine)
            .Select(p => p.GetType().Name).ToList();
        return (said, drawn);
    }

    private static void Check(List<string> found, string lane, WpfPlot chart, CorrelatedCrosshairManager manager, double hoverX)
    {
        var (said, drawn) = Holds(chart);
        if (said.Count != 1 || said[0] != "read failed")
        {
            found.Add($"{lane}: the failed lane says {string.Join(" and ", said.Select(t => $"'{t}'"))}");
        }

        if (drawn.Count > 0)
        {
            found.Add($"{lane}: the failed lane still draws {string.Join(", ", drawn)}");
        }

        var hover = manager.HoverLinesAt(chart, hoverX);
        if (hover.Count > 0)
        {
            found.Add($"{lane}: the crosshair still reads {string.Join(" / ", hover)}");
        }
    }

    private static List<string> WithLanes(Func<CorrelatedTimelineLanesControl, CorrelatedCrosshairManager, DateTime, DateTime, List<string>> body)
    {
        List<string> found = [];
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new List<ResourceDictionary>
            {
                new() { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") },
                new() { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") },
            };
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }

            try
            {
                var control = new CorrelatedTimelineLanesControl();
                control.Initialize(null!, 1, () => TimeZoneInfo.Utc);
                var window = new Window { Content = control, Width = 1000, Height = 700, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var to = DateTime.UtcNow;
                    var from = to.AddHours(-24);
                    found = body(control, ManagerOf(control), from, to);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Remove(dictionary);
                }
            }
        });
        return found;
    }

    /// <summary>A lane that drew a day of data and whose next read fails keeps none of it: not the line, not the crosshair's values.</summary>
    [Fact]
    public void ALaneThatHeldData_ThenFailed_SaysOnlyThatTheReadFailed_AndKeepsNoSeries()
    {
        var problems = WithLanes((control, manager, from, to) =>
        {
            var found = new List<string>();
            var mid = from.AddHours(12).ToOADate();

            control.UpdateCpuLane(Series(from, to, 5, 90), Series(from, to, 8, 95));
            control.UpdateLane(control.MemoryChart, "Buffer Pool MB", Series(from, to, 2000, 9000), "#CE93D8");
            control.SyncXAxes(24, null, null);
            if (manager.HoverLinesAt(control.CpuChart, mid).Count == 0 || manager.HoverLinesAt(control.MemoryChart, mid).Count == 0)
            {
                found.Add("the lanes did not take the data they were given");
            }

            ShowFailed(control, control.CpuChart, "CPU %");
            ShowFailed(control, control.MemoryChart, "Memory MB");
            control.SyncXAxes(24, null, null);

            Check(found, "CPU %", control.CpuChart, manager, mid);
            Check(found, "Buffer Pool MB", control.MemoryChart, manager, mid);
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A lane whose read fails twice in a row says "read failed" once.</summary>
    [Fact]
    public void ALaneThatFailed_ThenFailedAgain_SaysThatTheReadFailedOnce()
    {
        var problems = WithLanes((control, manager, from, to) =>
        {
            var found = new List<string>();
            var mid = from.AddHours(12).ToOADate();

            ShowFailed(control, control.WaitStatsChart, "Wait ms/sec");
            control.SyncXAxes(24, null, null);
            ShowFailed(control, control.WaitStatsChart, "Wait ms/sec");
            control.SyncXAxes(24, null, null);

            Check(found, "Wait ms/sec", control.WaitStatsChart, manager, mid);
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The Blocking lane draws the series that read and is then marked when one did not. After a day of bars, a refresh whose two
    /// reads both fail leaves one note and no bars; a second such refresh still leaves one note.
    /// </summary>
    [Fact]
    public void TheBlockingLaneThatHeldBars_ThenHadItsReadsFail_SaysOnlyThatTheReadFailed_OnceEachRefresh()
    {
        var problems = WithLanes((control, manager, from, to) =>
        {
            var found = new List<string>();
            var none = new List<(double Time, double Value)>();

            control.UpdateBlockingLane(Series(from, to, 1, 6), Series(from, to, 0, 2).Take(5).ToList(), windowSpanDays: 1);
            control.SyncXAxes(24, null, null);
            if (Holds(control.BlockingChart).Drawn.Count == 0)
            {
                found.Add("the Blocking lane did not take the bars it was given");
            }

            foreach (var refresh in new[] { "first", "second" })
            {
                control.UpdateBlockingLane(none, none, windowSpanDays: 1);
                MarkFailed(control, control.BlockingChart);
                control.SyncXAxes(24, null, null);
                var (said, drawn) = Holds(control.BlockingChart);
                if (said.Count != 1 || said[0] != "read failed")
                {
                    found.Add($"Blocking, {refresh} failed refresh: the lane says {string.Join(" and ", said.Select(t => $"'{t}'"))}");
                }

                if (drawn.Count > 0)
                {
                    found.Add($"Blocking, {refresh} failed refresh: the lane still draws {string.Join(", ", drawn)}");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}

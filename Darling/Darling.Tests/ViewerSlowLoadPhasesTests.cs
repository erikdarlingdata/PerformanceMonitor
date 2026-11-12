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
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: a slow server-tab load must say which read was slow. Overview, Queries, Daily Summary, Collection Health, File I/O, Perfmon and
/// Latches &amp; Spinlocks logged "no store phase timed" and counted the whole load as client work, because none of their reads was booked on
/// the load's clock. The store here is a port nothing listens on, so every read fails at once; a failed read is still booked as a phase, so
/// the load's clock holds one named phase per read the tab started, which is what the SlowLoad line prints.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerSlowLoadPhasesTests
{
    [Fact]
    public void Describe_ListsTheClientSteps_AfterTheClientShare()
    {
        var phases = new List<(string Phase, long StartedMs, long EndedMs)> { ("CPU read", 0, 900) };
        var steps = new List<(string Step, long Ms)> { ("CPU lane draw", 3000), ("lane axis sync", 700), ("CPU lane draw", 500) };
        var line = ViewerLoadTimer.Describe("Overview", 5600, phases, steps);
        Assert.NotNull(line);
        Assert.Contains("CPU read 900 ms", line, StringComparison.Ordinal);
        Assert.Contains("4700 ms of client work", line, StringComparison.Ordinal);
        Assert.Contains("[client steps: CPU lane draw 3500 ms, lane axis sync 700 ms]", line, StringComparison.Ordinal);
    }

    private static long ClientWorkMs(string line)
    {
        var m = System.Text.RegularExpressions.Regex.Match(line, @"then (\d+) ms of client work");
        Assert.True(m.Success, line);
        return long.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task Finish_NamesAReadStillInFlight_AsStoppedByANewerPick_AndKeepsItsTimeOutOfClientWork()
    {
        /* #8549: a newer pick superseded a load while its read was running, and the load wrote its line then. The read's watcher had not
           booked it, so the line left it out and counted its whole run as client work. */
        var timer = new ViewerLoadTimer(0);
        var read = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        timer.Track("job history read", read.Task);
        await Task.Delay(400, TestContext.Current.CancellationToken);

        timer.MarkSuperseded();
        var line = timer.Finish("Job History");

        Assert.NotNull(line);
        Assert.Contains("job history read ", line, StringComparison.Ordinal);
        Assert.Contains("(stopped by a newer pick)", line, StringComparison.Ordinal);
        Assert.DoesNotContain("no store phase timed", line, StringComparison.Ordinal);
        Assert.True(ClientWorkMs(line) < 300, line);
        read.SetCanceled();
    }

    [Fact]
    public async Task Finish_NamesAReadTheNewerPickCancelled_AsStopped_WhicheverSideOfTheWatcherItLandsOn()
    {
        var timer = new ViewerLoadTimer(0);
        using var cts = new System.Threading.CancellationTokenSource();
        var read = Task.Delay(System.Threading.Timeout.Infinite, cts.Token);
        timer.Track("job history read", read);
        await Task.Delay(400, TestContext.Current.CancellationToken);

        cts.Cancel();
        await Assert.ThrowsAsync<TaskCanceledException>(() => read);
        timer.MarkSuperseded();
        var line = timer.Finish("Job History");

        Assert.NotNull(line);
        Assert.Contains("job history read ", line, StringComparison.Ordinal);
        Assert.Contains("(stopped by a newer pick)", line, StringComparison.Ordinal);
        Assert.True(ClientWorkMs(line) < 300, line);
    }

    [Fact]
    public void Step_RunsTheWork_AndBooksItByName()
    {
        var timer = new ViewerLoadTimer();
        var ran = 0;
        timer.Step("grid bind", () => ran++);
        Assert.Equal(7, timer.Step("chart draw", () => 7));
        Assert.Equal(1, ran);
        Assert.Equal(new[] { "grid bind", "chart draw" }, timer.StepNames);
    }

    [Theory]
    [InlineData("Overview", "CPU read,wait read,blocking read,deadlock read,memory read,file I/O read,CPU baseline read,wait baseline read,I/O baseline read,blocking baseline read")]
    [InlineData("Queries", "query duration trend read,procedure duration trend read,Query Store duration trend read,execution count trend read")]
    [InlineData("Daily Summary", "daily summary read")]
    [InlineData("Collection Health", "health summary read,collection log data start read,collection log read,duration trend read,caveats read")]
    [InlineData("File I/O", "latency trend read")]
    [InlineData("Perfmon", "counter list read")]
    [InlineData("Latches & Spinlocks", "latch trend read,latch snapshot read,spinlock trend read,spinlock snapshot read")]
    public void ALoadOfTheTab_BooksItsStoreReadsByName(string header, string expectedPhases)
    {
        var expected = expectedPhases.Split(',');
        var port = FreePort();
        List<string> booked = new();
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService(ClosedStore(port)),
                new DarlingServer(1, "slow-load-phases", "slow-load-phases", true, null));
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var inner = (TabControl)tab.FindName("InnerTabs");
                var index = inner.Items.OfType<TabItem>().ToList().FindIndex(t => Equals(t.Header, header));
                Assert.True(index >= 0, header);
                if (header == "Queries")
                {
                    /* The Queries tab loads its active sub-tab. Its sub-tabs only select once the outer tab is shown, so pick Performance Trends the way a first open does. */
                    ((TabControl)tab.FindName("QueriesSubTabControl")).SelectedIndex = 0;
                }

                var timer = new ViewerLoadTimer();
                await tab.LoadInnerTabAsync(index, timer);
                booked = await PhasesAsync(timer, expected.Length);
            }
            finally
            {
                window.Close();
            }
        });

        foreach (var phase in expected)
        {
            Assert.True(booked.Contains(phase), "got " + string.Join(",", booked) + " want " + phase);
        }
    }

    /// <summary>A phase is booked by a watcher that finishes just after the read's own awaiter, so wait for the last one.</summary>
    private static async Task<List<string>> PhasesAsync(ViewerLoadTimer timer, int expected)
    {
        for (var wait = 0; wait < 100 && timer.PhaseCount < expected; wait++)
        {
            await Task.Delay(50);
        }

        return timer.PhaseNames.ToList();
    }

    private static string ClosedStore(int port) => $"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=5";

    /// <summary>Runs <paramref name="body"/> on the STA thread with the Viewer's theme resources in the application, as the tabs' cell styles need them.</summary>
    private static void RunThemed(Func<Task> body) => StaTestThread.Run(async () =>
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
            await body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    });

    /// <summary>The line the load would log if it were slow: a clock whose threshold is zero, so the load's own phases and steps are what the line names.</summary>
    private static ViewerLoadTimer SlowClock() => new(slowThresholdMs: 0);

    [Fact]
    public void AgentAlerts_ARefresh_BooksItsServerListRead_AndWritesASlowLoadLine()
    {
        var port = FreePort();
        string? line = null;
        List<string> booked = new();
        RunThemed(async () =>
        {
            var tab = new AgentAlertsTab();
            tab.Initialize(new ViewerDataService(ClosedStore(port)));
            var window = new Window { Content = tab, Width = 1200, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var timer = SlowClock();
                await tab.RefreshAsync(timer);
                booked = await PhasesAsync(timer, 1);
                line = timer.Finish("Agent Alerts");
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Contains("server list read", booked);
        Assert.NotNull(line);
        Assert.Contains("slow load: Agent Alerts", line, StringComparison.Ordinal);
        Assert.Contains("server list read", line, StringComparison.Ordinal);
    }

    [Fact]
    public void AlertHistory_ALoad_BooksItsRead_AndWritesASlowLoadLine()
    {
        var port = FreePort();
        string? line = null;
        List<string> booked = new();
        RunThemed(async () =>
        {
            var tab = new AlertsHistoryTab();
            tab.Initialize(new ViewerDataService(ClosedStore(port)));
            var window = new Window { Content = tab, Width = 1200, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var timer = SlowClock();
                await tab.RefreshAlertsAsync(timer);
                booked = await PhasesAsync(timer, 1);
                line = timer.Finish("Alert History");
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Contains("alert history read", booked);
        Assert.NotNull(line);
        Assert.Contains("slow load: Alert History", line, StringComparison.Ordinal);
        Assert.Contains("alert history read", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Optimization", "idle databases panel,tempdb summary panel,wait categories panel,expensive queries panel,memory grants panel")]
    [InlineData("Server Inventory", "server inventory read")]
    [InlineData("Utilization", "utilization read")]
    [InlineData("Database Resources", "database resources read")]
    [InlineData("Storage Growth", "storage growth read")]
    [InlineData("Locking & Contention", "locking databases read,index locking read,optimized locking note read,separately monitored read")]
    [InlineData("Database Sizes", "database sizes read,table sizes read")]
    [InlineData("Version Store (PVS)", "version store read,version store trend read")]
    [InlineData("High Impact", "high impact queries read")]
    [InlineData("Application Connections", "application connections read")]
    [InlineData("Index Analysis", "index analysis read")]
    [InlineData("Cost Savings", "recommendations read")]
    public void FinOps_ALoadOfTheSubTab_BooksItsReadsByName_AndWritesASlowLoadLine(string header, string expectedPhases)
        => FinOpsSubTabLoad(header, expectedPhases, drill: null);

    /// <summary>The Storage Growth sub-tab's drill levels read other things than its parent grid, so a refresh while drilled in names those reads.</summary>
    [Theory]
    [InlineData(1, "display zone read")]
    [InlineData(2, "index detail read")]
    public void FinOps_StorageGrowthDrilledIn_BooksTheDrillLevelsReadsByName(int level, string expectedPhases)
        => FinOpsSubTabLoad("Storage Growth", expectedPhases, drill: level);

    private static void FinOpsSubTabLoad(string header, string expectedPhases, int? drill)
    {
        var expected = expectedPhases.Split(',');
        var port = FreePort();
        string? line = null;
        List<string> booked = new();
        RunThemed(async () =>
        {
            var tab = new FinOpsTab();
            tab.Initialize(new ViewerDataService(ClosedStore(port)));
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var selector = (ComboBox)tab.FindName("ServerSelector");
                selector.ItemsSource = new[] { new DarlingServer(1, "finops-slow-load", "finops-slow-load", true, null) };
                selector.SelectedIndex = 0;
                var subTabs = (TabControl)tab.FindName("FinOpsSubTabControl");
                subTabs.SelectedIndex = subTabs.Items.OfType<TabItem>().ToList().FindIndex(t => Equals(t.Header, header));
                Assert.True(subTabs.SelectedIndex >= 0, header);
                if (drill is int level)
                {
                    /* The drill state is private to the tab (a row double-click sets it). Set it the way that click does: level, then the database, schema and table. */
                    const System.Reflection.BindingFlags instance = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic;
                    var levelField = typeof(FinOpsTab).GetField("_finopsStorageLevel", instance)!;
                    levelField.SetValue(tab, Enum.ToObject(levelField.FieldType, level));
                    typeof(FinOpsTab).GetField("_finopsObjDrillDb", instance)!.SetValue(tab, "drilldb");
                    typeof(FinOpsTab).GetField("_finopsObjDrillSchema", instance)!.SetValue(tab, "dbo");
                    typeof(FinOpsTab).GetField("_finopsObjDrillTable", instance)!.SetValue(tab, "drilltable");
                }

                var timer = SlowClock();
                await tab.LoadActiveSubTabGuardedAsync(timer, logSlowLoad: false);
                booked = await PhasesAsync(timer, expected.Length);
                line = timer.Finish("FinOps");
            }
            finally
            {
                window.Close();
            }
        });

        foreach (var phase in expected)
        {
            Assert.True(booked.Contains(phase), "got " + string.Join(",", booked) + " want " + phase);
            Assert.Contains(phase, line, StringComparison.Ordinal);
        }

        Assert.Contains("slow load: FinOps > " + header, line, StringComparison.Ordinal);
    }

    [Fact]
    public void AlertHistory_AFirstOpen_CountsTheWaitAndTheLayout_AndWritesOneLine()
    {
        var port = FreePort();
        var lines = new List<string>();
        RunThemed(async () =>
        {
            var tab = new AlertsHistoryTab { FirstOpenSlowThresholdMs = 0, SlowLoadSink = lines.Add };
            tab.Initialize(new ViewerDataService(ClosedStore(port)));
            var window = new Window { Content = tab, Width = 1200, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                await Task.Delay(150);
                tab.FirstOpenRefreshStarted();
                await tab.RefreshAlertsAsync();
            }
            finally
            {
                window.Close();
            }
        });

        var line = Assert.Single(lines);
        Assert.Contains("slow load: first open > Alert History", line, StringComparison.Ordinal);
        Assert.Contains("waited for the shell to start this refresh", line, StringComparison.Ordinal);
        /* The wait is booked before the read, and the closed store fails the read, so the line shows the wait and the clock that ran from the show. */
        Assert.True(line.Contains("waited for the shell to start this refresh 1", StringComparison.Ordinal)
            || line.Contains("waited for the shell to start this refresh 2", StringComparison.Ordinal), line);
    }

    [Fact]
    public void ASlowClock_NamesTheLayoutPassItWaitedFor()
    {
        string? line = null;
        RunThemed(async () =>
        {
            var window = new Window { Width = 100, Height = 100 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var timer = SlowClock();
                await timer.SettleLayoutAsync(window.Dispatcher);
                line = timer.Finish("Anything");
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Contains("layout and render", line, StringComparison.Ordinal);
    }

    [Fact]
    public void JobHistory_ALoad_BooksTheGridRead_AndEachChoicesRead_OnItsOwn()
    {
        var port = FreePort();
        string? line = null;
        List<string> booked = new();
        RunThemed(async () =>
        {
            var tab = new JobHistoryTab();
            tab.Initialize(new ViewerDataService(ClosedStore(port)));
            var window = new Window { Content = tab, Width = 1200, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var timer = SlowClock();
                await tab.RefreshJobsAsync(timer);
                booked = await PhasesAsync(timer, 4);
                line = timer.Finish("Job History");
            }
            finally
            {
                window.Close();
            }
        });

        /* The Category list read is what took 21 s in the field and wrote no line: it is booked apart from the grid's read and the server list. */
        foreach (var phase in new[] { "job history read", "category choices read", "server choices read", "data start read" })
        {
            Assert.True(booked.Contains(phase), "got " + string.Join(",", booked) + " want " + phase);
        }

        Assert.NotNull(line);
        Assert.Contains("slow load: Job History", line, StringComparison.Ordinal);
        Assert.Contains("category choices read", line, StringComparison.Ordinal);
    }

    [Fact]
    public void JobHistory_AFirstOpen_CountsTheWait_AndWritesOneLine()
    {
        var port = FreePort();
        var lines = new List<string>();
        RunThemed(async () =>
        {
            var tab = new JobHistoryTab { FirstOpenSlowThresholdMs = 0, SlowLoadSink = lines.Add };
            tab.Initialize(new ViewerDataService(ClosedStore(port)));
            var window = new Window { Content = tab, Width = 1200, Height = 800 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                await Task.Delay(150);
                tab.FirstOpenRefreshStarted();
                await tab.RefreshJobsAsync();
            }
            finally
            {
                window.Close();
            }
        });

        var line = Assert.Single(lines);
        Assert.Contains("slow load: first open > Job History", line, StringComparison.Ordinal);
        Assert.Contains("waited for the shell to start this refresh", line, StringComparison.Ordinal);
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

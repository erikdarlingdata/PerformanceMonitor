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
using PerformanceMonitor.Collectors;
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
        /* The read ran 400 ms. Counted as client work it would make the client's share at least the read's own time; the share here is only the gap between
           MarkSuperseded and Finish, so compare the two lines of the same load and not a fixed wall-clock number (#9249). */
        Assert.True(ClientWorkMs(line) < ReadMs(line, "job history read"), line);
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
        Assert.True(ClientWorkMs(line) < ReadMs(line, "job history read"), line);
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
    [InlineData("Queries", "query duration and execution count trend read,procedure duration trend read,Query Store duration trend read")]
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

    private static long ReadMs(string line, string read)
    {
        var m = System.Text.RegularExpressions.Regex.Match(line, System.Text.RegularExpressions.Regex.Escape(read) + @" (\d+) ms");
        Assert.True(m.Success, line);
        return long.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// What the client steps the line names add up to (the grid builds and chart draws the load timed as steps).
    /// </summary>
    private static long ClientStepsMs(string line)
    {
        var m = System.Text.RegularExpressions.Regex.Match(line, @"\[client steps: ([^\]]*)\]");
        Assert.True(m.Success, line);
        return System.Text.RegularExpressions.Regex.Matches(m.Groups[1].Value, @"(\d+) ms")
            .Sum(x => long.Parse(x.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A seamed read's time must not be left in "client work after the last read". What may stay there is the draws, which the line names as client
    /// steps, so the bound is a second plus their own time and not a fixed wall-clock number: a slow runner stretches its draws and the bound with them
    /// (#9249). A read counted as client work adds its whole time and breaks the bound.
    /// </summary>
    private static void AssertClientWorkIsOnlyTheDraws(string line) =>
        Assert.True(ClientWorkMs(line) < 1000 + ClientStepsMs(line), line);

    [Fact]
    public void CpuTab_ALoadWithSlowReads_NamesEachRead_AndLeavesOnlyTheUnaccountedTimeToClientWork()
    {
        /* #9112: a CPU refresh logged its whole read time as "client work after the last read" because none of its reads was handed to
           the load's clock. The tab's reads here take known time (the scheduler pair runs side by side). */
        const int utilizationMs = 1500;
        const int trendMs = 1000;
        const int snapshotMs = 700;
        string? line = null;
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService(FailsAtOnceStore()),
                new DarlingServer(1, "cpu-slow-load", "cpu-slow-load", true, null));
            tab.CpuReadsOverride = new ViewerServerTab.CpuTabReads(
                async (_, _) =>
                {
                    await Task.Delay(utilizationMs);
                    return new List<CpuUtilizationSample>();
                },
                async (_, _) =>
                {
                    await Task.Delay(trendMs);
                    return new List<CpuSchedulerTrendPoint>();
                },
                async (_, _) =>
                {
                    await Task.Delay(snapshotMs);
                    return null;
                });
            tab.SlowLoadSink = text => line = text;
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var inner = (TabControl)tab.FindName("InnerTabs");
                var index = inner.Items.OfType<TabItem>().ToList().FindIndex(t => Equals(t.Header, "CPU"));
                Assert.True(index >= 0);
                await tab.LoadInnerTabAsync(index, new ViewerLoadTimer(0));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.NotNull(line);
        Assert.True(ReadMs(line, "CPU read") >= utilizationMs - 100, line);
        Assert.True(ReadMs(line, "CPU scheduler trend read") >= trendMs - 100, line);
        Assert.True(ReadMs(line, "CPU scheduler snapshot read") >= snapshotMs - 100, line);
        Assert.Contains("CPU chart draw", line, StringComparison.Ordinal);
        /* Untimed, the 3.2 s of reads would all sit here. Only the draws after the last read remain. */
        AssertClientWorkIsOnlyTheDraws(line);
    }

    /// <summary>Loads one inner tab of a server tab whose reads <paramref name="seam"/> has replaced, on a clock that always writes its line, and returns the line.</summary>
    private static string LoadTabAndReadTheLine(DarlingServer server, Action<ViewerServerTab> seam, Func<ViewerServerTab, int> tabIndex)
    {
        string? line = null;
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(new ViewerDataService(FailsAtOnceStore()), server);
            seam(tab);
            tab.SlowLoadSink = text => line = text;
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                await tab.LoadInnerTabAsync(tabIndex(tab), new ViewerLoadTimer(0));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.NotNull(line);
        return line;
    }

    private static int InnerTabIndexOf(ViewerServerTab tab, string header)
    {
        var inner = (TabControl)tab.FindName("InnerTabs");
        var index = inner.Items.OfType<TabItem>().ToList().FindIndex(t => Equals(t.Header, header));
        Assert.True(index >= 0);
        return index;
    }

    [Fact]
    public void MemoryTab_ALoadWithSlowReads_NamesEachRead_NestedPlanCacheAndSemaphoreOnesToo()
    {
        /* #9112: the Memory tab's refresh read its summary, trends, pressure events, Plan Cache and Resource Semaphore sub-tabs with none
           of them on the load's clock, so the whole read time was logged as "client work after the last read". */
        var line = LoadTabAndReadTheLine(
            new DarlingServer(1, "memory-slow-load", "memory-slow-load", true, null),
            tab => tab.MemoryReadsOverride = new ViewerServerTab.MemoryTabReads(
                async () => { await Task.Delay(1200); return null; },
                async (_, _) => { await Task.Delay(900); return new List<MemoryTrendPoint>(); },
                async (_, _) => { await Task.Delay(300); return new List<MemoryGrantTrendPoint>(); },
                async (_, _) => { await Task.Delay(300); return new List<string>(); },
                async (_, _) => { await Task.Delay(300); return new List<MemoryGrantChartPoint>(); },
                async (_, _) => { await Task.Delay(300); return (DateTime?)null; },
                async (_, _) => { await Task.Delay(600); return new List<MemoryPressureEventRow>(); },
                async (_, _, _) => { await Task.Delay(300); return new Dictionary<string, List<MemoryClerkTrendPoint>>(); },
                async (_, _) => { await Task.Delay(700); return new List<PlanCacheTrendPoint>(); },
                async (_, _) => { await Task.Delay(500); return new List<PlanCacheGroupRow>(); },
                async (_, _) => { await Task.Delay(600); return new PlanCacheSummary(0, 0, null); },
                async (_, _) => { await Task.Delay(800); return new List<MemoryGrantWindowRow>(); },
                async (_, _) => { await Task.Delay(400); return new List<ResourceSemaphoreRow>(); }),
            tab => InnerTabIndexOf(tab, "Memory"));

        Assert.True(ReadMs(line, "Memory summary read") >= 1200 - 100, line);
        Assert.True(ReadMs(line, "Memory trend read") >= 900 - 100, line);
        Assert.True(ReadMs(line, "Memory pressure events read") >= 600 - 100, line);
        Assert.True(ReadMs(line, "Memory grant trend read") >= 300 - 100, line);
        Assert.True(ReadMs(line, "Memory clerk list read") >= 300 - 100, line);
        Assert.True(ReadMs(line, "Memory grant chart read") >= 300 - 100, line);
        Assert.True(ReadMs(line, "Memory pressure data start read") >= 300 - 100, line);
        Assert.True(ReadMs(line, "Plan Cache by type read") >= 500 - 100, line);
        Assert.True(ReadMs(line, "Plan Cache trend read") >= 700 - 100, line);
        Assert.True(ReadMs(line, "Plan Cache summary read") >= 600 - 100, line);
        Assert.True(ReadMs(line, "Resource Semaphore window read") >= 800 - 100, line);
        Assert.True(ReadMs(line, "Resource Semaphore snapshot read") >= 400 - 100, line);
        Assert.Contains("Memory chart draw", line, StringComparison.Ordinal);
        /* Untimed, about 3 s of reads would all sit here. Only the draws and the unseamed probes (they fail at once here, #9249) remain. */
        AssertClientWorkIsOnlyTheDraws(line);
    }

    [Fact]
    public void WaitStatsTab_ALoadWithSlowReads_NamesEachRead_AndLeavesOnlyTheUnaccountedTimeToClientWork()
    {
        /* #9112: the Wait Stats tab's picker read and trend read were not on the load's clock. */
        var line = LoadTabAndReadTheLine(
            new DarlingServer(1, "waits-slow-load", "waits-slow-load", true, null),
            tab => tab.WaitStatsReadsOverride = new ViewerServerTab.WaitStatsTabReads(
                async (_, _) =>
                {
                    await Task.Delay(1500);
                    return new List<string> { "CXPACKET", "PAGEIOLATCH_SH" };
                },
                async (_, _, _) =>
                {
                    await Task.Delay(1000);
                    return new Dictionary<string, List<WaitStatsTrendPoint>>();
                }),
            tab => InnerTabIndexOf(tab, "Wait Stats"));

        Assert.True(ReadMs(line, "Wait Stats picker read") >= 1500 - 100, line);
        Assert.True(ReadMs(line, "Wait Stats trend read") >= 1000 - 100, line);
        Assert.Contains("Wait Stats chart draw", line, StringComparison.Ordinal);
        AssertClientWorkIsOnlyTheDraws(line);
    }

    [Fact]
    public void PostgresStorageTab_ALoadWithSlowReads_NamesEachRead_AndLeavesOnlyTheUnaccountedTimeToClientWork()
    {
        /* #9112: a PostgreSQL tab's reads were not on the load's clock either. Storage is the one with two plain reads (they run side by side). */
        var line = LoadTabAndReadTheLine(
            new DarlingServer(1, "pg-storage-slow-load", "pg-storage-slow-load", true, sqlMajorVersion: 0,
                engineKind: MonitoredEngineKind.Postgres, postgresMajorVersion: 17),
            tab => tab.PgStorageReadsOverride = new ViewerServerTab.PgStorageTabReads(
                async (_, _, _) =>
                {
                    await Task.Delay(1500);
                    return new List<DarlingPgTableBloatReader.PgTableBloatRow>();
                },
                async (_, _, _) =>
                {
                    await Task.Delay(1000);
                    return new List<DarlingPgIndexUsageReader.PgIndexUsageRow>();
                }),
            _ => ViewerServerTab.PgStorageInnerTabIndex);

        Assert.True(ReadMs(line, "PostgreSQL table bloat read") >= 1500 - 100, line);
        Assert.True(ReadMs(line, "PostgreSQL index usage read") >= 1000 - 100, line);
        Assert.Contains("PostgreSQL table bloat grid build", line, StringComparison.Ordinal);
        AssertClientWorkIsOnlyTheDraws(line);
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

    /// <summary>A store whose every read fails at once: a unix socket path nothing is at, so no connect attempt waits on the network. A closed TCP port makes
    /// Windows retry the connect for about 2 s, and every read the test does not seam (a probe, a nested sub-tab's read) then sits in the line as client work
    /// when its watcher has not booked it as the load ends (#9249).</summary>
    private static string FailsAtOnceStore() => "Host=/no-such-socket-dir;Port=5432;Database=darling;Username=darling;Timeout=5";

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

    [Fact]
    public void Describe_AnOverTheThresholdLoad_SplitsStoreAndRender_AndNamesRowsAndWindow()
    {
        /* #9426: the store share ends when the last read is in hand, the render share is the rest of the load. */
        var phases = new List<(string Phase, long StartedMs, long EndedMs)> { ("grid read", 0, 900), ("slicer read", 100, 700) };
        var line = ViewerLoadTimer.Describe("Top Queries", 2500, phases, window: "2026-11-24 08:00 to 2026-11-24 09:00 UTC", rows: 42);

        Assert.NotNull(line);
        Assert.Contains("took 2500 ms (store 900 ms, render 1600 ms, 42 rows, window 2026-11-24 08:00 to 2026-11-24 09:00 UTC)", line, StringComparison.Ordinal);
    }

    [Fact]
    public void ALoadUnderTheThreshold_WritesNoLine()
    {
        var lines = new List<string>();
        var timer = new ViewerLoadTimer();
        timer.Track("grid read", Task.FromResult(new List<int> { 1, 2 }));

        timer.FinishAndLog("Top Queries", lines.Add);

        Assert.Empty(lines);
    }

    [Fact]
    public async Task ALoadOverTheThreshold_WritesOneLine_WithTheRowsItsReadsReturned()
    {
        var lines = new List<string>();
        var timer = new ViewerLoadTimer(0) { Window = "last hour" };
        await timer.Track("grid read", Task.FromResult(new List<int> { 1, 2, 3 }));
        await timer.Track("slicer read", Task.FromResult(new List<string> { "a", "b" }));
        await timer.Track("no rows read", Task.FromResult(7));

        timer.FinishAndLog("Top Queries", lines.Add);

        var line = Assert.Single(lines);
        Assert.Contains(", 5 rows, window last hour)", line, StringComparison.Ordinal);
        Assert.Contains("store ", line, StringComparison.Ordinal);
        Assert.Contains(" ms, render ", line, StringComparison.Ordinal);
    }

    [Fact]
    public void ASinkThatThrows_DoesNotFailTheLoadItDescribes()
    {
        var timer = new ViewerLoadTimer(0);
        var called = 0;

        timer.FinishAndLog("Top Queries", _ =>
        {
            called++;
            throw new System.IO.IOException("the log file cannot be written");
        });

        Assert.Equal(1, called);
    }

    [Fact]
    public void AServerTabLoad_OverTheThreshold_WritesOneLine_SplitIntoStoreAndRender_WithTheWindow()
    {
        /* #9426: through the server tab's shared load path, with a read that takes known time. */
        const int readMs = 400;
        var lines = new List<string>();
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService(FailsAtOnceStore()),
                new DarlingServer(1, "panel-timing", "panel-timing", true, null));
            tab.CpuReadsOverride = new ViewerServerTab.CpuTabReads(
                async (_, _) =>
                {
                    await Task.Delay(readMs);
                    return new List<CpuUtilizationSample>();
                },
                (_, _) => Task.FromResult(new List<CpuSchedulerTrendPoint>()),
                (_, _) => Task.FromResult<CpuSchedulerSnapshot?>(null));
            tab.SlowLoadSink = lines.Add;
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                await tab.LoadInnerTabAsync(InnerTabIndexOf(tab, "CPU"), new ViewerLoadTimer(0));
            }
            finally
            {
                window.Close();
            }
        });

        var line = Assert.Single(lines);
        var m = System.Text.RegularExpressions.Regex.Match(line, @"\(store (\d+) ms, render (\d+) ms, (\d+) rows, window ([^)]* UTC)\)");
        Assert.True(m.Success, line);
        Assert.True(long.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture) >= readMs - 100, line);
        Assert.StartsWith("[panel-timing] slow load: CPU took ", line, StringComparison.Ordinal);
        /* The render share ends after the layout pass the load's binds queued, which the line books as its own client step. */
        Assert.Contains("layout and render ", line, StringComparison.Ordinal);
    }

    [Fact]
    public void AServerTabLoad_TheReaderCancelled_WritesNoLine()
    {
        var lines = new List<string>();
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService(FailsAtOnceStore()),
                new DarlingServer(1, "panel-timing", "panel-timing", true, null));
            using var cts = new System.Threading.CancellationTokenSource();
            tab.LoadStartHook = (_, _) =>
            {
                cts.Cancel();
                return Task.CompletedTask;
            };
            tab.SlowLoadSink = lines.Add;
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                await tab.LoadInnerTabAsync(InnerTabIndexOf(tab, "CPU"), new ViewerLoadTimer(0), cts.Token);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Empty(lines);
    }

    [Fact]
    public void AServerTabLoad_WhoseLogSinkThrows_StillEnds()
    {
        var called = 0;
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService(FailsAtOnceStore()),
                new DarlingServer(1, "panel-timing", "panel-timing", true, null));
            tab.SlowLoadSink = _ =>
            {
                called++;
                throw new System.IO.IOException("the log file cannot be written");
            };
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                await tab.LoadInnerTabAsync(InnerTabIndexOf(tab, "CPU"), new ViewerLoadTimer(0));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Equal(1, called);
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>#9887: the Queries sub-tabs the slow-load line names, with the sub-tab's index, the read the line books for its rows and how many rows its held read answers.</summary>
    private static (int Index, string Read, int Rows, Func<Func<Task>, ViewerServerTab.QueriesSubTabReads> Reads) QueriesSubTab(string subTab)
    {
        static Task<DateTime?> NoStart(DateTime a, DateTime b) => Task.FromResult<DateTime?>(null);
        return subTab switch
        {
            "Query Store Regressions" => (6, "grid read", 3, hold => new ViewerServerTab.QueriesSubTabReads
            {
                RegressionsDataStart = NoStart,
                Regressions = async (_, _) =>
                {
                    await hold();
                    return new ViewerQueryStoreRegressionsRead(new List<ViewerQueryStoreRegressionRow> { new(), new(), new() }, null);
                },
            }),
            "Query Store Clutter" => (7, "grid read", 3, hold => new ViewerServerTab.QueriesSubTabReads
            {
                ClutterDataStart = NoStart,
                Clutter = async (_, _) =>
                {
                    await hold();
                    return new ViewerDataService.QueryStoreClutterResult(
                        new List<ViewerDataService.QueryStoreClutterRow> { new(), new(), new() }, new ViewerDataService.QueryStoreOverheadSummary());
                },
            }),
            "Plan Corrections" => (8, "grid read", 3, hold => new ViewerServerTab.QueriesSubTabReads
            {
                PlanCorrectionsDataStart = NoStart,
                PlanCorrections = async (_, _) =>
                {
                    await hold();
                    return new List<PlanCorrectionRow> { new(), new(), new() };
                },
            }),
            "Query Heatmap" => (9, "heatmap read", 0, hold => new ViewerServerTab.QueriesSubTabReads
            {
                HeatmapDataStart = NoStart,
                Heatmap = async (_, _, _) =>
                {
                    await hold();
                    return new HeatmapResult();
                },
            }),
            _ => throw new ArgumentOutOfRangeException(nameof(subTab), subTab, null),
        };
    }

    /// <summary>#9919: a slow-load threshold no load reaches, so a test that does not hold a read to make a line reads no wall clock. The clock a test
    /// creates before the helper builds and shows a tab counts the build, and the tab's own first-open clock counts the tab's construction, so on a
    /// loaded machine either passes the real 2,000 ms with no read held.</summary>
    private const int OutOfReachMs = int.MaxValue;

    /// <summary>Loads one Queries sub-tab whose store reads <paramref name="reads"/> hands in, on <paramref name="timer"/>, and returns the lines the load wrote.
    /// #9919: the tab's first-open clock, which the sub-tab pick below starts, has its threshold out of reach, so the only line a load writes is the one
    /// <paramref name="timer"/> decides: the test sets that clock's threshold.</summary>
    private static List<string> LoadQueriesSubTab(int subTabIndex, ViewerServerTab.QueriesSubTabReads reads, ViewerLoadTimer timer, Func<Task>? whileLoading = null)
    {
        var lines = new List<string>();
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(new ViewerDataService(FailsAtOnceStore()), new DarlingServer(1, "queries-subtab", "queries-subtab", true, null))
            {
                FirstOpenSlowThresholdMs = OutOfReachMs,
            };
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                /* The sub-tab is picked before the reads are handed in, so a load the pick starts itself reads the (failing) store and not these. */
                ((TabControl)tab.FindName("QueriesSubTabControl")).SelectedIndex = subTabIndex;
                tab.QueriesReadsOverride = reads;
                tab.SlowLoadSink = lines.Add;
                var load = tab.LoadInnerTabAsync(InnerTabIndexOf(tab, "Queries"), timer);
                if (whileLoading is not null)
                {
                    await whileLoading();
                }

                await load;
            }
            finally
            {
                window.Close();
            }
        });

        return lines;
    }

    [Theory]
    [InlineData("Query Store Regressions")]
    [InlineData("Query Store Clutter")]
    [InlineData("Plan Corrections")]
    [InlineData("Query Heatmap")]
    public void QueriesSubTab_AHeldRead_WritesALineNamingTheSubTab_ItsRead_AndTheRowsItAnswered(string subTab)
    {
        /* #9887: these four sub-tabs opened with none of their reads on the load's clock, so a slow open logged all of it as client work and named no
           sub-tab. The read is held until the clock has run on, so its time is the store's and not the client's. */
        const int heldMs = 150;
        var (index, read, rows, reads) = QueriesSubTab(subTab);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var timer = new ViewerLoadTimer(0);

        var lines = LoadQueriesSubTab(index, reads(() =>
        {
            started.TrySetResult();
            return gate.Task;
        }), timer, async () =>
        {
            await started.Task;
            var heldFrom = timer.ElapsedMs;
            while (timer.ElapsedMs - heldFrom < heldMs)
            {
                await Task.Yield();
            }

            gate.SetResult();
        });

        var line = Assert.Single(lines);
        Assert.StartsWith($"[queries-subtab] slow load: Queries > {subTab} took ", line, StringComparison.Ordinal);
        Assert.True(ReadMs(line, read) >= heldMs - 10, line);
        if (rows > 0)
        {
            Assert.Contains($", {rows} rows", line, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("Query Store Regressions", "data start,grid read")]
    [InlineData("Query Store Clutter", "data start,grid read")]
    [InlineData("Plan Corrections", "data start,grid read")]
    [InlineData("Query Heatmap", "data start,heatmap read")]
    public void QueriesSubTab_EachStoreCall_BooksOneTimedRead_AndAFastLoadWritesNoLine(string subTab, string expectedPhases)
    {
        /* #9919: the load's clock has its threshold out of reach, so the line is absent whatever the machine's speed and a sub-tab load that writes a
           line whatever its threshold fails this test. ALoadUnderTheThreshold_WritesNoLine holds the threshold rule on its own. */
        var expected = expectedPhases.Split(',');
        var (index, _, _, reads) = QueriesSubTab(subTab);
        var timer = new ViewerLoadTimer(OutOfReachMs);
        List<string> booked = new();

        var lines = LoadQueriesSubTab(index, reads(() => Task.CompletedTask), timer, async () =>
        {
            booked = await PhasesAsync(timer, expected.Length);
        });

        Assert.Equal(expected.OrderBy(n => n), booked.OrderBy(n => n));
        Assert.Empty(lines);
    }

    [Theory]
    [InlineData("Top Queries by Duration", 3)]
    [InlineData("Top Procedures by Duration", 4)]
    [InlineData("Query Store by Duration", 5)]
    public void QueriesGridRead_TheLineCountsTheGridsRows_NotOnlyTheSlicers(string subTab, int index)
    {
        /* #9887: the grid reads answer a routed read and a tuple, neither a collection, so the line's row count held only the slicer's buckets. */
        var reads = index switch
        {
            3 => new ViewerServerTab.QueriesSubTabReads
            {
                TopQueriesGrid = (_, _) => Task.FromResult(new ViewerRoutedRead<ViewerQueryStatsRow>(new List<ViewerQueryStatsRow> { new(), new(), new(), new() }, "raw", null)),
            },
            4 => new ViewerServerTab.QueriesSubTabReads
            {
                TopProceduresGrid = (_, _) => Task.FromResult(new ViewerRoutedRead<ViewerProcedureStatsRow>(new List<ViewerProcedureStatsRow> { new(), new(), new(), new() }, "raw", null)),
            },
            _ => new ViewerServerTab.QueriesSubTabReads
            {
                QueryStoreGrid = (_, _) => Task.FromResult((new List<ViewerQueryStoreRow> { new(), new(), new(), new() }, (QueryStoreIntervalWide.WideReadPlan?)null)),
            },
        };

        var line = Assert.Single(LoadQueriesSubTab(index, reads, new ViewerLoadTimer(0)));

        Assert.Contains($"Queries > {subTab} took ", line, StringComparison.Ordinal);
        Assert.Contains(", 4 rows", line, StringComparison.Ordinal);
    }
}

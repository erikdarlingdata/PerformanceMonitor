/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: a server tab's first open took 11.7 s in the field and wrote no SlowLoad line, because the only clock started inside the landing
/// sub-tab's load and the reads before it (server clock, data start, freshness, permission count, collection interval) ran outside it.
/// The first open now runs on one clock from the tab's activation to the first stamp. The slow store here is a listener that accepts a
/// connection and never answers, so a read waits out the connection's one-second timeout, and the fast store is a port nothing listens on.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerFirstOpenSlowLoadTests
{
    [Fact]
    public void ASlowReadBeforeTheLandingSubTab_IsNamedInTheFirstOpensSlowLoadLine()
    {
        var lines = new List<string>();
        RunThemed(async () =>
        {
            using var silent = new TcpListener(IPAddress.Loopback, 0);
            silent.Start();
            var port = ((IPEndPoint)silent.LocalEndpoint).Port;
            var tab = new ViewerServerTab(
                new ViewerDataService($"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=1"),
                new DarlingServer(7, "first-open-host.invalid", "first-open-alias", true, null))
            {
                FirstOpenSlowThresholdMs = 500,
                SlowLoadSink = lines.Add,
            };
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                await tab.RefreshActiveInnerTabAsync();
            }
            finally
            {
                window.Close();
                silent.Stop();
            }
        });

        var line = Assert.Single(lines);
        Assert.Contains("[first-open-alias]", line, StringComparison.Ordinal);
        Assert.DoesNotContain("first-open-host.invalid", line, StringComparison.Ordinal);
        Assert.Contains("server tab first open > ", line, StringComparison.Ordinal);
        /* The step that ran before the landing sub-tab's load is named, not lumped into client work. */
        Assert.Contains("server clock read ", line, StringComparison.Ordinal);
        Assert.Contains("waited for the shell to start this refresh ", line, StringComparison.Ordinal);
        Assert.Contains("tab construction ", line, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOpenUnderTheThreshold_WritesNothing()
    {
        var lines = new List<string>();
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService($"Host=127.0.0.1;Port={FreePort()};Database=darling;Username=darling;Timeout=5"),
                new DarlingServer(8, "first-open-fast", "first-open-fast", true, null))
            {
                FirstOpenSlowThresholdMs = 600_000,
                SlowLoadSink = lines.Add,
            };
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                await tab.RefreshActiveInnerTabAsync();
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Empty(lines);
    }

    [Fact]
    public void ASecondRefresh_IsNotAFirstOpen_AndKeepsItsOwnClock()
    {
        var lines = new List<string>();
        RunThemed(async () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService($"Host=127.0.0.1;Port={FreePort()};Database=darling;Username=darling;Timeout=5"),
                new DarlingServer(9, "first-open-twice", "first-open-twice", true, null))
            {
                /* Zero makes every load that runs on a first-open clock write a line. */
                FirstOpenSlowThresholdMs = 0,
                SlowLoadSink = lines.Add,
            };
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                await tab.RefreshActiveInnerTabAsync();
                lines.Clear();
                tab.BeginFirstOpen();
                await tab.RefreshActiveInnerTabAsync();
            }
            finally
            {
                window.Close();
            }
        });

        Assert.DoesNotContain(lines, l => l.Contains("server tab first open", StringComparison.Ordinal));
    }

    /* A later refresh (a sub-tab pick, a range change, the auto-refresh tick) runs the same reads as the first open does. The server clock,
       data start and collection interval reads run before its sub-tab's load, on that refresh's clock too, so its line names them and its
       total counts them. The freshness and permission-count reads run beside the load, and the line names one only when it ended before
       the load did, so neither is asked for here. The first open here writes nothing (its threshold is out of reach), so the one line is
       the later refresh's. */
    [Fact]
    public void ASlowReadBeforeTheSubTabsLoad_IsNamedInALaterRefreshsSlowLoadLine()
    {
        var lines = new List<string>();
        RunThemed(async () =>
        {
            using var silent = new TcpListener(IPAddress.Loopback, 0);
            silent.Start();
            var port = ((IPEndPoint)silent.LocalEndpoint).Port;
            var tab = new ViewerServerTab(
                new ViewerDataService($"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=1"),
                new DarlingServer(10, "later-refresh-host.invalid", "later-refresh-alias", true, null))
            {
                FirstOpenSlowThresholdMs = 600_000,
                SlowLoadSink = lines.Add,
            };
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                await tab.RefreshActiveInnerTabAsync();
                lines.Clear();
                await tab.RefreshActiveInnerTabAsync();
            }
            finally
            {
                window.Close();
                silent.Stop();
            }
        });

        var line = Assert.Single(lines);
        Assert.Contains("[later-refresh-alias]", line, StringComparison.Ordinal);
        Assert.DoesNotContain("server tab first open", line, StringComparison.Ordinal);
        Assert.Contains("server clock read ", line, StringComparison.Ordinal);
        Assert.Contains("server data start read ", line, StringComparison.Ordinal);
        Assert.Contains("collection interval read ", line, StringComparison.Ordinal);
    }

    /* A refresh asked for while a sub-tab's load runs is queued, and loads when that one ends. Its clock starts at the ask, so the wait
       behind the load in flight is in its line by name. The store here tells the test each time a read reaches it, and the test asks for
       a refresh at every one until the first load's line is written: an ask that lands before the load begins is served by that load,
       and the first one after it is the queued refresh's start. Two lines follow, the first open's and the queued refresh's. */
    [Fact]
    public void ARefreshAskedForWhileALoadRuns_BooksItsWaitInItsOwnSlowLoadLine()
    {
        var lines = new List<string>();
        RunThemed(async () =>
        {
            using var silent = new TcpListener(IPAddress.Loopback, 0);
            silent.Start();
            var port = ((IPEndPoint)silent.LocalEndpoint).Port;
            var asking = true;
            var tab = new ViewerServerTab(
                new ViewerDataService($"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=1"),
                new DarlingServer(11, "queued-refresh-host.invalid", "queued-refresh-alias", true, null))
            {
                /* Zero makes the first open write its line, which is when the test stops asking. */
                FirstOpenSlowThresholdMs = 0,
                SlowLoadSink = line =>
                {
                    asking = false;
                    lines.Add(line);
                },
            };
            var held = new List<TcpClient>();

            async Task AskAtEveryReadAsync()
            {
                while (true)
                {
                    try
                    {
                        held.Add(await silent.AcceptTcpClientAsync());
                    }
                    catch (Exception ex) when (ex is SocketException or ObjectDisposedException or InvalidOperationException)
                    {
                        return;
                    }

                    if (asking)
                    {
                        await tab.RefreshActiveInnerTabAsync();
                    }
                }
            }

            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                var first = tab.RefreshActiveInnerTabAsync();
                var asks = AskAtEveryReadAsync();
                await first;
                silent.Stop();
                await asks;
            }
            finally
            {
                window.Close();
                silent.Stop();
                foreach (var client in held)
                {
                    client.Dispose();
                }
            }
        });

        Assert.Contains(lines, l => l.Contains("server tab first open", StringComparison.Ordinal));
        var queued = Assert.Single(lines, l => !l.Contains("server tab first open", StringComparison.Ordinal));
        Assert.Contains("[queued-refresh-alias]", queued, StringComparison.Ordinal);
        Assert.Contains("waited for the refresh in flight ", queued, StringComparison.Ordinal);
    }

    /* The freshness readout and the permission-denied count feed the tab's header, not the sub-tab's load, so a slow one does not hold
       the load back. The store here never answers, so each read is out until its one-second timeout. The sub-tab is the Plan Viewer,
       whose load reads nothing and stamps at once: the stamp arrives while the freshness read is still out, so the readout still shows
       what it showed before the refresh. The read then gives up and blanks the readout, and the refresh returns only after that. The
       line the load writes names the reads that ended before it did, and neither of the two still out. */
    [Fact]
    public void ASlowFreshnessOrPermissionCountRead_DoesNotHoldBackTheSubTabsLoad()
    {
        const string before = "collected before this refresh";
        var lines = new List<string>();
        string? readoutAtTheStamp = null;
        string? readoutAfterTheRefresh = null;
        RunThemed(async () =>
        {
            using var silent = new TcpListener(IPAddress.Loopback, 0);
            silent.Start();
            var port = ((IPEndPoint)silent.LocalEndpoint).Port;
            var tab = new ViewerServerTab(
                new ViewerDataService($"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=1"),
                new DarlingServer(12, "beside-load-host.invalid", "beside-load-alias", true, null))
            {
                /* Zero makes the first open write its line when the sub-tab's load ends. */
                FirstOpenSlowThresholdMs = 0,
                SlowLoadSink = lines.Add,
            };
            var readout = (TextBlock)tab.FindName("ServerFreshnessText");
            ((TabControl)tab.FindName("InnerTabs")).SelectedIndex = ViewerServerTab.PlanViewerInnerTabIndex;
            readout.Text = before;
            tab.StatusChanged += status =>
            {
                if (status.Contains(": refreshed ", StringComparison.Ordinal))
                {
                    readoutAtTheStamp ??= readout.Text;
                }
            };
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                await tab.RefreshActiveInnerTabAsync();
                readoutAfterTheRefresh = readout.Text;
            }
            finally
            {
                window.Close();
                silent.Stop();
            }
        });

        Assert.Equal(before, readoutAtTheStamp);
        Assert.Equal(string.Empty, readoutAfterTheRefresh);
        var line = Assert.Single(lines);
        Assert.Contains("server clock read ", line, StringComparison.Ordinal);
        Assert.DoesNotContain("server freshness read", line, StringComparison.Ordinal);
        Assert.DoesNotContain("permission denied count read", line, StringComparison.Ordinal);
    }

    /* A read that runs beside a load is booked when it ends. One that ended while the load ran is on the load's line with its own time.
       One still out when the load ends is not on the line. */
    [Fact]
    public void AReadBesideTheLoad_IsOnItsSlowLoadLine_OnlyWhenItEndedBeforeTheLoadDid()
    {
        var timer = new ViewerLoadTimer(slowThresholdMs: 0);
        var stillOut = new TaskCompletionSource();
        Assert.Same(Task.CompletedTask, ViewerServerTab.Timed(timer, "server freshness read", Task.CompletedTask));
        Assert.Same(stillOut.Task, ViewerServerTab.Timed(timer, "permission denied count read", stillOut.Task));
        var line = timer.Finish("Overview");
        stillOut.SetResult();

        Assert.NotNull(line);
        Assert.Contains("server freshness read ", line, StringComparison.Ordinal);
        Assert.DoesNotContain("permission denied count read", line, StringComparison.Ordinal);
    }

    [Fact]
    public void AClockStartedAfterTheTabsConstruction_CountsTheConstructionInItsTotal()
    {
        var timer = new ViewerLoadTimer(slowThresholdMs: 3000, alreadyElapsedMs: 3500);
        timer.BookStep("tab construction", 3500);
        timer.Lead = "server tab first open > ";
        var line = timer.Finish("Overview");
        Assert.NotNull(line);
        Assert.StartsWith("slow load: server tab first open > Overview took 35", line, StringComparison.Ordinal);
        Assert.Contains("[client steps: tab construction 3500 ms]", line, StringComparison.Ordinal);
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

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
}

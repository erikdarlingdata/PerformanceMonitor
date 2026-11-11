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
        var booked = StaTestThread.Run(async () =>
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
                var tab = new ViewerServerTab(
                    new ViewerDataService($"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=5"),
                    new DarlingServer(1, "slow-load-phases", "slow-load-phases", true, null));
                var window = new Window { Content = tab, Width = 1600, Height = 900 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var inner = (TabControl)tab.FindName("InnerTabs");
                    var index = inner.Items.OfType<TabItem>().ToList().FindIndex(t => Equals(t.Header, header));
                    Assert.True(index >= 0, header);
                    var timer = new ViewerLoadTimer();
                    await tab.LoadInnerTabAsync(index, timer);

                    /* A phase is booked by a watcher that finishes just after the read's own awaiter, so wait for the last one. */
                    for (var wait = 0; wait < 100 && timer.PhaseCount < expected.Length; wait++)
                    {
                        await Task.Delay(50);
                    }

                    return timer.PhaseNames.ToList();
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
        });

        foreach (var phase in expected)
        {
            Assert.Contains(phase, booked);
        }
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

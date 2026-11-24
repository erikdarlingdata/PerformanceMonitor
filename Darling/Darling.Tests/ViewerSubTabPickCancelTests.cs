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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9194: picking a sub-tab while another sub-tab's refresh ran queued the picked tab behind that whole refresh, behind an empty tab
/// (#8549 made the queue, so no two loads overlap). The refresh of the sub-tab being left is now cancelled, so the picked tab's read
/// starts at once. The held load here is the test's own hook, which stands in for a slow Performance Trends read, and the store is a
/// port nothing listens on, so the loads after the hold fail fast.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerSubTabPickCancelTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /* The hold would last forever without the cancel, so the picked tab's start inside the bound is the cancel's doing. */
    [Fact]
    public void PickingASubTab_CancelsTheLeftLoad_AndStartsThePickedLoadAtOnce_WithNoLineAndNoError()
    {
        var lines = new List<string>();
        var statuses = new List<string>();
        var pickedLoads = 0;
        var heldCancelled = false;
        var errorsBeforePickedStart = false;
        var pickedStartedInBound = false;
        var firstDone = false;
        RunThemed(async () =>
        {
            var heldStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var pickedStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tab = NewTab(13, "pick-cancel-alias", lines, statuses);
            tab.LoadStartHook = async (index, token) =>
            {
                if (index == ViewerServerTab.QueriesInnerTabIndex)
                {
                    heldStarted.TrySetResult();
                    try
                    {
                        await Task.Delay(Timeout.Infinite, token);
                    }
                    catch (OperationCanceledException)
                    {
                        heldCancelled = true;
                        throw;
                    }
                }
                else if (index == ViewerServerTab.FileIoInnerTabIndex)
                {
                    pickedLoads++;
                    pickedStartedInBound = true;
                    errorsBeforePickedStart |= pickedLoads == 1 && statuses.Any(s => s.Contains("did not load", StringComparison.Ordinal));
                    pickedStarted.TrySetResult();
                }
            };
            var inner = (TabControl)tab.FindName("InnerTabs");
            inner.SelectedIndex = ViewerServerTab.QueriesInnerTabIndex;
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                var first = tab.RefreshActiveInnerTabAsync();
                Assert.Same(heldStarted.Task, await Task.WhenAny(heldStarted.Task, Task.Delay(Bound)));

                inner.SelectedIndex = ViewerServerTab.FileIoInnerTabIndex;
                await Task.WhenAny(pickedStarted.Task, Task.Delay(Bound));
                await Task.WhenAny(first, Task.Delay(Bound));
                firstDone = first.IsCompleted;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(pickedStartedInBound, "the picked load never started inside the bound");
        Assert.True(firstDone, "the refresh did not end after the pick");
        Assert.True(heldCancelled, "the left load's hold was not cancelled");
        Assert.False(errorsBeforePickedStart, "the cancelled load reported an error: " + string.Join(" / ", statuses));
        /* The picked tab's own load may be slow against this store and say so. The left load and the first open it was on write nothing. */
        Assert.DoesNotContain(lines, l => l.Contains("first open", StringComparison.Ordinal) || l.Contains("Queries", StringComparison.Ordinal));

        Assert.True(pickedLoads >= 1);
    }

    /* A load that runs without a token is not cut short, so the picked tab waits behind it with no overlap, and the status line names
       the refresh it waits for. */
    [Fact]
    public void PickingASubTab_WhileAnUncancellableLoadRuns_NamesTheWait_AndDoesNotOverlap()
    {
        var statuses = new List<string>();
        var lines = new List<string>();
        var pickedStartedWhileHeld = false;
        var pickedStartedAfterRelease = false;
        RunThemed(async () =>
        {
            var heldStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var pickedStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tab = NewTab(14, "pick-wait-alias", lines, statuses);
            tab.LoadStartHook = async (index, token) =>
            {
                if (index == ViewerServerTab.QueriesInnerTabIndex)
                {
                    heldStarted.TrySetResult();
                    await release.Task;
                }
                else if (index == ViewerServerTab.FileIoInnerTabIndex)
                {
                    pickedStarted.TrySetResult();
                }
            };
            var inner = (TabControl)tab.FindName("InnerTabs");
            inner.SelectedIndex = ViewerServerTab.QueriesInnerTabIndex;
            var window = new Window { Content = tab, Width = 1600, Height = 900 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.BeginFirstOpen();
                var first = tab.RefreshActiveInnerTabAsync();
                Assert.Same(heldStarted.Task, await Task.WhenAny(heldStarted.Task, Task.Delay(Bound)));

                inner.SelectedIndex = ViewerServerTab.FileIoInnerTabIndex;
                await Task.Delay(TimeSpan.FromSeconds(1));
                pickedStartedWhileHeld = pickedStarted.Task.IsCompleted;
                release.SetResult();
                await Task.WhenAny(pickedStarted.Task, Task.Delay(Bound));
                pickedStartedAfterRelease = pickedStarted.Task.IsCompleted;
                await Task.WhenAny(first, Task.Delay(Bound));
            }
            finally
            {
                window.Close();
            }
        });

        Assert.Contains(statuses, s => s.Contains("waiting for the Queries refresh to finish", StringComparison.Ordinal));
        Assert.False(pickedStartedWhileHeld, "the picked load started while the load in flight still ran");
        Assert.True(pickedStartedAfterRelease, "the picked load never started");
    }

    /* The cancel reaches a store read the tab's loaders start without a token. The store here accepts the connection, finishes the startup
       and then never answers the query, and the connection's command timeout is thirty seconds away, so the read ends inside the bound only
       because the scope's token cancelled it. */
    [Fact]
    public async Task AStoreRead_StartedInAScope_EndsWhenTheScopeIsCancelled()
    {
        using var silent = new TcpListener(IPAddress.Loopback, 0);
        silent.Start();
        var port = ((IPEndPoint)silent.LocalEndpoint).Port;
        var serving = ServeStartupThenSilenceAsync(silent);
        await using var service = new ViewerDataService(
            $"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=30;Server Compatibility Mode=NoTypeLoading;SSL Mode=Disable");
        using var cts = new CancellationTokenSource();
        ViewerReadScope.Enter(cts.Token);
        try
        {
            var now = DateTime.UtcNow;
            var read = service.GetFileIoLatencyTrendAsync(1, now.AddHours(-1), now);
            cts.CancelAfter(TimeSpan.FromMilliseconds(500));
            var done = await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.Same(read, done);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);
        }
        finally
        {
            ViewerReadScope.Clear();
            silent.Stop();
            await Task.WhenAny(serving, Task.Delay(TimeSpan.FromSeconds(2)));
        }
    }

    /// <summary>A store that answers the startup and then reads every message without replying, so a query is out for as long as the
    /// connection lives.</summary>
    private static async Task ServeStartupThenSilenceAsync(TcpListener listener)
    {
        var clients = new List<Task>();
        while (true)
        {
            TcpClient client;
            try
            {
                client = await listener.AcceptTcpClientAsync();
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException or InvalidOperationException)
            {
                break;
            }

            clients.Add(Task.Run(async () =>
            {
                using var held = client;
                try
                {
                    var stream = held.GetStream();
                    var head = new byte[8];
                    await stream.ReadExactlyAsync(head);
                    var code = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(head, 4));
                    if (code == 80877102)
                    {
                        return;
                    }

                    var length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(head, 0));
                    await stream.ReadExactlyAsync(new byte[length - 8]);
                    static byte[] Message(char type, params byte[][] parts)
                    {
                        var body = parts.SelectMany(p => p).ToArray();
                        return new[] { (byte)type }.Concat(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(body.Length + 4))).Concat(body).ToArray();
                    }

                    static byte[] Text(string text) => System.Text.Encoding.UTF8.GetBytes(text + "\0");
                    static byte[] Int(int value) => BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));
                    await stream.WriteAsync(Message('R', Int(0)));
                    foreach (var (name, value) in new[] { ("server_version", "16.0"), ("client_encoding", "UTF8"), ("integer_datetimes", "on"), ("standard_conforming_strings", "on") })
                    {
                        await stream.WriteAsync(Message('S', Text(name), Text(value)));
                    }

                    await stream.WriteAsync(Message('K', Int(1), Int(2)));
                    await stream.WriteAsync(Message('Z', new[] { (byte)'I' }));
                    var sink = new byte[4096];
                    while (await stream.ReadAsync(sink) > 0)
                    {
                    }
                }
                catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
                {
                    /* The reader gave up on the connection: the end of the read this serves. */
                }
            }));
        }

        await Task.WhenAll(clients);
    }

    private static ViewerServerTab NewTab(int serverId, string alias, List<string> lines, List<string> statuses)
    {
        /* A port nothing listens on: the reads around the held load fail at once. */
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        var tab = new ViewerServerTab(
            new ViewerDataService($"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=1"),
            new DarlingServer(serverId, alias + "-host.invalid", alias, true, null))
        {
            /* Zero makes a first open that ends normally write its line, so a cancelled one that wrote none shows. */
            FirstOpenSlowThresholdMs = 0,
            SlowLoadSink = lines.Add,
        };
        tab.StatusChanged += statuses.Add;
        return tab;
    }

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

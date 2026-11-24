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
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8810: an auto-refresh tick that lands while a server tab's load is still running used to queue a second full load, which started the
/// moment the first ended, so a slow store answered the same reads twice in a row. The tick is a poll now: while a load runs it starts
/// nothing and queues nothing. A user trigger during a load (a second refresh here, standing for a range pick or a sub-tab switch) still
/// queues its reload. The store is a listener that closes every connection at once but holds the last one a load makes until the test
/// releases it, so the load is in its own pass while it waits, and what separates one load from two is how many connections reach the store.
/// A trigger that lands before the load's own pass is served by that pass, which is why the held read is the last one.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerAutoRefreshTickDuringLoadTests
{
    [Fact]
    public void ATickDuringALoad_IsDropped_SoExactlyOneLoadRuns()
    {
        var oneLoad = ConnectionsForOneLoad();
        var withTick = ConnectionsAfter(oneLoad, ScenarioStep.Tick);

        Assert.True(oneLoad > 0, "the load never reached the store");
        Assert.Equal(oneLoad, withTick);
    }

    [Fact]
    public void AUserRefreshDuringALoad_StillQueuesItsReload()
    {
        var oneLoad = ConnectionsForOneLoad();
        var withUserRefresh = ConnectionsAfter(oneLoad, ScenarioStep.UserRefresh);

        Assert.True(withUserRefresh > oneLoad, $"the second refresh was not served: {withUserRefresh} connections, {oneLoad} for one load");
    }

    private enum ScenarioStep
    {
        Tick,
        UserRefresh,
    }

    /// <summary>How many connections one load makes when the store closes every one at once.</summary>
    private static int ConnectionsForOneLoad()
    {
        var connections = 0;
        RunThemed(async () =>
        {
            using var store = new HeldStore(int.MaxValue);
            var tab = NewTab(store);
            var window = Show(tab);
            try
            {
                await tab.RefreshActiveInnerTabAsync();
                connections = store.Connections;
            }
            finally
            {
                window.Close();
            }
        });
        return connections;
    }

    /// <summary>Starts a load against a store that holds the last connection a load makes (number <paramref name="parkAt"/>), does
    /// <paramref name="during"/> while it is held, lets it go, waits for everything queued to finish, and returns how many connections
    /// reached the store.</summary>
    private static int ConnectionsAfter(int parkAt, ScenarioStep during)
    {
        var connections = 0;
        RunThemed(async () =>
        {
            using var store = new HeldStore(parkAt);
            var tab = NewTab(store);
            var window = Show(tab);
            try
            {
                var load = tab.RefreshActiveInnerTabAsync();
                Assert.True(await Task.Run(() => store.WaitForParked(TimeSpan.FromSeconds(60))), "the load never reached its last read");
                Assert.False(load.IsCompleted, "the load finished before the test could touch it");

                var second = during == ScenarioStep.Tick ? tab.AutoRefreshTickAsync() : tab.RefreshActiveInnerTabAsync();
                Assert.True(second.IsCompleted, "the second trigger did not return at once while the load ran");

                store.Release();
                await load;
                await second;
                connections = store.Connections;
            }
            finally
            {
                window.Close();
            }
        });
        return connections;
    }

    private static ViewerServerTab NewTab(HeldStore store) => new(
        new ViewerDataService(store.ConnectionString),
        new DarlingServer(8810, "tick-during-load", "tick-during-load", true, null));

    private static Window Show(ViewerServerTab tab)
    {
        var window = new Window { Content = tab, Width = 1600, Height = 900 };
        StaTestThread.ShowOffScreen(window);
        return window;
    }

    /// <summary>A loopback store that closes every connection at once except the one numbered <c>parkAt</c>, which it holds until
    /// <see cref="Release"/>. It counts them all.</summary>
    private sealed class HeldStore : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly int _parkAt;
        private readonly List<Socket> _parked = new();
        private readonly ManualResetEventSlim _hasParked = new();
        private readonly CancellationTokenSource _stopping = new();
        private int _connections;

        public HeldStore(int parkAt)
        {
            _parkAt = parkAt;
            _listener.Start();
            var port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            ConnectionString = $"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=120;Pooling=false";
            _ = AcceptAsync();
        }

        public string ConnectionString { get; }

        public int Connections => Volatile.Read(ref _connections);

        public bool WaitForParked(TimeSpan limit) => _hasParked.Wait(limit);

        public void Release()
        {
            lock (_parked)
            {
                foreach (var socket in _parked)
                {
                    socket.Close();
                }

                _parked.Clear();
            }
        }

        public void Dispose()
        {
            _stopping.Cancel();
            Release();
            _listener.Stop();
            _hasParked.Dispose();
            _stopping.Dispose();
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (!_stopping.IsCancellationRequested)
                {
                    var socket = await _listener.AcceptSocketAsync(_stopping.Token);
                    if (Interlocked.Increment(ref _connections) == _parkAt)
                    {
                        lock (_parked)
                        {
                            _parked.Add(socket);
                        }

                        _hasParked.Set();
                    }
                    else
                    {
                        socket.Close();
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (SocketException)
            {
            }
        }
    }

    /// <summary>Runs <paramref name="body"/> on the STA thread with the Viewer theme resources in the application, as the tab cell styles need them.</summary>
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

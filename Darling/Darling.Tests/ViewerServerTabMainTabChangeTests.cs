/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8506: the server tab's main-tab <c>SelectionChanged</c> handler is <c>async void</c>: through the active-tab refresh it awaits the server
/// clock read and then redraws the time range picker. The code after that await must run on the tab's own thread whatever synchronization
/// context the thread that selected the tab had. The app's UI thread has the dispatcher's. A host that drives the tab on its dispatcher thread
/// with no context, which is what a synchronous test body is, would otherwise send the continuation to the thread pool, where the redraw
/// touches the picker from the wrong thread and the exception escapes the handler as an unhandled thread error that ends the test host.
/// The store is a listener that accepts the connection and never answers, so the clock read is parked at its first await until the test lets
/// it go, and the thread the continuation then runs on is the thing under test: no timing decides it.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerServerTabMainTabChangeTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    [Fact]
    public void SelectingAMainTab_WithNoDispatcherContext_RedrawsThePickerOnTheTabsThread()
    {
        using var store = new HeldStore();
        var context = new PoolContext();
        StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = NewTab(store);
            var window = Host(tab);
            try
            {
                var inner = (TabControl)tab.FindName("InnerTabs");
                var target = inner.Items.OfType<TabItem>().First(t => Equals(t.Header, "Wait Stats"));
                Assert.NotSame(target, inner.SelectedItem);

                /* The selection runs under a context that behaves as none does: what is posted to it runs on the thread pool. It is there only
                   to keep what the handler throws off the tab's thread, which with no context at all would end the process, and to say when
                   the handler has finished. */
                StaTestThread.Step("selecting the Wait Stats tab with no dispatcher context");
                var prior = SynchronizationContext.Current;
                SynchronizationContext.SetSynchronizationContext(context);
                try
                {
                    inner.SelectedItem = target;
                }
                finally
                {
                    SynchronizationContext.SetSynchronizationContext(prior);
                }

                Assert.Same(target, inner.SelectedItem);
                Assert.True(context.Outstanding > 0, "the main-tab handler did not reach its first await: the tab was not loaded, or its refresh was suppressed");
                Assert.True(store.WaitForConnection(Limit), "the handler never opened the store connection for the clock read");

                /* Let the clock read fail. The handler goes on to redraw the picker, on the thread its await brought it back to. */
                StaTestThread.Step("releasing the held clock read");
                store.Release();
                PumpUntil(tab.Dispatcher, () => context.Outstanding == 0 && context.Pending == 0, Limit, "the main-tab handler to finish");
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(
            context.Thrown.IsEmpty,
            "the main-tab SelectionChanged handler touched the tab off its thread after its first await:" + Environment.NewLine
            + string.Join(Environment.NewLine, context.Thrown.Select(e => e.ToString())));
    }

    [Fact]
    public void TheActiveTabRefresh_CalledWithNoSynchronizationContext_CompletesOnTheTabsThread()
    {
        using var store = new HeldStore();
        var fault = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = NewTab(store);
            var window = Host(tab);
            try
            {
                /* A synchronous body runs with no context, as the rendered classes' bodies do. */
                Assert.Null(SynchronizationContext.Current);
                StaTestThread.Step("starting the active-tab refresh with no synchronization context");
                var refresh = tab.RefreshActiveInnerTabAsync();
                Assert.False(refresh.IsCompleted, "the refresh completed before its clock read: nothing was held");
                Assert.True(store.WaitForConnection(Limit), "the refresh never opened the store connection for the clock read");

                StaTestThread.Step("releasing the held clock read");
                store.Release();
                PumpUntil(tab.Dispatcher, () => refresh.IsCompleted, Limit, "the active-tab refresh to complete");
                return refresh.Exception?.GetBaseException().ToString();
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(fault is null, "the active-tab refresh touched the tab off its thread after its first await:" + Environment.NewLine + fault);
    }

    private static ViewerServerTab NewTab(HeldStore store) => new(
        new ViewerDataService(store.ConnectionString),
        new DarlingServer(1, "main-tab-change", "main-tab-change", true, null));

    /// <summary>Shows <paramref name="tab"/> in a window the way the rendered classes do, pumped until the tab is loaded (the handler ignores a selection before that).</summary>
    private static Window Host(ViewerServerTab tab)
    {
        var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = 1280, Height = 800 };
        StaTestThread.ShowOffScreen(window);
        StaTestThread.Settle(window);
        Assert.True(tab.IsLoaded, "the server tab was not loaded after the window settled");
        return window;
    }

    /// <summary>
    /// Runs a dispatcher frame until <paramref name="done"/> holds, so a continuation posted to the tab's dispatcher can run meanwhile, for at
    /// most <paramref name="limit"/> (cut to what the body's hang guard has left). The condition is read from a background-priority timer, after
    /// whatever the dispatcher had queued ahead of it; a send-priority timer ends the frame when the limit passes, as the shared idle wait does.
    /// </summary>
    private static void PumpUntil(Dispatcher dispatcher, Func<bool> done, TimeSpan limit, string what)
    {
        limit = StaHost.WithinGuard(limit, out _);
        var frame = new DispatcherFrame();
        var satisfied = false;
        var poll = new DispatcherTimer(DispatcherPriority.Background, dispatcher) { Interval = TimeSpan.FromMilliseconds(25) };
        poll.Tick += (_, _) =>
        {
            satisfied = done();
            if (satisfied)
            {
                frame.Continue = false;
            }
        };
        var expiry = new DispatcherTimer(DispatcherPriority.Send, dispatcher) { Interval = limit };
        expiry.Tick += (_, _) => frame.Continue = false;
        poll.Start();
        expiry.Start();
        try
        {
            Dispatcher.PushFrame(frame);
        }
        finally
        {
            expiry.Stop();
            poll.Stop();
        }

        Assert.True(satisfied, $"waited {limit.TotalMilliseconds:0} ms for {what}");
    }

    /// <summary>Runs <paramref name="body"/> with the Viewer's dark theme merged into the application resources, as the rendered classes do, and removes it after.</summary>
    private static T WithTheme<T>(Func<T> body)
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
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }

    private static void WithTheme(Action body) => WithTheme(() =>
    {
        body();
        return 0;
    });

    /// <summary>
    /// A store that holds every read: a loopback listener that accepts each connection and never answers, so a read parks at its first await
    /// until <see cref="Release"/> closes what it accepted and stops listening, after which the read fails at once and every later connection
    /// is refused at once.
    /// </summary>
    private sealed class HeldStore : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly List<Socket> _accepted = new();
        private readonly ManualResetEventSlim _connected = new();
        private readonly CancellationTokenSource _stopping = new();

        public HeldStore()
        {
            _listener.Start();
            var port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            ConnectionString = $"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=120";
            _ = AcceptAsync();
        }

        public string ConnectionString { get; }

        public bool WaitForConnection(TimeSpan limit) => _connected.Wait(limit);

        public void Release()
        {
            _stopping.Cancel();
            lock (_accepted)
            {
                foreach (var socket in _accepted)
                {
                    socket.Close();
                }

                _accepted.Clear();
            }

            _listener.Stop();
        }

        public void Dispose()
        {
            Release();
            _connected.Dispose();
            _stopping.Dispose();
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (!_stopping.IsCancellationRequested)
                {
                    var socket = await _listener.AcceptSocketAsync(_stopping.Token);
                    lock (_accepted)
                    {
                        _accepted.Add(socket);
                    }

                    _connected.Set();
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

    /// <summary>
    /// A synchronization context that behaves as none does: what is posted to it runs on the thread pool. It counts the <c>async void</c>
    /// methods started under it and the posts still to run, so a test can tell when a handler has finished, and keeps what a posted callback
    /// throws, which is where an <c>async void</c> method's exception goes: with no context it would be rethrown on the pool and end the process.
    /// </summary>
    private sealed class PoolContext : SynchronizationContext
    {
        private int _outstanding;
        private int _pending;

        public ConcurrentQueue<Exception> Thrown { get; } = new();

        public int Outstanding => Volatile.Read(ref _outstanding);

        public int Pending => Volatile.Read(ref _pending);

        public override void OperationStarted() => Interlocked.Increment(ref _outstanding);

        public override void OperationCompleted() => Interlocked.Decrement(ref _outstanding);

        public override void Post(SendOrPostCallback d, object? state)
        {
            Interlocked.Increment(ref _pending);
            ThreadPool.QueueUserWorkItem(_ =>
            {
                try
                {
                    d(state);
                }
                catch (Exception ex)
                {
                    Thrown.Enqueue(ex);
                }
                finally
                {
                    Interlocked.Decrement(ref _pending);
                }
            });
        }

        public override SynchronizationContext CreateCopy() => this;
    }
}

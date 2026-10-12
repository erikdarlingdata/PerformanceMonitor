/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;
using Xunit;

/// <summary>
/// #5602: every test that needs WPF runs its body on ONE shared STA thread per test process, instead of starting a thread of its
/// own. WPF keeps process-wide state that is not thread-safe (#5559: the <c>DependencyPropertyDescriptor</c> cache and its
/// unsynchronised <c>AddValueChanged</c> dictionary) and per-thread state that is expensive to leave behind (#5596: a weak-event
/// table and a dispatcher per thread, 300 ms each at process exit). Fifty test files each starting their own thread meant both.
/// <para>
/// <b>One body at a time, end to end.</b> A body that pumps (<c>Dispatcher.PushFrame</c>, a modal loop) lets a second queued
/// <c>Invoke</c> run inside it, which is #5559 again. So the CALLER takes a process-wide gate before it queues its body and
/// releases it after the body and its drain have returned; the dispatcher never has two bodies queued. The wait for the gate is
/// not charged to the hang guard.
/// </para>
/// <para>
/// <b>Drain.</b> After each body the thread closes any window the body left open and runs the dispatcher to application-idle,
/// so work one test queued does not run during the next. An exception thrown by queued work (a timer tick, a Loaded handler)
/// does not end the thread and the process: it is handed to the test whose body or drain ran it.
/// </para>
/// <para>
/// <b>Hang guard.</b> A body that does not return within <see cref="HangGuard"/> fails the calling test, naming it. The shared
/// thread is then stuck inside that body and cannot be freed, so every later WPF test in the process fails at once with a
/// message naming the stuck test, instead of each waiting out its own 60 s.
/// </para>
/// <para>
/// Tests that must own a thread (a dispatcher shutdown, a thread that has to END while the test looks at the process) stay on
/// their own thread and take <see cref="EnterExclusive"/> around it, so they cannot overlap a body running here. The allow list
/// is in <c>StaThreadCensusTests</c>, with a reason per file.
/// </para>
/// This file lives in Darling.Tests and is compiled into Lite.Tests by a link (one host, two suites). It is in the global
/// namespace so the Lite classes, which sit in a different namespace, need no extra <c>using</c>.
/// </summary>
internal static class StaTestThread
{
    private static readonly StaHost s_host = new();

    /// <summary>How long a body (and its drain) may take before the calling test fails. Settable so a test can shorten it.</summary>
    internal static TimeSpan HangGuard { get => s_host.HangGuard; set => s_host.HangGuard = value; }

    /// <summary>Runs <paramref name="body"/> on the shared STA thread and returns when it and its drain are done. Its exception comes back here with its own stack.</summary>
    internal static void Run(Action body) => s_host.Run(body);

    /// <summary>Runs <paramref name="body"/> on the shared STA thread and returns its result.</summary>
    internal static T Run<T>(Func<T> body) => s_host.Run(body);

    /// <summary>Runs an async <paramref name="body"/> on the shared STA thread: its continuations come back to that thread's dispatcher.</summary>
    internal static void Run(Func<Task> body) => s_host.Run(body);

    /// <summary>
    /// For a test that must own its thread: waits for the gate and returns the token that releases it. Not re-entrant, and not to
    /// be called from a body already running in <see cref="Run(Action)"/>.
    /// </summary>
    internal static IDisposable EnterExclusive() => s_host.EnterExclusive();
}

/// <summary>
/// The machinery behind <see cref="StaTestThread"/>: one lazily started background STA thread running a dispatcher, and the gate.
/// The suites use the single instance behind <see cref="StaTestThread"/>; the host's own tests make their own, so the hang guard
/// can be proven without leaving the shared thread stuck for the rest of the process.
/// </summary>
internal sealed class StaHost
{
    internal TimeSpan HangGuard { get; set; } = TimeSpan.FromSeconds(60);

    /* A SemaphoreSlim, not a Monitor: a test may be async, so the releasing thread is not promised to be the taking one. */
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly object _startLock = new();
    private readonly List<Exception> _strays = new();
    private Dispatcher? _dispatcher;
    private volatile string? _stuckTest;

    internal string? StuckTest => _stuckTest;

    /// <summary>The dispatcher of the host thread (starting the thread if it is not yet). For the host's own tests.</summary>
    internal Dispatcher HostDispatcher => EnsureThread();

    internal void Run(Action body) => RunCore(() => { WithoutSyncContext(body); return Task.CompletedTask; });

    internal T Run<T>(Func<T> body)
    {
        T result = default!;
        RunCore(() => { WithoutSyncContext(() => result = body()); return Task.CompletedTask; });
        return result;
    }

    /// <summary>
    /// A synchronous body runs with no synchronization context, as it did on a thread of its own that never ran a dispatcher. Inside the
    /// dispatcher's own callback the current context is the dispatcher's, and a body that blocks on a task (<c>.GetAwaiter().GetResult()</c>)
    /// whose awaits capture it would wait for the very thread it is blocking. A frame the body pushes installs its own context for the
    /// frame's life and restores this one, so a pumping body is unaffected.
    /// </summary>
    private static void WithoutSyncContext(Action body)
    {
        var prior = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            body();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(prior);
        }
    }

    internal void Run(Func<Task> body) => RunCore(body);

    internal IDisposable EnterExclusive()
    {
        _gate.Wait();
        return new Releaser(_gate);
    }

    /// <summary>Ends the host thread's dispatcher. Only for a host a test made for itself.</summary>
    internal void Shutdown() => _dispatcher?.BeginInvokeShutdown(DispatcherPriority.Send);

    private void RunCore(Func<Task> body)
    {
        FailIfStuck();
        _gate.Wait();
        try
        {
            FailIfStuck();
            var dispatcher = EnsureThread();
            ExceptionDispatchInfo? error = null;
            lock (_strays)
            {
                _strays.Clear();
            }

            var finished = dispatcher.InvokeAsync(() => Job(body, e => error = e)).Task.Unwrap();
            if (!finished.Wait(HangGuard))
            {
                var name = CurrentTestName();
                _stuckTest = name;
                throw new TimeoutException(
                    $"WPF test body did not return within {HangGuard.TotalSeconds:0} s: {name}. The shared STA test thread is stuck inside it, "
                    + "so every later WPF test in this process fails at once naming this test (#5602).");
            }

            error?.Throw();
            lock (_strays)
            {
                if (_strays.Count > 0)
                {
                    var strays = _strays.ToArray();
                    _strays.Clear();
                    throw new AggregateException("Work the test queued on the dispatcher threw while the shared STA thread ran it (#5602):", strays);
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>The body, then the cleanup. Never throws: the body's exception is handed back for the caller to rethrow with its stack.</summary>
    private async Task Job(Func<Task> body, Action<ExceptionDispatchInfo> fail)
    {
        try
        {
            await body();
        }
        catch (Exception ex)
        {
            fail(ExceptionDispatchInfo.Capture(ex));
        }

        try
        {
            CloseLeftoverWindows();
            await Dispatcher.Yield(DispatcherPriority.ApplicationIdle);
        }
        catch (Exception ex)
        {
            fail(ExceptionDispatchInfo.Capture(ex));
        }
    }

    /// <summary>A test that failed before its own <c>Close()</c> leaves a window (and its HWND) behind; the next test must not meet it.</summary>
    private static void CloseLeftoverWindows()
    {
        foreach (PresentationSource source in PresentationSource.CurrentSources)
        {
            if (source.RootVisual is Window window && window.Dispatcher == Dispatcher.CurrentDispatcher)
            {
                window.Close();
            }
        }
    }

    private void FailIfStuck()
    {
        var stuck = _stuckTest;
        if (stuck is not null)
        {
            throw new InvalidOperationException(
                $"The shared WPF STA test thread is stuck inside a body that never returned: {stuck}. This test did not run (#5602).");
        }
    }

    private static string CurrentTestName() => TestContext.Current.Test?.TestDisplayName ?? "(test name unavailable)";

    private Dispatcher EnsureThread()
    {
        var existing = _dispatcher;
        if (existing is not null)
        {
            return existing;
        }

        lock (_startLock)
        {
            if (_dispatcher is not null)
            {
                return _dispatcher;
            }

            using var ready = new ManualResetEventSlim();
            Dispatcher? made = null;
            var thread = new Thread(() =>
            {
                var dispatcher = Dispatcher.CurrentDispatcher;
                dispatcher.UnhandledException += (_, e) =>
                {
                    lock (_strays)
                    {
                        _strays.Add(e.Exception);
                    }

                    e.Handled = true;
                };
                made = dispatcher;
                ready.Set();
                Dispatcher.Run();
            })
            {
                Name = "StaTestThread",
                /* Background: a stuck body must not keep the test process alive after the run returns. */
                IsBackground = true,
            };
            thread.SetApartmentState(ApartmentState.STA);
            thread.Start();
            ready.Wait();
            _dispatcher = made;
            return made!;
        }
    }

    private sealed class Releaser(SemaphoreSlim gate) : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                gate.Release();
            }
        }
    }
}

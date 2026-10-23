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
/// <b>Where it stuck (#6216).</b> .NET Core cannot capture another thread's stack, so the timeout message carries what can be read from
/// outside: the last step the body named with <see cref="Step"/>, how many dispatcher operations ran (a loop that never settles starts
/// them without end; one that never finishes leaves one running; a pump waiting on a frame starts none), and the thread's state and CPU
/// over a quarter-second sample.
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
    /// #6216: names the step the running body has reached ("tab 3, Alert rules"). Costs one field write. When the hang guard fires, its
    /// message says which step the stuck body last named and how long ago, because a thread stuck in a native call has no stack to read.
    /// </summary>
    internal static void Step(string what) => s_host.Step(what);

    /// <summary>How long <see cref="Settle"/> waits for application idle. Half the hang guard, so a settle that never ends fails its own test before the guard strands the shared thread.</summary>
    internal static readonly TimeSpan IdleLimit = TimeSpan.FromSeconds(30);

    /// <summary>
    /// #6333: shows the window and lets the dispatcher reach application idle, with the wait bounded. Replaces the unbounded
    /// <c>PushFrame</c> each WPF test class copied: a frame that is never woken left the body, and with it the shared thread, waiting
    /// until the 60 s hang guard stranded every later WPF test in the process (a Lite shard lost a whole class that way).
    /// </summary>
    internal static void Settle(Window window)
    {
        Step("Settle: Show() on the window");
        window.Show();
        WaitForIdle(window.Dispatcher, IdleLimit);
    }

    /// <summary>
    /// Runs a dispatcher frame until an application-idle operation runs, for at most <paramref name="limit"/>. When the limit passes
    /// the test fails alone, saying what it waited for and how many dispatcher operations ran meanwhile, and the shared thread stays usable.
    /// </summary>
    internal static void WaitForIdle(Dispatcher dispatcher, TimeSpan limit) => WaitForIdle(dispatcher, limit, DispatcherPriority.ApplicationIdle);

    /// <summary>
    /// <see cref="WaitForIdle(Dispatcher, TimeSpan)"/> at a chosen <paramref name="idlePriority"/>. An overload, not an optional
    /// parameter: an optional <see cref="DispatcherPriority"/> stores a constant of a WindowsBase type in this method's metadata, and
    /// the nightly test map's AltCover (Mono.Cecil) cannot resolve WindowsBase when it rewrites the assembly. It failed every test-map
    /// shard from nightly 569 on with "Failed to resolve System.Windows.Threading.DispatcherPriority".
    /// </summary>
    internal static void WaitForIdle(Dispatcher dispatcher, TimeSpan limit, DispatcherPriority idlePriority)
    {
        var frame = new DispatcherFrame();
        var idle = false;
        var operations = 0;
        var lastPriority = DispatcherPriority.Invalid;
        var priorToLast = DispatcherPriority.Invalid;
        DispatcherHookEventHandler counter = (_, e) =>
        {
            operations++;
            priorToLast = lastPriority;
            lastPriority = e.Operation.Priority;
        };
        dispatcher.Hooks.OperationStarted += counter;

        /* Send priority and promoted by the dispatcher on every pass, so it fires even when the queue is never empty. */
        var expiry = new DispatcherTimer(DispatcherPriority.Send, dispatcher) { Interval = limit };
        expiry.Tick += (_, _) =>
        {
            /* The tick is itself an operation the counter saw; the message counts the dispatcher's work, not the guard's. */
            operations--;
            lastPriority = priorToLast;
            frame.Continue = false;
        };
        try
        {
            dispatcher.BeginInvoke(idlePriority, new Action(() =>
            {
                idle = true;
                frame.Continue = false;
            }));
            expiry.Start();
            Step("Settle: waiting in a dispatcher frame for application idle");
            Dispatcher.PushFrame(frame);
        }
        finally
        {
            expiry.Stop();
            dispatcher.Hooks.OperationStarted -= counter;
        }

        if (!idle)
        {
            throw new TimeoutException(
                $"The dispatcher did not reach {(idlePriority == DispatcherPriority.ApplicationIdle ? "application idle" : idlePriority.ToString())} within {limit.TotalMilliseconds:0} ms: it ran {operations} operations meanwhile"
                + (operations == 0 ? " (none: the frame was not woken to run what the window queued)" : $", the last at priority {lastPriority}")
                + ". Work queued ahead of idle without end looks like the second; a pump that never wakes looks like the first (#6216, #6333).");
        }
    }

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

    /* #6632: how long after it started the body the guard gave up on did return, or 0 while it has not. A body that returns late was
       slow (a cold first WPF start under load); one that never returns is blocked for good. The later tests' refusals say which. */
    private long _stuckReturnedAfterMs;

    /* #6216: what the hang guard reports about a body that did not return. The host thread writes the dispatcher counters (the hooks run
       on it); the body writes the step; the caller reads them all once the guard expires. */
    private volatile string _step = "";
    private long _stepTick;
    private long _opsStarted;
    private long _opsCompleted;
    private long _lastStartedTick;
    private volatile DispatcherOperation? _lastStarted;
    private int _nativeThreadId;

    internal string? StuckTest => _stuckTest;

    /// <summary>Names the step the running body has reached, for the hang guard's message. See <see cref="StaTestThread.Step"/>.</summary>
    internal void Step(string what)
    {
        _step = what;
        Interlocked.Exchange(ref _stepTick, Environment.TickCount64);
    }

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

            var began = Environment.TickCount64;
            var opsAtStart = Interlocked.Read(ref _opsStarted);
            _step = "(the body named no step)";
            Interlocked.Exchange(ref _stepTick, began);
            var finished = dispatcher.InvokeAsync(() => Job(body, e => error = e)).Task.Unwrap();
            if (!finished.Wait(HangGuard))
            {
                var name = CurrentTestName();
                _stuckTest = name;
                _ = finished.ContinueWith(
                    _ => Interlocked.Exchange(ref _stuckReturnedAfterMs, Math.Max(1, Environment.TickCount64 - began)),
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                throw new TimeoutException(
                    $"WPF test body did not return within {HangGuard.TotalSeconds:0} s: {name}. The shared STA test thread is stuck inside it, "
                    + "so every later WPF test in this process fails at once naming this test (#5602). "
                    + DescribeStuckBody(opsAtStart));
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
            /* #6632: the drain runs in the body's own dispatcher operation, so without its own step a drain that stuck would be
               reported under the body's last step. */
            Step("the host's drain after the body: closing leftover windows, then waiting for application idle");
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
            var returnedAfter = Interlocked.Read(ref _stuckReturnedAfterMs);
            var since = returnedAfter > 0
                ? $" That body did return, {returnedAfter} ms after it started, so it was slow, not blocked for good (#6632)."
                : " That body has not returned yet (#6632).";
            throw new InvalidOperationException(
                $"The shared WPF STA test thread was stranded by a body that did not return within its hang guard: {stuck}. This test did not run (#5602)." + since);
        }
    }

    private static string CurrentTestName() => TestContext.Current.Test?.TestDisplayName ?? "(test name unavailable)";

    private static readonly TimeSpan s_sampleWindow = TimeSpan.FromMilliseconds(250);
    private static readonly System.Reflection.FieldInfo? s_operationMethod =
        typeof(DispatcherOperation).GetField("_method", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    private static extern uint GetCurrentThreadId();

    /// <summary>
    /// #6216: what another thread can read about a body that did not return. .NET Core cannot capture another thread's stack, and a
    /// thread stuck in a native call or a message loop has none to show, so this reports what is observable: the last step the body
    /// named, what the dispatcher has been doing (a run of operations that never ends, one operation that never finishes, or nothing at
    /// all), and whether the thread is burning CPU or waiting. It samples for a quarter second, so "burning" means it is still going now.
    /// </summary>
    private string DescribeStuckBody(long opsAtStart)
    {
        try
        {
            var step = _step;
            var stepAgo = Environment.TickCount64 - Interlocked.Read(ref _stepTick);
            var startedBefore = Interlocked.Read(ref _opsStarted);
            var cpuBefore = ReadThread(out _);
            Darling.Tests.TestWait.Elapse(s_sampleWindow);
            var startedAfter = Interlocked.Read(ref _opsStarted);
            var cpuAfter = ReadThread(out var threadState);
            var running = startedAfter - Interlocked.Read(ref _opsCompleted);
            var last = _lastStarted;
            var lastName = last is null
                ? "none"
                : $"priority {last.Priority}{(s_operationMethod?.GetValue(last) is Delegate d ? $", {d.Method.DeclaringType?.Name}.{d.Method.Name}" : "")}, "
                    + $"{Environment.TickCount64 - Interlocked.Read(ref _lastStartedTick)} ms ago";
            var cpu = cpuAfter - cpuBefore;
            var ran = startedAfter - startedBefore;
            var verdict = cpu.TotalMilliseconds >= s_sampleWindow.TotalMilliseconds / 2
                ? (ran > 0 ? "it is still running dispatcher work that does not end" : "it is spinning inside one call")
                : (ran > 0 ? "it is mostly waiting while the dispatcher still runs operations" : "it is waiting, and the dispatcher has nothing to run");
            return $"Where it was: the body's last step was \"{step}\", named {stepAgo} ms ago. "
                + $"The dispatcher started {startedBefore - opsAtStart} operations during the body and {ran} more in the next {s_sampleWindow.TotalMilliseconds:0} ms, "
                + $"{running} of them still running; the last it started was {lastName}. "
                + $"The thread is {threadState} and used {cpu.TotalMilliseconds:0} ms of CPU in those {s_sampleWindow.TotalMilliseconds:0} ms, so {verdict} (#6216).";
        }
        catch (Exception ex)
        {
            return $"(where the body was could not be read: {ex.GetType().Name}: {ex.Message}) (#6216)";
        }
    }

    /// <summary>The host thread's CPU time and state, read from the process's thread list by the native id taken when the thread started.</summary>
    private TimeSpan ReadThread(out string state)
    {
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        foreach (System.Diagnostics.ProcessThread thread in process.Threads)
        {
            if (thread.Id == _nativeThreadId)
            {
                state = thread.ThreadState == System.Diagnostics.ThreadState.Wait ? $"Wait ({thread.WaitReason})" : thread.ThreadState.ToString();
                return thread.TotalProcessorTime;
            }
        }

        state = "not in the process's thread list";
        return TimeSpan.Zero;
    }

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
                /* #6216: the hang guard's message says what the dispatcher was doing, so every operation it starts is counted here. */
                dispatcher.Hooks.OperationStarted += (_, e) =>
                {
                    Interlocked.Increment(ref _opsStarted);
                    Interlocked.Exchange(ref _lastStartedTick, Environment.TickCount64);
                    _lastStarted = e.Operation;
                };
                dispatcher.Hooks.OperationCompleted += (_, _) =>
                {
                    Interlocked.Increment(ref _opsCompleted);
                };
                _nativeThreadId = (int)GetCurrentThreadId();
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

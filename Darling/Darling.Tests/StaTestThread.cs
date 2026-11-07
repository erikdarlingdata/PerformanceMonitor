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
/// thread is then stuck inside that body and cannot be freed, so every later WPF test that asks while the body is still inside
/// it fails at once with a message naming the stuck test, instead of each waiting out its own 180 s. A body that does return
/// (it was slow, not blocked for good: #6632, and the five stalls seen under #7600 all returned within 126 s, so the guard is 180 s and not 60 s) frees the thread
/// again once its drain is done, and the tests after that run; only the slow body's own test fails (#7600).
/// </para>
/// <para>
/// <b>Render engine start-up (#7600).</b> The first WPF body of a process pays WPF's render engine start-up (5 to 12 s on a green CI leg, 61 to
/// 126 s on the stalled ones, the thread waiting in <c>wpfgfx_cor3!MilComposition_SyncFlush</c> for the render thread), and every later WPF test
/// is fast. So the thread shows and closes one window before its first body, under a guard of its own (<see cref="StaHost.StartUpGuard"/>) and
/// never under a test's hang guard, so a host whose test shortens <see cref="HangGuard"/> to 300 ms still measures only its body: a start-up that
/// overruns its guard is named as the start-up, not as whichever test came first, and later tests are refused until it returns. Its duration goes
/// to the leg's log.
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

    /// <summary>
    /// How long <see cref="Settle"/> waits for application idle at most. A sixth of the 180 s hang guard, so a settle that never ends fails its own test well before the guard
    /// strands the shared thread. #7968: the wait is also cut to what the body's hang guard has left (<see cref="StaHost.WithinGuard"/>), because a body that
    /// spent 34 s before it began waiting (a window's first composition flush on a loaded runner) could not otherwise reach this limit before the guard.
    /// </summary>
    internal static readonly TimeSpan IdleLimit = TimeSpan.FromSeconds(30);

    /// <summary>
    /// #6333: shows the window and lets the dispatcher reach application idle, with the wait bounded. Replaces the unbounded
    /// <c>PushFrame</c> each WPF test class copied: a frame that is never woken left the body, and with it the shared thread, waiting
    /// until the hang guard (60 s then, 180 s now) stranded every later WPF test in the process (a Lite shard lost a whole class that way).
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
        var asked = limit;
        limit = StaHost.WithinGuard(limit, out var cut);
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
                + ". Work queued ahead of idle without end looks like the second; a pump that never wakes looks like the first (#6216, #6333)."
                + (cut is null ? "" : $" The wait was cut from {asked.TotalMilliseconds:0} ms to {limit.TotalMilliseconds:0} ms: {cut} (#7968)."));
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
    internal TimeSpan HangGuard { get; set; } = TimeSpan.FromSeconds(180);

    /// <summary>
    /// #7600: how long the render engine start-up ahead of the first body may take before the calling test fails naming the start-up. Separate from
    /// <see cref="HangGuard"/>, which a host's own test shortens to prove the guard on a body: the start-up costs seconds on a cold runner, so a body
    /// guard of 300 ms must never have to cover it. Settable so a test can force the overrun.
    /// </summary>
    internal TimeSpan StartUpGuard { get; set; } = TimeSpan.FromSeconds(180);

    /// <summary>
    /// #7600: when given, the start-up does not return until this is set, after its window has shown and closed, so a host test can make the start-up
    /// overrun <see cref="StartUpGuard"/> for certain. The engine's own cost cannot be relied on for that: in a process where an earlier host has loaded
    /// it, the window shows and closes in 0 ms, inside even a 1 ms guard. Null for the suites' shared host.
    /// </summary>
    internal ManualResetEventSlim? StartUpHold { get; init; }

    /* A SemaphoreSlim, not a Monitor: a test may be async, so the releasing thread is not promised to be the taking one. */
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly object _startLock = new();
    private readonly List<Exception> _strays = new();
    private Dispatcher? _dispatcher;
    private volatile string? _stuckTest;

    /* #7600: the host thread's start, completing with its dispatcher once the render engine start-up queued ahead of every body has run. */
    private Task<Dispatcher>? _starting;
    private const string RenderEngineStartUp = "WPF's render engine start-up, before the first test body";

    /* #7600: the job the guard gave up on. It completes when the body AND the host's drain have returned, which is when the thread is
       free again, so FailIfStuck lifts the refusal then. Read and written under _stuckLock together with _stuckTest. */
    private Task? _stuckJob;
    private readonly object _stuckLock = new();

    /* #7968: when the running body's hang guard started (Environment.TickCount64), 0 while no body runs. A bounded wait inside the body
       reads it to stop before the guard does, whatever the body spent before the wait began. */
    private long _bodyBegan;

    /* The host whose body is running on this thread. The host thread runs one body at a time, and an async body resumes on it. */
    [ThreadStatic]
    private static StaHost? t_running;

    /* #6216: what the hang guard reports about a body that did not return. The host thread writes the dispatcher counters (the hooks run
       on it); the body writes the step; the caller reads them all once the guard expires. */
    private volatile string _step = "";
    private long _stepTick;
    private long _opsStarted;
    private long _opsCompleted;
    private long _lastStartedTick;
    private volatile DispatcherOperation? _lastStarted;
    private int _nativeThreadId;
    private volatile Thread? _thread;

    internal string? StuckTest => _stuckTest;

    /// <summary>Names the step the running body has reached, for the hang guard's message. See <see cref="StaTestThread.Step"/>.</summary>
    internal void Step(string what)
    {
        _step = what;
        Interlocked.Exchange(ref _stepTick, Environment.TickCount64);
    }

    /// <summary>
    /// #7968: a wait the body makes with its own limit (<see cref="StaTestThread.WaitForIdle(Dispatcher, TimeSpan)"/>) must end before the hang guard
    /// does, or the guard fires first and strands the shared thread instead of failing the one test. A body that spends its first half minute building
    /// a window then waits 30 s more, and the second half of that wait is past the guard. This returns <paramref name="limit"/> cut to what the running
    /// body's guard has left, less a tenth of the guard so the wait's own failure (and the body's drain) can finish first; never under a quarter second.
    /// Off the host thread, or outside a body, it returns <paramref name="limit"/> unchanged. <paramref name="cut"/> says why it was cut, or is null.
    /// </summary>
    internal static TimeSpan WithinGuard(TimeSpan limit, out string? cut)
    {
        cut = null;
        var host = t_running;
        var began = host is null ? 0 : Interlocked.Read(ref host._bodyBegan);
        if (host is null || began == 0)
        {
            return limit;
        }

        var used = TimeSpan.FromMilliseconds(Environment.TickCount64 - began);
        var room = host.HangGuard - used - host.HangGuard / 10;
        var bound = room < TimeSpan.FromMilliseconds(250) ? TimeSpan.FromMilliseconds(250) : room;
        if (bound >= limit)
        {
            return limit;
        }

        cut = $"the body had used {used.TotalMilliseconds:0} ms of its {host.HangGuard.TotalMilliseconds:0} ms hang guard before it began waiting, so the wait ends before the guard does and fails this test alone";
        return bound;
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

    /// <summary>Ends the host thread's dispatcher, once a start-up still in flight (one a host test holds, then releases on its way out) has returned. Only for a host a test made for itself.</summary>
    internal void Shutdown() =>
        _starting?.ContinueWith(
            started => started.Result.BeginInvokeShutdown(DispatcherPriority.Send),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnRanToCompletion,
            TaskScheduler.Default);

    private static string GuardText(TimeSpan guard) => guard < TimeSpan.FromSeconds(1) ? $"{guard.TotalMilliseconds:0} ms" : $"{guard.TotalSeconds:0} s";

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
            Interlocked.Exchange(ref _bodyBegan, began);
            var opsAtStart = Interlocked.Read(ref _opsStarted);
            var engineLoadedAtStart = StaThreadProbe.RenderEngineLoaded();
            _step = "(the body named no step)";
            Interlocked.Exchange(ref _stepTick, began);
            var finished = dispatcher.InvokeAsync(() => Job(body, e => error = e)).Task.Unwrap();
            if (!finished.Wait(HangGuard))
            {
                var name = CurrentTestName();
                lock (_stuckLock)
                {
                    _stuckJob = finished;
                    _stuckTest = name;
                }

                throw new TimeoutException(
                    $"WPF test body did not return within {GuardText(HangGuard)}: {name}. The shared STA test thread is stuck inside it, "
                    + "so every WPF test that starts while it is still inside fails at once naming this test (#5602). "
                    + DescribeStuckBody(opsAtStart, engineLoadedAtStart));
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
            Interlocked.Exchange(ref _bodyBegan, 0);
            _gate.Release();
        }
    }

    /// <summary>The body, then the cleanup. Never throws: the body's exception is handed back for the caller to rethrow with its stack.</summary>
    private async Task Job(Func<Task> body, Action<ExceptionDispatchInfo> fail)
    {
        t_running = this;
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
        finally
        {
            t_running = null;
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
            /* #7600: a body that returns late (two PR runs on 11-04: 68 s and 126 s, in bodies that normally take well under a second)
               leaves the thread free again, its dispatcher drained. Refusing every later test for the rest of the process turned one slow
               body into a whole leg of failures. The slow body's own test still fails at the guard, naming itself, and a test that asks
               while the body is still inside it is still refused at once. */
            lock (_stuckLock)
            {
                if (_stuckJob is { IsCompleted: true })
                {
                    _stuckJob = null;
                    _stuckTest = null;
                    return;
                }
            }

            throw new InvalidOperationException(
                $"The shared WPF STA test thread was stranded by a body that did not return within its hang guard: {stuck}. This test did not run (#5602). "
                + "That body has not returned yet (#6632), and a later test runs once it does (#7600).");
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
    private string DescribeStuckBody(long opsAtStart, bool engineLoadedAtStart)
    {
        try
        {
            var step = _step;
            var stepAgo = Environment.TickCount64 - Interlocked.Read(ref _stepTick);
            var startedBefore = Interlocked.Read(ref _opsStarted);
            var cpuBefore = ReadThread(out _, out _);
            var countersBefore = ReadProbe(StaThreadProbe.ReadCounters);
            var stackBefore = ReadText(() => StaThreadProbe.NativeStack(_nativeThreadId));
            Darling.Tests.TestWait.Elapse(s_sampleWindow);
            var startedAfter = Interlocked.Read(ref _opsStarted);
            var cpuAfter = ReadThread(out var threadState, out var inWait);
            var countersAfter = ReadProbe(StaThreadProbe.ReadCounters);
            var stackAfter = ReadText(() => StaThreadProbe.NativeStack(_nativeThreadId));
            var renderThreads = ReadText(() => StaThreadProbe.RenderThreadStacks(_nativeThreadId));
            var running = startedAfter - Interlocked.Read(ref _opsCompleted);
            var last = _lastStarted;
            var lastName = last is null
                ? "none"
                : $"priority {last.Priority}{(s_operationMethod?.GetValue(last) is Delegate d ? $", {d.Method.DeclaringType?.Name}.{d.Method.Name}" : "")}, "
                    + $"{Environment.TickCount64 - Interlocked.Read(ref _lastStartedTick)} ms ago";
            var cpu = cpuAfter - cpuBefore;
            var ran = startedAfter - startedBefore;
            /* #7081: operations started during the sample are the proof the dispatcher is still working, whatever CPU the thread got. Under load a
               pumping thread can get well under half of the window's CPU, and the CPU reading alone called that a wait. CPU only tells the
               two no-progress cases apart: one call burning CPU, or a thread with nothing to do. A spinner the scheduler kept off the CPU for the
               sample is not in the wait state, so a thread that is not waiting counts as spinning too. */
            var verdict = ran > 0
                ? "it is still running dispatcher work that does not end"
                : (cpu.TotalMilliseconds >= s_sampleWindow.TotalMilliseconds / 2 || !inWait
                    ? "it is spinning inside one call"
                    : "it is waiting, and the dispatcher has nothing to run");
            return $"Where it was: the body's last step was \"{step}\", named {stepAgo} ms ago. "
                + $"The dispatcher started {startedBefore - opsAtStart} operations during the body and {ran} more in the next {s_sampleWindow.TotalMilliseconds:0} ms, "
                + $"{running} of them still running; the last it started was {lastName}. "
                + $"The thread is {threadState} and used {cpu.TotalMilliseconds:0} ms of CPU in those {s_sampleWindow.TotalMilliseconds:0} ms, so {verdict} (#6216). "
                + DescribeWhere(stackBefore, stackAfter, countersBefore, countersAfter, engineLoadedAtStart, renderThreads);
        }
        catch (Exception ex)
        {
            return $"(where the body was could not be read: {ex.GetType().Name}: {ex.Message}) (#6216)";
        }
    }

    /// <summary>
    /// #7600: the two hangs of 11-04 were slow bodies (126 s and 68 s, then they returned) waiting with no CPU, which the step names could not
    /// explain. This adds what names the wait: the runtime's own state for the thread (WaitSleepJoin is a .NET wait on a lock, an event or a
    /// task; Running with an operating-system wait is a native call or a message wait), its native stack at the start and the end of the
    /// sample (the same frames twice is a thread parked in one call, not one that makes slow progress), and the process counters around it.
    /// The three stalls of 11-06 then showed the thread waiting in <c>wpfgfx_cor3!MilComposition_SyncFlush</c> for WPF's render thread, early in a
    /// fresh process, in bodies of one TextBlock or one small window. So the message also says whether the render engine was already loaded when
    /// the body began (a body that loads it pays the engine's start-up) and carries the render thread's own stack, which names what it waited on.
    /// </summary>
    private string DescribeWhere(
        string stackBefore, string stackAfter, Result<StaThreadProbe.Counters> before, Result<StaThreadProbe.Counters> after, bool engineLoadedAtStart, string renderThreads)
    {
        var managed = _thread is { } thread ? thread.ThreadState.ToString() : "(the thread object was not kept)";
        var counters = before.Failure is null && after.Failure is null
            ? StaThreadProbe.Describe(before.Value, after.Value, s_sampleWindow)
            : $"(the process counters could not be read: {before.Failure ?? after.Failure})";
        var moved = stackBefore == stackAfter ? "the same at the start of the sample" : $"different at the start of the sample: {stackBefore}";
        return $"The runtime's state for the thread is {managed}. Its native stack, leaf first, was {stackAfter}, and it was {moved}. {counters} "
            + $"WPF's render engine ({StaThreadProbe.RenderEngine}) was {(engineLoadedAtStart ? "already" : "not yet")} loaded when the body began. {renderThreads}";
    }

    private static string ReadText(Func<string> read)
    {
        try
        {
            return read();
        }
        catch (Exception ex)
        {
            return $"(could not be read: {ex.GetType().Name}: {ex.Message})";
        }
    }

    private static Result<T> ReadProbe<T>(Func<T> read)
    {
        try
        {
            return new Result<T>(read(), null);
        }
        catch (Exception ex)
        {
            return new Result<T>(default!, $"{ex.GetType().Name}: {ex.Message}");
        }
    }

    private readonly record struct Result<T>(T Value, string? Failure);

    /// <summary>The host thread's CPU time and state, read from the process's thread list by the native id taken when the thread started.</summary>
    private TimeSpan ReadThread(out string state, out bool inWait)
    {
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        foreach (System.Diagnostics.ProcessThread thread in process.Threads)
        {
            if (thread.Id == _nativeThreadId)
            {
                inWait = thread.ThreadState == System.Diagnostics.ThreadState.Wait;
                state = inWait ? $"Wait ({thread.WaitReason})" : thread.ThreadState.ToString();
                return thread.TotalProcessorTime;
            }
        }

        inWait = true;
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

            var loadedBefore = StaThreadProbe.RenderEngineLoaded();
            var starting = _starting ??= StartThread();
            /* #7600: the wait is bounded by StartUpGuard, not HangGuard, so a host whose test shortened the hang guard measures only its body. */
            if (!starting.Wait(StartUpGuard))
            {
                /* #7600: the start-up is refused to later tests the way a stuck body is, and the refusal lifts the same way once it has returned. */
                lock (_stuckLock)
                {
                    _stuckJob = starting;
                    _stuckTest = RenderEngineStartUp;
                }

                throw new TimeoutException(
                    $"WPF's render engine did not start within {GuardText(StartUpGuard)}: the shared STA test thread shows and closes one window before its first test body, "
                    + "so the engine's start-up is paid once per process and outside every test's hang guard (#7600). It has not returned, and this test did not run. "
                    + DescribeStuckBody(0, loadedBefore));
            }

            _dispatcher = starting.Result;
            return _dispatcher;
        }
    }

    /// <summary>Starts the host thread. Its task completes, with the dispatcher, once the render engine start-up queued ahead of every body has run.</summary>
    private Task<Dispatcher> StartThread()
    {
        var started = new TaskCompletionSource<Dispatcher>(TaskCreationOptions.RunContinuationsAsynchronously);
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
            _thread = Thread.CurrentThread;
            dispatcher.BeginInvoke(DispatcherPriority.Send, new Action(() =>
            {
                StartRenderEngine();
                if (StartUpHold is { } hold)
                {
                    Step("holding the start-up for the host's own test until it releases it");
                    hold.Wait();
                }

                started.SetResult(dispatcher);
            }));
            Dispatcher.Run();
        })
        {
            Name = "StaTestThread",
            /* Background: a stuck body must not keep the test process alive after the run returns. */
            IsBackground = true,
        };
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        return started.Task;
    }

    /// <summary>
    /// #7600: in every leg's result file the first WPF body of the process paid WPF's render engine start-up (5 to 12 s on a green leg, 61 to 126 s on
    /// the five stalled ones, the thread waiting in <c>wpfgfx_cor3!MilComposition_SyncFlush</c> for the render thread) and every later WPF test was fast.
    /// Showing and closing one window makes the first MediaContext, loads the engine, starts its render thread and makes the first composition round
    /// trip, so that cost is paid once per process, here, before any body, and never inside a test's hang guard. The duration goes to the leg's log.
    /// </summary>
    private void StartRenderEngine()
    {
        var loaded = StaThreadProbe.RenderEngineLoaded();
        var began = Environment.TickCount64;
        Step("starting WPF's render engine: showing and closing one window before the first test body");
        try
        {
            var window = new Window { Width = 50, Height = 50, ShowInTaskbar = false };
            window.Show();
            window.Close();
            Console.WriteLine(
                $"StaTestThread: WPF's render engine {(loaded ? "was already loaded, and a first window showed and closed" : "started, with a first window shown and closed,")} "
                + $"in {Environment.TickCount64 - began} ms before the first test body (#7600).");
        }
        catch (Exception ex)
        {
            /* The bodies find out for themselves what WPF cannot do here; the start-up only reports it. */
            Console.WriteLine($"StaTestThread: WPF's render engine start-up failed after {Environment.TickCount64 - began} ms: {ex.GetType().Name}: {ex.Message} (#7600).");
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

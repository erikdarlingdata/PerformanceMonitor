/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Ui;

/// <summary>How much a refresh pass covers. A wider scope includes the narrower one.</summary>
internal enum RefreshScope
{
    /// <summary>Only the visible tab's read (a tab or sub-tab switch).</summary>
    VisibleTab = 1,

    /// <summary>The visible tab's read plus the chrome around it: server clock, alert badge, status line.</summary>
    Full = 2,
}

/// <summary>
/// <para>One refresh pass in flight at a time, and the newest request that arrives meanwhile runs when it ends (#5371).
/// Lite's server tab had a one-way check: the timer's refresh looked at <c>_isRefreshing</c> and bailed, but a tab
/// switch never set it, so a timer tick that fired while a tab-switch read was still waiting started a second full
/// refresh on top of it (the field trace ran 484 s). And the same check dropped a tab switch or time-range change
/// that arrived during a refresh, leaving the combo on the new choice and the charts on the old one.</para>
///
/// <para><b>Latest wins.</b> A <see cref="RequestAsync"/> that arrives while a pass runs is remembered (only the newest;
/// the widest scope among them, plus the scope of the pass it superseded, so a timer pass's alert badge and status
/// are not lost when a tab switch cancels it). The running pass's token is cancelled so it can stop at its next
/// stage boundary instead of painting a result nobody wants, and the remembered request runs once the pass returns.
/// A pass reads the UI's selection and window at its own start, so the replay always loads whatever is selected
/// when it begins, not what was selected when the request was made.</para>
///
/// <para><b>A poll does not queue.</b> <see cref="PollAsync"/> is the timer tick: when a pass is already running or
/// remembered the data is being refreshed now, so the tick starts nothing and leaves nothing behind. (A user gesture
/// is the opposite: it asks for a state the running pass was not started for, so it must run.)</para>
///
/// <para><b>Released on failure.</b> The flag is cleared in a <c>finally</c>, a pass that throws is reported through the
/// fault callback and the loop carries on to any remembered request, so one failed read cannot wedge every refresh
/// after it. Nothing here throws to the caller: callers are <c>async void</c> handlers.</para>
///
/// <para><b>Not thread-safe, deliberately.</b> Every caller is a dispatcher handler, so requests and the loop's
/// continuations are serialised on the UI thread (the same stance as <see cref="ScopedLoadGenerations"/>).</para>
/// </summary>
internal sealed class RefreshCoordinator
{
    private readonly Func<RefreshScope, CancellationToken, Task> _pass;
    private readonly Action<Exception> _onFault;

    private bool _running;
    private RefreshScope? _pending;
    private CancellationTokenSource? _current;
    private RefreshScope _currentScope;
    private TaskCompletionSource? _drained;

    /// <param name="pass">One refresh pass. It must read the selection and window when it starts, and check the token
    /// between its stages (and before painting) so a superseded pass stops.</param>
    /// <param name="onFault">Told about a pass that threw (anything but its own cancellation). Must not throw.</param>
    internal RefreshCoordinator(Func<RefreshScope, CancellationToken, Task> pass, Action<Exception> onFault)
    {
        ArgumentNullException.ThrowIfNull(pass);
        ArgumentNullException.ThrowIfNull(onFault);
        _pass = pass;
        _onFault = onFault;
    }

    /// <summary>True while a pass is running.</summary>
    internal bool IsRunning => _running;

    /// <summary>True when a request arrived during the running pass and will run when it ends.</summary>
    internal bool HasPending => _pending is not null;

    /// <summary>
    /// A user gesture or a catch-up (tab switch, time-range change, filter change, manual refresh). Starts a pass when
    /// idle. When one is running, remembers this request (latest wins) and supersedes the running pass. The task
    /// completes when the whole run, replay included, has finished, so a caller that joined can still await it.
    /// </summary>
    internal Task RequestAsync(RefreshScope scope)
    {
        if (!_running)
        {
            return RunAsync(scope);
        }

        _pending = Widest(Widest(_pending, scope), _currentScope);
        _current?.Cancel();
        return _drained!.Task;
    }

    /// <summary>
    /// The timer tick. Starts a <see cref="RefreshScope.Full"/> pass when idle; does nothing, and queues nothing, when a
    /// pass is running or remembered.
    /// </summary>
    internal Task PollAsync() => _running ? _drained!.Task : RunAsync(RefreshScope.Full);

    private static RefreshScope Widest(RefreshScope? a, RefreshScope b) => a is { } x && x > b ? x : b;

    private async Task RunAsync(RefreshScope first)
    {
        _running = true;
        _drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var scope = first;
        try
        {
            while (true)
            {
                using var cts = new CancellationTokenSource();
                _current = cts;
                _currentScope = scope;
                _pending = null;

                try
                {
                    await _pass(scope, cts.Token);
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    /* The pass stopped because a newer request superseded it; the replay below does its work. */
                }
                catch (Exception ex)
                {
                    try
                    {
                        _onFault(ex);
                    }
                    catch (Exception)
                    {
                        /* The fault callback is logging; losing a log line must not strand the guard. */
                    }
                }

                _current = null;
                if (_pending is not { } next)
                {
                    break;
                }

                scope = next;
            }
        }
        finally
        {
            _current = null;
            _pending = null;
            _running = false;
            _drained.TrySetResult();
        }
    }
}

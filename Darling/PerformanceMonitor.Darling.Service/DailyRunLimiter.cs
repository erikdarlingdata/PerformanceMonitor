/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #4938, #4999: the limit on how many detached daily runs go at once, with a cap that can move while runs hold
/// permits. A <see cref="SemaphoreSlim"/> cannot be resized, and the cap is derived from the store's connection
/// pool and the sweep width (<see cref="DarlingWorker.DailyRunCapFor"/>), both of which an operator can change
/// while the service is running.
///
/// <para>Widening hands the new permits to the runs that are waiting, oldest first. Narrowing never takes a permit
/// back from a run that holds one: the runs already going finish, and no run starts until fewer than the new cap
/// are going. A waiting run is never dropped, and one that is cancelled while it waits leaves the queue and
/// takes no permit. Waiters are served in the order they arrived.</para>
/// </summary>
internal sealed class DailyRunLimiter
{
    private readonly object _lock = new();
    private readonly LinkedList<Waiter> _waiters = new();
    private readonly Func<DateTime> _clock;
    private int _cap;
    private int _inUse;

    /// <summary>A limiter that lets <paramref name="cap"/> runs go at once (at least one).</summary>
    internal DailyRunLimiter(int cap)
        : this(cap, static () => DateTime.UtcNow)
    {
    }

    /// <param name="clock">The UTC clock that stamps when a run starts waiting; tests pin it.</param>
    internal DailyRunLimiter(int cap, Func<DateTime> clock)
    {
        _cap = Math.Max(1, cap);
        _clock = clock;
    }

    /// <summary>One run waiting for a permit: its wake-up and when it started waiting.</summary>
    private sealed record Waiter(TaskCompletionSource Source, DateTime SinceUtc);

    /// <summary>How many runs may go at once right now.</summary>
    internal int Cap
    {
        get
        {
            lock (_lock)
            {
                return _cap;
            }
        }
    }

    /// <summary>How many runs hold a permit right now. It can exceed <see cref="Cap"/> after the cap is narrowed.</summary>
    internal int InUse
    {
        get
        {
            lock (_lock)
            {
                return _inUse;
            }
        }
    }

    /// <summary>How many runs are waiting for a permit right now.</summary>
    internal int Waiting
    {
        get
        {
            lock (_lock)
            {
                return _waiters.Count;
            }
        }
    }

    /// <summary>
    /// When the run that has waited longest started waiting, or null when none is waiting. With <see cref="Waiting"/>,
    /// the size and the age of the permit backlog: on a fleet whose daily runs need more permit time than a day holds,
    /// this is the cause a lost daily run names (<see cref="DescribeBacklog"/>).
    /// </summary>
    internal DateTime? OldestWaitUtc
    {
        get
        {
            lock (_lock)
            {
                return _waiters.First?.Value.SinceUtc;
            }
        }
    }

    /// <summary>
    /// The backlog in words, for the line a lost daily run logs and the server's collection_cycle row: how many runs wait
    /// for one of <paramref name="cap"/> permits and how long the oldest has waited, or that none waits, which means the
    /// run that holds the collector's slot is still executing.
    /// </summary>
    internal static string DescribeBacklog(int waiting, int cap, DateTime? oldestWaitUtc, DateTime nowUtc)
    {
        if (waiting <= 0 || oldestWaitUtc is not { } oldest)
        {
            return "no daily run is waiting for a permit, so the previous run is still executing";
        }

        var minutes = nowUtc > oldest ? (int)(nowUtc - oldest).TotalMinutes : 0;
        var age = minutes < 60
            ? string.Create(CultureInfo.InvariantCulture, $"{minutes} min")
            : string.Create(CultureInfo.InvariantCulture, $"{minutes / 60} h {minutes % 60} min");
        return string.Create(CultureInfo.InvariantCulture,
            $"{waiting} daily run(s) are waiting for one of {cap} permits, and the oldest has waited {age}");
    }

    /// <summary>
    /// Moves the cap to <paramref name="cap"/> (at least one) and hands any permit it frees to the waiting runs.
    /// Returns whether the cap changed.
    /// </summary>
    internal bool SetCap(int cap)
    {
        List<TaskCompletionSource>? granted;
        lock (_lock)
        {
            cap = Math.Max(1, cap);
            if (cap == _cap)
            {
                return false;
            }

            _cap = cap;
            granted = GrantWaitersLocked();
        }

        Complete(granted);
        return true;
    }

    /// <summary>
    /// Takes a permit when one is free and nobody is queued ahead, and says whether it did. Never waits. A run
    /// that gets true must call <see cref="Release"/> once.
    /// </summary>
    internal bool TryAcquire()
    {
        lock (_lock)
        {
            if (_waiters.Count == 0 && _inUse < _cap)
            {
                _inUse++;
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Takes a permit, waiting for one when all are in use. Ends with an <see cref="OperationCanceledException"/>
    /// when <paramref name="cancellationToken"/> is cancelled while the run waits, and the run then holds
    /// nothing. A run that is handed a permit at the moment of the cancel keeps it, as a semaphore does, and
    /// must call <see cref="Release"/> once.
    /// </summary>
    internal async Task AcquireAsync(CancellationToken cancellationToken)
    {
        var waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        LinkedListNode<Waiter> node;
        lock (_lock)
        {
            if (_waiters.Count == 0 && _inUse < _cap)
            {
                _inUse++;
                return;
            }

            node = _waiters.AddLast(new Waiter(waiter, _clock()));
        }

        using var registration = cancellationToken.Register(() => CancelWaiter(node, cancellationToken));
        await waiter.Task.ConfigureAwait(false);
    }

    /// <summary>Gives a permit back and hands it to the oldest waiting run when the cap allows one more to go.</summary>
    internal void Release()
    {
        List<TaskCompletionSource>? granted;
        lock (_lock)
        {
            if (_inUse > 0)
            {
                _inUse--;
            }

            granted = GrantWaitersLocked();
        }

        Complete(granted);
    }

    private void CancelWaiter(LinkedListNode<Waiter> node, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (node.List is null)
            {
                /* Already handed a permit: the run that waited keeps it. */
                return;
            }

            _waiters.Remove(node);
        }

        node.Value.Source.TrySetCanceled(cancellationToken);
    }

    private List<TaskCompletionSource>? GrantWaitersLocked()
    {
        List<TaskCompletionSource>? granted = null;
        while (_inUse < _cap && _waiters.First is { } first)
        {
            _waiters.RemoveFirst();
            _inUse++;
            (granted ??= []).Add(first.Value.Source);
        }

        return granted;
    }

    private static void Complete(List<TaskCompletionSource>? granted)
    {
        if (granted is null)
        {
            return;
        }

        foreach (var waiter in granted)
        {
            waiter.TrySetResult();
        }
    }
}

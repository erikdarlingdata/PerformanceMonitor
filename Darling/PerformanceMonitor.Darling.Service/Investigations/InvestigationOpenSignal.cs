/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The in-process wake-up of the investigation long-poll (#5822). <c>wait_for_investigation_requests</c> registers a
/// <see cref="Waiter"/> for the routes it waits on; the dispatcher calls <see cref="Opened"/> when a request OPENS (never on a
/// join or at a limit), and every waiter on that route wakes. A waiter carries no payload: the woken call reads the store,
/// which is the queue (decision 3: nothing is held in memory for an agent that is not waiting).
///
/// <para>One waiter per waiting call: it leaves the registry when the call is answered, times out or is cancelled
/// (<see cref="Waiter.Dispose"/>). With nobody waiting, <see cref="Opened"/> costs a lock and an empty loop.</para>
/// </summary>
public sealed class InvestigationOpenSignal
{
    /// <summary>
    /// The process's one signal: the worker's dispatcher fires it and the MCP host's tool waits on it (the two run in one
    /// process, as <c>StoreHostProfileCache.Shared</c> does). A test builds its own.
    /// </summary>
    public static InvestigationOpenSignal Shared { get; } = new();

    private readonly object _gate = new();
    private readonly List<Waiter> _waiters = [];

    /// <summary>How many calls are waiting now.</summary>
    public int WaiterCount
    {
        get
        {
            lock (_gate)
            {
                return _waiters.Count;
            }
        }
    }

    /// <summary>
    /// Registers a waiter for <paramref name="routeIds"/> (every route when null or empty). Register BEFORE the store read
    /// that decides whether to wait, so an open that commits between the read and the wait still wakes it.
    /// </summary>
    public Waiter Register(IReadOnlyCollection<int>? routeIds)
    {
        var waiter = new Waiter(this, routeIds is { Count: > 0 } ? new HashSet<int>(routeIds) : null);
        lock (_gate)
        {
            _waiters.Add(waiter);
        }

        return waiter;
    }

    /// <summary>Wakes every waiter on <paramref name="routeId"/>, and every waiter on all routes. Never throws.</summary>
    public void Opened(int routeId)
    {
        lock (_gate)
        {
            foreach (var waiter in _waiters)
            {
                if (waiter.Routes is null || waiter.Routes.Contains(routeId))
                {
                    waiter.Wake();
                }
            }
        }
    }

    private void Remove(Waiter waiter)
    {
        lock (_gate)
        {
            _waiters.Remove(waiter);
        }
    }

    /// <summary>One waiting call's registration. Dispose it when the call ends, however it ends.</summary>
    public sealed class Waiter : IDisposable
    {
        private readonly InvestigationOpenSignal _owner;
        private TaskCompletionSource _woken = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Waiter(InvestigationOpenSignal owner, IReadOnlySet<int>? routes)
        {
            _owner = owner;
            Routes = routes;
        }

        /// <summary>The routes it waits on; null for every route.</summary>
        internal IReadOnlySet<int>? Routes { get; }

        internal void Wake() => Volatile.Read(ref _woken).TrySetResult();

        /// <summary>
        /// Waits until a request opens on one of its routes since the last <see cref="Rearm"/> (or the registration), up to
        /// <paramref name="timeout"/>. True when woken, false at the timeout; a cancellation throws.
        /// </summary>
        public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (timeout <= TimeSpan.Zero)
            {
                return Volatile.Read(ref _woken).Task.IsCompleted;
            }

            try
            {
                await Volatile.Read(ref _woken).Task.WaitAsync(timeout, cancellationToken);
                return true;
            }
            catch (TimeoutException)
            {
                return false;
            }
        }

        /// <summary>Clears a wake that was already read, so the next <see cref="WaitAsync"/> waits for a new open.</summary>
        public void Rearm()
        {
            var current = Volatile.Read(ref _woken);
            if (current.Task.IsCompleted)
            {
                Interlocked.CompareExchange(ref _woken, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), current);
            }
        }

        /// <summary>Leaves the registry.</summary>
        public void Dispose() => _owner.Remove(this);
    }
}

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

namespace PerformanceMonitor.Common;

/// <summary>
/// #4477: a small single-flight, time-boxed memo for one async fetch, shared by every caller that races
/// a cold cache — the shape the Viewer's fleet-health read (<c>GetFleetCollectionHealthByServerAsync</c>)
/// needs, extracted so its two defects can be fixed and pinned once instead of per call site.
///
/// <para><b>Defect (a), the synchronous-completion wedge.</b> The naive pattern
/// (<c>_inFlight ??= Fetch();</c> under a lock, with the <c>finally</c> inside <c>Fetch</c> clearing the
/// field) races itself when <c>Fetch</c>'s task can complete — successfully or by fault — before the
/// assignment to <c>_inFlight</c> happens, which a caller cannot prevent for an <c>async</c> method whose
/// first await is already satisfied (a cached/no-op path) or whose setup throws synchronously before any
/// await at all. In THAT ordering, the <c>finally</c> clears a field the <c>??=</c> has not written to
/// yet, and the very next line then writes the already-finished (possibly faulted) task into it — where
/// it sits, unraced, until the TTL. <see cref="GetOrStartAsync"/> avoids the whole race by never running
/// the fetch inline under the lock: it starts the fetch with <see cref="Task.Run(Func{Task})"/> so the
/// delegate cannot complete before <see cref="GetOrStartAsync"/> has returned the assigned task to its
/// caller, and the clear runs in a <c>finally</c> inside that same task, gated by the same lock — so the
/// clear can only ever race the assignment as "assignment happens strictly first", never the reverse, and
/// it has finished before any caller's await of the task resumes.</para>
///
/// <para><b>Defect (b), a waiter that cannot cancel.</b> A caller's own
/// <see cref="CancellationToken"/> must be honored for THAT caller without cancelling the shared fetch
/// for every other racer — <see cref="GetOrStartAsync"/> returns
/// <c>sharedTask.WaitAsync(cancellationToken)</c>: the shared fetch itself always runs with
/// <see cref="CancellationToken.None"/> (it is not owned by any one caller), and only the wait each
/// caller performs on it is cancellable.</para>
/// </summary>
public sealed class SingleFlightTtlCache<T>
{
    private readonly object _gate = new();
    private readonly TimeSpan _ttl;

    private T _value = default!;
    private bool _hasValue;
    private DateTime _valueAtUtc;
    private Task<T>? _inFlight;

    public SingleFlightTtlCache(TimeSpan ttl)
    {
        _ttl = ttl;
    }

    /// <summary>
    /// Returns the cached value if it is still inside the TTL; otherwise starts exactly ONE fetch (or joins
    /// one already running) and returns a task every racing caller can await. <paramref name="fetch"/> runs
    /// with no caller's cancellation token — it is shared work — while <paramref name="cancellationToken"/>
    /// only cancels THIS caller's wait on it.
    /// </summary>
    public Task<T> GetOrStartAsync(Func<Task<T>> fetch, CancellationToken cancellationToken = default)
        => GetOrStartAsync(fetch, shouldCache: null, cancellationToken);

    /// <summary>Same as <see cref="GetOrStartAsync(Func{Task{T}}, CancellationToken)"/>, with
    /// <paramref name="shouldCache"/> deciding whether a completed fetch's result is worth caching — the
    /// store-size read uses this to skip caching a null reading (a transient read failure) so the very
    /// next call retries instead of serving null for the rest of the TTL.</summary>
    public Task<T> GetOrStartAsync(Func<Task<T>> fetch, Func<T, bool>? shouldCache, CancellationToken cancellationToken = default)
    {
        Task<T> sharedTask;

        lock (_gate)
        {
            if (_hasValue && DateTime.UtcNow - _valueAtUtc < _ttl)
            {
                return Task.FromResult(_value);
            }

            if (_inFlight is not null)
            {
                sharedTask = _inFlight;
            }
            else
            {
                /* Task.Run, not an inline call: the delegate cannot finish (successfully, by fault, or by a
                   synchronous throw before its first await) before this method has finished assigning
                   _inFlight below — closing defect (a). The delegate's own finally clears _inFlight and
                   writes the cache under _gate, so it can only run AFTER the assignment, never race ahead
                   of it. */
                /* The clear and the cache write run INSIDE the task callers wait on (the lock below cannot be
                   taken until this block has assigned _inFlight), so a caller that sees the fetch fault and
                   calls again at once finds _inFlight already cleared and starts a fresh fetch. A separate
                   ContinueWith could run after the caller's own continuation and hand it the faulted task. */
                Task<T>? started = null;
                started = Task.Run(async () =>
                {
                    var succeeded = false;
                    T result = default!;
                    try
                    {
                        result = await fetch().ConfigureAwait(false);
                        succeeded = true;
                        return result;
                    }
                    finally
                    {
                        lock (_gate)
                        {
                            if (ReferenceEquals(_inFlight, started))
                            {
                                _inFlight = null;
                            }

                            if (succeeded && (shouldCache is null || shouldCache(result)))
                            {
                                _value = result;
                                _hasValue = true;
                                _valueAtUtc = DateTime.UtcNow;
                            }
                        }
                    }
                });
                _inFlight = started;
                sharedTask = started;
            }
        }

        /* Defect (b): one caller's own token only cancels ITS wait on the shared task, never the shared
           fetch itself — the other racers still get the result when it lands. */
        return sharedTask.WaitAsync(cancellationToken);
    }
}

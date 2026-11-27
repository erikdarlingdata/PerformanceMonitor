/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #9535: the plans one server's plan fetch phase skipped on their own (a lock timeout or a command timeout on a
/// single-key ask), kept across runs so the next run asks for them last. A plan that takes longer than the command
/// timeout to render stays in the same chunk every run, and asked first it would spend the budget of every run on one
/// key. Asked after every other key, and only while budget remains, it costs the others nothing.
///
/// <para>The set holds at most <c>capacity</c> identities, the phase's own cap on plans per run: a full set drops its
/// oldest entry for a new one. A key leaves the set when a fetch command answers it. An entry whose plan no longer
/// appears in a run stays until it is pushed out, and matches nothing meanwhile. It holds identities only, never the
/// rows the keys came from.</para>
/// </summary>
/// <typeparam name="TId">The identity of a plan, compared by value.</typeparam>
internal sealed class PlanFetchLaterKeys<TId> where TId : notnull
{
    private readonly int _capacity;
    private readonly Dictionary<TId, long> _order = [];
    private readonly object _gate = new();
    private long _sequence;

    internal PlanFetchLaterKeys(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _capacity = capacity;
    }

    /// <summary>How many plans are held.</summary>
    internal int Count
    {
        get
        {
            lock (_gate)
            {
                return _order.Count;
            }
        }
    }

    internal bool Contains(TId id)
    {
        lock (_gate)
        {
            return _order.ContainsKey(id);
        }
    }

    /// <summary>Holds <paramref name="id"/> as the newest entry, dropping the oldest when the set is full.</summary>
    internal void Remember(TId id)
    {
        lock (_gate)
        {
            _order[id] = ++_sequence;
            if (_order.Count <= _capacity)
            {
                return;
            }

            var oldest = default(TId);
            var oldestSequence = long.MaxValue;
            foreach (var (held, sequence) in _order)
            {
                if (sequence < oldestSequence)
                {
                    oldest = held;
                    oldestSequence = sequence;
                }
            }

            _order.Remove(oldest!);
        }
    }

    /// <summary>Drops <paramref name="id"/>: a fetch command answered it.</summary>
    internal void Forget(TId id)
    {
        lock (_gate)
        {
            _order.Remove(id);
        }
    }
}

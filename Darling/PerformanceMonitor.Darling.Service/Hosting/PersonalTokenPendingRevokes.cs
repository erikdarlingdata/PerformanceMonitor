/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>What <see cref="PersonalTokenPendingRevokes.Add"/> did with an entry.</summary>
internal enum PersonalTokenPendingAdd
{
    /// <summary>The owner had no entry; it was queued.</summary>
    Queued,

    /// <summary>The owner already had an entry; the broader of the two stays.</summary>
    Merged,

    /// <summary>The queue is full and the entry was dropped; this is the first drop, so the caller says so.</summary>
    DroppedFirst,

    /// <summary>The queue is full and the entry was dropped; the first drop was already reported.</summary>
    Dropped,
}

/// <summary>
/// The revokes a sign-in could not make (#8737): no owner connection yet, or the store write threw. They live in memory only and are drained from
/// the web supervisor tick, so a sign-in during a store outage keeps its session and the revoke lands within a tick of the store coming back. One
/// entry per owner key; an entry leaves only after its revoke commits. A restart loses the queue.
/// </summary>
internal sealed class PersonalTokenPendingRevokes
{
    /// <summary>The most owners queued at once. A full queue drops the newest entry, with one warning.</summary>
    internal const int MaxOwners = 10_000;

    /// <summary>One queued revoke: the owner, whether only operator tokens go, and the reason code the revoke journals.</summary>
    internal sealed record Entry(string OwnerKey, bool OperatorOnly, string Reason);

    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(System.StringComparer.Ordinal);
    private readonly int _max;
    private bool _overflowReported;

    internal PersonalTokenPendingRevokes(int maxOwners = MaxOwners) => _max = maxOwners;

    internal int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    /// <summary>Queues the revoke. For an owner that already has one, the later entry wins when it is broader (all roles over operator-only), and
    /// the reason comes from the winning entry; an equal or narrower later entry changes nothing.</summary>
    internal PersonalTokenPendingAdd Add(string ownerKey, bool operatorOnly, string reason)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(ownerKey, out var existing))
            {
                if (existing.OperatorOnly && !operatorOnly)
                {
                    _entries[ownerKey] = new Entry(ownerKey, operatorOnly, reason);
                }

                return PersonalTokenPendingAdd.Merged;
            }

            if (_entries.Count >= _max)
            {
                var first = !_overflowReported;
                _overflowReported = true;
                return first ? PersonalTokenPendingAdd.DroppedFirst : PersonalTokenPendingAdd.Dropped;
            }

            _entries[ownerKey] = new Entry(ownerKey, operatorOnly, reason);
            return PersonalTokenPendingAdd.Queued;
        }
    }

    /// <summary>The queued entries now, for one drain pass.</summary>
    internal IReadOnlyList<Entry> Snapshot()
    {
        lock (_gate)
        {
            return _entries.Values.ToList();
        }
    }

    /// <summary>Removes the owner's entry after its revoke committed, but only if it is still the entry that was revoked: a broader one that
    /// replaced it meanwhile stays for the next pass.</summary>
    internal void Complete(Entry done)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(done.OwnerKey, out var current) && ReferenceEquals(current, done))
            {
                _entries.Remove(done.OwnerKey);
                if (_entries.Count < _max)
                {
                    _overflowReported = false;
                }
            }
        }
    }
}

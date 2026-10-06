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
/// One cached plan identity (#5158): the content digest the store holds for it (null for a plan over the
/// capture cap, which has no stored plan), its measured size, whether a commit has proved it, and when this
/// host last saw the identity. <paramref name="RenderedOrdinal"/> is the caller's own count of the run that
/// rendered the plan (a collector that expires entries by age in runs keeps one); it is never refreshed by a hit,
/// and is 0 when the caller keeps none.
/// </summary>
internal readonly record struct PlanDigestEntry(
    string? Digest, long? Bytes, bool Confirmed, DateTime LastSeenUtc, long RenderedOrdinal = 0);

/// <summary>
/// What the host remembers about statement plans it has already committed, keyed by whatever identity a
/// collector uses for "this exact plan" (#5158). One instance per server, held by the runner for the service's
/// lifetime, so a restart starts empty and each server renders its plans once more.
///
/// <para><b>Why the host's own commits and no store probe.</b> A probe before rendering costs a store round
/// trip per run, and #2831 measured that cost as client-side time rather than store time (query_store's
/// probe was about half its wall time even after #3216). The commit is already proof: an entry is
/// <see cref="Confirmed"/> only after the batch that wrote its digest committed, so a hit means the store
/// holds the plan and nothing needs asking.</para>
///
/// <para><b>Lifecycle.</b> <see cref="AddPending"/> records a plan this run rendered, unusable until
/// <see cref="ConfirmPending"/> runs after the commit; <see cref="DiscardPending"/> drops it when the write
/// failed. <see cref="Evict"/> removes digests the store reported missing (compared case-insensitively:
/// <c>PayloadDimensionWriter.FlushAsync</c> reports upper-case hex), and <see cref="Prune"/> forgets
/// identities unseen for a while so the map follows the plan cache instead of growing.</para>
///
/// <para>Free of any collector's key shape: <typeparamref name="TKey"/> only needs value equality.
/// Thread-safe per instance.</para>
/// </summary>
internal sealed class PlanDigestCache<TKey>
    where TKey : IEquatable<TKey>
{
    private readonly object _gate = new();
    private readonly Dictionary<TKey, PlanDigestEntry> _entries = new();

    /// <summary>How many identities are held, confirmed or pending.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    /// <summary>
    /// A hit is a CONFIRMED entry only: a pending one has not been proved by a commit. A hit refreshes the
    /// entry's last-seen time, which is what <see cref="Prune"/> ages on.
    /// </summary>
    public bool TryGet(TKey key, DateTime nowUtc, out PlanDigestEntry entry)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var found) && found.Confirmed)
            {
                entry = found with { LastSeenUtc = nowUtc };
                _entries[key] = entry;
                return true;
            }
        }

        entry = default;
        return false;
    }

    /// <summary>
    /// Records a plan this run rendered. <paramref name="digest"/> is null for a plan over the cap.
    /// <paramref name="renderedOrdinal"/> is the caller's count of this run, kept for <see cref="PlanDigestEntry.RenderedOrdinal"/>.
    /// </summary>
    public void AddPending(TKey key, string? digest, long? bytes, DateTime nowUtc, long renderedOrdinal = 0)
    {
        lock (_gate)
        {
            _entries[key] = new PlanDigestEntry(digest, bytes, Confirmed: false, nowUtc, renderedOrdinal);
        }
    }

    /// <summary>Marks pending entries proved. Call only after the batch carrying them committed.</summary>
    public void ConfirmPending(IEnumerable<TKey> keys, DateTime nowUtc)
    {
        ArgumentNullException.ThrowIfNull(keys);

        lock (_gate)
        {
            foreach (var key in keys)
            {
                if (_entries.TryGetValue(key, out var found) && !found.Confirmed)
                {
                    _entries[key] = found with { Confirmed = true, LastSeenUtc = nowUtc };
                }
            }
        }
    }

    /// <summary>Forgets every entry, confirmed and pending (#5367 review, A-L1: a cache filled under one filter mode
    /// must not answer a run in another).</summary>
    public void Clear()
    {
        lock (_gate)
        {
            _entries.Clear();
        }
    }

    /// <summary>Drops pending entries whose write failed. A confirmed entry is never touched.</summary>
    public void DiscardPending(IEnumerable<TKey> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);

        lock (_gate)
        {
            foreach (var key in keys)
            {
                if (_entries.TryGetValue(key, out var found) && !found.Confirmed)
                {
                    _entries.Remove(key);
                }
            }
        }
    }

    /// <summary>
    /// Removes every entry (confirmed or pending) holding one of <paramref name="digests"/>, so the next
    /// sighting re-renders. The comparison ignores case.
    /// </summary>
    /// <returns>How many entries were removed.</returns>
    public int Evict(IEnumerable<string> digests)
    {
        ArgumentNullException.ThrowIfNull(digests);

        var wanted = new HashSet<string>(digests, StringComparer.OrdinalIgnoreCase);
        if (wanted.Count == 0)
        {
            return 0;
        }

        lock (_gate)
        {
            List<TKey>? doomed = null;
            foreach (var (key, entry) in _entries)
            {
                if (entry.Digest is not null && wanted.Contains(entry.Digest))
                {
                    (doomed ??= new List<TKey>()).Add(key);
                }
            }

            if (doomed is null)
            {
                return 0;
            }

            foreach (var key in doomed)
            {
                _entries.Remove(key);
            }

            return doomed.Count;
        }
    }

    /// <summary>Forgets entries last seen before <paramref name="cutoffUtc"/>, pending ones included.</summary>
    /// <returns>How many entries were removed.</returns>
    public int Prune(DateTime cutoffUtc)
    {
        lock (_gate)
        {
            List<TKey>? stale = null;
            foreach (var (key, entry) in _entries)
            {
                if (entry.LastSeenUtc < cutoffUtc)
                {
                    (stale ??= new List<TKey>()).Add(key);
                }
            }

            if (stale is null)
            {
                return 0;
            }

            foreach (var key in stale)
            {
                _entries.Remove(key);
            }

            return stale.Count;
        }
    }
}

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
/// #4659: tells readers that cache an answer derived from <c>query_store_stats</c> that the table changed for a
/// server. Every Query Store batch write goes through <see cref="DarlingCollectorRunner"/>'s COPY chokepoint, which
/// calls <see cref="BeginWrite"/> before its transaction and <see cref="EndWrite"/> in a <c>finally</c> after it: the
/// live fan-out (one transaction per database under one collection_time), overlapped runs, retries and the backfill.
/// A reader snapshots the fence before it reads and trusts a cached answer only if no write was in flight, the
/// server is not poisoned and the generation has not moved. One writer per store is the product's invariant; this
/// fence covers every writer in it.
/// A write that exits by exception is ambiguous: a commit that faulted on the client (a timeout, a cancel, a dead
/// socket) after COMMIT was sent may still become visible on the server later. That write is treated as "may have
/// landed": the server stays non-quiet, so nothing is reused or stored, until a clean write proves the table
/// state is known again. A success clears the poison only when it began with no write in flight, no failure ended
/// during its span, and nothing else is still in flight when it ends; anything less keeps the server poisoned.
/// </summary>
public sealed class QueryStoreWriteFence
{
    private readonly object _gate = new();
    private readonly Dictionary<int, (long Generation, int InFlight, bool Poisoned, bool CleanSpan)> _state = new();

    /* #5518: the database names each server's batches have named, with the sequence number of the latest one. A
       reader that caches a list of names (QueryStoreBackfill's per-tick candidate list) asks whether any name
       outside the list was written since it read. The sequence is one counter for the whole fence, so a snapshot
       of it orders against every server's writes. Bounded by the number of distinct database names. */
    private readonly Dictionary<int, Dictionary<string, long>> _writtenNames = new();
    private long _nameSequence;

    /// <param name="serverId">The server the write targets.</param>
    /// <param name="databases">The database names the batch writes (#5518), recorded BEFORE the transaction opens,
    /// like the generation, so a name is visible to a reader the moment its write is in flight, and a write that
    /// then fails is recorded as well. Null or empty for a caller with no names to give.</param>
    public void BeginWrite(int serverId, IReadOnlyCollection<string>? databases = null)
    {
        lock (_gate)
        {
            if (databases is { Count: > 0 })
            {
                if (!_writtenNames.TryGetValue(serverId, out var names))
                {
                    names = new Dictionary<string, long>(StringComparer.Ordinal);
                    _writtenNames[serverId] = names;
                }

                foreach (var database in databases)
                {
                    if (database is not null)
                    {
                        names[database] = ++_nameSequence;
                    }
                }
            }

            var s = _state.GetValueOrDefault(serverId);
            /* A write that starts with nothing in flight opens a fresh clean span; an overlapping one inherits it. */
            _state[serverId] = (s.Generation + 1, s.InFlight + 1, s.Poisoned, s.InFlight == 0 || s.CleanSpan);
        }
    }

    /// <param name="serverId">The server the write targeted.</param>
    /// <param name="succeeded">False when the fenced block exited by exception, so the commit's outcome is unknown.</param>
    public void EndWrite(int serverId, bool succeeded)
    {
        lock (_gate)
        {
            var s = _state.GetValueOrDefault(serverId);
            var inFlight = Math.Max(0, s.InFlight - 1);
            if (!succeeded)
            {
                _state[serverId] = (s.Generation + 1, inFlight, true, false);
            }
            else
            {
                var clears = inFlight == 0 && s.CleanSpan;
                _state[serverId] = (s.Generation + 1, inFlight, clears ? false : s.Poisoned, s.CleanSpan);
            }
        }
    }

    /// <summary>#5518: the name sequence now, and whether no write is in flight and none ended ambiguously. A reader
    /// that caches a list of names takes this BEFORE it reads and stores the list only when Quiet: a write already in
    /// flight may commit after the read and its name sits at a sequence the reader would never look at.</summary>
    public (long Sequence, bool Quiet) NameSnapshot(int serverId)
    {
        lock (_gate)
        {
            var s = _state.GetValueOrDefault(serverId);
            return (_nameSequence, s.InFlight == 0 && !s.Poisoned);
        }
    }

    /// <summary>#5518: true when a batch named a database that is NOT in <paramref name="known"/> after
    /// <paramref name="sinceSequence"/> (a <see cref="NameSnapshot"/>'s). A name that is in the set does not change a
    /// list of names, however many rows it writes; an unknown one may be a database the list is missing.</summary>
    public bool WroteUnknownNameSince(int serverId, long sinceSequence, IReadOnlySet<string> known)
    {
        lock (_gate)
        {
            if (!_writtenNames.TryGetValue(serverId, out var names))
            {
                return false;
            }

            foreach (var (name, sequence) in names)
            {
                if (sequence > sinceSequence && !known.Contains(name))
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>The server's generation, and whether no write is in flight and none ended ambiguously.</summary>
    public (long Generation, bool Quiet) Snapshot(int serverId)
    {
        lock (_gate)
        {
            var s = _state.GetValueOrDefault(serverId);
            return (s.Generation, s.InFlight == 0 && !s.Poisoned);
        }
    }
}

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

    public void BeginWrite(int serverId)
    {
        lock (_gate)
        {
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

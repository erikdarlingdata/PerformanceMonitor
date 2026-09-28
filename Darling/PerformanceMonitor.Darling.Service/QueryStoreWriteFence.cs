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
/// A reader snapshots the fence before it reads and trusts a cached answer only if no write was in flight and the
/// generation has not moved. One writer per store is the product's invariant; this fence covers every writer in it.
/// </summary>
public sealed class QueryStoreWriteFence
{
    private readonly object _gate = new();
    private readonly Dictionary<int, (long Generation, int InFlight)> _state = new();

    public void BeginWrite(int serverId)
    {
        lock (_gate)
        {
            var s = _state.GetValueOrDefault(serverId);
            _state[serverId] = (s.Generation + 1, s.InFlight + 1);
        }
    }

    public void EndWrite(int serverId)
    {
        lock (_gate)
        {
            var s = _state.GetValueOrDefault(serverId);
            _state[serverId] = (s.Generation + 1, Math.Max(0, s.InFlight - 1));
        }
    }

    /// <summary>The server's generation, and whether no write is in flight.</summary>
    public (long Generation, bool Quiet) Snapshot(int serverId)
    {
        lock (_gate)
        {
            var s = _state.GetValueOrDefault(serverId);
            return (s.Generation, s.InFlight == 0);
        }
    }
}

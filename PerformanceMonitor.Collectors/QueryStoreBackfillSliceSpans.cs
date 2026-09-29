/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The window the next Query Store backfill slice uses on each server, shared by both products' workers so they
/// size a slice by the same rule (#4771). A server whose hour-wide slices die at the command timeout digs in
/// narrower chunks until one fits (<see cref="QueryStoreBackfillState.AdaptiveSpan"/>, #2111): each failed slice
/// halves the server's span, floored at <see cref="QueryStoreBackfillState.MinAdaptiveSpan"/>.
///
/// <para>A completed slice keeps the span that just worked. It used to send the server straight back to the full
/// hour, so a database whose 30-minute slice fit but whose 60-minute slice did not ran 60 (timed out), 30, 60
/// (timed out), 30 and so on, a full command-timeout read wasted on every other tick. Now the span stays where it
/// fit, and only a run of <see cref="QueryStoreBackfillState.WidenAfterConsecutiveSuccesses"/> completed slices
/// widens it by one halving step, never above <see cref="QueryStoreBackfillState.MaxSliceSpan"/>, so a server that
/// has recovered still works its way back to the full width.</para>
///
/// <para>Per server, not per database, for the reason <see cref="QueryStoreBackfillFailureLedger"/> gives: a command
/// timeout usually means the whole server is loaded, and sizing the slice per database would add timed-out
/// queries against a server that is already struggling. In memory on purpose, like the ledger: a restart forgets
/// it and costs one full-width slice. Thread-safe for symmetry with the ledger, though each worker is
/// single-threaded today.</para>
/// </summary>
public sealed class QueryStoreBackfillSliceSpans
{
    private readonly object _gate = new();

    /// <summary>Only servers that are narrower than the full span; a server at the full span has no entry.</summary>
    private readonly Dictionary<int, (TimeSpan Span, int Successes)> _entries = [];

    /// <summary>The span the server's next slice should use: the full <see cref="QueryStoreBackfillState.MaxSliceSpan"/>
    /// until a slice fails.</summary>
    public TimeSpan Current(int serverId)
    {
        lock (_gate)
        {
            return _entries.TryGetValue(serverId, out var entry) ? entry.Span : QueryStoreBackfillState.MaxSliceSpan;
        }
    }

    /// <summary>A failed slice halves the server's span (floored at <see cref="QueryStoreBackfillState.MinAdaptiveSpan"/>)
    /// and ends any run of completed slices.</summary>
    public void RecordFailure(int serverId)
    {
        lock (_gate)
        {
            var span = _entries.TryGetValue(serverId, out var entry) ? entry.Span : QueryStoreBackfillState.MaxSliceSpan;
            _entries[serverId] = (QueryStoreBackfillState.AdaptiveSpan(span, 1), 0);
        }
    }

    /// <summary>A completed slice keeps the span that just worked. The
    /// <see cref="QueryStoreBackfillState.WidenAfterConsecutiveSuccesses"/>th completed slice in a row widens it by
    /// one halving step, up to <see cref="QueryStoreBackfillState.MaxSliceSpan"/>, and starts a new run.</summary>
    public void RecordCompletion(int serverId)
    {
        lock (_gate)
        {
            /* No entry means the server is already at the full span: nothing to widen. */
            if (!_entries.TryGetValue(serverId, out var entry))
            {
                return;
            }

            var successes = entry.Successes + 1;
            if (successes < QueryStoreBackfillState.WidenAfterConsecutiveSuccesses)
            {
                _entries[serverId] = (entry.Span, successes);
                return;
            }

            var widened = TimeSpan.FromTicks(Math.Min(QueryStoreBackfillState.MaxSliceSpan.Ticks, entry.Span.Ticks << 1));
            if (widened >= QueryStoreBackfillState.MaxSliceSpan)
            {
                _entries.Remove(serverId);
            }
            else
            {
                _entries[serverId] = (widened, 0);
            }
        }
    }
}

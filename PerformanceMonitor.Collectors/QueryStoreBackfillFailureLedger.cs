/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Concurrent;
using System.Threading;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The in-memory count of consecutive failed Query Store backfill slices per (server, database), shared by
/// both products' workers so they skip a failing database by the same rule. In memory on purpose, like the
/// live path's counters: a restart forgets it and costs at most <see cref="QueryStoreBackfillState.SkipAfterConsecutiveSliceFailures"/>
/// more failed slices. Entries are removed by a completed slice, so the map holds only databases that are
/// failing right now.
///
/// <para>The count has one job: once it reaches <see cref="QueryStoreBackfillState.SkipAfterConsecutiveSliceFailures"/>
/// the loop serves the databases behind that database first. It does NOT size the slice window. That is each
/// worker's own per-server failure count (<see cref="QueryStoreBackfillState.AdaptiveSpan"/>), kept as it was,
/// because a command timeout usually means the whole server is loaded and narrowing per database would only add
/// timed-out queries against it. Each failure also takes a ticket from a running sequence so the skipped
/// databases can be retried in turn, least recently failed first; without it the first skipped database in the
/// list would starve every other skipped one, the same stall one level down.</para>
/// </summary>
public sealed class QueryStoreBackfillFailureLedger
{
    private readonly ConcurrentDictionary<(int ServerId, string Database), (int Failures, long Ticket)> _entries = new();
    private long _ticket;

    /// <summary>Consecutive failed slices for the database; 0 when none (or it last completed). Feeds the skip
    /// decision only, never the window size.</summary>
    public int Failures(int serverId, string database)
        => _entries.TryGetValue((serverId, database), out var entry) ? entry.Failures : 0;

    /// <summary>True once the database has failed <see cref="QueryStoreBackfillState.SkipAfterConsecutiveSliceFailures"/> slices in a row.</summary>
    public bool IsSkipped(int serverId, string database)
        => Failures(serverId, database) >= QueryStoreBackfillState.SkipAfterConsecutiveSliceFailures;

    /// <summary>Counts one failed slice and returns the new consecutive count.</summary>
    public int RecordFailure(int serverId, string database)
    {
        var ticket = Interlocked.Increment(ref _ticket);
        return _entries.AddOrUpdate(
            (serverId, database),
            (1, ticket),
            (_, current) => (current.Failures + 1, ticket)).Failures;
    }

    /// <summary>A completed slice clears the database's count.</summary>
    public void RecordCompletion(int serverId, string database)
        => _entries.TryRemove((serverId, database), out _);

    /// <summary>The ticket of the database's latest failure; smaller = failed longer ago, 0 = never.</summary>
    public long LastFailureTicket(int serverId, string database)
        => _entries.TryGetValue((serverId, database), out var entry) ? entry.Ticket : 0;
}

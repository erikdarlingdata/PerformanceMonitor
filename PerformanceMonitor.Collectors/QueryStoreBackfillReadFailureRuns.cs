/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Concurrent;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Which of the Query Store backfill's own store reads (the candidate list for a server, the stored floor for a
/// server and database) are in the middle of a run of failures, shared by both products' workers so they log a
/// failed read the same way (#4772). Both reads turn an error into "no work": a failed candidate read
/// drops every hole and tail on the server for that tick, a failed floor read drops that one database. They used
/// to say so only at Debug, which the default Information level drops, so the tails and outage holes stopped
/// filling and nothing said why.
///
/// <para>The signal is one Warning at the first failure of a run, and the repeats stay at Debug so a store
/// that is down does not write a line per tick. A read that completes ends the run, so the next failure is a
/// new run and warns again. It is the once-per-run rule <see cref="QueryStoreBackfillFailureLedger"/> applies to
/// failed slices, without the count: nothing here acts on how many reads failed. In memory on purpose, like the
/// ledger: a restart forgets it and costs at most one repeated Warning.</para>
/// </summary>
public sealed class QueryStoreBackfillReadFailureRuns
{
    /// <summary>The key of a read that is not tied to one database (the candidate list). A database name can never
    /// be empty, so it cannot collide with a per-database read.</summary>
    private const string ServerWide = "";

    private readonly ConcurrentDictionary<(int ServerId, string Database), byte> _failing = new();

    /// <summary>Notes a failed server-wide read (the candidate list). True when it is the first failure of a run,
    /// which is the one to log at Warning; false while the run continues.</summary>
    public bool RecordFailure(int serverId) => RecordFailure(serverId, ServerWide);

    /// <summary>Notes a failed per-database read (the stored floor). True when it is the first failure of a run
    /// for that database, which is the one to log at Warning; false while the run continues.</summary>
    public bool RecordFailure(int serverId, string database) => _failing.TryAdd((serverId, database), 0);

    /// <summary>A server-wide read that completed ends its run, so the next failure warns again.</summary>
    public void RecordSuccess(int serverId) => RecordSuccess(serverId, ServerWide);

    /// <summary>A per-database read that completed ends that database's run, so its next failure warns again.</summary>
    public void RecordSuccess(int serverId, string database) => _failing.TryRemove((serverId, database), out _);
}

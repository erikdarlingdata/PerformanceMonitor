/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The in-memory sightings buffer (#5985, part of #5971): what the collectors append to on the per-batch write path, so
/// that path makes no store round trip. One buffer per service, shared by every collector and server, under one lock.
///
/// <para><b>Append is memory only.</b> Each sighting is folded into the entry for its stored identity (kind, key,
/// application, login, host, database): <c>seen_count</c> is summed, the first text sample is kept and cut to
/// <see cref="QueryTagSightingWriter.TextSampleChars"/> as it is buffered. A key sighted ten thousand times in a minute is
/// one entry.</para>
///
/// <para><b>The map step flushes.</b> <see cref="QueryTagMapBuilder.RunMapPassAsync"/> calls <see cref="FlushAsync"/>
/// first, in one upsert, before it reads the changed keys, so the same pass tags what the last minute sighted.</para>
///
/// <para><b>Bounded.</b> A buffer at <see cref="SightingBufferCap"/> entries flushes at the next append instead of
/// growing. The cap is a memory bound, not a rate: the entries are distinct identities, and a minute of a large fleet is
/// a few thousand of them.</para>
///
/// <para><b>A failed flush drops its entries; a cancelled one keeps them.</b> A store failure is logged and the entries are
/// gone: a sighting lost is tagged a minute later, when its query is sighted again. There is no retry and a failed flush never
/// fails a collection cycle or a map pass. A flush cancelled by a stop puts what it had not written back, and the worker's
/// shutdown drain makes one last flush (<see cref="FlushOnShutdownAsync"/>), so a query that ran only in the last minute is
/// still sighted.</para>
/// </summary>
public sealed class QueryTagSightingBuffer
{
    /// <summary>
    /// The most distinct sightings held before the next append flushes. The worst case is this cap times
    /// <see cref="QueryTagSightingWriter.TextSampleChars"/>: 10,000 entries of 4,000 characters is about 80 MB of UTF-16,
    /// and that is only reached when the map step has stalled, since it flushes every minute anyway. A minute of a large fleet
    /// is a few thousand distinct entries (a distinct entry is a distinct query and caller, and query_stats alone names a few
    /// thousand a minute), so in normal running the cap is not reached, and when it is, it bounds memory rather than ending
    /// the sightings.
    /// </summary>
    public const int SightingBufferCap = 10_000;

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;
    private readonly int _cap;
    private readonly object _gate = new();
    private Dictionary<int, QueryTagSightingWriter.SightingSet> _byServer = new();
    private int _entries;

    public QueryTagSightingBuffer(NpgsqlDataSource postgres, ILogger? logger = null, int cap = SightingBufferCap)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
        _cap = cap > 0 ? cap : SightingBufferCap;
    }

    /// <summary>How many distinct sightings are waiting to be flushed.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries;
            }
        }
    }

    /// <summary>
    /// Adds a batch's sightings for one server. Memory only, and a no-op for an empty list. When the buffer already holds
    /// <see cref="SightingBufferCap"/> entries it flushes first, which is the only time this touches the store.
    /// </summary>
    public async Task AppendAsync(int serverId, IReadOnlyList<QueryTagSighting> sightings, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sightings);
        if (sightings.Count == 0)
        {
            return;
        }

        if (Count >= _cap)
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        lock (_gate)
        {
            if (!_byServer.TryGetValue(serverId, out var set))
            {
                set = new QueryTagSightingWriter.SightingSet(serverId);
                _byServer[serverId] = set;
            }

            int before = set.Count;
            foreach (var sighting in sightings)
            {
                set.Add(sighting);
            }

            _entries += set.Count - before;
        }
    }

    /// <summary>
    /// Writes what is buffered and empties the buffer. Opens no connection when nothing is buffered. Each flush takes the
    /// entries it writes out of the buffer under the lock, so two flushes (the map step's and a cap-triggered one) never write
    /// the same entry twice, and it upserts them in the seven-part identity order, so two flushes that overlap on a key lock its
    /// row in the same order and never wait on each other.
    ///
    /// <para>A store failure does not throw: the entries are dropped and the failure logged. A cancellation does throw, and the
    /// entries not yet written go back into the buffer first (counts summed into any entry sighted since, the first sample
    /// kept), so a flush cut short by a stop leaves them for the next flush.</para>
    /// </summary>
    /// <returns>How many distinct sightings were written.</returns>
    public async Task<int> FlushAsync(CancellationToken cancellationToken)
    {
        List<QueryTagSightingWriter.CollapsedSighting> rows;
        lock (_gate)
        {
            if (_entries == 0)
            {
                return 0;
            }

            rows = new List<QueryTagSightingWriter.CollapsedSighting>(_entries);
            foreach (var set in _byServer.Values)
            {
                rows.AddRange(set.Items);
            }

            _byServer = new Dictionary<int, QueryTagSightingWriter.SightingSet>();
            _entries = 0;
        }

        QueryTagSightingWriter.SortByIdentity(rows);
        var progress = new QueryTagSightingWriter.WriteProgress();
        try
        {
            return await QueryTagSightingWriter.WriteAsync(_postgres, rows, _logger, cancellationToken, progress).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            PutBack(rows, progress.Written);
            throw;
        }
    }

    /// <summary>
    /// The longest the shutdown flush may take. The host allows a stop 30 seconds in all and the in-flight collection bodies
    /// have already used up to 15 of them, so this is one short statement's worth (a flush is a few thousand rows in one upsert):
    /// long enough to land a normal buffer, short enough that a store that has stopped answering cannot hold the stop open.
    /// </summary>
    public static readonly TimeSpan ShutdownFlushLimit = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The last flush before the service stops (#5985): writes what is buffered, with its own time limit because the service's
    /// token is already cancelled. A flush that runs out of time keeps its rows in the buffer, which a stopping process then
    /// loses; this never throws.
    /// </summary>
    /// <returns>How many distinct sightings were written.</returns>
    public async Task<int> FlushOnShutdownAsync(TimeSpan limit)
    {
        using var timeout = new CancellationTokenSource(limit);
        try
        {
            return await FlushAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            _logger?.LogWarning("Query-tag sightings: the shutdown flush did not finish within {Limit}; the sightings still buffered are lost with the process", limit);
            return 0;
        }
    }

    private void PutBack(List<QueryTagSightingWriter.CollapsedSighting> rows, int written)
    {
        lock (_gate)
        {
            for (int i = written; i < rows.Count; i++)
            {
                var row = rows[i];
                if (!_byServer.TryGetValue(row.ServerId, out var set))
                {
                    set = new QueryTagSightingWriter.SightingSet(row.ServerId);
                    _byServer[row.ServerId] = set;
                }

                int before = set.Count;
                set.Merge(row);
                _entries += set.Count - before;
            }
        }
    }
}

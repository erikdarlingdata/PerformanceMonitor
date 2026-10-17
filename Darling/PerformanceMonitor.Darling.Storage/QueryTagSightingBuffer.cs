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
/// <para><b>A failed flush drops its entries.</b> They are logged and gone: a sighting lost is tagged a minute later,
/// when its query is sighted again. There is no retry and a failed flush never fails a collection cycle or a map pass.</para>
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
    /// Writes what is buffered in one upsert and empties the buffer. Never throws on a store failure: the entries are
    /// dropped and the failure logged. Opens no connection when nothing is buffered. Each flush takes the entries it writes out of the
    /// buffer under the lock, so two flushes (the map step's and a cap-triggered one) never write the same entry twice; a rare overlap on
    /// one key can deadlock the two statements, and the loser is a logged, dropped flush like any other failure.
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

        return await QueryTagSightingWriter.WriteAsync(_postgres, rows, _logger, cancellationToken).ConfigureAwait(false);
    }
}

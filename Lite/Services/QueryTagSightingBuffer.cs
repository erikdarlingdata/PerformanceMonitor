/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #5990: the in-memory buffer between the collectors and <c>query_tag_sightings</c>. A collection batch only appends here,
/// so the per-batch path makes no store write. The buffer is keyed by the seven-part identity (server, key kind, key,
/// database, application, login, host): <c>seen_count</c> is summed, the first text sample is kept (cut to
/// <see cref="QueryTagLimits.TextSampleCap"/> characters by <see cref="QueryTagLimits.CutText"/>) and <c>last_seen</c> is the latest. The map pass flushes it in ONE upsert before it
/// reads changed keys. A buffer holding <see cref="MaxEntries"/> entries flushes at the next append. A flush takes the WRITE lock (an
/// upsert can collide with another writer, DuckDbInitializer's lock rule) and opens its own connection. A flush that is
/// cancelled, or cannot get the lock in time, writes nothing and leaves its entries buffered. A write that fails is logged
/// and its entries are dropped, with no retry: the keys are tagged a pass later, when they are sighted again.
/// </summary>
internal sealed class QueryTagSightingBuffer
{
    /* Darling's SightingBufferCap. */
    internal const int MaxEntries = 10_000;

    private readonly object _gate = new();
    private Dictionary<Identity, Entry> _entries = new();

    private readonly record struct Identity(int ServerId, QueryTagKeyKind Kind, string Key, string Database, string App, string Login, string Host);

    private sealed class Entry(string objectName, string sample, DateTime now)
    {
        public string ObjectName = objectName;
        public string Sample = sample;
        public long Count;
        public DateTime FirstSeen = now;
    }

    internal int Count
    {
        get { lock (_gate) { return _entries.Count; } }
    }

    internal void Append<TRow>(ICollectorDefinition<TRow> definition, List<TRow> rows, int serverId, DuckDbInitializer? overflowDb = null)
    {
        try
        {
            if (overflowDb is not null && Count >= MaxEntries)
            {
                Flush(overflowDb);
            }

            var now = DateTime.UtcNow;
            foreach (var row in rows)
            {
                var sightings = definition.DescribeQueryTagSightings(row);
                if (sightings is null)
                {
                    continue;
                }

                lock (_gate)
                {
                    foreach (var s in sightings)
                    {
                        var id = new Identity(serverId, s.KeyKind, s.Key, s.Database ?? string.Empty, s.Application ?? string.Empty,
                            s.Login ?? string.Empty, s.Host ?? string.Empty);
                        if (!_entries.TryGetValue(id, out var entry))
                        {
                            var sample = QueryTagLimits.CutText(s.TextSample) ?? string.Empty;
                            entry = new Entry(s.ObjectRef?.Qualified ?? string.Empty, sample, now);
                            _entries[id] = entry;
                        }

                        entry.Count++;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Warn("QueryTags", $"Buffering the query-tag sightings failed; the collection is unaffected: {ex.Message}");
        }
    }

    /// <summary>
    /// Writes the buffered sightings in one upsert under the write lock and empties the buffer. A cancelled token, or a write
    /// lock not free within <paramref name="lockBudget"/> (default <see cref="LocalDataService.WriteLockBudget"/>), writes
    /// nothing and leaves the entries buffered. A failed write is logged and its entries are dropped.
    /// </summary>
    internal void Flush(DuckDbInitializer duckDb, TimeSpan? lockBudget = null, CancellationToken cancellationToken = default)
    {
        if (Count == 0)
        {
            return;
        }

        if (SchemaHeldBack(duckDb))
        {
            return;
        }

        IDisposable writeLock;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            writeLock = duckDb.AcquireWriteLock(timeout: lockBudget ?? LocalDataService.WriteLockBudget);
        }
        catch (Exception ex) when (ex is OperationCanceledException or TimeoutException)
        {
            AppLogger.Debug("QueryTags", $"The query-tag sightings flush did not run ({ex.GetType().Name}); its entries stay buffered");
            return;
        }

        using (writeLock)
        {
            Dictionary<Identity, Entry> taken;
            lock (_gate)
            {
                if (cancellationToken.IsCancellationRequested || _entries.Count == 0)
                {
                    return;
                }

                taken = _entries;
                _entries = new Dictionary<Identity, Entry>();
            }

            WriteTaken(duckDb, taken);
        }
    }

    /// <summary>How many times the held-back-schema warning was logged by this buffer (it logs once).</summary>
    internal int HeldBackWarningCount { get; private set; }

    /// <summary>
    /// While the v71 migration is held back (the old six-part table is still in place) the upsert cannot work. The buffered
    /// entries are dropped and ONE warning is logged, not one per round; the next start retries the migration.
    /// </summary>
    internal bool SchemaHeldBack(DuckDbInitializer duckDb)
    {
        if (!duckDb.QueryTagSchemaHeldBack)
        {
            return false;
        }

        lock (_gate)
        {
            _entries = new Dictionary<Identity, Entry>();
            if (HeldBackWarningCount == 0)
            {
                HeldBackWarningCount++;
                AppLogger.Warn("QueryTags", "The query-tag sightings table could not be upgraded (the schema is held back at v70); sightings are not recorded and the tag map is not built until the next start retries the upgrade");
            }
        }

        return true;
    }

    private static void WriteTaken(DuckDbInitializer duckDb, Dictionary<Identity, Entry> taken)
    {
        try
        {
            /* One order for every flush (the seven-part identity), as Darling sorts its rows; Lite's single writer under the
               write lock cannot deadlock, the sort keeps the two stores alike. */
            /* last_seen is stamped HERE, at the write, from the C# UTC clock: the map pass captures its cursor clock from the
               same clock (DateTime.UtcNow, plain TIMESTAMP), so a row kept by an earlier timed-out flush is never behind a cursor. */
            var flushedAt = DateTime.UtcNow;
            var list = taken.OrderBy(e => e.Key.ServerId).ThenBy(e => e.Key.Kind)
                .ThenBy(e => e.Key.Key, StringComparer.Ordinal).ThenBy(e => e.Key.Database, StringComparer.Ordinal)
                .ThenBy(e => e.Key.App, StringComparer.Ordinal).ThenBy(e => e.Key.Login, StringComparer.Ordinal)
                .ThenBy(e => e.Key.Host, StringComparer.Ordinal).ToList();
            using var connection = duckDb.CreateConnection();
            connection.Open();
            using var command = connection.CreateCommand();
            /* Empty text stands for NULL in the lists (a list parameter cannot be relied on to carry NULL elements); the
               timestamps are explicit UTC, never now(), which is local time. */
            command.CommandText = @"
INSERT INTO query_tag_sightings
    (server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample,
     first_seen, last_seen, seen_count)
SELECT sv, k, c_key, d, NULLIF(o, ''), a, l, h, NULLIF(t, ''), f, ls, n
FROM (SELECT UNNEST($1) AS sv, UNNEST($2) AS k, UNNEST($3) AS c_key, UNNEST($4) AS d, UNNEST($5) AS o, UNNEST($6) AS a,
             UNNEST($7) AS l, UNNEST($8) AS h, UNNEST($9) AS t, UNNEST($10) AS f, UNNEST($11) AS ls, UNNEST($12) AS n)
ON CONFLICT (server_id, key_kind, key, database_name, application_name, login_name, host_name) DO UPDATE SET
    seen_count = query_tag_sightings.seen_count + excluded.seen_count,
    last_seen = GREATEST(query_tag_sightings.last_seen, excluded.last_seen)";
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Key.ServerId).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => KindText(e.Key.Kind)).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Key.Key).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Key.Database).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Value.ObjectName).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Key.App).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Key.Login).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Key.Host).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Value.Sample).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Value.FirstSeen).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(_ => flushedAt).ToList() });
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Value.Count).ToList() });
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            AppLogger.Warn("QueryTags", $"The query-tag sightings flush failed and its {taken.Count} entries were dropped; the keys are tagged a pass later: {ex.Message}");
        }
    }

    internal static string KindText(QueryTagKeyKind kind) => kind switch
    {
        QueryTagKeyKind.Hash => "hash",
        QueryTagKeyKind.Object => "object",
        _ => "caller",
    };
}

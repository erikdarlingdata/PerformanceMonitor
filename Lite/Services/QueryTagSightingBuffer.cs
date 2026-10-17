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
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #5990: the in-memory buffer between the collectors and <c>query_tag_sightings</c>. A collection batch only appends here,
/// so the per-batch path makes no store write. The buffer is keyed by the seven-part identity (server, key kind, key,
/// database, application, login, host): <c>seen_count</c> is summed, the first text sample is kept (cut to
/// <see cref="SampleCap"/> characters) and <c>last_seen</c> is the latest. The map pass flushes it in ONE upsert before it
/// reads changed keys. A buffer over <see cref="MaxEntries"/> entries flushes at the next append. A failed flush is logged
/// and its entries are dropped, with no retry: the keys are tagged a pass later, when they are sighted again.
/// </summary>
internal sealed class QueryTagSightingBuffer
{
    internal const int SampleCap = 500;
    internal const int MaxEntries = 50_000;

    private readonly object _gate = new();
    private Dictionary<Identity, Entry> _entries = new();

    private readonly record struct Identity(int ServerId, QueryTagKeyKind Kind, string Key, string Database, string App, string Login, string Host);

    private sealed class Entry(string objectName, string sample, DateTime now)
    {
        public string ObjectName = objectName;
        public string Sample = sample;
        public long Count;
        public DateTime FirstSeen = now;
        public DateTime LastSeen = now;
    }

    internal int Count
    {
        get { lock (_gate) { return _entries.Count; } }
    }

    internal void Append<TRow>(ICollectorDefinition<TRow> definition, List<TRow> rows, int serverId, DuckDBConnection? overflowConnection = null)
    {
        try
        {
            if (overflowConnection is not null && Count > MaxEntries)
            {
                Flush(overflowConnection);
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
                            entry = new Entry(s.ObjectRef?.Qualified ?? string.Empty, sample.Length > SampleCap ? sample[..SampleCap] : sample, now);
                            _entries[id] = entry;
                        }

                        entry.Count++;
                        entry.LastSeen = now;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Warn("QueryTags", $"Buffering the query-tag sightings failed; the collection is unaffected: {ex.Message}");
        }
    }

    /// <summary>Writes the buffered sightings in one upsert and empties the buffer. A failure is logged and the entries are dropped.</summary>
    internal void Flush(DuckDBConnection connection)
    {
        Dictionary<Identity, Entry> taken;
        lock (_gate)
        {
            if (_entries.Count == 0)
            {
                return;
            }

            taken = _entries;
            _entries = new Dictionary<Identity, Entry>();
        }

        try
        {
            var list = taken.ToList();
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
            command.Parameters.Add(new DuckDBParameter { Value = list.Select(e => e.Value.LastSeen).ToList() });
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

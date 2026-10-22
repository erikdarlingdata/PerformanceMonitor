/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The ship guard for look-back reads (#6254, #6279): per database, the key -> (<c>last_execution_time</c>,
/// <c>count_executions</c>) of rows already shipped. A look-back read re-reads rows it already wrote, and after a
/// primary read a replica read can write an OLDER state of the same row with a NEWER <c>collection_time</c>, which
/// the readers prefer (they keep the newest row per key by (collection_time, execution_count)). The guard keeps
/// those rows out: a row ships only if its key is new or its state is newer.
///
/// <para><b>The key</b> is the readers' newest-row key: Lite's <c>LocalDataService.QueryStore.cs</c> partitions
/// on (database, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc,
/// replica_role) and Darling's <c>QueryStoreIntervalLatest</c> on the same columns minus execution_type_desc.
/// The guard uses the finer, Lite key, so a row the Darling reader would show is never dropped on a coarser
/// match.</para>
///
/// <para><b>Lifecycle.</b> <see cref="Filter"/> stages the batch's entries; <see cref="Commit"/> promotes them
/// only after the database's write succeeded (<see cref="Discard"/> on a failed write, so the rows ship next
/// run), then evicts every entry with <c>last_execution_time &lt;= W - L</c>: every later cutoff is at least
/// W - L under a strict <c>&gt;</c>, so an evicted key cannot come back. L is the LARGEST look-back used for the
/// database in this process, because it can grow (a capturing secondary appears, F changes). A database's map
/// is dropped when it leaves the enumeration (<see cref="RetainDatabases"/>) or its server is removed
/// (<see cref="Clear"/>). Backfill rows feed it (<see cref="RecordShipped"/>).</para>
///
/// <para>Also holds the per-database "last read was cut" marker (<see cref="MarkCut"/>) the cutoff needs. Memory
/// only: after a restart the guard is empty, one read writes its window again, and an open interval can show a
/// lower count for one flush at most. One instance serves one server; the lock makes it safe for the one runner
/// that works that server at a time, and for a stray overlapping call.</para>
/// </summary>
public sealed class QueryStoreShipGuard
{
    /// <summary>The readers' newest-row key (without database, which is the map's).</summary>
    public readonly record struct Key(
        long? RuntimeStatsIntervalId,
        long PlanId,
        long QueryId,
        string? ReplicaRole,
        DateTime? FirstExecutionTime,
        string? ExecutionTypeDesc);

    private readonly record struct State(DateTime? LastExecutionTime, long ExecutionCount);

    private sealed class DatabaseMap
    {
        public readonly Dictionary<Key, State> Shipped = new();
        public Dictionary<Key, State>? Staged;
        public DateTime? Watermark;
        public TimeSpan LargestLookBack;
        public bool LastReadWasCut;
    }

    private readonly object _gate = new();
    private readonly Dictionary<string, DatabaseMap> _databases = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The guard's key for a row.</summary>
    public static Key KeyOf(QueryStoreCollector.Row row)
    {
        ArgumentNullException.ThrowIfNull(row);
        return new Key(
            row.RuntimeStatsIntervalId,
            row.PlanId,
            row.QueryId,
            row.ReplicaRole,
            row.FirstExecutionTime,
            row.ExecutionTypeDesc);
    }

    /// <summary>
    /// Returns the rows that ship (key new, or state newer than the shipped one) and STAGES their entries. Staging
    /// replaces anything an earlier call for the database left uncommitted. Nothing is shipped-state until
    /// <see cref="Commit"/>.
    /// </summary>
    public List<QueryStoreCollector.Row> Filter(string database, IReadOnlyList<QueryStoreCollector.Row> rows)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(rows);

        lock (_gate)
        {
            var map = MapFor(database);
            var staged = new Dictionary<Key, State>();
            var ship = new List<QueryStoreCollector.Row>(rows.Count);
            foreach (var row in rows)
            {
                var key = KeyOf(row);
                var state = new State(row.LastExecutionTime, row.ExecutionCount);

                /* Newer than what is shipped, and newer than what this very batch already staged for the key. */
                if (map.Shipped.TryGetValue(key, out var shipped) && !IsNewer(state, shipped))
                {
                    continue;
                }

                if (staged.TryGetValue(key, out var inBatch) && !IsNewer(state, inBatch))
                {
                    continue;
                }

                staged[key] = state;
                ship.Add(row);
            }

            map.Staged = staged;
            return ship;
        }
    }

    /// <summary>
    /// Promotes the staged entries after the database's write succeeded, raises the database's watermark to
    /// <paramref name="watermark"/> (it only rises), and evicts entries at or below W - L with the database's own
    /// largest L. <paramref name="lookBack"/> is this read's L.
    /// </summary>
    public void Commit(string database, DateTime? watermark, TimeSpan lookBack)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            var map = MapFor(database);
            if (map.Staged is { } staged)
            {
                foreach (var (key, state) in staged)
                {
                    map.Shipped[key] = state;
                }

                map.Staged = null;
            }

            if (lookBack > map.LargestLookBack)
            {
                map.LargestLookBack = lookBack;
            }

            if (watermark is DateTime w && (map.Watermark is null || w > map.Watermark))
            {
                map.Watermark = w;
            }

            Evict(map);
        }
    }

    /// <summary>A failed write: the staged entries are dropped, so the rows ship again next run.</summary>
    public void Discard(string database)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            if (_databases.TryGetValue(database, out var map))
            {
                map.Staged = null;
            }
        }
    }

    /// <summary>
    /// Backfill rows feed the guard once their write succeeded: a later look-back read that meets the same key
    /// with the same or an older state then drops it. Entries older than the database's eviction line go at the
    /// next <see cref="Commit"/>.
    /// </summary>
    public void RecordShipped(string database, IReadOnlyList<QueryStoreCollector.Row> rows)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(rows);

        lock (_gate)
        {
            var map = MapFor(database);
            foreach (var row in rows)
            {
                var key = KeyOf(row);
                var state = new State(row.LastExecutionTime, row.ExecutionCount);
                if (!map.Shipped.TryGetValue(key, out var shipped) || IsNewer(state, shipped))
                {
                    map.Shipped[key] = state;
                }
            }
        }
    }

    /// <summary>Whether the database's last read was cut by the row cap or byte budget (the next cutoff is then W, not W - L).</summary>
    public bool WasCut(string database)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            return _databases.TryGetValue(database, out var map) && map.LastReadWasCut;
        }
    }

    /// <summary>Records whether the database's last read was cut. Call it after the read's write succeeded.</summary>
    public void MarkCut(string database, bool cut)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            MapFor(database).LastReadWasCut = cut;
        }
    }

    /// <summary>Drops the maps of every database not in <paramref name="enumerated"/> (it left the enumeration).</summary>
    public void RetainDatabases(IEnumerable<string> enumerated)
    {
        ArgumentNullException.ThrowIfNull(enumerated);

        lock (_gate)
        {
            var keep = new HashSet<string>(enumerated, StringComparer.OrdinalIgnoreCase);
            foreach (var name in _databases.Keys.Where(n => !keep.Contains(n)).ToList())
            {
                _databases.Remove(name);
            }
        }
    }

    /// <summary>Drops everything: the server was removed.</summary>
    public void Clear()
    {
        lock (_gate)
        {
            _databases.Clear();
        }
    }

    /// <summary>Entries held for a database (shipped, not staged); 0 when it has no map.</summary>
    public int EntryCount(string database)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            return _databases.TryGetValue(database, out var map) ? map.Shipped.Count : 0;
        }
    }

    /// <summary>Whether the guard still holds a map for the database.</summary>
    public bool HasDatabase(string database)
    {
        ArgumentNullException.ThrowIfNull(database);

        lock (_gate)
        {
            return _databases.ContainsKey(database);
        }
    }

    private DatabaseMap MapFor(string database)
    {
        if (!_databases.TryGetValue(database, out var map))
        {
            map = new DatabaseMap();
            _databases[database] = map;
        }

        return map;
    }

    private static void Evict(DatabaseMap map)
    {
        if (map.Watermark is not DateTime w || map.LargestLookBack <= TimeSpan.Zero)
        {
            return;
        }

        var line = w - map.LargestLookBack;
        foreach (var key in map.Shipped.Where(e => e.Value.LastExecutionTime is not DateTime t || t <= line)
                     .Select(e => e.Key)
                     .ToList())
        {
            map.Shipped.Remove(key);
        }
    }

    /// <summary>Newer by time, or the same time with a higher count. A null time is the oldest.</summary>
    private static bool IsNewer(State candidate, State shipped)
    {
        var c = Nullable.Compare(candidate.LastExecutionTime, shipped.LastExecutionTime);
        return c > 0 || (c == 0 && candidate.ExecutionCount > shipped.ExecutionCount);
    }
}

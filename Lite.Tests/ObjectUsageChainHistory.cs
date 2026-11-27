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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A hand-built history of daily <c>index_object_stats</c> snapshots for one database (#9596), shared by the chain tests. Fixed numbers, no
/// randomness. One instance answers (NODE1) except for four days of a failover to a second one (NODE2). The history carries each kind of step
/// the stepper decides on: growth, two restarts, one index cleared on its own, a node that answers for the first time inside a window, a return
/// to a node already seen, an object that is never used, an object dropped, an object created late, and a 12 day gap longer than the
/// baseline's 8 day reach.
/// </summary>
internal static class ObjectUsageChainHistory
{
    /// <summary>"Now" for every test of the chain: the newest snapshot is this day.</summary>
    internal static readonly DateTime Now = new(2026, 11, 14, 12, 0, 0, DateTimeKind.Utc);

    internal const string Node1 = "NODE1";
    internal const string Node2 = "NODE2";

    internal static DateTime Day(int daysAgo) => DateTime.SpecifyKind(Now.AddDays(-daysAgo), DateTimeKind.Unspecified);

    /// <summary>The days the history has a snapshot on, oldest first: 100 days ago to today, without the gap (60 to 49 days ago).</summary>
    internal static IEnumerable<int> SnapshotDays()
    {
        for (var daysAgo = 100; daysAgo >= 0; daysAgo--)
        {
            if (daysAgo is <= 60 and >= 49)
            {
                continue;
            }

            yield return daysAgo;
        }
    }

    /// <summary>One stored index row. <paramref name="Node"/> is the stored <c>answering_server_name</c> and <paramref name="MemoryOptimized"/> the stored
    /// <c>is_memory_optimized</c>, either of which a store written before the column existed holds as null.</summary>
    internal sealed record Row(
        DateTime CollectionTime, DateTime Start, string? Node, int ObjectId, string Table, int IndexId, long Seeks, long Scans, long Lookups,
        long Updates, DateTime? LastSeek, DateTime? LastScan, DateTime? LastLookup, DateTime? LastUpdate, bool? MemoryOptimized = false);

    private sealed class Counters
    {
        public long Seeks, Scans, Lookups, Updates;
        public DateTime? LastSeek, LastScan, LastLookup, LastUpdate;
    }

    /// <summary>The history's rows, oldest snapshot first.</summary>
    internal static List<Row> Generate()
    {
        var rows = new List<Row>();
        var state = new Dictionary<(string Node, int ObjectId, int IndexId), Counters>();
        var starts = new Dictionary<string, DateTime>
        {
            [Node1] = new DateTime(2026, 1, 1, 3, 0, 0),
            [Node2] = Day(26).Date,
        };

        foreach (var daysAgo in SnapshotDays())
        {
            var time = Day(daysAgo);
            var node = daysAgo is <= 25 and >= 22 ? Node2 : Node1;

            /* Restarts of the first node: everything it holds goes back to nothing and its start time moves. */
            if (daysAgo is 70 or 10)
            {
                starts[Node1] = time.AddHours(-3);
                foreach (var key in new List<(string, int, int)>(state.Keys))
                {
                    if (key.Item1 == Node1)
                    {
                        state[key] = new Counters();
                    }
                }
            }

            /* One index cleared on its own: its counters and stamps go, nothing else moves. */
            if (daysAgo == 40)
            {
                state[(Node1, 6, 3)] = new Counters();
            }

            foreach (var (objectId, table, indexId) in Indexes())
            {
                var created = objectId == 5 ? 20 : int.MaxValue;
                if (objectId == 5 && daysAgo > created)
                {
                    continue;
                }

                if (objectId == 4 && daysAgo < 30)
                {
                    continue;
                }

                if (!state.TryGetValue((node, objectId, indexId), out var c))
                {
                    state[(node, objectId, indexId)] = c = new Counters();
                }

                Grow(c, objectId, indexId, daysAgo, time);
                rows.Add(new Row(
                    time, starts[node], node, objectId, table, indexId, c.Seeks, c.Scans, c.Lookups, c.Updates, c.LastSeek, c.LastScan, c.LastLookup,
                    c.LastUpdate));
            }
        }

        return rows;
    }

    /// <summary>Snapshots older than this many days hold no <c>answering_server_name</c>: the column was added after they were collected.</summary>
    internal const int NamedFromDaysAgo = 80;

    /// <summary>Snapshots older than this many days hold no <c>is_memory_optimized</c>.</summary>
    internal const int MemoryFlagFromDaysAgo = 66;

    /// <summary>
    /// <see cref="Generate"/> as a store upgraded through the two column additions holds it, which is every store written before them: the early
    /// snapshots carry a null <c>answering_server_name</c> (the first node is named for the first time at <see cref="NamedFromDaysAgo"/>) and a null
    /// <c>is_memory_optimized</c> (every index's identity changes from null to false at <see cref="MemoryFlagFromDaysAgo"/>, so every run closes and
    /// reopens).
    /// </summary>
    internal static List<Row> GenerateUpgraded() =>
        Generate()
            .Select(r => r with
            {
                Node = r.CollectionTime < Day(NamedFromDaysAgo) ? null : r.Node,
                MemoryOptimized = r.CollectionTime < Day(MemoryFlagFromDaysAgo) ? null : r.MemoryOptimized,
            })
            .ToList();

    private static IEnumerable<(int ObjectId, string Table, int IndexId)> Indexes()
    {
        yield return (1, "Orders", 1);
        yield return (1, "Orders", 2);
        yield return (2, "Customers", 1);
        yield return (3, "Audit", 0);
        yield return (4, "Legacy", 1);
        yield return (5, "Late", 1);
        yield return (6, "Wide", 1);
        yield return (6, "Wide", 2);
        yield return (6, "Wide", 3);
    }

    /// <summary>One day's use of one index on the node that answers. The Audit heap is never used. Legacy is used until 80 days ago.</summary>
    private static void Grow(Counters c, int objectId, int indexId, int daysAgo, DateTime time)
    {
        if (objectId == 3 || (objectId == 4 && daysAgo < 80))
        {
            return;
        }

        var day = (long)(daysAgo % 7);
        c.Seeks += (objectId * 3) + day + indexId;
        c.Lookups += indexId == 1 ? day : 0;
        c.Scans += objectId == 2 ? 2 : 0;
        c.Updates += indexId <= 1 ? 4 + day : 0;
        c.LastSeek = time.AddHours(-2);
        if (objectId == 2)
        {
            c.LastScan = time.AddHours(-3);
        }

        if (indexId == 1)
        {
            c.LastLookup = time.AddHours(-4);
            c.LastUpdate = time.AddHours(-1);
        }
    }

    private static long s_nextLogId = -1_000_000_000;

    /// <summary>
    /// Stores one finished <c>index_object_stats</c> collection run in <c>collection_log</c> for each start: stamped with the start (the same
    /// second as the rows) and a duration, the way the collector logs a run when it ends. The chain builder folds a snapshot only once such a
    /// row covers it.
    /// </summary>
    internal static async Task SeedLogAsync(DuckDbInitializer duckDb, int serverId, string serverName, IEnumerable<DateTime> starts)
    {
        using var writeLock = duckDb.AcquireWriteLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();
        foreach (var start in starts)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
                VALUES ($1, $2, $3, 'index_object_stats', $4, 2000, 'SUCCESS', 1)";
            cmd.Parameters.Add(new DuckDBParameter { Value = System.Threading.Interlocked.Decrement(ref s_nextLogId) });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
            cmd.Parameters.Add(new DuckDBParameter { Value = start });
            await cmd.ExecuteNonQueryAsync();
        }

        transaction.Commit();
    }

    /// <summary>
    /// Stores the rows for one server and database in <c>index_object_stats</c>, one transaction under the store's write lock, and one finished
    /// collection run in <c>collection_log</c> for each snapshot time unless <paramref name="logRuns"/> is false.
    /// </summary>
    internal static async Task SeedAsync(
        DuckDbInitializer duckDb, int serverId, string serverName, string database, IEnumerable<Row> rows, long firstId = -1, bool logRuns = true)
    {
        var materialized = rows as IReadOnlyCollection<Row> ?? rows.ToList();
        if (logRuns)
        {
            await SeedLogAsync(duckDb, serverId, serverName, materialized.Select(r => r.CollectionTime).Distinct().ToList());
        }

        rows = materialized;
        using var writeLock = duckDb.AcquireWriteLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();
        var id = firstId;
        foreach (var row in rows)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"INSERT INTO index_object_stats
                (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
                 schema_name, object_id, table_name, index_id, index_name, index_type_desc, user_seeks, user_scans, user_lookups, user_updates,
                 last_user_seek, last_user_scan, last_user_lookup, last_user_update, is_indexed_view, answering_server_name, is_memory_optimized)
                VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,$9,NULL,'CLUSTERED',$10,$11,$12,$13,$14,$15,$16,$17,false,$18,$19)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(id--); P(row.CollectionTime); P(serverId); P(serverName); P(row.Start); P(database); P(row.ObjectId); P(row.Table); P(row.IndexId);
            P(row.Seeks); P(row.Scans); P(row.Lookups); P(row.Updates); P(row.LastSeek); P(row.LastScan); P(row.LastLookup); P(row.LastUpdate); P(row.Node); P(row.MemoryOptimized);
            await cmd.ExecuteNonQueryAsync();
        }

        transaction.Commit();
    }
}

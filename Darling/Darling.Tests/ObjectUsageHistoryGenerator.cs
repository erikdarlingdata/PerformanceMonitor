/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace Darling.Tests;

/// <summary>What a generated history is meant to do to the persisted read (#9349 proof A).</summary>
internal enum HistoryTag
{
    /// <summary>The persisted read answers every window itself (apart from a start after the newest snapshot's reach).</summary>
    None,

    /// <summary>Two named nodes alternate, then four snapshots lose the name and the node that did not answer before them answers again:
    /// a window that starts in the unnamed stretch compares a snapshot the chain step did not, so it must fall back.</summary>
    UnnamedAfterNamed,

    /// <summary>The start time drifts 40 seconds a snapshot for nine snapshots: the chain keeps the first value and a window that starts
    /// mid-drift keeps another, so their restart decisions differ and the window must fall back.</summary>
    StartDrift,

    /// <summary>A database with no rows at all.</summary>
    Empty,
}

/// <summary>One generated <c>collect.index_object_stats</c> row.</summary>
internal sealed record GeneratedRow(
    DateTime Time, DateTime? Start, string? Node, int ObjectId, int IndexId, string Schema, string Name, string IndexType,
    long Seeks, long Scans, long Lookups, long Updates, DateTime? LastSeek, DateTime? LastScan, DateTime? LastLookup, DateTime? LastUpdate,
    bool IsView, bool? MemoryOptimized);

/// <summary>One database's generated history.</summary>
internal sealed class GeneratedHistory
{
    public required string Database { get; init; }
    public required HistoryTag Tag { get; init; }
    public required IReadOnlySet<string> Features { get; init; }
    public required IReadOnlyList<GeneratedRow> Rows { get; init; }

    /// <summary>The distinct snapshot times, oldest first.</summary>
    public required IReadOnlyList<DateTime> Times { get; init; }

    /// <summary>The index of the first snapshot after a gap longer than the baseline's reach, or -1.</summary>
    public int GapBefore { get; init; } = -1;
}

/// <summary>
/// A seeded generator of object usage histories for the #9349 equivalence proof. Same seed, same history. Each history is one database of one
/// server: 30 to 120 daily snapshots, 5 to 60 objects of 1 to 4 indexes, and a random choice of events (restart, database-wide clear, lone index
/// clear, cleared stamps, an index dropped and created again after a gap, a dropped object, a new object, a rename, duplicate rows, two named
/// nodes answering in turn, a failover, an unnamed prefix, a memory-optimized flag that gains a value, a collection gap, start jitter). History
/// <c>h</c> carries feature <c>h % features</c> for sure, so every feature appears in many histories.
/// </summary>
internal static class ObjectUsageHistoryGenerator
{
    public static readonly string[] FeatureNames =
    [
        "restart", "dbclear", "loneclear", "stampnull", "recreate", "dropobj", "newobj", "rename", "dupes", "alternate", "failover",
        "unnamedprefix", "memnull", "gap", "jitter", "hole",
    ];

    private sealed class Counters
    {
        public long Seeks, Scans, Lookups, Updates;
        public DateTime? LastSeek, LastScan, LastLookup, LastUpdate;
        public long Total => Seeks + Scans + Lookups + Updates;
    }

    private sealed class Node
    {
        public required string Name { get; init; }
        public DateTime BaseStart { get; set; }
        public bool Big { get; set; }
        public Dictionary<(int, int), Counters> State { get; } = new();
    }

    private sealed class Idx
    {
        public int Id;
        public string Type = "NONCLUSTERED";
        public bool Used;
        public bool UpdatesOnly;
        public int DropFrom = -1;
        public int DropTo = -1;
    }

    private sealed class Obj
    {
        public int Id;
        public string Schema = "dbo";
        public int Created;
        public int Dropped = int.MaxValue;
        public int RenameAt = -1;
        public bool IsView;
        public int MemFrom = -1;
        public bool MemValue;
        public List<Idx> Indexes = new();
    }

    public static string DatabaseName(int h) => $"gen{h:000}";

    public static GeneratedHistory Generate(int h)
    {
        var database = DatabaseName(h);
        var rng = new Random(934900 + h);
        var tag = h == 199 ? HistoryTag.Empty : h % 20 == 1 ? HistoryTag.UnnamedAfterNamed : h % 20 == 2 ? HistoryTag.StartDrift : HistoryTag.None;
        var features = new HashSet<string>();
        for (var k = 0; k < FeatureNames.Length; k++)
        {
            if (h % FeatureNames.Length == k || rng.NextDouble() < 0.25)
            {
                features.Add(FeatureNames[k]);
            }
        }

        if (tag == HistoryTag.Empty)
        {
            return new GeneratedHistory { Database = database, Tag = tag, Features = features, Rows = [], Times = [] };
        }

        if (tag == HistoryTag.UnnamedAfterNamed)
        {
            features.Remove("unnamedprefix");
            features.Remove("failover");
            features.Remove("restart");
            features.Add("alternate");
        }

        if (tag == HistoryTag.StartDrift)
        {
            foreach (var f in new[] { "restart", "jitter", "alternate", "failover", "unnamedprefix", "gap" })
            {
                features.Remove(f);
            }
        }

        if (features.Contains("alternate"))
        {
            features.Remove("failover");
            features.Remove("unnamedprefix");
        }

        var n = 30 + rng.Next(91);
        var t0 = new DateTime(2026, 6, 1, 2, 0, 0, DateTimeKind.Unspecified).AddSeconds(rng.Next(0, 3600)).AddTicks(rng.Next(0, 1_000_000) * 10L);
        var gapIdx = features.Contains("gap") ? 10 + rng.Next(n - 20) : -1;
        var times = new List<DateTime> { t0 };
        for (var i = 1; i < n; i++)
        {
            var step = TimeSpan.FromHours(24) + TimeSpan.FromMinutes(rng.Next(-90, 91)) + TimeSpan.FromTicks(rng.Next(0, 1000) * 10L);
            if (i == gapIdx)
            {
                step = TimeSpan.FromDays(10 + rng.Next(5)) + TimeSpan.FromMinutes(rng.Next(0, 90));
            }
            else if (features.Contains("hole") && rng.Next(15) == 0)
            {
                step += TimeSpan.FromDays(1);
            }

            times.Add(times[^1] + step);
        }

        int At(double low, double high) => (int)(n * (low + rng.NextDouble() * (high - low)));

        var objects = BuildCatalog(rng, h, n, features);

        /* Two nodes: alpha is the instance that has run since before the history, beta an older instance (alternate) or the failover target. */
        var alpha = new Node { Name = "alpha", BaseStart = t0.AddDays(-rng.Next(3, 60)).AddSeconds(rng.Next(0, 86400)), Big = true };
        var failoverAt = features.Contains("failover") ? Math.Max(At(0.3, 0.6), 7) : -1;
        var failBackAt = features.Contains("failover") && rng.Next(2) == 0 ? failoverAt + 5 + rng.Next(10) : -1;
        var betaInside = rng.Next(2) == 0;
        var beta = new Node
        {
            Name = "beta",
            Big = !betaInside,
            BaseStart = betaInside && failoverAt >= 0
                ? times[failoverAt].AddHours(-6).AddSeconds(rng.Next(0, 3600))
                : t0.AddDays(-rng.Next(40, 80)).AddSeconds(rng.Next(0, 86400)),
        };

        var restartAt = new HashSet<int>();
        if (features.Contains("restart"))
        {
            restartAt.Add(At(0.15, 0.5));
            if (rng.Next(2) == 0)
            {
                restartAt.Add(At(0.55, 0.9));
            }
        }

        var dbClearAt = features.Contains("dbclear") ? At(0.2, 0.9) : -1;
        var loneClearAt = features.Contains("loneclear") ? At(0.2, 0.9) : -1;
        var stampNullAt = features.Contains("stampnull") ? At(0.2, 0.9) : -1;
        var stampNullAll = rng.Next(2) == 0;
        var prefix = features.Contains("unnamedprefix") ? 1 + rng.Next(5) : 0;
        if (failoverAt >= 0 && failoverAt < prefix + 2)
        {
            failoverAt = prefix + 2;
        }

        var stretchStart = tag == HistoryTag.UnnamedAfterNamed ? n / 2 : -1;
        var driftStart = tag == HistoryTag.StartDrift ? n / 2 : -1;

        var rows = new List<GeneratedRow>();
        for (var i = 0; i < n; i++)
        {
            var time = times[i];

            /* Which node answers, and whether its name is stored. */
            Node node;
            var named = i >= prefix;
            if (features.Contains("alternate"))
            {
                var turn = i == 1 ? 1 : i == 0 ? 0 : rng.Next(2);
                if (tag == HistoryTag.UnnamedAfterNamed)
                {
                    turn = i % 2;
                    if (i >= stretchStart && i < stretchStart + 4)
                    {
                        turn = (stretchStart - 1) % 2;
                        named = false;
                    }
                }

                node = turn == 0 ? alpha : beta;
            }
            else if (failoverAt >= 0 && i >= failoverAt && (failBackAt < 0 || i < failBackAt))
            {
                node = beta;
            }
            else
            {
                node = alpha;
            }

            if (restartAt.Contains(i))
            {
                node.BaseStart = time.AddHours(-rng.Next(1, 20)).AddSeconds(-rng.Next(0, 3000));
                node.Big = false;
                node.State.Clear();
            }

            if (i == dbClearAt)
            {
                foreach (var c in node.State.Values.Where(c => c.Total > 0))
                {
                    Clear(c, rng);
                }
            }

            if (i == loneClearAt)
            {
                var used = node.State.Values.Where(c => c.Total > 30).ToList();
                if (used.Count >= 4)
                {
                    Clear(used[rng.Next(used.Count)], rng);
                }
            }

            if (i == stampNullAt)
            {
                var targets = node.State.Values.Where(c => c.LastSeek is not null || c.LastUpdate is not null).ToList();
                foreach (var c in stampNullAll ? targets : targets.Take(1))
                {
                    c.LastSeek = c.LastScan = c.LastLookup = c.LastUpdate = null;
                }
            }

            if (driftStart >= 0 && i >= driftStart && i < driftStart + 9)
            {
                node.BaseStart = node.BaseStart.AddSeconds(40);
            }

            var jitter = features.Contains("jitter") ? TimeSpan.FromTicks(rng.Next(-25_000, 25_001) * 10_000L) : TimeSpan.Zero;
            var start = node.BaseStart + jitter;
            var nodeName = named ? node.Name : null;

            foreach (var obj in objects)
            {
                if (i < obj.Created || i >= obj.Dropped)
                {
                    continue;
                }

                var name = obj.RenameAt >= 0 && i < obj.RenameAt ? $"T{obj.Id}_old" : $"T{obj.Id}";
                bool? memory = obj.MemFrom >= 0 && i < obj.MemFrom ? null : obj.MemValue;
                foreach (var idx in obj.Indexes)
                {
                    if (idx.DropFrom >= 0 && i >= idx.DropFrom && i < idx.DropTo)
                    {
                        if (i == idx.DropFrom)
                        {
                            alpha.State.Remove((obj.Id, idx.Id));
                            beta.State.Remove((obj.Id, idx.Id));
                        }

                        continue;
                    }

                    var key = (obj.Id, idx.Id);
                    if (!node.State.TryGetValue(key, out var c))
                    {
                        c = new Counters();
                        var big = node.Big && obj.Created == 0 && idx.DropTo < 0;
                        if (idx.Used && big)
                        {
                            c.Seeks = rng.Next(1000, 5_000_000);
                            c.Scans = rng.Next(0, 5000);
                            c.Lookups = rng.Next(0, 50_000);
                            c.Updates = rng.Next(0, 500_000);
                            c.LastSeek = time.AddHours(-rng.Next(1, 700));
                            c.LastLookup = time.AddHours(-rng.Next(1, 700));
                            c.LastUpdate = time.AddHours(-rng.Next(1, 700));
                        }

                        node.State[key] = c;
                    }

                    Advance(c, idx, time, rng);
                    rows.Add(new GeneratedRow(
                        time, start, nodeName, obj.Id, idx.Id, obj.Schema, name, idx.Id == 0 ? "HEAP" : idx.Type, c.Seeks, c.Scans, c.Lookups, c.Updates,
                        c.LastSeek, c.LastScan, c.LastLookup, c.LastUpdate, obj.IsView, memory));
                    if (features.Contains("dupes") && rng.Next(8) == 0)
                    {
                        rows.Add(rows[^1] with { Seeks = Math.Max(0, c.Seeks + rng.Next(-5, 40)) });
                    }
                }
            }
        }

        return new GeneratedHistory
        {
            Database = database,
            Tag = tag,
            Features = features,
            Rows = rows,
            Times = times,
            GapBefore = gapIdx,
        };
    }

    private static List<Obj> BuildCatalog(Random rng, int h, int n, HashSet<string> features)
    {
        var count = h % 5 == 0 ? 5 + rng.Next(56) : 5 + rng.Next(8);
        var objects = new List<Obj>();
        for (var k = 0; k < count; k++)
        {
            var obj = new Obj { Id = 1000 + k * 7, IsView = rng.Next(10) == 0 };
            var indexCount = 1 + rng.Next(4);
            for (var j = 0; j < indexCount; j++)
            {
                var first = j == 0;
                obj.Indexes.Add(new Idx
                {
                    Id = first ? (rng.Next(5) == 0 ? 0 : 1) : j + 1,
                    Type = first ? "CLUSTERED" : "NONCLUSTERED",
                    Used = k == 0 && first || rng.NextDouble() < (first ? 0.7 : 0.55),
                    UpdatesOnly = rng.Next(10) < 3,
                });
            }

            objects.Add(obj);
        }

        int Pick(Func<Obj, bool> ok)
        {
            for (var tries = 0; tries < 20; tries++)
            {
                var k = 1 + rng.Next(objects.Count - 1);
                if (ok(objects[k]))
                {
                    return k;
                }
            }

            return -1;
        }

        int At(double low, double high) => (int)(n * (low + rng.NextDouble() * (high - low)));

        if (features.Contains("dropobj"))
        {
            for (var t = 0; t < 1 + rng.Next(3); t++)
            {
                var k = Pick(o => o.Dropped == int.MaxValue && o.Created == 0);
                if (k >= 0)
                {
                    objects[k].Dropped = At(0.2, 0.8);
                }
            }
        }

        if (features.Contains("newobj"))
        {
            for (var t = 0; t < 1 + rng.Next(3); t++)
            {
                var k = Pick(o => o.Dropped == int.MaxValue && o.Created == 0);
                if (k >= 0)
                {
                    objects[k].Created = Math.Max(1, At(0.1, 0.7));
                }
            }
        }

        if (features.Contains("rename"))
        {
            var k = Pick(o => o.RenameAt < 0 && o.Created == 0);
            if (k >= 0)
            {
                objects[k].RenameAt = At(0.2, 0.8);
            }
        }

        if (features.Contains("recreate"))
        {
            var k = Pick(o => o.Indexes.Count > 1 && o.Created == 0 && o.Dropped == int.MaxValue);
            if (k >= 0)
            {
                var idx = objects[k].Indexes[1 + rng.Next(objects[k].Indexes.Count - 1)];
                idx.Used = true;
                idx.UpdatesOnly = false;
                idx.DropFrom = At(0.2, 0.7);
                idx.DropTo = idx.DropFrom + 1 + rng.Next(4);
            }
        }

        if (features.Contains("memnull"))
        {
            foreach (var o in objects)
            {
                if (rng.Next(10) < 3)
                {
                    o.MemFrom = At(0.2, 0.8);
                    o.MemValue = rng.Next(2) == 0;
                }
                else if (rng.Next(10) == 0)
                {
                    o.MemValue = true;
                }
            }
        }

        return objects;
    }

    /// <summary>Clears an index's counters to a small value below the old one, the way a cleared DMV starts over.</summary>
    private static void Clear(Counters c, Random rng)
    {
        static long Down(Random rng, long old) => old <= 0 ? 0 : rng.NextInt64(0, Math.Min(old, 10));
        c.Seeks = Down(rng, c.Seeks);
        c.Scans = Down(rng, c.Scans);
        c.Lookups = Down(rng, c.Lookups);
        c.Updates = Down(rng, c.Updates);
        c.LastSeek = c.LastScan = c.LastLookup = c.LastUpdate = null;
    }

    /// <summary>One day of use on one index: daily growth for a used index, writes only for some, nothing for a never-used one.</summary>
    private static void Advance(Counters c, Idx idx, DateTime time, Random rng)
    {
        DateTime Stamp() => time.AddMinutes(-rng.Next(1, 600)).AddTicks(-rng.Next(0, 1000) * 10L);

        if (idx.Used)
        {
            var seeks = rng.Next(0, 300);
            var scans = rng.Next(3) == 0 ? rng.Next(0, 15) : 0;
            var lookups = rng.Next(2) == 0 ? rng.Next(0, 80) : 0;
            var updates = rng.Next(0, 200);
            c.Seeks += seeks;
            c.Scans += scans;
            c.Lookups += lookups;
            c.Updates += updates;
            c.LastSeek = seeks > 0 ? Stamp() : c.LastSeek;
            c.LastScan = scans > 0 ? Stamp() : c.LastScan;
            c.LastLookup = lookups > 0 ? Stamp() : c.LastLookup;
            c.LastUpdate = updates > 0 ? Stamp() : c.LastUpdate;
        }
        else if (idx.UpdatesOnly)
        {
            var updates = rng.Next(0, 50);
            c.Updates += updates;
            c.LastUpdate = updates > 0 ? Stamp() : c.LastUpdate;
        }
    }

    /// <summary>Copies the history's rows into <c>collect.index_object_stats</c> with one COPY.</summary>
    public static async Task PlantAsync(NpgsqlConnection connection, int serverId, string serverName, IEnumerable<GeneratedHistory> histories, CancellationToken ct)
    {
        const string Columns =
            "collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, "
            + "index_id, index_type_desc, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup, "
            + "last_user_update, is_indexed_view, answering_server_name, is_memory_optimized";
        await using var writer = await connection.BeginTextImportAsync($"COPY collect.index_object_stats ({Columns}) FROM STDIN", ct);

        static string Time(DateTime? value) => value is { } v ? v.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) : "\\N";
        static string Flag(bool? value) => value is { } v ? (v ? "t" : "f") : "\\N";
        static string Text(string? value) => value ?? "\\N";
        static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

        foreach (var history in histories)
        {
            foreach (var r in history.Rows)
            {
                await writer.WriteLineAsync(string.Join(
                    '\t',
                    Number(CollectionIdGenerator.Next()), Time(r.Time), Number(serverId), serverName, Time(r.Start), history.Database, "7", r.Schema,
                    Number(r.ObjectId), r.Name, Number(r.IndexId), r.IndexType, Number(r.Seeks), Number(r.Scans), Number(r.Lookups), Number(r.Updates),
                    Time(r.LastSeek), Time(r.LastScan), Time(r.LastLookup), Time(r.LastUpdate), Flag(r.IsView), Text(r.Node), Flag(r.MemoryOptimized)));
            }
        }
    }
}

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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One cached per-database Query Store watermark. <see cref="Value"/> is the newest
/// <c>last_execution_time</c> over every row with <c>collection_time</c> above <see cref="SeedFloor"/>.
/// <see cref="Witness"/> is a LOWER bound on the <c>collection_time</c> of a row whose value equals
/// <see cref="Value"/>; null when that is not known. <see cref="SeededAt"/> is the run clock at the store read
/// that seeded the entry; <c>Advance</c> never moves it.
/// </summary>
internal readonly record struct DatabaseWatermarkEntry(DateTime? Value, DateTime? Witness, DateTime SeedFloor, DateTime SeededAt);

/// <summary>
/// What one item's batch would contribute to the cache, computed at read time and landed only after the
/// batch's COPY transaction commits.
/// </summary>
internal readonly record struct StagedDatabaseWatermark(DateTime? BatchMax, bool Foreign, DateTime CollectionTime);

/// <summary>
/// Exact per-(server, database) cache of the Query Store per-item watermark. On a hit it returns what the
/// bounded store read (<c>MAX(last_execution_time) WHERE collection_time &gt; floor</c>) would return; when
/// that cannot be proven it misses and the caller reads the store.
///
/// <para><b>Hit rule</b>, for this cycle's floor F:</para>
/// <list type="bullet">
/// <item>no entry: miss.</item>
/// <item>F below the seeding floor: miss (floors are monotonic, so this is a guard).</item>
/// <item>seeded at least <see cref="ReseedInterval"/> ago: miss, so the store is re-read at least hourly.</item>
/// <item>value null: hit, null. Every write since the seed went through <see cref="Advance"/>, and none carried a value.</item>
/// <item>value set and witness above F: hit, the value. The witness row is inside the read, so the read is at least the value, and every row inside the read is inside the seed's read, so none is above it.</item>
/// <item>value set and witness null or not above F: miss.</item>
/// </list>
///
/// <para><b>Assumption:</b> the only writers of <c>query_store_stats</c> are the runner's live path (which
/// advances this cache), the backfill (which invalidates it) and retention (which drops whole days, never
/// a row newer than the floor). Retention is configured in whole days, so it can never remove a witness
/// row inside a three-hour floor. A new writer must invalidate this cache.</para>
///
/// <para><b>Hourly re-seed.</b> One writer per store is the product's invariant, and the hourly re-seed makes an
/// unsupported second writer's deletion non-fatal: the cached watermark comes back down within the hour, while
/// the target's Query Store still holds the rows. Age is measured on the run's collection clock, and only a
/// store read (<see cref="Seed"/>) resets it.</para>
///
/// <para>Values are truncated to microseconds (what the store keeps) and returned with Kind Unspecified,
/// as the store read returns them. <see cref="Advance"/> on a missing entry never seeds, and bumps the
/// generation so a store read that started earlier cannot land after a newer batch committed.</para>
/// </summary>
internal sealed class DatabaseWatermarkCache
{
    private readonly object _gate = new();
    private readonly Dictionary<(int ServerId, string Database), DatabaseWatermarkEntry> _entries = new();
    private long _generation;

    /// <summary>The longest an entry may answer without a fresh store read.</summary>
    internal static readonly TimeSpan ReseedInterval = TimeSpan.FromHours(1);

    internal static DateTime Micro(DateTime v) =>
        DateTime.SpecifyKind(new DateTime(v.Ticks - v.Ticks % TimeSpan.TicksPerMicrosecond), DateTimeKind.Unspecified);

    public long Generation
    {
        get { lock (_gate) { return _generation; } }
    }

    public bool TryGet(int serverId, string database, DateTime floor, DateTime now, out DateTime? value)
    {
        lock (_gate)
        {
            value = null;
            if (!_entries.TryGetValue((serverId, database), out var e) || Micro(floor) < e.SeedFloor)
            {
                return false;
            }

            if (now - e.SeededAt >= ReseedInterval)
            {
                return false;
            }

            if (e.Value is null)
            {
                return true;
            }

            if (e.Witness is DateTime w && w > Micro(floor))
            {
                value = e.Value;
                return true;
            }

            return false;
        }
    }

    public void Seed(int serverId, string database, DateTime? value, DateTime floor, DateTime now, long generation)
    {
        lock (_gate)
        {
            if (generation != _generation)
            {
                return;
            }

            _entries[(serverId, database)] = new DatabaseWatermarkEntry(
                value is DateTime v ? Micro(v) : null, null, Micro(floor), now);
        }
    }

    public void Advance(int serverId, string database, DateTime? batchMax, DateTime batchCollectionTime)
    {
        lock (_gate)
        {
            if (!_entries.TryGetValue((serverId, database), out var e))
            {
                _generation++;
                return;
            }

            if (batchMax is not DateTime raw)
            {
                return;
            }

            var m = Micro(raw);
            var ct = Micro(batchCollectionTime);
            if (e.Value is null || m > e.Value)
            {
                _entries[(serverId, database)] = e with { Value = m, Witness = ct };
            }
            else if (m == e.Value)
            {
                _entries[(serverId, database)] = e with { Witness = e.Witness is DateTime w && w > ct ? w : ct };
            }
        }
    }

    public void Invalidate(int serverId, string database)
    {
        lock (_gate)
        {
            _generation++;
            _entries.Remove((serverId, database));
        }
    }

    public void InvalidateServer(int serverId)
    {
        lock (_gate)
        {
            _generation++;
            foreach (var k in _entries.Keys.Where(k => k.ServerId == serverId).ToList())
            {
                _entries.Remove(k);
            }
        }
    }
}

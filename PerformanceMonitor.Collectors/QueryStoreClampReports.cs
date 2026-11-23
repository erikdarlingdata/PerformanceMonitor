/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Remembers which Query Store catch-up clamp a host has already reported for a database (#9088), so a stale
/// stored watermark logs its warning and records its backfill hole once per real gap instead of once per cycle.
///
/// <para>The stored watermark is the newest row the store holds for the database. A database whose Query
/// Store has nothing new never advances it, so every cycle clamps the same raw value to a floor that has moved
/// forward with the clock. The first cycle's read already covered [floor, now], so the later cycles open no new
/// hole. Both hosts share this class so they decide the same way.</para>
///
/// <para>The decision lands only from a cycle whose read and flush succeeded (<see cref="Land"/>). A cycle that
/// faulted lands nothing, so the next one reports again and widens the hole, as it did before. A landed cycle
/// vouches for the range up to its collection time, so a later clamp whose floor lies past that time
/// (an outage longer than the catch-up window) is a new gap and is reported even for the same raw value.
/// A restart would forget all of it, so each landed entry is also kept in the host's state store (#9359):
/// <see cref="Land"/> hands back the one write or delete that keeps the stored copy current (a delete repeats until
/// <see cref="Confirm"/> reports the write succeeded), and
/// <see cref="Seed"/> reads it back before a server's first clamp decision. Without it the first clamp after a
/// restart opened a hole from the raw watermark of an idle database, which the backfill then dug for nothing.</para>
/// </summary>
public sealed class QueryStoreClampReports
{
    private readonly ConcurrentDictionary<(int ServerId, string Database), (DateTime Raw, DateTime ReadThrough)> _landed = new();

    /// <summary>The databases whose entry the state store holds (written by this process or read by <see cref="Seed"/>), so a
    /// database that never clamps costs no delete, and one that stops clamping costs one that is repeated only until it lands.</summary>
    private readonly ConcurrentDictionary<(int ServerId, string Database), byte> _stored = new();

    private readonly ConcurrentDictionary<int, byte> _seeded = new();

    /// <summary>True once <see cref="Seed"/> has run for the server, whatever the store held.</summary>
    public bool IsSeeded(int serverId) => _seeded.ContainsKey(serverId);

    /// <summary>
    /// Loads the entries a previous process stored for the server (#9359), from its backfill state rows. Call it before the
    /// server's first clamp decision. A malformed value is no entry, and an entry already landed in this process wins.
    /// </summary>
    public void Seed(int serverId, IReadOnlyDictionary<string, string>? backfillState)
    {
        _seeded[serverId] = 0;
        if (backfillState is null)
        {
            return;
        }

        foreach (var entry in backfillState)
        {
            if (!entry.Key.StartsWith(QueryStoreBackfillState.LandedKeyPrefix, StringComparison.Ordinal)
                || !QueryStoreBackfillState.TryDecodeLanded(entry.Value, out var raw, out var readThrough))
            {
                continue;
            }

            var database = entry.Key.Substring(QueryStoreBackfillState.LandedKeyPrefix.Length);
            _landed.TryAdd((serverId, database), (raw, readThrough));
            _stored[(serverId, database)] = 0;
        }
    }

    /// <summary>True when a landed cycle already reported this raw watermark and read every range the new floor
    /// would skip: no new warning and no new hole.</summary>
    public bool AlreadyReported(int serverId, string database, DateTime raw, DateTime floor)
        => _landed.TryGetValue((serverId, database), out var seen)
            && seen.Raw == raw
            && floor <= seen.ReadThrough;

    /// <summary>
    /// Where a clamp hole for this raw watermark should start (#9352). A landed cycle read everything up to its
    /// collection time, so a later clamp that is a new gap (an outage that outlasted the catch-up window) leaves
    /// only the span from that time to the new floor unread. Opening the hole at the raw watermark instead made
    /// it overlap the hole an earlier clamp recorded, and merging the two sent the dig back over a range it had
    /// filled and a range the live read held. Any other raw value, database or server gets the raw watermark.
    /// </summary>
    public DateTime HoleFloor(int serverId, string database, DateTime raw)
        => _landed.TryGetValue((serverId, database), out var seen) && seen.Raw == raw && seen.ReadThrough > raw
            ? seen.ReadThrough
            : raw;

    /// <summary>
    /// Where a clamp hole starts for a read that went through <see cref="QueryStoreReplicaRouting.ComputeCutoff"/> (#9352). The
    /// look-back is what that cutoff already put between the raw watermark and the hole start (<c>raw - cutoff.HoleFrom</c>, zero
    /// when the read used none). It applies to <see cref="HoleFloor"/>'s answer, so a read with a look-back and a read without
    /// one follow one rule: the hole starts the look-back before what the landed cycle read. Passing the cutoff's own
    /// <c>HoleFrom</c> to <see cref="HoleFloor"/> never matched the landed raw watermark, so a look-back read kept the overlap.
    ///
    /// <para>Callers reach this only when <see cref="AlreadyReported"/> was false. When this raw watermark landed, that means the
    /// new floor lies past the landed read-through, so the start (read-through minus the look-back) stays below the hole's end, the
    /// floor. For any other raw watermark the start is the raw watermark minus the look-back, below the floor it clamped to.</para>
    /// </summary>
    public DateTime HoleFrom(int serverId, string database, DateTime raw, QueryStoreCutoff cutoff)
    {
        ArgumentNullException.ThrowIfNull(cutoff);
        var lookBack = cutoff.HoleFrom is DateTime from && from < raw ? raw - from : TimeSpan.Zero;
        return HoleFloor(serverId, database, raw) - lookBack;
    }

    /// <summary>Lands a successful cycle. <paramref name="clampedRaw"/> is the raw watermark that cycle clamped,
    /// or null when it did not clamp (the watermark is fresh again, so a later stale one is a new gap).
    /// Returns the change to make to the state store (#9359): the new value for a clamped cycle, a delete for the first
    /// cycle that no longer clamps after one was stored, and null otherwise, so a database that does not clamp is never written.</summary>
    public QueryStoreLandedChange? Land(int serverId, string database, DateTime? clampedRaw, DateTime collectionTime)
    {
        var key = QueryStoreBackfillState.LandedKeyPrefix + database;
        if (clampedRaw is DateTime raw)
        {
            _landed[(serverId, database)] = (raw, collectionTime);
            _stored[(serverId, database)] = 0;
            return new QueryStoreLandedChange(key, QueryStoreBackfillState.EncodeLanded(raw, collectionTime));
        }

        /* The stored entry stays marked until Confirm says the delete landed: writes are best effort, so a failed delete must come
           back on the next cycle that does not clamp, or the row would sit in the store until something else retires it. */
        _landed.TryRemove((serverId, database), out _);
        return _stored.ContainsKey((serverId, database)) ? new QueryStoreLandedChange(key, null) : null;
    }

    /// <summary>Records that the store write for these changes succeeded (#9359). A confirmed delete clears the database's stored
    /// mark, so <see cref="Land"/> stops returning it. A write that failed is never confirmed, so its delete is returned again.
    /// A database that clamped again since the delete was returned keeps its mark, because its upsert replaced the row.</summary>
    public void Confirm(int serverId, IEnumerable<QueryStoreLandedChange> changes)
    {
        foreach (var change in changes)
        {
            if (change.Value is null && change.Key.StartsWith(QueryStoreBackfillState.LandedKeyPrefix, StringComparison.Ordinal))
            {
                var database = change.Key.Substring(QueryStoreBackfillState.LandedKeyPrefix.Length);
                if (!_landed.ContainsKey((serverId, database)))
                {
                    _stored.TryRemove((serverId, database), out _);
                }
            }
        }
    }
}

/// <summary>One change to a database's stored clamp entry (#9359): <see cref="Value"/> is the text to upsert under
/// <see cref="Key"/>, or null to delete the key.</summary>
public readonly record struct QueryStoreLandedChange(string Key, string? Value);

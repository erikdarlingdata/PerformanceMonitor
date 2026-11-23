/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

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
/// In memory only: a restart reports once more.</para>
/// </summary>
public sealed class QueryStoreClampReports
{
    private readonly ConcurrentDictionary<(int ServerId, string Database), (DateTime Raw, DateTime ReadThrough)> _landed = new();

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
    /// or null when it did not clamp (the watermark is fresh again, so a later stale one is a new gap).</summary>
    public void Land(int serverId, string database, DateTime? clampedRaw, DateTime collectionTime)
    {
        if (clampedRaw is DateTime raw)
        {
            _landed[(serverId, database)] = (raw, collectionTime);
        }
        else
        {
            _landed.TryRemove((serverId, database), out _);
        }
    }
}

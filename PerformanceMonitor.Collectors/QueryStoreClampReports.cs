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

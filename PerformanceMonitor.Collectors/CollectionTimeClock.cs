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
/// Guarantees that the <c>collection_time</c> stamped onto one (server, collector) pair's rows strictly
/// increases within THIS process (#3936). Both hosts capture <c>collection_time</c> once per run
/// (<c>DarlingCollectorRunner.RunAsync</c>, <c>RemoteCollectorService.RunCollectorDefinitionAsync</c>) as a
/// bare <c>DateTime.UtcNow</c>, and nothing made two runs' captures distinct: a config-reload that rebuilds
/// a server's run loop while an earlier body for the same server is still finishing its write, or simply
/// two runs landing close enough together that the clock's own resolution cannot tell them apart, both
/// stamp their row with the SAME instant. For a one-row-per-run, no-watermark collector (cpu_scheduler_stats
/// is the reported case, and every collector sharing that shape is exposed the same way) that is the only
/// thing distinguishing "this cycle's row" from "last cycle's row" — sharing it makes "the newest reading"
/// ambiguous, and a per-server <c>ORDER BY collection_time DESC LIMIT 1</c> with no tiebreak can return
/// either snapshot from one read to the next (#3936's fleet card / MCP / analysis / viewer symptom).
///
/// <para><b>The fix is at the stamping site, not at each reader.</b> Nudging the captured instant forward by
/// the smallest possible amount when it would collide with this same (server, collector) pair's last stamp
/// costs nothing measurable against any collector's cadence (the smallest nudge is one tick = 100ns) and
/// makes every row's <c>collection_time</c> unique BY CONSTRUCTION for the rest of this process's life,
/// closing the ambiguity where it is created instead of leaving every reader to paper over it. Readers still
/// break a tie on <c>collection_id</c> (#3936) for rows already stored before this existed, and for the one
/// case this cannot see — a second case below.</para>
///
/// <para><b>Per-process only, like <see cref="CollectionIdGenerator"/> beside it.</b> It cannot see another
/// process's clock, so two hosts genuinely monitoring the same server independently (a misconfiguration, not
/// a supported topology) can still collide across processes. That is an operational question outside this
/// guard's scope; the reader-side tiebreak keeps even that case deterministic rather than flickering.</para>
///
/// <para>Never pruned: one <c>long</c> per (server, collector) pair ever run, bounded by fleet size times
/// collector count — the same shape as <c>DarlingWorker</c>'s <c>_detachedCollectorGates</c> /
/// <c>_queryStoreGates</c>.</para>
/// </summary>
public static class CollectionTimeClock
{
    private static readonly ConcurrentDictionary<(int ServerId, string CollectorName), long> s_lastTicks = new();

    /// <summary>
    /// Returns <paramref name="now"/> unchanged unless it would not be STRICTLY greater than the last value
    /// handed out for this (server, collector) pair, in which case it returns that last value plus one tick.
    /// Thread-safe (a lock-free compare-and-swap via <see cref="ConcurrentDictionary{TKey,TValue}.AddOrUpdate{TArg}"/>)
    /// and never blocks — safe to call from every collector run, including the N=4 concurrent server bodies.
    /// </summary>
    public static DateTime NextStrictlyAfter(int serverId, string collectorName, DateTime now)
    {
        var resultTicks = s_lastTicks.AddOrUpdate(
            (serverId, collectorName),
            addValueFactory: static (_, seedTicks) => seedTicks,
            updateValueFactory: static (_, lastTicks, seedTicks) => seedTicks > lastTicks ? seedTicks : lastTicks + 1,
            factoryArgument: now.Ticks);

        return new DateTime(resultTicks, now.Kind);
    }
}

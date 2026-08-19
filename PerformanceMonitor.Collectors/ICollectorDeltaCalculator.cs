/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Delta computation contract for cumulative DMV counters. Definitions own WHICH fields are
/// delta'd, under WHICH counter-group names and keys, and with WHICH gap policy — that is
/// parity-critical monitoring brain. Hosts supply the stateful implementation (Lite:
/// <c>DeltaCalculator</c>'s in-memory per-server cache; Darling: its service-side equivalent).
/// Semantics contract (mirrors Lite's DeltaCalculator): first sighting returns 0 and baselines;
/// counter reset (decrease) returns 0; a gap larger than <paramref name="maxGapSeconds"/> returns
/// 0 to avoid inflated deltas after restarts.
/// </summary>
public interface ICollectorDeltaCalculator
{
    long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
        DateTime? collectionTime = null, int maxGapSeconds = 0);

    long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
        out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0);

    /// <summary>
    /// As <see cref="CalculateDeltaWithInterval"/>, but for a counter whose SERIES can restart under a
    /// brand-new key — telling "this key is new to us" apart from "this counter is new to the world"
    /// (#2235).
    ///
    /// <para><b>The defect this exists for.</b> <c>query_stats</c> keys its deltas on
    /// <c>plan_handle</c>, which changes on every recompile, so a churning plan presents a fresh key on
    /// nearly every sighting and the first sighting of a key reports 0. On a plan-churning instance that
    /// discards most of the server's CPU: a query Datadog measured at ~43% of an 8-vCPU box read as 18
    /// executions and 2,824 ms over 168 hours. Worse, it is INVISIBLE — the reset branch below reports
    /// <c>interval = 0</c> precisely so a reader can tell a fabricated zero from an idle one, but that
    /// branch needs the SAME key to reappear lower, and a recompile never does. Same class of harm as
    /// the 300-second gap policy #2233 replaced: it did not merely lose data, it invented quiet.</para>
    ///
    /// <para><b>Why the caller cannot decide this itself.</b> Only the implementation knows whether a key
    /// is new, and only the caller knows how old the underlying series is. So the caller passes the age
    /// and the implementation combines it with its own record of when it last looked.</para>
    ///
    /// <para><paramref name="seriesAgeSeconds"/> is how long ago the counter series began, measured on
    /// the SOURCE's clock at collection time — an age, deliberately, not a timestamp. A
    /// <c>creation_time</c> from a DMV is in the monitored server's local time while collection times are
    /// UTC, so comparing the two directly is a timezone bug on every server that is not UTC;
    /// <c>DATEDIFF(SECOND, qs.creation_time, GETDATE())</c> is evaluated where both clocks are the same
    /// and travels safely. Pass <c>null</c> when unknown, which behaves exactly as
    /// <see cref="CalculateDeltaWithInterval"/>.</para>
    ///
    /// <para>Default-implemented as a pass-through so existing implementers — including test doubles —
    /// keep compiling and keep today's behaviour until they opt in.</para>
    /// </summary>
    long CalculateDeltaWithSeriesAge(int serverId, string collectorName, string key, long currentValue,
        int? seriesAgeSeconds, out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        => CalculateDeltaWithInterval(serverId, collectorName, key, currentValue, out intervalSeconds,
            collectionTime, maxGapSeconds);
}

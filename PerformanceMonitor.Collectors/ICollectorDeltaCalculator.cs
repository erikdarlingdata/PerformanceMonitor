/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #4428: the outcome of <see cref="ICollectorDeltaCalculator.DecideRow"/> — whether ANY counter family
/// of one row restarted under its shared key, and, when one did, whether the #2235 series-age test places
/// that restart inside the gap since the calculator last looked.
/// </summary>
/// <param name="AnyReset">True when at least one family's current value read smaller than what this
/// calculator last cached for it — the same shrink a per-family delta call already refuses on its own.
/// False (the default) reports no row-level restart, and every family keeps its own ordinary delta.</param>
/// <param name="CreditedInGap">Only meaningful when <see cref="AnyReset"/> is true: whether the restart
/// falls inside the gap since the previous pass, so the row's counters are knowably "current value since
/// the restart" rather than unknowable.</param>
/// <param name="IntervalSeconds">The real interval to report for the row when <see cref="CreditedInGap"/>
/// is true (the gap the restart fell inside); 0 otherwise.</param>
public readonly record struct RowResetDecision(bool AnyReset = false, bool CreditedInGap = false, int IntervalSeconds = 0);

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

    /// <summary>
    /// #4428: peeks whether ANY family in <paramref name="counters"/> would restart under
    /// <paramref name="key"/> — without updating any cached baseline — so a caller with several counters
    /// sharing one row identity can decide the reset ROW-COHERENTLY instead of family by family.
    ///
    /// <para><b>The defect this exists for.</b> A cached plan's <c>dm_exec_query_stats</c> row can restart
    /// (SQL Server reuses the plan handle for a new compile, or the counters otherwise restart under the
    /// same key) while the counters do not all restart the same way in the same instant a per-family
    /// <see cref="CalculateDeltaWithSeriesAge"/> call observes them: a counter that shrank is caught by the
    /// reset branch and reported as unknowable, but a sibling counter of the SAME row that had already
    /// re-grown PAST its pre-restart value looks like an ordinary increase to a call that only ever sees
    /// its own family — so one row's restart was read as a shrink in one counter and a giant real increment
    /// in another. In the field: executions 1 → 16 against a CPU counter that fell from 57,695,259 to
    /// 703,943 read as “executions +15” rather than “16 executions since the restart”.</para>
    ///
    /// <para><b>The rule.</b> If ANY family in <paramref name="counters"/> would reset (its current value
    /// is smaller than this calculator's cached value for it under <paramref name="key"/>), the WHOLE row
    /// is a reset — not just the family that shrank. #2235's series-age test then decides what the row's
    /// counters become: if <paramref name="seriesAgeSeconds"/> places the restart inside the gap since the
    /// previous pass (the same test <see cref="CalculateDeltaWithSeriesAge"/> already applies per family),
    /// every counter's delta is knowable as “current value since the restart”; otherwise every counter is
    /// unknowable, the same (0, 0) pairing a single reset already reports.</para>
    ///
    /// <para>A peek, not a delta: it neither updates a cached baseline nor advances the pass window, so
    /// calling it costs nothing a caller has not already decided to pay when it goes on to call
    /// <see cref="CalculateDeltaWithSeriesAge"/> for each family, which performs the real update.</para>
    ///
    /// <para>Default-implemented to report no reset — <c>AnyReset = false</c> — so existing implementers,
    /// including every test double in this repo, keep compiling and keep today's per-family behaviour until
    /// they opt in by overriding it (the shared <see cref="CollectorDeltaCalculator"/> both SKUs run does).
    /// </para>
    /// </summary>
    /// <param name="counters">Every family of the row, family name paired with its current value, in the
    /// SAME collection pass and under the SAME <paramref name="key"/>.</param>
    RowResetDecision DecideRow(int serverId, IReadOnlyList<(string Family, long Current)> counters, string key,
        int? seriesAgeSeconds, DateTime? collectionTime, int maxGapSeconds)
        => default;

    /// <summary>
    /// #4428: rolls a caller-named pass window forward when <paramref name="observedTime"/> is new for
    /// (<paramref name="serverId"/>, <paramref name="group"/>), and returns the PREVIOUS value of that
    /// window — the same bookkeeping <see cref="DecideRow"/> already keeps on the collector's own clock
    /// (<c>collectionTime</c>), exposed generically so a definition that must place a restart on a
    /// DIFFERENT clock — a source's own <c>now()</c>, captured in the same read, rather than the collector
    /// host's — can track that clock's own previous pass without a second cache of its own.
    ///
    /// <para><paramref name="group"/> is a caller-chosen namespace. It shares nothing with any
    /// <c>collectorName</c> a delta family uses elsewhere — the point of the method is that a target-clock
    /// window and a collector-clock window never mix, so a caller MUST pick a name no ordinary delta call
    /// also passes as its <c>collectorName</c>.</para>
    ///
    /// <para>Default-implemented to return null, like every other member here, so an implementer that
    /// tracks no such window — every test double in this repo until it opts in — keeps compiling.</para>
    /// </summary>
    DateTime? PreviousPass(int serverId, string group, DateTime observedTime)
        => null;

    /// <summary>
    /// Forgets every baseline and every pass window cached for <paramref name="serverId"/>, because the
    /// counters behind that id are no longer the counters the baselines were read from (#3653 A5, the
    /// identity-epoch item of #3540).
    ///
    /// <para><b>Why this is on the DEFINITION's contract and not only on the host's calculator.</b> The
    /// hosts already forget a server they stop monitoring (Lite's tab close, Darling's reconcile-remove
    /// branch, #3540 A4) and they hold the concrete calculator to do it. But the discontinuities that
    /// happen WHILE a server stays monitored — the instance restarted, the listener or the DNS endpoint
    /// now lands on a different replica, <c>pg_stat_statements_reset()</c> was called — are visible only
    /// to a definition, in the row it is reading, and only that definition can act BEFORE its own
    /// subtraction: a host that learns of the epoch after the run has already stored one interval of
    /// <c>new instance's counter minus old instance's baseline</c>. So the definition that observes the
    /// epoch (see <see cref="ServerEpoch"/>) forgets through the same handle it subtracts through.</para>
    ///
    /// <para><paramref name="discontinuity"/> is the one-line human account of what changed (old and new
    /// value, named), which the implementation may keep for the host to log — the definition has no
    /// logger, the host has no view of the row. Null when the caller has nothing to say (the host's own
    /// remove path).</para>
    ///
    /// <para>Default-implemented as a no-op, like <see cref="CalculateDeltaWithSeriesAge"/>, so an
    /// implementer that caches nothing per server — every test double in this repo — keeps compiling with
    /// nothing to forget. The shared <see cref="CollectorDeltaCalculator"/> both SKUs run overrides both
    /// members; an implementation that caches baselines and inherits these no-ops has the continuity bug
    /// this paragraph is the only warning of.</para>
    /// </summary>
    void ClearServer(int serverId, string? discontinuity = null)
    {
    }

    /// <summary>
    /// As <see cref="ClearServer"/>, but for the named delta GROUPS only — the <c>collectorName</c> values
    /// the family's <c>CalculateDelta*</c> calls pass — leaving every other family's baselines intact.
    /// For an epoch that belongs to one family alone: <c>pg_stat_statements_info.stats_reset</c> moves when
    /// the statements counters are reset or are a different instance's, and says nothing about any other
    /// counter on the server, so forgetting the whole server for it would throw away knowable intervals
    /// elsewhere. Same default, for the same reason.
    /// </summary>
    void ClearGroups(int serverId, string? discontinuity, params string[] groups)
    {
    }
}

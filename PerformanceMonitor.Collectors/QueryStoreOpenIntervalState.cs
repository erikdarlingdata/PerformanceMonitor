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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-database stamp of the last cycle that read the OPEN Query Store interval (#2312), the third
/// sibling of <see cref="QueryStorePlanXmlState"/> and <see cref="QueryStoreTextState"/> — same
/// per-database key shape, same conservative-default rules, its own state owner.
///
/// <para><b>Why this exists.</b> Query Store runtime stats are cumulative per-interval snapshots, and the
/// collector re-fetches the OPEN interval every cycle so the read side can collapse to the latest snapshot
/// (<c>rn = 1</c>). On a large multi-tenant primary that means re-aggregating the entire current interval's
/// slices across every tenant database, every cycle, each pass pricier as the interval fills — measured on
/// the prod fleet at 40–110 s per run around the clock, rising through the day with workload, with a
/// 554 s worst case (#2312). Closed intervals cost nothing extra: the time watermark's
/// <c>MAX(last_execution_time) &gt; @cutoff_time</c> already excludes any interval fully collected. The
/// open interval is the whole bill, and most of its re-reads buy nothing a reader keeps — every snapshot
/// but the latest is discarded by <c>rn = 1</c>.</para>
///
/// <para><b>The mechanism:</b> most cycles ship only intervals that have CLOSED
/// (<c>i.end_time &lt;= SYSUTCDATETIME()</c>) — they are immutable and therefore final on first
/// collection — and the open interval is included only when this stamp says its last inclusion is at
/// least <see cref="RefreshEvery"/> ago. Correctness leans on the cumulative-snapshot contract twice
/// over: a newly CLOSED interval whose final content differs from our last open-snapshot must contain
/// executions newer than the watermark (counters only move with executions), so the standing time filter
/// picks it up; and one whose content did not change IS our last snapshot, so there is nothing to miss.</para>
///
/// <para><b>Time-based, not cycle-counting</b>, so the refresh survives cadence changes and delivered-vs-
/// configured drift (at fleet scale the delivered cadence runs well behind the configured one, and an
/// every-Nth-cycle rule would stretch with it). <b>Include is the conservative default</b>: an absent,
/// malformed or future-stamped row reads as "include the open interval now", so a first run, a restarted
/// host and a broken store all behave exactly like today's collector rather than silently going stale.
/// The same conservatism governs the write side: both hosts land the stamp only after that database's
/// read AND flush succeed, so a per-database fault the sweep tolerates re-includes next cycle instead of
/// spending the refresh window on a cycle that captured nothing.</para>
/// </summary>
public static class QueryStoreOpenIntervalState
{
    /// <summary>
    /// The collector name this state is stored under — its own owner, like the plan and text watermarks,
    /// because a prefix pruned under the wrong owner silently deletes nothing and the three advance for
    /// unrelated reasons.
    /// </summary>
    public const string StateCollectorName = "query_store_open_interval";

    /// <summary>Prefix for the per-database state key.</summary>
    public const string WatermarkKeyPrefix = "qsowm:";

    /// <summary>
    /// How stale the open interval's stored snapshot may grow before a cycle refreshes it. Thirty minutes
    /// against the 60-minute Query Store default interval means ~2 snapshots per interval instead of one
    /// per cycle (~12 at the 5-minute cadence) — roughly five sixths of the open-interval spend removed —
    /// while the CURRENT interval's view in any reader lags real time by at most this much. Readers of
    /// closed history lose nothing at all. Deliberately NOT configurable per-server: one number with a
    /// recorded rationale beats a knob nobody can reason about.
    ///
    /// <para>Moved from 15 to 30 (#2759) because the #2312 yardstick it was tuned against — multi-53 at
    /// ~50 s/run — stopped holding. Measured live on <c>prod-pos-use1-multi-45</c> (2026-09-01, 3-hour
    /// window of <c>collection_log</c>): open-interval-inclusive runs cost 110–133 s each (2–2.5x the
    /// original yardstick), landing every 12–19 minutes — the 15-minute skip window was already firing as
    /// designed, spacing the expensive runs out, but each one had grown too large for that spacing to be
    /// enough. Doubling the window halves their frequency without changing anything about the query
    /// itself or what a reader sees once an interval closes.</para>
    /// </summary>
    public static readonly TimeSpan RefreshEvery = TimeSpan.FromMinutes(30);

    /// <summary>The state key for one database.</summary>
    public static string KeyFor(string databaseName) => WatermarkKeyPrefix + databaseName;

    /// <summary>
    /// Whether this cycle should read the OPEN interval for one database. True — today's behavior — for an
    /// absent, malformed or future stamp (clock skew must not pin the snapshot stale), or one at least
    /// <see cref="RefreshEvery"/> old.
    /// </summary>
    public static bool ShouldIncludeOpenInterval(
        IReadOnlyDictionary<string, string>? state, string databaseName, DateTime utcNow)
    {
        if (state is null || !state.TryGetValue(KeyFor(databaseName), out var raw) || string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }

        if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var unixSeconds)
            || unixSeconds <= 0)
        {
            return true;
        }

        /* Same guard as QueryStoreTextState.TryParse: long.TryParse accepts values far outside
           FromUnixTimeSeconds's year-0001..9999 range, and an out-of-range-but-numeric stamp is just
           another flavor of corrupt row — the conservative include, not an exception. */
        try
        {
            var stamped = DateTimeOffset.FromUnixTimeSeconds(unixSeconds).UtcDateTime;
            return stamped > utcNow || utcNow - stamped >= RefreshEvery;
        }
        catch (ArgumentOutOfRangeException)
        {
            return true;
        }
    }

    /// <summary>Formats the stamp for one inclusion of the open interval.</summary>
    public static string Format(DateTime includedAtUtc) =>
        new DateTimeOffset(DateTime.SpecifyKind(includedAtUtc, DateTimeKind.Utc)).ToUnixTimeSeconds()
            .ToString(CultureInfo.InvariantCulture);
}

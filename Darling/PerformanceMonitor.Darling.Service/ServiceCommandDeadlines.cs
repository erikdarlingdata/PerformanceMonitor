/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Explicit command deadlines for this project's store access (#2874), one constant per BUDGET REGIME.
///
/// <para><c>.Service</c> is roughly fifteen regimes, not one, and they are being closed a regime at a
/// time; this file is where each one's constant lands. Only the collection sweep is here so far.
/// <b>Do not reuse a constant across regimes</b> — the four numbers this sweep has already produced
/// (60 s in <c>.Analysis</c> from a 120 s <c>CancelAfter</c>, 10 s for the alert pass from its 30 s
/// cadence, <c>.Storage</c>'s five, <c>.Viewer</c>'s 15/5/10 from a connection permit) each came from a
/// different enclosing constraint and none of them transferred.</para>
/// </summary>
public static class ServiceCommandDeadlines
{
    /// <summary>
    /// The per-server collection body's store reads and writes — every collector's state read, its
    /// watermark read, its <c>collection_log</c> row, and the binary COPY that is the collector's
    /// actual store write on all four transports.
    ///
    /// <para><b>The regime.</b> Nothing encloses these: <c>ProcessServerSweepAsync</c> runs on the plain
    /// stopping token, with no <c>CancelAfter</c> anywhere between the 15 s launch tick and the
    /// collector call. What the body DOES hold is scarce — one of <c>max_concurrent_sweeps</c> permits
    /// and, post-#2822, one borrowed store connection out of <c>MaxPoolSize = 24</c>, both for its whole
    /// duration — and the 60 s <c>DarlingWorker.SweepWatchdogSeconds</c> only LOGS, so nothing
    /// interrupts a stalled body. The per-command deadline is the only bound that exists.</para>
    ///
    /// <para><b>ABOVE the measured worst case.</b> The store-write half of every collector run is
    /// already instrumented as <c>collection_log.store_duration_ms</c>. Over 24 h on the busiest server
    /// of the larger production fleet (43 servers, a 305 GB store ingesting ~15-20 GB/day) the worst of
    /// 200 runs was <b>1.53 s</b> — <c>query_store</c> writing 11,614 rows as a per-database batch
    /// sequence. Fleet-wide, the per-run store-write average across all 39 collectors on both
    /// production stores tops out at <b>1,511 ms</b> (<c>index_object_stats</c>, ~21,090 rows/run) with
    /// most collectors in the single-digit-to-low-tens ms. So <b>1.53 s</b> is the worst thing actually
    /// observed, and 10 s keeps ~6.5x headroom over it.</para>
    ///
    /// <para><b>Store connection acquisition is NOT in this floor</b>, and the reason is what
    /// <c>CommandTimeout</c> actually bounds. #2819 measured acquisition at 673-893 ms, and an earlier
    /// derivation of this constant added it to the store write to reach a ~2.4 s floor. It does not
    /// belong there: <c>CommandTimeout</c> starts when the command EXECUTES on an already-open
    /// connection, while the connect phase is tracked by the connection string's <c>Timeout</c>. That is
    /// measured rather than reasoned — with <c>Timeout=2</c> a connect failure lands at 2.0 s whether
    /// <c>CommandTimeout</c> is 1 or 60. Folding an acquisition figure into a <c>CommandTimeout</c>
    /// floor credits this knob with bounding a phase it cannot reach. The 673-893 ms is real and still
    /// binds where it belongs — pool pressure, in <c>DarlingManagedPostgres</c> and
    /// <c>DarlingCollectorRunner</c> — just not here.</para>
    ///
    /// <para><b>BELOW the point where the deadline costs more than it saves</b>, and the bound is the
    /// 60 s watchdog rather than a cadence, because the body runs its due collectors SEQUENTIALLY. Real
    /// cycles on that same server span 13 s (a light tick) to 48 s (a tick including
    /// <c>query_store</c>). One blown deadline on a 48 s cycle lands at 58 s and stays under the
    /// watchdog; one blown 30 s DEFAULT lands at 78 s and manufactures a "collection body has not
    /// completed after 60s" warning on a body that is merely slow. Keeping a single overshoot inside the
    /// watchdog is what stops this change from re-creating the #1581/#2170 warning herd.</para>
    ///
    /// <para><b>Why it is derived at <c>max_concurrent_sweeps</c> = 8, not the seeded 4.</b> The knob
    /// (V59, clamped 1-16) is going from 4 to 8, which doubles the bodies in flight and therefore the
    /// concurrent store connections the sweep holds — the pool is NOT the binding constraint at either
    /// width (borrowing put peak demand at the sweep width itself, and
    /// <c>DarlingManagedPostgres</c>'s own note establishes even 16-wide fits inside 24 with the
    /// seams), but the DURATION of a stalled hold is: C bodies each stalled T seconds withhold C x T
    /// connection-seconds from retention, alerting and observability. At the 30 s default and C = 8 that
    /// is 8 of 24 connections held for 30 s, a third of the pool, against a sixth at C = 4. So the
    /// floor above was doubled before the headroom was taken — 1.53 s of measured store write projected
    /// to <b>~3.1 s</b> if write latency scaled linearly with twice the concurrent writers is what 10 s
    /// is set to clear, and it clears it by ~3.2x. <b>The value is correct at 8 and at 16;
    /// it was chosen so that raising the knob does not invalidate it.</b></para>
    ///
    /// <para><b>What this is NOT derived from.</b> Not the 120 s <c>PerItemWallClockBudget</c>
    /// abandonment tail — that was root-caused to external contention on the monitored target
    /// (confined to 2 of 43 servers, load-invariant across a 6.1x cadence increase: runs x6.1,
    /// abandonments x1.11), so a store-side deadline is not the lever for it. And not
    /// <c>collector_cost.max_sql_ms</c>, which on a budgeted collector is CENSORED by that same 120 s
    /// budget and measures how long an abandon took to unwind rather than stall severity.</para>
    ///
    /// <para>It happens to land on the same 10 s the alert pass uses. That is two derivations meeting,
    /// not a number being reused: the alert pass is bounded above by its own 30 s sweep interval and
    /// below by a 1,744.9 ms forced-plan read, neither of which appears anywhere above.</para>
    /// </summary>
    public const int CollectionSweepSeconds = 10;
}

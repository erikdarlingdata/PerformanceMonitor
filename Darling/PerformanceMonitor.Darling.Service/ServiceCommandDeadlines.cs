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
/// time; this file is where each one's constant lands. The collection sweep (#2928), the post-analysis
/// force-plan hook and the CLI verbs (#2874 group E) are here so far.
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
    /// most collectors in the single-digit-to-low-tens ms. On top of that sits store connection
    /// acquisition, measured at <b>673-893 ms</b> in #2819. So ~2.4 s covers the worst thing actually
    /// observed, and 10 s keeps ~4x headroom over it.</para>
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
    /// to ~3.1 s if write latency scaled linearly with twice the concurrent writers, plus ~0.9 s of
    /// acquisition, is the ~4.0 s that 10 s is set to clear. <b>The value is correct at 8 and at 16;
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

    /// <summary>
    /// <c>PgPlanForceActionStore</c>'s four commands — the force-plan bot's journal write and its windowed
    /// history read, plus the two reads #2731's write path will consume.
    ///
    /// <para><b>The regime, and why #2882 deferred it.</b> The alert pass excluded these deliberately: they
    /// look like members of that family and are not. <c>PlanForceBot.RunAfterAnalysisAsync</c> is built at
    /// <c>DarlingWorker.cs</c>'s <c>postPassHook</c> closing over the plain <c>stoppingToken</c>, so it runs
    /// OUTSIDE <c>passCts.CancelAfter(s_analysisTimeout)</c> — whose token is threaded only into
    /// <c>analysisService.AnalyzeAsync</c> — despite being invoked lexically inside the pass. Being
    /// lexically inside a budgeted method is not the same as being under its budget. So nothing encloses
    /// these, exactly as in #2882; what differs is the CADENCE, which is the analysis interval clamped
    /// 5-360 minutes rather than the 30 s alert sweep the 10 s was bounded against.</para>
    ///
    /// <para><b>The ceiling is the analysis pass's own budget, not a cadence and not the watchdog.</b> The
    /// hook runs inside <c>ProcessServerSweepAsync</c>, holding one of <c>max_concurrent_sweeps</c> permits
    /// for its whole duration, and that server's collection cannot relaunch while the body is in flight. The
    /// 60 s <c>DarlingWorker.SweepWatchdogSeconds</c> is NOT the bound here the way it is for the collection
    /// sweep: a body that ran an analysis pass has already been eligible to trip it, since the pass alone is
    /// budgeted at 120 s. What the pass's 120 s IS, is the product's own statement of how long a server's
    /// collection may be suspended for analysis — so a hook that rides the pass must not exceed it. The bot
    /// performs at most <c>PlanForceBot.MaxTargetsPerPass</c> (10) history reads plus one journal write each,
    /// sequentially, on separate store connections: <b>20 commands</b>. 20 x 5 s = 100 s, inside the pass's
    /// 120 s. At Npgsql's inherited 30 s default it is 600 s — <b>five times the budget of the pass it rides
    /// on</b>, for a hook that is not part of it.</para>
    ///
    /// <para><b>ABOVE the measured worst case, which is dominated by connection acquisition rather than by
    /// the query.</b> Measured on PostgreSQL 17 against a <c>collect.plan_force_actions</c> seeded to 5.0 M
    /// rows / 1,382 MB — 43 servers x 8 databases x 1,000 distinct queries over a 1,095-day horizon, because
    /// this table has NO retention path and grows for the life of the deployment — <c>GetQueryHistoryAsync</c>
    /// runs in <b>3.59 ms cold</b> and 0.84-1.01 ms warm, and the journal <c>INSERT ... RETURNING</c> in
    /// <b>0.346 ms</b>. It is flat in table size: both indexes V107 creates are used, and the only subquery
    /// that scales with volume is the trailing-24 h server count, which stayed at 0.47 ms with a
    /// deliberately pathological 2,925-row window (the volume a per-query cooldown of zero would produce).
    /// So the per-command floor is <b>~0.9 s of store connection acquisition</b> (673-893 ms, #2819) plus
    /// single-digit milliseconds of work, and 5 s carries ~5.6x over it — the same ratio #2901 took on
    /// <c>.Viewer</c>'s interactive reads.</para>
    ///
    /// <para><b>The two reads with no caller yet.</b> <c>GetPendingReviewsAsync</c> and
    /// <c>GetRecentActionsAsync</c> are specced ahead of #2731 and have no production caller in this build,
    /// so they take this constant because the class they sit on is the bot's. When
    /// <c>GetRecentActionsAsync</c> gains the <c>get_plan_force_actions</c> tool its doc comment names, it
    /// becomes an MCP read and belongs on <c>Mcp.McpCommandDeadlines.ReadSeconds</c> instead — flagged here
    /// rather than pre-empted, because moving it now would bound a caller that does not exist against a
    /// regime it is not yet in.</para>
    /// </summary>
    public const int PostAnalysisForcePlanSeconds = 5;

    /// <summary>
    /// The store reads and writes the CLI verbs perform — <c>--enable-mcp</c>/<c>--disable-mcp</c>/
    /// <c>--enable-web</c>/<c>--disable-web</c>'s <c>config_service</c> update, <c>--configure-firewall</c>'s
    /// endpoint-toggle read, and <c>--recompress-plan-dim</c>'s plan-codec preflight.
    ///
    /// <para><b>This number is not new: the product already derived it, and two of the three sites did not
    /// get it.</b> <c>DarlingCliCommands.TryReadEndpointTogglesAsync</c> wraps its read in a linked
    /// <c>CancellationTokenSource</c> at ten seconds, with the reason written down — <i>"the store is down"
    /// and "the store is slow" must not differ in how long an installer hangs</i> — and it prints that number
    /// back to the operator ("the store did not answer within 10 seconds"). That argument is a property of
    /// the SURFACE, not of that one verb: every one of these runs with a person or an installer script
    /// waiting on a console, with no enclosing budget, no retry and no next tick. The other two sites simply
    /// never had it, which is the #2786 shape — a bound that names the arm it was written for.</para>
    ///
    /// <para><b>So the ceiling is the promise in that message.</b> A <c>CommandTimeout</c> above ten seconds
    /// would let a command outlive the budget whose expiry the operator is told about, and Npgsql's inherited
    /// 30 s does exactly that: the CTS fires at 10 s while the command believes it has 20 s left, so the two
    /// bounds disagree about the same wait. Making the command agree with the budget is what stops the
    /// message from being a lie. The floor is a single-row read or a single-row primary-key update of a
    /// one-row <c>config_service</c> table — sub-millisecond work — plus the same ~0.9 s of store connection
    /// acquisition, so ten seconds is over 10x the worst thing that can legitimately happen.</para>
    ///
    /// <para>It lands on the same ten seconds as <see cref="CollectionSweepSeconds"/> and #2882's alert pass.
    /// That is a third derivation meeting them rather than a number reused: nothing above mentions a 30 s
    /// sweep interval, a 60 s watchdog or <c>max_concurrent_sweeps</c>, and this one is bounded by a sentence
    /// printed to an operator.</para>
    /// </summary>
    public const int CliStoreReadSeconds = 10;
}

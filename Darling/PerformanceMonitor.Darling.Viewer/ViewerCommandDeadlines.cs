/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The explicit command deadlines for the viewer's store reads (#2874). Every one of this project's
/// 193 command sites previously set no <c>CommandTimeout</c> and so inherited Npgsql's undocumented
/// 30 s default — a value nobody chose, and the defect class behind three production failures
/// (#2810, #2871, #2796): exceeding the ceiling surfaces as <c>Exception while reading from stream</c>,
/// which reads as a network fault rather than a deadline.
///
/// <para><b>Three regimes, because this surface really has three.</b> The bulk of the project is one
/// regime and deliberately so: the fleet timer and the per-tab auto-refresh timer call the SAME
/// <c>ViewerDataService</c> methods a user gesture calls — <c>RefreshActiveInnerTabAsync</c> is
/// literally the method tab-activation invokes — so "interactive" and "background refresh" cannot be
/// told apart at the command site, and the set of reads reached ONLY by the unattended fleet fan-out
/// is empty. Splitting them would need a budget threaded through every call site, not a constant. The
/// two regimes that ARE separable are separable because something else bounds them: the command plane
/// sits inside a real orchestration budget, and the connect-time gate runs before the window is usable
/// and swallows its own failures.</para>
///
/// <para><b>What is NOT a regime here.</b> Export was the obvious fourth candidate and it does not
/// exist: <c>PerformanceMonitor.Ui.DataGridExport</c> is synchronous, store-unaware, and iterates
/// <c>grid.Items</c>, so every CSV/copy path formats rows a visible-tab load already paid for. The
/// long-running operations a user knowingly waits minutes for (snapshot_now, analyze_now, purge_now,
/// Get Actual Plan) are COMMANDS on the command plane below, not reads.</para>
///
/// <para><b>Why none of these is <c>StorageCommandDeadlines.McpReadSeconds</c>.</b> That constant is
/// 30 s for the MCP read surface and its derivation does not transfer. The MCP's worst verified read
/// was 685 ms and its permit is an unbounded pool; the viewer's worst measured read is 3.0 s — 4.4x
/// slower — yet its permit is far scarcer (<see cref="ViewerStorePool.MaxPoolSize"/>, ten on the
/// managed derivation and the operator's value on a bring-your-own store), and a single control fans
/// out to exactly ten concurrent reads and so can hold every one of a managed seat's permits. Slower
/// reads against a scarcer permit land BELOW 30 s, not at it.
/// (This paragraph also read "and an unguarded fleet-timer read can re-fire every 10 s" when it landed,
/// which was true and is no longer: the fan-out those timers fire unawaited is single-flight since
/// #2907, so a slow read is no longer joined by the next tick's. The panel fan-out above it is the
/// binding permit argument on its own, and it is not a guarding problem — those ten reads are one
/// deliberate <c>Task.WhenAll</c>, not an overlap.)</para>
/// </summary>
public static class ViewerCommandDeadlines
{
    /// <summary>
    /// The interactive/refresh store reads — 188 of the project's 193 command sites: everything
    /// except the three on the command plane and the two connect-gate probes, which share
    /// <c>ViewerDataService.cs</c> with two ordinary reads, so the regime is decided per SITE here
    /// rather than per file.
    ///
    /// <para>ABOVE the measured worst case. Timed against a store stood up by the product's own
    /// <c>PgMigrations.MigrateAsync</c> at V109 with TimescaleDB and seeded to EXACT production
    /// per-server density (<c>get_collector_cost</c> over 2 days x 42 servers: 189,414 query_stats
    /// rows/server/day), across the collector's full 30-day retention horizon — 5.68 M query_stats
    /// rows and 1.98 M procedure_stats rows for one server. The heaviest shipped per-server read,
    /// <c>TopQueriesSql</c> (a windowed aggregate plus a <c>LEFT JOIN LATERAL</c> back through
    /// <c>v_query_stats</c> and a <c>ROW_NUMBER</c> module join), measured COLD: 588 ms on the default
    /// 1-hour preset, 1.12 s on the widest 7-day preset, and 3.01 s on a 30-day custom range — the
    /// widest window any shipped read can be asked for, since retention drops the data behind it.
    /// Fifteen seconds is 5x that worst case, 13x the widest preset, and 25x the default.</para>
    ///
    /// <para>BELOW the point where a stalled read is worse than a failed one, which here is a permit
    /// argument rather than a budget one. Nothing encloses these reads — no <c>CancelAfter</c>, no
    /// <c>SemaphoreSlim</c>, no <c>WaitAsync</c>, and no request timeout, because the viewer is a WPF
    /// <c>WinExe</c> and hosts no web endpoint — so this deadline IS the budget, the same finding
    /// #2882 and #2888 both made, and the same reason both erred short. What it competes for is the store
    /// connection pool (<see cref="ViewerStorePool.MaxPoolSize"/> — ten on a managed seat, the operator's
    /// value on a bring-your-own one), and while permits are held the sidebar freshness dots, the alert
    /// poll and every other panel get nothing. Fifteen seconds is half the inherited 30 s, which halves
    /// that worst-case hold.</para>
    ///
    /// <para><b>A read that cannot get a permit never reaches this deadline, so it is not the thing that
    /// bounds one</b> (#3016). It waits <c>ConnectionTimeoutSeconds</c> (default 5) for a slot and then
    /// fails with Npgsql's own pool-exhaustion message, which names the exhausted pool and both knobs and
    /// does NOT read as a network fault — measured against Npgsql 10.0.3, correcting the earlier claim
    /// here that it misattributed a slow store to the network. What it does misattribute is nothing: on
    /// the fleet overview those failures are caught per server and the card is simply absent, so a fleet
    /// wider than the pool rendered short with no error at all.</para>
    ///
    /// <para>The two fleet-wide fan-outs cannot reach that state since #3016 — they run pool-many lanes
    /// rather than fleet-many reads. The literal-width sites can still reach it, but only on a
    /// bring-your-own store the operator configured with FEWER permits than
    /// <see cref="MeasuredFanOutWidth"/>: their widths are two to ten, which a managed seat serves without
    /// a queue. <c>ViewerCommandTimeoutTests</c> holds every literal width at or under that bound, so the
    /// residual cannot widen without someone deciding to widen it.</para>
    ///
    /// <para><b>This value bounds a read issued ALONE, and only that</b> (#3004). The permit argument
    /// above says a concurrent fan-out is the state worth protecting the pool from; it does not say a
    /// solo read's ceiling can bound one. On the same rig, ten concurrent 30-day reads measured
    /// 24.4-64.1 s EACH against 3.01 s solo — ten-way contention costs 8-21x, and running the ten
    /// together cost 64.1 s of wall clock against 30.1 s of serial work, so concurrency is net negative
    /// on this store rather than merely crowded. Fifteen seconds is below the whole of that band, so it
    /// is not a ceiling a ten-wide batch can finish under and applying it to one would fail every read in
    /// it on every attempt, auto-refresh included. A read that is part of a fan-out therefore takes
    /// <see cref="FanOutReadSeconds"/>, derived from that concurrent measurement, and the two are the
    /// same number for any width the pool can serve without contention worth pricing.</para>
    ///
    /// <para>The asymmetry, worked out for this surface rather than assumed: too short and one panel
    /// shows an error the user can retry — and the auto-refresh timer retries it within 30 s anyway,
    /// unprompted. Too long and the user watches a spinner while a pooled connection is held, which is
    /// the failure mode that cannot be diagnosed from the UI. Erring short is right here.</para>
    /// </summary>
    public const int InteractiveReadSeconds = 15;

    /// <summary>
    /// The per-lane contention allowance a concurrent read is granted on top of
    /// <see cref="InteractiveReadSeconds"/>, and the only number here derived from a CONCURRENT
    /// measurement rather than a solo one.
    ///
    /// <para>The derivation is one division. #2901's rig — a store stood up by the product's own
    /// <c>PgMigrations.MigrateAsync</c> at V109 with TimescaleDB, seeded to exact production per-server
    /// density across the collector's full 30-day retention horizon — measured ten concurrent 30-day
    /// reads at 24.4-64.1 s each. The worst read in that batch is what a deadline has to cover, and the
    /// width that produced it is ten: 64.1 / 10 = 6.41 s of allowance per lane, rounded UP to 7 so the
    /// rounding falls on the side that does not fail a legitimate panel. At the full pool width that is
    /// 70 s, about 9% over the worst read actually measured there.</para>
    ///
    /// <para><b>Why per-lane and not one wider constant.</b> A single number big enough for the ten-wide
    /// case would hand 70 s to a read with no contention at all, which is the same error as handing 15 s
    /// to a read with nine siblings — the wrong population, in the other direction. Scaling by the width
    /// the caller declares is what keeps a solo read on its own measured floor.</para>
    ///
    /// <para><b>Nothing enclosing this is smaller.</b> The per-tab auto-refresh timer floors at 30 s and
    /// the fleet timer at 10 s, but neither bounds a read: the fan-out those timers fire is single-flight
    /// (<c>_isRefreshing</c> in <see cref="CorrelatedTimelineLanesControl"/>, and #2907 for the unawaited
    /// pair), so a read outliving its own tick is not joined by the next one. A cadence shorter than the
    /// deadline would matter only if a slow read could be re-entered, and it cannot.</para>
    /// </summary>
    public const int FanOutLaneSeconds = 7;

    /// <summary>
    /// The width <see cref="FanOutLaneSeconds"/> was measured at, and so the widest fan-out this family
    /// can price from data rather than from extrapolation.
    ///
    /// <para>It equals <see cref="ViewerSettings.ManagedMaxPoolSize"/> because ten permits are what
    /// BOUNDED #2901's rig — ten concurrent reads was the widest batch that store would serve at once —
    /// not because a resource ceiling and a measurement width are the same kind of fact. They are two
    /// facts that share a number today, named apart so a sweep at a different width can move one without
    /// dragging the other (#3016). The sweep #3004 asked for (per-read duration at width 1, 2, 4, 6, 8,
    /// 10) is what would let this exceed ten; until it exists, this is the honest edge of the
    /// data.</para>
    /// </summary>
    public const int MeasuredFanOutWidth = ViewerSettings.ManagedMaxPoolSize;

    /// <summary>
    /// The deadline for a read issued as one of <paramref name="concurrentReads"/> reads running
    /// together — the ceiling that has to cover contention with its siblings rather than a solo read's.
    ///
    /// <para>Priced against the pool this seat actually has
    /// (<see cref="ViewerStorePool.MaxPoolSize"/>) rather than the managed constant it used to read
    /// (#3016). The constant is applied only to the managed derivation, so on a bring-your-own store it
    /// was not the pool size but a guess about it.</para>
    ///
    /// <para>Floored at <see cref="InteractiveReadSeconds"/> so this can only ever grant a read MORE time
    /// than a solo read gets, never less. A width the pool serves without contention worth pricing lands
    /// on the floor and is indistinguishable from an unscoped read, which is the correct outcome: two
    /// concurrent reads are not the state the concurrent measurement describes.</para>
    /// </summary>
    public static int FanOutReadSeconds(int concurrentReads) =>
        FanOutReadSeconds(concurrentReads, ViewerStorePool.MaxPoolSize);

    /// <summary>
    /// The same ceiling against an EXPLICIT pool size — the pure form, so the derivation can be pinned at
    /// pool sizes this machine is not configured for rather than only at whatever the process published.
    ///
    /// <para><b>Two ceilings on the lane count, and the smaller wins, because they answer different
    /// questions.</b> PERMITS (<paramref name="maxPoolSize"/>): contention stops growing where the slots
    /// run out — an eleventh concurrent read against a ten-connection pool is not an eleventh contender,
    /// it is a read waiting on <c>ConnectionTimeoutSeconds</c> for a slot, and a deadline cannot help it.
    /// MEASUREMENT (<see cref="MeasuredFanOutWidth"/>): <see cref="FanOutLaneSeconds"/> is one division of
    /// one measured batch, taken at ten wide, so a wider pool multiplies an allowance nothing has measured
    /// — 100 permits would read as 700 s, a number no panel and no user has any relationship with. Holding
    /// the lane count to the measured width keeps every value this returns inside the band it was derived
    /// from, and <c>ViewerReadFanOut</c> holds the CONCURRENCY to the same bound so the two agree
    /// (#3016).</para>
    /// </summary>
    public static int FanOutReadSeconds(int concurrentReads, int maxPoolSize)
    {
        var permits = Math.Max(1, maxPoolSize);
        var priced = Math.Min(permits, MeasuredFanOutWidth);
        var lanes = Math.Clamp(concurrentReads, 1, priced);

        return Math.Max(InteractiveReadSeconds, FanOutLaneSeconds * lanes);
    }

    /// <summary>
    /// What all 187 interactive command sites stamp: <see cref="FanOutReadSeconds"/> for the width the
    /// enclosing fan-out site declared through <see cref="ViewerReadFanOut"/>, or
    /// <see cref="InteractiveReadSeconds"/> when nothing declared one.
    ///
    /// <para>Read at command construction, which is what makes it correct: the width is already in the
    /// task's execution context by then, and the value is frozen onto that one command rather than shared
    /// with any other read in flight.</para>
    ///
    /// <para>The sites take this instead of <see cref="InteractiveReadSeconds"/> uniformly, including the
    /// reads no fan-out currently reaches. A read outside a scope gets the same number either way, so
    /// there is no behavioural difference to weigh — what uniformity buys is that a future
    /// <c>Task.WhenAll</c> over any of them is bounded the day it is written instead of silently
    /// inheriting a solo read's ceiling, which is the trap this whole constant family exists to close.
    /// <c>ViewerCommandTimeoutTests</c> holds that uniformity as a pin.</para>
    /// </summary>
    public static int CurrentInteractiveReadSeconds => FanOutReadSeconds(ViewerReadFanOut.CurrentWidth);

    /// <summary>
    /// The store&lt;-&gt;service command plane — the three commands in
    /// <c>ViewerDataService.Commands.cs</c> (enqueue, poll, delete).
    ///
    /// <para>ABOVE the measured worst case with room to spare: the poll is a single-row primary-key
    /// lookup on <c>config_command</c>, measured at 3.9 ms cold and 0.1 ms warm; the enqueue and the
    /// delete are single-row writes on that same table. Five seconds is three orders of magnitude over
    /// the cold measurement, deliberately, because the delete is what removes a DPAPI
    /// credential-bearing <c>args_json</c> row after a <c>test_connect</c> and should not be the thing
    /// that gives up early.</para>
    ///
    /// <para>BELOW the budget it shares, which unlike the interactive regime is real and explicit:
    /// <c>PollCommandResultAsync</c> loops until <c>DefaultCommandTimeout</c> (45 s) or
    /// <c>ImperativeCommandTimeout</c> (3 min), re-issuing the poll every 400 ms. That budget is
    /// checked only BETWEEN iterations, so with no deadline on the read itself one hung poll overshot
    /// the stated 45 s by up to Npgsql's 30 s — the loop was bounded and the read inside it was not.
    /// Five seconds puts the read an order of magnitude under the smaller of the two enclosing
    /// budgets, which restores the loop's budget as the binding constraint. That is the point: too
    /// short and the poll throws where the caller is written to receive null and show "still running /
    /// try again"; too long and a dialog's stated 45 s silently becomes 75 s.</para>
    /// </summary>
    public const int CommandPlaneSeconds = 5;

    /// <summary>
    /// The connect-time gate — <c>ReadOnlyProbeSql</c> in <c>DetectReadOnlyAsync</c> and
    /// <c>StoreSchemaProbeSql</c> in <c>GetStoreSchemaVersionAsync</c>.
    ///
    /// <para>ABOVE the measured worst case, and this one does not grow with the store: both read
    /// <c>information_schema</c> / <c>has_table_privilege</c> only, touching no hypertable and scanning
    /// no data. The 85-sentinel schema probe measured 79 ms cold and 49 ms warm on the seeded V109 rig
    /// above; ten seconds is ~127x that. A store ten times larger does not move it, which is why this
    /// regime can sit far tighter than the interactive one despite having no budget either.</para>
    ///
    /// <para>BELOW the point where startup hangs. These are the first two statements after connect, in
    /// <c>MainWindow.OnLoaded</c>, before any timer starts and before the window is usable — and there
    /// is no splash to explain the wait. Ten seconds is twice the default connect budget
    /// (<c>ConnectionTimeoutSeconds</c> = 5) and bounded well under its 60 s ceiling, so a slow link
    /// cannot turn the gate into a silent hang.</para>
    ///
    /// <para>The asymmetry here is different from the other two regimes and is the reason this is its
    /// own constant: both probes CATCH their own failures. <c>GetStoreSchemaVersionAsync</c> fails open
    /// (returns null, so a healthy store is never blocked by a probe hiccup) and
    /// <c>DetectReadOnlyAsync</c> fails safe (records read-only, so the UI hides writes rather than
    /// dead-clicking a permission error). So a blown deadline here raises nothing — it silently
    /// MIS-CLASSIFIES the store, hiding every write affordance on a writable one. A visible,
    /// reconnectable mis-classification is the better trade against a startup that never finishes.</para>
    /// </summary>
    public const int ConnectGateSeconds = 10;
}

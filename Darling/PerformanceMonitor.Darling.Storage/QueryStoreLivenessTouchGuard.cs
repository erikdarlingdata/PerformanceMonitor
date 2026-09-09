/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one width of the liveness-touch guard, shared by every <c>last_seen</c> touch the Query Store fetch
/// probe issues: <see cref="QueryStorePlanMap.TouchAndProbeSql"/>'s <c>touched</c> CTE, that statement's
/// <c>dim_touch</c>, and <see cref="QueryStoreTextStore.TouchAndProbeSql"/>'s <c>touched</c> CTE.
///
/// <para><b>One constant, because the map row and the dimension row must not be able to age out at
/// different times.</b> <see cref="QueryStorePlanMap.TouchAndProbeSql"/> calls that structurally impossible
/// rather than carefully avoided, and separate literals per site make it neither — they are copies that
/// happen to agree, and moving some of them is precisely the divergence that claim rules out. The width is
/// spelled once here and interpolated into all three sites, so a partial migration cannot be expressed.
/// <c>QueryStoreTouchGuardSingleSourceTests</c> pins the single-sourcing rather than the value: it fails on
/// a hard-coded interval anywhere in either statement's source, on a guard site appearing or disappearing,
/// and on a width that stops following the margin it is derived from.</para>
///
/// <para><b>What the guard is.</b> A PER-ROW predicate, not a statement-level skip. A row that keeps being
/// referenced has its stamp refreshed at least once per guard window, so the width is exactly the bound on
/// how stale a live row's <c>last_seen</c> can be. Orphaning content needs a stamp as stale as the GC
/// horizon, which needs that many hours of NO references — the intended behaviour, and not something a
/// guard width can manufacture. Widening the guard therefore reduces WRITES and not examinations: the
/// verdict <c>SELECT</c> at the bottom of both statements joins the whole batch against the store and
/// returns <c>resolved</c>/<c>hash_stale</c> for every reference regardless of this predicate.</para>
///
/// <para><b>Derived from the MARGIN, not from the horizon — and that distinction is the whole design.</b>
/// <c>DarlingRetention.ComputeMapCutoff</c> prunes the map at
/// <c>now - (widestFactRetention + PruneMarginDays)</c> and <c>ComputeDimensionCutoff</c> takes the
/// dimension at <c>now - (widestFactRetention + ChunkIntervalDays + 1)</c>, and both of those trailing
/// margins are documented as covering this guard's staleness. The RETENTION term is the operator's: it
/// varies per collector, moves with the plan-content knob, and lends the guard nothing it can rely on. The
/// MARGIN is the part reserved for exactly this. So the width is a stated share of the margin and
/// deliberately does NOT scale with retention — a guard expressed as a fraction of the retention horizon
/// would widen whenever an operator widened retention, while the budget it actually spends from would not
/// move at all. That is a relationship that does not hold, dressed as a derivation.</para>
///
/// <para>Taken from the TIGHTEST margin among the tables this touch writes, because one shared width has to
/// be safe on all of them. Which tables those are is a stated criterion with a test behind it, not an
/// implied enumeration — see <see cref="StampSkewMarginDays"/>. Each table's cutoff also carries the
/// retention term on top of its margin, which is further room the guard does not count.</para>
/// </summary>
public static class QueryStoreLivenessTouchGuard
{
    /// <summary>
    /// The allowance the guard's staleness is sized against, in days: the SMALLEST <c>PruneMarginDays</c>
    /// declared by any table this guard's touch writes <c>last_seen</c> on, so one shared width is safe on
    /// all of them. Derived rather than restated, so tightening any of those margins narrows the guard
    /// instead of silently eating its headroom. It is a conservative PROXY for the room actually available
    /// rather than the room itself — see the third paragraph, which is the one that matters if you are
    /// changing this.
    ///
    /// <para><b>The INCLUSION CRITERION, because a <c>min</c> over an unstated enumeration is the next
    /// defect.</b> A table contributes a term exactly when this guard's touch writes its <c>last_seen</c>.
    /// That is three tables — <c>query_store_plan_map</c> and <c>query_plan_dim</c> from
    /// <see cref="QueryStorePlanMap.TouchAndProbeSql"/>, and <c>query_store_text</c> from
    /// <see cref="QueryStoreTextStore.TouchAndProbeSql"/> — and it is emphatically NOT "every table with a
    /// <c>PruneMarginDays</c>". Membership is enforced rather than asserted:
    /// <c>QueryStoreTouchGuardSingleSourceTests</c> derives the guarded store types by scanning the whole
    /// project for guard sites and requires the set of <c>PruneMarginDays</c> terms in this initializer to
    /// equal it, so a term that is wrong to include reds even when it cannot move the value.</para>
    ///
    /// <para><b>The anchor is a conservative PROXY for the room, and the precondition matters.</b>
    /// <c>PruneMarginDays</c> is the map's margin only while <c>ComputeMapCutoff</c>'s COUPLED arm wins. With
    /// the plan-content knob enabled — and its default is 21 — the dedicated arm governs and the map's cutoff
    /// is the knob itself with no margin term in it at all. So this constant is not "the room available"; it
    /// is the smallest stamp-trailing allowance the retention arithmetic names anywhere, and it is strictly
    /// SMALLER than the least room any reachable configuration leaves. Swept over the shipped arithmetic
    /// rather than argued, across dim-feeding retention (floored at 1 day by <c>Math.Max</c>, and at 1 day by
    /// <c>StoreConfigProvider.ValidRetention</c> before that) against the knob (off, or clamped to its 7–365
    /// range): the least room is TWO days, at retention 1 with the knob off, and it is 21 days in the default
    /// configuration. Anchoring on the named one-day allowance rather than on a floor computed from three
    /// interacting knobs is the deliberate trade: it keeps the derivation readable, and it keeps the guard
    /// inside the true room by a further factor of two even at the worst point.</para>
    ///
    /// <para><b>Only two of the three tables contribute a term, and the third needs none.</b>
    /// <c>query_plan_dim</c> has no <c>PruneMarginDays</c> of its own — its room above the fact horizon is
    /// <c>ChunkIntervalDays + 1</c> in <c>DarlingRetention.ComputeDimensionCutoff</c>, whose trailing day is
    /// documented there as covering this very guard. <see cref="QueryStorePlanMap.MarginOrderingHolds"/>
    /// already pins <c>PruneMarginDays &lt; ChunkIntervalDays + 1</c>, so a guard that fits inside the map's
    /// margin fits inside the dimension's by that existing invariant rather than by a second copy of the
    /// arithmetic. The chain is: guard ≤ map margin &lt; dim margin.</para>
    ///
    /// <para><b><see cref="PgStatementText.PruneMarginDays"/> is deliberately excluded, and it is the worked
    /// example the criterion exists for.</b> It is 2, so adding it could not change this value — which is
    /// exactly why leaving the criterion implicit was the hazard. <see cref="PgStatementText"/> has no guard
    /// site at all: its <c>UpsertSql</c> conflict arm advances <c>last_seen</c> on every conflict under a
    /// MONOTONICITY guard (<c>EXCLUDED.last_seen &gt;= …</c>), not a staleness one, so nothing about this
    /// width can make its stamp trail. Its margin is a fact-OUTLIVING margin — its own doc says "so text
    /// outlives the statistics rows that reference it" — which is a different job wearing the same constant
    /// name. The test asserts that file is inside the scanned population and outside the guarded set, so the
    /// exclusion is a measured fact rather than a claim about a file the scan might be blind to.</para>
    /// </summary>
    public const int StampSkewMarginDays =
        QueryStorePlanMap.PruneMarginDays < QueryStoreTextStore.PruneMarginDays
            ? QueryStorePlanMap.PruneMarginDays
            : QueryStoreTextStore.PruneMarginDays;

    /// <summary>The same margin in hours, which is the unit the guard is expressed in.</summary>
    public const int StampSkewMarginHours = StampSkewMarginDays * 24;

    /// <summary>
    /// What share of <see cref="StampSkewMarginHours"/> the guard is allowed to consume, as a divisor.
    ///
    /// <para><b>CHOSEN, not measured, and nothing measured picks it.</b> Every value from 1 to
    /// <see cref="StampSkewMarginHours"/> keeps the guard inside the margin, and the whole range sits orders
    /// of magnitude below the horizons seen in the field — on one production store the dimension held
    /// 15.58 days while the fact tables referencing these digests retained 4, roughly 11.6 days of slack on
    /// top of the margin, and only 21.2% of dimension rows had been touched in the last day at all, so most
    /// of them survive on the horizon rather than on liveness. Safety does not select a value here.</para>
    ///
    /// <para>What the divisor trades is WRITE VOLUME against stamp freshness. <c>last_seen</c> is indexed on
    /// all three touched tables and is the column the touch writes, so a heap-only update is impossible and
    /// every touch is a full non-HOT update: a new heap tuple, a new b-tree entry, a dead one left behind,
    /// WAL for both, and autovacuum work afterwards. Measured on one production store at a one-hour width,
    /// 22.1 M non-HOT <c>last_seen</c> updates a day across the three tables. The touch rate is inversely
    /// proportional to the width, so this divisor is also, to within the arrival pattern, the divisor on that
    /// rate.</para>
    ///
    /// <para>A quarter leaves 4x headroom inside the margin for the collection cadence, for clock skew
    /// between the service and the store, and for a sweep that runs late. There is no measurement that
    /// prefers 4 to 3 or to 6; it is a safety factor, and it is stated as one so nobody reads the width
    /// below as a measured quantity.</para>
    /// </summary>
    public const int MarginShareDivisor = 4;

    /// <summary>
    /// The guard width in hours: <see cref="StampSkewMarginHours"/> divided by
    /// <see cref="MarginShareDivisor"/>. Integer division, so <see cref="GuardStaysInsideStampSkewMargin"/>
    /// is what refuses a divisor that does not divide the margin cleanly rather than letting it truncate
    /// quietly.
    /// </summary>
    public const int GuardHours = StampSkewMarginHours / MarginShareDivisor;

    /// <summary>
    /// The SQL fragment the three guard sites interpolate. Plural unconditionally — PostgreSQL accepts
    /// <c>interval '1 hours'</c>, so the rendering needs no singular arm that only one value would ever
    /// exercise.
    /// </summary>
    public static readonly string GuardInterval =
        "interval '" + GuardHours.ToString(CultureInfo.InvariantCulture) + " hours'";

    /// <summary>
    /// The invariant the derivation has to satisfy: the guard is at least an hour, it fits inside
    /// <see cref="StampSkewMarginHours"/>, and <see cref="MarginShareDivisor"/> divides the margin exactly
    /// so the stated share is the share actually taken.
    ///
    /// <para>A function rather than a comment for the same reason as
    /// <see cref="QueryStorePlanMap.MarginOrderingHolds"/>: a later change to either prune margin, or to the
    /// divisor, fails a test instead of quietly spending headroom the margin was reserving.</para>
    /// </summary>
    public static bool GuardStaysInsideStampSkewMargin() =>
        GuardHours >= 1
        && GuardHours * MarginShareDivisor == StampSkewMarginHours;
}

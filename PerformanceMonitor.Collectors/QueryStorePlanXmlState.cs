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
/// What one plan-fetch pass earned: the watermark to persist, and whether the pass's rows actually arrived in
/// the plan_id order its <c>ORDER BY</c> promises (#2210). One value rather than two calls so a caller cannot
/// take the watermark without being handed the reason it may not have moved — the ordering guard is only
/// useful if the violation gets LOGGED, and a signal a caller can forget to ask for is one that eventually
/// nobody asks for.
/// </summary>
/// <param name="Watermark">The plan_id to persist; the standing value when the pass earned no advance.</param>
/// <param name="ArrivedInPlanIdOrder">False when a descent was seen, meaning the advance was abandoned and the
/// caller should log a precondition violation rather than treat a static watermark as a quiet pass.</param>
public readonly record struct PlanWatermarkAdvance(long Watermark, bool ArrivedInPlanIdOrder);

/// <summary>
/// The persisted per-database plan-XML watermark (#2164) — the highest <c>plan_id</c> whose execution-plan
/// XML has actually been stored for a database, so collection stops re-shipping plans the store already
/// holds. 97% of the plan XML shipped in a three-hour fleet window was for plans held for over an hour, and
/// because streaming rows is 94-97% of a pass and costs per-row LOB bytes, not fetching beats fetching less.
///
/// <para>Owned by the HOST under its own <see cref="StateCollectorName"/>, exactly like
/// <see cref="QueryStoreBackfillState"/> and for the same reason: the query_store DEFINITION keeps declaring
/// no state keys, so <c>CollectorStateContractTests</c> stays honest and adding per-database state does not
/// silently become a two-host contract change. The keys are dynamic (one per database), which the host's
/// state read supports because it loads every row for a collector name rather than a declared key list — the
/// definition's <c>StateKeys</c> could not express these anyway.</para>
///
/// <para>Lives in the shared collectors project rather than either host because it is watermark-shaped state
/// that must decode identically wherever it is read: a row written by Darling today has to keep meaning the
/// same thing after an upgrade, and Lite reads the same definition.</para>
/// </summary>
public static class QueryStorePlanXmlState
{
    /// <summary>
    /// The collector_state owner name for these rows — deliberately NOT the query_store definition's name,
    /// which is the seam that lets the definition declare no state keys while the host still persists
    /// per-database state for it.
    /// </summary>
    public const string StateCollectorName = "query_store_plan_xml";

    /// <summary>
    /// State key prefix; the remainder is the database name, because <c>plan_id</c> is only unique within one
    /// database's Query Store and means nothing across databases.
    /// </summary>
    public const string WatermarkKeyPrefix = "planwm:";

    /// <summary>
    /// The target period for ONE FULL RE-VERIFICATION SWEEP of a database's plans — not an expiry, and
    /// emphatically not a refetch trigger. <c>QueryStorePlanMap.CursorSliceWidth</c> derives the cursor's
    /// per-pass id slice from it, so this constant sets the PACE of re-verification rather than a deadline
    /// anything has to beat.
    ///
    /// <para>It used to mean "after this long, drop the watermark to zero and refetch every plan's XML", and
    /// that was measured to be unreachable on the catalogs it mattered most for: 2.2-15.1 GB of plan XML per
    /// catalog on the production fleet, which at a 12 MB budget and 5-minute cadence is 15.9 to 107.5 HOURS of
    /// walking — so a 1-day expiry meant the biggest catalogs restarted from their lowest plan_id forever and
    /// never once reached their newest plans. The optimization could not converge on exactly the databases it
    /// existed for. Raising the number does not fix that shape; the sweep has to stop being a byte-volume walk.
    /// It now is one: hash-only, bounded by ROW count (77k ids at ~270 per pass), re-fetching XML solely where
    /// something actually changed.</para>
    ///
    /// <para>THREE MECHANISMS, each owning one failure, none of them this constant on its own:</para>
    ///
    /// <para>• <b>A Query Store reset</b> — the map's absent-content signal on the runtime stream
    /// (<c>TouchSql</c>), recovering in one cycle. The ONLY thing permitted to zero a watermark.<br/>
    /// • <b>Dormant plans</b> — the cursor finds a map row ABSENT at an id the watermark already passed, and
    /// fetches it. No heuristic separates dormancy from a reset, because it does not have to: mass absence is
    /// caught wholesale by the reset arm within a cycle.<br/>
    /// • <b>In-place XML rewrites</b> — the cursor finds a stored <c>plan_hash</c> that DIFFERS from the live
    /// one and re-fetches that plan alone. Across a day of fleet data this was 0 of 38,420 plan_ids, which is
    /// why paying for it with a full walk was the wrong trade.</para>
    ///
    /// <para>ONE DAY remains the right pace for a hash-only sweep, for the reason the old value was chosen and
    /// for a new one: the redundancy removed is per-pass, and a sweep bounded by rows rather than bytes finishes
    /// comfortably inside a day on every catalog measured.</para>
    ///
    /// <para>Historical note on the three guarantees the old expiry claimed, kept because the reasoning still
    /// explains why each mechanism above exists:</para>
    ///
    /// <para>1. In-place XML rewrites. plan_id is monotonic and a plan's identity is stable (0 of 38,420
    /// plan_ids changed their plan hash in a day of fleet data), but nothing guarantees a feature like
    /// memory-grant feedback never edits grant values inside the XML of a plan that keeps its id. The
    /// expiry means that question does not have to be load-bearing.</para>
    ///
    /// <para>2. A Query Store RESET. Clearing Query Store restarts plan_id at 1, so every new plan sorts
    /// below a stale watermark and its XML would be suppressed. This is NOT what covers that any more — the
    /// tempting detection test ("the highest plan_id seen this pass is below the standing watermark") is TRUE in
    /// any ordinary quiet window, so it would drop the watermark constantly; but the map gives the payload a
    /// signal it never had on its own. A plan_id at or below the watermark whose content the store has never
    /// resolved is a RENUMBERED plan, which "no new plans this window" cannot produce, and
    /// <c>QueryStorePlanMap.TouchSql</c> surfaces exactly those rows from the batch join it already performs.
    /// That is the reset mechanism, it recovers in ONE CYCLE, and it is the only thing permitted to zero the
    /// watermark.</para>
    ///
    /// <para>3. The dormant-plan gap: plan_id is monotonic in COMPILE order, which is not the same as "we
    /// have stored it", so a plan compiled before monitoring began and dormant through every collected
    /// window arrives below the watermark.</para>
    ///
    /// <para>ONE DAY, not a week: the redundancy removed is per-pass (a 15-minute cadence re-ships a plan
    /// ~96 times a day), so a daily full fetch already eliminates ~99% of it and a weekly one adds almost
    /// nothing — while buying 7x the exposure on all three guarantees above, including a reset blackout
    /// measured in days.</para>
    ///
    /// <para>That trade omits a term, named here because it is the one that will move this number: expiry
    /// resets the watermark to zero, so "one expensive pass" is really a full budgeted catalog WALK. At a 12 MB
    /// ship budget an 82k-plan catalog spends most of a day walking, which means the largest catalogs — the ones
    /// this optimization matters most for — are close to continuously refetching, and shortening the horizon
    /// makes that worse rather than safer. Once the stream signal above covers resets, the walk buys only the
    /// in-place-rewrite case (speculative: 0 of 38,420 plan_ids changed hash in a day of fleet data) and dormant
    /// plans (real, small), and a longer horizon is likely correct. Measure the walk cost on the worst catalog
    /// before changing it.</para>
    /// </summary>
    public static readonly TimeSpan RefreshAfter = TimeSpan.FromDays(1);

    /// <summary>The state key for one database.</summary>
    public static string KeyFor(string databaseName) => WatermarkKeyPrefix + databaseName;

    /// <summary>
    /// The average plan size assumed for a database with no previous pass to learn from. Deliberately near the
    /// LARGE end of the measured fleet range (per-quartile averages of 162 / 80 / 39 / 15 KB across 2,166
    /// budget-cut passes on a 52-server fleet), because the estimate feeds a DIVISOR: over-estimating plan size
    /// yields a SMALL candidate window, and small is the safe direction. A window that is too small merely
    /// advances the watermark more slowly; one that is too large decompresses plans it will never ship, which
    /// is the exact cost the window exists to bound.
    /// </summary>
    public const long FirstContactAvgPlanBytes = 160L * 1024L;

    /// <summary>
    /// Floor on the candidate window, so progress is always possible. Even if the observed average is wildly
    /// over-stated — one enormous plan in a quiet pass — a database must still be able to walk its catalog.
    /// </summary>
    public const int MinCandidatePlans = 32;

    /// <summary>
    /// Ceiling on the candidate window. The smallest measured quartile average (15 KB) puts a 12 MB budget at
    /// ~820 plans, so this leaves headroom for genuinely tiny plans while refusing to let a near-zero estimate
    /// turn the window back into "the whole catalog" — which is the first-contact trap this window exists to
    /// prevent.
    /// </summary>
    public const int MaxCandidatePlans = 2048;

    /// <summary>
    /// How far past the budget the window reaches, in expected plans. The window is the COARSE bound and the
    /// running byte total is the exact one, so the margin only has to cover the estimate being wrong in the
    /// "plans are smaller than expected" direction — where extra plans genuinely fit the budget.
    ///
    /// <para>Kept modest at 1.5x because margin is not free: a windowed running total is evaluated over every
    /// row IN the window, so the server decompresses all K plans to compute it whether the budget is reached at
    /// plan 5 or plan 500. Margin buys reachability and costs decompression, which is why the estimate errs
    /// large and the margin stays small.</para>
    /// </summary>
    public const double CandidatePlanMargin = 1.5;

    /// <summary>
    /// The per-database average plan size to carry into the next pass, from a pass's own totals — free, because
    /// both numbers are already in hand when a pass ends, and no probe can measure plan size without
    /// decompressing the plans. Zero rows yields null: a quiet pass teaches nothing about plan size and must
    /// leave the previous estimate standing rather than replace it with a divide-by-zero fallback.
    /// </summary>
    public static long? ObservedAvgPlanBytes(long planBytesShipped, int plansShipped) =>
        plansShipped <= 0 || planBytesShipped <= 0 ? null : planBytesShipped / plansShipped;

    /// <summary>
    /// One database's carried plan-size estimate (#2312 Finding 1): the observed average the next pass
    /// sizes its candidate window from, and whether the walk is mid-backlog (which biases the sample
    /// small — the <see cref="CandidatePlanCount(long?, long, bool, out bool)"/> overload floors it).
    /// <c>AvgBytes</c> of zero means "never learned"; callers pass null to CandidatePlanCount then.
    /// </summary>
    public readonly record struct PlanSizeEstimate(long AvgBytes, bool CatchUpInProgress);

    /// <summary>
    /// Folds one pass's outcome into the carried estimate. The rules, each load-bearing:
    /// a pass that shipped nothing teaches nothing about size (previous average stands) but DOES
    /// prove the walk is caught up (nothing qualified past the watermark), so catch-up clears;
    /// a pass cut by either bound — the candidate window consumed or the byte budget reached —
    /// proves a backlog remains, so catch-up sets; an ordinary partial pass learns its average and
    /// clears catch-up. Pure so the table is pinnable; the runner owns only the dictionary.
    ///
    /// <para>Two counts on purpose (the review catch): <paramref name="plansShipped"/> is the RAW
    /// row count — NULL-XML plans deliberately ship as rows so the watermark can pass unpersistable
    /// plans, and the window/catch-up comparison wants exactly that count. But the average's divisor
    /// is <paramref name="plansMeasured"/>, the rows that actually carried XML: dividing real bytes
    /// by a NULL-inflated count would understate the average, which INFLATES the next window — the
    /// unsafe direction the whole estimator errs away from.</para>
    /// </summary>
    public static PlanSizeEstimate Learn(
        PlanSizeEstimate previous, long bytesShipped, int plansShipped, int plansMeasured, int candidateWindow, long budgetBytes)
    {
        if (plansShipped <= 0)
        {
            return new PlanSizeEstimate(previous.AvgBytes, CatchUpInProgress: false);
        }

        var catchUp = plansShipped >= candidateWindow || bytesShipped >= budgetBytes;
        var avg = ObservedAvgPlanBytes(bytesShipped, plansMeasured) ?? previous.AvgBytes;
        return new PlanSizeEstimate(avg, catchUp);
    }

    /// <summary>
    /// How many plans one pass may CONSIDER: enough that the byte budget is the binding constraint, few enough
    /// that the server never decompresses a catalog to discover which plans fit.
    ///
    /// <para>This is the trap mitigation. <c>SUM(DATALENGTH(query_plan)) OVER (ORDER BY plan_id)</c> has to
    /// materialize the XML to measure it — <c>query_store_plan.query_plan</c> is decompressed BY the TVF on
    /// access — so an unbounded candidate set pays the whole catalog's decompression to enforce a budget meant
    /// to prevent exactly that. Bounding the window first on the cheap columns costs nothing and caps it.</para>
    ///
    /// <para>Per-database rather than one fleet constant because measured plan size spans 11x (162 KB to 15 KB
    /// by quartile). A constant sized for the small-plan end (~820) would decompress ~134 MB to ship 12 MB on
    /// the large-plan end; one sized for the large end would never reach the budget on the small end. No single
    /// value is both, which is what makes this adaptive rather than tunable.</para>
    ///
    /// <para><paramref name="clamped"/> reports that a bound was applied, so the caller can LOG it. A window
    /// silently pinned at its ceiling looks identical to one that fit, and that is how a cap becomes invisible.</para>
    /// </summary>
    public static int CandidatePlanCount(long? observedAvgPlanBytes, long budgetBytes, out bool clamped)
        => CandidatePlanCount(observedAvgPlanBytes, budgetBytes, catchUpInProgress: false, out clamped);

    /// <summary>
    /// As above, with the catch-up guard: while <paramref name="catchUpInProgress"/> — the watermark still below
    /// the server's newest plan_id — the observed average is FLOORED at
    /// <see cref="FirstContactAvgPlanBytes"/> rather than trusted.
    ///
    /// <para>The estimator is biased during exactly that window, and measurably so: the average is computed over
    /// the plans a pass actually shipped, which under plan_id-ascending shipping are the OLDEST ids in the
    /// catalog. On one production catalog the plans the fetch shipped averaged 15 KB while the newest 300 plans
    /// in the same catalog averaged 46 KB — a 3x under-estimate, which inflates K threefold and decompresses
    /// that much more than the budget can ship. Flooring at the seed applies the same over-estimate-is-safe
    /// logic the seed itself rests on, for the one window where the sample is known to be unrepresentative.
    /// Once the first walk has converged the sample spans the catalog and the observed average is trusted.</para>
    /// </summary>
    public static int CandidatePlanCount(long? observedAvgPlanBytes, long budgetBytes, bool catchUpInProgress, out bool clamped)
    {
        var avg = observedAvgPlanBytes is long observed && observed > 0 ? observed : FirstContactAvgPlanBytes;

        if (catchUpInProgress && avg < FirstContactAvgPlanBytes)
        {
            avg = FirstContactAvgPlanBytes;
        }

        if (budgetBytes <= 0)
        {
            clamped = true;
            return MinCandidatePlans;
        }

        /* double for the margin, then ONE cap before the cast — at int.MaxValue rather than at
           MaxCandidatePlans, deliberately. Capping at the bound here would pre-clamp the value and leave the
           comparison below unable to tell a clamp from a natural landing, which is the false positive this
           reports on. int.MaxValue only guards the cast itself, since the budget is operator input. */
        var wanted = (double)budgetBytes / avg * CandidatePlanMargin;
        var unclamped = wanted >= int.MaxValue ? int.MaxValue : (int)Math.Ceiling(wanted);
        var bounded = Math.Clamp(unclamped, MinCandidatePlans, MaxCandidatePlans);

        /* Reports that a bound CHANGED the answer, not that the answer happens to equal one. A window whose
           measured size lands naturally on 32 or 2048 was sized by the measurement and needs no log line; saying
           "clamped" there is a false positive against this contract, and a caller that logs on it teaches its
           reader to ignore the message. */
        clamped = bounded != unclamped;
        return bounded;
    }

    /// <summary>
    /// The watermark a pass earned, given the plan_ids whose XML actually landed. Under plan_id-ordered
    /// shipping a budget cut truncates a SUFFIX, so the highest landed id is safe to keep even from a cut pass
    /// — which is the whole point of the reordering (#2210): the previous design shipped in
    /// <c>last_execution_time</c> order, where a cut left an arbitrary SUBSET and no value was safe, so the
    /// watermark could not advance on 97.8% of passes and therefore never advanced at all.
    ///
    /// <para>Defensive on the precondition rather than trusting it: a DESCENT anywhere in
    /// <paramref name="landedPlanIdsInOrder"/> abandons the advance entirely and reports itself through
    /// <see cref="PlanWatermarkAdvance.ArrivedInPlanIdOrder"/>. Honouring the leading ascending run instead
    /// looks safer and is not — given <c>{105, 101}</c> it would advance to 105, and once ordering is broken
    /// there is no longer any basis for inferring that every SELECTED plan below 105 landed, so a plan whose
    /// XML never arrived gets suppressed until the refresh horizon. Ordering is what makes a cut a suffix; with
    /// it gone the pass has earned nothing, and one lost pass of progress is the cheap side of that trade.</para>
    ///
    /// <para>The verdict and the signal come back TOGETHER, in one value, deliberately. Two separate functions
    /// would let a caller take the watermark and never ask whether ordering held — a watermark that quietly
    /// stops moving with nothing logged, which is precisely the failure this whole redesign exists to correct
    /// and would be a poor thing to reintroduce one level up.</para>
    ///
    /// <para>Never moves backward: a pass that lands nothing, or only ids at or below the standing watermark,
    /// returns the standing value. Lowering it would refetch the catalog, and "no new plans this window" is an
    /// ordinary quiet pass, not a reset — the reset signal lives on the runtime stream, where a plan at or below
    /// the watermark that the store has never resolved can actually be observed.</para>
    /// </summary>
    public static PlanWatermarkAdvance AdvanceWatermark(long standing, IReadOnlyList<long> landedPlanIdsInOrder)
    {
        if (landedPlanIdsInOrder is null || landedPlanIdsInOrder.Count == 0)
        {
            return new PlanWatermarkAdvance(standing, true);
        }

        var advanced = standing;
        var previous = long.MinValue;

        foreach (var planId in landedPlanIdsInOrder)
        {
            if (planId < previous)
            {
                return new PlanWatermarkAdvance(standing, false);
            }

            previous = planId;

            if (planId > advanced)
            {
                advanced = planId;
            }
        }

        return new PlanWatermarkAdvance(advanced, true);
    }

    /// <summary>
    /// The watermark to apply for one database, or 0 — meaning "fetch every plan's XML" — for an absent,
    /// malformed, EXPIRED or future-stamped one. Zero is the documented conservative path: absent is what a
    /// first run, a restarted host and a broken store all look like, and all three must refetch rather than
    /// skip. A future stamp means the clock moved backwards, which would otherwise pin the watermark for as
    /// long as the skew lasts.
    /// </summary>
    public static long Resolve(IReadOnlyDictionary<string, string> state, string databaseName, DateTime utcNow)
    {
        if (!TryParse(state, databaseName, out var planId, out var stamped))
        {
            return 0;
        }

        if (stamped > utcNow || utcNow - stamped >= RefreshAfter)
        {
            return 0;
        }

        return planId;
    }

    /// <summary>
    /// The stored stamp — when this database last did a FULL plan-XML fetch — with no expiry applied, so a
    /// write-back can carry it forward across an advance instead of renewing the refresh horizon. Null when
    /// there is nothing parseable to carry, which the caller treats as "stamp now".
    /// </summary>
    public static DateTime? ResolveStamp(IReadOnlyDictionary<string, string> state, string databaseName) =>
        TryParse(state, databaseName, out _, out var stamped) ? stamped : null;

    /// <summary>
    /// Formats a watermark for storage: highest stored plan_id plus the stamp dating the last FULL fetch.
    /// The stamp is a parameter rather than "now" precisely because it must survive advances — re-stamping
    /// on every advance would push the horizon out forever on any database that keeps compiling plans, which
    /// is the busy ones where a stale plan matters most, and the bounded refresh would never fire.
    /// </summary>
    public static string Format(long planId, DateTime fullFetchAtUtc) =>
        planId.ToString(CultureInfo.InvariantCulture) + ":" +
        new DateTimeOffset(DateTime.SpecifyKind(fullFetchAtUtc, DateTimeKind.Utc)).ToUnixTimeSeconds()
            .ToString(CultureInfo.InvariantCulture);

    private static bool TryParse(
        IReadOnlyDictionary<string, string> state, string databaseName, out long planId, out DateTime stamped)
    {
        planId = 0;
        stamped = default;

        if (state is null || !state.TryGetValue(KeyFor(databaseName), out var raw) || string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        var parts = raw.Split(':');
        if (parts.Length != 2
            || !long.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out planId)
            || !long.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var stampedUnix)
            || planId <= 0)
        {
            planId = 0;
            return false;
        }

        stamped = DateTimeOffset.FromUnixTimeSeconds(stampedUnix).UtcDateTime;
        return true;
    }
}

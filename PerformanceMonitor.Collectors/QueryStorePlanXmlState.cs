/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-database SIZING for the plan-XML fetch (#2312 Finding 1, wired in #2322): how many plans one pass may
/// hand <see cref="QueryStoreCollector.BuildPlanFetchByIdsQuery"/>, learned from each database's own
/// bytes-per-plan rather than a fleet constant, because measured plan size spans 11x across databases
/// (162 KB to 15 KB by quartile) and no single value is right at both ends.
///
/// <para><b>What this class no longer is.</b> Through #2210 it owned the persisted per-database plan-id
/// WATERMARK (<c>planwm:</c> under the <c>query_store_plan_xml</c> state owner) — the resume point for a
/// budgeted whole-catalog walk, with a daily refresh expiry standing in for a re-verify cursor that was
/// designed but never wired. #2312 Finding 4 measured what that actually did in production: catalogs whose
/// full walk needs more than a day expired MID-walk, restarted from plan_id 0, and looped the full catalog
/// fetch forever. The fetch is now activity-driven — the cycle's collected rows name their plans, the STORE
/// answers which are missing (<c>QueryStorePlanMap</c>'s touch-and-probe), and only those are fetched — so
/// there is no watermark, no expiry, and no state rows. The retired <c>planwm:</c>/<c>textwm:</c> rows are
/// deleted once by the V77 migration.</para>
///
/// <para>What remains is the sizing estimator, which still matters: the missing set of a first-contact
/// database is its whole catalog, and the fetch's running byte total has to DECOMPRESS every plan it
/// considers to measure it, so the id list handed to one pass must be capped near what the byte budget can
/// actually ship. Lives in the shared collectors project because it is pure arithmetic pinned by tests in
/// both hosts' suites.</para>
/// </summary>
public static class QueryStorePlanXmlState
{
    /// <summary>
    /// The average plan size assumed for a database with no previous pass to learn from. Deliberately near the
    /// LARGE end of the measured fleet range (per-quartile averages of 162 / 80 / 39 / 15 KB across 2,166
    /// budget-cut passes on a 52-server fleet), because the estimate feeds a DIVISOR: over-estimating plan size
    /// yields a SMALL candidate cap, and small is the safe direction. A cap that is too small merely spreads
    /// the catch-up across more cycles; one that is too large decompresses plans it will never ship, which is
    /// the exact cost the cap exists to bound.
    /// </summary>
    public const long FirstContactAvgPlanBytes = 160L * 1024L;

    /// <summary>
    /// Floor on the candidate cap, so progress is always possible. Even if the observed average is wildly
    /// over-stated — one enormous plan in a quiet pass — a database must still be able to work off its
    /// missing set.
    /// </summary>
    public const int MinCandidatePlans = 32;

    /// <summary>
    /// Ceiling on the candidate cap. The smallest measured quartile average (15 KB) puts a 12 MB budget at
    /// ~820 plans, so this leaves headroom for genuinely tiny plans while refusing to let a near-zero estimate
    /// turn one pass back into "decompress the whole catalog" — which is the first-contact trap the cap exists
    /// to prevent.
    /// </summary>
    public const int MaxCandidatePlans = 2048;

    /// <summary>
    /// #2683: a store whose fetch stays catch-up-clamped for this many CONSECUTIVE passes is treated as
    /// RUNAWAY — its plan population churns faster than any pass can drain (a tenant generating tens of
    /// thousands of plans/day from ad-hoc/unparameterized SQL: one production catalog held 100k plans and
    /// added ~5k/hour, so the fetch clamped at <see cref="MaxCandidatePlans"/> every pass indefinitely,
    /// spending ~40s decompressing plan XML that had aged out of cache before it was fetched). Chasing every
    /// plan is futile there and just makes the collector the hog it must never be on a stock Query Store.
    /// Past this streak the per-pass ceiling drops to <see cref="RunawayCandidatePlans"/> so the collection
    /// body stays short instead of grinding at the full cap forever. A pass that finally DRAINS (ships fewer
    /// than the cap and under budget) resets the streak — so a legitimately-heavy store catching up keeps the
    /// full cap and is never throttled.
    /// </summary>
    public const int RunawayClampedStreakThreshold = 24;

    /// <summary>The reduced per-pass ceiling for a runaway store (see <see cref="RunawayClampedStreakThreshold"/>).</summary>
    public const int RunawayCandidatePlans = 512;

    /// <summary>
    /// #2683b: how many CONSECUTIVE drained passes DISARM the sticky runaway flag once it is set. This is the
    /// hysteresis half — arming on sustained clamping was not enough on its own, because a runaway store's
    /// candidate-count estimator OSCILLATES: a small-capped pass "drains" (ships fewer than its cap), which
    /// flipped the old per-pass streak back to zero, so the next pass trusted the small average, capped to
    /// thousands, and spiked to 60–250s before re-clamping — measured on AYR AFTER it had already been flagged.
    /// Requiring this many drains IN A ROW to clear means one oscillation drain never disarms it; only a store
    /// that has genuinely caught up (drains continuously) clears and regains the full cap.
    /// </summary>
    public const int RunawayClearDrainedThreshold = 24;

    /// <summary>
    /// How far past the budget the cap reaches, in expected plans. The cap is the COARSE bound and the
    /// running byte total is the exact one, so the margin only has to cover the estimate being wrong in the
    /// "plans are smaller than expected" direction — where extra plans genuinely fit the budget.
    ///
    /// <para>Kept modest at 1.5x because margin is not free: a windowed running total is evaluated over every
    /// row handed to the fetch, so the server decompresses all of them to compute it whether the budget is
    /// reached at plan 5 or plan 500. Margin buys reachability and costs decompression, which is why the
    /// estimate errs large and the margin stays small.</para>
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
    /// caps its id list from, and whether the fetch is mid-backlog (which biases the sample small — the
    /// <see cref="CandidatePlanCount(long?, long, bool, out bool)"/> overload floors it).
    /// <c>AvgBytes</c> of zero means "never learned"; callers pass null to CandidatePlanCount then.
    /// </summary>
    public readonly record struct PlanSizeEstimate(long AvgBytes, bool CatchUpInProgress, int ClampedStreak = 0, int DrainedStreak = 0, bool Runaway = false);

    /// <summary>
    /// Folds one pass's outcome into the carried estimate. The rules, each load-bearing:
    /// a pass that shipped nothing teaches nothing about size (previous average stands) but DOES
    /// prove the fetch is caught up (nothing was missing, or nothing fit), so catch-up clears;
    /// a pass cut by either bound — the candidate cap consumed or the byte budget reached —
    /// proves a backlog remains, so catch-up sets; an ordinary partial pass learns its average and
    /// clears catch-up. Pure so the table is pinnable; the runner owns only the dictionary.
    ///
    /// <para>Two counts on purpose (the review catch): <paramref name="plansShipped"/> is the RAW
    /// row count — NULL-XML plans deliberately ship as rows so the store can record the content-less
    /// marker, and the cap/catch-up comparison wants exactly that count. But the average's divisor
    /// is <paramref name="plansMeasured"/>, the rows that actually carried XML: dividing real bytes
    /// by a NULL-inflated count would understate the average, which INFLATES the next cap — the
    /// unsafe direction the whole estimator errs away from.</para>
    /// </summary>
    public static PlanSizeEstimate Learn(
        PlanSizeEstimate previous, long bytesShipped, int plansShipped, int plansMeasured, int candidateWindow, long budgetBytes)
    {
        // A pass that hit a bound (clamped) proves a backlog remains — catch-up. A pass that ships nothing, or
        // fewer than its cap and under budget, proves caught-up — a DRAIN, and teaches no size (previous
        // average stands). #2683b tracks both as consecutive streaks and arms/disarms a STICKY runaway flag.
        var catchUp = plansShipped > 0 && (plansShipped >= candidateWindow || bytesShipped >= budgetBytes);
        var avg = plansShipped > 0
            ? ObservedAvgPlanBytes(bytesShipped, plansMeasured) ?? previous.AvgBytes
            : previous.AvgBytes;

        var clampedStreak = catchUp ? previous.ClampedStreak + 1 : 0;
        var drainedStreak = catchUp ? 0 : previous.DrainedStreak + 1;
        // #2683b HYSTERESIS: ARM runaway on RunawayClampedStreakThreshold consecutive clamps; DISARM only on
        // RunawayClearDrainedThreshold consecutive drains. #2683 reset a single per-pass streak on any drain,
        // but a runaway store's estimator OSCILLATES (a small-capped pass drains, the next trusts the small
        // average and caps to thousands), so the flag never held and the store spiked back to 60–250s. The
        // sticky flag keeps it capped through the oscillation; only a store that genuinely catches up (drains
        // continuously) clears it and regains the full cap.
        var runaway = previous.Runaway
            ? drainedStreak < RunawayClearDrainedThreshold
            : clampedStreak >= RunawayClampedStreakThreshold;
        return new PlanSizeEstimate(avg, catchUp, clampedStreak, drainedStreak, runaway);
    }

    /// <summary>
    /// How many plans one pass may CONSIDER: enough that the byte budget is the binding constraint, few enough
    /// that the server never decompresses a catalog to discover which plans fit.
    ///
    /// <para>This is the trap mitigation. <c>SUM(DATALENGTH(query_plan)) OVER (ORDER BY plan_id)</c> has to
    /// materialize the XML to measure it — <c>query_store_plan.query_plan</c> is decompressed BY the TVF on
    /// access — so an unbounded id list pays the whole missing set's decompression to enforce a budget meant
    /// to prevent exactly that. Capping the list first on the cheap side costs nothing.</para>
    ///
    /// <para>Per-database rather than one fleet constant because measured plan size spans 11x (162 KB to 15 KB
    /// by quartile). A constant sized for the small-plan end (~820) would decompress ~134 MB to ship 12 MB on
    /// the large-plan end; one sized for the large end would never reach the budget on the small end. No single
    /// value is both, which is what makes this adaptive rather than tunable.</para>
    ///
    /// <para><paramref name="clamped"/> reports that a bound was applied, so the caller can LOG it. A cap
    /// silently pinned at its ceiling looks identical to one that fit, and that is how a cap becomes invisible.</para>
    /// </summary>
    public static int CandidatePlanCount(long? observedAvgPlanBytes, long budgetBytes, out bool clamped)
        => CandidatePlanCount(observedAvgPlanBytes, budgetBytes, catchUpInProgress: false, out clamped);

    /// <summary>
    /// As above, with the catch-up guard: while <paramref name="catchUpInProgress"/> — the missing set still
    /// larger than one pass can ship — the observed average is FLOORED at
    /// <see cref="FirstContactAvgPlanBytes"/> rather than trusted.
    ///
    /// <para>The estimator is biased during exactly that window, and measurably so: the average is computed over
    /// the plans a pass actually shipped, which under plan_id-ascending shipping are the OLDEST ids in the
    /// missing set. On one production catalog the plans the fetch shipped averaged 15 KB while the newest 300
    /// plans in the same catalog averaged 46 KB — a 3x under-estimate, which inflates the cap threefold and
    /// decompresses that much more than the budget can ship. Flooring at the seed applies the same
    /// over-estimate-is-safe logic the seed itself rests on, for the one window where the sample is known to be
    /// unrepresentative. Once caught up the sample spans the catalog and the observed average is trusted.</para>
    /// </summary>
    public static int CandidatePlanCount(long? observedAvgPlanBytes, long budgetBytes, bool catchUpInProgress, out bool clamped, bool runaway = false)
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
        var unclamped = wanted >= int.MaxValue ? int.MaxValue : (int)System.Math.Ceiling(wanted);
        // #2683/#2683b: a runaway store (the STICKY flag — armed by sustained clamping, held through the
        // estimator's oscillation until sustained draining) gets a reduced ceiling so its fetch stops grinding
        // at the full cap; a store legitimately catching up (not flagged) keeps MaxCandidatePlans.
        var ceiling = runaway ? RunawayCandidatePlans : MaxCandidatePlans;
        var bounded = System.Math.Clamp(unclamped, MinCandidatePlans, ceiling);

        /* Reports that a bound CHANGED the answer, not that the answer happens to equal one. A cap whose
           measured size lands naturally on 32 or 2048 was sized by the measurement and needs no log line; saying
           "clamped" there is a false positive against this contract, and a caller that logs on it teaches its
           reader to ignore the message. */
        clamped = bounded != unclamped;
        return bounded;
    }
}

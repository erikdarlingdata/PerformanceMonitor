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
    /// Ceiling on the candidate cap — FLAT, not adaptive, and deliberately no longer sized to what the
    /// smallest measured plans could theoretically use.
    ///
    /// <para><b>#2683/#2685 tried the adaptive alternative and it failed at the moment it mattered.</b> Those
    /// issues detected a "runaway" store (one whose plan population churns faster than any pass can drain)
    /// by watching for 24 CONSECUTIVE passes clamped at a high ceiling (2048), then dropping to a low one
    /// (512) with hysteresis to survive the estimator's oscillation. Verified against the 2026-08-29 peak
    /// window on OMEGA — the pathological store the detector was built for: the log showed ZERO "candidate cap
    /// clamped" lines and ZERO RUNAWAY/Holding lines during a stretch where plan_fetch still ran 38–73s
    /// across twelve straight passes. The learned average happened to keep "wanted" just under the 2048
    /// ceiling, so no pass ever clamped, the streak never advanced, and the throttle that existed
    /// specifically for this store never engaged when the store needed it.</para>
    ///
    /// <para>A flat ceiling doesn't need to detect anything — it is always in effect, so this failure mode
    /// cannot occur. The trade is the one this file's own philosophy already accepts: "a cap that is too
    /// small merely spreads the catch-up across more cycles" (see <see cref="FirstContactAvgPlanBytes"/>) —
    /// a store with many small plans converges over more passes instead of fewer, which is acceptable
    /// specifically for query_store because it serves HISTORICAL analysis, not in-the-moment
    /// troubleshooting (unlike query_stats/procedure_stats, which stay fully adaptive because they ARE used
    /// live). 512 is the value #2683 already proved safe as a throttled ceiling — it just no longer waits
    /// for detection to apply it.</para>
    /// </summary>
    public const int MaxCandidatePlans = 512;

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
    ///
    /// <para><c>ConsecutiveFetchFailures</c> (#2776) is the backoff counter — how many passes in a row have
    /// thrown before settling their debt. It rides in this record rather than a dictionary of its own so it
    /// is carried and pruned with the estimate it belongs to; a parallel dictionary would be a second thing
    /// to remember to clear. Defaulted so every existing construction site still compiles and still means
    /// "no failures".</para>
    /// </summary>
    public readonly record struct PlanSizeEstimate(
        long AvgBytes, bool CatchUpInProgress, int ConsecutiveFetchFailures = 0);

    /// <summary>
    /// How many consecutive failures it takes to reach <see cref="MinCandidatePlans"/> from
    /// <see cref="MaxCandidatePlans"/> by halving: 512 → 256 → 128 → 64 → 32.
    ///
    /// <para>Counting past it is pointless — the width is already floored — so
    /// <see cref="RecordFetchFailure"/> saturates here rather than growing without bound. Saturating also
    /// keeps the number meaningful to a human reading state: 4 means "at the floor", not "has been failing
    /// since Tuesday".</para>
    /// </summary>
    public const int MaxBackoffHalvings = 4;

    /// <summary>
    /// Folds a THROWN pass into the estimate (#2776): the size estimate is left exactly as it was — a pass
    /// that failed measured nothing — and the failure counter advances, saturating at
    /// <see cref="MaxBackoffHalvings"/>.
    /// </summary>
    public static PlanSizeEstimate RecordFetchFailure(PlanSizeEstimate previous) =>
        previous with
        {
            ConsecutiveFetchFailures = previous.ConsecutiveFetchFailures >= MaxBackoffHalvings
                ? MaxBackoffHalvings
                : previous.ConsecutiveFetchFailures + 1,
        };

    /// <summary>
    /// Folds a pass that COMPLETED into the estimate (#2776): full width is restored immediately rather than
    /// decayed. One success proves the narrowed width fits, and the cheapest way to find the real ceiling
    /// again is to try it — a database recovering from a transient store stall should not spend four more
    /// cycles crawling back up. If the full width genuinely does not fit, the next pass throws and narrows
    /// again, which costs one pass and is self-correcting; decaying slowly would charge every database that
    /// ever blipped a standing tax instead.
    /// </summary>
    public static PlanSizeEstimate RecordFetchSuccess(PlanSizeEstimate previous) =>
        previous with { ConsecutiveFetchFailures = 0 };

    /// <summary>
    /// The backoff itself (#2776): halve the attempt width once per consecutive failure, floored so a
    /// database always keeps working — NEVER a stop.
    ///
    /// <para><b>Why narrowing the width is the right lever for a store-write timeout.</b> The failures this
    /// exists for are Npgsql cancels on the STORE side: <c>QueryStorePlanWriter</c> writes up to the whole
    /// per-pass byte budget of plan XML plus the map upsert inside ONE transaction, and on a store serving a
    /// 4-wide sweep that can exceed the command timeout. Bytes shipped track the id count (count × average
    /// size, until the in-SQL budget binds), so halving the count halves the write — the operation that is
    /// actually timing out. The backoff therefore converges on a width the store CAN commit rather than
    /// retrying the same impossible one forever, and it halves what the target decompresses to produce it,
    /// which is the cost #2776 measured being re-paid every cycle.</para>
    ///
    /// <para><b>Why a floor and not a give-up.</b> A latch that is never re-probed turns a TRANSIENT failure
    /// into a restart-only outage — this codebase has been bitten by that shape before. At the floor a
    /// database still attempts <see cref="MinCandidatePlans"/> ids every cycle, so it recovers on its own the
    /// moment the store does, with no operator action and no restart. The floor is the constant the candidate
    /// cap already floors at, so a maximally-backed-off pass is exactly a minimum-width pass — nothing new to
    /// reason about.</para>
    ///
    /// <para>Zero failures returns <paramref name="fullWidth"/> unchanged, so this is inert on every healthy
    /// database: shipped behaviour is identical to before #2776 until something actually throws.</para>
    /// </summary>
    public static int NarrowForFailures(int fullWidth, int consecutiveFetchFailures)
    {
        if (consecutiveFetchFailures <= 0 || fullWidth <= MinCandidatePlans)
        {
            return fullWidth;
        }

        var halvings = consecutiveFetchFailures >= MaxBackoffHalvings
            ? MaxBackoffHalvings
            : consecutiveFetchFailures;

        var narrowed = fullWidth;
        for (var halving = 0; halving < halvings; halving++)
        {
            narrowed /= 2;
            if (narrowed <= MinCandidatePlans)
            {
                return MinCandidatePlans;
            }
        }

        return narrowed;
    }

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
        // average stands).
        var catchUp = plansShipped > 0 && (plansShipped >= candidateWindow || bytesShipped >= budgetBytes);
        var avg = plansShipped > 0
            ? ObservedAvgPlanBytes(bytesShipped, plansMeasured) ?? previous.AvgBytes
            : previous.AvgBytes;

        /* #2776: the failure counter is NOT this fold's business and must survive it. Learn runs mid-pass,
           before the store write that is the thing most likely to throw — so returning a fresh record here
           (dropping the count to zero) would clear the backoff on exactly the pass about to fail, and the
           narrowing would never engage. Success is recorded separately, once the pass has actually
           completed. */
        return new PlanSizeEstimate(avg, catchUp, previous.ConsecutiveFetchFailures);
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
        var unclamped = wanted >= int.MaxValue ? int.MaxValue : (int)System.Math.Ceiling(wanted);
        var bounded = System.Math.Clamp(unclamped, MinCandidatePlans, MaxCandidatePlans);

        /* Reports that a bound CHANGED the answer, not that the answer happens to equal one. A cap whose
           measured size lands naturally on 32 or 2048 was sized by the measurement and needs no log line; saying
           "clamped" there is a false positive against this contract, and a caller that logs on it teaches its
           reader to ignore the message. */
        clamped = bounded != unclamped;
        return bounded;
    }
}

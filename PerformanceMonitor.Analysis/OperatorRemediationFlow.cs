/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// What live Query Store showed for one target after a targeted eviction — the inputs the re-score
/// judges. Every field is measured on the server AFTER the evict, against the pre-evict evidence the
/// journal already recorded.
/// </summary>
/// <param name="ObservedExecutions">Executions accumulated since the eviction, across all plans for the
/// query. Zero is a real and common answer: an evicted plan for a query nobody called back is not
/// evidence of anything.</param>
/// <param name="ElapsedSinceEvict">Wall-clock since the eviction.</param>
/// <param name="ActivePlanHash">The <c>query_plan_hash</c> executions have been attributed to SINCE the
/// eviction — read from post-eviction runtime-stats intervals, not from whether a plan row exists. Null
/// when no post-eviction interval has attributed one yet, which is the normal reading for the first
/// several minutes.
///
/// <para><b>The distinction is load-bearing, not pedantic.</b> A targeted
/// <c>DBCC FREEPROCCACHE(plan_handle)</c> evicts from the PLAN CACHE; Query Store keeps its plan row —
/// that is the whole difference between the two stores. So "is the regressed plan's hash present in Query
/// Store" is always yes after an eviction and is evidence of nothing. Only "which plan did post-eviction
/// executions run under" answers the question evict-first asks, and that fact does not exist until
/// runtime stats for those executions have been flushed and attributed.</para>
///
/// <para>Compared against the regressed plan's hash, NOT the best plan's: the question is "did the
/// optimizer make the same mistake again", and only the regressed hash answers that one.</para></param>
/// <param name="ObservedCpuPerExecUs">Post-evict cpu/exec in microseconds, or null when the server has
/// nothing to average yet.</param>
public sealed record RemediationObservation(
    long ObservedExecutions,
    TimeSpan ElapsedSinceEvict,
    string? ActivePlanHash,
    double? ObservedCpuPerExecUs);

/// <summary>
/// Which limb of the observation window closed it. Both limbs are real answers and they are NOT
/// interchangeable — see <see cref="OperatorRemediationFlow.Observe"/> for why the executions limb can
/// support a cost verdict and the elapsed limb cannot.
/// </summary>
public enum ObservationWindowLimb
{
    /// <summary>Neither limb has fired; keep observing.</summary>
    Open,

    /// <summary>Enough executions accumulated to judge cost — the evidence floor detection itself used.</summary>
    Executions,

    /// <summary>The time limit expired. A TIMEOUT, not a measurement: it says the window is over, and
    /// nothing at all about how much evidence arrived inside it.</summary>
    Elapsed,
}

/// <summary>
/// The verdict after the observation window closes — the four outcomes the design's step 2 names, plus the
/// honest fifth for a window that closed without enough evidence to rule.
/// </summary>
public enum RemediationObservationVerdict
{
    /// <summary>The window is still open. Not journaled; not a decision.</summary>
    StillObserving,

    /// <summary>A different plan is active AND it is measurably cheaper. Done — nothing more to do, and
    /// the force is NOT offered.</summary>
    OptimizerRecovered,

    /// <summary>The optimizer compiled the same regressed plan again. The force becomes available as a
    /// SECOND, separate operator decision; this verdict does not place it.</summary>
    RegressedPlanReturned,

    /// <summary>Measurably worse than the regressed baseline the eviction was meant to escape. Journal and
    /// stop — offering a force here would be acting against the only evidence we have.</summary>
    Worse,

    /// <summary>The window closed without evidence that discriminates any of the above. Journaled as its
    /// own outcome and the flow stops: the operator may start a fresh observation. Deliberately NOT folded
    /// into <see cref="RegressedPlanReturned"/>, which would offer a force on the strength of not having
    /// looked.</summary>
    Inconclusive,
}

/// <summary>One observation's whole answer, so a caller cannot take the verdict and drop the limb or the
/// reason (the record-return discipline the alert gates and <see cref="ForcePlanBotPolicy"/> use).</summary>
/// <param name="Verdict">The decision.</param>
/// <param name="Limb">Which limb closed the window (<see cref="ObservationWindowLimb.Open"/> while it is
/// still running).</param>
/// <param name="ForceOffered">Whether the operator's SECOND click becomes available. True only for
/// <see cref="RemediationObservationVerdict.RegressedPlanReturned"/> — a property of the value rather than
/// something each call site re-derives from the verdict, so no surface can offer a force after a verdict
/// that did not authorize one.</param>
public sealed record RemediationObservationResult(
    RemediationObservationVerdict Verdict,
    ObservationWindowLimb Limb,
    bool ForceOffered);

/// <summary>
/// The evict-first observation state machine (#2138 phase 1, design step 2). Pure and static — no clock,
/// no I/O, no store; the caller measures and passes the numbers in, exactly like
/// <see cref="ForcePlanBotPolicy"/>, so the whole decision table is unit-testable without a host, a store
/// or a server.
///
/// <para><b>Human-armed by construction.</b> Nothing here executes anything. The eviction happened before
/// the caller could ask this question, and the force — when this returns
/// <see cref="RemediationObservationVerdict.RegressedPlanReturned"/> — is a separate operator decision
/// that a separate call has to carry out. <see cref="RemediationObservationResult.ForceOffered"/> is
/// permission to DRAW a control, never permission to act.</para>
/// </summary>
public static class OperatorRemediationFlow
{
    /// <summary>
    /// The journaled decision strings for each verdict. A consumer API like
    /// <see cref="ForcePlanBotPolicy"/>'s reasons — the audit trail is read by people and by agents, so
    /// these are stable names, never re-spelled.
    /// </summary>
    public const string DecisionOptimizerRecovered = "optimizer_recovered";

    public const string DecisionRegressedPlanReturned = "regressed_plan_returned";

    public const string DecisionWorseAfterEvict = "worse_after_evict";

    public const string DecisionObservationInconclusive = "observation_inconclusive";

    /// <summary>
    /// How much better than the pre-evict baseline counts as recovery, and how much worse counts as worse.
    /// A dead band on purpose: cpu/exec on a live server moves a few percent for reasons that have nothing
    /// to do with which plan compiled, and a state machine with no dead band would call that noise a
    /// verdict. 25% mirrors <see cref="ForcePlanBotSettings.NetBenefitRatio"/>'s bar so the flow and the
    /// self-review that judges its forces do not disagree about what "better" means.
    /// </summary>
    public const double MaterialChangeRatio = 0.75;

    /// <summary>
    /// Judge one observation.
    /// </summary>
    /// <param name="observation">What the server showed after the eviction.</param>
    /// <param name="regressedPlanHash">The <c>query_plan_hash</c> of the plan the eviction removed —
    /// <c>StructuredForcePlanTarget.LatestPlanHash</c>, the verdict object's own field.</param>
    /// <param name="baselineCpuPerExecUs">The pre-evict regressed cpu/exec the decision was taken on —
    /// the verdict object's <c>StructuredForcePlanEvidence.LatestCpuPerExecUs</c>. Comparing against the
    /// evidence the operator was SHOWN, rather than re-reading a baseline now, is what makes the outcome
    /// auditable: the journal row holds both numbers and the comparison can be re-done by hand.</param>
    /// <param name="minObservationExecutions">The executions limb.
    /// Callers pass <see cref="ForcePlanBotSettings.MinReviewExecutions"/> — there is no literal here, so
    /// the floor cannot drift away from the one the review and detection use.</param>
    /// <param name="observationWindow">The elapsed limb. Callers pass
    /// <see cref="ForcePlanBotSettings.ObservationWindowMinutes"/> as a <see cref="TimeSpan"/>.</param>
    public static RemediationObservationResult Observe(
        RemediationObservation observation,
        string? regressedPlanHash,
        double baselineCpuPerExecUs,
        int minObservationExecutions,
        TimeSpan observationWindow)
    {
        if (observation is null)
        {
            throw new ArgumentNullException(nameof(observation));
        }

        var limb = Limb(observation, minObservationExecutions, observationWindow);
        if (limb == ObservationWindowLimb.Open)
        {
            return new RemediationObservationResult(
                RemediationObservationVerdict.StillObserving, limb, ForceOffered: false);
        }

        /* Plan IDENTITY first, and it carries a LOWER evidence requirement than cost — but not a zero
           one, and the difference between those two readings is why this sits below the window guard
           rather than above it.

           Lower: which plan the optimizer chose is not a statistical quantity, so a handful of attributed
           post-eviction executions settle it, and this arm is legitimate on a window the timeout closed
           with three. Cost is statistical and gets the executions floor below. Collapsing the two would
           either refuse to report a plan that demonstrably came back, or claim a cost improvement measured
           on nothing.

           Not zero, which is the part worth stating because the code reads as though it could run on every
           call: the fact this arm tests does not EXIST before some executions have been attributed.
           FREEPROCCACHE evicts the plan cache and Query Store keeps its plan row, so the regressed hash is
           present the instant after the eviction and stays present — see ActivePlanHash's remarks. Running
           this arm while the window is still open would therefore not report an early recompile; it would
           report the pre-eviction plan, on every first call, and offer a force on it. Query Store's own
           flush interval (DATA_FLUSH_INTERVAL_SECONDS, 900 by default) is why the elapsed limb's 30
           minutes is the right order of magnitude rather than a round number.

           There is a second reason to keep both verdicts behind one window even if the instrument were
           instantaneous: OptimizerRecovered needs the executions floor, so an identity arm that fired
           earlier would make the FORCE the quick answer and "nothing here needs pinning" the slow one.
           That is the wrong asymmetry for a lever whose premise is that the cheapest fix pins nothing.
           Pinned by OperatorRemediationFlowTests' window-open-with-a-matching-hash cases. */
        if (SameHash(observation.ActivePlanHash, regressedPlanHash))
        {
            return new RemediationObservationResult(
                RemediationObservationVerdict.RegressedPlanReturned, limb, ForceOffered: true);
        }

        /* Below the cost floor nothing about cost can be claimed. Reached by the elapsed limb almost by
           definition, and reachable by the executions limb only when the server gave us no average — both
           are "we did not learn anything", which is a result and gets journaled as one. */
        if (limb == ObservationWindowLimb.Elapsed && observation.ObservedExecutions < minObservationExecutions)
        {
            return Inconclusive(limb);
        }

        if (observation.ObservedCpuPerExecUs is not double observed || !double.IsFinite(observed) ||
            baselineCpuPerExecUs <= 0 || !double.IsFinite(baselineCpuPerExecUs))
        {
            return Inconclusive(limb);
        }

        if (observed <= baselineCpuPerExecUs * MaterialChangeRatio)
        {
            /* A cheaper plan the optimizer found on its own. The force is deliberately NOT offered: the
               whole point of evict-first is that the cheapest fix is the one that pins nothing. */
            return new RemediationObservationResult(
                RemediationObservationVerdict.OptimizerRecovered, limb, ForceOffered: false);
        }

        if (observed >= baselineCpuPerExecUs / MaterialChangeRatio)
        {
            /* Worse than what the operator was already unhappy with. Stop — and specifically do not offer
               the force, because the plan now running is not the regressed plan we have a known-better
               alternative to, so there is nothing here the force is the answer to. */
            return new RemediationObservationResult(
                RemediationObservationVerdict.Worse, limb, ForceOffered: false);
        }

        /* Inside the dead band: a different plan, indistinguishable in cost. Not recovery (nothing got
           better), not worse, and not the regressed plan returning. */
        return Inconclusive(limb);
    }

    /// <summary>
    /// Which limb closed the window, if either. Executions is checked first so a window that satisfied
    /// BOTH limbs reports the one that carries evidence — the elapsed limb would be true of the same
    /// observation and would suppress a cost verdict the executions actually support.
    /// </summary>
    public static ObservationWindowLimb Limb(
        RemediationObservation observation,
        int minObservationExecutions,
        TimeSpan observationWindow)
    {
        if (observation is null)
        {
            throw new ArgumentNullException(nameof(observation));
        }

        if (observation.ObservedExecutions >= minObservationExecutions)
        {
            return ObservationWindowLimb.Executions;
        }

        return observation.ElapsedSinceEvict >= observationWindow
            ? ObservationWindowLimb.Elapsed
            : ObservationWindowLimb.Open;
    }

    /// <summary>The journal's decision string for a verdict. Throws on
    /// <see cref="RemediationObservationVerdict.StillObserving"/> rather than inventing a string: an
    /// in-progress observation is not a decision, and a caller journaling one has a bug this hides.</summary>
    public static string DecisionFor(RemediationObservationVerdict verdict) => verdict switch
    {
        RemediationObservationVerdict.OptimizerRecovered => DecisionOptimizerRecovered,
        RemediationObservationVerdict.RegressedPlanReturned => DecisionRegressedPlanReturned,
        RemediationObservationVerdict.Worse => DecisionWorseAfterEvict,
        RemediationObservationVerdict.Inconclusive => DecisionObservationInconclusive,
        _ => throw new ArgumentOutOfRangeException(
            nameof(verdict), verdict, "an observation still running is not a journalable decision"),
    };

    private static RemediationObservationResult Inconclusive(ObservationWindowLimb limb) =>
        new(RemediationObservationVerdict.Inconclusive, limb, ForceOffered: false);

    /* Query Store renders a plan hash as 0x-prefixed hex, and the two sides of this comparison arrive
       from different places (the persisted target, and a live read), so case and prefix are not
       guaranteed to match even when the hashes do. Compared as normalized text rather than parsed to
       bytes because a malformed hash must make this return false — not throw inside a verdict. */
    private static bool SameHash(string? left, string? right)
    {
        var a = Normalize(left);
        var b = Normalize(right);

        /* An absent hash on either side never matches. A null ActivePlanHash means the server has not
           attributed a post-evict plan, which is the opposite of evidence that the regressed one is back. */
        return a.Length > 0 && b.Length > 0 && string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    }

    private static string Normalize(string? hash)
    {
        var text = (hash ?? string.Empty).Trim();
        return text.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? text.Substring(2) : text;
    }
}

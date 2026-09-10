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
/// What the target server said about a bot-placed force when the review ran — the caller runs the
/// verify read (the same questions <c>StructuredForcePlanTarget.VerifySql</c> asks: did the force
/// stick, what has the query cost since) and hands the numbers here.
/// </summary>
/// <param name="ForcedAtUtc">When the bot's force was journaled.</param>
/// <param name="BaselineCpuPerExecUs">The REGRESSED plan's cpu/exec at decision time — what the
/// force was meant to beat. The baseline is the evidence snapshot from the journal, never
/// re-derived at review time, so the review judges the force against the numbers that justified it.</param>
/// <param name="PlanIsStillForced">Whether <c>sys.query_store_plan.is_forced_plan</c> still reports
/// the bot's plan forced.</param>
/// <param name="ForceFailureCount"><c>force_failure_count</c> for the forced plan.</param>
/// <param name="ExecutionsSinceForce">Executions of the query (all plans) since the force.</param>
/// <param name="ObservedCpuPerExecUs">Execution-weighted cpu/exec of the query (all plans) since
/// the force, or null when the window returned no rows.</param>
public sealed record ForcePlanReviewInput(
    DateTime ForcedAtUtc,
    double BaselineCpuPerExecUs,
    bool PlanIsStillForced,
    long ForceFailureCount,
    long ExecutionsSinceForce,
    double? ObservedCpuPerExecUs);

public enum ForcePlanReviewVerdictKind
{
    /// <summary>No checkpoint is due yet, or the due checkpoint lacks evidence and the final one has
    /// not arrived — do nothing, ask again next pass.</summary>
    KeepWatching,

    /// <summary>A non-final checkpoint judged the force a net benefit — journal the checkpoint,
    /// keep the force, keep watching until the final checkpoint.</summary>
    KeepForced,

    /// <summary>Take the force back and journal why. The reason feeds the failure-memory window
    /// (<see cref="ForcePlanBotHistory.RecentFailedForces"/>), so a query whose forces keep being
    /// taken back cools down instead of flapping force/unforce forever.</summary>
    Unforce,

    /// <summary>The review is over and the force stands — terminal, no further checkpoints.</summary>
    ReviewComplete,
}

/// <summary>One review evaluation's whole answer — verdict plus the named reason that lands in the
/// journal row, as a single value so neither can travel without the other.</summary>
public sealed record ForcePlanReviewVerdict(
    ForcePlanReviewVerdictKind Kind,
    string Reason);

/// <summary>
/// The post-force self-review (#2138, pure): after the bot forces a plan, checkpoints
/// at +<see cref="ForcePlanBotSettings.FirstReviewMinutes"/> and
/// +<see cref="ForcePlanBotSettings.FinalReviewMinutes"/> re-judge the query's ACTUAL cost against
/// the baseline the force was sold on, and take the force back when it is not a net benefit. Pure
/// and clockless like <see cref="ForcePlanBotPolicy"/> — the caller reads the server, passes
/// <c>nowUtc</c>, and persists/executes the verdict.
///
/// <para>The flap guard is structural, not stateful: an unforce verdict's reason is journaled, the
/// journal read counts it into the failure-memory window, and
/// <see cref="ForcePlanBotPolicy.Evaluate"/> blocks re-forcing while the window holds — so
/// force→unforce→force oscillation is bounded by <see cref="ForcePlanBotSettings.FailedForceThreshold"/>
/// per <see cref="ForcePlanBotSettings.FailedForceCooldownHours"/>, and eligibility returns by the
/// window sliding, never by anyone clearing a flag (#2677).</para>
///
/// <para>The caller that drives this — reading the server, executing the unforce — arrives with the
/// write path (#2731). The verdict table lands here first, and complete, on purpose: the rules a
/// live force will be judged and taken back by are the part worth settling BEFORE anything can place
/// one, and pure + clockless means the whole table is specced without a host, a store or a server
/// (<c>ForcePlanSelfReviewTests</c> is that spec).</para>
/// </summary>
public static class ForcePlanSelfReview
{
    public const string ReasonBeforeFirstCheckpoint = "before_first_checkpoint";
    public const string ReasonAwaitingExecutions = "awaiting_executions";
    public const string ReasonNoLongerForced = "no_longer_forced";
    public const string ReasonForceFailing = "force_failing";
    public const string ReasonNotNetBenefit = "not_net_benefit";
    public const string ReasonNetBenefitAtCheckpoint = "net_benefit_at_checkpoint";
    public const string ReasonNetBenefitConfirmed = "net_benefit_confirmed";
    public const string ReasonInsufficientExecutionsKept = "insufficient_executions_kept";
    public const string ReasonNoBaseline = "no_baseline_kept";

    public static ForcePlanReviewVerdict Evaluate(
        ForcePlanReviewInput input,
        ForcePlanBotSettings settings,
        DateTime nowUtc)
    {
        if (input is null)
        {
            throw new ArgumentNullException(nameof(input));
        }

        if (settings is null)
        {
            throw new ArgumentNullException(nameof(settings));
        }

        /* Somebody else's hands were here: the plan is no longer forced (an operator, or automatic
           plan correction, unforced it). The bot's review ends — it never re-forces on its own
           authority, and unforce-of-an-unforced would act on state it no longer owns. Checked before
           the failure test because a plan that is not forced cannot be "failing to force". */
        if (!input.PlanIsStillForced)
        {
            return new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.ReviewComplete, ReasonNoLongerForced);
        }

        /* The engine tried to apply the forced plan and could not (force_failure_count counts
           compile-time forcing failures). A force that fails to apply delivers nothing and still
           pins intent against the query — take it back immediately, without waiting for a
           checkpoint, and let the failure memory cool the query down. */
        if (input.ForceFailureCount > 0)
        {
            return new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.Unforce, ReasonForceFailing);
        }

        var elapsed = nowUtc - input.ForcedAtUtc;
        var firstDue = elapsed >= TimeSpan.FromMinutes(settings.FirstReviewMinutes);
        var finalDue = elapsed >= TimeSpan.FromMinutes(settings.FinalReviewMinutes);

        if (!firstDue)
        {
            return new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.KeepWatching, ReasonBeforeFirstCheckpoint);
        }

        /* A baseline of zero (or garbage) means the ratio below is undefined — never judged, never
           divided. Terminal KEEP rather than unforce: the force's own gate required real evidence,
           so a missing baseline here is a journal defect, and reversing a plan force over a
           bookkeeping hole would be acting on nothing. The named reason makes the hole visible. */
        if (!double.IsFinite(input.BaselineCpuPerExecUs) || input.BaselineCpuPerExecUs <= 0)
        {
            return new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.ReviewComplete, ReasonNoBaseline);
        }

        var hasEvidence =
            input.ObservedCpuPerExecUs is double observed &&
            double.IsFinite(observed) &&
            input.ExecutionsSinceForce >= settings.MinReviewExecutions;

        if (!hasEvidence)
        {
            /* The query has not run enough to judge. Before the final checkpoint: keep watching. AT
               the final checkpoint: the query went quiet under the forced plan — keep the force and
               close the review, with a reason that says the KEEP rests on absence of evidence, not
               on measured benefit. Unforcing an idle query would trade a known-good plan for a
               recompile nobody asked for. */
            return finalDue
                ? new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.ReviewComplete, ReasonInsufficientExecutionsKept)
                : new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.KeepWatching, ReasonAwaitingExecutions);
        }

        var ratio = input.ObservedCpuPerExecUs!.Value / input.BaselineCpuPerExecUs;

        if (ratio > settings.NetBenefitRatio)
        {
            /* Not a net benefit: the query still costs more than the bar relative to what the force
               was meant to fix. Unforce at ANY due checkpoint — a bad force should not get to run
               until the final checkpoint just because the first one already saw it failing. */
            return new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.Unforce, ReasonNotNetBenefit);
        }

        return finalDue
            ? new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.ReviewComplete, ReasonNetBenefitConfirmed)
            : new ForcePlanReviewVerdict(ForcePlanReviewVerdictKind.KeepForced, ReasonNetBenefitAtCheckpoint);
    }
}

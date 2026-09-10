/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The post-force self-review state machine (#2138): a bot-placed force must pay rent (at least the
/// net-benefit margin against the baseline it was sold on) or be taken back, and every terminal
/// verdict carries the named reason the journal records. The state machine is pure — these tests ARE
/// its complete behavioral spec, since no host or server participates.
/// </summary>
public sealed class ForcePlanSelfReviewTests
{
    private static readonly DateTime ForcedAt = new(2026, 8, 31, 0, 0, 0, DateTimeKind.Utc);

    private static readonly ForcePlanBotSettings Settings = ForcePlanBotSettings.Default with { Enabled = true };

    private static ForcePlanReviewInput Input(
        double baseline = 50000,
        bool stillForced = true,
        long failureCount = 0,
        long executions = 100,
        double? observed = 10000) => new(
            ForcedAtUtc: ForcedAt,
            BaselineCpuPerExecUs: baseline,
            PlanIsStillForced: stillForced,
            ForceFailureCount: failureCount,
            ExecutionsSinceForce: executions,
            ObservedCpuPerExecUs: observed);

    private static DateTime AtFirstCheckpoint => ForcedAt.AddMinutes(Settings.FirstReviewMinutes);
    private static DateTime AtFinalCheckpoint => ForcedAt.AddMinutes(Settings.FinalReviewMinutes);

    /* ---------------- somebody else's hands ---------------- */

    [Fact]
    public void AnUnforcedPlan_EndsTheReview_AndNeverReforces()
    {
        /* Own-forces-only, seen from the review side: an operator (or APC) unforcing the bot's plan
           closes the bot's interest. Pinned WITH a failure count so the ordering is load-bearing —
           "no longer forced" must win over "failing to force", or the bot would issue an unforce for
           a force that no longer exists. */
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(stillForced: false, failureCount: 3), Settings, AtFirstCheckpoint);

        Assert.Equal(ForcePlanReviewVerdictKind.ReviewComplete, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonNoLongerForced, verdict.Reason);
    }

    /* ---------------- forcing failures ---------------- */

    [Fact]
    public void AFailingForce_IsTakenBackImmediately_WithoutWaitingForACheckpoint()
    {
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(failureCount: 1), Settings, ForcedAt.AddMinutes(5));

        Assert.Equal(ForcePlanReviewVerdictKind.Unforce, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonForceFailing, verdict.Reason);
    }

    /* ---------------- the checkpoints ---------------- */

    [Fact]
    public void BeforeTheFirstCheckpoint_TheReviewOnlyWatches()
    {
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(), Settings, ForcedAt.AddMinutes(Settings.FirstReviewMinutes - 1));

        Assert.Equal(ForcePlanReviewVerdictKind.KeepWatching, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonBeforeFirstCheckpoint, verdict.Reason);
    }

    [Fact]
    public void InsufficientExecutions_KeepWatchingUntilTheFinalCheckpoint_ThenKeepAndClose()
    {
        var thin = Input(executions: Settings.MinReviewExecutions - 1);

        var mid = ForcePlanSelfReview.Evaluate(thin, Settings, AtFirstCheckpoint);
        Assert.Equal(ForcePlanReviewVerdictKind.KeepWatching, mid.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonAwaitingExecutions, mid.Reason);

        /* At the final checkpoint a quiet query KEEPS its force — unforcing an idle query trades a
           known-good plan for a recompile nobody asked for — and the reason says the keep rests on
           absence of evidence, not measured benefit. */
        var final = ForcePlanSelfReview.Evaluate(thin, Settings, AtFinalCheckpoint);
        Assert.Equal(ForcePlanReviewVerdictKind.ReviewComplete, final.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonInsufficientExecutionsKept, final.Reason);
    }

    [Fact]
    public void NoObservedCost_IsTreatedAsInsufficientEvidence()
    {
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(observed: null), Settings, AtFirstCheckpoint);

        Assert.Equal(ForcePlanReviewVerdictKind.KeepWatching, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonAwaitingExecutions, verdict.Reason);
    }

    [Fact]
    public void AMissingBaseline_ClosesWithoutJudging_AndNamesTheHole()
    {
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(baseline: 0), Settings, AtFirstCheckpoint);

        Assert.Equal(ForcePlanReviewVerdictKind.ReviewComplete, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonNoBaseline, verdict.Reason);
    }

    /* ---------------- the net-benefit bar ---------------- */

    [Fact]
    public void NotANetBenefit_UnforcesAtTheFirstDueCheckpoint()
    {
        /* 40,000/50,000 = 0.80 > the 0.75 bar. The unforce fires at the FIRST checkpoint — a bad
           force does not get to run until the final one just because an earlier gate saw it failing. */
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(observed: 40000), Settings, AtFirstCheckpoint);

        Assert.Equal(ForcePlanReviewVerdictKind.Unforce, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonNotNetBenefit, verdict.Reason);
    }

    [Fact]
    public void ExactlyAtTheBar_Passes()
    {
        /* 37,500/50,000 = exactly 0.75: "at least 25% better" is inclusive, so the boundary keeps. */
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(observed: 37500), Settings, AtFirstCheckpoint);

        Assert.Equal(ForcePlanReviewVerdictKind.KeepForced, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonNetBenefitAtCheckpoint, verdict.Reason);
    }

    [Fact]
    public void ANetBenefit_KeepsAtTheFirstCheckpoint_AndConfirmsAtTheFinal()
    {
        var healthy = Input(observed: 10000);

        var mid = ForcePlanSelfReview.Evaluate(healthy, Settings, AtFirstCheckpoint);
        Assert.Equal(ForcePlanReviewVerdictKind.KeepForced, mid.Kind);

        var final = ForcePlanSelfReview.Evaluate(healthy, Settings, AtFinalCheckpoint);
        Assert.Equal(ForcePlanReviewVerdictKind.ReviewComplete, final.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonNetBenefitConfirmed, final.Reason);
    }

    [Fact]
    public void ARegressionAtTheFinalCheckpoint_StillUnforces()
    {
        /* The final checkpoint is not an amnesty: worse-than-the-bar at +24h unforces exactly like
           worse-than-the-bar at +1h. */
        var verdict = ForcePlanSelfReview.Evaluate(
            Input(observed: 60000), Settings, AtFinalCheckpoint);

        Assert.Equal(ForcePlanReviewVerdictKind.Unforce, verdict.Kind);
        Assert.Equal(ForcePlanSelfReview.ReasonNotNetBenefit, verdict.Reason);
    }
}

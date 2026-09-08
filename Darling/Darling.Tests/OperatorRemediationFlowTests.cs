/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2138 phase-1 evict-then-observe state machine, case by case. The contracts:
/// <list type="number">
/// <item>the two window limbs are NOT interchangeable — plan identity needs one compile, cost needs the
/// executions floor, and a window the timeout closed cannot support a cost verdict;</item>
/// <item>the force is offered for exactly ONE verdict, and offering it is a property of the returned
/// value so no caller can re-derive it differently;</item>
/// <item>the floors come from <see cref="ForcePlanBotSettings"/> and are not restated here — pinned by
/// driving the boundary off the settings value rather than off a literal.</item>
/// </list>
/// </summary>
public sealed class OperatorRemediationFlowTests
{
    private const string RegressedHash = "0x2222222222222222";

    private const string OtherHash = "0x3333333333333333";

    private const double Baseline = 50000;

    private static readonly int Floor = ForcePlanBotSettings.Default.MinReviewExecutions;

    private static readonly TimeSpan Window =
        TimeSpan.FromMinutes(ForcePlanBotSettings.Default.ObservationWindowMinutes);

    private static RemediationObservationResult Observe(
        long executions,
        TimeSpan elapsed,
        string? activePlanHash,
        double? observedCpu) =>
        OperatorRemediationFlow.Observe(
            new RemediationObservation(executions, elapsed, activePlanHash, observedCpu),
            RegressedHash,
            Baseline,
            Floor,
            Window);

    /* ---------------- the window ---------------- */

    [Fact]
    public void BelowBothLimbs_TheWindowIsStillOpen_AndNothingIsOffered()
    {
        var result = Observe(Floor - 1, Window - TimeSpan.FromMinutes(1), OtherHash, 1000);

        Assert.Equal(RemediationObservationVerdict.StillObserving, result.Verdict);
        Assert.Equal(ObservationWindowLimb.Open, result.Limb);
        Assert.False(result.ForceOffered);
    }

    /// <summary>
    /// The executions limb fires AT the floor and not one execution before it, and the boundary is read
    /// off <see cref="ForcePlanBotSettings.MinReviewExecutions"/> rather than a literal 25 — so a flow
    /// that restated the number, or ignored the parameter, fails here rather than agreeing by coincidence.
    /// </summary>
    [Fact]
    public void TheExecutionsLimbFiresAtTheSettingsFloor_NotAtALiteral()
    {
        var justBelow = new RemediationObservation(Floor - 1, TimeSpan.Zero, OtherHash, 1000);
        var atTheFloor = new RemediationObservation(Floor, TimeSpan.Zero, OtherHash, 1000);

        Assert.Equal(
            ObservationWindowLimb.Open,
            OperatorRemediationFlow.Limb(justBelow, Floor, Window));
        Assert.Equal(
            ObservationWindowLimb.Executions,
            OperatorRemediationFlow.Limb(atTheFloor, Floor, Window));

        /* And it moves WITH the setting. A hardcoded 25 would keep the two assertions above passing
           while failing this one, which is the whole point of the parameter. */
        var raised = Floor * 4;
        Assert.Equal(
            ObservationWindowLimb.Open,
            OperatorRemediationFlow.Limb(new RemediationObservation(Floor, TimeSpan.Zero, OtherHash, 1000), raised, Window));
    }

    [Fact]
    public void TheElapsedLimbIsATimeout_NotASecondMeasurement()
    {
        var result = Observe(3, Window, OtherHash, 1000);

        Assert.Equal(ObservationWindowLimb.Elapsed, result.Limb);

        /* Three executions of a different plan, at 2% of baseline. Cheap-looking, and refused: the window
           closed on the clock, so there is no cost evidence to be had. Calling this recovery is exactly
           what the two-limb split exists to prevent. */
        Assert.Equal(RemediationObservationVerdict.Inconclusive, result.Verdict);
        Assert.False(result.ForceOffered);
    }

    /// <summary>
    /// When both limbs are satisfied the EXECUTIONS limb is reported. It has to win: the elapsed limb
    /// would suppress a cost verdict the executions actually support, so reporting the timeout for a
    /// window that also gathered enough evidence would throw away a real measurement.
    /// </summary>
    [Fact]
    public void WhenBothLimbsAreSatisfied_TheOneCarryingEvidenceWins()
    {
        var result = Observe(Floor * 2, Window * 2, OtherHash, Baseline * 0.2);

        Assert.Equal(ObservationWindowLimb.Executions, result.Limb);
        Assert.Equal(RemediationObservationVerdict.OptimizerRecovered, result.Verdict);
    }

    /* ---------------- the four verdicts ---------------- */

    [Fact]
    public void ACheaperDifferentPlan_IsOptimizerRecovered_AndOffersNoForce()
    {
        var result = Observe(Floor, TimeSpan.Zero, OtherHash, Baseline * 0.2);

        Assert.Equal(RemediationObservationVerdict.OptimizerRecovered, result.Verdict);
        Assert.False(result.ForceOffered);
        Assert.Equal(
            OperatorRemediationFlow.DecisionOptimizerRecovered,
            OperatorRemediationFlow.DecisionFor(result.Verdict));
    }

    [Fact]
    public void TheRegressedPlanComingBack_OffersTheForce_AsTheSecondDecision()
    {
        var result = Observe(Floor, TimeSpan.Zero, RegressedHash, Baseline);

        Assert.Equal(RemediationObservationVerdict.RegressedPlanReturned, result.Verdict);
        Assert.True(result.ForceOffered);
    }

    /// <summary>
    /// Plan IDENTITY does not need the cost floor: one compile settles which plan the optimizer chose, so
    /// the regressed plan returning is a legitimate verdict on a window the timeout closed with three
    /// executions. This is the arm that would be lost by giving both limbs the same evidence requirement,
    /// and it is the arm the whole evict-first strategy turns on — "the eviction changed nothing" is the
    /// answer that justifies the force.
    /// </summary>
    [Fact]
    public void TheRegressedPlanComingBack_IsJudgedOnIdentity_EvenOnTheTimeoutLimb()
    {
        var result = Observe(3, Window, RegressedHash, null);

        Assert.Equal(ObservationWindowLimb.Elapsed, result.Limb);
        Assert.Equal(RemediationObservationVerdict.RegressedPlanReturned, result.Verdict);
        Assert.True(result.ForceOffered);
    }

    /// <summary>
    /// Query Store renders a plan hash as 0x-prefixed hex, and the two sides of this comparison come from
    /// different places — the persisted target, and a live read — so neither case nor prefix is guaranteed
    /// to agree even when the hashes do. A comparison that missed on either would report the regressed
    /// plan as "some other plan, indistinguishable in cost" and silently drop the force.
    /// </summary>
    [Theory]
    [InlineData("0x2222222222222222")]
    [InlineData("2222222222222222")]
    [InlineData("0X2222222222222222")]
    [InlineData("  0x2222222222222222  ")]
    public void PlanHashComparisonSurvivesPrefixCaseAndPadding(string activeHash)
    {
        Assert.Equal(
            RemediationObservationVerdict.RegressedPlanReturned,
            Observe(Floor, TimeSpan.Zero, activeHash, Baseline).Verdict);
    }

    [Fact]
    public void AMeasurablyWorsePlan_JournalsAndStops_WithNoForceOffered()
    {
        var result = Observe(Floor, TimeSpan.Zero, OtherHash, Baseline * 2);

        Assert.Equal(RemediationObservationVerdict.Worse, result.Verdict);
        Assert.False(result.ForceOffered);
        Assert.Equal(
            OperatorRemediationFlow.DecisionWorseAfterEvict,
            OperatorRemediationFlow.DecisionFor(result.Verdict));
    }

    /// <summary>
    /// Inside the dead band a different plan is neither recovery nor worse. Without the band, cpu/exec
    /// drifting a few percent for reasons unrelated to which plan compiled would be reported as a verdict.
    /// </summary>
    [Theory]
    [InlineData(0.9)]
    [InlineData(1.0)]
    [InlineData(1.1)]
    public void ADifferentPlanIndistinguishableInCost_IsInconclusive(double ratio)
    {
        var result = Observe(Floor, TimeSpan.Zero, OtherHash, Baseline * ratio);

        Assert.Equal(RemediationObservationVerdict.Inconclusive, result.Verdict);
        Assert.False(result.ForceOffered);
    }

    /// <summary>
    /// The band's edges are inclusive on the verdict side: exactly at the bar counts. Pinned because
    /// "at least 25% better" and "more than 25% better" are one character apart and the difference decides
    /// whether a borderline eviction reports recovery.
    /// </summary>
    [Fact]
    public void ExactlyAtTheBar_Counts()
    {
        Assert.Equal(
            RemediationObservationVerdict.OptimizerRecovered,
            Observe(Floor, TimeSpan.Zero, OtherHash, Baseline * OperatorRemediationFlow.MaterialChangeRatio).Verdict);

        Assert.Equal(
            RemediationObservationVerdict.Worse,
            Observe(Floor, TimeSpan.Zero, OtherHash, Baseline / OperatorRemediationFlow.MaterialChangeRatio).Verdict);
    }

    /* ---------------- honest absences ---------------- */

    /// <summary>
    /// A null active plan hash means the server has not attributed a post-evict compile — the OPPOSITE of
    /// evidence that the regressed plan is back. Pinned because a hash comparison that treats two absences
    /// as equal would report every un-recompiled query as the regressed plan returning, and offer a force
    /// on it.
    /// </summary>
    [Fact]
    public void ANullActivePlanHash_NeverReadsAsTheRegressedPlanReturning()
    {
        var result = Observe(Floor, TimeSpan.Zero, null, Baseline);

        Assert.NotEqual(RemediationObservationVerdict.RegressedPlanReturned, result.Verdict);
        Assert.False(result.ForceOffered);
    }

    [Fact]
    public void AnAbsentRegressedHashOnTheTargetSide_AlsoNeverMatches()
    {
        var result = OperatorRemediationFlow.Observe(
            new RemediationObservation(Floor, TimeSpan.Zero, null, Baseline),
            regressedPlanHash: null,
            Baseline,
            Floor,
            Window);

        Assert.NotEqual(RemediationObservationVerdict.RegressedPlanReturned, result.Verdict);
    }

    /// <summary>
    /// A cost the server could not give us is not a cost of zero. Written as a loop over the three
    /// unusable shapes rather than as <c>[InlineData(null)]</c>, which binds the null to the attribute's
    /// whole <c>params</c> array instead of to the parameter — a real ambiguity, not a formatting choice.
    /// </summary>
    [Fact]
    public void AnUnusableObservedCost_IsInconclusive_RatherThanAVerdict()
    {
        foreach (var observedCpu in new double?[] { null, double.NaN, double.PositiveInfinity })
        {
            Assert.Equal(
                RemediationObservationVerdict.Inconclusive,
                Observe(Floor, TimeSpan.Zero, OtherHash, observedCpu).Verdict);
        }
    }

    [Fact]
    public void AnUnusableBaseline_IsInconclusive_RatherThanAVerdict()
    {
        foreach (var baseline in new[] { 0d, -1d, double.NaN })
        {
            var result = OperatorRemediationFlow.Observe(
                new RemediationObservation(Floor, TimeSpan.Zero, OtherHash, 1000),
                RegressedHash,
                baseline,
                Floor,
                Window);

            Assert.Equal(RemediationObservationVerdict.Inconclusive, result.Verdict);
        }
    }

    /* ---------------- the invariants ---------------- */

    /// <summary>
    /// The force is offered for EXACTLY ONE verdict, checked over every verdict the enum has rather than
    /// case by case — so a verdict added later without a decision about the force fails here instead of
    /// inheriting whichever arm it was written next to.
    /// </summary>
    [Fact]
    public void ExactlyOneVerdictOffersTheForce()
    {
        var offering = Enum.GetValues<RemediationObservationVerdict>()
            .Where(OffersForce)
            .ToList();

        Assert.Equal(new[] { RemediationObservationVerdict.RegressedPlanReturned }, offering);

        static bool OffersForce(RemediationObservationVerdict verdict) => verdict switch
        {
            /* Derived from the machine's OWN output, not from a table retyped here: each verdict is
               reproduced by an observation that reaches it, and the value's ForceOffered is read back. A
               retyped table would agree with itself forever. */
            RemediationObservationVerdict.StillObserving =>
                Observe(0, TimeSpan.Zero, OtherHash, null).ForceOffered,
            RemediationObservationVerdict.OptimizerRecovered =>
                Observe(Floor, TimeSpan.Zero, OtherHash, Baseline * 0.2).ForceOffered,
            RemediationObservationVerdict.RegressedPlanReturned =>
                Observe(Floor, TimeSpan.Zero, RegressedHash, Baseline).ForceOffered,
            RemediationObservationVerdict.Worse =>
                Observe(Floor, TimeSpan.Zero, OtherHash, Baseline * 2).ForceOffered,
            RemediationObservationVerdict.Inconclusive =>
                Observe(Floor, TimeSpan.Zero, OtherHash, Baseline).ForceOffered,
            _ => throw new InvalidOperationException(
                $"verdict {verdict} has no reproducing observation in this test — decide whether it " +
                "offers the force and add one"),
        };
    }

    /// <summary>
    /// Every verdict the machine can reach maps to a journal decision string, and the one that cannot be
    /// journaled throws rather than inventing one. An in-progress observation is not a decision, and a
    /// caller journaling it has a bug that a friendly fallback string would hide.
    /// </summary>
    [Fact]
    public void EveryTerminalVerdictHasADecisionString_AndTheNonTerminalOneThrows()
    {
        foreach (var verdict in Enum.GetValues<RemediationObservationVerdict>())
        {
            if (verdict == RemediationObservationVerdict.StillObserving)
            {
                Assert.Throws<ArgumentOutOfRangeException>(
                    () => OperatorRemediationFlow.DecisionFor(verdict));
                continue;
            }

            var decision = OperatorRemediationFlow.DecisionFor(verdict);
            Assert.False(string.IsNullOrWhiteSpace(decision));
        }

        /* The strings are distinct: two verdicts sharing one would make the journal unable to tell them
           apart, which is the one thing the journal is for. */
        var decisions = Enum.GetValues<RemediationObservationVerdict>()
            .Where(v => v != RemediationObservationVerdict.StillObserving)
            .Select(OperatorRemediationFlow.DecisionFor)
            .ToList();

        Assert.Equal(decisions.Count, decisions.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// The flow's "better" bar and the self-review's net-benefit bar are the same number, so an eviction
    /// judged a recovery and a force judged worth keeping cannot disagree about what better means. Pinned
    /// against the setting rather than the literal both happen to equal today.
    /// </summary>
    [Fact]
    public void TheFlowAndTheSelfReviewShareOneDefinitionOfBetter()
    {
        Assert.Equal(
            ForcePlanBotSettings.Default.NetBenefitRatio,
            OperatorRemediationFlow.MaterialChangeRatio);
    }
}

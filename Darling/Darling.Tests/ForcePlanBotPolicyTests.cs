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
/// The #2138 bot's decision table, pinned case by case. The two contracts that must never regress:
/// (1) a target with ANY policy blocker — parameter sensitivity above all — can never come back
/// Force or WouldForce, whatever the gates say; (2) a live Force requires BOTH write gates
/// (global dry-run off AND per-server opt-in), and each closed gate is NAMED in the reasons so a
/// shadow journal row says exactly what stands between it and a live write.
/// </summary>
public sealed class ForcePlanBotPolicyTests
{
    private static readonly DateTime Now = new(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc);

    private static ForcePlanTarget Target(
        double regressionFactor = 10.0,
        bool psp = false,
        string? replicaRole = null) => new(
            Database: "orders",
            QueryId: 42,
            PlanId: 7,
            BestPlanHash: "0x1111111111111111",
            LatestPlanHash: "0x2222222222222222",
            LatestCpuPerExecUs: 50000,
            BestCpuPerExecUs: 5000,
            RegressionFactor: regressionFactor,
            ReplicaRole: replicaRole,
            ParameterSensitivityCoFired: psp);

    private static ForcePlanBotSettings Enabled(bool dryRun = true) =>
        ForcePlanBotSettings.Default with { Enabled = true, DryRun = dryRun };

    private static ForcePlanBotDecision Evaluate(
        ForcePlanTarget target,
        ForcePlanBotSettings settings,
        bool serverOptedIn = false,
        ForcePlanBotHistory? history = null) =>
        ForcePlanBotPolicy.Evaluate(
            target, FactRemediation.ForcePlanBlockers(target), serverOptedIn,
            settings, history ?? ForcePlanBotHistory.Empty, Now);

    /* ---------------- the global off switch ---------------- */

    [Fact]
    public void DisabledBot_SuppressesEverything_EvenAFullyActionableTarget()
    {
        var decision = Evaluate(Target(), ForcePlanBotSettings.Default, serverOptedIn: true);

        Assert.Equal(ForcePlanBotDecisionKind.Suppressed, decision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonBotDisabled }, decision.Reasons);
    }

    [Fact]
    public void TheShippedDefaults_AreOffAndDryRun()
    {
        /* THE safety pin: a stock config must not evaluate, and even an operator who only flips
           Enabled must still be in dry run. Both halves of "no server gets a write without two
           deliberate gates" start from these two literals. */
        Assert.False(ForcePlanBotSettings.Default.Enabled);
        Assert.True(ForcePlanBotSettings.Default.DryRun);
    }

    /* ---------------- the never-auto-force contract ---------------- */

    [Fact]
    public void ParameterSensitivityCoFire_IsAlwaysBlocked_EvenWithEveryGateOpen()
    {
        /* #2140's standing rule as a data contract: the blockers come from the SAME
           FactRemediation.ForcePlanBlockers the MCP surfaces serve, and a flagged target is Blocked
           — not WouldForce, not Force — with the flag named, no matter that dry-run is off and the
           server opted in. */
        var decision = Evaluate(Target(psp: true), Enabled(dryRun: false), serverOptedIn: true);

        Assert.Equal(ForcePlanBotDecisionKind.Blocked, decision.Kind);
        Assert.Contains("parameter_sensitivity_cofired", decision.Reasons);
    }

    [Fact]
    public void SecondaryReplicaEvidence_IsBlocked_WithTheBlockerNamed()
    {
        var decision = Evaluate(Target(replicaRole: "Secondary"), Enabled(dryRun: false), serverOptedIn: true);

        Assert.Equal(ForcePlanBotDecisionKind.Blocked, decision.Kind);
        Assert.Contains("secondary_replica_evidence", decision.Reasons);
    }

    /* ---------------- the bot's own gates ---------------- */

    [Fact]
    public void BelowTheRegressionFloor_IsSuppressed_NotJournaled()
    {
        var decision = Evaluate(Target(regressionFactor: 1.5), Enabled());

        Assert.Equal(ForcePlanBotDecisionKind.Suppressed, decision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonBelowRegressionFloor }, decision.Reasons);
    }

    [Fact]
    public void QueryCooldown_SuppressesARepeatDecision_AndExpiryRestoresIt()
    {
        var inside = new ForcePlanBotHistory(Now.AddHours(-23), 0, 0);
        var outside = new ForcePlanBotHistory(Now.AddHours(-25), 0, 0);

        Assert.Equal(ForcePlanBotDecisionKind.Suppressed, Evaluate(Target(), Enabled(), history: inside).Kind);
        Assert.Equal(ForcePlanBotDecisionKind.WouldForce, Evaluate(Target(), Enabled(), history: outside).Kind);
    }

    [Fact]
    public void QueryCooldown_AlsoDedupesBlockedTargets()
    {
        /* A PSP-flagged query that stays regressed must not journal an identical 'blocked' row every
           analysis pass — the cooldown outranks the blocker check on purpose. */
        var inside = new ForcePlanBotHistory(Now.AddHours(-1), 0, 0);

        var decision = Evaluate(Target(psp: true), Enabled(), history: inside);

        Assert.Equal(ForcePlanBotDecisionKind.Suppressed, decision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonQueryCooldownActive }, decision.Reasons);
    }

    [Fact]
    public void FailedForceMemory_Blocks_AndHealsByTheWindowSliding()
    {
        /* Two failed forces inside the window block; the SAME policy with the window slid past them
           (the caller's read returns 0) is simply eligible again. No reset call exists — that is the
           #2677 lesson as an API shape: there is no flag to clear because there is no flag. */
        var twoFailed = new ForcePlanBotHistory(null, 0, 2);
        var oneFailed = new ForcePlanBotHistory(null, 0, 1);
        var windowSlid = new ForcePlanBotHistory(null, 0, 0);

        var blocked = Evaluate(Target(), Enabled(), history: twoFailed);
        Assert.Equal(ForcePlanBotDecisionKind.Blocked, blocked.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonFailedForceCooldown }, blocked.Reasons);

        Assert.Equal(ForcePlanBotDecisionKind.WouldForce, Evaluate(Target(), Enabled(), history: oneFailed).Kind);
        Assert.Equal(ForcePlanBotDecisionKind.WouldForce, Evaluate(Target(), Enabled(), history: windowSlid).Kind);
    }

    [Fact]
    public void ServerDailyBudget_BlocksTheFourthAction()
    {
        var exhausted = new ForcePlanBotHistory(null, 3, 0);

        var decision = Evaluate(Target(), Enabled(), history: exhausted);

        Assert.Equal(ForcePlanBotDecisionKind.Blocked, decision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonServerDailyBudgetExhausted }, decision.Reasons);
    }

    /* ---------------- the two write gates ---------------- */

    [Fact]
    public void DryRun_YieldsWouldForce_WithBothClosedGatesNamed()
    {
        var decision = Evaluate(Target(), Enabled(dryRun: true), serverOptedIn: false);

        Assert.Equal(ForcePlanBotDecisionKind.WouldForce, decision.Kind);
        Assert.Equal(
            new[] { ForcePlanBotPolicy.ReasonDryRun, ForcePlanBotPolicy.ReasonServerNotOptedIn },
            decision.Reasons);
    }

    [Fact]
    public void LiveMode_WithoutServerOptIn_StaysAdvisory()
    {
        var decision = Evaluate(Target(), Enabled(dryRun: false), serverOptedIn: false);

        Assert.Equal(ForcePlanBotDecisionKind.WouldForce, decision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonServerNotOptedIn }, decision.Reasons);
    }

    [Fact]
    public void DryRun_WithServerOptIn_StaysAdvisory()
    {
        var decision = Evaluate(Target(), Enabled(dryRun: true), serverOptedIn: true);

        Assert.Equal(ForcePlanBotDecisionKind.WouldForce, decision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonDryRun }, decision.Reasons);
    }

    [Fact]
    public void BothGatesOpen_IsTheOnlyPathToForce()
    {
        var decision = Evaluate(Target(), Enabled(dryRun: false), serverOptedIn: true);

        Assert.Equal(ForcePlanBotDecisionKind.Force, decision.Kind);
        Assert.Empty(decision.Reasons);
    }

    /* ---------------- settings hygiene ---------------- */

    [Fact]
    public void Normalize_ClampsTheValuesThatWouldDisarmTheCooldowns()
    {
        var reckless = new ForcePlanBotSettings
        {
            Enabled = true,
            DryRun = false,
            MinRegressionFactor = 0,
            QueryCooldownHours = 0,
            MaxActionsPerServerPerDay = 0,
            FailedForceThreshold = 0,
            FailedForceCooldownHours = -5,
            FirstReviewMinutes = 0,
            FinalReviewMinutes = -1,
            MinReviewExecutions = 0,
            NetBenefitRatio = 5.0,
        }.Normalize();

        Assert.Equal(1.0, reckless.MinRegressionFactor);
        Assert.Equal(1, reckless.QueryCooldownHours);
        Assert.Equal(1, reckless.MaxActionsPerServerPerDay);
        Assert.Equal(1, reckless.FailedForceThreshold);
        Assert.Equal(1, reckless.FailedForceCooldownHours);
        Assert.Equal(5, reckless.FirstReviewMinutes);
        /* The final checkpoint can never precede the first. */
        Assert.Equal(5, reckless.FinalReviewMinutes);
        Assert.Equal(1, reckless.MinReviewExecutions);
        Assert.Equal(1.0, reckless.NetBenefitRatio);
        /* Normalize clamps knobs, never flips gates — an operator's explicit arm survives. */
        Assert.True(reckless.Enabled);
        Assert.False(reckless.DryRun);
    }
}

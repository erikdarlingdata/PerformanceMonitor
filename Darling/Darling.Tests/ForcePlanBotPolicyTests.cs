/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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

    /* ---------------- the bot's whole blocker list (#3654) ---------------- */

    private static readonly DateTime Observed = new(2026, 9, 18, 12, 0, 0, DateTimeKind.Utc);

    private static ForcePlanTargetState Clean(string? flgp = "OFF", DateTime? enablementAt = null) => new(
        PlanIsForced: null, PlanForcingType: null, ForceFailureCount: null, LastForceFailureReason: null,
        PlanObservedAtUtc: null,
        OtherForcedPlanId: null, OtherForcedPlanForcingType: null, OtherForcedPlanObservedAtUtc: null,
        ApcState: null, ApcStateReason: null, ApcRegressedPlanId: null, ApcLastGoodPlanId: null,
        ApcLastGoodPlanForcingType: null, ApcLastGoodPlanIsForced: null, ApcLastGoodPlanForceFailureReason: null,
        ApcExecuteActionInitiatedBy: null, ApcObservedAtUtc: null,
        ForceLastGoodPlanActualState: flgp, EnablementObservedAtUtc: enablementAt ?? (flgp is null ? null : Observed));

    [Fact]
    public void Blockers_IsTheWholeSharedGate_PlusNothing_WhenTheStateIsCleanAndObserved()
    {
        /* The happy path: state read, FLGP OFF, nothing forced, no recommendation. The bot's list is
           exactly the two-argument gate's (empty here) — no bot-only blocker fires on a clean read. */
        var blockers = ForcePlanBotPolicy.Blockers(Target(), Clean(), stateUnavailableReason: null);

        Assert.Empty(blockers);
        Assert.Empty(ForcePlanBotPolicy.Names(blockers));
        Assert.Null(ForcePlanBotPolicy.Evidence(blockers));
    }

    [Fact]
    public void Blockers_CarriesTheSharedGatesVerdict_Verbatim()
    {
        /* Same function, same names, same evidence strings as structured_remediation — the bot never
           recomputes the gate (#2146). APC AUTO-forced on the target plan, plus the target's own PSP flag. */
        var state = Clean() with
        {
            PlanIsForced = true, PlanForcingType = "AUTO", ForceFailureCount = 0, PlanObservedAtUtc = Observed,
            ApcState = "Verifying", ApcLastGoodPlanId = 7, ApcObservedAtUtc = Observed,
        };
        var target = Target(psp: true);

        var shared = FactRemediation.ForcePlanBlockers(target, state);
        var bot = ForcePlanBotPolicy.Blockers(target, state, stateUnavailableReason: null);

        Assert.Equal(shared, bot);
        Assert.Equal(new[] { "parameter_sensitivity_cofired", "apc_owns_it" }, ForcePlanBotPolicy.Names(bot));
    }

    [Fact]
    public void Blockers_FlgpOn_AddsTheStandDown_AfterTheSharedGate()
    {
        var state = Clean(flgp: "ON") with
        {
            PlanIsForced = true, PlanForcingType = "AUTO", PlanObservedAtUtc = Observed,
        };

        var blockers = ForcePlanBotPolicy.Blockers(Target(), state, stateUnavailableReason: null);

        Assert.Equal(new[] { "apc_owns_it", ForcePlanBotPolicy.ReasonApcEnabledForDatabase }, ForcePlanBotPolicy.Names(blockers));
        var standDown = blockers[1];
        Assert.Contains("force_last_good_plan_actual_state = ON for orders at 2026-09-18T12:00:00Z", standDown.Evidence, StringComparison.Ordinal);
        Assert.Contains("second forcer", standDown.Evidence, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("ON")]
    [InlineData("on")]
    [InlineData("On")]
    public void Blockers_FlgpOn_IsCaseInsensitive_OnTheStoredSpelling(string spelling)
    {
        var blockers = ForcePlanBotPolicy.Blockers(Target(), Clean(flgp: spelling), stateUnavailableReason: null);

        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonApcEnabledForDatabase }, ForcePlanBotPolicy.Names(blockers));
    }

    [Theory]
    [InlineData("OFF")]
    [InlineData("off")]
    public void Blockers_FlgpOff_FiresNothing(string spelling)
    {
        Assert.Empty(ForcePlanBotPolicy.Blockers(Target(), Clean(flgp: spelling), stateUnavailableReason: null));
    }

    [Fact]
    public void Blockers_NullState_IsUnavailable_QuotingTheReadersReason()
    {
        var blockers = ForcePlanBotPolicy.Blockers(Target(), state: null, stateUnavailableReason: "the read failed (NpgsqlException: timeout)");

        var only = Assert.Single(blockers);
        Assert.Equal(ForcePlanBotPolicy.ReasonStateUnavailable, only.Name);
        Assert.StartsWith("the read failed (NpgsqlException: timeout) — an unattended force cannot proceed", only.Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void Blockers_NullState_WithNoReason_IsUnavailable_NamingTheMissingKey()
    {
        var blockers = ForcePlanBotPolicy.Blockers(Target(), state: null, stateUnavailableReason: null);

        var only = Assert.Single(blockers);
        Assert.Equal(ForcePlanBotPolicy.ReasonStateUnavailable, only.Name);
        Assert.Contains("returned no row for plan 7 of query 42 in orders", only.Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void Blockers_NullState_StillCarriesTheTargetHalf_First()
    {
        /* Unknown state does not erase what the target itself says: PSP is named, then the
           unavailability. */
        var blockers = ForcePlanBotPolicy.Blockers(Target(psp: true), state: null, stateUnavailableReason: "boom");

        Assert.Equal(new[] { "parameter_sensitivity_cofired", ForcePlanBotPolicy.ReasonStateUnavailable }, ForcePlanBotPolicy.Names(blockers));
    }

    [Fact]
    public void Blockers_EmptyState_IsUnavailable_BecauseTheEnablementHalfIsTheCollectorsEveryDatabaseRow()
    {
        var empty = Clean(flgp: null);
        Assert.True(empty.IsEmpty);

        var blockers = ForcePlanBotPolicy.Blockers(Target(), empty, stateUnavailableReason: null);

        var only = Assert.Single(blockers);
        Assert.Equal(ForcePlanBotPolicy.ReasonStateUnavailable, only.Name);
        Assert.Contains("observed nothing for this target inside the last 24 hours", only.Evidence, StringComparison.Ordinal);
        Assert.Contains("no plan_correction capture for orders at all", only.Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void Blockers_APlanNotObservedButADatabaseThatIs_IsNotUnavailable()
    {
        /* The ordinary shape for a best plan that is not executing: no query_store_stats row for it, no
           recommendation — but the enablement row is there (OFF), so the store CAN see the database and
           the bot proceeds. Null on a half means not observed, not unavailable. */
        var state = Clean(flgp: "OFF");
        Assert.False(state.IsEmpty);

        Assert.Empty(ForcePlanBotPolicy.Blockers(Target(), state, stateUnavailableReason: null));
    }

    [Fact]
    public void Evidence_IsOneLinePerBlocker_NameColonEvidence()
    {
        var blockers = ForcePlanBotPolicy.Blockers(Target(psp: true), Clean(flgp: "ON"), stateUnavailableReason: null);

        var detail = ForcePlanBotPolicy.Evidence(blockers);
        Assert.NotNull(detail);
        var lines = detail!.Split('\n');
        Assert.Equal(2, lines.Length);
        Assert.StartsWith("parameter_sensitivity_cofired: ", lines[0], StringComparison.Ordinal);
        Assert.StartsWith("apc_enabled_for_database: ", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public void TheBotsOwnReasons_AreNotInTheSharedVocabulary()
    {
        /* They belong to the actor, not the advice: an agent reading structured_remediation must never
           see state_unavailable or apc_enabled_for_database as a blocker, because for a reader neither
           IS one. Pinned as strings because both are consumer API. */
        Assert.Equal("state_unavailable", ForcePlanBotPolicy.ReasonStateUnavailable);
        Assert.Equal("apc_enabled_for_database", ForcePlanBotPolicy.ReasonApcEnabledForDatabase);

        var shared = typeof(ForcePlanBlockerNames).GetFields()
            .Select(f => (string)f.GetValue(null)!)
            .ToArray();
        Assert.DoesNotContain(ForcePlanBotPolicy.ReasonStateUnavailable, shared);
        Assert.DoesNotContain(ForcePlanBotPolicy.ReasonApcEnabledForDatabase, shared);
    }

    [Fact]
    public void Evaluate_WithTheBotsBlockers_IsBlocked_EvenWithEveryGateOpen()
    {
        /* End to end through the decision table: the state names apc_enabled_for_database, and no
           combination of gates turns that into WouldForce or Force. */
        var blockers = ForcePlanBotPolicy.Blockers(Target(), Clean(flgp: "ON"), stateUnavailableReason: null);

        var decision = ForcePlanBotPolicy.Evaluate(
            Target(), ForcePlanBotPolicy.Names(blockers), serverOptedIn: true, Enabled(dryRun: false),
            ForcePlanBotHistory.Empty, Now);

        Assert.Equal(ForcePlanBotDecisionKind.Blocked, decision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonApcEnabledForDatabase }, decision.Reasons);
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
    /* ---------------- #3953: the best plan's age ---------------- */

    /// <summary>
    /// The interval table makes the 14-day window real, so a best plan can be two weeks old. The unattended bot does
    /// not act on one older than <see cref="ForcePlanBotPolicy.MaxBestPlanAgeDays"/>: it is Blocked with the reason
    /// named, even with every gate open. A plan inside the age runs the ordinary decision, and a target with no age
    /// (a finding from before the column, when raw's retention bounded it) is not gated.
    /// </summary>
    [Fact]
    public void AStaleBestPlan_IsBlocked_AFreshOneIsNot_AndAnUnknownAgeIsNotGated()
    {
        var stale = Target() with { BestPlanLastSeenUtc = Now.AddDays(-(ForcePlanBotPolicy.MaxBestPlanAgeDays + 1)) };
        var fresh = Target() with { BestPlanLastSeenUtc = Now.AddDays(-(ForcePlanBotPolicy.MaxBestPlanAgeDays - 1)) };

        var staleDecision = Evaluate(stale, Enabled(dryRun: false), serverOptedIn: true);
        Assert.Equal(ForcePlanBotDecisionKind.Blocked, staleDecision.Kind);
        Assert.Equal(new[] { ForcePlanBotPolicy.ReasonBestPlanStale }, staleDecision.Reasons);

        Assert.Equal(ForcePlanBotDecisionKind.Force, Evaluate(fresh, Enabled(dryRun: false), serverOptedIn: true).Kind);
        Assert.Equal(ForcePlanBotDecisionKind.Force, Evaluate(Target(), Enabled(dryRun: false), serverOptedIn: true).Kind);
        Assert.Equal("best_plan_stale", ForcePlanBotPolicy.ReasonBestPlanStale);
        Assert.Equal(4, ForcePlanBotPolicy.MaxBestPlanAgeDays);
    }

    /// <summary>The drill-down's <c>best_plan_last_seen</c> reaches the target through the shared extractor.</summary>
    [Fact]
    public void TheExtractor_CarriesTheBestPlansLastSeen_FromTheDrillDown()
    {
        var lastSeen = new DateTime(2026, 8, 25, 13, 45, 0, DateTimeKind.Unspecified);
        var finding = new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = Now,
            ServerId = 42,
            ServerName = "SQL01",
            DatabaseName = "orders",
            TimeRangeStart = Now.AddHours(-4),
            TimeRangeEnd = Now,
            Severity = 1.6,
            Confidence = 1.0,
            Category = "queries",
            StoryPath = "PLAN_REGRESSION",
            StoryPathHash = "hash_PLAN_REGRESSION",
            StoryText = "story",
            RootFactKey = "PLAN_REGRESSION",
            DrillDown = new Dictionary<string, object>
            {
                ["regressed_queries"] = new[]
                {
                    new Dictionary<string, object?>
                    {
                        ["database"] = "orders",
                        ["query_id"] = 42L,
                        ["best_plan_id"] = 7L,
                        ["regression_factor"] = 10.0,
                        ["best_plan_last_seen"] = lastSeen,
                    },
                },
            },
        };

        var target = Assert.Single(FactRemediation.ExtractPlanRegressionTargets(finding));
        Assert.Equal(lastSeen, target.BestPlanLastSeenUtc);
    }
    /// <summary>
    /// #3953: the advice states how old the faster plan is when the fact carries <c>best_plan_age_days</c>, and says
    /// what it always said when it does not (a fact from before the key existed, or Lite's older payloads).
    /// </summary>
    [Fact]
    public void ThePlanRegressionAdvice_StatesTheBestPlansAge_WhenTheFactCarriesIt()
    {
        static IReadOnlyDictionary<string, Fact> Facts(double? ageDays)
        {
            var metadata = new Dictionary<string, double>
            {
                ["worst_regression_factor"] = 6,
                ["offender_count"] = 2,
                ["latest_cpu_per_exec_us"] = 60000,
                ["best_cpu_per_exec_us"] = 10000,
            };
            if (ageDays is double age)
            {
                metadata["best_plan_age_days"] = age;
            }

            return new Dictionary<string, Fact>
            {
                ["PLAN_REGRESSION"] = new Fact { Source = "queries", Key = "PLAN_REGRESSION", Value = 6, Metadata = metadata },
            };
        }

        Assert.Contains("the faster plan on record (it last ran 9 days ago), so",
            FactAdvice.Compose("PLAN_REGRESSION", Facts(9.6))!.Investigation, StringComparison.Ordinal);
        Assert.Contains("the faster plan on record (it last ran within the past day), so",
            FactAdvice.Compose("PLAN_REGRESSION", Facts(0.4))!.Investigation, StringComparison.Ordinal);
        Assert.Contains("the faster plan on record, so",
            FactAdvice.Compose("PLAN_REGRESSION", Facts(null))!.Investigation, StringComparison.Ordinal);
    }
}

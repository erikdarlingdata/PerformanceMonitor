/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins #3652: the force-plan verdict reads the store's forcing and automatic-plan-correction state, and
/// <c>eligible</c> means eligible. The fixtures are the SHAPES of the live cross-reference that filed the
/// issue — five targets on one production incident, every one <c>eligible: true, blockers: []</c>, every
/// one contraindicated by the same server's <c>plan_correction</c> snapshot minutes later: target plan ==
/// APC's <c>last_good_plan_id</c> with <c>is_forced TRUE, forcing_type AUTO, state Verifying</c> (×2);
/// <c>Reverted / ForcingFailed</c> on the proposed plan; <c>Expired / TempTableChanged</c>; <c>Success</c>
/// via a DIFFERENT plan. Ids here are synthetic; the shapes are the evidence.
///
/// <para>Three layers: the pure gate (<see cref="FactRemediation.ForcePlanBlockers(ForcePlanTarget, ForcePlanTargetState?)"/>
/// and the state-taking <c>BuildStructuredRemediation</c>) with the unknown-state and FLGP-on arms; the
/// in-app arming gate (<see cref="OperatorRemediationGate"/>) refusing a surface on the new blockers; and
/// the DuckDB reader (<see cref="ForcePlanTargetStateReader"/>) round-tripped over seeded
/// <c>query_store_stats</c> + <c>plan_correction</c> rows, plus the parity pin holding Darling's Postgres
/// twin to the same text.</para>
/// </summary>
public sealed class ForcePlanTargetStateTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 3652;
    private const string Db = "Orders";

    private static readonly DateTime Snap = new(2026, 9, 18, 21, 30, 0, DateTimeKind.Utc);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;
    /* The server the seeders write rows for; a test that reads through the tool sets it to the tool's own id. */
    private int _seedServerId = ServerId;

    public ForcePlanTargetStateTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /* ------------------------------------------------------------------ fixtures */

    private static ForcePlanTarget Target(long queryId = 123, long planId = 99, bool psp = false, string? replica = null) =>
        new(Db, queryId, planId, "0xBEST", "0xLATEST", 9000, 1200, 7.5, replica, psp);

    private static RemediationAction Action(params ForcePlanTarget[] targets) => new("PLAN_REGRESSION", "force", targets);

    /// <summary>A state with every half null — the reader's shape for a target nothing was collected for.</summary>
    private static ForcePlanTargetState Empty() => new(
        null, null, null, null, null,
        null, null, null,
        null, null, null, null, null, null, null, null, null,
        null, null);

    private static ForcePlanTargetState State(
        bool? planForced = null, string? forcingType = null, long? failures = null, string? failureReason = null,
        long? otherForcedPlan = null, string? otherForcingType = null,
        string? apcState = null, string? apcReason = null, long? apcLastGood = null, string? apcLastGoodForcingType = null,
        bool? apcLastGoodForced = null, string? apcLastGoodFailure = null, string? initiatedBy = null,
        string? flgp = null) =>
        new(
            PlanIsForced: planForced,
            PlanForcingType: forcingType,
            ForceFailureCount: failures,
            LastForceFailureReason: failureReason,
            PlanObservedAtUtc: planForced is null && failures is null ? null : Snap,
            OtherForcedPlanId: otherForcedPlan,
            OtherForcedPlanForcingType: otherForcingType,
            OtherForcedPlanObservedAtUtc: otherForcedPlan is null ? null : Snap,
            ApcState: apcState,
            ApcStateReason: apcReason,
            ApcRegressedPlanId: apcState is null ? null : 7,
            ApcLastGoodPlanId: apcLastGood,
            ApcLastGoodPlanForcingType: apcLastGoodForcingType,
            ApcLastGoodPlanIsForced: apcLastGoodForced,
            ApcLastGoodPlanForceFailureReason: apcLastGoodFailure,
            ApcExecuteActionInitiatedBy: initiatedBy,
            ApcObservedAtUtc: apcState is null ? null : Snap.AddMinutes(2),
            ForceLastGoodPlanActualState: flgp,
            EnablementObservedAtUtc: flgp is null ? null : Snap.AddMinutes(2));

    private static Dictionary<ForcePlanTargetKey, ForcePlanTargetState> For(ForcePlanTarget t, ForcePlanTargetState s) =>
        new(ForcePlanTargetKey.Comparer) { [ForcePlanTargetKey.Of(t)] = s };

    private static StructuredForcePlanTarget Project(ForcePlanTarget t, ForcePlanTargetState? s, string? reason = null) =>
        Assert.Single(FactRemediation.BuildStructuredRemediation(
            Action(t), s is null ? null : For(t, s), reason)!.ForcePlanTargets);

    /* ------------------------------------------------------------------ 1. the five arms, live shapes */

    [Fact]
    public void ApcVerifyingItsOwnAutoForce_OnTheProposedPlan_IsApcOwnsIt_NotEligible()
    {
        /* Two of the five: query_store_stats says the target plan is forced AUTO, plan_correction says
           Verifying with last_good_plan_id == the target. Before #3652 this read eligible, and a manual
           force here replaces AUTO forcing and deletes the engine's revert path. */
        var t = Target();
        var projected = Project(t, State(
            planForced: true, forcingType: "AUTO",
            apcState: "Verifying", apcLastGood: 99, apcLastGoodForcingType: "AUTO", apcLastGoodForced: true,
            initiatedBy: "System", flgp: "ON"));

        Assert.False(projected.Eligible);
        Assert.Equal(new[] { ForcePlanBlockerNames.ApcOwnsIt }, projected.Blockers);
        var evidence = Assert.Single(projected.BlockerEvidence!);
        Assert.Equal("apc_owns_it", evidence.Name);
        Assert.Contains("plan_forcing_type = AUTO", evidence.Evidence, StringComparison.Ordinal);
        Assert.Contains("2026-09-18T21:30:00Z", evidence.Evidence, StringComparison.Ordinal);
        Assert.Null(projected.StateNote);
        /* The verb changed: FLGP is on, and the guidance names the engine's own state. */
        Assert.Equal("on", projected.ApcMode);
        Assert.Contains("Automatic plan correction is ON for [Orders]", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains("currently Verifying for query 123 via plan 99 (the plan proposed here)", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains("intervene only if it reverts or expires", projected.Guidance, StringComparison.Ordinal);
        /* ForceSql stays for the operator who has read all that and decided. */
        Assert.Contains("sp_query_store_force_plan @query_id = 123, @plan_id = 99", projected.ForceSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ANewerExpiredState_MakesTheOlderAutoForcedSiblingStale_AndTheGuidanceSaysTheEngineHasBackedOut()
    {
        /* #8628, the Lite twin of the Darling gate tests: plan 55 of the query was seen AUTO-forced at the snapshot, and the
           correction state two minutes later says Expired and that plan 55 is not forced. The older row must not raise
           apc_owns_it, and "intervene only if it reverts or expires" must not be said about a state that already is. */
        var t = Target();
        var projected = Project(t, State(
            otherForcedPlan: 55, otherForcingType: "AUTO",
            apcState: "Expired", apcReason: "StatisticsChanged", apcLastGood: 55, apcLastGoodForced: false, flgp: "ON"));

        Assert.True(projected.Eligible);
        Assert.Empty(projected.Blockers);
        Assert.Contains("is currently Expired (StatisticsChanged) for query 123 via plan 55 (not the plan proposed here, 99)", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains("the engine has backed out of it, so a manual force here is yours to decide", projected.Guidance, StringComparison.Ordinal);
        Assert.DoesNotContain("intervene only if", projected.Guidance, StringComparison.Ordinal);
    }

    [Fact]
    public void ARevertedTarget_StaysBlocked_AndItsGuidanceNamesTheBlockerAndTheBackedOutEngine()
    {
        var t = Target();
        var projected = Project(t, State(
            planForced: true, forcingType: "AUTO",
            apcState: "Reverted", apcReason: "VerificationForcedQueryRecompile", apcLastGood: 99, apcLastGoodForced: false, flgp: "ON"));

        Assert.False(projected.Eligible);
        Assert.DoesNotContain(ForcePlanBlockerNames.ApcOwnsIt, projected.Blockers);
        Assert.Contains(ForcePlanBlockerNames.ForcingFailedOnThisPlan, projected.Blockers);
        Assert.Contains("the engine has backed out of it, so a manual force here is yours to decide", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains("This target is not eligible: forcing_failed_on_this_plan (", projected.Guidance, StringComparison.Ordinal);
        Assert.DoesNotContain("intervene only if", projected.Guidance, StringComparison.Ordinal);
    }

    [Fact]
    public void ApcVerifying_WithoutAQueryStoreRowForThePlan_StillOwnsIt_FromThePlanCorrectionHalf()
    {
        /* The best plan is not executing, so query_store_stats has no recent row for it (null, not false):
           the plan_correction half alone carries the verdict. */
        var projected = Project(Target(), State(apcState: "Verifying", apcLastGood: 99, apcLastGoodForced: true, apcLastGoodForcingType: "AUTO"));

        Assert.Equal(new[] { "apc_owns_it" }, projected.Blockers);
        Assert.Contains("recommendation state Verifying with last_good_plan_id = 99", projected.BlockerEvidence![0].Evidence, StringComparison.Ordinal);
        Assert.Contains("last_good_plan_is_forced = true, last_good_plan_forcing_type = AUTO", projected.BlockerEvidence[0].Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void RevertedForcingFailed_OnTheProposedPlan_IsForcingFailedOnThisPlan()
    {
        /* The third of the five: the engine already tried exactly this plan and could not force it. */
        var projected = Project(Target(), State(
            apcState: "Reverted", apcReason: "ForcingFailed", apcLastGood: 99, apcLastGoodFailure: "NO_PLAN"));

        Assert.False(projected.Eligible);
        Assert.Equal(new[] { ForcePlanBlockerNames.ForcingFailedOnThisPlan }, projected.Blockers);
        var e = projected.BlockerEvidence![0].Evidence;
        Assert.Contains("Reverted / ForcingFailed", e, StringComparison.Ordinal);
        Assert.Contains("last_good_plan_force_failure_reason = NO_PLAN", e, StringComparison.Ordinal);
        Assert.Contains("could not force it", e, StringComparison.Ordinal);
    }

    [Fact]
    public void ForceFailureCounter_OnTheProposedPlan_IsForcingFailedOnThisPlan_TheAlertFamilysOwnEvidence()
    {
        /* The Forced Plan Failing family (#2157/#3579) pages on this counter; the verdict now reads it. */
        var projected = Project(Target(), State(planForced: true, forcingType: "MANUAL", failures: 4, failureReason: "GENERAL_FAILURE"));

        Assert.Contains(ForcePlanBlockerNames.ForcingFailedOnThisPlan, projected.Blockers);
        Assert.Contains(ForcePlanBlockerNames.AlreadyForced, projected.Blockers);
        var failed = projected.BlockerEvidence!.Single(b => b.Name == "forcing_failed_on_this_plan").Evidence;
        Assert.Contains("force_failure_count = 4, last_force_failure_reason = GENERAL_FAILURE", failed, StringComparison.Ordinal);
    }

    [Fact]
    public void ExpiredTempTableChanged_OnTheProposedPlan_IsApcWithdrewIt_QuotingTheEnginesReason()
    {
        /* The fourth of the five. The reason is the engine's own vocabulary, verbatim. */
        var projected = Project(Target(), State(apcState: "Expired", apcReason: "TempTableChanged", apcLastGood: 99));

        Assert.False(projected.Eligible);
        Assert.Equal(new[] { ForcePlanBlockerNames.ApcWithdrewIt }, projected.Blockers);
        Assert.Contains("Expired / TempTableChanged", projected.BlockerEvidence![0].Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void ExpiredForcingFailed_IsNamedAFailure_NotAWithdrawal()
    {
        /* Evaluation order: the failure arm runs before the Expired arm on purpose. */
        var projected = Project(Target(), State(apcState: "Expired", apcReason: "ForcingFailed", apcLastGood: 99));
        Assert.Equal(new[] { ForcePlanBlockerNames.ForcingFailedOnThisPlan }, projected.Blockers);
    }

    [Fact]
    public void SuccessViaADifferentPlan_IsApcResolvedDifferently_NamingThatPlan()
    {
        /* The fifth of the five: the engine already fixed this query — with plan 101, not 99. */
        var projected = Project(Target(), State(
            apcState: "Success", apcReason: "LastGoodPlanForced", apcLastGood: 101, apcLastGoodForced: true, apcLastGoodForcingType: "AUTO"));

        Assert.False(projected.Eligible);
        Assert.Equal(new[] { ForcePlanBlockerNames.ApcResolvedDifferently }, projected.Blockers);
        Assert.Contains("last_good_plan_id = 101 (not 99)", projected.BlockerEvidence![0].Evidence, StringComparison.Ordinal);
        Assert.Contains("through plan 101", projected.BlockerEvidence[0].Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void SuccessOnTheProposedPlan_IsApcOwnsIt()
    {
        var projected = Project(Target(), State(apcState: "Success", apcReason: "LastGoodPlanForced", apcLastGood: 99, apcLastGoodForced: true, apcLastGoodForcingType: "AUTO"));
        Assert.Equal(new[] { ForcePlanBlockerNames.ApcOwnsIt }, projected.Blockers);
    }

    [Fact]
    public void ManuallyForcedAlready_OnThisPlan_IsAlreadyForced_NothingToDo()
    {
        var projected = Project(Target(), State(planForced: true, forcingType: "MANUAL", failures: 0, failureReason: "NONE"));
        Assert.Equal(new[] { ForcePlanBlockerNames.AlreadyForced }, projected.Blockers);
        Assert.Contains("there is nothing to force", projected.BlockerEvidence![0].Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void ADifferentPlanForcedByHand_IsAlreadyForced_AsAConflictNamingIt()
    {
        var projected = Project(Target(), State(otherForcedPlan: 42, otherForcingType: "MANUAL"));
        Assert.Equal(new[] { ForcePlanBlockerNames.AlreadyForced }, projected.Blockers);
        Assert.Contains("plan 42 of this query is forced", projected.BlockerEvidence![0].Evidence, StringComparison.Ordinal);
        Assert.Contains("forcing 99 silently replaces it", projected.BlockerEvidence[0].Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void ADifferentPlanForcedAuto_IsApcOwnsIt_NotAlreadyForced()
    {
        var projected = Project(Target(), State(otherForcedPlan: 42, otherForcingType: "AUTO"));
        Assert.Equal(new[] { ForcePlanBlockerNames.ApcOwnsIt }, projected.Blockers);
        Assert.Contains("plan 42 of this query is forced with plan_forcing_type = AUTO", projected.BlockerEvidence![0].Evidence, StringComparison.Ordinal);
    }

    [Fact]
    public void ActiveRecommendationNamingTheProposedPlan_BlocksNothing_TheEngineAgrees()
    {
        /* FLGP off, the engine offering the script for exactly this plan: the manual force is what it
           suggests. Eligible, no note, no guidance. */
        var projected = Project(Target(), State(apcState: "Active", apcReason: "AutomaticTuningOptionNotEnabled", apcLastGood: 99, flgp: "OFF"));

        Assert.True(projected.Eligible);
        Assert.Empty(projected.Blockers);
        Assert.Null(projected.StateNote);
        Assert.Equal("off", projected.ApcMode);
        Assert.Null(projected.Guidance);
    }

    [Fact]
    public void ActiveRecommendationNamingAnotherPlan_IsGuidance_NotABlocker()
    {
        var projected = Project(Target(), State(apcState: "Active", apcLastGood: 101, flgp: "OFF"));
        Assert.True(projected.Eligible);
        Assert.Contains("names plan 101, not 99", projected.Guidance, StringComparison.Ordinal);
    }

    [Fact]
    public void ActiveRecommendationNamingTheProposedPlanAsRegressed_SaysSo_AndTheAdvisoryVerdictStaysOpen()
    {
        /* #4736: the fixture's regressed plan is 7, so a target on plan 7 is the plan the engine calls the
           worse one. The guidance says so instead of only "two candidate plans"; the bot (not this
           surface) carries the blocker. */
        var projected = Project(Target(planId: 7), State(apcState: "Active", apcLastGood: 101, flgp: "OFF"));

        Assert.True(projected.Eligible);
        Assert.Contains("names plan 7", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains("the plan proposed here", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains("worse", projected.Guidance, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Reverted")]
    [InlineData("Expired")]
    public void WithdrawnRecommendationNamingTheProposedPlanAsRegressed_SaysNothing(string apcState)
    {
        var projected = Project(Target(planId: 7), State(apcState: apcState, apcLastGood: 101, flgp: "OFF"));

        Assert.Null(projected.Guidance);
    }

    [Fact]
    public void ActiveRecommendationNamingTheProposedPlanAsRegressed_WithAutomaticPlanCorrectionOn_SaysSoInTheOnWording()
    {
        /* #4736: with FORCE_LAST_GOOD_PLAN on the guidance is the "Automatic plan correction is ON" text, a
           separate branch from the off-mode text above, and it carries its own sentence for the same fact.
           Every other test of this pair runs with the setting off, so only this one covers that sentence. */
        var projected = Project(Target(planId: 7), State(apcState: "Active", apcLastGood: 101, flgp: "ON"));

        Assert.Equal("on", projected.ApcMode);
        Assert.Contains("Automatic plan correction is ON for [Orders]", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains(
            "The engine names plan 7 (the plan proposed here) as the regressed, worse plan.",
            projected.Guidance, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Active", 99)]      /* open, but it names plan 7 as the regressed one and the target is plan 99 */
    [InlineData("Reverted", 7)]     /* names the target, but the engine withdrew it */
    [InlineData("Expired", 7)]
    public void WithAutomaticPlanCorrectionOn_TheRegressedPlanSentence_NeedsAnOpenRecommendationNamingTheTargetPlan(
        string apcState, long targetPlan)
    {
        var projected = Project(Target(planId: targetPlan), State(apcState: apcState, apcLastGood: 101, flgp: "ON"));

        Assert.Equal("on", projected.ApcMode);
        Assert.Contains("Automatic plan correction is ON for [Orders]", projected.Guidance, StringComparison.Ordinal);
        Assert.DoesNotContain("as the regressed, worse plan", projected.Guidance, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTwoOriginalBlockers_StackWithTheNewOnes_AndCarryEvidenceToo()
    {
        var projected = Project(Target(psp: true, replica: "Secondary"), State(planForced: true, forcingType: "AUTO"));
        Assert.Equal(
            new[] { "parameter_sensitivity_cofired", "secondary_replica_evidence", "apc_owns_it" },
            projected.Blockers);
        Assert.Equal(3, projected.BlockerEvidence!.Count);
        Assert.All(projected.BlockerEvidence, b => Assert.False(string.IsNullOrWhiteSpace(b.Evidence)));
    }

    /* ------------------------------------------------------------------ 2. unknown state is SAID */

    [Fact]
    public void NoStateAtAll_IsThe2138Verdict_WithAStateNote_NeverSilentlyEligible()
    {
        /* The state-less overload, and the state-taking overload with null — both say so. */
        var legacy = Assert.Single(FactRemediation.BuildStructuredRemediation(Action(Target()))!.ForcePlanTargets);
        Assert.True(legacy.Eligible);
        Assert.Empty(legacy.Blockers);
        Assert.StartsWith("unknown: ", legacy.StateNote, StringComparison.Ordinal);
        Assert.Contains("was not read", legacy.StateNote, StringComparison.Ordinal);
        Assert.Null(legacy.ForcingState);
        Assert.Null(legacy.ApcMode);

        var failed = Project(Target(), null, "the forcing and automatic-plan-correction state read failed (NpgsqlException: relation missing)");
        Assert.Equal("unknown: the forcing and automatic-plan-correction state read failed (NpgsqlException: relation missing)", failed.StateNote);
    }

    [Fact]
    public void ATargetTheReadFoundNothingFor_GetsTheLookbackNote_AndNoBlockers()
    {
        var projected = Project(Target(), Empty());
        Assert.True(projected.Eligible);
        Assert.Contains("no query_store_stats or plan_correction row for this target within the last 24 hours", projected.StateNote, StringComparison.Ordinal);
        Assert.Null(projected.ForcingState);

        /* And a dictionary that simply lacks the key reads the same way. */
        var other = Target(queryId: 999);
        var missing = Assert.Single(FactRemediation.BuildStructuredRemediation(Action(other), For(Target(), State(planForced: true, forcingType: "AUTO")))!.ForcePlanTargets);
        Assert.True(missing.Eligible);
        Assert.Contains("within the last 24 hours", missing.StateNote, StringComparison.Ordinal);
    }

    [Fact]
    public void FlgpOn_WithNoOpenRecommendation_ChangesTheVerb_WithoutBlocking()
    {
        var projected = Project(Target(), State(flgp: "ON"));
        Assert.True(projected.Eligible);
        Assert.Equal("on", projected.ApcMode);
        Assert.Contains("has no open recommendation for query 123", projected.Guidance, StringComparison.Ordinal);
        Assert.Contains("the engine may later revert or replace it", projected.Guidance, StringComparison.Ordinal);

        var remediation = FactRemediation.BuildStructuredRemediation(Action(Target()), For(Target(), State(flgp: "ON")));
        Assert.Contains("FORCE_LAST_GOOD_PLAN) is ON for at least one of these databases", remediation!.Guidance, StringComparison.Ordinal);

        /* FLGP off or unknown: no remediation-level guidance. */
        Assert.Null(FactRemediation.BuildStructuredRemediation(Action(Target()), For(Target(), State(flgp: "OFF")))!.Guidance);
        Assert.Null(FactRemediation.BuildStructuredRemediation(Action(Target()))!.Guidance);
    }

    /* ------------------------------------------------------------------ 3. the in-app Apply gate honors it */

    [Fact]
    public void TheArmingGate_RefusesASurface_OnEveryNewBlocker()
    {
        /* OperatorRemediationGate reads Eligible and Blockers from the projection and nothing else — so
           the five #3652 blockers disarm the in-app force without the gate learning a single new word. */
        var capable = new EvictCapability(StatementSupported: true, CredentialHoldsAlterServerState: true);
        var states = new[]
        {
            State(planForced: true, forcingType: "AUTO"),
            State(planForced: true, forcingType: "MANUAL"),
            State(apcState: "Reverted", apcReason: "ForcingFailed", apcLastGood: 99),
            State(apcState: "Expired", apcReason: "TempTableChanged", apcLastGood: 99),
            State(apcState: "Success", apcLastGood: 101),
        };
        foreach (var s in states)
        {
            var projected = Project(Target(), s);
            Assert.False(projected.Eligible);
            Assert.Null(OperatorRemediationGate.SurfaceFor(projected, remediationCredentialConfigured: true, capable));
        }

        /* And the clean case still arms. */
        var clean = Project(Target(), State(apcState: "Active", apcLastGood: 99, flgp: "OFF"));
        Assert.NotNull(OperatorRemediationGate.SurfaceFor(clean, remediationCredentialConfigured: true, capable));
    }

    /* ------------------------------------------------------------------ 4. wire shape + vocabulary */

    [Fact]
    public void WireShape_CarriesTheNewFields_SnakeCase_UnderTheMcpOptions()
    {
        var json = JsonSerializer.Serialize(
            FactRemediation.BuildStructuredRemediation(Action(Target()), For(Target(), State(planForced: true, forcingType: "AUTO", apcState: "Verifying", apcLastGood: 99, flgp: "ON"))),
            McpHelpers.JsonOptions);

        foreach (var field in new[]
        {
            "\"blocker_evidence\"", "\"blocker\"", "\"evidence\"", "\"forcing_state\"", "\"state_note\"", "\"apc_mode\"", "\"guidance\"",
            "\"plan_is_forced\"", "\"plan_forcing_type\"", "\"force_failure_count\"", "\"last_force_failure_reason\"", "\"plan_observed_at\"",
            "\"other_forced_plan_id\"", "\"apc_state\"", "\"apc_state_reason\"", "\"apc_last_good_plan_id\"", "\"apc_observed_at\"",
            "\"force_last_good_plan_actual_state\"", "\"enablement_observed_at\"",
        })
        {
            Assert.Contains(field, json, StringComparison.Ordinal);
        }

        /* The names are the contract. */
        Assert.Equal("apc_owns_it", ForcePlanBlockerNames.ApcOwnsIt);
        Assert.Equal("already_forced", ForcePlanBlockerNames.AlreadyForced);
        Assert.Equal("forcing_failed_on_this_plan", ForcePlanBlockerNames.ForcingFailedOnThisPlan);
        Assert.Equal("apc_withdrew_it", ForcePlanBlockerNames.ApcWithdrewIt);
        Assert.Equal("apc_resolved_differently", ForcePlanBlockerNames.ApcResolvedDifferently);
        Assert.Equal("parameter_sensitivity_cofired", ForcePlanBlockerNames.ParameterSensitivityCoFired);
        Assert.Equal("secondary_replica_evidence", ForcePlanBlockerNames.SecondaryReplicaEvidence);
    }

    [Fact]
    public void VerifySql_SpeaksTheBlockersVocabulary()
    {
        /* "verify after forcing" reads the same facts "eligible before forcing" read: the forcing type
           apc_owns_it keys on, the failure counter, and the engine's own recommendation state. */
        var sql = Project(Target(), null).VerifySql;
        Assert.Contains("plan_forcing_type = qsp.plan_forcing_type_desc", sql, StringComparison.Ordinal);
        Assert.Contains("qsp.force_failure_count", sql, StringComparison.Ordinal);
        Assert.Contains("apc_state = JSON_VALUE(dtr.state, '$.currentValue')", sql, StringComparison.Ordinal);
        Assert.Contains("apc_state_reason = JSON_VALUE(dtr.state, '$.reason')", sql, StringComparison.Ordinal);
        Assert.Contains("apc_last_good_plan_id = JSON_VALUE(dtr.details, '$.planForceDetails.recommendedPlanId')", sql, StringComparison.Ordinal);
        Assert.Contains("FROM sys.dm_db_tuning_recommendations AS dtr", sql, StringComparison.Ordinal);
        /* The query id is compared as the bigint the collector shreds it to (PlanCorrectionCollector's
           TRY_CAST precedent), not as a string literal — review catch on #3655. */
        Assert.Contains("WHERE TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.queryId') AS bigint) = 123;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePlanForceBotsGate_IsTheWholeGate_PlusItsOwnTwo_AndTheOverloadsStillAgreeOnTheTargetHalf()
    {
        /* Until #3654 this pinned the GAP: PlanForceBot consulted the one-argument overload, which has
           no state and therefore none of the #3652 blockers. Now it pins the closure: the bot judges
           through ForcePlanBotPolicy.Blockers, which is the two-argument gate's list verbatim (same
           names, same evidence — what an agent reads is what the bot enforces) plus the two blockers
           only an unattended actor needs. On the live #3652 shape — APC verifying its own AUTO force on
           exactly the proposed plan — the one-argument overload still says nothing, and the bot's gate
           says apc_owns_it. */
        var t = Target();
        var apcVerifying = State(planForced: true, forcingType: "AUTO", failures: 0,
            apcState: "Verifying", apcLastGood: 99, apcLastGoodForcingType: "AUTO", apcLastGoodForced: true, flgp: "OFF");

        Assert.Empty(FactRemediation.ForcePlanBlockers(t));
        var shared = FactRemediation.ForcePlanBlockers(t, apcVerifying);
        var bot = ForcePlanBotPolicy.Blockers(t, apcVerifying, stateUnavailableReason: null);
        Assert.Equal(new[] { ForcePlanBlockerNames.ApcOwnsIt }, shared.Select(b => b.Name));
        Assert.Equal(shared, bot);

        /* The bot's own two ride AFTER the shared gate's and never appear in the shared vocabulary:
           FLGP on for the database stands the bot down where the advice only changes its verb, and an
           unreadable state is a blocker for the bot where the advice writes a note. */
        var flgpOn = apcVerifying with { ForceLastGoodPlanActualState = "ON" };
        Assert.Equal(
            new[] { ForcePlanBlockerNames.ApcOwnsIt, ForcePlanBotPolicy.ReasonApcEnabledForDatabase },
            ForcePlanBotPolicy.Blockers(t, flgpOn, null).Select(b => b.Name));
        Assert.Equal("on", Project(t, flgpOn).ApcMode);
        Assert.Equal(
            new[] { ForcePlanBotPolicy.ReasonStateUnavailable },
            ForcePlanBotPolicy.Blockers(t, null, "read failed").Select(b => b.Name));
        Assert.StartsWith("unknown: read failed", Project(t, null, "read failed").StateNote, StringComparison.Ordinal);

        /* The two overloads still agree on the target-carried half. */
        var carried = Target(psp: true, replica: "Secondary");
        Assert.Equal(
            FactRemediation.ForcePlanBlockers(carried),
            FactRemediation.ForcePlanBlockers(carried, null).Select(b => b.Name).ToList());
    }

    /* ------------------------------------------------------------------ 5. the reader SQL, both SKUs */

    [Fact]
    public void StateReaderSql_IsTheDarlingText_ButForTheViewNamesAndTheCasts()
    {
        var lite = ForcePlanTargetStateReader.SqlTemplate;
        var darling = ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Analysis/DarlingForcePlanTargetStateReader.cs");
        const string marker = "public const string SqlTemplate = @\"";
        var darlingSql = darling[(darling.IndexOf(marker, StringComparison.Ordinal) + marker.Length)..];
        darlingSql = darlingSql[..darlingSql.IndexOf("\";", StringComparison.Ordinal)];

        Assert.Contains("JOIN v_query_store_stats AS qs", lite, StringComparison.Ordinal);
        Assert.Contains("JOIN query_store_stats AS qs", darlingSql, StringComparison.Ordinal);
        Assert.Equal(
            darlingSql.ReplaceLineEndings("\n"),
            lite.Replace("JOIN v_query_store_stats AS qs", "JOIN query_store_stats AS qs", StringComparison.Ordinal)
                .Replace("JOIN v_plan_correction AS p", "JOIN plan_correction AS p", StringComparison.Ordinal)
                .Replace("FROM v_plan_correction AS e", "FROM plan_correction AS e", StringComparison.Ordinal)
                .ReplaceLineEndings("\n"));

        /* The VALUES rows: Postgres needs the casts, DuckDB does not; the ordinals are the same. */
        Assert.Equal("($3, $4, $5)", ForcePlanTargetStateReader.ValuesRow(0));
        Assert.Equal("($6, $7, $8)", ForcePlanTargetStateReader.ValuesRow(1));
        /* The Darling row is built by interpolation, so the source reads `${3 + index * 3}::text` — pin the
           cast spellings as written rather than a rendered row the source never contains. */
        Assert.Contains("}::text, ${", darling, StringComparison.Ordinal);
        Assert.Contains("}::bigint, ${", darling, StringComparison.Ordinal);
        Assert.Contains("}::bigint)\"", darling, StringComparison.Ordinal);
        Assert.Contains("VALUES ($3, $4, $5), ($6, $7, $8)", ForcePlanTargetStateReader.BuildSql(2), StringComparison.Ordinal);

        /* All three scans (plan, recommendation, enablement) are bounded by the bound $2, never now(). */
        Assert.DoesNotContain("now()", lite, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(lite, @"collection_time > \$2").Count);
    }

    /* ------------------------------------------------------------------ 6. the DuckDB round trip */

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private async Task SeedQueryStoreAsync(DateTime at, long queryId, long planId, bool forced, string forcingType, long failures, string? reason)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        /* Two interval rows per collection — the forcing columns repeat and the read's MAX has to collapse them. */
        for (var interval = 0; interval < 2; interval++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     query_plan_hash, is_forced_plan, force_failure_count, plan_forcing_type, last_force_failure_reason,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
            cmd.Parameters.Add(new DuckDBParameter { Value = _seedServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "Srv" });
            cmd.Parameters.Add(new DuckDBParameter { Value = Db });
            cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
            cmd.Parameters.Add(new DuckDBParameter { Value = planId });
            cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
            cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at.AddMinutes(-10 - interval)) });
            cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
            cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 1" });
            cmd.Parameters.Add(new DuckDBParameter { Value = "0xQ" });
            cmd.Parameters.Add(new DuckDBParameter { Value = 100L + interval });
            cmd.Parameters.Add(new DuckDBParameter { Value = 500L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 900L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 40L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
            cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
            cmd.Parameters.Add(new DuckDBParameter { Value = "0xP" + planId });
            cmd.Parameters.Add(new DuckDBParameter { Value = forced });
            cmd.Parameters.Add(new DuckDBParameter { Value = failures });
            cmd.Parameters.Add(new DuckDBParameter { Value = forcingType });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)reason ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = 1000L + interval });
            cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at.AddMinutes(-10 - interval)) });
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task SeedPlanCorrectionAsync(DateTime at, string database, string flgp, long? queryId, string? state, string? reason, long? lastGood, string? lastGoodForcingType, bool? lastGoodForced, string? lastGoodFailure)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO plan_correction
    (collection_id, collection_time, server_id, server_name, database_name,
     force_last_good_plan_desired_state, force_last_good_plan_actual_state,
     recommendation_name, recommendation_state, recommendation_state_reason, score,
     query_id, regressed_plan_id, last_good_plan_id, last_good_plan_forcing_type, last_good_plan_is_forced,
     last_good_plan_force_failure_reason, execute_action_initiated_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _seedServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Srv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = flgp });
        cmd.Parameters.Add(new DuckDBParameter { Value = flgp });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)(queryId is null ? null : $"PR_{queryId}") ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)state ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)reason ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)(queryId is null ? null : 50) ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)queryId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)(queryId is null ? null : 7L) ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)lastGood ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)lastGoodForcingType ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)lastGoodForced ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)lastGoodFailure ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)(state is null ? null : "System") ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Reader_ReturnsEveryRequestedTarget_FoldedFromBothCollectors_NewestRowWins()
    {
        var now = DateTime.UtcNow;
        var t1 = now.AddMinutes(-40);
        var t2 = now.AddMinutes(-20);

        /* Query 123: plan 99 (the target) forced AUTO in the newest collection, unforced in the older
           one — the newest must win. Plan 7 (the regressed plan) present, never forced. */
        await SeedQueryStoreAsync(t1, 123, 99, forced: false, "NONE", 0, "NONE");
        await SeedQueryStoreAsync(t2, 123, 99, forced: true, "AUTO", 0, "NONE");
        await SeedQueryStoreAsync(t2, 123, 7, forced: false, "NONE", 0, "NONE");
        /* Query 200: plan 5 is the target; plan 6 is forced by hand. */
        await SeedQueryStoreAsync(t2, 200, 6, forced: true, "MANUAL", 2, "GENERAL_FAILURE");
        /* Query 300: nothing at all inside the lookback (a row from two days ago does not count). */
        await SeedQueryStoreAsync(now.AddDays(-2), 300, 1, forced: true, "MANUAL", 0, "NONE");

        /* plan_correction: an older Active row and a newer Verifying row for 123 — newest wins; the
           enablement half comes off the newest capture (t2), which says ON for Orders and OFF for Other. */
        await SeedPlanCorrectionAsync(t1, Db, "ON", 123, "Active", "AutomaticTuningOptionNotEnabled", 99, null, null, null);
        await SeedPlanCorrectionAsync(t2, Db, "ON", 123, "Verifying", null, 99, "AUTO", true, null);
        await SeedPlanCorrectionAsync(t2, "Other", "OFF", null, null, null, null, null, null, null);

        var targets = new List<ForcePlanTarget>
        {
            new(Db, 123, 99), new(Db, 200, 5), new(Db, 300, 1), new("Other", 400, 2),
            /* the same key twice (two replicas, #1882) — one row back */
            new(Db, 123, 99, ReplicaRole: "Secondary"),
        };

        var states = await ReadStatesAsync(targets);

        Assert.Equal(4, states.Count);

        var owned = states[new ForcePlanTargetKey("orders", 123, 99)]; /* case-insensitive key */
        Assert.True(owned.PlanIsForced);
        Assert.Equal("AUTO", owned.PlanForcingType);
        Assert.Equal(0, owned.ForceFailureCount);
        Assert.Equal("NONE", owned.LastForceFailureReason);
        Assert.Equal(DateTimeKind.Utc, owned.PlanObservedAtUtc!.Value.Kind);
        Assert.Equal(Naive(t2).Ticks / TimeSpan.TicksPerSecond, owned.PlanObservedAtUtc.Value.Ticks / TimeSpan.TicksPerSecond);
        Assert.Null(owned.OtherForcedPlanId);
        Assert.Equal("Verifying", owned.ApcState);
        Assert.Null(owned.ApcStateReason);
        Assert.Equal(99, owned.ApcLastGoodPlanId);
        Assert.Equal("AUTO", owned.ApcLastGoodPlanForcingType);
        Assert.True(owned.ApcLastGoodPlanIsForced);
        Assert.Equal("System", owned.ApcExecuteActionInitiatedBy);
        Assert.Equal("ON", owned.ForceLastGoodPlanActualState);
        Assert.True(owned.ApcIsOn);

        var conflict = states[new ForcePlanTargetKey(Db, 200, 5)];
        Assert.Null(conflict.PlanIsForced);            /* no row for plan 5: not observed, not "false" */
        Assert.Equal(6, conflict.OtherForcedPlanId);
        Assert.Equal("MANUAL", conflict.OtherForcedPlanForcingType);
        Assert.Null(conflict.ApcState);
        Assert.Equal("ON", conflict.ForceLastGoodPlanActualState);

        var stale = states[new ForcePlanTargetKey(Db, 300, 1)];
        Assert.Null(stale.PlanIsForced);
        Assert.Null(stale.OtherForcedPlanId);
        Assert.False(stale.IsEmpty);                   /* the enablement half still speaks for its database */
        Assert.Equal("ON", stale.ForceLastGoodPlanActualState);

        var other = states[new ForcePlanTargetKey("Other", 400, 2)];
        Assert.Equal("OFF", other.ForceLastGoodPlanActualState);
        Assert.False(other.ApcIsOn);

        /* End to end through the projection: the owned target is blocked, the conflict is named, the
           stale one is eligible-with-a-verb-change and no false blocker. */
        var action = new RemediationAction("PLAN_REGRESSION", "force", targets.Take(3).ToList());
        var projected = FactRemediation.BuildStructuredRemediation(action, states)!.ForcePlanTargets;
        Assert.Equal(new[] { "apc_owns_it" }, projected[0].Blockers);
        Assert.Equal(new[] { "already_forced" }, projected[1].Blockers);
        Assert.True(projected[2].Eligible);
        Assert.Equal("on", projected[2].ApcMode);
        Assert.Null(projected[2].StateNote);
    }

    /* The product read: the analysis service's own handle, which the MCP analysis tools use (the force-plan state has no
       LocalDataService entry point). A failed read comes back as a reason, so an unavailable state fails the test. */
    private async Task<IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>> ReadStatesAsync(IReadOnlyList<ForcePlanTarget> targets)
    {
        var finding = new AnalysisFinding { Remediation = new RemediationAction("PLAN_REGRESSION", "force", targets) };
        var (states, reason) = await new PerformanceMonitorLite.Analysis.AnalysisService(_duckDb)
            .TryGetForcePlanTargetStatesAsync(ServerId, [finding]);
        Assert.Null(reason);
        return states!;
    }

    [Fact]
    public async Task Reader_CarriesTheRegressedPlanId_AndItReachesTheGuidance()
    {
        /* #4736: a recommendation row names two plans, the regressed (worse) one and the last good one, and the
           guidance and the bot's blocker both compare the regressed id with the proposed plan. The seed names
           plan 7 as regressed and plan 99 as last good, so a reader that took the wrong column would hand back
           99, and one that dropped the column would hand back nothing. */
        await SeedPlanCorrectionAsync(DateTime.UtcNow.AddMinutes(-20), Db, "OFF", 123, "Active", null, 99, null, null, null);

        var worse = new ForcePlanTarget(Db, 123, 7);
        var noRecommendation = new ForcePlanTarget(Db, 124, 7);
        var states = await ReadStatesAsync(new[] { worse, noRecommendation });

        var state = states[ForcePlanTargetKey.Of(worse)];
        Assert.Equal("Active", state.ApcState);
        Assert.Equal(7L, state.ApcRegressedPlanId);
        Assert.Equal(99L, state.ApcLastGoodPlanId);
        Assert.Null(states[ForcePlanTargetKey.Of(noRecommendation)].ApcRegressedPlanId);

        var projected = Assert.Single(FactRemediation.BuildStructuredRemediation(Action(worse), states)!.ForcePlanTargets);
        Assert.Contains("names plan 7 (the plan proposed here) as the regressed, worse plan", projected.Guidance, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Reader_TakesEachDatabasesNewestEnablementRowInTheLookback_NotOnlyTheServersNewestCapture()
    {
        /* #4770: a capture lands one database at a time, so the server's newest capture can lack a database
           that an earlier capture (3 hours ago) had. That database keeps its earlier state; one with no row
           inside the 24-hour lookback comes back with no enablement at all (unknown). */
        var now = DateTime.UtcNow;
        await SeedPlanCorrectionAsync(now.AddHours(-3), Db, "ON", null, null, null, null, null, null, null);
        await SeedPlanCorrectionAsync(now.AddMinutes(-10), "Other", "OFF", null, null, null, null, null, null, null);
        await SeedPlanCorrectionAsync(now.AddHours(-30), "Old", "ON", null, null, null, null, null, null, null);

        var states = await ReadStatesAsync(
            new List<ForcePlanTarget> { new(Db, 1, 1), new("Other", 2, 2), new("Old", 3, 3) });

        var earlier = states[new ForcePlanTargetKey(Db, 1, 1)];
        Assert.Equal("ON", earlier.ForceLastGoodPlanActualState);
        Assert.NotNull(earlier.EnablementObservedAtUtc);
        Assert.Equal("OFF", states[new ForcePlanTargetKey("Other", 2, 2)].ForceLastGoodPlanActualState);
        var none = states[new ForcePlanTargetKey("Old", 3, 3)];
        Assert.Null(none.ForceLastGoodPlanActualState);
        Assert.Null(none.EnablementObservedAtUtc);
    }

    [Fact]
    public async Task Reader_OnAnEmptyStore_ReturnsAnEmptyStateForEachTarget_SoTheProjectionSaysUnknown()
    {
        var t = Target();
        var states = await ReadStatesAsync(new[] { t });
        var state = Assert.Single(states).Value;
        Assert.True(state.IsEmpty);

        var projected = Assert.Single(FactRemediation.BuildStructuredRemediation(Action(t), states)!.ForcePlanTargets);
        Assert.True(projected.Eligible);
        Assert.Contains("within the last 24 hours", projected.StateNote, StringComparison.Ordinal);
    }

    /* ------------------------------------------------------------------ 7. the copy-paste text follows the verdict (#8542) */

    /// <summary>The lines of a remediation text that SQL Server would run: everything that is not a comment.</summary>
    private static List<string> RunnableLines(string? sql) =>
        (sql ?? string.Empty).Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && !line.StartsWith("--", StringComparison.Ordinal))
            .ToList();

    private static bool Forces(IEnumerable<string> runnableLines, long queryId, long planId) =>
        runnableLines.Any(line =>
            line.Contains("sp_query_store_force_plan", StringComparison.Ordinal) &&
            line.Contains($"@query_id = {queryId},", StringComparison.Ordinal) &&
            line.Contains($"@plan_id = {planId};", StringComparison.Ordinal));

    /// <summary>
    /// #8542, through the tool and the store: one persisted PLAN_REGRESSION finding with three targets.
    /// The store says automatic plan correction withdrew its recommendation for the second target's plan
    /// and that forcing the third target's plan is failing. The reply's own structured_remediation marks
    /// those two ineligible, so its remediation_command gives no runnable force statement for them and
    /// names them with their blockers in comments. A fourth target is parameter-sensitive and nothing
    /// in the store is against it: the reply marks it ineligible too, and it is the one exception, its
    /// statement staying under a caution (#2138).
    /// </summary>
    [Fact]
    public async Task GetAnalysisFindings_RemediationCommand_WithholdsTheStatementTheStoreBlocks_AndKeepsItForAParameterSensitiveTarget()
    {
        var tempDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ForcePlanEligibility_" + Guid.NewGuid().ToString("N")[..8]);
        System.IO.Directory.CreateDirectory(tempDir);
        try
        {
            var serverManager = new ServerManager(tempDir);
            var server = new PerformanceMonitorLite.Models.ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
            serverManager.AddServer(server);
            _seedServerId = RemoteCollectorService.GetDeterministicHashCode(
                RemoteCollectorService.GetServerNameForStorage(server));

            var now = DateTime.UtcNow;
            var seen = now.AddMinutes(-20);

            /* Query 201 / plan 11: collected, not forced, never failed. Eligible. */
            await SeedQueryStoreAsync(seen, 201, 11, forced: false, "NONE", 0, "NONE");
            /* Query 202 / plan 12: the engine recommended this plan and withdrew the recommendation. */
            await SeedPlanCorrectionAsync(seen, Db, "OFF", 202, "Expired", "TempTableChanged", 12, null, false, null);
            /* Query 203 / plan 13: forcing this plan is already failing. */
            await SeedQueryStoreAsync(seen, 203, 13, forced: false, "NONE", 2, "GENERAL_FAILURE");
            /* Query 204 / plan 14: collected, not forced, never failed, and parameter-sensitive. */
            await SeedQueryStoreAsync(seen, 204, 14, forced: false, "NONE", 0, "NONE");

            var finding = new AnalysisFinding
            {
                FindingId = 854201,
                AnalysisTime = now,
                ServerId = _seedServerId,
                ServerName = "TestServer",
                TimeRangeStart = now.AddHours(-4),
                TimeRangeEnd = now,
                Severity = 1.6,
                Confidence = 0.9,
                Category = "queries",
                StoryPath = "PLAN_REGRESSION",
                StoryPathHash = "force_eligibility_hash",
                StoryText = "planted finding",
                RootFactKey = "PLAN_REGRESSION",
                RootFactValue = 1.0,
                FactCount = 1,
                IncidentId = "force-eligibility-test",
                Remediation = Action(Target(201, 11), Target(202, 12), Target(203, 13), Target(204, 14, psp: true))
            };
            var context = new AnalysisContext
            {
                ServerId = _seedServerId,
                ServerName = "TestServer",
                TimeRangeStart = now.AddHours(-4),
                TimeRangeEnd = now
            };
            await new PerformanceMonitorLite.Analysis.FindingStore(_duckDb)
                .InsertFindingsAsync(new List<AnalysisFinding> { finding }, context);

            var json = await PerformanceMonitorLite.Mcp.McpAnalysisTools.GetAnalysisFindings(
                new PerformanceMonitorLite.Analysis.AnalysisService(_duckDb), serverManager, "TestServer", 24);

            using var doc = JsonDocument.Parse(json);
            var reply = Assert.Single(doc.RootElement.GetProperty("findings").EnumerateArray());
            var targets = reply.GetProperty("structured_remediation").GetProperty("force_plan_targets").EnumerateArray().ToList();
            Assert.Equal(4, targets.Count);

            /* The fixture produced the verdicts the issue names, each from the store's own rows. */
            var verdicts = targets.ToDictionary(
                t => t.GetProperty("query_id").GetInt64(),
                t => (Eligible: t.GetProperty("eligible").GetBoolean(),
                      PlanId: t.GetProperty("plan_id").GetInt64(),
                      Blockers: t.GetProperty("blockers").EnumerateArray().Select(b => b.GetString()!).ToArray()));
            Assert.True(verdicts[201].Eligible);
            Assert.Equal(new[] { ForcePlanBlockerNames.ApcWithdrewIt }, verdicts[202].Blockers);
            Assert.Equal(new[] { ForcePlanBlockerNames.ForcingFailedOnThisPlan }, verdicts[203].Blockers);
            Assert.False(verdicts[204].Eligible);
            Assert.Equal(new[] { ForcePlanBlockerNames.ParameterSensitivityCoFired }, verdicts[204].Blockers);

            var command = reply.GetProperty("remediation_command").GetString();
            var runnable = RunnableLines(command);

            /* The rule, target by target: a statement is given when the verdict is eligible, and withheld when
               the store blocks the target. The one exception is a target whose only blockers are the two it
               carries from its own finding (parameter sensitivity, secondary replica): the reply marks it
               ineligible, and its statement stays, under its caution or disclosure. */
            foreach (var (queryId, verdict) in verdicts)
            {
                var ownEvidenceOnly = !verdict.Eligible && verdict.Blockers.All(b =>
                    b is ForcePlanBlockerNames.ParameterSensitivityCoFired or ForcePlanBlockerNames.SecondaryReplicaEvidence);
                var statementExpected = verdict.Eligible || ownEvidenceOnly;
                Assert.True(
                    statementExpected == Forces(runnable, queryId, verdict.PlanId),
                    $"query {queryId} / plan {verdict.PlanId} is eligible: {verdict.Eligible} with blockers [{string.Join(", ", verdict.Blockers)}], but remediation_command " +
                    $"{(Forces(runnable, queryId, verdict.PlanId) ? "gives" : "does not give")} a runnable force statement for it");
            }

            /* The exception, by name: ineligible in the verdict, and still handed over. */
            Assert.True(Forces(runnable, 204, 14), "the parameter-sensitive target is ineligible and keeps its statement under its caution");

            /* The ineligible targets are named with their blockers, in comments. */
            var comments = (command ?? string.Empty).Split('\n')
                .Select(line => line.Trim())
                .Where(line => line.StartsWith("--", StringComparison.Ordinal))
                .ToList();
            Assert.Contains(comments, line => line.Contains("query_id = 202", StringComparison.Ordinal) && line.Contains("plan_id = 12", StringComparison.Ordinal));
            Assert.Contains(comments, line => line.Contains(ForcePlanBlockerNames.ApcWithdrewIt, StringComparison.Ordinal) && line.Contains("TempTableChanged", StringComparison.Ordinal));
            Assert.Contains(comments, line => line.Contains("query_id = 203", StringComparison.Ordinal) && line.Contains("plan_id = 13", StringComparison.Ordinal));
            Assert.Contains(comments, line => line.Contains(ForcePlanBlockerNames.ForcingFailedOnThisPlan, StringComparison.Ordinal));
        }
        finally
        {
            _seedServerId = ServerId;
            try { System.IO.Directory.Delete(tempDir, recursive: true); }
            catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>
    /// #8542, through the Recommendations cards' own read: the same three targets and the same store rows
    /// as the tool test above. The card's copy-paste command gives the statement for the target the
    /// store holds nothing against, and names the two the store blocks with their blockers.
    /// </summary>
    [Fact]
    public async Task RecommendationCard_WithholdsTheStatement_ForATargetTheStoreBlocks()
    {
        _seedServerId = 854203;
        try
        {
            var now = DateTime.UtcNow;
            var seen = now.AddMinutes(-20);
            await SeedQueryStoreAsync(seen, 201, 11, forced: false, "NONE", 0, "NONE");
            await SeedPlanCorrectionAsync(seen, Db, "OFF", 202, "Expired", "TempTableChanged", 12, null, false, null);
            await SeedQueryStoreAsync(seen, 203, 13, forced: false, "NONE", 2, "GENERAL_FAILURE");

            var finding = new AnalysisFinding
            {
                FindingId = 854203,
                AnalysisTime = now,
                ServerId = _seedServerId,
                ServerName = "TestServer",
                TimeRangeStart = now.AddHours(-4),
                TimeRangeEnd = now,
                Severity = 1.6,
                Confidence = 0.9,
                Category = "queries",
                StoryPath = "PLAN_REGRESSION",
                StoryPathHash = "force_card_hash",
                StoryText = "planted finding",
                RootFactKey = "PLAN_REGRESSION",
                RootFactValue = 1.0,
                FactCount = 1,
                IncidentId = "force-card-test",
                Remediation = Action(Target(201, 11), Target(202, 12), Target(203, 13))
            };
            var context = new AnalysisContext
            {
                ServerId = _seedServerId,
                ServerName = "TestServer",
                TimeRangeStart = now.AddHours(-4),
                TimeRangeEnd = now
            };
            var findingStore = new PerformanceMonitorLite.Analysis.FindingStore(_duckDb);
            await findingStore.InsertFindingsAsync(new List<AnalysisFinding> { finding }, context);

            var cards = await new PerformanceMonitorLite.Analysis.Recommendations.LiteRecommendationsReader(findingStore)
                .GetRecommendationsAsync(_seedServerId, "TestServer");

            var card = Assert.Single(cards);
            var runnable = RunnableLines(card.CopyPasteSql);
            Assert.True(Forces(runnable, 201, 11), "the target the store holds nothing against keeps its statement");
            Assert.False(Forces(runnable, 202, 12), "plan 12 is apc_withdrew_it in the store, and the card must not give a runnable force statement for it");
            Assert.False(Forces(runnable, 203, 13), "plan 13 is forcing_failed_on_this_plan in the store, and the card must not give a runnable force statement for it");
            Assert.Contains(ForcePlanBlockerNames.ApcWithdrewIt, card.CopyPasteSql, StringComparison.Ordinal);
            Assert.Contains(ForcePlanBlockerNames.ForcingFailedOnThisPlan, card.CopyPasteSql, StringComparison.Ordinal);
        }
        finally
        {
            _seedServerId = ServerId;
        }
    }

    /// <summary>
    /// #8542: only the store's forcing state withholds a statement. A parameter-sensitive target and a
    /// target measured on a secondary carry their caution and disclosure WITH the statement (#1882,
    /// #2138), with no state and with a state that holds nothing against them.
    /// </summary>
    [Fact]
    public void CopyPasteCommand_KeepsTheStatement_ForATargetOnlyItsOwnEvidenceCautions()
    {
        var clean = Target(201, 11);
        var psp = Target(202, 12, psp: true);
        var secondary = Target(203, 13, replica: "Secondary");
        var action = Action(clean, psp, secondary);
        var quiet = new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer)
        {
            [ForcePlanTargetKey.Of(clean)] = State(planForced: false, forcingType: "NONE", failures: 0),
            [ForcePlanTargetKey.Of(psp)] = State(planForced: false, forcingType: "NONE", failures: 0),
            [ForcePlanTargetKey.Of(secondary)] = State(planForced: false, forcingType: "NONE", failures: 0),
        };

        foreach (var sql in new[] { FactRemediation.RenderCopyPasteCommand(action), FactRemediation.RenderCopyPasteCommand(action, quiet) })
        {
            var runnable = RunnableLines(sql);
            Assert.True(Forces(runnable, 201, 11), "the clean target keeps its statement");
            Assert.True(Forces(runnable, 202, 12), "the parameter-sensitive target keeps its statement under its caution");
            Assert.True(Forces(runnable, 203, 13), "the secondary-replica target keeps its statement under its disclosure");
        }
    }

    /// <summary>
    /// #8542: a parameter-sensitive target the store ALSO blocks gets no statement, and the comment names
    /// every blocker of the gate, the store's and the target's own.
    /// </summary>
    [Fact]
    public void CopyPasteCommand_WithholdsTheStatement_WhenTheStoreBlocksAParameterSensitiveTarget()
    {
        var psp = Target(202, 12, psp: true);
        var sql = FactRemediation.RenderCopyPasteCommand(
            Action(Target(201, 11), psp),
            For(psp, State(apcState: "Expired", apcReason: "TempTableChanged", apcLastGood: 12)));

        var runnable = RunnableLines(sql);
        Assert.True(Forces(runnable, 201, 11));
        Assert.False(Forces(runnable, 202, 12));
        Assert.Contains(ForcePlanBlockerNames.ApcWithdrewIt, sql, StringComparison.Ordinal);
        Assert.Contains(ForcePlanBlockerNames.ParameterSensitivityCoFired, sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #8542: when the store blocks every target the command is still text (the targets and their blockers,
    /// in comments), it says no statement is offered, and it holds nothing SQL Server would run.
    /// </summary>
    [Fact]
    public void CopyPasteCommand_WhenTheStoreBlocksEveryTarget_SaysSo_AndHoldsNoStatement()
    {
        var withdrawn = Target(202, 12);
        var failing = Target(203, 13);
        var states = new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer)
        {
            [ForcePlanTargetKey.Of(withdrawn)] = State(apcState: "Expired", apcReason: "TempTableChanged", apcLastGood: 12),
            [ForcePlanTargetKey.Of(failing)] = State(failures: 2, failureReason: "GENERAL_FAILURE"),
        };

        var sql = FactRemediation.RenderCopyPasteCommand(Action(withdrawn, failing), states);

        Assert.False(string.IsNullOrWhiteSpace(sql));
        Assert.Empty(RunnableLines(sql));
        Assert.Contains("query_id = 202", sql, StringComparison.Ordinal);
        Assert.Contains("query_id = 203", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #8542: a database name, a replica role, a plan hash and a blocker's evidence are all text from the
    /// monitored server. A line break in any of them stays inside the comment it is written in, so a
    /// blocked target's text holds nothing SQL Server would run.
    /// </summary>
    [Fact]
    public void ALineBreakInCollectedText_StaysInsideTheComment_ForABlockedTarget()
    {
        const string injected = "\nEXEC sys.sp_query_store_force_plan @query_id = 7, @plan_id = 9;";
        var database = "Sales" + injected;
        var target = new ForcePlanTarget(database, 202, 12, "0xBEST" + injected, "0xLATEST" + injected, 9000, 1200, 7.5, "Secondary" + injected);
        var states = For(target, State(apcState: "Expired", apcReason: "TempTableChanged" + injected, apcLastGood: 12));

        Assert.Empty(RunnableLines(FactRemediation.RenderCopyPasteCommand(Action(target), states)));

        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            DrillDown = new Dictionary<string, object>
            {
                ["regressed_queries"] = new[]
                {
                    new Dictionary<string, object>
                    {
                        ["database"] = database,
                        ["query_id"] = 202L,
                        ["best_plan_id"] = 12L,
                        ["regression_factor"] = 7.5,
                        ["replica_role"] = "Secondary" + injected,
                        ["best_plan_hash"] = "0xBEST" + injected,
                        ["latest_plan_hash"] = "0xLATEST" + injected
                    }
                }
            }
        };

        Assert.Empty(RunnableLines(FactRemediation.GenerateForFinding(finding, states)));
    }

    /// <summary>
    /// #8542: a finding with no persisted action renders its statements from its drill-down, so the state
    /// read takes that finding's targets from the drill-down too and the store's verdict reaches them.
    /// </summary>
    [Fact]
    public async Task StateRead_CoversAFindingWithNoPersistedAction_ThroughItsDrillDown()
    {
        await SeedPlanCorrectionAsync(DateTime.UtcNow.AddMinutes(-20), Db, "OFF", 202, "Expired", "TempTableChanged", 12, null, false, null);

        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            DrillDown = new Dictionary<string, object>
            {
                ["regressed_queries"] = new[]
                {
                    new Dictionary<string, object>
                    {
                        ["database"] = Db,
                        ["query_id"] = 202L,
                        ["best_plan_id"] = 12L,
                        ["regression_factor"] = 7.5
                    }
                }
            }
        };

        var (states, reason) = await new PerformanceMonitorLite.Analysis.AnalysisService(_duckDb)
            .TryGetForcePlanTargetStatesAsync(ServerId, [finding]);

        Assert.Null(reason);
        Assert.True(states!.TryGetValue(new ForcePlanTargetKey(Db, 202, 12), out var state), "the drill-down's target was not read");
        Assert.False(Forces(RunnableLines(FactRemediation.GenerateForFinding(finding, states)), 202, 12));
        Assert.Equal("Expired", state!.ApcState);
    }

    private static AnalysisFinding DrillDownFinding(params (long QueryId, long PlanId)[] rows) => new()
    {
        RootFactKey = "PLAN_REGRESSION",
        DrillDown = new Dictionary<string, object>
        {
            ["regressed_queries"] = rows.Select(row => new Dictionary<string, object>
            {
                ["database"] = Db,
                ["query_id"] = row.QueryId,
                ["best_plan_id"] = row.PlanId,
                ["regression_factor"] = 7.5
            }).ToArray()
        }
    };

    /// <summary>
    /// #8542: a failed state read withholds nothing and says so. Every statement stays, and the text opens
    /// with the renderer's not-checked line, in the command, the preview and the Lite card. With no state
    /// and no reason the text carries no such line.
    /// </summary>
    [Fact]
    public void AFailedStateRead_KeepsEveryStatement_AndTheTextOpensBySayingNothingWasChecked()
    {
        const string reason = "the forcing and automatic-plan-correction state read failed (TimeoutException)";
        var action = Action(Target(201, 11), Target(202, 12));

        var command = FactRemediation.RenderCopyPasteCommand(action, states: null, stateUnavailableReason: reason);
        Assert.StartsWith(FactRemediation.StateNotReadPrefix, command, StringComparison.Ordinal);
        Assert.True(Forces(RunnableLines(command), 201, 11));
        Assert.True(Forces(RunnableLines(command), 202, 12));
        Assert.DoesNotContain(FactRemediation.StateNotReadPrefix, FactRemediation.RenderCopyPasteCommand(action, states: null, stateUnavailableReason: null), StringComparison.Ordinal);

        var finding = DrillDownFinding((201, 11), (202, 12));
        var preview = FactRemediation.GenerateForFinding(finding, states: null, stateUnavailableReason: reason);
        Assert.StartsWith(FactRemediation.StateNotReadPrefix, preview, StringComparison.Ordinal);
        Assert.True(Forces(RunnableLines(preview), 201, 11));
        Assert.DoesNotContain(FactRemediation.StateNotReadPrefix, FactRemediation.GenerateForFinding(finding, states: null, stateUnavailableReason: null), StringComparison.Ordinal);

        var card = PerformanceMonitorLite.Analysis.Recommendations.LiteRecommendationsReader.MapFinding(
            new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", Remediation = action }, "TestServer",
            forcePlanStates: null, forcePlanStateUnavailableReason: reason);
        Assert.StartsWith(FactRemediation.StateNotReadPrefix, card.CopyPasteSql, StringComparison.Ordinal);
        Assert.True(Forces(RunnableLines(card.CopyPasteSql), 202, 12));

        /* A text whose every target has a state row never opens with the line, whatever the rows hold. */
        var readStates = For(Target(202, 12), State(failures: 2, failureReason: "GENERAL_FAILURE"));
        readStates[ForcePlanTargetKey.Of(Target(201, 11))] = State(planForced: false, forcingType: "NONE", failures: 0);
        var read = FactRemediation.RenderCopyPasteCommand(action, readStates);
        Assert.False(read!.StartsWith(FactRemediation.StateNotReadPrefix, StringComparison.Ordinal));
    }

    /// <summary>
    /// #8542: a target the state read found no row for keeps its statement, and the line above that
    /// statement says its state is unknown and to check before forcing, as structured_remediation's
    /// state_note does. The reader's all-null row and a missing key read the same. A target whose state
    /// was read and holds nothing against it carries no such line.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ATargetTheStateReadFoundNoRowFor_KeepsItsStatement_UnderALineSayingItsStateIsUnknown(bool allNullRow)
    {
        var unknown = Target(201, 11);
        var quiet = Target(202, 12);
        var states = For(quiet, State(planForced: false, forcingType: "NONE", failures: 0));
        if (allNullRow)
            states[ForcePlanTargetKey.Of(unknown)] = Empty();

        var note = FactRemediation.BuildStructuredRemediation(Action(unknown, quiet), states)!.ForcePlanTargets[0].StateNote;
        Assert.StartsWith("unknown:", note, StringComparison.Ordinal);

        var texts = new[]
        {
            FactRemediation.RenderCopyPasteCommand(Action(unknown, quiet), states),
            FactRemediation.GenerateForFinding(DrillDownFinding((201, 11), (202, 12)), states),
        };
        foreach (var text in texts)
        {
            var lines = (text ?? string.Empty).Split('\n').Select(line => line.Trim()).ToList();
            Assert.True(Forces(RunnableLines(text), 201, 11), "the target with no state row keeps its statement");
            Assert.True(Forces(RunnableLines(text), 202, 12), "the target with a quiet state keeps its statement");

            var notChecked = lines.Where(line => line.StartsWith(FactRemediation.StateNotReadPrefix, StringComparison.Ordinal)).ToList();
            Assert.True(notChecked.Count == 1, $"one target has no state row, and {notChecked.Count} lines say a state is unknown in:{Environment.NewLine}{text}");
            Assert.Contains("unknown", notChecked[0], StringComparison.Ordinal);
            Assert.Contains("before forcing", notChecked[0], StringComparison.Ordinal);

            /* The line is the unknown target's: above its statement, which is the first of the two. */
            var at = lines.IndexOf(notChecked[0]);
            var unknownStatement = lines.FindIndex(line => Forces([line], 201, 11));
            var quietStatement = lines.FindIndex(line => Forces([line], 202, 12));
            Assert.True(at < unknownStatement && unknownStatement < quietStatement,
                $"the unknown-state line sits above the statement of the target it is about in:{Environment.NewLine}{text}");
        }
    }

    /// <summary>
    /// #8542: a state read that faults gives back the exception's type and never its message, because the
    /// reason goes into the command text a card shows and a notification sends. The store here is a file
    /// in a folder that does not exist, so the fault's message names that path.
    /// </summary>
    [Fact]
    public async Task AFailedStateRead_NamesTheExceptionsType_AndNeverItsMessage()
    {
        const string marker = "marker8542inthepath";
        var missing = System.IO.Path.Combine(System.IO.Path.GetTempPath(), marker + Guid.NewGuid().ToString("N")[..8], "nowhere", "store.duckdb");
        using var store = new DuckDbInitializer(missing);

        var (states, reason) = await PerformanceMonitorLite.Analysis.AnalysisService.ReadForcePlanTargetStatesAsync(
            store, ServerId, [new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", Remediation = Action(Target(201, 11)) }]);

        Assert.Null(states);
        Assert.False(string.IsNullOrWhiteSpace(reason), "a read over a store that cannot be opened fails, and says so");
        Assert.DoesNotContain(marker, reason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Exception", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// #8542: the notification's remediation T-SQL is built over the same state. The message gives the
    /// force statement for the target the store holds nothing against and none for the two it blocks,
    /// and names their blockers.
    /// </summary>
    [Fact]
    public void NotificationAdvice_GivesNoForceStatement_ForATargetTheStoreBlocks()
    {
        var finding = DrillDownFinding((201, 11), (202, 12), (203, 13));
        var states = new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer)
        {
            [new ForcePlanTargetKey(Db, 201, 11)] = State(planForced: false, forcingType: "NONE", failures: 0),
            [new ForcePlanTargetKey(Db, 202, 12)] = State(apcState: "Expired", apcReason: "TempTableChanged", apcLastGood: 12),
            [new ForcePlanTargetKey(Db, 203, 13)] = State(failures: 2, failureReason: "GENERAL_FAILURE"),
        };

        var context = PerformanceMonitor.Notifications.FindingMessageFormatter.BuildContext(finding, 1.5, states);
        var text = string.Join("\n", context.Details.Select(detail => detail.Body ?? string.Empty));
        var runnable = RunnableLines(text);

        Assert.True(Forces(runnable, 201, 11), "the target the store holds nothing against keeps its statement");
        Assert.False(Forces(runnable, 202, 12), "plan 12 is apc_withdrew_it in the store, and the notification must not carry a runnable force statement for it");
        Assert.False(Forces(runnable, 203, 13), "plan 13 is forcing_failed_on_this_plan in the store, and the notification must not carry a runnable force statement for it");
        Assert.Contains(ForcePlanBlockerNames.ApcWithdrewIt, text, StringComparison.Ordinal);
        Assert.Contains(ForcePlanBlockerNames.ForcingFailedOnThisPlan, text, StringComparison.Ordinal);

        /* A failed read reaches the message too. */
        var unread = PerformanceMonitor.Notifications.FindingMessageFormatter.BuildContext(finding, 1.5, null, "the read failed (TimeoutException)");
        Assert.Contains(unread.Details, detail => (detail.Body ?? string.Empty).StartsWith(FactRemediation.StateNotReadPrefix, StringComparison.Ordinal));
    }
}

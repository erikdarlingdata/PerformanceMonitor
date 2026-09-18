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
        var darling = ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingForcePlanTargetStateReader.cs");
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
                .Replace("FROM v_plan_correction WHERE server_id = $1", "FROM plan_correction WHERE server_id = $1", StringComparison.Ordinal)
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

        /* Both scans are bounded by the bound $2, never now(). */
        Assert.DoesNotContain("now()", lite, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(lite, @"collection_time > \$2").Count);
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
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
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
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
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

        var service = new LocalDataService(_duckDb);
        var states = await service.GetForcePlanTargetStatesAsync(ServerId, targets);

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

    [Fact]
    public async Task Reader_OnAnEmptyStore_ReturnsAnEmptyStateForEachTarget_SoTheProjectionSaysUnknown()
    {
        var service = new LocalDataService(_duckDb);
        var t = Target();
        var states = await service.GetForcePlanTargetStatesAsync(ServerId, new[] { t });
        var state = Assert.Single(states).Value;
        Assert.True(state.IsEmpty);

        var projected = Assert.Single(FactRemediation.BuildStructuredRemediation(Action(t), states)!.ForcePlanTargets);
        Assert.True(projected.Eligible);
        Assert.Contains("within the last 24 hours", projected.StateNote, StringComparison.Ordinal);
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// What the store already knows about ONE force-plan target at READ time (#3652): the forcing state of
/// the plan the advice proposes, whether the engine's own automatic plan correction (APC) has a live
/// recommendation on the query, and whether FORCE_LAST_GOOD_PLAN is switched on for the database. Two
/// collectors write these facts on every SQL Server target — <c>query_store_stats</c>
/// (QueryStoreCollector: <c>is_forced_plan</c>, <c>plan_forcing_type</c>, <c>force_failure_count</c>,
/// <c>last_force_failure_reason</c>) and <c>plan_correction</c> (PlanCorrectionCollector:
/// <c>recommendation_state</c>, <c>recommendation_state_reason</c>, <c>last_good_plan_id</c>,
/// <c>last_good_plan_forcing_type</c>, <c>last_good_plan_is_forced</c>,
/// <c>force_last_good_plan_actual_state</c>) — and until #3652 the eligibility verdict read neither. On one
/// production store a PLAN_REGRESSION incident listed five force targets, every one <c>eligible: true,
/// blockers: []</c>; the same server's <c>plan_correction</c> snapshot minutes later showed all five
/// contraindicated (two mid-verification under APC on exactly the proposed plan, one a known forcing
/// failure, one withdrawn by the engine, one already resolved through a different plan). This record is
/// the missing half of that verdict.
///
/// <para><b>Read at read time, never persisted.</b> A finding's targets were extracted when the analysis
/// ran; the forcing and APC state moves after that (APC completes a verification in minutes), so a state
/// frozen into the finding would be the stale reading the issue is about. Each half carries the
/// <c>collection_time</c> of the snapshot it came from so a reader can see HOW stale the evidence is —
/// a blocker built from a snapshot two hours old says so.</para>
///
/// <para><b>Nullable means "not observed", never "false".</b> <see cref="PlanIsForced"/> is null when the
/// target plan had no <c>query_store_stats</c> row inside the reader's lookback — which is the ordinary
/// case for a "best" plan that is not currently executing (the regressed plan is), and is NOT evidence
/// that it is unforced. The blockers below only fire on observed values; the projection reports the
/// absence as a note rather than letting it read as clean.</para>
///
/// <para>Serialized as the target's <c>forcing_state</c> so the raw facts behind every blocker are on the
/// wire beside the blocker names — an agent that disputes a verdict can see what it was built from.
/// snake_case by attribute for the same reason <see cref="StructuredRemediation"/> is.</para>
/// </summary>
public sealed record ForcePlanTargetState(
    /* ---- query_store_stats: the TARGET plan's newest row inside the lookback ---- */

    /// <summary><c>is_forced_plan</c> on the target plan's newest snapshot; null when no row was seen.</summary>
    [property: JsonPropertyName("plan_is_forced")] bool? PlanIsForced,

    /// <summary><c>plan_forcing_type</c> (<c>MANUAL</c> / <c>AUTO</c> / <c>NONE</c>) on that snapshot.
    /// AUTO is the automatic-plan-correction spelling and is what makes a forced plan APC's rather than
    /// an operator's.</summary>
    [property: JsonPropertyName("plan_forcing_type")] string? PlanForcingType,

    /// <summary><c>force_failure_count</c> on that snapshot — the same counter the Forced Plan Failing
    /// alert family (#2157, #3579) pages on.</summary>
    [property: JsonPropertyName("force_failure_count")] long? ForceFailureCount,

    /// <summary><c>last_force_failure_reason</c> on that snapshot; the collector stores the engine's
    /// <c>last_force_failure_reason_desc</c>, so <c>NONE</c> means no failure.</summary>
    [property: JsonPropertyName("last_force_failure_reason")] string? LastForceFailureReason,

    /// <summary>The <c>collection_time</c> of the target plan's snapshot (naive UTC as stored).</summary>
    [property: JsonPropertyName("plan_observed_at")] DateTime? PlanObservedAtUtc,

    /* ---- query_store_stats: a DIFFERENT plan of the same query that is forced ---- */

    /// <summary>The newest forced plan of this query inside the lookback when it is NOT the target plan;
    /// null when the target is the forced one or nothing is forced. SQL Server permits one forced plan per
    /// query, so a manual force of the target would silently replace this one.</summary>
    [property: JsonPropertyName("other_forced_plan_id")] long? OtherForcedPlanId,

    /// <summary>That other plan's <c>plan_forcing_type</c> — AUTO means APC placed it.</summary>
    [property: JsonPropertyName("other_forced_plan_forcing_type")] string? OtherForcedPlanForcingType,

    [property: JsonPropertyName("other_forced_plan_observed_at")] DateTime? OtherForcedPlanObservedAtUtc,

    /* ---- plan_correction: the newest RECOMMENDATION row for (database, query_id) ---- */

    /// <summary><c>recommendation_state</c> — the engine's <c>Active</c> / <c>Verifying</c> /
    /// <c>Success</c> / <c>Reverted</c> / <c>Expired</c> (sys.dm_db_tuning_recommendations
    /// <c>state.currentValue</c>). Null when the query has no recommendation row in the lookback.</summary>
    [property: JsonPropertyName("apc_state")] string? ApcState,

    /// <summary><c>recommendation_state_reason</c> — the engine's <c>state.reason</c>:
    /// <c>ForcingFailed</c>, <c>TempTableChanged</c>, <c>SchemaChanged</c>, <c>StatisticsChanged</c>,
    /// <c>VerificationAborted</c>, <c>VerificationForcedQueryRecompile</c>, <c>LastGoodPlanForced</c>,
    /// <c>PlanForcedByUser</c>, <c>PlanUnforcedByUser</c>, <c>UserForcedDifferentPlan</c>,
    /// <c>UnsupportedStatementType</c>, <c>AutomaticTuningOptionDisabled</c>,
    /// <c>AutomaticTuningOptionNotEnabled</c>. Quoted into the blocker evidence verbatim — the engine's
    /// vocabulary, not a paraphrase.</summary>
    [property: JsonPropertyName("apc_state_reason")] string? ApcStateReason,

    /// <summary>The plan the engine judged regressed (<c>planForceDetails.regressedPlanId</c>).</summary>
    [property: JsonPropertyName("apc_regressed_plan_id")] long? ApcRegressedPlanId,

    /// <summary>The plan the engine recommended or forced (<c>planForceDetails.recommendedPlanId</c>,
    /// stored as <c>last_good_plan_id</c>). Compared against the target's plan to tell "APC is doing this
    /// exact thing" from "APC resolved this through a different plan".</summary>
    [property: JsonPropertyName("apc_last_good_plan_id")] long? ApcLastGoodPlanId,

    [property: JsonPropertyName("apc_last_good_plan_forcing_type")] string? ApcLastGoodPlanForcingType,

    [property: JsonPropertyName("apc_last_good_plan_is_forced")] bool? ApcLastGoodPlanIsForced,

    /// <summary>The collector's <c>NULLIF(last_force_failure_reason_desc, 'NONE')</c> on the last-good plan —
    /// null when it never failed to force.</summary>
    [property: JsonPropertyName("apc_last_good_plan_force_failure_reason")] string? ApcLastGoodPlanForceFailureReason,

    /// <summary><c>execute_action_initiated_by</c> — <c>System</c> when APC applied the recommendation,
    /// <c>User</c> when an operator ran its script.</summary>
    [property: JsonPropertyName("apc_execute_action_initiated_by")] string? ApcExecuteActionInitiatedBy,

    [property: JsonPropertyName("apc_observed_at")] DateTime? ApcObservedAtUtc,

    /* ---- plan_correction: the database's FORCE_LAST_GOOD_PLAN enablement at the newest capture ---- */

    /// <summary><c>force_last_good_plan_actual_state</c> (<c>ON</c> / <c>OFF</c>) for the target's database
    /// at the server's newest plan_correction capture; null when the collector has never written a row for
    /// that database.</summary>
    [property: JsonPropertyName("force_last_good_plan_actual_state")] string? ForceLastGoodPlanActualState,

    [property: JsonPropertyName("enablement_observed_at")] DateTime? EnablementObservedAtUtc)
{
    /// <summary>
    /// How far back the state readers look for a target's newest <c>query_store_stats</c> and
    /// <c>plan_correction</c> rows. Shared by both SKUs' readers so the two stores answer the same
    /// question. Wide enough to span a Query Store flush cadence many times over and the collectors'
    /// schedules with margin; narrow enough that a plan nobody has executed for a day reads as "not
    /// observed" (null) rather than as last week's forcing state — and bounds the hypertable scan on
    /// Darling to one day of one server.
    /// </summary>
    public static readonly TimeSpan Lookback = TimeSpan.FromHours(24);

    /// <summary>The APC enablement spelling the engine uses for a database that is actually forcing.</summary>
    public const string EnablementOn = "ON";

    /// <summary>The <c>plan_forcing_type_desc</c> spelling for a plan automatic plan correction forced.</summary>
    public const string ForcingTypeAuto = "AUTO";

    /// <summary>Nothing observed on any of the three halves — the state read ran and found no row for
    /// this target inside <see cref="Lookback"/>.</summary>
    public bool IsEmpty =>
        PlanObservedAtUtc is null && OtherForcedPlanObservedAtUtc is null &&
        ApcObservedAtUtc is null && EnablementObservedAtUtc is null;

    /// <summary>FORCE_LAST_GOOD_PLAN is ON for this target's database (case-insensitive on the stored
    /// spelling; false when unknown — an unknown enablement never changes the verb).</summary>
    public bool ApcIsOn =>
        string.Equals(ForceLastGoodPlanActualState, EnablementOn, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// The identity of one force-plan target inside one server — the key the state readers return their rows
/// under and <c>FactRemediation.BuildStructuredRemediation</c> looks them up by. Database names compare
/// case-insensitively because SQL Server's are (under every default collation) and because the finding's
/// extractor and the two collectors spell the same database from different DMVs.
/// </summary>
public readonly record struct ForcePlanTargetKey(string Database, long QueryId, long PlanId)
{
    public static ForcePlanTargetKey Of(ForcePlanTarget target) =>
        new(target.Database, target.QueryId, target.PlanId);

    /// <summary>The comparer every state dictionary is built with.</summary>
    public static IEqualityComparer<ForcePlanTargetKey> Comparer { get; } = new KeyComparer();

    private sealed class KeyComparer : IEqualityComparer<ForcePlanTargetKey>
    {
        public bool Equals(ForcePlanTargetKey x, ForcePlanTargetKey y) =>
            x.QueryId == y.QueryId && x.PlanId == y.PlanId &&
            string.Equals(x.Database, y.Database, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode(ForcePlanTargetKey key) =>
            HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(key.Database ?? string.Empty),
                key.QueryId,
                key.PlanId);
    }
}

/// <summary>
/// One named blocker WITH its evidence (#3652). <see cref="Name"/> is the contract string an agent or the
/// bot branches on (the same string that appears in <c>StructuredForcePlanTarget.Blockers</c>);
/// <see cref="Evidence"/> quotes the store values it was built from and the <c>collection_time</c> of the
/// snapshot, so a stale snapshot is visible in the verdict rather than hidden behind it.
/// </summary>
public sealed record ForcePlanBlocker(
    [property: JsonPropertyName("blocker")] string Name,
    [property: JsonPropertyName("evidence")] string Evidence);

/// <summary>
/// The blocker vocabulary — consumer-API strings like the alert fact names: add freely, never respell.
/// The first two are #2138's; the rest are #3652's, one per way the live cross-reference found a
/// "no blockers" target to be contraindicated.
/// </summary>
public static class ForcePlanBlockerNames
{
    /// <summary>#2138 gap 3: the query also carries the PSP signature — forcing pins one shape for every
    /// parameter value.</summary>
    public const string ParameterSensitivityCoFired = "parameter_sensitivity_cofired";

    /// <summary>#1882 as data: the regression was measured on a non-primary replica but the statement
    /// forces on the primary.</summary>
    public const string SecondaryReplicaEvidence = "secondary_replica_evidence";

    /// <summary>Automatic plan correction is doing this: the target plan is forced with
    /// <c>plan_forcing_type = AUTO</c>, or the engine's recommendation on this query is
    /// <c>Verifying</c> / <c>Success</c> (on this plan or, via a different forced AUTO plan, on the query).
    /// A manual <c>sp_query_store_force_plan</c> REPLACES the AUTO forcing and removes the engine's revert
    /// path — it hijacks an in-flight correction rather than helping it.</summary>
    public const string ApcOwnsIt = "apc_owns_it";

    /// <summary>A plan is already forced by hand: on the target plan there is nothing to do; on a different
    /// plan of the same query, forcing the target would silently replace it (one forced plan per query) —
    /// a conflict to decide on purpose, not a force to add.</summary>
    public const string AlreadyForced = "already_forced";

    /// <summary>Forcing THIS plan is a known failure: <c>force_failure_count &gt; 0</c> on it (the Forced
    /// Plan Failing family's own evidence), or the engine's recommendation on it ended
    /// <c>Reverted</c> / <c>ForcingFailed</c>.</summary>
    public const string ForcingFailedOnThisPlan = "forcing_failed_on_this_plan";

    /// <summary>The engine recommended this very plan and then withdrew the recommendation
    /// (<c>Expired</c>, reason quoted — <c>TempTableChanged</c>, <c>SchemaChanged</c>,
    /// <c>StatisticsChanged</c>, <c>VerificationAborted</c>, ...). Contraindicated by the engine's own
    /// reasoning, which the evidence names.</summary>
    public const string ApcWithdrewIt = "apc_withdrew_it";

    /// <summary>The engine already resolved this query's regression (<c>Success</c>) through a DIFFERENT
    /// plan than the one proposed; the evidence says which.</summary>
    public const string ApcResolvedDifferently = "apc_resolved_differently";
}

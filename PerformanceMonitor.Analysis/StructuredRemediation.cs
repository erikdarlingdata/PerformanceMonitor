/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The machine-first projection of a remediation (#2138): the same decisions the prose caution and
/// disclosure blocks carry, as NAMED FIELDS, because MCP consumers read these findings more than people
/// do — an agent should never have to regex a T-SQL comment to learn what a gate decided. Built at READ
/// time from the persisted <see cref="ForcePlanTarget"/>s, deliberately never persisted itself: there is
/// no DTO mirror to forget (the #2140 review catch) and exactly one source of truth. Serialized property
/// names are snake_case by attribute because the MCP surfaces carry no naming policy — sibling fields are
/// snake_case only by their anonymous-object spellings.
/// <para>Currently force-plan only (the #2138 arc); other verbs return null from the builder and can gain
/// their own shapes when an agent consumer needs them.</para>
/// <para>#3652: <see cref="Guidance"/> is the remediation-level sentence for the case where the verb has
/// changed — automatic plan correction is ON for at least one target's database, so the remediation is no
/// longer "here is SQL to run" but "the engine is doing this; intervene only if it reverts or expires".
/// Null when no target is under APC. Per-target detail lives on each target's own
/// <see cref="StructuredForcePlanTarget.Guidance"/>.</para>
/// </summary>
public sealed record StructuredRemediation(
    [property: JsonPropertyName("fact_key")] string FactKey,
    [property: JsonPropertyName("verb")] string Verb,
    [property: JsonPropertyName("force_plan_targets")] IReadOnlyList<StructuredForcePlanTarget> ForcePlanTargets,
    [property: JsonPropertyName("guidance")] string? Guidance = null);

/// <summary>
/// One force-plan target, machine-first. The verdict half (<see cref="Eligible"/> /
/// <see cref="Blockers"/>) is the future auto-force bot's policy surface: it is filled by
/// <c>FactRemediation.ForcePlanBlockers</c>, the SAME function Phase 1+ will consult before acting — so
/// what agents inspect today is what the bot enforces tomorrow, and "never auto-force a flagged target"
/// is a testable data contract rather than a promise in comments. The artifacts are split
/// (<see cref="ForceSql"/> / <see cref="UnforceSql"/> / <see cref="VerifySql"/>) because agents compose
/// steps; a single blob makes them parse it apart. All three are ADVISORY — the read-only MCP surfaces
/// never execute anything.
///
/// <para><b>#3652 — the verdict now reads the store, and says when it could not.</b> <see cref="Blockers"/>
/// stays the list of names (the #2138 wire contract; <c>OperatorRemediationGate</c> and the bot branch on
/// it) and <see cref="Eligible"/> stays <c>blockers.Count == 0</c> — what changed is that the list is now
/// built from the persisted target AND a read-time <see cref="ForcePlanTargetState"/>, so "eligible"
/// means eligible rather than "not PSP, not on a secondary". The additions are appended with defaults so
/// the hand-built construction sites (the gate's disagreement pin) keep compiling:
/// <see cref="BlockerEvidence"/> pairs every blocker with the store values and snapshot time it was built
/// from; <see cref="ForcingState"/> is the raw state itself; <see cref="StateNote"/> is non-null whenever
/// the state was NOT available (reader failed, or no row inside the lookback) — the verdict never reads
/// as clean by silence; <see cref="ApcMode"/> / <see cref="Guidance"/> carry the verb change when
/// FORCE_LAST_GOOD_PLAN is ON for the target's database. <see cref="ForceSql"/> stays present under
/// every blocker for the operator who has read the evidence and decided; the Apply affordance reads
/// <see cref="Eligible"/>, so it cannot be clicked into a hijack.</para>
/// </summary>
public sealed record StructuredForcePlanTarget(
    [property: JsonPropertyName("database")] string Database,
    [property: JsonPropertyName("query_id")] long QueryId,
    [property: JsonPropertyName("plan_id")] long PlanId,
    [property: JsonPropertyName("latest_plan_hash")] string? LatestPlanHash,
    [property: JsonPropertyName("best_plan_hash")] string? BestPlanHash,
    [property: JsonPropertyName("replica_role")] string? ReplicaRole,
    [property: JsonPropertyName("eligible")] bool Eligible,
    [property: JsonPropertyName("blockers")] IReadOnlyList<string> Blockers,
    [property: JsonPropertyName("evidence")] StructuredForcePlanEvidence Evidence,
    [property: JsonPropertyName("force_sql")] string ForceSql,
    [property: JsonPropertyName("unforce_sql")] string UnforceSql,
    [property: JsonPropertyName("verify_sql")] string VerifySql,
    [property: JsonPropertyName("blocker_evidence")] IReadOnlyList<ForcePlanBlocker>? BlockerEvidence = null,
    [property: JsonPropertyName("forcing_state")] ForcePlanTargetState? ForcingState = null,
    [property: JsonPropertyName("state_note")] string? StateNote = null,
    [property: JsonPropertyName("apc_mode")] string? ApcMode = null,
    [property: JsonPropertyName("guidance")] string? Guidance = null);

/// <summary>
/// The numbers behind a target's verdict — only what the persisted target actually carries, nothing
/// re-derived or estimated at read time.
/// </summary>
public sealed record StructuredForcePlanEvidence(
    [property: JsonPropertyName("regression_factor")] double RegressionFactor,
    [property: JsonPropertyName("latest_cpu_per_exec_us")] double LatestCpuPerExecUs,
    [property: JsonPropertyName("best_cpu_per_exec_us")] double BestCpuPerExecUs,
    [property: JsonPropertyName("parameter_sensitivity_cofired")] bool ParameterSensitivityCoFired);

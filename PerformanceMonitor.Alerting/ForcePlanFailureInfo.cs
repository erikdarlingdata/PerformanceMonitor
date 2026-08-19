/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Alerting;

/// <summary>
/// A forced Query Store plan whose <c>force_failure_count</c> ROSE between the two most recent
/// collections — the unit the forced-plan-failure alert fires on (#2157). The read adapter returns only
/// risen rows, exactly as the database-state adapter returns only deviating ones, so the engine never
/// has to decide what "new" means.
///
/// <para><b>Why the delta and not the level.</b> <c>force_failure_count</c> is cumulative and it travels
/// with the database: restore a database somewhere else and its Query Store arrives carrying every
/// historical failure. An alert on the level would therefore fire forever about failures that happened
/// on a machine the operator may no longer own. A rise, by contrast, means the engine is failing to
/// reproduce that plan RIGHT NOW — which is the actionable event, because the query silently falls back
/// to the optimizer's plan and nothing else in the product witnesses it.</para>
///
/// <para>A counter that DROPS (an unforce/re-force cycle resets it) is not a failure and must not alert;
/// the adapter treats that as a silent re-arm.</para>
/// </summary>
public sealed class ForcePlanFailureInfo
{
    /// <summary>The user database the forced plan lives in.</summary>
    public string DatabaseName { get; set; } = "";

    /// <summary>Query Store <c>query_id</c> — half of the identity an operator needs to find the plan.</summary>
    public long QueryId { get; set; }

    /// <summary>Query Store <c>plan_id</c> — the forced plan itself.</summary>
    public long PlanId { get; set; }

    /// <summary>
    /// <c>plan_forcing_type_desc</c>: MANUAL (a human or a tool forced it) or AUTO (Automatic Plan
    /// Correction did). Both matter and are reported: a failing MANUAL force is somebody's mitigation
    /// silently not working, while a failing AUTO force is the engine's own correction not applying.
    /// </summary>
    public string ForcingType { get; set; } = "";

    /// <summary>
    /// <c>last_force_failure_reason_desc</c> — the engine's own words for why the plan could not be
    /// reproduced (NO_PLAN, NO_INDEX, INVALID_STARTING_JOIN_ORDER, …). Carried into the alert body
    /// because it is the difference between "the index it needs is gone" and "the plan is unusable".
    /// </summary>
    public string FailureReason { get; set; } = "";

    /// <summary>How much the counter rose between the two samples — how many failures are NEW.</summary>
    public long FailureDelta { get; set; }

    /// <summary>The cumulative count as of the newer sample, for context on whether this is chronic.</summary>
    public long TotalFailures { get; set; }
}

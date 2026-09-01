/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One write's outcome — success, or the error text the journal records. <see cref="Note"/> rides a
/// SUCCESS that did less than the verb implies (an evict that found nothing cached), so the journal
/// can say what happened without the row counting as a failure: failure outcomes feed
/// <see cref="PerformanceMonitor.Analysis.ForcePlanBotHistory.RecentFailedForces"/>' cooldown window,
/// and a plan that aged out on its own must never cool a query down.
/// </summary>
public sealed record PlanForceExecutionResult(bool Succeeded, string? Error, string? Note = null)
{
    public static PlanForceExecutionResult Success { get; } = new(true, null);

    public static PlanForceExecutionResult Failed(string error) => new(false, error);

    public static PlanForceExecutionResult NoOp(string note) => new(true, null, note);
}

/// <summary>The verify read's answer — the inputs <c>ForcePlanSelfReview.Evaluate</c> judges.</summary>
public sealed record PlanForceVerifyResult(
    bool PlanIsStillForced,
    long ForceFailureCount,
    string? LastForceFailureReason,
    long ExecutionsSinceForce,
    double? ObservedCpuPerExecUs);

/// <summary>
/// The bot's ONLY route to a monitored SQL Server (#2138). A seam rather than inline SQL so the
/// orchestration is testable with a fake, and so there is exactly one reviewable place where a write
/// statement can originate.
///
/// <para><b>Phase 1 ships this interface with NO implementation, deliberately.</b> Nothing in this
/// build constructs an implementer, nothing hands one to <see cref="PlanForceBot"/>, and
/// <c>PlanForceNoWritePathTests</c> asserts both — plus that the shipped service assembly contains no
/// <c>sp_query_store_force_plan</c> / <c>sp_query_store_unforce_plan</c> / <c>DBCC FREEPROCCACHE</c>
/// string anywhere in it. So phase 1 is a detection, evidence and dry-run-journal feature that
/// CANNOT write to a monitored server, and the write path arrives as its own reviewable change
/// (#2731) which necessarily has to unlock those pins in the open.</para>
///
/// <para>The declaration lands here rather than with that change so the orchestrator's shape, the
/// journal's vocabulary, and the policy's verdicts are reviewed against the contract they will
/// actually be wired to — and so the write path's diff is the write path, nothing else.</para>
/// </summary>
public interface IPlanForceExecutor
{
    Task<PlanForceExecutionResult> ForcePlanAsync(string database, long queryId, long planId, CancellationToken ct);

    Task<PlanForceExecutionResult> UnforcePlanAsync(string database, long queryId, long planId, CancellationToken ct);

    /// <summary>
    /// The evict-first lever: targeted <c>DBCC FREEPROCCACHE(plan_handle)</c> for every cached plan
    /// whose <c>query_plan_hash</c> matches, giving the optimizer one free shot at recovering on its
    /// own before any force. Declared here; implemented with the rest of the write path (#2731), and
    /// the bot's orchestration of it (evict, observe, then force only if the bad plan returns) is
    /// phase 2 after that.
    /// </summary>
    Task<PlanForceExecutionResult> EvictPlanAsync(string database, string queryPlanHashHex, CancellationToken ct);

    /// <summary>The self-review's read: did the force stick, and what has the query cost since.
    /// Read-only — safe on any server regardless of gates — but it has nothing to read until a live
    /// force exists, so it too arrives with the write path.</summary>
    Task<PlanForceVerifyResult?> VerifyAsync(string database, long queryId, long planId, DateTime forcedAtUtc, CancellationToken ct);
}

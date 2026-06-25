/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// A per-fact-type remediation handler. Translates a structured
    /// <see cref="RemediationAction"/> into read-only preflight and typed,
    /// per-target apply/unapply operations through an <see cref="IRemediationExecutor"/>.
    ///
    /// <para>
    /// <see cref="ApplyAsync"/> / <see cref="UnapplyAsync"/> take NO preflight
    /// result — they cannot be handed a forged "all-clear"; the authoritative gate
    /// is re-derived per target via the executor (which gates on a single
    /// connection, R2-MOD-1). Preflight is advisory display only.
    /// </para>
    /// </summary>
    public interface IRemediationHandler
    {
        /// <summary>Registry key; matches <see cref="RemediationAction.FactKey"/>.</summary>
        string FactKey { get; }

        /// <summary>
        /// Whether the action is DESTRUCTIVE (B3 Phase 3): true gates the
        /// informed-consent (acknowledge-each-risk) confirm UI. Force-plan and the
        /// always-safe DB-config fixes are not destructive (false); RcsiHandler is the
        /// first (and only) handler to flip this true. Read per-handler by
        /// RemediationApplyService.BuildConfirmRequest, which is exactly why RCSI needs
        /// a distinct handler rather than a per-target flag on DbConfigHandler.
        /// </summary>
        bool IsDestructive { get; }

        /// <summary>
        /// Whether this fix type can be un-applied. Force-plan can (unforce); the
        /// always-safe DB-config fixes have no sensible reverse, so DbConfigHandler
        /// returns false. The UI gates the Un-apply button on this, and
        /// RemediationApplyService fails safe (clean report, no exception) if an
        /// un-apply is requested for a handler where this is false.
        /// </summary>
        bool SupportsUnapply { get; }

        /// <summary>Read-only, per-target disposition for the UI. Advisory.</summary>
        Task<PreflightResult> PreflightAsync(RemediationAction action, IRemediationExecutor exec, CancellationToken ct);

        /// <summary>
        /// Structured, per-target apply. Hard-blocks every target when the audit
        /// table is absent (no mutation). Otherwise applies one target at a time,
        /// re-gated by the executor, and writes one audit row per attempt.
        /// </summary>
        Task<ApplyResult> ApplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct);

        /// <summary>Inverse of <see cref="ApplyAsync"/>, restricted to targets B3 itself applied.</summary>
        Task<ApplyResult> UnapplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct);
    }
}

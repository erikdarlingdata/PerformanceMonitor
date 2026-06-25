/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// Top-level outcome of a <see cref="RemediationApplyService"/> run. Distinguishes
    /// the three terminal states the UI must surface differently: there was no
    /// handler for the fact key (no Apply affordance should ever have been shown),
    /// the operator declined the confirm modal (nothing ran — the gate held), or the
    /// privileged handler actually ran (per-target results follow).
    /// </summary>
    public enum RemediationRunStatus
    {
        /// <summary>No registered handler for the action's fact key — nothing ran.</summary>
        NoHandler,

        /// <summary>The operator declined the confirm modal — the gate held; no mutation.</summary>
        NotConfirmed,

        /// <summary>
        /// An un-apply was requested for a handler that does not support it
        /// (SupportsUnapply == false). Fails SAFE: nothing ran, no exception (m-C).
        /// The UI gates the Un-apply button on SupportsUnapply, so this is a
        /// defensive backstop for a future mis-wired caller.
        /// </summary>
        UnapplyNotSupported,

        /// <summary>The handler ran; see <see cref="RemediationRunReport.Targets"/>.</summary>
        Ran
    }

    /// <summary>
    /// Why a post-apply audit INSERT failed (LOW-2). Only meaningful for a target
    /// whose force succeeded but whose audit row could not be written against a
    /// present <c>config.remediation_action_log</c>.
    /// </summary>
    public enum AuditWriteFailureKind
    {
        /// <summary>No audit failure — the row was written.</summary>
        None,

        /// <summary>
        /// The login can INSERT, so the failure was a one-off (deadlock / timeout) —
        /// a soft warning; retrying or Un-apply/Apply again will likely log fine.
        /// </summary>
        Transient,

        /// <summary>
        /// The login lacks INSERT on a present <c>config.remediation_action_log</c>
        /// — a setup/grant misconfiguration that will silently drop EVERY audit row
        /// until fixed. Surfaced loudly ("fix this") so a force-succeeds-but-never-
        /// logged deployment is visible, not buried.
        /// </summary>
        Permanent,

        /// <summary>The classification probe itself failed — surface as a soft warning.</summary>
        Unknown
    }

    /// <summary>
    /// The resolved (or deliberately unresolved) source server for an alert. M3 is
    /// fail-closed: a missing or ambiguous resolution yields <see cref="Server"/>
    /// null and a <see cref="Reason"/> for the disabled-Apply tooltip — the UI never
    /// silently picks a server.
    /// </summary>
    public sealed class ServerResolution
    {
        public ServerConnection? Server { get; init; }

        /// <summary>True only when a single, unambiguous server was resolved.</summary>
        public bool IsResolved => Server is not null;

        /// <summary>
        /// True when resolution fell back to a unique <c>ServerName</c> match
        /// (the persisted ServerId was an int-id fallback / legacy / missing GUID).
        /// Surfaced as a note in the confirm modal.
        /// </summary>
        public bool ResolvedByName { get; init; }

        /// <summary>Why Apply is disabled (tooltip text) when <see cref="IsResolved"/> is false.</summary>
        public string? Reason { get; init; }
    }

    /// <summary>One target row shown in the confirm modal (the "what / where").</summary>
    public sealed class RemediationConfirmTarget
    {
        public string Database { get; init; } = "";
        public long QueryId { get; init; }
        public long PlanId { get; init; }

        /// <summary>Original regression_factor from the finding (M2 — surfaced so the operator owns the still-better judgment).</summary>
        public double RegressionFactor { get; init; }

        /// <summary>
        /// Fact-key-neutral display title for a non-force-plan row (DB_CONFIG renders
        /// this instead of query_id/plan_id), e.g. "[Foo] SET AUTO_SHRINK OFF — was ON".
        /// Null for force-plan rows (they render the query_id/plan_id head).
        /// </summary>
        public string? StatusTitle { get; init; }

        /// <summary>Advisory preflight disposition for this target (display only).</summary>
        public RemediationDisposition Disposition { get; init; }
        public string? DispositionMessage { get; init; }
    }

    /// <summary>
    /// The five-W payload the confirm modal renders. Built by
    /// <see cref="RemediationApplyService"/> from the read-only preflight + the
    /// action; handed to the operator's confirm callback. The callback returning
    /// true is the ONLY thing that lets the privileged handler run.
    /// </summary>
    public sealed class RemediationConfirmRequest
    {
        public string ServerDisplayName { get; init; } = "";
        public bool IsUnapply { get; init; }

        /// <summary>The action's fact key ("PLAN_REGRESSION" | "DB_CONFIG" | "RCSI"), so the modal can show a fact-key-specific header/banner.</summary>
        public string FactKey { get; init; } = "";

        /// <summary>Exact SQL preview shown verbatim (the code-block T-SQL for apply; the unforce statements for un-apply).</summary>
        public string PreviewSql { get; init; } = "";

        /// <summary>OS / Windows user driving the Dashboard (audited as operator_identity).</summary>
        public string OperatorIdentity { get; init; } = "";

        /// <summary>SUSER_SNAME() under the monitoring connection (from preflight) — the identity the mutation actually runs as.</summary>
        public string? ExecutingLogin { get; init; }

        public IReadOnlyList<RemediationConfirmTarget> Targets { get; init; } = new List<RemediationConfirmTarget>();

        /// <summary>False when the target server is pre-3.0.0 schema — apply will hard-block.</summary>
        public bool AuditTableExists { get; init; }

        /// <summary>
        /// B3 Phase 3: true iff the resolved handler's <c>IsDestructive</c> is true.
        /// When true, the confirm dialog renders the two-sided <see cref="Risks"/> as
        /// acknowledge-each-risk checkboxes and keeps Apply disabled until ALL are
        /// checked (in addition to <see cref="AnyActionable"/>). Always false for the
        /// always-safe and force-plan actions (their single-confirm is unchanged).
        /// The DIALOG is the trust boundary; this bool is the input to that UI gate.
        /// </summary>
        public bool RequiresInformedConsent { get; init; }

        /// <summary>
        /// B3 Phase 3: the two-sided informed-consent risk content (risks of changing +
        /// risks of NOT changing, the latter quantified from this server's monitoring
        /// data). Null for non-destructive actions. Rendered as checkboxes in-app (PR-B)
        /// and as read-only disclosure on email/webhook/MCP surfaces.
        /// </summary>
        public RiskDisclosure? Risks { get; init; }

        /// <summary>
        /// True when at least one target is in a state where applying can do something.
        /// Ok / WarnFailing are actionable (force-plan AND a DB_CONFIG Ok). The new
        /// DB_CONFIG dispositions (AlreadyInDesiredState, BlockDatabaseNotFound) are
        /// deliberately NOT actionable — an all-already-desired DB_CONFIG request
        /// disables Apply, exactly like an all-AlreadyForced force-plan request. This
        /// set is unchanged for force-plan (no loosening).
        /// </summary>
        public bool AnyActionable =>
            AuditTableExists &&
            Targets.Any(t => t.Disposition is RemediationDisposition.Ok or RemediationDisposition.WarnFailing);

        /// <summary>
        /// M2 caveat: freshness proves the plan is present and forceable — NOT that
        /// the forced plan is still the better choice. That judgment is the
        /// operator's. Mirrors FactAdvice.cs:393-394; surfaced in the modal so it
        /// cannot be missed by not reading the preview comment.
        /// </summary>
        public const string StillBetterCaveat =
            "Freshness was verified (plan present, forceable, Query Store READ_WRITE) — " +
            "but NOT that the forced plan is still the better choice. Schema changes, data " +
            "growth, or updated statistics may have made the historical plan stale. Verify " +
            "the forced plan is still better against current data before applying.";
    }

    /// <summary>Per-target result the UI surfaces after a run.</summary>
    public sealed class RemediationTargetReport
    {
        public string Database { get; init; } = "";
        public long QueryId { get; init; }
        public long PlanId { get; init; }
        public RemediationStatus Status { get; init; }
        public string? Message { get; init; }
        public bool AuditWritten { get; init; }

        /// <summary>The mutation succeeded but the audit row could not be written (O3).</summary>
        public bool AppliedButUnlogged { get; init; }

        /// <summary>Permanent-vs-transient classification for an applied-but-unlogged target (LOW-2).</summary>
        public AuditWriteFailureKind AuditFailureKind { get; init; }
    }

    /// <summary>Aggregate run report handed back to the UI.</summary>
    public sealed class RemediationRunReport
    {
        public RemediationRunStatus Status { get; init; }
        public bool IsUnapply { get; init; }
        public IReadOnlyList<RemediationTargetReport> Targets { get; init; } = new List<RemediationTargetReport>();

        public bool AnySuccess => Targets.Any(t => t.Status == RemediationStatus.Success);
    }
}

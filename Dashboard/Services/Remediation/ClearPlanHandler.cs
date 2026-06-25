/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// Handler for the DESTRUCTIVE clear-cached-plan fix: clears the currently-cached
    /// execution plan(s) for an abnormally-expensive query via
    /// <c>DBCC FREEPROCCACHE(@plan_handle)</c> (per resolved handle), forcing a recompile.
    /// The SECOND <see cref="IsDestructive"/> consumer (after <see cref="RcsiHandler"/>);
    /// it is gated behind the informed-consent (acknowledge-each-risk) dialog.
    ///
    /// <para>
    /// Routed via the distinct fact key "CLEAR_PLAN" so it is never reachable through any
    /// always-safe handler. The handler hands the executor only the stable
    /// <c>query_hash</c>; the executor (<c>exec.ClearProcCacheAsync</c>) re-derives the
    /// authoritative gate (the NAMED <c>ALTER SERVER STATE</c> permission, fail-closed) +
    /// the live <c>plan_handle</c> resolve (with the null/zero-length guard, M-1) + the
    /// per-handle DBCC, all on ONE open connection to the target server. The handler
    /// cannot hand it a forged all-clear, and there is NO bare/whole-cache DBCC path.
    /// </para>
    ///
    /// <para>
    /// APPLY-ONLY: <see cref="SupportsUnapply"/> is false — a cleared plan cannot be
    /// un-cleared (the prior plan is gone). <see cref="UnapplyAsync"/> throws, and
    /// RemediationApplyService short-circuits any un-apply to a clean UnapplyNotSupported
    /// report before reaching it. One audit row is written per attempt with
    /// <c>action='clear_cached_plan'</c>, <c>consent_acknowledged=true</c> (the gate was
    /// satisfied to reach apply), and query_id/plan_id NULL (this is plan-cache, not
    /// Query Store). UNREGISTERED in PR-A (dead-code-safe) — PR-B wires it up.
    /// </para>
    /// </summary>
    public sealed class ClearPlanHandler : IRemediationHandler
    {
        public string FactKey => "CLEAR_PLAN";

        // DESTRUCTIVE: clearing a cached plan forces a recompile whose new plan is not
        // guaranteed better, and clears EVERY currently-cached plan for the query hash
        // (possibly spanning more than the expected query). The second true in the
        // codebase; it requests the informed-consent gate.
        public bool IsDestructive => true;

        // Apply-only: you cannot un-clear a cache — the prior plan is gone. prior_value
        // records what was cleared for the audit trail.
        public bool SupportsUnapply => false;

        public async Task<PreflightResult> PreflightAsync(RemediationAction action, IRemediationExecutor exec, CancellationToken ct)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (exec is null) throw new ArgumentNullException(nameof(exec));

            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);

            var targets = new List<TargetPreflight>();
            foreach (var t in ClearTargets(action))
            {
                ct.ThrowIfCancellationRequested();

                var pf = new TargetPreflight
                {
                    Database = t.Database,
                    Disposition = auditTableExists ? RemediationDisposition.Ok : RemediationDisposition.BlockAuditTableAbsent,
                    Message = auditTableExists
                        ? $"Ready to clear the cached plan(s) for query hash {t.QueryHash} on this server (live-resolved at apply; requires ALTER SERVER STATE)."
                        : AuditAbsentMessage
                };
                targets.Add(pf);
            }

            return new PreflightResult { Targets = targets, AuditTableExists = auditTableExists };
        }

        public Task<ApplyResult> ApplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (exec is null) throw new ArgumentNullException(nameof(exec));
            if (identity is null) throw new ArgumentNullException(nameof(identity));
            return RunApplyAsync(action, exec, identity, ct);
        }

        // Apply-only. RemediationApplyService short-circuits an un-apply for a
        // non-supporting handler to a clean report BEFORE any confirm or handler call,
        // so this is never reached in practice.
        public Task<ApplyResult> UnapplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
            => throw new NotSupportedException("Clearing a cached plan is apply-only; a cleared plan cannot be un-cleared (the prior plan is gone — the cleared-plan summary is recorded for the audit trail).");

        private static async Task<ApplyResult> RunApplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
        {
            var outcomes = new List<TargetOutcome>();
            var targets = ClearTargets(action);

            // Audit-table-absent is a HARD BLOCK before any mutation. No DBCC is
            // attempted; no audit row is written (nowhere to write it).
            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);
            if (!auditTableExists)
            {
                foreach (var t in targets)
                {
                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        Status = RemediationStatus.Blocked,
                        Message = AuditAbsentMessage,
                        AuditWritten = false,
                        AppliedButUnlogged = false
                    });
                }
                return new ApplyResult { Outcomes = outcomes };
            }

            foreach (var t in targets)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    // The executor re-derives the authoritative gate (ALTER SERVER STATE,
                    // fail-closed) + live handle resolve (null/zero-length guard) + the
                    // single-handle DBCC, on one connection — the handler cannot hand it a
                    // forged all-clear, and there is no bare/whole-cache path.
                    var outcome = await exec.ClearProcCacheAsync(t.QueryHash, identity, ct).ConfigureAwait(false);

                    var written = await WriteAuditAsync(exec, identity, t, outcome, ct).ConfigureAwait(false);

                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        Status = outcome.Status,
                        Message = outcome.Message,
                        ExecutingLogin = outcome.ExecutingLogin,
                        AuditWritten = written,
                        // A real mutation that we failed to log against a present table.
                        AppliedButUnlogged = outcome.Cleared && !written
                    });
                }
                catch (Exception ex)
                {
                    // Per-target independence: one target's failure never aborts the rest.
                    var record = BuildRecord(identity, t, RemediationStatus.Error, ex.Message, priorValue: null, generatedSql: null, executingLogin: null);
                    var written = await RemediationAuditHelpers.TryWriteAuditSafeAsync(exec, record, ct).ConfigureAwait(false);
                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        Status = RemediationStatus.Error,
                        Message = ex.Message,
                        AuditWritten = written,
                        AppliedButUnlogged = false
                    });
                }
            }

            return new ApplyResult { Outcomes = outcomes };
        }

        private static async Task<bool> WriteAuditAsync(
            IRemediationExecutor exec,
            RemediationIdentity identity,
            ClearPlanTarget target,
            ClearPlanOutcome outcome,
            CancellationToken ct)
        {
            var record = BuildRecord(
                identity, target, outcome.Status, outcome.Message,
                priorValue: outcome.PriorValue, generatedSql: outcome.GeneratedSql, executingLogin: outcome.ExecutingLogin);
            return await exec.WriteAuditAsync(record, ct).ConfigureAwait(false);
        }

        private static RemediationAuditRecord BuildRecord(
            RemediationIdentity identity,
            ClearPlanTarget target,
            RemediationStatus status,
            string? message,
            string? priorValue,
            string? generatedSql,
            string? executingLogin)
            => new()
            {
                OperatorIdentity = identity.OperatorIdentity,
                ExecutingLogin = executingLogin,
                TargetDatabase = target.Database,
                FactKey = "CLEAR_PLAN",
                QueryId = null,                 // plan-cache, not Query Store — no query_id/plan_id
                PlanId = null,
                Action = "clear_cached_plan",   // 17 chars; fits the VarChar(32) @action param
                PriorValue = priorValue,        // short summary, e.g. "{N} plan(s) cached for this query hash"
                GeneratedSql = generatedSql,    // the DBCC FREEPROCCACHE statements actually run
                Result = RemediationAuditHelpers.AuditResult(status),
                ErrorMessage = RemediationAuditHelpers.IsErrorish(status) ? message : null,
                // The informed-consent gate was satisfied to reach apply: a destructive
                // clear that got here passed every risk checkbox.
                ConsentAcknowledged = true,
                SourceAlertRef = identity.SourceAlertRef
            };

        private static IReadOnlyList<ClearPlanTarget> ClearTargets(RemediationAction action) =>
            action.ClearPlanTargets ?? Array.Empty<ClearPlanTarget>();

        private const string AuditAbsentMessage =
            "This server is not on the 3.0.0 schema (config.remediation_action_log is absent). " +
            "Upgrade this server to 3.0.0 to enable audited Apply Fix; no change was made.";
    }
}

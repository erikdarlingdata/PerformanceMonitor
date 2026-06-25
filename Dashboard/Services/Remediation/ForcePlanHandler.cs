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
    /// Handler for PLAN_REGRESSION findings: forces / unforces a Query Store plan
    /// via <c>sp_query_store_force_plan</c> / <c>sp_query_store_unforce_plan</c>,
    /// one target at a time, never an opaque batch.
    ///
    /// <para>
    /// The privileged path is structurally self-gating: <see cref="ApplyAsync"/>
    /// takes no preflight disposition and does not trust one. Before any mutation
    /// it (1) hard-blocks every target if the audit table is absent (no mutation),
    /// then (2) for each target delegates to the executor, which re-derives the
    /// authoritative gate (correct DB + ALTER + freshness) and the EXEC on one
    /// connection. One audit row is written per attempt — success, skip, error, or
    /// abort.
    /// </para>
    /// </summary>
    public sealed class ForcePlanHandler : IRemediationHandler
    {
        public string FactKey => "PLAN_REGRESSION";

        // Force-plan is reversible (unforce backs it out), so it is not a
        // destructive action and does not require the typed-confirm hook.
        public bool IsDestructive => false;

        // Force-plan has a real inverse (unforce), so it supports un-apply.
        public bool SupportsUnapply => true;

        public async Task<PreflightResult> PreflightAsync(RemediationAction action, IRemediationExecutor exec, CancellationToken ct)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (exec is null) throw new ArgumentNullException(nameof(exec));

            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);

            var targets = new List<TargetPreflight>();
            foreach (var t in action.Targets)
            {
                ct.ThrowIfCancellationRequested();

                TargetPreflight pf;
                try
                {
                    pf = await exec.PreflightForcePlanAsync(t.Database, t.QueryId, t.PlanId, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    pf = new TargetPreflight
                    {
                        Database = t.Database,
                        QueryId = t.QueryId,
                        PlanId = t.PlanId,
                        Disposition = RemediationDisposition.Error,
                        Message = ex.Message
                    };
                    targets.Add(pf);
                    continue;
                }

                // Audit-table-absent is a server-wide hard block; it overrides any
                // per-target disposition because no apply can be audited there.
                if (!auditTableExists)
                {
                    pf.Disposition = RemediationDisposition.BlockAuditTableAbsent;
                    pf.Message = AuditAbsentMessage;
                }
                else
                {
                    pf.Disposition = ClassifyPreflight(pf, t.Database);
                    pf.Message ??= DispositionMessage(pf.Disposition, t.Database);
                }

                targets.Add(pf);
            }

            return new PreflightResult { Targets = targets, AuditTableExists = auditTableExists };
        }

        public Task<ApplyResult> ApplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
            => RunAsync(action, exec, identity, isUnapply: false, ct);

        public Task<ApplyResult> UnapplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
            => RunAsync(action, exec, identity, isUnapply: true, ct);

        private async Task<ApplyResult> RunAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, bool isUnapply, CancellationToken ct)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (exec is null) throw new ArgumentNullException(nameof(exec));
            if (identity is null) throw new ArgumentNullException(nameof(identity));

            var actionVerb = isUnapply ? "unforce" : "force";
            var outcomes = new List<TargetOutcome>();

            // R2-MOD-2: audit-table-absent is a HARD BLOCK before any mutation.
            // No sp_query_store_force_plan/unforce is attempted; no audit row is
            // written (there is nowhere to write it). This makes "applied-but-
            // unlogged" impossible on an un-upgraded server.
            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);
            if (!auditTableExists)
            {
                foreach (var t in action.Targets)
                {
                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        QueryId = t.QueryId,
                        PlanId = t.PlanId,
                        Status = RemediationStatus.Blocked,
                        Message = AuditAbsentMessage,
                        AuditWritten = false,
                        AppliedButUnlogged = false
                    });
                }
                return new ApplyResult { Outcomes = outcomes };
            }

            foreach (var t in action.Targets)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    // Un-apply is restricted to targets B3 itself forced (and that
                    // are not already unforced) — never undo something we did not do.
                    if (isUnapply)
                    {
                        var priorForce = await exec.HasPriorForceAsync(t.Database, t.QueryId, t.PlanId, ct).ConfigureAwait(false);
                        if (!priorForce)
                        {
                            var skip = new TargetOutcome
                            {
                                Database = t.Database,
                                QueryId = t.QueryId,
                                PlanId = t.PlanId,
                                Status = RemediationStatus.Skipped,
                                Message = "No prior Apply-Fix force for this target to undo.",
                                AuditWritten = false,
                                AppliedButUnlogged = false
                            };
                            // Audit the skip so the log reflects the attempt.
                            var skipWritten = await WriteAudit(exec, identity, t, actionVerb, RemediationStatus.Skipped, skip.Message, executingLogin: null, ct).ConfigureAwait(false);
                            outcomes.Add(WithAudit(skip, skipWritten, appliedButUnlogged: false));
                            continue;
                        }
                    }

                    var outcome = isUnapply
                        ? await exec.UnforcePlanAsync(t.Database, t.QueryId, t.PlanId, identity, ct).ConfigureAwait(false)
                        : await exec.ForcePlanAsync(t.Database, t.QueryId, t.PlanId, identity, ct).ConfigureAwait(false);

                    var written = await WriteAudit(exec, identity, t, actionVerb, outcome.Status, outcome.Message, outcome.ExecutingLogin, ct).ConfigureAwait(false);

                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        QueryId = t.QueryId,
                        PlanId = t.PlanId,
                        Status = outcome.Status,
                        Message = outcome.Message,
                        ExecutingLogin = outcome.ExecutingLogin,
                        AuditWritten = written,
                        // O3: a real mutation that we failed to log against a present table.
                        AppliedButUnlogged = outcome.Forced && !written
                    });
                }
                catch (Exception ex)
                {
                    // Per-target independence: one target's failure never aborts the rest.
                    var written = await TryWriteAuditSafe(exec, identity, t, actionVerb, RemediationStatus.Error, ex.Message, ct).ConfigureAwait(false);
                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        QueryId = t.QueryId,
                        PlanId = t.PlanId,
                        Status = RemediationStatus.Error,
                        Message = ex.Message,
                        AuditWritten = written,
                        AppliedButUnlogged = false
                    });
                }
            }

            return new ApplyResult { Outcomes = outcomes };
        }

        private static async Task<bool> WriteAudit(
            IRemediationExecutor exec,
            RemediationIdentity identity,
            ForcePlanTarget target,
            string actionVerb,
            RemediationStatus status,
            string? message,
            string? executingLogin,
            CancellationToken ct)
        {
            var record = new RemediationAuditRecord
            {
                OperatorIdentity = identity.OperatorIdentity,
                ExecutingLogin = executingLogin,
                TargetDatabase = target.Database,
                FactKey = "PLAN_REGRESSION",
                QueryId = target.QueryId,
                PlanId = target.PlanId,
                Action = actionVerb,
                GeneratedSql = GeneratedSql(actionVerb, target.QueryId, target.PlanId),
                Result = AuditResult(status),
                ErrorMessage = status is RemediationStatus.Error or RemediationStatus.PermissionDenied or RemediationStatus.Blocked ? message : null,
                // Force-plan is reversible (not destructive) — never went through the
                // informed-consent gate (M-3 regression-guard: explicit false).
                ConsentAcknowledged = false,
                SourceAlertRef = identity.SourceAlertRef
            };

            return await exec.WriteAuditAsync(record, ct).ConfigureAwait(false);
        }

        private static async Task<bool> TryWriteAuditSafe(
            IRemediationExecutor exec,
            RemediationIdentity identity,
            ForcePlanTarget target,
            string actionVerb,
            RemediationStatus status,
            string? message,
            CancellationToken ct)
        {
            try
            {
                return await WriteAudit(exec, identity, target, actionVerb, status, message, executingLogin: null, ct).ConfigureAwait(false);
            }
            catch
            {
                // The audit write itself threw while handling a target error — do
                // not let it mask the original per-target error outcome.
                return false;
            }
        }

        private static TargetOutcome WithAudit(TargetOutcome o, bool written, bool appliedButUnlogged) => new()
        {
            Database = o.Database,
            QueryId = o.QueryId,
            PlanId = o.PlanId,
            Status = o.Status,
            Message = o.Message,
            ExecutingLogin = o.ExecutingLogin,
            AuditWritten = written,
            AppliedButUnlogged = appliedButUnlogged
        };

        private static string GeneratedSql(string actionVerb, long queryId, long planId)
        {
            var proc = actionVerb == "unforce" ? "sp_query_store_unforce_plan" : "sp_query_store_force_plan";
            return $"EXEC sys.{proc} @query_id = {queryId}, @plan_id = {planId};";
        }

        private static string AuditResult(RemediationStatus status) => status switch
        {
            RemediationStatus.Success => "success",
            RemediationStatus.Skipped => "skipped",
            RemediationStatus.PermissionDenied => "skipped",
            RemediationStatus.Blocked => "aborted",
            RemediationStatus.Error => "error",
            _ => "error"
        };

        private static RemediationDisposition ClassifyPreflight(TargetPreflight pf, string expectedDatabase)
        {
            if (!string.Equals(pf.CurrentDatabase, expectedDatabase, StringComparison.Ordinal))
                return RemediationDisposition.BlockWrongDatabase;
            if (!pf.HasAlter)
                return RemediationDisposition.BlockNoAlter;
            if (!string.Equals(pf.QueryStoreState, "READ_WRITE", StringComparison.OrdinalIgnoreCase))
                return RemediationDisposition.BlockQueryStoreOff;
            if (!pf.PlanPresent)
                return RemediationDisposition.BlockStale;
            if (pf.IsForcedPlan)
                return RemediationDisposition.AlreadyForced;
            if (pf.ForceFailureCount > 0)
                return RemediationDisposition.WarnFailing;
            return RemediationDisposition.Ok;
        }

        private static string DispositionMessage(RemediationDisposition d, string database) => d switch
        {
            RemediationDisposition.Ok => "Ready to apply.",
            RemediationDisposition.AlreadyForced => "Already forced to this plan — nothing to do.",
            RemediationDisposition.WarnFailing => "This plan has a non-zero force_failure_count; re-forcing may not help. Review sys.query_store_plan force_failure_reason.",
            RemediationDisposition.BlockQueryStoreOff => "Query Store is not READ_WRITE on this database — forcing is not possible.",
            RemediationDisposition.BlockStale => "The plan/query is no longer present in Query Store — the suggestion may be superseded.",
            RemediationDisposition.BlockNoAlter => NoAlterMessage(database),
            RemediationDisposition.BlockWrongDatabase => "Connected database does not match the target — apply will not proceed.",
            RemediationDisposition.BlockAuditTableAbsent => AuditAbsentMessage,
            _ => "Unable to determine target state."
        };

        private static string NoAlterMessage(string database) =>
            $"The monitoring login lacks ALTER on {database}. Map the monitoring login into {database} " +
            $"(CREATE USER [login] FOR LOGIN [login];) and GRANT ALTER — the same map-then-grant pattern " +
            $"the README documents for FinOps Index Analysis (it grants reads; Apply Fix needs ALTER) — or " +
            $"connect with a login that already has it. No elevation prompt is offered.";

        private const string AuditAbsentMessage =
            "This server is not on the 3.0.0 schema (config.remediation_action_log is absent). " +
            "Upgrade this server to 3.0.0 to enable audited Apply Fix; no change was made.";
    }
}

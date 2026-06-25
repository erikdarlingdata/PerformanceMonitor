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
    /// Handler for the DESTRUCTIVE RCSI fix (B3 Phase 3): enables
    /// <c>READ_COMMITTED_SNAPSHOT</c> on one target database via
    /// <c>ALTER DATABASE … SET READ_COMMITTED_SNAPSHOT ON</c>. This is the FIRST
    /// handler in the codebase to flip <see cref="IsDestructive"/> to true; it is
    /// gated behind the informed-consent (acknowledge-each-risk) dialog — the consent
    /// gate lives in <em>what makes the confirm callback return true</em>
    /// (RemediationApplyService is the trust boundary; the dialog enforces it).
    ///
    /// <para>
    /// Routed via the distinct fact key "RCSI" so it is never reachable through the
    /// always-safe <see cref="DbConfigHandler"/> (which keeps IsDestructive == false).
    /// It reuses the Phase-2 <c>DbConfigTarget</c> machinery (a single target with
    /// <see cref="DbConfigSetting.ReadCommittedSnapshotOn"/>) and the Phase-2
    /// self-gating executor unchanged: <c>exec.SetDatabaseOptionAsync</c> re-derives
    /// the authoritative gate (parameterized sys.databases existence + ALTER
    /// permission + live <c>is_read_committed_snapshot_on</c> freshness) and runs the
    /// ALTER on ONE monitoring connection. The handler cannot hand it a forged
    /// all-clear.
    /// </para>
    ///
    /// <para>
    /// APPLY-ONLY: <see cref="SupportsUnapply"/> is false (turning RCSI back OFF is
    /// itself destructive and would need its own symmetric gate);
    /// <see cref="UnapplyAsync"/> throws, and RemediationApplyService short-circuits
    /// any un-apply to a clean UnapplyNotSupported report before reaching it (m-C).
    /// One audit row is written per attempt with <c>consent_acknowledged = true</c>
    /// (the gate was satisfied to reach apply).
    /// </para>
    /// </summary>
    public sealed class RcsiHandler : IRemediationHandler
    {
        public string FactKey => "RCSI";

        // DESTRUCTIVE: enabling RCSI takes a brief exclusive DB lock, adds tempdb
        // version-store load, and changes reader/writer concurrency semantics. This
        // is the first true in the codebase; it requests the informed-consent gate.
        public bool IsDestructive => true;

        // Apply-only for v1: turning RCSI OFF is itself a destructive change (blocking
        // returns) and would need its own two-sided gate. prior_value is recorded for
        // manual reversal.
        public bool SupportsUnapply => false;

        public async Task<PreflightResult> PreflightAsync(RemediationAction action, IRemediationExecutor exec, CancellationToken ct)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (exec is null) throw new ArgumentNullException(nameof(exec));

            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);

            var targets = new List<TargetPreflight>();
            foreach (var t in DbTargets(action))
            {
                ct.ThrowIfCancellationRequested();

                TargetPreflight pf;
                try
                {
                    var probe = await exec.PreflightDbConfigAsync(t.Database, t.Setting, ct).ConfigureAwait(false);
                    pf = ToTargetPreflight(t, probe);
                }
                catch (Exception ex)
                {
                    pf = new TargetPreflight
                    {
                        Database = t.Database,
                        Disposition = RemediationDisposition.Error,
                        Message = ex.Message
                    };
                    targets.Add(pf);
                    continue;
                }

                // Audit-table-absent is a server-wide hard block; it overrides any
                // per-target disposition (no apply can be audited there).
                if (!auditTableExists)
                {
                    pf.Disposition = RemediationDisposition.BlockAuditTableAbsent;
                    pf.Message = AuditAbsentMessage;
                }

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

        // Apply-only. The UI gates Un-apply on SupportsUnapply==false, and
        // RemediationApplyService short-circuits an un-apply for a non-supporting
        // handler to a clean report (m-C) BEFORE any confirm or handler call, so this
        // is never reached in practice.
        public Task<ApplyResult> UnapplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
            => throw new NotSupportedException("RCSI is apply-only; turning READ_COMMITTED_SNAPSHOT back OFF is itself destructive and is not auto-reversed (the prior value is recorded for manual reversal).");

        private static async Task<ApplyResult> RunApplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
        {
            var outcomes = new List<TargetOutcome>();
            var dbTargets = DbTargets(action);

            // R2-MOD-2: audit-table-absent is a HARD BLOCK before any mutation. No
            // ALTER is attempted; no audit row is written (nowhere to write it).
            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);
            if (!auditTableExists)
            {
                foreach (var t in dbTargets)
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

            foreach (var t in dbTargets)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    // The executor re-derives the authoritative gate (existence + ALTER
                    // + live is_read_committed_snapshot_on freshness) and runs the ALTER
                    // on one connection — the handler cannot hand it a forged all-clear.
                    var outcome = await exec.SetDatabaseOptionAsync(t.Database, t.Setting, identity, ct).ConfigureAwait(false);

                    var written = await WriteAuditAsync(exec, identity, t, outcome.Status, outcome.Message, outcome.PriorValue, outcome.GeneratedSql, outcome.ExecutingLogin, ct).ConfigureAwait(false);

                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        Status = outcome.Status,
                        Message = outcome.Message,
                        ExecutingLogin = outcome.ExecutingLogin,
                        AuditWritten = written,
                        // O3: a real mutation that we failed to log against a present table.
                        AppliedButUnlogged = outcome.Applied && !written
                    });
                }
                catch (Exception ex)
                {
                    // Per-target independence: one target's failure never aborts the rest.
                    var record = BuildRecord(identity, t, RemediationStatus.Error, ex.Message, priorValue: t.CurrentValue, generatedSql: null, executingLogin: null);
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
            DbConfigTarget target,
            RemediationStatus status,
            string? message,
            string? priorValue,
            string? generatedSql,
            string? executingLogin,
            CancellationToken ct)
        {
            var record = BuildRecord(identity, target, status, message, priorValue, generatedSql, executingLogin);
            return await exec.WriteAuditAsync(record, ct).ConfigureAwait(false);
        }

        private static RemediationAuditRecord BuildRecord(
            RemediationIdentity identity,
            DbConfigTarget target,
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
                FactKey = "RCSI",
                QueryId = null,                 // RCSI rows have no query_id/plan_id
                PlanId = null,
                Action = "set_rcsi_on",
                PriorValue = priorValue ?? target.CurrentValue,
                GeneratedSql = generatedSql,
                Result = RemediationAuditHelpers.AuditResult(status),
                ErrorMessage = RemediationAuditHelpers.IsErrorish(status) ? message : null,
                // The informed-consent gate was satisfied to reach apply (B-3 / M-3):
                // a destructive RCSI attempt that got here passed every risk checkbox.
                ConsentAcknowledged = true,
                SourceAlertRef = identity.SourceAlertRef
            };

        private static TargetPreflight ToTargetPreflight(DbConfigTarget t, DbConfigPreflight probe)
        {
            var pf = new TargetPreflight
            {
                Database = t.Database,
                HasAlter = probe.HasAlter,
                CurrentDatabase = t.Database,
                ExecutingLogin = probe.ExecutingLogin
            };
            pf.Disposition = Classify(probe);
            pf.Message = DispositionMessage(pf.Disposition, t, probe);
            return pf;
        }

        private static RemediationDisposition Classify(DbConfigPreflight probe)
        {
            if (!probe.DatabaseExists) return RemediationDisposition.BlockDatabaseNotFound;
            if (!probe.HasAlter) return RemediationDisposition.BlockNoAlter;
            if (probe.AlreadyInDesiredState) return RemediationDisposition.AlreadyInDesiredState;
            return RemediationDisposition.Ok;
        }

        private static string DispositionMessage(RemediationDisposition d, DbConfigTarget t, DbConfigPreflight probe) => d switch
        {
            RemediationDisposition.Ok => $"Ready to enable {DbConfigHandler.SettingTitle(t.Setting)} on [{t.Database}] (currently {probe.CurrentValue}).",
            RemediationDisposition.AlreadyInDesiredState => "RCSI is already enabled — will be skipped.",
            RemediationDisposition.BlockDatabaseNotFound => "Database not found on the server — will not proceed.",
            RemediationDisposition.BlockNoAlter => $"The monitoring login lacks ALTER on {t.Database} — will fail closed (no change).",
            RemediationDisposition.BlockAuditTableAbsent => AuditAbsentMessage,
            _ => "Unable to determine target state."
        };

        private static IReadOnlyList<DbConfigTarget> DbTargets(RemediationAction action) =>
            action.DbConfigTargets ?? Array.Empty<DbConfigTarget>();

        private const string AuditAbsentMessage =
            "This server is not on the 3.0.0 schema (config.remediation_action_log is absent). " +
            "Upgrade this server to 3.0.0 to enable audited Apply Fix; no change was made.";
    }
}

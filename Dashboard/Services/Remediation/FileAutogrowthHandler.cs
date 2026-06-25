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
    /// Handler for FILE_AUTOGROWTH_PERCENT findings: switches each offending file from
    /// PERCENT autogrowth to a fixed-MB step via
    /// <c>ALTER DATABASE [db] MODIFY FILE (NAME = [logical], FILEGROWTH = N MB)</c>, one
    /// (database, file) target at a time. The fixed-MB step (N) is the already-computed
    /// <see cref="FileGrowthTarget.RecommendedGrowthMb"/> (1024 for data/ROWS, 64 for LOG)
    /// — the handler never recomputes it. This is a metadata-only, online,
    /// <b>non-destructive</b> change (same safety class as AUTO_SHRINK OFF), so this is
    /// APPLY-ONLY: there is no sensible reverse (you would never re-introduce percent
    /// autogrowth), <see cref="SupportsUnapply"/> is false (<see cref="UnapplyAsync"/>
    /// throws), and the prior value is recorded in the audit row for any manual reversal.
    ///
    /// <para>
    /// Structurally self-gating, exactly like <see cref="DbConfigHandler"/>:
    /// <see cref="ApplyAsync"/> takes no preflight disposition and trusts none. It
    /// hard-blocks every target when the audit table is absent (no mutation), then
    /// delegates each target to <c>exec.SetFileGrowthAsync</c>, which re-derives the
    /// authoritative gate (parameterized sys.databases + sys.master_files existence + ALTER
    /// permission + live freshness) and the ALTER on ONE connection. One audit row is
    /// written per attempt.
    /// </para>
    /// </summary>
    public sealed class FileAutogrowthHandler : IRemediationHandler
    {
        public string FactKey => "FILE_AUTOGROWTH_PERCENT";

        // MODIFY FILE FILEGROWTH is an always-safe online metadata change — not destructive.
        public bool IsDestructive => false;

        // Apply-only: there is no sensible reverse (you would never re-introduce percent
        // autogrowth); the prior growth value is recorded in the audit row for manual back-out.
        public bool SupportsUnapply => false;

        public async Task<PreflightResult> PreflightAsync(RemediationAction action, IRemediationExecutor exec, CancellationToken ct)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (exec is null) throw new ArgumentNullException(nameof(exec));

            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);

            var targets = new List<TargetPreflight>();
            foreach (var t in FileTargets(action))
            {
                ct.ThrowIfCancellationRequested();

                TargetPreflight pf;
                try
                {
                    var probe = await exec.PreflightFileGrowthAsync(t.Database, t.LogicalFileName, t.RecommendedGrowthMb, ct).ConfigureAwait(false);
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

        // Apply-only. Defensive: the UI gates Un-apply on SupportsUnapply==false, and
        // RemediationApplyService short-circuits an un-apply for a non-supporting handler to
        // a clean report (m-C), so this is never reached in practice.
        public Task<ApplyResult> UnapplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
            => throw new NotSupportedException("Percent-autogrowth fixes are apply-only; there is no un-apply for FILEGROWTH.");

        private static async Task<ApplyResult> RunApplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
        {
            var outcomes = new List<TargetOutcome>();
            var fileTargets = FileTargets(action);

            // Audit-table-absent is a HARD BLOCK before any mutation. No ALTER is attempted;
            // no audit row is written (nowhere to write it).
            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);
            if (!auditTableExists)
            {
                foreach (var t in fileTargets)
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

            foreach (var t in fileTargets)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    // The executor re-derives the authoritative gate and runs the ALTER on one
                    // connection — the handler cannot hand it a forged all-clear. The growth
                    // target is the already-computed RecommendedGrowthMb (never recomputed).
                    var outcome = await exec.SetFileGrowthAsync(t.Database, t.LogicalFileName, t.RecommendedGrowthMb, identity, ct).ConfigureAwait(false);

                    var written = await WriteAuditAsync(exec, identity, t, outcome.Status, outcome.Message, outcome.PriorValue, outcome.GeneratedSql, outcome.ExecutingLogin, ct).ConfigureAwait(false);

                    outcomes.Add(new TargetOutcome
                    {
                        Database = t.Database,
                        Status = outcome.Status,
                        Message = outcome.Message,
                        ExecutingLogin = outcome.ExecutingLogin,
                        AuditWritten = written,
                        // A real mutation that we failed to log against a present table.
                        AppliedButUnlogged = outcome.Applied && !written
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
            FileGrowthTarget target,
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
            FileGrowthTarget target,
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
                FactKey = "FILE_AUTOGROWTH_PERCENT",
                QueryId = null,                 // FILE_AUTOGROWTH rows have no query_id/plan_id
                PlanId = null,
                Action = "set_file_growth",
                // Prior growth ("N%"/"N MB") when the executor read it; the logical file name +
                // the exact statement ride in GeneratedSql (the audit record has no file column).
                PriorValue = priorValue,
                GeneratedSql = generatedSql,
                Result = RemediationAuditHelpers.AuditResult(status),
                ErrorMessage = RemediationAuditHelpers.IsErrorish(status) ? message : null,
                // The MODIFY FILE fix is not destructive — never went through the informed-consent
                // gate (regression-guard: explicit false).
                ConsentAcknowledged = false,
                SourceAlertRef = identity.SourceAlertRef
            };

        private static TargetPreflight ToTargetPreflight(FileGrowthTarget t, FileGrowthPreflight probe)
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

        private static RemediationDisposition Classify(FileGrowthPreflight probe)
        {
            if (!probe.DatabaseExists) return RemediationDisposition.BlockDatabaseNotFound;
            if (!probe.FileExists) return RemediationDisposition.BlockDatabaseNotFound;
            if (!probe.HasAlter) return RemediationDisposition.BlockNoAlter;
            if (probe.AlreadyInDesiredState) return RemediationDisposition.AlreadyInDesiredState;
            return RemediationDisposition.Ok;
        }

        private static string DispositionMessage(RemediationDisposition d, FileGrowthTarget t, FileGrowthPreflight probe) => d switch
        {
            RemediationDisposition.Ok => $"Ready to set FILEGROWTH = {t.RecommendedGrowthMb}MB on [{t.Database}].[{t.LogicalFileName}] (currently {probe.CurrentValue}).",
            RemediationDisposition.AlreadyInDesiredState => "Already at the recommended fixed-MB growth — will be skipped.",
            RemediationDisposition.BlockDatabaseNotFound => !probe.DatabaseExists
                ? "Database not found on the server — will not proceed."
                : $"Logical file '{t.LogicalFileName}' not found on [{t.Database}] — will not proceed.",
            RemediationDisposition.BlockNoAlter => $"The monitoring login lacks ALTER on {t.Database} — will fail closed (no change).",
            RemediationDisposition.BlockAuditTableAbsent => AuditAbsentMessage,
            _ => "Unable to determine target state."
        };

        private static IReadOnlyList<FileGrowthTarget> FileTargets(RemediationAction action) =>
            action.FileGrowthTargets ?? Array.Empty<FileGrowthTarget>();

        private const string AuditAbsentMessage =
            "This server is not on the 3.0.0 schema (config.remediation_action_log is absent). " +
            "Upgrade this server to 3.0.0 to enable audited Apply Fix; no change was made.";
    }
}

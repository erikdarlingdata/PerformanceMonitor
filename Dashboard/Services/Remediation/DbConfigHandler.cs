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
    /// Handler for DB_CONFIG findings: applies the three ALWAYS-SAFE database
    /// settings — AUTO_SHRINK OFF, AUTO_CLOSE OFF, PAGE_VERIFY CHECKSUM — one
    /// (database, setting) target at a time via <c>ALTER DATABASE … SET …</c>.
    /// RCSI is intentionally NOT in scope (destructive). This is APPLY-ONLY: there
    /// is no sensible reverse for these settings (<see cref="SupportsUnapply"/> is
    /// false; <see cref="UnapplyAsync"/> throws), and the prior value is recorded in
    /// the audit row for any manual reversal.
    ///
    /// <para>
    /// Structurally self-gating, exactly like <see cref="ForcePlanHandler"/>:
    /// <see cref="ApplyAsync"/> takes no preflight disposition and trusts none. It
    /// hard-blocks every target when the audit table is absent (no mutation), then
    /// delegates each target to <c>exec.SetDatabaseOptionAsync</c>, which re-derives
    /// the authoritative gate (parameterized sys.databases existence + ALTER
    /// permission + live freshness) and the ALTER on ONE connection. One audit row
    /// is written per attempt.
    /// </para>
    /// </summary>
    public sealed class DbConfigHandler : IRemediationHandler
    {
        public string FactKey => "DB_CONFIG";

        // The three settings are always-safe online metadata changes — not destructive.
        public bool IsDestructive => false;

        // Apply-only: these settings have no sensible reverse (you would never
        // re-enable AUTO_SHRINK/AUTO_CLOSE nor downgrade PAGE_VERIFY below CHECKSUM).
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

        // Apply-only. Defensive: the UI gates Un-apply on SupportsUnapply==false, and
        // RemediationApplyService short-circuits an un-apply for a non-supporting
        // handler to a clean report (m-C), so this is never reached in practice.
        public Task<ApplyResult> UnapplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
            => throw new NotSupportedException("DB-config fixes are apply-only; there is no un-apply for AUTO_SHRINK/AUTO_CLOSE/PAGE_VERIFY.");

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
                    // The executor re-derives the authoritative gate and runs the ALTER
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
                FactKey = "DB_CONFIG",
                QueryId = null,                 // DB_CONFIG rows have no query_id/plan_id (B-1)
                PlanId = null,
                Action = AuditAction(target.Setting),
                PriorValue = priorValue ?? target.CurrentValue,
                GeneratedSql = generatedSql,
                Result = RemediationAuditHelpers.AuditResult(status),
                ErrorMessage = RemediationAuditHelpers.IsErrorish(status) ? message : null,
                // The always-safe DB-config fixes are not destructive — never went
                // through the informed-consent gate (M-3 regression-guard: explicit false).
                ConsentAcknowledged = false,
                SourceAlertRef = identity.SourceAlertRef
            };

        /// <summary>The precise audit <c>action</c> taxonomy value per setting (fits varchar(32)).</summary>
        private static string AuditAction(DbConfigSetting setting) => setting switch
        {
            DbConfigSetting.AutoShrinkOff => "set_auto_shrink_off",
            DbConfigSetting.AutoCloseOff => "set_auto_close_off",
            DbConfigSetting.PageVerifyChecksum => "set_page_verify_checksum",
            _ => throw new ArgumentOutOfRangeException(nameof(setting), setting, "Unknown DbConfigSetting")
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
            RemediationDisposition.Ok => $"Ready to apply {SettingTitle(t.Setting)} on [{t.Database}] (currently {probe.CurrentValue}).",
            RemediationDisposition.AlreadyInDesiredState => "Already in the desired state — will be skipped.",
            RemediationDisposition.BlockDatabaseNotFound => "Database not found on the server — will not proceed.",
            RemediationDisposition.BlockNoAlter => $"The monitoring login lacks ALTER on {t.Database} — will fail closed (no change).",
            RemediationDisposition.BlockAuditTableAbsent => AuditAbsentMessage,
            _ => "Unable to determine target state."
        };

        /// <summary>Short human title for a setting (display only).</summary>
        public static string SettingTitle(DbConfigSetting setting) => setting switch
        {
            DbConfigSetting.AutoShrinkOff => "AUTO_SHRINK OFF",
            DbConfigSetting.AutoCloseOff => "AUTO_CLOSE OFF",
            DbConfigSetting.PageVerifyChecksum => "PAGE_VERIFY CHECKSUM",
            // m-2: an RCSI confirm row reuses DbConfigTargets, so without this arm the
            // status title would render the raw enum name "ReadCommittedSnapshotOn".
            DbConfigSetting.ReadCommittedSnapshotOn => "Read Committed Snapshot Isolation",
            _ => setting.ToString()
        };

        private static IReadOnlyList<DbConfigTarget> DbTargets(RemediationAction action) =>
            action.DbConfigTargets ?? Array.Empty<DbConfigTarget>();

        private const string AuditAbsentMessage =
            "This server is not on the 3.0.0 schema (config.remediation_action_log is absent). " +
            "Upgrade this server to 3.0.0 to enable audited Apply Fix; no change was made.";
    }
}

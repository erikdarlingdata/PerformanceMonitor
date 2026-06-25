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
    /// Handler for SERVER_CONFIG findings (WS3): applies the two ALWAYS-SAFE server-level settings
    /// — MAXDOP and Cost Threshold for Parallelism — one target at a time via
    /// <c>sp_configure</c>+<c>RECONFIGURE</c>. max/min server memory are ADVISE-ONLY: their right
    /// value is RAM/workload-dependent, so the handler records an advise-only outcome and NEVER runs
    /// sp_configure for them (the executor refuses them too — defense-in-depth). This is APPLY-ONLY
    /// for the executable settings: there is no sensible reverse (you would never re-introduce
    /// MAXDOP 0 / CTFP 5), <see cref="SupportsUnapply"/> is false, and the prior value is recorded
    /// in the audit row for any manual reversal.
    ///
    /// <para>
    /// Structurally self-gating, exactly like <see cref="DbConfigHandler"/> /
    /// <see cref="FileAutogrowthHandler"/>: <see cref="ApplyAsync"/> takes no preflight disposition
    /// and trusts none. It hard-blocks every target when the audit table is absent (no mutation),
    /// then delegates each executable target to <c>exec.SetServerConfigAsync</c>, which re-derives
    /// the authoritative gate (named ALTER SETTINGS / sysadmin / serveradmin + live freshness) and
    /// the sp_configure on ONE connection. One audit row is written per attempt.
    /// </para>
    /// </summary>
    public sealed class ServerConfigHandler : IRemediationHandler
    {
        public string FactKey => "SERVER_CONFIG";

        // sp_configure MAXDOP/CTFP + RECONFIGURE is an online metadata change — not destructive.
        public bool IsDestructive => false;

        // Apply-only: there is no sensible reverse (you would never re-introduce MAXDOP 0 / CTFP 5);
        // the prior value is recorded in the audit row for manual back-out.
        public bool SupportsUnapply => false;

        /// <summary>Sentinel for the NOT-NULL target_database column (server-config rows have no database).</summary>
        private const string ServerScopeSentinel = "(server)";

        public async Task<PreflightResult> PreflightAsync(RemediationAction action, IRemediationExecutor exec, CancellationToken ct)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (exec is null) throw new ArgumentNullException(nameof(exec));

            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);

            var targets = new List<TargetPreflight>();
            foreach (var t in ServerTargets(action))
            {
                ct.ThrowIfCancellationRequested();

                TargetPreflight pf;
                try
                {
                    var probe = await exec.PreflightServerConfigAsync(t.Setting, t.RecommendedValue, ct).ConfigureAwait(false);
                    pf = ToTargetPreflight(t, probe);
                }
                catch (Exception ex)
                {
                    pf = new TargetPreflight
                    {
                        Database = ServerScopeSentinel,
                        Disposition = RemediationDisposition.Error,
                        Message = ex.Message
                    };
                    targets.Add(pf);
                    continue;
                }

                // Audit-table-absent is a server-wide hard block; it overrides any per-target
                // disposition (no apply can be audited there). An advise-only target is never
                // applied, so the absent-table block does not apply to it — it stays AdviseOnly.
                if (!auditTableExists && IsExecutable(t.Setting))
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
        // RemediationApplyService short-circuits an un-apply for a non-supporting handler to a clean
        // report (m-C), so this is never reached in practice.
        public Task<ApplyResult> UnapplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
            => throw new NotSupportedException("Server-config fixes are apply-only; there is no un-apply for MAXDOP/CTFP.");

        private static async Task<ApplyResult> RunApplyAsync(RemediationAction action, IRemediationExecutor exec, RemediationIdentity identity, CancellationToken ct)
        {
            var outcomes = new List<TargetOutcome>();
            var serverTargets = ServerTargets(action);

            // Advise-only memory targets never mutate and never need the audit table; record their
            // outcome FIRST, independent of the audit-table state, so an absent table still yields a
            // clean "advise-only" result for them (not a spurious block).
            var executableTargets = new List<ServerConfigTarget>();
            foreach (var t in serverTargets)
            {
                if (IsExecutable(t.Setting))
                {
                    executableTargets.Add(t);
                    continue;
                }

                outcomes.Add(new TargetOutcome
                {
                    Database = ServerScopeSentinel,
                    Status = RemediationStatus.Skipped,
                    Message = AdviseOnlyMessage(t.Setting),
                    AuditWritten = false,        // nothing ran — nothing to audit
                    AppliedButUnlogged = false
                });
            }

            if (executableTargets.Count == 0)
                return new ApplyResult { Outcomes = outcomes };

            // R2-MOD-2: audit-table-absent is a HARD BLOCK before any executable mutation. No
            // sp_configure is attempted; no audit row is written (nowhere to write it).
            var auditTableExists = await exec.AuditTableExistsAsync(ct).ConfigureAwait(false);
            if (!auditTableExists)
            {
                foreach (var t in executableTargets)
                {
                    outcomes.Add(new TargetOutcome
                    {
                        Database = ServerScopeSentinel,
                        Status = RemediationStatus.Blocked,
                        Message = AuditAbsentMessage,
                        AuditWritten = false,
                        AppliedButUnlogged = false
                    });
                }
                return new ApplyResult { Outcomes = outcomes };
            }

            foreach (var t in executableTargets)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    // The executor re-derives the authoritative gate and runs sp_configure on one
                    // connection — the handler cannot hand it a forged all-clear. The value is the
                    // already-computed RecommendedValue (never recomputed here).
                    var outcome = await exec.SetServerConfigAsync(t.Setting, t.RecommendedValue, identity, ct).ConfigureAwait(false);

                    var written = await WriteAuditAsync(exec, identity, t, outcome.Status, outcome.Message, outcome.PriorValue, outcome.GeneratedSql, outcome.ExecutingLogin, ct).ConfigureAwait(false);

                    outcomes.Add(new TargetOutcome
                    {
                        Database = ServerScopeSentinel,
                        Status = outcome.Status,
                        Message = outcome.Message,
                        ExecutingLogin = outcome.ExecutingLogin,
                        AuditWritten = written,
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
                        Database = ServerScopeSentinel,
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
            ServerConfigTarget target,
            RemediationStatus status,
            string? message,
            long? priorValue,
            string? generatedSql,
            string? executingLogin,
            CancellationToken ct)
        {
            var record = BuildRecord(identity, target, status, message, priorValue, generatedSql, executingLogin);
            return await exec.WriteAuditAsync(record, ct).ConfigureAwait(false);
        }

        private static RemediationAuditRecord BuildRecord(
            RemediationIdentity identity,
            ServerConfigTarget target,
            RemediationStatus status,
            string? message,
            long? priorValue,
            string? generatedSql,
            string? executingLogin)
            => new()
            {
                OperatorIdentity = identity.OperatorIdentity,
                ExecutingLogin = executingLogin,
                TargetDatabase = ServerScopeSentinel,   // server-scoped; target_database is NOT NULL
                FactKey = "SERVER_CONFIG",
                QueryId = null,                          // SERVER_CONFIG rows have no query_id/plan_id
                PlanId = null,
                Action = AuditAction(target.Setting),
                PriorValue = priorValue?.ToString(System.Globalization.CultureInfo.InvariantCulture),
                GeneratedSql = generatedSql,
                Result = RemediationAuditHelpers.AuditResult(status),
                ErrorMessage = RemediationAuditHelpers.IsErrorish(status) ? message : null,
                // The MAXDOP/CTFP fix is not destructive — never went through the informed-consent
                // gate (regression-guard: explicit false).
                ConsentAcknowledged = false,
                SourceAlertRef = identity.SourceAlertRef
            };

        /// <summary>The precise audit <c>action</c> taxonomy value per setting (fits varchar(32)).</summary>
        private static string AuditAction(ServerConfigSetting setting) => setting switch
        {
            ServerConfigSetting.Maxdop => "set_maxdop",
            ServerConfigSetting.CostThreshold => "set_cost_threshold",
            ServerConfigSetting.MaxServerMemory => "advise_max_memory",
            ServerConfigSetting.MinServerMemory => "advise_min_memory",
            _ => throw new ArgumentOutOfRangeException(nameof(setting), setting, "Unknown ServerConfigSetting")
        };

        private static TargetPreflight ToTargetPreflight(ServerConfigTarget t, ServerConfigPreflight probe)
        {
            var pf = new TargetPreflight
            {
                Database = ServerScopeSentinel,
                HasAlter = probe.HasPermission,
                CurrentDatabase = ServerScopeSentinel,
                ExecutingLogin = probe.ExecutingLogin
            };
            pf.Disposition = Classify(t, probe);
            pf.Message = DispositionMessage(pf.Disposition, t, probe);
            return pf;
        }

        private static RemediationDisposition Classify(ServerConfigTarget t, ServerConfigPreflight probe)
        {
            if (!IsExecutable(t.Setting)) return RemediationDisposition.AdviseOnly;
            if (!probe.HasPermission) return RemediationDisposition.BlockNoAlter;
            if (probe.AlreadyInDesiredState) return RemediationDisposition.AlreadyInDesiredState;
            return RemediationDisposition.Ok;
        }

        private static string DispositionMessage(RemediationDisposition d, ServerConfigTarget t, ServerConfigPreflight probe) => d switch
        {
            RemediationDisposition.Ok => $"Ready to set {SettingTitle(t.Setting)} to {t.RecommendedValue} (currently {probe.CurrentValue}).",
            RemediationDisposition.AlreadyInDesiredState => "Already at the recommended value — will be skipped.",
            RemediationDisposition.BlockNoAlter => "The monitoring login lacks ALTER SETTINGS — will fail closed (no change).",
            RemediationDisposition.AdviseOnly => AdviseOnlyMessage(t.Setting),
            RemediationDisposition.BlockAuditTableAbsent => AuditAbsentMessage,
            _ => "Unable to determine target state."
        };

        /// <summary>Short human title for a server-config setting (display only).</summary>
        public static string SettingTitle(ServerConfigSetting setting) => setting switch
        {
            ServerConfigSetting.Maxdop => "MAXDOP",
            ServerConfigSetting.CostThreshold => "Cost Threshold for Parallelism",
            ServerConfigSetting.MaxServerMemory => "max server memory",
            ServerConfigSetting.MinServerMemory => "min server memory",
            _ => setting.ToString()
        };

        private static bool IsExecutable(ServerConfigSetting setting) =>
            setting is ServerConfigSetting.Maxdop or ServerConfigSetting.CostThreshold;

        private static string AdviseOnlyMessage(ServerConfigSetting setting) =>
            $"{SettingTitle(setting)} is advise-only — its correct value is RAM/workload-dependent and is not applied automatically. Copy the statement and set the value you choose.";

        private static IReadOnlyList<ServerConfigTarget> ServerTargets(RemediationAction action) =>
            action.ServerConfigTargets ?? Array.Empty<ServerConfigTarget>();

        private const string AuditAbsentMessage =
            "This server is not on the 3.0.0 schema (config.remediation_action_log is absent). " +
            "Upgrade this server to 3.0.0 to enable audited Apply Fix; no change was made.";
    }
}

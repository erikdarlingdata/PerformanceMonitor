/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// The single, UI-agnostic entry point the Dashboard uses to apply / un-apply a
    /// remediation. It is the ONLY non-core caller of the privileged remediation
    /// machinery (registry / handler / executor) — the UI binds to this facade and
    /// never touches those types directly, which is what keeps "reachable only
    /// through the gate" both true and statically checkable.
    ///
    /// <para>
    /// The confirm gate lives INSIDE this service: <see cref="RunAsync"/> invokes the
    /// operator's confirm callback and calls the handler's privileged
    /// <c>ApplyAsync</c>/<c>UnapplyAsync</c> ONLY when that callback returns true.
    /// There is no auto-apply, no apply-on-load, and no batch-without-confirm path.
    /// The read-only preflight that drives the modal is display only; the handler /
    /// executor re-derive the authoritative gate on the mutating connection (PR-A).
    /// </para>
    /// </summary>
    public sealed class RemediationApplyService
    {
        private readonly ServerManager _serverManager;
        private readonly RemediationHandlerRegistry _registry;
        private readonly Func<ServerConnection, IRemediationExecutor> _executorFactory;
        private readonly Func<ServerConnection, CancellationToken, Task<AuditWriteFailureKind>> _auditFailureClassifier;

        /// <summary>
        /// Production constructor. Builds the v1 registry (force-plan only) and wires
        /// the executor + audit-failure classifier over the existing per-server
        /// monitoring connection (no elevation — the connection is reused as-is).
        /// </summary>
        public RemediationApplyService(ServerManager serverManager, ICredentialService credentialService)
        {
            _serverManager = serverManager ?? throw new ArgumentNullException(nameof(serverManager));
            if (credentialService is null) throw new ArgumentNullException(nameof(credentialService));

            // B3 Phase 3 (PR-B): RcsiHandler is LIVE. Clear-cached-plan (PR-B): ClearPlanHandler
            // is now LIVE too. Both are IsDestructive; reaching either requires the informed-
            // consent (acknowledge-each-risk) confirm dialog returning true. Each is routed via
            // its OWN distinct fact key ("RCSI" / "CLEAR_PLAN") so neither can ever be reached
            // through the always-safe DbConfigHandler/ForcePlanHandler, and they cannot cross
            // each other (the registry keys on FactKey).
            // FileAutogrowthHandler (FILE_AUTOGROWTH_PERCENT) is always-safe (metadata-only,
            // online, non-destructive — same class as DbConfigHandler) and rides its own fact
            // key, so it never crosses the destructive RCSI/CLEAR_PLAN handlers.
            // ServerConfigHandler (SERVER_CONFIG, WS3) is always-safe too (sp_configure MAXDOP/CTFP
            // + RECONFIGURE — online metadata; the advise-only memory settings never mutate) and
            // rides its OWN fact key, so it never crosses the destructive handlers either.
            _registry = new RemediationHandlerRegistry(CreateDefaultHandlers());
            _executorFactory = server =>
                new DatabaseServiceRemediationExecutor(new DatabaseService(server.GetConnectionString(credentialService)));
            _auditFailureClassifier = (server, ct) =>
                AuditWritabilityProbe.ClassifyAsync(server.GetConnectionString(credentialService), ct);
        }

        /// <summary>
        /// Test seam (InternalsVisibleTo Dashboard.Tests): inject a fake registry,
        /// executor factory, and audit-failure classifier. Routes through the exact
        /// same gated <see cref="RunAsync"/>, so it cannot bypass the confirm gate.
        /// </summary>
        internal RemediationApplyService(
            ServerManager serverManager,
            RemediationHandlerRegistry registry,
            Func<ServerConnection, IRemediationExecutor> executorFactory,
            Func<ServerConnection, CancellationToken, Task<AuditWriteFailureKind>>? auditFailureClassifier = null)
        {
            _serverManager = serverManager;
            _registry = registry;
            _executorFactory = executorFactory;
            _auditFailureClassifier = auditFailureClassifier
                ?? ((_, _) => Task.FromResult(AuditWriteFailureKind.Unknown));
        }

        /// <summary>
        /// The production set of remediation handlers — one per Apply-able fact key. Extracted from
        /// the constructor so a contract test (InternalsVisibleTo Dashboard.Tests) can assert the
        /// registered handler keys match the set of fact keys the FactRemediation builders / the
        /// recommendations reader produce. A builder fact key with no handler here makes Apply
        /// silently no-op; a handler with no producing builder is dead. Order is irrelevant — the
        /// registry keys on FactKey — but every handler MUST expose a distinct, non-empty FactKey.
        /// </summary>
        internal static IRemediationHandler[] CreateDefaultHandlers() =>
            new IRemediationHandler[]
            {
                new ForcePlanHandler(),
                new DbConfigHandler(),
                new RcsiHandler(),
                new ClearPlanHandler(),
                new FileAutogrowthHandler(),
                new ServerConfigHandler(),
            };

        /// <summary>
        /// Whether a registered handler exists for this fact key (one half of the
        /// UI's Apply-affordance gate; the other half is unambiguous server
        /// resolution). Null / unknown fact keys yield no Apply button.
        /// </summary>
        public bool HasHandlerFor(string? factKey) => _registry.TryGet(factKey) is not null;

        /// <summary>
        /// M3 fail-closed server resolution. GUID match first; on a miss (incl. the
        /// int-id fallback / legacy / empty ServerId) fall back to a UNIQUE
        /// ServerName match; ambiguous (&gt;1) or unresolved (0) yields no server and
        /// a reason for the disabled-Apply tooltip. Never silently picks a server.
        /// </summary>
        public ServerResolution ResolveServer(string? serverId, string serverName)
            => ResolveServer(serverId, serverName, _serverManager.GetAllServers());

        /// <summary>Pure resolution logic (unit-testable without a live ServerManager).</summary>
        public static ServerResolution ResolveServer(string? serverId, string serverName, IReadOnlyList<ServerConnection> allServers)
        {
            if (allServers is null)
                return new ServerResolution { Reason = "No servers are configured." };

            // 1. Exact GUID match (the normal path for alerts produced by the GUID resolver).
            if (!string.IsNullOrEmpty(serverId))
            {
                var byId = allServers.FirstOrDefault(s => string.Equals(s.Id, serverId, StringComparison.Ordinal));
                if (byId is not null)
                    return new ServerResolution { Server = byId };
            }

            // 2/3. GUID miss (incl. the int-id fallback from MainWindow's notify-time
            // resolver, and legacy/empty ServerId) -> resolve by a UNIQUE ServerName.
            var byName = allServers
                .Where(s => string.Equals(s.ServerName, serverName, StringComparison.OrdinalIgnoreCase))
                .ToList();

            if (byName.Count == 1)
                return new ServerResolution { Server = byName[0], ResolvedByName = true };

            // 4. Ambiguous or unresolved -> FAIL CLOSED. This is the wrong-server boundary.
            var reason = byName.Count > 1
                ? $"Cannot unambiguously resolve the source server for this alert: " +
                  $"{byName.Count} configured servers are named \"{serverName}\". Apply is disabled."
                : "Cannot resolve the source server for this alert (it may have been renamed or " +
                  "removed since the alert fired). Apply is disabled.";
            return new ServerResolution { Reason = reason };
        }

        /// <summary>
        /// Apply a remediation against a resolved server. Runs read-only preflight,
        /// presents the confirm request, and — ONLY if the operator confirms —
        /// invokes the privileged handler.
        /// </summary>
        public Task<RemediationRunReport> ApplyAsync(
            RemediationAction action,
            ServerConnection server,
            string? previewSql,
            string operatorIdentity,
            string? sourceAlertRef,
            Func<RemediationConfirmRequest, Task<bool>> confirm,
            CancellationToken ct,
            AnalysisFinding? finding = null)
            => RunAsync(action, server, previewSql, operatorIdentity, sourceAlertRef, confirm, isUnapply: false, ct, finding);

        /// <summary>Un-apply (unforce) a previously applied remediation. Same gated shape as Apply.</summary>
        public Task<RemediationRunReport> UnapplyAsync(
            RemediationAction action,
            ServerConnection server,
            string operatorIdentity,
            string? sourceAlertRef,
            Func<RemediationConfirmRequest, Task<bool>> confirm,
            CancellationToken ct,
            AnalysisFinding? finding = null)
            => RunAsync(action, server, previewSql: null, operatorIdentity, sourceAlertRef, confirm, isUnapply: true, ct, finding);

        private async Task<RemediationRunReport> RunAsync(
            RemediationAction action,
            ServerConnection server,
            string? previewSql,
            string operatorIdentity,
            string? sourceAlertRef,
            Func<RemediationConfirmRequest, Task<bool>> confirm,
            bool isUnapply,
            CancellationToken ct,
            AnalysisFinding? finding = null)
        {
            if (action is null) throw new ArgumentNullException(nameof(action));
            if (server is null) throw new ArgumentNullException(nameof(server));
            if (confirm is null) throw new ArgumentNullException(nameof(confirm));

            var handler = _registry.TryGet(action.FactKey);
            if (handler is null)
                return new RemediationRunReport { Status = RemediationRunStatus.NoHandler, IsUnapply = isUnapply };

            // m-C fail-safe: an un-apply for a handler that doesn't support it (e.g.
            // a future mis-wired DB_CONFIG un-apply) short-circuits to a clean report
            // BEFORE any confirm or handler call — never bubbles NotSupportedException.
            // The privileged action is a no-op either way; this removes the only path
            // that turns the guard violation into a crash.
            if (isUnapply && !handler.SupportsUnapply)
                return new RemediationRunReport { Status = RemediationRunStatus.UnapplyNotSupported, IsUnapply = true };

            var exec = _executorFactory(server);

            // Read-only preflight: DISPLAY DRIVER ONLY. Populates the confirm modal's
            // per-target dispositions and executing identity. It is NOT the gate —
            // the handler/executor re-derive the authoritative gate (correct-DB +
            // ALTER + freshness) on the mutating connection (PR-A, R2-MOD-1).
            var preflight = await handler.PreflightAsync(action, exec, ct).ConfigureAwait(false);

            var preview = string.IsNullOrEmpty(previewSql)
                ? RenderPreview(action, isUnapply)
                : previewSql!;

            // B-1: thread the resolved handler (and the finding) in so the request can
            // set RequiresInformedConsent = handler.IsDestructive and, when destructive,
            // the two-sided Risks. This is a signature change, not a one-liner.
            var request = BuildConfirmRequest(action, server, preview, operatorIdentity, isUnapply, preflight, handler, finding);

            // ── THE GATE ──────────────────────────────────────────────────────────
            // The privileged handler.ApplyAsync/UnapplyAsync below is reached ONLY
            // when the operator's confirm callback returns true. This is the single
            // sanctioned path to the executor; there is no automatic, on-load, or
            // batch-without-confirm route.
            var confirmed = await confirm(request).ConfigureAwait(false);
            if (!confirmed)
                return new RemediationRunReport { Status = RemediationRunStatus.NotConfirmed, IsUnapply = isUnapply };

            var identity = new RemediationIdentity(operatorIdentity, sourceAlertRef);
            var result = isUnapply
                ? await handler.UnapplyAsync(action, exec, identity, ct).ConfigureAwait(false)
                : await handler.ApplyAsync(action, exec, identity, ct).ConfigureAwait(false);

            var targets = new List<RemediationTargetReport>(result.Outcomes.Count);
            foreach (var o in result.Outcomes)
            {
                // LOW-2: only an applied-but-unlogged target needs the permanent-vs-
                // transient classification; everything else is None.
                var failureKind = AuditWriteFailureKind.None;
                if (o.AppliedButUnlogged)
                    failureKind = await _auditFailureClassifier(server, ct).ConfigureAwait(false);

                targets.Add(new RemediationTargetReport
                {
                    Database = o.Database,
                    QueryId = o.QueryId,
                    PlanId = o.PlanId,
                    Status = o.Status,
                    Message = o.Message,
                    AuditWritten = o.AuditWritten,
                    AppliedButUnlogged = o.AppliedButUnlogged,
                    AuditFailureKind = failureKind
                });
            }

            return new RemediationRunReport
            {
                Status = RemediationRunStatus.Ran,
                IsUnapply = isUnapply,
                Targets = targets
            };
        }

        private static RemediationConfirmRequest BuildConfirmRequest(
            RemediationAction action,
            ServerConnection server,
            string preview,
            string operatorIdentity,
            bool isUnapply,
            PreflightResult preflight,
            IRemediationHandler handler,
            AnalysisFinding? finding)
        {
            // Preflight targets align 1:1 with action targets (each handler builds them
            // in order). Match by index; tolerate a short preflight list defensively.
            // Branch on which target list is populated: DB_CONFIG carries
            // DbConfigTargets (action.Targets is empty for it), force-plan carries Targets.
            List<RemediationConfirmTarget> confirmTargets;
            if (action.ClearPlanTargets is { Count: > 0 } clearTargets)
            {
                // CLEAR_PLAN rides ClearPlanTargets (action.Targets and DbConfigTargets are
                // empty for it). Each row is one abnormal query hash; the per-handle list
                // (database + query_text snippet) is RESOLVED LIVE at apply (§1/§4 — the
                // handle set is not known until then), so the confirm preview shows the
                // per-query identity + anomaly figures the detector captured. The two-sided
                // Risks (below) carry the per-handle blast-radius framing the operator must ack.
                confirmTargets = new List<RemediationConfirmTarget>(clearTargets.Count);
                for (var i = 0; i < clearTargets.Count; i++)
                {
                    var t = clearTargets[i];
                    var pf = i < preflight.Targets.Count ? preflight.Targets[i] : null;
                    var dbLabel = string.IsNullOrEmpty(t.Database) ? "(server-wide)" : t.Database;
                    var title = t.AnomalyRatio > 0
                        ? $"[{dbLabel}] query hash {t.QueryHash} — ~{t.AnomalyRatio.ToString("0.0", CultureInfo.InvariantCulture)}x per-exec CPU (live-resolve at apply)"
                        : $"[{dbLabel}] query hash {t.QueryHash} (live-resolve at apply)";
                    confirmTargets.Add(new RemediationConfirmTarget
                    {
                        Database = t.Database,
                        StatusTitle = title,
                        Disposition = pf?.Disposition ?? RemediationDisposition.Error,
                        DispositionMessage = pf?.Message
                    });
                }
            }
            else if (action.DbConfigTargets is { Count: > 0 } dbTargets)
            {
                confirmTargets = new List<RemediationConfirmTarget>(dbTargets.Count);
                for (var i = 0; i < dbTargets.Count; i++)
                {
                    var t = dbTargets[i];
                    var pf = i < preflight.Targets.Count ? preflight.Targets[i] : null;
                    confirmTargets.Add(new RemediationConfirmTarget
                    {
                        Database = t.Database,
                        StatusTitle = $"[{t.Database}] {DbConfigHandler.SettingTitle(t.Setting)} — was {t.CurrentValue}",
                        Disposition = pf?.Disposition ?? RemediationDisposition.Error,
                        DispositionMessage = pf?.Message
                    });
                }
            }
            else if (action.FileGrowthTargets is { Count: > 0 } fileTargets)
            {
                // FILE_AUTOGROWTH_PERCENT rides FileGrowthTargets (action.Targets / DbConfigTargets
                // are empty for it). One row per offending file; the always-safe MODIFY FILE fix
                // is non-destructive, so no two-sided Risks (RequiresInformedConsent stays false).
                confirmTargets = new List<RemediationConfirmTarget>(fileTargets.Count);
                for (var i = 0; i < fileTargets.Count; i++)
                {
                    var t = fileTargets[i];
                    var pf = i < preflight.Targets.Count ? preflight.Targets[i] : null;
                    confirmTargets.Add(new RemediationConfirmTarget
                    {
                        Database = t.Database,
                        StatusTitle = $"[{t.Database}] {t.LogicalFileName} FILEGROWTH → {t.RecommendedGrowthMb}MB",
                        Disposition = pf?.Disposition ?? RemediationDisposition.Error,
                        DispositionMessage = pf?.Message
                    });
                }
            }
            else if (action.ServerConfigTargets is { Count: > 0 } serverTargets)
            {
                // SERVER_CONFIG (WS3) rides ServerConfigTargets (the other target lists are empty for
                // it). One row per setting; the always-safe sp_configure fix is non-destructive, so no
                // two-sided Risks (RequiresInformedConsent stays false). Memory targets render with an
                // advise-only disposition (they never run).
                confirmTargets = new List<RemediationConfirmTarget>(serverTargets.Count);
                for (var i = 0; i < serverTargets.Count; i++)
                {
                    var t = serverTargets[i];
                    var pf = i < preflight.Targets.Count ? preflight.Targets[i] : null;
                    confirmTargets.Add(new RemediationConfirmTarget
                    {
                        Database = "",   // server-scoped — no database column
                        StatusTitle = $"{ServerConfigHandler.SettingTitle(t.Setting)} {t.CurrentValue} → {t.RecommendedValue}",
                        Disposition = pf?.Disposition ?? RemediationDisposition.Error,
                        DispositionMessage = pf?.Message
                    });
                }
            }
            else
            {
                confirmTargets = new List<RemediationConfirmTarget>(action.Targets.Count);
                for (var i = 0; i < action.Targets.Count; i++)
                {
                    var t = action.Targets[i];
                    var pf = i < preflight.Targets.Count ? preflight.Targets[i] : null;
                    confirmTargets.Add(new RemediationConfirmTarget
                    {
                        Database = t.Database,
                        QueryId = t.QueryId,
                        PlanId = t.PlanId,
                        RegressionFactor = t.RegressionFactor,
                        Disposition = pf?.Disposition ?? RemediationDisposition.Error,
                        DispositionMessage = pf?.Message
                    });
                }
            }

            var executingLogin = preflight.Targets
                .Select(p => p.ExecutingLogin)
                .FirstOrDefault(l => !string.IsNullOrEmpty(l));

            // B-1: the consent gate is keyed on the resolved handler's IsDestructive.
            // For a destructive action, attach the two-sided risk disclosure (the
            // dialog renders the acknowledge-each-risk checkboxes in PR-B). For a
            // non-destructive action both stay default (false / null) — single-confirm
            // path unchanged.
            var requiresConsent = handler.IsDestructive;
            var risks = requiresConsent ? FactRiskDisclosure.GetForAction(action, finding) : null;

            return new RemediationConfirmRequest
            {
                ServerDisplayName = string.IsNullOrEmpty(server.DisplayName) ? server.ServerName : server.DisplayName,
                IsUnapply = isUnapply,
                FactKey = action.FactKey,
                PreviewSql = preview,
                OperatorIdentity = operatorIdentity,
                ExecutingLogin = executingLogin,
                Targets = confirmTargets,
                AuditTableExists = preflight.AuditTableExists,
                RequiresInformedConsent = requiresConsent,
                Risks = risks
            };
        }

        /// <summary>
        /// Canonical EXEC preview rendered from the typed targets — exactly the
        /// statement that will run (matches the audited generated_sql). Used for the
        /// un-apply modal, and as a fallback when no code-block preview is supplied.
        /// </summary>
        private static string RenderPreview(RemediationAction action, bool isUnapply)
        {
            // DB_CONFIG: render the ALTER DATABASE statements (apply-only — there is no
            // un-apply branch). Display only; the executor builds its own validated +
            // bracketed statement and never executes this text. Reaches here only as a
            // fallback when no code-block preview was supplied.
            if (action.DbConfigTargets is { Count: > 0 } dbTargets)
            {
                var sb2 = new StringBuilder();
                foreach (var t in dbTargets)
                {
                    sb2.Append("ALTER DATABASE ").Append(QuoteIdentifier(t.Database))
                       .Append(' ').Append(SetClauseFor(t.Setting))
                       .Append(";   -- was ").Append(t.CurrentValue).Append('\n');
                }
                return sb2.ToString().TrimEnd('\n');
            }

            // FILE_AUTOGROWTH_PERCENT: render one ALTER DATABASE … MODIFY FILE per file (apply-only
            // — no un-apply branch). Display only; the executor builds its own validated +
            // bracketed statement and never executes this text. The shared QUOTENAME-safe renderer
            // keeps it byte-identical to the drill-down / reader copy-paste.
            if (action.FileGrowthTargets is { Count: > 0 } fileTargets)
            {
                var sb3 = new StringBuilder();
                foreach (var t in fileTargets)
                {
                    sb3.Append(FactRemediation.BuildModifyFileStatement(t.Database, t.LogicalFileName, t.RecommendedGrowthMb))
                       .Append('\n');
                }
                return sb3.ToString().TrimEnd('\n');
            }

            // SERVER_CONFIG: render one sp_configure + RECONFIGURE per setting (apply-only — no
            // un-apply branch). Display only; the executor builds its own batch with a bound @value
            // and never executes this text. The shared renderer keeps it byte-identical to the
            // reader copy-paste.
            if (action.ServerConfigTargets is { Count: > 0 } serverTargets)
            {
                var sb4 = new StringBuilder();
                foreach (var t in serverTargets)
                {
                    sb4.Append(FactRemediation.BuildSpConfigureStatement(t.Setting, t.RecommendedValue))
                       .Append('\n');
                }
                return sb4.ToString().TrimEnd('\n');
            }

            var proc = isUnapply ? "sp_query_store_unforce_plan" : "sp_query_store_force_plan";
            var sb = new StringBuilder();
            foreach (var t in action.Targets)
            {
                sb.Append("-- [").Append(t.Database).Append("]\n");
                sb.Append("EXEC sys.").Append(proc)
                  .Append(" @query_id = ").Append(t.QueryId)
                  .Append(", @plan_id = ").Append(t.PlanId).Append(";\n");
            }
            return sb.ToString().TrimEnd('\n');
        }

        // Display-only bracketing for the fallback preview (mirrors the executor's
        // QUOTENAME doubling). The executed statement is built by the executor, not here.
        private static string QuoteIdentifier(string identifier) =>
            "[" + identifier.Replace("]", "]]") + "]";

        private static string SetClauseFor(DbConfigSetting setting) => setting switch
        {
            DbConfigSetting.AutoShrinkOff => "SET AUTO_SHRINK OFF",
            DbConfigSetting.AutoCloseOff => "SET AUTO_CLOSE OFF",
            DbConfigSetting.PageVerifyChecksum => "SET PAGE_VERIFY CHECKSUM",
            DbConfigSetting.ReadCommittedSnapshotOn => "SET READ_COMMITTED_SNAPSHOT ON",
            _ => "SET /* unknown */"
        };
    }
}

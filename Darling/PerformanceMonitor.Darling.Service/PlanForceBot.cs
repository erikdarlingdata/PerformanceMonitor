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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The auto force-plan bot's orchestration (#2138 phase 1): runs after each scheduled analysis pass,
/// re-judges the pass's force-plan targets through the SAME policy gate agents inspect on the MCP
/// surfaces (<see cref="FactRemediation.ForcePlanBlockers"/> feeding
/// <see cref="ForcePlanBotPolicy.Evaluate"/>), and journals every decision to
/// <c>collect.plan_force_actions</c> with the evidence that produced it.
///
/// <para><b>Phase 1 cannot write to a monitored server, structurally.</b> This class holds no
/// <see cref="IPlanForceExecutor"/> — no field, no constructor parameter, no factory — and no
/// implementation of that interface ships in this build (see the interface's remarks and
/// <c>PlanForceNoWritePathTests</c>). So the bot's whole output is journal rows: would-force,
/// blocked-with-named-reasons, and — if an operator opens all three gates on a build that has no
/// write path — a force decision journaled as WITHHELD. The write path, the self-review's execution
/// arm and their tests are #2731.</para>
///
/// <para>Shipped state is off anyway: globally OFF, and when enabled, DRY RUN. Dry run is not a
/// separate code path — it is the same policy, cooldowns and budget with the executor absent — so the
/// shadow-mode journal rows ARE the decisions a live bot would have executed (Erik's dogfood plan on
/// the issue: score the would-force ledger before any write path is armed).</para>
///
/// <para>Failure-isolated at every seam: a journal fault logs and moves on — the bot must never take
/// an analysis pass or a sweep down with it.</para>
/// </summary>
public sealed class PlanForceBot
{
    /* Belt over the extractor's per-finding cap: the pass evaluates at most this many targets, so a
       pathological drill-down can never turn one analysis pass into a journal flood. */
    internal const int MaxTargetsPerPass = 10;

    private readonly IPlanForceActionStore _store;
    private readonly ForcePlanBotSettings _settings;
    private readonly ILogger _logger;

    public PlanForceBot(
        IPlanForceActionStore store,
        ForcePlanBotSettings settings,
        ILogger logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _settings = (settings ?? throw new ArgumentNullException(nameof(settings))).Normalize();
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>Whether the evaluation pass runs at all.</summary>
    public bool Enabled => _settings.Enabled;

    /// <summary>
    /// One post-analysis bot pass for one server: evaluate the pass's force-plan targets and journal
    /// the verdicts. <paramref name="currentConfig"/> is the server's CURRENT registry view (the
    /// reload-swapped <c>ServerLoopState.Config</c>, not the connect-time snapshot on the runtime),
    /// so revoking the per-server opt-in takes effect on the next pass, not the next reconnect.
    /// </summary>
    public async Task RunAfterAnalysisAsync(
        ServerRuntime runtime,
        MonitoredServer currentConfig,
        IReadOnlyList<AnalysisFinding> findings,
        CancellationToken ct)
    {
        if (runtime is null || currentConfig is null)
        {
            return;
        }

        if (!_settings.Enabled)
        {
            return;
        }

        /* The engine seam (#2213's lesson): this bot reasons about Query Store plan forcing, which is
           a SQL Server concept, and its journal rows claim a SQL Server (database, query_id, plan_id)
           identity. PLAN_REGRESSION cannot fire for a PostgreSQL target today, but the gate lives
           HERE — at the boundary a write would eventually cross — rather than relying on the upstream
           fact never learning to. */
        if (currentConfig.IsPostgres || runtime.Config.IsPostgres)
        {
            return;
        }

        try
        {
            await EvaluateTargetsAsync(runtime, currentConfig, findings, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "[{Server}] Force-plan bot evaluation pass failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
        }
    }

    private async Task EvaluateTargetsAsync(
        ServerRuntime runtime,
        MonitoredServer currentConfig,
        IReadOnlyList<AnalysisFinding> findings,
        CancellationToken ct)
    {
        if (findings is not { Count: > 0 })
        {
            return;
        }

        var nowUtc = DateTime.UtcNow;
        var seen = new HashSet<(string Database, long QueryId)>();
        var evaluated = 0;

        foreach (var finding in findings)
        {
            if (finding?.Remediation is not { FactKey: "PLAN_REGRESSION" } remediation ||
                remediation.Targets is not { Count: > 0 } targets)
            {
                continue;
            }

            foreach (var target in targets)
            {
                if (evaluated >= MaxTargetsPerPass)
                {
                    return;
                }

                if (!seen.Add((target.Database, target.QueryId)))
                {
                    continue;
                }

                evaluated++;

                /* THE shared gate: the same function that fills structured_remediation's blockers
                   on the MCP surfaces. Never recomputed locally, so advise and act cannot drift. */
                var blockers = FactRemediation.ForcePlanBlockers(target);
                var history = await _store.GetQueryHistoryAsync(
                    runtime.ServerId, target.Database, target.QueryId, _settings, nowUtc, ct);

                var decision = ForcePlanBotPolicy.Evaluate(
                    target, blockers, currentConfig.PlanForceBotEnabled, _settings, history, nowUtc);

                switch (decision.Kind)
                {
                    case ForcePlanBotDecisionKind.Suppressed:
                        continue;

                    case ForcePlanBotDecisionKind.Blocked:
                        await _store.JournalAsync(BuildRecord(
                            runtime, target, PgPlanForceActionStore.ActionBlocked, decision.Reasons,
                            PgPlanForceActionStore.OutcomeLogged, detail: null, nowUtc), ct);
                        continue;

                    case ForcePlanBotDecisionKind.WouldForce:
                        await _store.JournalAsync(BuildRecord(
                            runtime, target, PgPlanForceActionStore.ActionWouldForce, decision.Reasons,
                            PgPlanForceActionStore.OutcomeLogged, detail: null, nowUtc), ct);
                        continue;

                    case ForcePlanBotDecisionKind.Force:
                        await JournalWithheldForceAsync(runtime, target, nowUtc, ct);
                        continue;
                }
            }
        }
    }

    /// <summary>
    /// Every gate the operator controls is open — and this build still has no write path, so the
    /// decision is journaled as WITHHELD rather than executed.
    ///
    /// <para>Journaling it (rather than quietly downgrading it to would_force) is the point: an
    /// operator who has opened all three gates believes the bot is live, and the trail has to say
    /// out loud that it is not. The row spends the same per-server daily budget and per-query
    /// cooldown a live force would, so the shadow ledger stays a faithful rehearsal; it does NOT
    /// count as a failed force (nothing failed), and it can never surface as an outstanding force
    /// owed a self-review, because no plan was pinned.</para>
    /// </summary>
    private async Task JournalWithheldForceAsync(
        ServerRuntime runtime, ForcePlanTarget target, DateTime nowUtc, CancellationToken ct)
    {
        await _store.JournalAsync(BuildRecord(
            runtime, target, PgPlanForceActionStore.ActionForce, Array.Empty<string>(),
            PgPlanForceActionStore.OutcomeWithheld,
            detail: "no write path in this build (#2138 phase 1 is detection, evidence and dry-run only)",
            nowUtc), ct);

        _logger.LogWarning(
            /* The statement an operator would run is deliberately NOT rendered here. The analysis
               finding's own remediation script already carries it (FactRemediation, which is where
               hand-run T-SQL belongs), and keeping every force/unforce statement out of the service
               assembly is what PlanForceNoWritePathTests pins — a log-line copy would defeat the pin
               for the sake of duplicating text the operator already has. */
            "[{Server}] Force-plan bot would have FORCED plan {PlanId} for query {QueryId} in {Database} (regression {Factor:F1}x) with every gate open — WITHHELD: this build ships no write path (#2138 phase 1). The finding's remediation script has the statement if you mean to run it by hand.",
            runtime.Config.DisplayName, target.PlanId, target.QueryId, target.Database,
            target.RegressionFactor);
    }

    private PlanForceActionRecord BuildRecord(
        ServerRuntime runtime,
        ForcePlanTarget target,
        string action,
        IReadOnlyList<string> reasons,
        string outcome,
        string? detail,
        DateTime nowUtc) => new(
            ActionId: 0,
            ActionTimeUtc: nowUtc,
            ServerId: runtime.ServerId,
            ServerName: runtime.StorageName,
            DatabaseName: target.Database,
            QueryId: target.QueryId,
            PlanId: target.PlanId,
            Action: action,
            Mode: _settings.DryRun ? PgPlanForceActionStore.ModeDryRun : PgPlanForceActionStore.ModeLive,
            Decision: action,
            Reasons: string.Join(",", reasons),
            RegressionFactor: target.RegressionFactor,
            LatestCpuPerExecUs: target.LatestCpuPerExecUs,
            BestCpuPerExecUs: target.BestCpuPerExecUs,
            ReplicaRole: target.ReplicaRole,
            ParameterSensitivityCoFired: target.ParameterSensitivityCoFired,
            Outcome: outcome,
            Detail: detail,
            RelatedActionId: null);
}

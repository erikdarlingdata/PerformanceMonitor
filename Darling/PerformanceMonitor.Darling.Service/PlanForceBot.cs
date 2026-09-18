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
/// surfaces (<see cref="FactRemediation.ForcePlanBlockers(ForcePlanTarget, ForcePlanTargetState?)"/>,
/// both halves, wrapped by <see cref="ForcePlanBotPolicy.Blockers"/> and feeding
/// <see cref="ForcePlanBotPolicy.Evaluate"/>), and journals every decision to
/// <c>collect.plan_force_actions</c> with the evidence that produced it.
///
/// <para><b>The state half is read here, once per pass (#3654).</b> The gate's second half needs what
/// the store knows about each target's forcing and automatic-plan-correction state NOW — the same
/// <c>query_store_stats</c> / <c>plan_correction</c> read the MCP tools make (#3652), batched into one
/// statement for the pass's whole candidate list through the store seam — and the bot judges each
/// target with it. Without it the bot's gate was the two #2138 blockers only, and on the five live
/// targets #3652 cross-referenced it would have judged all five unblocked.</para>
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
       pathological drill-down can never turn one analysis pass into a journal flood.

       It counts targets EVALUATED, not rows journaled, and that is the load-bearing choice: each
       evaluated target costs a store round trip (GetQueryHistoryAsync) whatever the verdict turns out
       to be — plus, since #3654, the pass costs ONE more for all of them together (the batched state
       read, sized by this same cap) — so the cap bounds the WORK a pass can do, not just its output.
       Counting only journaled rows would let a pass whose targets are all inside their cooldown spend
       an unbounded number of history reads for an empty journal — the one shape the budget exists to
       stop. The cost is that
       a suppressed target can occupy a slot a later actionable one wanted; that is acceptable because
       FactRemediation.ExtractPlanRegressionTargets already caps each finding at 5 targets ordered
       worst-regression-first, so the actionable ones are at the front of the list by construction. If
       that per-finding cap ever rises, revisit this ordering assumption rather than this number. */
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
        var candidates = CollectCandidates(findings);
        if (candidates.Count == 0)
        {
            return;
        }

        /* The gate's STATE half, read once for the whole pass (#3654). One statement for every candidate
           rather than one per target — the read is one server's day of query_store_stats hashed on the
           targets, and ten of those where one would do is the shape the per-pass cap exists to prevent.
           Read BEFORE the per-target loop so every target in the pass is judged against the same snapshot;
           a failure comes back as a reason, never an exception, because for this caller an unreadable
           state is a verdict (below), not a fault to log and skip past. */
        var (states, stateUnavailableReason) = await _store.TryGetTargetStatesAsync(
            runtime.ServerId, candidates, nowUtc, ct);

        foreach (var target in candidates)
        {
            ForcePlanTargetState? state = null;
            if (states is not null)
            {
                states.TryGetValue(ForcePlanTargetKey.Of(target), out state);
            }

            /* THE shared gate, whole: the same two-argument FactRemediation.ForcePlanBlockers that fills
               structured_remediation's blockers and blocker_evidence on the MCP surfaces (#3652), never
               recomputed locally, so advise and act cannot drift — plus the two blockers only a bot
               needs, which ForcePlanBotPolicy.Blockers adds on top.

               Why this caller fails CLOSED where the advice merely notes (#3654, #3652). The advisory
               surface, handed a state it could not read, writes `state_note: unknown` on the target and
               leaves `eligible` as the finding-only verdict, because its reader is a human or an agent
               who will cross-reference get_plan_corrections and sys.query_store_plan before running
               anything — #3652 was found exactly that way. This caller has no reader between its verdict
               and the write: with every gate open, Force is sp_query_store_force_plan on a production
               server with nobody looking. The five #3652 blockers are the facts that would have stopped
               five contraindicated forces, and every one of them fires only on an OBSERVED value, so an
               unread state does not merely weaken the gate, it reproduces the pre-#3652 gate exactly. So
               a null state (the read failed, or returned no row for this key) and an empty one (the read
               ran and observed nothing — not even the enablement row the plan-correction collector
               writes for every database it can see, so FORCE_LAST_GOOD_PLAN is unknown here) are both a
               blocker with the reason as evidence, and the journal says `state_unavailable` rather than
               a would_force row that nothing checked. Unknown is a note for a reader and a NO for an
               actor. Likewise FLGP-on: the advice changes its verb and leaves the statement for an
               operator who has read the guidance; the bot stands down for the whole database
               (`apc_enabled_for_database`), because two forcers on one database is the failure #3652
               documented and the bot cannot be the one who read the guidance. */
            var blockers = ForcePlanBotPolicy.Blockers(
                target, state, states is null ? stateUnavailableReason : null);
            var history = await _store.GetQueryHistoryAsync(
                runtime.ServerId, target.Database, target.QueryId, _settings, nowUtc, ct);

            var decision = ForcePlanBotPolicy.Evaluate(
                target, ForcePlanBotPolicy.Names(blockers), currentConfig.PlanForceBotEnabled, _settings,
                history, nowUtc);

            switch (decision.Kind)
            {
                case ForcePlanBotDecisionKind.Suppressed:
                    continue;

                case ForcePlanBotDecisionKind.Blocked:
                    /* reasons = the blocker NAMES (the consumer API, comma-joined as ever); detail = each
                       name with the evidence it was built from, one per line, so a blocked row is
                       auditable against the snapshot it was judged on (#3654). The policy returns the
                       blocker list as its reasons whenever the list is non-empty; when it is empty the
                       Blocked came from the bot's own history gates (failed-force memory, daily budget),
                       which carry no evidence beyond their name, and detail stays null as before. */
                    await _store.JournalAsync(BuildRecord(
                        runtime, target, PgPlanForceActionStore.ActionBlocked, decision.Reasons,
                        PgPlanForceActionStore.OutcomeLogged,
                        detail: ForcePlanBotPolicy.Evidence(blockers), nowUtc), ct);
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

    /// <summary>
    /// The pass's candidate list: PLAN_REGRESSION targets in finding order, one per (database, query)
    /// across findings (a query regressed in two findings is one decision), cut at
    /// <see cref="MaxTargetsPerPass"/>. Materialized up front (rather than judged as it is walked, as it
    /// was before #3654) so the state read can be ONE statement over the whole list; the order and the
    /// cap are exactly what the walk produced, so the worst-regression-first assumption the cap rests on
    /// is unchanged.
    /// </summary>
    private static List<ForcePlanTarget> CollectCandidates(IReadOnlyList<AnalysisFinding> findings)
    {
        var seen = new HashSet<(string Database, long QueryId)>();
        var candidates = new List<ForcePlanTarget>(MaxTargetsPerPass);

        foreach (var finding in findings)
        {
            if (finding?.Remediation is not { FactKey: "PLAN_REGRESSION" } remediation ||
                remediation.Targets is not { Count: > 0 } targets)
            {
                continue;
            }

            foreach (var target in targets)
            {
                if (candidates.Count >= MaxTargetsPerPass)
                {
                    return candidates;
                }

                if (!seen.Add((target.Database, target.QueryId)))
                {
                    continue;
                }

                candidates.Add(target);
            }
        }

        return candidates;
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
            /* Always the bot: this class IS the bot, and it has no operator-driven arm. Stamped as a
               constant rather than passed in so there is no argument to get wrong — and it is what makes
               GetPendingReviewsAsync' actor filter meet rows it can actually match. */
            Actor: PgPlanForceActionStore.ActorBot,
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

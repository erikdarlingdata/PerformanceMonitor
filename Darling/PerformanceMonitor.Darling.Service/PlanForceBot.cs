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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The auto force-plan bot's orchestration (#2138 phase 1): runs after each scheduled analysis pass,
/// re-judges the pass's force-plan targets through the SAME policy gate agents inspect on the MCP
/// surfaces (<see cref="FactRemediation.ForcePlanBlockers"/> feeding
/// <see cref="ForcePlanBotPolicy.Evaluate"/>), journals every decision to
/// <c>collect.plan_force_actions</c>, and — only when the global gates AND the per-server opt-in are
/// all open — executes through <see cref="IPlanForceExecutor"/>. Shipped state: globally OFF; when
/// enabled, DRY RUN. The dry run is live mode with the executor withheld, so shadow-mode journal
/// rows ARE the decisions a live bot would have executed (Erik's dogfood plan on the issue: score
/// the would-force ledger before any write path is armed).
///
/// <para>Failure-isolated at every seam: a journal or executor fault logs and moves on — the bot
/// must never take an analysis pass or a sweep down with it.</para>
/// </summary>
public sealed class PlanForceBot
{
    /* Belt over the extractor's per-finding cap: the pass evaluates at most this many targets, so a
       pathological drill-down can never turn one analysis pass into a journal flood. */
    internal const int MaxTargetsPerPass = 10;

    private readonly IPlanForceActionStore _store;
    private readonly ForcePlanBotSettings _settings;
    private readonly Func<string, IPlanForceExecutor> _executorFactory;
    private readonly ILogger _logger;

    public PlanForceBot(
        IPlanForceActionStore store,
        ForcePlanBotSettings settings,
        Func<string, IPlanForceExecutor> executorFactory,
        ILogger logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _settings = (settings ?? throw new ArgumentNullException(nameof(settings))).Normalize();
        _executorFactory = executorFactory ?? throw new ArgumentNullException(nameof(executorFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>Whether the EVALUATION half runs. The review half deliberately does not consult
    /// this — see <see cref="RunAfterAnalysisAsync"/>.</summary>
    public bool Enabled => _settings.Enabled;

    /// <summary>
    /// One post-analysis bot pass for one server: first review any of its own outstanding live
    /// forces (taking back a bad force outranks placing a new one), then evaluate the pass's
    /// force-plan targets. <paramref name="currentConfig"/> is the server's CURRENT registry view
    /// (the reload-swapped <c>ServerLoopState.Config</c>, not the connect-time snapshot on the
    /// runtime), so revoking the per-server opt-in takes effect on the next pass, not the next
    /// reconnect.
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

        /* Deliberately NOT gated on _settings.Enabled (#2731 review catch): the review half must
           outlive the switch that armed the force, or disarming the bot would orphan every
           outstanding live force from the self-review it was promised — pinned forever with nobody
           watching. A disabled bot still reviews its OWN outstanding forces (one indexed read of a
           table that is empty unless this deployment ever forced live), journals the verdicts, and
           WITHHOLDS the unforce write — the gatesOpen check in ExecuteUnforceAsync includes Enabled —
           so the operator gets one actionable journal row + log line per orphaned force, then quiet.
           Only the EVALUATION half is Enabled-gated. */

        /* The engine seam (#2213's lesson): this bot speaks T-SQL to SqlClient connections and
           nothing else. PLAN_REGRESSION cannot fire for a PostgreSQL target today, but the gate
           lives HERE — at the boundary that would open the connection — rather than relying on the
           upstream fact never learning to. */
        if (currentConfig.IsPostgres || runtime.Config.IsPostgres)
        {
            return;
        }

        try
        {
            await ReviewPendingForcesAsync(runtime, currentConfig, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "[{Server}] Force-plan bot review pass failed: {Message}",
                runtime.Config.DisplayName, ex.Message);
        }

        if (!_settings.Enabled)
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
                            PgPlanForceActionStore.OutcomeLogged, detail: null, relatedActionId: null, nowUtc), ct);
                        continue;

                    case ForcePlanBotDecisionKind.WouldForce:
                        await _store.JournalAsync(BuildRecord(
                            runtime, target, PgPlanForceActionStore.ActionWouldForce, decision.Reasons,
                            PgPlanForceActionStore.OutcomeLogged, detail: null, relatedActionId: null, nowUtc), ct);
                        continue;

                    case ForcePlanBotDecisionKind.Force:
                        await ExecuteForceAsync(runtime, target, nowUtc, ct);
                        continue;
                }
            }
        }
    }

    private async Task ExecuteForceAsync(
        ServerRuntime runtime, ForcePlanTarget target, DateTime nowUtc, CancellationToken ct)
    {
        /* Intent row FIRST, completion row second: if the process dies between the write and its
           journal entry, the trail still shows the attempt — an audit that can lose the action it
           audited is not one. Append-only, so the completion is its own row, not an UPDATE. */
        var intentId = await _store.JournalAsync(BuildRecord(
            runtime, target, PgPlanForceActionStore.ActionForce, Array.Empty<string>(),
            PgPlanForceActionStore.OutcomeAttempting, detail: null, relatedActionId: null, nowUtc), ct);

        var executor = _executorFactory(runtime.ConnectionString);
        var result = await executor.ForcePlanAsync(target.Database, target.QueryId, target.PlanId, ct);

        await _store.JournalAsync(BuildRecord(
            runtime, target, PgPlanForceActionStore.ActionForce, Array.Empty<string>(),
            result.Succeeded ? PgPlanForceActionStore.OutcomeSucceeded : PgPlanForceActionStore.OutcomeFailed,
            detail: result.Error, relatedActionId: intentId, DateTime.UtcNow), ct);

        if (result.Succeeded)
        {
            _logger.LogWarning(
                "[{Server}] Force-plan bot FORCED plan {PlanId} for query {QueryId} in {Database} (regression {Factor:F1}x) — self-review at +{First}m/+{Final}m",
                runtime.Config.DisplayName, target.PlanId, target.QueryId, target.Database,
                target.RegressionFactor, _settings.FirstReviewMinutes, _settings.FinalReviewMinutes);
        }
        else
        {
            _logger.LogWarning(
                "[{Server}] Force-plan bot force FAILED for query {QueryId} in {Database}: {Error}",
                runtime.Config.DisplayName, target.QueryId, target.Database, result.Error);
        }
    }

    private async Task ReviewPendingForcesAsync(
        ServerRuntime runtime, MonitoredServer currentConfig, CancellationToken ct)
    {
        /* Pending rows exist only if this bot placed a live force, so in a never-armed deployment
           this is one indexed read of an empty table per analysis pass. The read still runs under
           dry-run AND under a disabled bot — a force placed while live must not escape review
           because someone flipped a global switch back afterwards. The read also surfaces ORPHANED
           INTENTS (a force whose completion journal write failed after the server was touched); the
           verify read below answers what actually happened and the state machine closes them. */
        var pending = await _store.GetPendingReviewsAsync(runtime.ServerId, DateTime.UtcNow, ct);
        if (pending.Count == 0)
        {
            return;
        }

        var nowUtc = DateTime.UtcNow;
        var executor = _executorFactory(runtime.ConnectionString);

        foreach (var force in pending)
        {
            /* Read-only verify — safe on any server regardless of gates. Null = the server could not
               answer; the state machine's KeepWatching-on-no-evidence posture is applied by simply
               trying again next pass. */
            var verify = await executor.VerifyAsync(
                force.DatabaseName, force.QueryId, force.PlanId, force.ActionTimeUtc, ct);
            if (verify is null)
            {
                continue;
            }

            var verdict = ForcePlanSelfReview.Evaluate(
                new ForcePlanReviewInput(
                    ForcedAtUtc: force.ActionTimeUtc,
                    /* The baseline is the journaled evidence snapshot — the regressed cpu/exec the
                       force was sold on — never re-derived at review time. */
                    BaselineCpuPerExecUs: force.LatestCpuPerExecUs,
                    PlanIsStillForced: verify.PlanIsStillForced,
                    ForceFailureCount: verify.ForceFailureCount,
                    ExecutionsSinceForce: verify.ExecutionsSinceForce,
                    ObservedCpuPerExecUs: verify.ObservedCpuPerExecUs),
                _settings, nowUtc);

            var observedDetail = string.Create(CultureInfo.InvariantCulture,
                $"observed_cpu_per_exec_us={verify.ObservedCpuPerExecUs?.ToString("F1", CultureInfo.InvariantCulture) ?? "none"}; executions_since_force={verify.ExecutionsSinceForce}; baseline_cpu_per_exec_us={force.LatestCpuPerExecUs:F1}; force_failure_count={verify.ForceFailureCount}");

            switch (verdict.Kind)
            {
                case ForcePlanReviewVerdictKind.KeepWatching:
                case ForcePlanReviewVerdictKind.KeepForced:
                    /* Deliberately not journaled: a keep at a mid-window checkpoint would append an
                       identical row on every analysis pass between the checkpoints. The journal
                       records decisions and terminals; "still fine, still watching" is the absence
                       of either. */
                    continue;

                case ForcePlanReviewVerdictKind.ReviewComplete:
                    await _store.JournalAsync(force with
                    {
                        ActionId = 0,
                        ActionTimeUtc = nowUtc,
                        Action = PgPlanForceActionStore.ActionReview,
                        Decision = verdict.Reason,
                        Reasons = verdict.Reason,
                        Outcome = PgPlanForceActionStore.OutcomeLogged,
                        Detail = observedDetail,
                        RelatedActionId = force.ActionId,
                    }, ct);
                    continue;

                case ForcePlanReviewVerdictKind.Unforce:
                    await ExecuteUnforceAsync(runtime, currentConfig, force, verdict, observedDetail, nowUtc, ct);
                    continue;
            }
        }
    }

    private async Task ExecuteUnforceAsync(
        ServerRuntime runtime,
        MonitoredServer currentConfig,
        PlanForceActionRecord force,
        ForcePlanReviewVerdict verdict,
        string observedDetail,
        DateTime nowUtc,
        CancellationToken ct)
    {
        /* The unforce write obeys the SAME two gates as the force did. If the operator has since
           closed either one, the verdict is journaled as withheld — the trail says what the bot
           wanted to do and why it did not, and the operator acts by hand. The row still closes the
           review (and still counts into the failure-memory window), so a withheld verdict is not
           re-litigated every pass. */
        var gatesOpen = _settings.Enabled && !_settings.DryRun && currentConfig.PlanForceBotEnabled;

        if (!gatesOpen)
        {
            await _store.JournalAsync(force with
            {
                ActionId = 0,
                ActionTimeUtc = nowUtc,
                Action = PgPlanForceActionStore.ActionUnforce,
                Decision = verdict.Reason,
                Reasons = verdict.Reason,
                Outcome = PgPlanForceActionStore.OutcomeLogged,
                Detail = "write withheld (bot disabled, dry_run, or server opt-in revoked); " + observedDetail,
                RelatedActionId = force.ActionId,
            }, ct);

            _logger.LogWarning(
                "[{Server}] Force-plan bot self-review verdict UNFORCE ({Reason}) for query {QueryId} in {Database} was WITHHELD — write gates closed. Unforce by hand: EXEC sys.sp_query_store_unforce_plan @query_id = {QueryId2}, @plan_id = {PlanId};",
                runtime.Config.DisplayName, verdict.Reason, force.QueryId, force.DatabaseName,
                force.QueryId, force.PlanId);
            return;
        }

        var executor = _executorFactory(runtime.ConnectionString);
        var result = await executor.UnforcePlanAsync(force.DatabaseName, force.QueryId, force.PlanId, ct);

        await _store.JournalAsync(force with
        {
            ActionId = 0,
            ActionTimeUtc = nowUtc,
            Action = PgPlanForceActionStore.ActionUnforce,
            Decision = verdict.Reason,
            Reasons = verdict.Reason,
            Outcome = result.Succeeded ? PgPlanForceActionStore.OutcomeSucceeded : PgPlanForceActionStore.OutcomeFailed,
            Detail = result.Succeeded ? observedDetail : result.Error + "; " + observedDetail,
            RelatedActionId = force.ActionId,
        }, ct);

        _logger.LogWarning(
            "[{Server}] Force-plan bot self-review UNFORCED plan {PlanId} for query {QueryId} in {Database} ({Reason}): {Outcome}",
            runtime.Config.DisplayName, force.PlanId, force.QueryId, force.DatabaseName,
            verdict.Reason, result.Succeeded ? "succeeded" : result.Error);
    }

    private PlanForceActionRecord BuildRecord(
        ServerRuntime runtime,
        ForcePlanTarget target,
        string action,
        IReadOnlyList<string> reasons,
        string outcome,
        string? detail,
        long? relatedActionId,
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
            RelatedActionId: relatedActionId);
}

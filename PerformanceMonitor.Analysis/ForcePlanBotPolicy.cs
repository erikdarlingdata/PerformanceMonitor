/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The auto force-plan bot's tunable knobs (#2138). Every default is the SAFE state: the bot is
/// globally OFF, and even when enabled it is a dry run — it journals what it WOULD do and touches
/// nothing. A live write additionally requires the per-server opt-in the caller passes to
/// <see cref="ForcePlanBotPolicy.Evaluate"/>, so no monitored server can be written to without BOTH
/// gates being deliberately opened. Dry-run is not a separate code path: it is live mode with the
/// executor swapped for the journal, so the decisions an operator inspects during shadow mode are
/// exactly the decisions the bot would have executed — same policy, same cooldowns, same caps.
/// (Phase 1 goes further and ships no executor at all, so those three gates guard a decision that
/// has nowhere to go; the write path they will really guard is #2731.)
/// </summary>
public sealed record ForcePlanBotSettings
{
    /// <summary>Global gate 1. False (the default) means the bot evaluates nothing at all — no
    /// journal rows, no store reads, no cost.</summary>
    public bool Enabled { get; init; }

    /// <summary>Global gate 2, default TRUE: even an enabled bot only journals would-force
    /// decisions. A live write requires this false AND the per-server opt-in.</summary>
    public bool DryRun { get; init; } = true;

    /// <summary>
    /// The bot's own action floor on <c>regression_factor</c> — deliberately a SECOND knob on top of
    /// the detection threshold (the fact fires at >= 2), so an operator can keep detection sensitive
    /// while making the bot act only on egregious regressions. Defaults equal to detection's floor.
    /// </summary>
    public double MinRegressionFactor { get; init; } = 2.0;

    /// <summary>One journaled decision per (server, database, query) per this window — the per-query
    /// blast-radius cap, and what keeps shadow mode from re-journaling the same verdict on every
    /// analysis pass.</summary>
    public int QueryCooldownHours { get; init; } = 24;

    /// <summary>Rolling 24h cap on actionable decisions per server (would-force rows count too, so
    /// the dry run rehearses the same budget the live bot spends).</summary>
    public int MaxActionsPerServerPerDay { get; init; } = 3;

    /// <summary>How many failed forces (a force that would not stick, or one the self-review had to
    /// take back) within <see cref="FailedForceCooldownHours"/> block the query from further forcing.</summary>
    public int FailedForceThreshold { get; init; } = 2;

    /// <summary>
    /// The failure-memory window, in hours (default one week). Deliberately a COOLDOWN and not a
    /// permanent flag: the history read counts failures WITHIN this window, so when the window slides
    /// past them the query becomes eligible again with no reset step, no restart, and no state to
    /// clean up (#2677's lesson — a "gave up" latch that is never re-probed turns a transient failure
    /// into a restart-only outage).
    /// </summary>
    public int FailedForceCooldownHours { get; init; } = 168;

    /// <summary>First self-review checkpoint after a live force, in minutes (default 1 hour).</summary>
    public int FirstReviewMinutes { get; init; } = 60;

    /// <summary>Final self-review checkpoint, in minutes (default 24 hours). At this point the review
    /// reaches a terminal verdict either way.</summary>
    public int FinalReviewMinutes { get; init; } = 1440;

    /// <summary>Executions the forced query must accumulate before a checkpoint judges cost — the
    /// same floor detection uses, so the review never rules on thinner evidence than the decision did.
    /// <para>Also the executions limb of the operator flow's post-eviction observation window
    /// (<see cref="OperatorRemediationFlow.Observe"/>), which takes it as a parameter rather than
    /// restating the number: one floor, so an evict-then-observe verdict and the self-review that later
    /// judges the same query cannot rule on different amounts of evidence.</para></summary>
    public int MinReviewExecutions { get; init; } = 25;

    /// <summary>
    /// The elapsed limb of the post-eviction observation window, in minutes — the operator flow observes
    /// until whichever comes first of <see cref="MinReviewExecutions"/> executions or this.
    ///
    /// <para>A TIMEOUT rather than a second measurement, and the state machine treats it as one: a window
    /// this limb closed cannot support a cost verdict, because it says the time is up and nothing about
    /// how much ran inside it (<see cref="ObservationWindowLimb"/>). Its job is to stop an observation on
    /// a query nobody called from waiting forever.</para>
    ///
    /// <para>Lives on the bot's settings rather than beside the operator flow because the design has the
    /// bot reuse this exact sequence in phase 2. One window, one knob — a separate operator-side default
    /// would let a human and the bot observe the same eviction for different lengths of time and reach
    /// different verdicts about it.</para>
    /// </summary>
    public int ObservationWindowMinutes { get; init; } = 30;

    /// <summary>
    /// The net-benefit bar: post-force cpu/exec must be at or below this fraction of the regressed
    /// baseline (default 0.75 = at least 25% better) or the self-review unforces. "No worse" is
    /// deliberately not good enough — a force that buys nothing still pins a plan against future
    /// data change, so it has to pay rent.
    /// </summary>
    public double NetBenefitRatio { get; init; } = 0.75;

    public static ForcePlanBotSettings Default { get; } = new();

    /// <summary>
    /// Clamps hand-edited values into sane ranges rather than failing the load, matching the alert
    /// knobs' posture. The floors matter more than the ceilings here: a zero or negative cooldown
    /// would let the bot journal (or in live mode, force) the same query every analysis pass.
    /// </summary>
    public ForcePlanBotSettings Normalize() => this with
    {
        MinRegressionFactor = double.IsFinite(MinRegressionFactor) ? Math.Max(1.0, MinRegressionFactor) : 2.0,
        QueryCooldownHours = Math.Clamp(QueryCooldownHours, 1, 720),
        MaxActionsPerServerPerDay = Math.Clamp(MaxActionsPerServerPerDay, 1, 50),
        FailedForceThreshold = Math.Clamp(FailedForceThreshold, 1, 10),
        FailedForceCooldownHours = Math.Clamp(FailedForceCooldownHours, 1, 8760),
        FirstReviewMinutes = Math.Clamp(FirstReviewMinutes, 5, 1440),
        FinalReviewMinutes = Math.Clamp(FinalReviewMinutes, Math.Clamp(FirstReviewMinutes, 5, 1440), 10080),
        MinReviewExecutions = Math.Clamp(MinReviewExecutions, 1, 100000),
        /* Floor of 1 minute: a zero or negative window would close the observation on the same pass the
           eviction ran, so every eviction would be judged before the optimizer had compiled anything —
           the elapsed limb's whole job is to be a bound, and an instant bound is not one. */
        ObservationWindowMinutes = Math.Clamp(ObservationWindowMinutes, 1, 1440),
        NetBenefitRatio = double.IsFinite(NetBenefitRatio) ? Math.Clamp(NetBenefitRatio, 0.05, 1.0) : 0.75,
    };
}

/// <summary>What the bot decided for one target — see <see cref="ForcePlanBotPolicy.Evaluate"/>.</summary>
public enum ForcePlanBotDecisionKind
{
    /// <summary>Nothing to record: the bot is off, the target is below its action floor, or this
    /// query's decision was already journaled inside the cooldown window. Deliberately NOT journaled —
    /// the audit trail records decisions, and a repeat inside the cooldown is the same decision.</summary>
    Suppressed,

    /// <summary>Journaled, no action: a named gate says this target must not be forced without a
    /// human. The reasons carry the gate names.</summary>
    Blocked,

    /// <summary>Every gate passed and the bot would force — journaled with the evidence, executed
    /// against nothing. The reasons say which of the two write gates kept it advisory
    /// (dry_run and/or server_not_opted_in).</summary>
    WouldForce,

    /// <summary>Every gate passed AND both write gates are open — the verdict that authorizes a
    /// live force. Phase 1 ships no write path, so its caller journals the decision as WITHHELD; the
    /// arm that executes it is #2731. The verdict is produced here either way so the decision table
    /// is one table, reviewed once, rather than something the write path adds a branch to.</summary>
    Force,
}

/// <summary>
/// One evaluation's whole answer as a single value, so a caller cannot take the verdict and drop
/// the reasons (the record-return discipline the alert gates use).
/// </summary>
public sealed record ForcePlanBotDecision(
    ForcePlanBotDecisionKind Kind,
    IReadOnlyList<string> Reasons);

/// <summary>
/// The store-derived history the policy judges against — fetched by the caller (the policy does no
/// I/O and takes no clock). Every field is WINDOWED by the caller's read, which is what makes the
/// give-up state self-healing: eligibility returns when the window slides past the failures, not
/// when someone clears a flag.
/// </summary>
/// <param name="LastJournaledForQueryUtc">When this (server, database, query) last got a journaled
/// decision of any kind, or null when it never has.</param>
/// <param name="ServerActionsLast24h">Journaled would-force/force decisions for this server in the
/// trailing 24 hours.</param>
/// <param name="RecentFailedForces">Failed forces for this query inside
/// <see cref="ForcePlanBotSettings.FailedForceCooldownHours"/>: forces that would not stick, plus
/// forces the self-review unforced as not-a-net-benefit.</param>
public sealed record ForcePlanBotHistory(
    DateTime? LastJournaledForQueryUtc,
    int ServerActionsLast24h,
    int RecentFailedForces)
{
    public static ForcePlanBotHistory Empty { get; } = new(null, 0, 0);
}

/// <summary>
/// The #2138 phase 1+ policy: whether the bot may act on one force-plan target, given the target's
/// verdict blockers, the operator's settings, and the journaled history. Pure and static — no clock,
/// no I/O, the caller passes <c>nowUtc</c> and persists the result — so the whole decision table is
/// unit-testable without a host, store, or server.
///
/// <para>The finding-level gate is NOT re-derived here: the caller passes
/// <see cref="FactRemediation.ForcePlanBlockers"/>' output straight through, so the bot consults the
/// SAME function agents inspect on the MCP surfaces (#2146's contract — what an agent reads today is
/// what the bot enforces, and "never auto-force a parameter-sensitivity-flagged target" stays one
/// implementation, not a promise kept in two places).</para>
/// </summary>
public static class ForcePlanBotPolicy
{
    /// <summary>Reason strings are a consumer API (agents key on them like alert fact names): add
    /// new ones freely, never redefine what an existing one means.</summary>
    public const string ReasonBotDisabled = "bot_disabled";
    public const string ReasonBelowRegressionFloor = "below_regression_floor";
    public const string ReasonQueryCooldownActive = "query_cooldown_active";
    public const string ReasonFailedForceCooldown = "failed_force_cooldown";
    public const string ReasonServerDailyBudgetExhausted = "server_daily_budget_exhausted";
    public const string ReasonDryRun = "dry_run";
    public const string ReasonServerNotOptedIn = "server_not_opted_in";

    /// <summary>
    /// #3953: the target's best plan last ran more than <see cref="MaxBestPlanAgeDays"/> before the pass. The
    /// interval table makes PLAN_REGRESSION's 14-day window real, so a best plan can now be up to two weeks old;
    /// the advisory surface shows such a plan with its age, and the unattended bot does not act on it.
    /// </summary>
    public const string ReasonBestPlanStale = "best_plan_stale";

    /// <summary>
    /// #3953: the oldest best plan the unattended bot will consider, in days before the pass. Four is raw Query
    /// Store's retention (<c>TimescaleSupport.RawRetentionInterval</c>): the regime the would-force journal on an
    /// armed store has been scored in, so the ledger the owner scores before arming #2138's write path does not
    /// silently mix two regimes. It narrows automation only, and widening it is a decision on the scored ledger.
    /// </summary>
    public const int MaxBestPlanAgeDays = 4;

    /* The bot's OWN two blockers (#3654) — on the policy, not in FactRemediation's shared vocabulary,
       because only an unattended actor needs them. The advisory surface can say "unknown" and hand
       the decision to a human who will cross-reference; a bot has no one to hand it to. */

    /// <summary>The store's forcing and automatic-plan-correction state for the target could not be
    /// read, or was read and held nothing. For the bot that is a blocker, not a note: see
    /// <see cref="Blockers"/>.</summary>
    public const string ReasonStateUnavailable = "state_unavailable";

    /// <summary>FORCE_LAST_GOOD_PLAN is ON for the target's database — the engine's own bot is the
    /// forcer there, and this one stands down for the whole database. See <see cref="Blockers"/>.</summary>
    public const string ReasonApcEnabledForDatabase = "apc_enabled_for_database";

    /// <summary>
    /// The bot's whole blocker list for one target (#3654): the shared gate's verdict, BOTH halves
    /// (<see cref="FactRemediation.ForcePlanBlockers(ForcePlanTarget, ForcePlanTargetState?)"/> — the two
    /// target-carried blockers and the five #3652 added from the store's forcing and automatic-plan-
    /// correction state), then the two only an unattended actor needs. Pure: the caller reads the state
    /// (<c>DarlingForcePlanTargetStateReader</c>, one batched statement per bot pass) and passes what it
    /// got; every arm is pinnable without a store.
    ///
    /// <para><b>Why the bot reads the whole gate and not the one-argument overload.</b> #3652's live
    /// cross-reference found five <c>eligible: true, blockers: []</c> targets that were five-for-five
    /// contraindicated by facts the store already held — two mid-verification under APC on exactly the
    /// proposed plan, one a known forcing failure, one withdrawn by the engine, one already resolved
    /// through another plan. The advisory surface learned to read them; a bot that still consulted the
    /// state-less overload would have judged all five unblocked and, with every gate open, forced them.
    /// Same function, same evidence strings — what an agent reads in <c>structured_remediation</c> is what
    /// the bot enforces (#2146), and the journal row carries the evidence so a <c>blocked</c> decision is
    /// auditable against the snapshot it was made on.</para>
    ///
    /// <para><b><c>apc_enabled_for_database</c> — the engine is the forcer here.</b> When
    /// <c>force_last_good_plan_actual_state</c> is ON for the target's database, automatic plan
    /// correction forces regressed queries' last good plans on its own, verifies them, and reverts the
    /// ones that do not pay. A second forcer on the same database is exactly the failure #3652
    /// documented: a manual force on a plan APC is verifying converts <c>AUTO</c> to <c>MANUAL</c> and
    /// deletes the engine's revert path, and a plan APC has not touched yet may be the one it is about to.
    /// So the bot does not compete for the database at all — every target in it is blocked with this
    /// name and the enablement snapshot as evidence, whatever the rest of the gate says. The advisory
    /// surface, by contrast, only changes its VERB there (<c>apc_mode: on</c> plus guidance) and leaves
    /// <c>force_sql</c> for the operator who has read it; a human can decide to intervene in an APC
    /// database on purpose, and the bot must not. A database-level fact evaluated per target rather than
    /// once per pass so the journal names it on every target it stopped and the cooldown dedups the
    /// repeats, the same way the other blockers are recorded.</para>
    ///
    /// <para><b><c>state_unavailable</c> — unknown fails closed.</b> Two shapes, one blocker, evidence
    /// distinguishing them. A null state (the read failed, or returned no row for this key) is the plain
    /// case: the bot has no idea what the engine is doing. An EMPTY state (the read ran and observed
    /// nothing inside <see cref="ForcePlanTargetState.Lookback"/>) is the subtler one: no
    /// <c>query_store_stats</c> row for the plan is ordinary for a best plan that is not executing, and
    /// no recommendation is ordinary for a query APC has not judged — but the enablement half comes from
    /// a row <c>PlanCorrectionCollector</c> writes for EVERY database it enumerates, recommendation or not,
    /// at the server's newest capture with no lookback bound. All four halves absent means the store
    /// cannot see this database's FORCE_LAST_GOOD_PLAN state, so the arm above cannot be evaluated, and
    /// an unattended forcer on a database whose APC enablement is unknown is the one place "unknown"
    /// must mean "no". The advisory surface says <c>state_note: unknown</c> for both shapes and lets the
    /// reader cross-reference (<c>get_plan_corrections</c>, <c>sys.query_store_plan</c>); the bot's
    /// journal row says <c>state_unavailable</c> with the reason and forces nothing. The cost is a bot
    /// that stays its hand on a target the store has not observed in a day — a best plan nobody has run
    /// for 24 hours is thin evidence for an unattended force anyway.</para>
    ///
    /// <para>Order: the shared gate's blockers first (the names an agent already sees), then the bot's
    /// own. A target can carry several — <c>apc_owns_it</c> and <c>apc_enabled_for_database</c> together
    /// is the expected shape for a plan APC is verifying on an APC database — and the journal keeps all
    /// of them; the decision is the same whichever fired first.</para>
    /// </summary>
    /// <param name="state">What the store knows about this target now, or null when the read failed or
    /// returned nothing for it.</param>
    /// <param name="stateUnavailableReason">The reader's stated reason when the whole read failed; quoted
    /// into the <c>state_unavailable</c> evidence so the journal says WHY the bot could not see.</param>
    public static IReadOnlyList<ForcePlanBlocker> Blockers(
        ForcePlanTarget target,
        ForcePlanTargetState? state,
        string? stateUnavailableReason)
    {
        if (target is null)
        {
            throw new ArgumentNullException(nameof(target));
        }

        var blockers = new List<ForcePlanBlocker>(FactRemediation.ForcePlanBlockers(target, state));

        if (state is { ApcIsOn: true })
        {
            blockers.Add(new ForcePlanBlocker(
                ReasonApcEnabledForDatabase,
                $"plan_correction: force_last_good_plan_actual_state = {state.ForceLastGoodPlanActualState} for {target.Database} at {FactRemediation.Stamp(state.EnablementObservedAtUtc)} — automatic plan correction owns plan forcing on this database; the bot stands down rather than be the second forcer (#3652: a manual force on a plan the engine is verifying replaces AUTO forcing and removes its revert path)"));
        }

        if (state is null)
        {
            blockers.Add(new ForcePlanBlocker(
                ReasonStateUnavailable,
                string.IsNullOrWhiteSpace(stateUnavailableReason)
                    ? $"the forcing and automatic-plan-correction state read returned no row for plan {target.PlanId} of query {target.QueryId} in {target.Database}; an unattended force cannot proceed on an unknown engine state"
                    : $"{stateUnavailableReason.Trim()} — an unattended force cannot proceed on an unknown engine state"));
        }
        else if (state.IsEmpty)
        {
            blockers.Add(new ForcePlanBlocker(
                ReasonStateUnavailable,
                $"the forcing and automatic-plan-correction state read ran and observed nothing for this target inside the last {ForcePlanTargetState.Lookback.TotalHours:0} hours: no query_store_stats row for plan {target.PlanId}, no forced sibling plan of query {target.QueryId}, no plan_correction recommendation, and no plan_correction capture for {target.Database} at all — FORCE_LAST_GOOD_PLAN enablement is unknown for this database, and an unattended force cannot proceed on unknown"));
        }

        return blockers;
    }

    /// <summary>The blocker names, in order, for <see cref="Evaluate"/>'s <c>policyBlockers</c> — and
    /// the journal's <c>reasons</c> column. The evidence travels separately (<see cref="Evidence"/>) so
    /// the names stay the comma-joinable consumer API they have always been.</summary>
    public static IReadOnlyList<string> Names(IReadOnlyList<ForcePlanBlocker> blockers)
    {
        /* Block-bodied, not an expression body: the T-SQL convention guard's member walk mis-reads the
           range of an expression-bodied member whose body holds a `{ }` property pattern (the #3607
           precedent), and a member the walk reads short is a member no census can see into. */
        if (blockers is not { Count: > 0 })
        {
            return Array.Empty<string>();
        }

        return blockers.Select(b => b.Name).ToList();
    }

    /// <summary>The journal's <c>detail</c> for a blocked decision: one line per blocker,
    /// <c>name: evidence</c>, so the row can be read against the snapshot it was judged on without a
    /// second query. Null when there is nothing to quote.</summary>
    public static string? Evidence(IReadOnlyList<ForcePlanBlocker> blockers)
    {
        /* Block-bodied for the same reason as Names. */
        if (blockers is not { Count: > 0 })
        {
            return null;
        }

        return string.Join("\n", blockers.Select(b => $"{b.Name}: {b.Evidence}"));
    }

    public static ForcePlanBotDecision Evaluate(
        ForcePlanTarget target,
        IReadOnlyList<string> policyBlockers,
        bool serverOptedIn,
        ForcePlanBotSettings settings,
        ForcePlanBotHistory history,
        DateTime nowUtc)
    {
        if (target is null)
        {
            throw new ArgumentNullException(nameof(target));
        }

        if (settings is null)
        {
            throw new ArgumentNullException(nameof(settings));
        }

        history ??= ForcePlanBotHistory.Empty;

        /* Total on purpose: callers short-circuit a disabled bot before ever fetching history, but
           the policy still answers correctly if one forgets, so "disabled means nothing happens" is
           a property of the decision function rather than of call-site discipline. */
        if (!settings.Enabled)
        {
            return new ForcePlanBotDecision(ForcePlanBotDecisionKind.Suppressed, new[] { ReasonBotDisabled });
        }

        if (target.RegressionFactor < settings.MinRegressionFactor)
        {
            return new ForcePlanBotDecision(ForcePlanBotDecisionKind.Suppressed, new[] { ReasonBelowRegressionFloor });
        }

        /* The cooldown is checked BEFORE the blockers, deliberately: a blocked target is journaled
           once per window too. Analysis runs every few minutes, and a PSP-flagged query that stays
           regressed would otherwise write an identical 'blocked' row on every pass — an audit trail
           that repeats itself into noise stops being read. */
        if (history.LastJournaledForQueryUtc is DateTime last &&
            last > nowUtc.AddHours(-settings.QueryCooldownHours))
        {
            return new ForcePlanBotDecision(ForcePlanBotDecisionKind.Suppressed, new[] { ReasonQueryCooldownActive });
        }

        if (policyBlockers is { Count: > 0 })
        {
            return new ForcePlanBotDecision(ForcePlanBotDecisionKind.Blocked, policyBlockers);
        }

        /* #3953: after the cooldown (a stale target is journaled once per window, like any blocked one) and the
           shared blockers. A null age is a finding from before the column existed, when raw's 4-day retention
           already bounded it, so it is not gated. */
        if (target.BestPlanLastSeenUtc is DateTime bestLastSeen && bestLastSeen < nowUtc.AddDays(-MaxBestPlanAgeDays))
        {
            return new ForcePlanBotDecision(ForcePlanBotDecisionKind.Blocked, new[] { ReasonBestPlanStale });
        }

        /* Failure memory. RecentFailedForces is already windowed by the caller's read (see
           ForcePlanBotHistory), so there is no expiry arithmetic here and nothing latches: two weeks
           after the second failed force, the same read returns 0 and the query is simply eligible. */
        if (history.RecentFailedForces >= settings.FailedForceThreshold)
        {
            return new ForcePlanBotDecision(ForcePlanBotDecisionKind.Blocked, new[] { ReasonFailedForceCooldown });
        }

        if (history.ServerActionsLast24h >= settings.MaxActionsPerServerPerDay)
        {
            return new ForcePlanBotDecision(ForcePlanBotDecisionKind.Blocked, new[] { ReasonServerDailyBudgetExhausted });
        }

        /* Actionable. The two write gates are evaluated LAST and independently, and BOTH of their
           names land in the reasons when they hold the action back — so a shadow-mode journal row
           says exactly which switch(es) stand between this decision and a live force. */
        var advisory = new List<string>(2);
        if (settings.DryRun)
        {
            advisory.Add(ReasonDryRun);
        }

        if (!serverOptedIn)
        {
            advisory.Add(ReasonServerNotOptedIn);
        }

        return advisory.Count > 0
            ? new ForcePlanBotDecision(ForcePlanBotDecisionKind.WouldForce, advisory)
            : new ForcePlanBotDecision(ForcePlanBotDecisionKind.Force, Array.Empty<string>());
    }
}

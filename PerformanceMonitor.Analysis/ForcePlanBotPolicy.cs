/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The auto force-plan bot's tunable knobs (#2138 phases 1-2). Every default is the SAFE state:
/// the bot is globally OFF, and even when enabled it is a dry run — it journals what it WOULD do
/// and touches nothing. A live write additionally requires the per-server opt-in the caller passes
/// to <see cref="ForcePlanBotPolicy.Evaluate"/>, so no monitored server can be written to without
/// BOTH gates being deliberately opened. Dry-run is not a separate code path: it is live mode with
/// the executor swapped for the journal, so the decisions an operator inspects during shadow mode
/// are exactly the decisions the bot would have executed — same policy, same cooldowns, same caps.
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
    /// same floor detection uses, so the review never rules on thinner evidence than the decision did.</summary>
    public int MinReviewExecutions { get; init; } = 25;

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

    /// <summary>Every gate passed AND both write gates are open: the bot forces the plan and
    /// journals the action with its outcome.</summary>
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

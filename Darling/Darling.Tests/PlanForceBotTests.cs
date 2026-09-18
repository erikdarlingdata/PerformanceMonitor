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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The orchestrator itself (#2138 / the #2731 review's coverage catch): policy, review and store each
/// have their own spec, but the wiring between them is where a decision goes wrong — so these tests
/// drive <see cref="PlanForceBot"/> over an in-memory journal and pin the seams: the enabled gate,
/// the engine gate, the per-pass cap and cross-finding dedup, what each verdict journals, and what
/// happens when an operator opens every switch on a build that has no write path.
///
/// <para>There is no executor fake here because there is no executor: <see cref="PlanForceBot"/>
/// takes none, and <c>PlanForceNoWritePathTests</c> pins that the build ships no implementation of
/// the seam and no force/unforce/evict statement anywhere in the service assembly. The write path's
/// own orchestration tests arrive with it (#2731).</para>
///
/// <para>#3654 added the gate's STATE half to the bot: the fake store answers the batched
/// forcing/APC state read too, so the five #3652 blockers, the unavailable-state arm and the FLGP
/// stand-down are driven end to end through the orchestrator and pinned on what it JOURNALS — names in
/// <c>reasons</c>, evidence in <c>detail</c>.</para>
/// </summary>
public sealed class PlanForceBotTests
{
    private const int ServerId = -777001;

    /* ---------------- fakes ---------------- */

    private sealed class FakeStore : IPlanForceActionStore
    {
        public List<PlanForceActionRecord> Journaled { get; } = new();
        public ForcePlanBotHistory History { get; set; } = ForcePlanBotHistory.Empty;
        private long _nextId = 1;

        /* The state half (#3654). Default: the read succeeds and every target it is asked about comes
           back CLEAN and observed (FLGP OFF, nothing forced, no recommendation) — the shape a bot on a
           healthy, APC-off server sees — so the pre-#3654 tests keep their meaning. A test overrides
           per key, or fails the read, or returns an empty dictionary to simulate a missing key. */
        public Dictionary<ForcePlanTargetKey, ForcePlanTargetState> States { get; } = new(ForcePlanTargetKey.Comparer);
        public string? StateReadFailure { get; set; }
        public bool ReturnOnlyExplicitStates { get; set; }
        public int StateReads { get; private set; }
        public List<IReadOnlyList<ForcePlanTarget>> StateReadTargets { get; } = new();

        public Task<long> JournalAsync(PlanForceActionRecord record, CancellationToken ct)
        {
            var id = _nextId++;
            Journaled.Add(record with { ActionId = id });
            return Task.FromResult(id);
        }

        public Task<ForcePlanBotHistory> GetQueryHistoryAsync(
            int serverId, string database, long queryId, ForcePlanBotSettings settings, DateTime nowUtc, CancellationToken ct) =>
            Task.FromResult(History);

        public Task<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>? States, string? UnavailableReason)> TryGetTargetStatesAsync(
            int serverId, IReadOnlyList<ForcePlanTarget> targets, DateTime nowUtc, CancellationToken ct)
        {
            StateReads++;
            StateReadTargets.Add(targets);

            if (StateReadFailure is not null)
            {
                return Task.FromResult<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>?, string?)>((null, StateReadFailure));
            }

            var result = new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer);
            foreach (var t in targets)
            {
                var key = ForcePlanTargetKey.Of(t);
                if (States.TryGetValue(key, out var s))
                {
                    result[key] = s;
                }
                else if (!ReturnOnlyExplicitStates)
                {
                    result[key] = Clean();
                }
            }

            return Task.FromResult<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>?, string?)>((result, null));
        }
    }

    /* ---------------- state shapes ---------------- */

    private static readonly DateTime Observed = new(2026, 9, 18, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>Read, observed, nothing in the way: FLGP OFF for the database, no forced plan, no
    /// recommendation. The one shape that lets the bot through.</summary>
    private static ForcePlanTargetState Clean(string flgp = "OFF") => new(
        PlanIsForced: null, PlanForcingType: null, ForceFailureCount: null, LastForceFailureReason: null,
        PlanObservedAtUtc: null,
        OtherForcedPlanId: null, OtherForcedPlanForcingType: null, OtherForcedPlanObservedAtUtc: null,
        ApcState: null, ApcStateReason: null, ApcRegressedPlanId: null, ApcLastGoodPlanId: null,
        ApcLastGoodPlanForcingType: null, ApcLastGoodPlanIsForced: null, ApcLastGoodPlanForceFailureReason: null,
        ApcExecuteActionInitiatedBy: null, ApcObservedAtUtc: null,
        ForceLastGoodPlanActualState: flgp, EnablementObservedAtUtc: Observed);

    /// <summary>Read ran, saw nothing at all — not even the enablement row.</summary>
    private static ForcePlanTargetState Empty() => Clean() with
    {
        ForceLastGoodPlanActualState = null,
        EnablementObservedAtUtc = null,
    };

    /* The five live #3652 shapes, against the test target (plan 7 of query 42 in orders). */

    /// <summary>APC mid-verification on exactly the proposed plan: AUTO-forced on plan 7.</summary>
    private static ForcePlanTargetState ApcOwnsIt() => Clean() with
    {
        PlanIsForced = true, PlanForcingType = "AUTO", ForceFailureCount = 0, LastForceFailureReason = "NONE",
        PlanObservedAtUtc = Observed,
        ApcState = "Verifying", ApcRegressedPlanId = 9, ApcLastGoodPlanId = 7,
        ApcLastGoodPlanForcingType = "AUTO", ApcLastGoodPlanIsForced = true, ApcObservedAtUtc = Observed,
    };

    /// <summary>An operator already forced plan 7 by hand.</summary>
    private static ForcePlanTargetState AlreadyForced() => Clean() with
    {
        PlanIsForced = true, PlanForcingType = "MANUAL", ForceFailureCount = 0, LastForceFailureReason = "NONE",
        PlanObservedAtUtc = Observed,
    };

    /// <summary>Forcing plan 7 is already failing on this server (the Forced Plan Failing counter).</summary>
    private static ForcePlanTargetState ForcingFailed() => Clean() with
    {
        PlanIsForced = true, PlanForcingType = "MANUAL", ForceFailureCount = 12,
        LastForceFailureReason = "NO_INDEX", PlanObservedAtUtc = Observed,
    };

    /// <summary>The engine recommended plan 7 and withdrew it: Expired / TempTableChanged.</summary>
    private static ForcePlanTargetState ApcWithdrewIt() => Clean() with
    {
        ApcState = "Expired", ApcStateReason = "TempTableChanged", ApcRegressedPlanId = 9, ApcLastGoodPlanId = 7,
        ApcObservedAtUtc = Observed,
    };

    /// <summary>The engine already resolved the query's regression through plan 5, not 7.</summary>
    private static ForcePlanTargetState ApcResolvedDifferently() => Clean() with
    {
        ApcState = "Success", ApcStateReason = "LastGoodPlanForced", ApcRegressedPlanId = 9, ApcLastGoodPlanId = 5,
        ApcLastGoodPlanForcingType = "AUTO", ApcLastGoodPlanIsForced = true, ApcObservedAtUtc = Observed,
    };

    /* ---------------- scaffolding ---------------- */

    private static ServerRuntime Runtime(string engine = "sqlserver") => new()
    {
        Config = new MonitoredServer { Name = "bot-e2e", Host = "bot-e2e-host", Engine = engine },
        ConnectionString = "Server=bot-e2e-host",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "bot-e2e-host",
        ServerId = ServerId,
        EngineEdition = 3,
    };

    private static MonitoredServer Config(bool optedIn = false, string engine = "sqlserver") =>
        new() { Name = "bot-e2e", Host = "bot-e2e-host", Engine = engine, PlanForceBotEnabled = optedIn };

    private static AnalysisFinding Finding(params ForcePlanTarget[] targets) => new()
    {
        ServerId = ServerId,
        Remediation = new RemediationAction("PLAN_REGRESSION", "force", targets),
    };

    private static ForcePlanTarget Target(long queryId = 42, double rf = 10.0, bool psp = false) => new(
        "orders", queryId, 7,
        BestPlanHash: "0x1111111111111111", LatestPlanHash: "0x2222222222222222",
        LatestCpuPerExecUs: 50000, BestCpuPerExecUs: 5000, RegressionFactor: rf,
        ParameterSensitivityCoFired: psp);

    private static (PlanForceBot Bot, FakeStore Store) Build(ForcePlanBotSettings settings)
    {
        var store = new FakeStore();
        var bot = new PlanForceBot(store, settings, NullLogger.Instance);
        return (bot, store);
    }

    private static ForcePlanBotSettings Enabled(bool dryRun = true) =>
        ForcePlanBotSettings.Default with { Enabled = true, DryRun = dryRun };

    /* ---------------- the gates that keep everything inert ---------------- */

    [Fact]
    public async Task DisabledBot_EvaluatesNothing()
    {
        var (bot, store) = Build(ForcePlanBotSettings.Default);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        Assert.False(bot.Enabled);
        Assert.Empty(store.Journaled);
    }

    [Fact]
    public async Task APostgresTarget_IsGatedAtTheBoundary_EvenWithEverySwitchOpen()
    {
        /* The #2213 seam lesson as a wiring test: the gate lives where a connection would be opened,
           not on the upstream fact never learning to fire for PostgreSQL. Both the connect-time
           snapshot and the current registry view are checked, because either one is the answer
           depending on when the engine was last read. */
        var (bot, store) = Build(Enabled(dryRun: false));

        await bot.RunAfterAnalysisAsync(Runtime(engine: "postgres"), Config(optedIn: true, engine: "postgres"),
            new[] { Finding(Target()) }, CancellationToken.None);

        Assert.Empty(store.Journaled);
    }

    /* ---------------- evaluation journaling ---------------- */

    [Fact]
    public async Task ShadowMode_JournalsWouldForce_WithBothClosedGatesNamed()
    {
        var (bot, store) = Build(Enabled());

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionWouldForce, row.Action);
        Assert.Equal(PgPlanForceActionStore.ModeDryRun, row.Mode);
        Assert.Equal("dry_run,server_not_opted_in", row.Reasons);
        Assert.Equal(PgPlanForceActionStore.OutcomeLogged, row.Outcome);
        Assert.Equal(10.0, row.RegressionFactor);
    }

    [Fact]
    public async Task ABlockedTarget_JournalsTheBlockers_FromTheSharedGate()
    {
        /* The blockers are FactRemediation.ForcePlanBlockers' output verbatim — the same function
           agents read in structured_remediation (#2146), never recomputed here, so the #2140
           never-auto-force-a-parameter-sensitive-target rule cannot drift between advise and act. */
        var (bot, store) = Build(Enabled(dryRun: false));

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target(psp: true)) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal("parameter_sensitivity_cofired", row.Reasons);
    }

    [Fact]
    public async Task ASuppressedTarget_JournalsNothing()
    {
        var (bot, store) = Build(Enabled());
        store.History = new ForcePlanBotHistory(DateTime.UtcNow.AddHours(-1), 0, 0);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { Finding(Target()) }, CancellationToken.None);

        Assert.Empty(store.Journaled);
    }

    [Fact]
    public async Task TargetsAreDedupedAcrossFindings_AndCappedPerPass()
    {
        var (bot, store) = Build(Enabled());

        /* Two findings share query 1 (journaled once); 14 distinct queries total, cap is 10. */
        var first = Finding(Enumerable.Range(1, 8).Select(q => Target(queryId: q)).ToArray());
        var second = Finding(Enumerable.Range(1, 14).Select(q => Target(queryId: q)).ToArray());

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { first, second }, CancellationToken.None);

        Assert.Equal(PlanForceBot.MaxTargetsPerPass, store.Journaled.Count);
        Assert.Equal(store.Journaled.Count, store.Journaled.Select(r => r.QueryId).Distinct().Count());
    }

    /* ---------------- the state half (#3654) ---------------- */

    [Fact]
    public async Task TheStateIsReadOnce_ForTheWholePass_BeforeAnyTargetIsJudged()
    {
        /* One batched statement per pass, sized by the same cap that bounds the history reads — not
           one per target. Fourteen distinct queries, cap ten: the read is asked about exactly the ten
           the pass will judge, once. */
        var (bot, store) = Build(Enabled());
        var finding = Finding(Enumerable.Range(1, 14).Select(q => Target(queryId: q)).ToArray());

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { finding }, CancellationToken.None);

        Assert.Equal(1, store.StateReads);
        var asked = Assert.Single(store.StateReadTargets);
        Assert.Equal(PlanForceBot.MaxTargetsPerPass, asked.Count);
        Assert.Equal(Enumerable.Range(1, 10).Select(q => (long)q), asked.Select(t => t.QueryId));
    }

    [Fact]
    public async Task NoCandidates_MeansNoStateRead()
    {
        /* A pass with no PLAN_REGRESSION targets must not spend the store round trip. */
        var (bot, store) = Build(Enabled());
        var other = new AnalysisFinding { ServerId = ServerId, Remediation = new RemediationAction("BLOCKING", "none", Array.Empty<ForcePlanTarget>()) };

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { other }, CancellationToken.None);

        Assert.Equal(0, store.StateReads);
        Assert.Empty(store.Journaled);
    }

    public static IEnumerable<object[]> TheFiveLiveShapes()
    {
        yield return new object[] { "apc_owns_it", ApcOwnsIt(), "plan_forcing_type = AUTO" };
        yield return new object[] { "already_forced", AlreadyForced(), "plan_forcing_type = MANUAL" };
        yield return new object[] { "forcing_failed_on_this_plan", ForcingFailed(), "force_failure_count = 12, last_force_failure_reason = NO_INDEX" };
        yield return new object[] { "apc_withdrew_it", ApcWithdrewIt(), "Expired / TempTableChanged" };
        yield return new object[] { "apc_resolved_differently", ApcResolvedDifferently(), "last_good_plan_id = 5 (not 7)" };
    }

    [Theory]
    [MemberData(nameof(TheFiveLiveShapes))]
    public async Task EachOfTheFiveStateBlockers_StopsTheForce_AndJournalsNameAndEvidence(
        string blocker, ForcePlanTargetState state, string evidenceFragment)
    {
        /* Every gate open — enabled, live, server opted in — and the target's own evidence is clean
           (no PSP, primary). Before #3654 each of these journaled as a WITHHELD force. Now the store's
           state stops it, the row names the blocker in reasons (the consumer API, comma-joined) and
           quotes the store values it was built from in detail, stamped with the snapshot time. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.States[new ForcePlanTargetKey("orders", 42, 7)] = state;

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.NotEqual(PgPlanForceActionStore.ActionForce, row.Action);
        Assert.Contains(blocker, row.Reasons.Split(','));
        Assert.NotNull(row.Detail);
        Assert.Contains($"{blocker}: ", row.Detail, StringComparison.Ordinal);
        Assert.Contains(evidenceFragment, row.Detail, StringComparison.Ordinal);
        Assert.Contains("2026-09-18T12:00:00Z", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStateReadFailure_IsABlocker_NotAFault_AndTheJournalSaysWhy()
    {
        /* The advisory surface notes 'unknown' and lets a human cross-reference; an unattended writer
           has nobody to hand the decision to, so unknown fails CLOSED. Every gate open, and the row is
           blocked/state_unavailable with the reader's reason quoted — not would_force, not force. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.StateReadFailure = "the forcing and automatic-plan-correction state read failed (NpgsqlException: timeout)";

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal(ForcePlanBotPolicy.ReasonStateUnavailable, row.Reasons);
        Assert.Contains("NpgsqlException: timeout", row.Detail, StringComparison.Ordinal);
        Assert.Contains("unattended force cannot proceed", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStateReadThatReturnsNoRowForTheTarget_IsAlsoUnavailable()
    {
        /* The reader LEFT JOINs from the requested keys so every key comes back — but the bot does not
           rely on that: a key the dictionary lacks is judged unknown, not clean. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.ReturnOnlyExplicitStates = true;

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal(ForcePlanBotPolicy.ReasonStateUnavailable, row.Reasons);
        Assert.Contains("returned no row for plan 7 of query 42 in orders", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEmptyState_IsUnavailable_BecauseTheEnablementIsUnknown()
    {
        /* Read ran, observed nothing on any half — including the enablement row the plan-correction
           collector writes for every database it can see. FORCE_LAST_GOOD_PLAN is unknown for the
           database, so the FLGP stand-down cannot be evaluated, and the bot does not guess. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.States[new ForcePlanTargetKey("orders", 42, 7)] = Empty();

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal(ForcePlanBotPolicy.ReasonStateUnavailable, row.Reasons);
        Assert.Contains("no plan_correction capture for orders at all", row.Detail, StringComparison.Ordinal);
        Assert.Contains("FORCE_LAST_GOOD_PLAN enablement is unknown", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FlgpOn_StandsTheBotDown_ForTheDatabase_WhateverElseTheGateSays()
    {
        /* Automatic plan correction is ON for orders and has not touched query 42: no forced plan, no
           recommendation, nothing for the shared gate to name. The bot still stands down — the engine is
           the forcer on this database and two forcers on one database is the #3652 failure. The advice
           only changes its verb here; the bot cannot be the operator who read the guidance. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.States[new ForcePlanTargetKey("orders", 42, 7)] = Clean(flgp: "ON");

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal(ForcePlanBotPolicy.ReasonApcEnabledForDatabase, row.Reasons);
        Assert.Contains("force_last_good_plan_actual_state = ON for orders at 2026-09-18T12:00:00Z", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FlgpOn_WithApcVerifyingThisPlan_JournalsBothNames()
    {
        /* The expected shape on an APC database: the shared gate names apc_owns_it (with the store
           evidence) and the bot adds its own stand-down. Both land, shared gate's first, so the row reads
           the way structured_remediation does plus the bot's reason. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.States[new ForcePlanTargetKey("orders", 42, 7)] = ApcOwnsIt() with { ForceLastGoodPlanActualState = "ON" };

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal("apc_owns_it,apc_enabled_for_database", row.Reasons);
        Assert.Contains("apc_owns_it: query_store_stats: plan 7 is_forced_plan = true", row.Detail, StringComparison.Ordinal);
        Assert.Contains("\napc_enabled_for_database: plan_correction: force_last_good_plan_actual_state = ON", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheTargetCarriedBlocker_StillRidesTheSameRow_WithEvidenceNow()
    {
        /* The two #2138 blockers did not change meaning; they now carry evidence in detail like the
           five, off the same two-argument gate. */
        var (bot, store) = Build(Enabled(dryRun: false));

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target(psp: true)) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal("parameter_sensitivity_cofired", row.Reasons);
        Assert.Contains("parameter_sensitivity_cofired: the query also carries the PARAMETER_SENSITIVITY detector's plan-cache signature", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABlockFromTheBotsOwnHistoryGates_KeepsANullDetail()
    {
        /* failed_force_cooldown is the policy's own reason, not a blocker with store evidence; the row
           carries the name and nothing to quote, exactly as before #3654. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.History = new ForcePlanBotHistory(null, 0, 2);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal(ForcePlanBotPolicy.ReasonFailedForceCooldown, row.Reasons);
        Assert.Null(row.Detail);
    }

    [Fact]
    public async Task TheCooldown_StillOutranksTheStateBlockers_SoABlockedRowIsNotRepeatedEveryPass()
    {
        /* The policy's ordering did not move: a target inside its cooldown is suppressed before the
           blockers are consulted, whichever half they came from. */
        var (bot, store) = Build(Enabled(dryRun: false));
        store.States[new ForcePlanTargetKey("orders", 42, 7)] = ApcOwnsIt();
        store.History = new ForcePlanBotHistory(DateTime.UtcNow.AddHours(-1), 0, 0);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        Assert.Empty(store.Journaled);
    }

    /* ---------------- every switch open, and still no write ---------------- */

    [Fact]
    public async Task EveryGateOpen_JournalsTheForceDecisionAsWithheld_BecauseThisBuildHasNoWritePath()
    {
        /* The honest end of the phase-1 story: an operator who sets enabled + dryRun:false + the
           per-server opt-in has opened every switch there is, the policy returns Force, and the bot
           still cannot touch the server. The row has to SAY that rather than quietly reading as a
           would-force, or the trail would tell an operator who believes the bot is live exactly what
           a shadow-mode bot tells one who knows it is not.

           Since #3654 this is also the happy path of the state half: the fake's default state is read,
           observed and clean (FLGP OFF, nothing forced, no recommendation), and the bot proceeds — the
           new blockers stop contraindicated forces, not forcing. */
        var (bot, store) = Build(Enabled(dryRun: false));

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionForce, row.Action);
        Assert.Equal(PgPlanForceActionStore.ModeLive, row.Mode);
        Assert.Equal(PgPlanForceActionStore.OutcomeWithheld, row.Outcome);
        Assert.Contains("no write path", row.Detail);

        /* Not 'attempting' and not 'failed', and those are load-bearing: 'attempting' would surface
           the row as an orphaned intent owed a self-review of a force that never happened, and
           'failed' would spend the query's failure-memory budget for a failure nobody had. */
        Assert.NotEqual(PgPlanForceActionStore.OutcomeAttempting, row.Outcome);
        Assert.NotEqual(PgPlanForceActionStore.OutcomeFailed, row.Outcome);

        /* One decision row, no completion row: there is nothing to complete. */
        Assert.Null(row.RelatedActionId);
    }
}

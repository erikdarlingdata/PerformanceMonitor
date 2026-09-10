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

        public Task<long> JournalAsync(PlanForceActionRecord record, CancellationToken ct)
        {
            var id = _nextId++;
            Journaled.Add(record with { ActionId = id });
            return Task.FromResult(id);
        }

        public Task<ForcePlanBotHistory> GetQueryHistoryAsync(
            int serverId, string database, long queryId, ForcePlanBotSettings settings, DateTime nowUtc, CancellationToken ct) =>
            Task.FromResult(History);
    }

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

    /* ---------------- every switch open, and still no write ---------------- */

    [Fact]
    public async Task EveryGateOpen_JournalsTheForceDecisionAsWithheld_BecauseThisBuildHasNoWritePath()
    {
        /* The honest end of the phase-1 story: an operator who sets enabled + dryRun:false + the
           per-server opt-in has opened every switch there is, the policy returns Force, and the bot
           still cannot touch the server. The row has to SAY that rather than quietly reading as a
           would-force, or the trail would tell an operator who believes the bot is live exactly what
           a shadow-mode bot tells one who knows it is not. */
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

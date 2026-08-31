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
/// The orchestrator itself (#2138 / the #2731 review's coverage catch): policy, review, store, and
/// executor each have their own spec, but the wiring between them is where a write escapes — so these
/// tests drive <see cref="PlanForceBot"/> over an in-memory journal and a recording executor and pin
/// the seams: the engine gate, the per-pass cap and cross-finding dedup, intent-then-completion
/// journaling, and above all the write-gate RE-CHECK on the unforce path — a self-review verdict must
/// journal as withheld, executor untouched, when the gates have closed since the force.
/// </summary>
public sealed class PlanForceBotTests
{
    private const int ServerId = -777001;

    /* ---------------- fakes ---------------- */

    private sealed class FakeStore : IPlanForceActionStore
    {
        public List<PlanForceActionRecord> Journaled { get; } = new();
        public ForcePlanBotHistory History { get; set; } = ForcePlanBotHistory.Empty;
        public List<PlanForceActionRecord> Pending { get; set; } = new();
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

        public Task<IReadOnlyList<PlanForceActionRecord>> GetPendingReviewsAsync(int serverId, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<PlanForceActionRecord>>(Pending);
    }

    private sealed class FakeExecutor : IPlanForceExecutor
    {
        public List<string> Calls { get; } = new();
        public PlanForceExecutionResult ForceResult { get; set; } = PlanForceExecutionResult.Success;
        public PlanForceExecutionResult UnforceResult { get; set; } = PlanForceExecutionResult.Success;
        public PlanForceVerifyResult? VerifyResult { get; set; }

        public Task<PlanForceExecutionResult> ForcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
        {
            Calls.Add($"force:{database}/{queryId}/{planId}");
            return Task.FromResult(ForceResult);
        }

        public Task<PlanForceExecutionResult> UnforcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
        {
            Calls.Add($"unforce:{database}/{queryId}/{planId}");
            return Task.FromResult(UnforceResult);
        }

        public Task<PlanForceExecutionResult> EvictPlanAsync(string database, string queryPlanHashHex, CancellationToken ct)
        {
            Calls.Add($"evict:{database}/{queryPlanHashHex}");
            return Task.FromResult(PlanForceExecutionResult.Success);
        }

        public Task<PlanForceVerifyResult?> VerifyAsync(string database, long queryId, long planId, DateTime forcedAtUtc, CancellationToken ct)
        {
            Calls.Add($"verify:{database}/{queryId}/{planId}");
            return Task.FromResult(VerifyResult);
        }
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

    private static PlanForceActionRecord PendingForce(DateTime forcedAtUtc) => new(
        ActionId: 99, ActionTimeUtc: forcedAtUtc, ServerId: ServerId, ServerName: "bot-e2e-host",
        DatabaseName: "orders", QueryId: 42, PlanId: 7,
        Action: PgPlanForceActionStore.ActionForce, Mode: PgPlanForceActionStore.ModeLive,
        Decision: PgPlanForceActionStore.ActionForce, Reasons: "",
        RegressionFactor: 10.0, LatestCpuPerExecUs: 50000, BestCpuPerExecUs: 5000,
        ReplicaRole: null, ParameterSensitivityCoFired: false,
        Outcome: PgPlanForceActionStore.OutcomeSucceeded, Detail: null, RelatedActionId: null);

    private static (PlanForceBot Bot, FakeStore Store, FakeExecutor Executor) Build(
        ForcePlanBotSettings settings)
    {
        var store = new FakeStore();
        var executor = new FakeExecutor();
        var bot = new PlanForceBot(store, settings, _ => executor, NullLogger.Instance);
        return (bot, store, executor);
    }

    private static ForcePlanBotSettings Enabled(bool dryRun = true) =>
        ForcePlanBotSettings.Default with { Enabled = true, DryRun = dryRun };

    /* ---------------- the gates that keep everything inert ---------------- */

    [Fact]
    public async Task DisabledBot_TouchesNothing()
    {
        var (bot, store, executor) = Build(ForcePlanBotSettings.Default);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        Assert.Empty(store.Journaled);
        Assert.Empty(executor.Calls);
    }

    [Fact]
    public async Task APostgresTarget_IsGatedAtTheBoundary_EvenWithTheBotArmed()
    {
        /* The #2213 seam lesson as a wiring test: the gate lives where the connection would open, so
           even a fully armed bot handed a PostgreSQL target does nothing — no journal, no executor,
           no review read. */
        var (bot, store, executor) = Build(Enabled(dryRun: false));

        await bot.RunAfterAnalysisAsync(Runtime(engine: "postgres"), Config(optedIn: true, engine: "postgres"),
            new[] { Finding(Target()) }, CancellationToken.None);

        Assert.Empty(store.Journaled);
        Assert.Empty(executor.Calls);
    }

    /* ---------------- evaluation journaling ---------------- */

    [Fact]
    public async Task ShadowMode_JournalsWouldForce_WithBothClosedGatesNamed_AndTouchesNoServer()
    {
        var (bot, store, executor) = Build(Enabled());

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { Finding(Target()) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionWouldForce, row.Action);
        Assert.Equal(PgPlanForceActionStore.ModeDryRun, row.Mode);
        Assert.Equal("dry_run,server_not_opted_in", row.Reasons);
        Assert.Equal(PgPlanForceActionStore.OutcomeLogged, row.Outcome);
        Assert.Equal(10.0, row.RegressionFactor);
        Assert.Empty(executor.Calls);
    }

    [Fact]
    public async Task ABlockedTarget_JournalsTheBlockers_AndNeverReachesTheExecutor()
    {
        var (bot, store, executor) = Build(Enabled(dryRun: false));

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target(psp: true)) }, CancellationToken.None);

        var row = Assert.Single(store.Journaled);
        Assert.Equal(PgPlanForceActionStore.ActionBlocked, row.Action);
        Assert.Equal("parameter_sensitivity_cofired", row.Reasons);
        Assert.Empty(executor.Calls);
    }

    [Fact]
    public async Task ASuppressedTarget_JournalsNothing()
    {
        var (bot, store, executor) = Build(Enabled());
        store.History = new ForcePlanBotHistory(DateTime.UtcNow.AddHours(-1), 0, 0);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { Finding(Target()) }, CancellationToken.None);

        Assert.Empty(store.Journaled);
        Assert.Empty(executor.Calls);
    }

    [Fact]
    public async Task TargetsAreDedupedAcrossFindings_AndCappedPerPass()
    {
        var (bot, store, _) = Build(Enabled());

        /* Two findings share query 1 (journaled once); 14 distinct queries total, cap is 10. */
        var first = Finding(Enumerable.Range(1, 8).Select(q => Target(queryId: q)).ToArray());
        var second = Finding(Enumerable.Range(1, 14).Select(q => Target(queryId: q)).ToArray());

        await bot.RunAfterAnalysisAsync(Runtime(), Config(), new[] { first, second }, CancellationToken.None);

        Assert.Equal(PlanForceBot.MaxTargetsPerPass, store.Journaled.Count);
        Assert.Equal(store.Journaled.Count, store.Journaled.Select(r => r.QueryId).Distinct().Count());
    }

    /* ---------------- the live force path ---------------- */

    [Fact]
    public async Task ALiveForce_JournalsIntentThenCompletion_LinkedByActionId()
    {
        var (bot, store, executor) = Build(Enabled(dryRun: false));

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        Assert.Equal("force:orders/42/7", Assert.Single(executor.Calls, c => c.StartsWith("force:", StringComparison.Ordinal)));
        Assert.Equal(2, store.Journaled.Count(r => r.Action == PgPlanForceActionStore.ActionForce));

        var intent = store.Journaled.Single(r => r.Outcome == PgPlanForceActionStore.OutcomeAttempting);
        var completion = store.Journaled.Single(r => r.Outcome == PgPlanForceActionStore.OutcomeSucceeded);
        Assert.Null(intent.RelatedActionId);
        Assert.Equal(intent.ActionId, completion.RelatedActionId);
        Assert.Equal(PgPlanForceActionStore.ModeLive, completion.Mode);
    }

    [Fact]
    public async Task AFailedForce_JournalsTheFailure_WithTheError()
    {
        var (bot, store, executor) = Build(Enabled(dryRun: false));
        executor.ForceResult = PlanForceExecutionResult.Failed("no plan with id 7");

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            new[] { Finding(Target()) }, CancellationToken.None);

        var completion = store.Journaled.Single(r => r.Outcome == PgPlanForceActionStore.OutcomeFailed);
        Assert.Equal("no plan with id 7", completion.Detail);
    }

    /* ---------------- the self-review path ---------------- */

    [Fact]
    public async Task AReviewUnforce_Executes_WhenBothGatesAreStillOpen()
    {
        var (bot, store, executor) = Build(Enabled(dryRun: false));
        store.Pending.Add(PendingForce(DateTime.UtcNow.AddHours(-2)));
        executor.VerifyResult = new PlanForceVerifyResult(
            PlanIsStillForced: true, ForceFailureCount: 0, LastForceFailureReason: null,
            ExecutionsSinceForce: 100, ObservedCpuPerExecUs: 49000);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            Array.Empty<AnalysisFinding>(), CancellationToken.None);

        Assert.Contains("unforce:orders/42/7", executor.Calls);
        var row = store.Journaled.Single(r => r.Action == PgPlanForceActionStore.ActionUnforce);
        Assert.Equal("not_net_benefit", row.Decision);
        Assert.Equal(PgPlanForceActionStore.OutcomeSucceeded, row.Outcome);
        Assert.Equal(99, row.RelatedActionId);
    }

    [Fact]
    public async Task AReviewUnforce_IsWithheld_WhenTheGlobalGateClosedAfterTheForce()
    {
        /* THE safety pin this class exists for: a live force is outstanding, the operator flips back
           to dry-run, the self-review says take it back — the verdict must JOURNAL as withheld and
           the executor must not be touched with a write. The verify READ still runs (it is what
           produced the verdict, and reads are safe everywhere). */
        var (bot, store, executor) = Build(Enabled(dryRun: true));
        store.Pending.Add(PendingForce(DateTime.UtcNow.AddHours(-2)));
        executor.VerifyResult = new PlanForceVerifyResult(true, 0, null, 100, 49000);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            Array.Empty<AnalysisFinding>(), CancellationToken.None);

        Assert.DoesNotContain(executor.Calls, c => c.StartsWith("unforce:", StringComparison.Ordinal));
        var row = store.Journaled.Single(r => r.Action == PgPlanForceActionStore.ActionUnforce);
        Assert.Equal(PgPlanForceActionStore.OutcomeLogged, row.Outcome);
        Assert.StartsWith("write withheld", row.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReviewUnforce_IsWithheld_WhenTheServerOptInWasRevoked()
    {
        var (bot, store, executor) = Build(Enabled(dryRun: false));
        store.Pending.Add(PendingForce(DateTime.UtcNow.AddHours(-2)));
        executor.VerifyResult = new PlanForceVerifyResult(true, 0, null, 100, 49000);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: false),
            Array.Empty<AnalysisFinding>(), CancellationToken.None);

        Assert.DoesNotContain(executor.Calls, c => c.StartsWith("unforce:", StringComparison.Ordinal));
        Assert.Equal(PgPlanForceActionStore.OutcomeLogged,
            store.Journaled.Single(r => r.Action == PgPlanForceActionStore.ActionUnforce).Outcome);
    }

    [Fact]
    public async Task AHealthyForceMidWindow_JournalsNothing()
    {
        /* KeepForced at a mid-window checkpoint is deliberately not journaled — an identical row per
           analysis pass between checkpoints would turn the audit trail into noise. */
        var (bot, store, executor) = Build(Enabled(dryRun: false));
        store.Pending.Add(PendingForce(DateTime.UtcNow.AddHours(-2)));
        executor.VerifyResult = new PlanForceVerifyResult(true, 0, null, 100, 10000);

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            Array.Empty<AnalysisFinding>(), CancellationToken.None);

        Assert.Empty(store.Journaled);
    }

    [Fact]
    public async Task AnUnansweredVerify_LeavesTheReviewPending()
    {
        var (bot, store, executor) = Build(Enabled(dryRun: false));
        store.Pending.Add(PendingForce(DateTime.UtcNow.AddHours(-2)));
        executor.VerifyResult = null;

        await bot.RunAfterAnalysisAsync(Runtime(), Config(optedIn: true),
            Array.Empty<AnalysisFinding>(), CancellationToken.None);

        Assert.Empty(store.Journaled);
        Assert.DoesNotContain(executor.Calls, c => c.StartsWith("unforce:", StringComparison.Ordinal));
    }
}

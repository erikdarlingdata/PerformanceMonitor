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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9879: the family stage starts the anomaly read beside the family reads when the pass hands it one. Every test holds
/// a read on a hook and releases it itself, so none of them depends on how long anything takes. The waits carry a limit
/// only so a broken stage fails the test and does not hang it.
/// </summary>
public sealed class AnalyzeServerDetectorBesideFamiliesTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    private readonly TaskCompletionSource _slowReadStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _slowReadStopped = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>A context whose coverage stamp found collection across the whole window.</summary>
    private static AnalysisContext Observed(int parallelism = PgFactCollector.MaxFamilyParallelism) => new()
    {
        FamilyParallelism = parallelism,
        Coverage = new WindowCoverage { NominalMs = 3_600_000, ObservedMs = 3_600_000, SampleCount = 4 }
    };

    private static Func<AnalysisContext, List<Fact>, Task> Quick(string key) =>
        (_, facts) =>
        {
            facts.Add(new Fact { Source = "test", Key = key });
            return Task.CompletedTask;
        };

    /// <summary>A family read that only its token ends, as a long store read does.</summary>
    private async Task CollectSlowThingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        _slowReadStarted.TrySetResult();
        try
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
        }
        finally
        {
            _slowReadStopped.TrySetResult();
        }
    }

    /// <summary>
    /// The anomaly read starts ahead of every family and holds one gate slot, so one fewer family runs beside it. It is
    /// running while the families are held, the stage waits for it after the families finish, and the stage's fact list
    /// holds the family facts in family order with the anomaly facts left for the pass to append.
    /// </summary>
    [Fact]
    public async Task TheAnomalyRead_StartsAheadOfTheFamilies_HoldsAGateSlot_AndTheStageWaitsForIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var running = 0;
        var finished = 0;
        var keys = Enumerable.Range(0, 6).Select(i => $"F{i}").ToArray();
        var releaseFamilies = Hook();
        var releaseAnomalyRead = Hook();
        var everyFamilyFinished = Hook();
        var anomalyReadStarted = false;
        var familiesRunningWhenItStarted = -1;

        Func<AnalysisContext, List<Fact>, Task> Held(string key) =>
            async (_, facts) =>
            {
                Interlocked.Increment(ref running);
                await releaseFamilies.Task.WaitAsync(Limit, ct);
                facts.Add(new Fact { Source = "test", Key = key });
                if (Interlocked.Increment(ref finished) == keys.Length)
                    everyFamilyFinished.TrySetResult();
            };

        var context = Observed();
        var beside = new AnomalyReadBesideFamilies(async _ =>
        {
            familiesRunningWhenItStarted = Volatile.Read(ref running);
            anomalyReadStarted = true;
            await releaseAnomalyRead.Task.WaitAsync(Limit, ct);
            return [new Fact { Source = "test", Key = "ANOMALY" }];
        });
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact> { new() { Source = "test", Key = "coverage" } };

        var stage = PgFactCollector.RunFamiliesAsync(context, facts, keys.Select(Held).ToArray());

        /* The stage came back at its first wait, so every read that starts without waiting has started: the anomaly read,
           ahead of every family, and one family for each slot it left free. */
        Assert.True(anomalyReadStarted);
        Assert.Equal(0, familiesRunningWhenItStarted);
        Assert.Equal(PgFactCollector.MaxFamilyParallelism - 1, Volatile.Read(ref running));
        Assert.False(stage.IsCompleted);
        Assert.NotNull(beside.Run);
        Assert.False(beside.Run.IsCompleted);

        /* Every family finishes, and the stage is still waiting for the anomaly read. */
        releaseFamilies.SetResult();
        await everyFamilyFinished.Task.WaitAsync(Limit, ct);
        Assert.False(stage.IsCompleted);

        releaseAnomalyRead.SetResult();
        await stage.WaitAsync(Limit, ct);

        Assert.Equal(new[] { "coverage" }.Concat(keys), facts.Select(f => f.Key));
        Assert.Equal(new[] { "ANOMALY" }, (await beside.Run).Select(f => f.Key));
    }

    /// <summary>A window the coverage stamp found no collection in, or one with no stamp, never starts the anomaly read.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnUnobservedWindow_NeverStartsTheAnomalyRead(bool stamped)
    {
        var calls = 0;
        var context = new AnalysisContext
        {
            FamilyParallelism = PgFactCollector.MaxFamilyParallelism,
            Coverage = stamped ? WindowCoverage.Unobserved(3_600_000) : null
        };
        var beside = new AnomalyReadBesideFamilies(_ =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(new List<Fact>());
        });
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact>();

        await PgFactCollector.RunFamiliesAsync(context, facts, [Quick("A"), Quick("B"), Quick("C")]);

        Assert.Equal(0, calls);
        Assert.Null(beside.Run);
        Assert.Equal(new[] { "A", "B", "C" }, facts.Select(f => f.Key));
    }

    /// <summary>A stage that reads its families one at a time, which is the scheduled pass, leaves the anomaly read to the pass.</summary>
    [Fact]
    public async Task ASerialStage_LeavesTheAnomalyReadToThePass()
    {
        var calls = 0;
        var context = Observed(parallelism: 1);
        var beside = new AnomalyReadBesideFamilies(_ =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(new List<Fact>());
        });
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact>();

        await PgFactCollector.RunFamiliesAsync(context, facts, [Quick("A"), Quick("B"), Quick("C")]);

        Assert.Equal(0, calls);
        Assert.Null(beside.Run);
        Assert.Equal(new[] { "A", "B", "C" }, facts.Select(f => f.Key));
    }

    /// <summary>
    /// The anomaly read's failures go to its own list, before and after the families merge theirs, so the pass's list
    /// holds the family failures alone and in family order when the stage ends.
    /// </summary>
    [Fact]
    public async Task TheAnomalyReadsFailures_StayOutOfThePassList_AndTheFamilyFailuresKeepFamilyOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        var releaseAnomalyRead = Hook();

        Func<AnalysisContext, List<Fact>, Task> Failing(string family) =>
            (context, _) =>
            {
                context.RecordCollectionFailure(family, "read", CollectionFailureOutcome.Timeout, new TimeoutException());
                return Task.CompletedTask;
            };

        var context = Observed();
        var beside = new AnomalyReadBesideFamilies(async c =>
        {
            c.RecordCollectionFailure("anomaly", "early", CollectionFailureOutcome.Error, new InvalidOperationException());
            await releaseAnomalyRead.Task.WaitAsync(Limit, ct);
            c.RecordCollectionFailure("anomaly", "late", CollectionFailureOutcome.Error, new InvalidOperationException());
            return [];
        });
        context.AnomalyReadBesideFamilies = beside;

        var stage = PgFactCollector.RunFamiliesAsync(context, [], [Failing("first"), Failing("second"), Failing("third")]);
        releaseAnomalyRead.SetResult();
        await stage.WaitAsync(Limit, ct);

        Assert.Equal(new[] { "first", "second", "third" }, context.CollectionFailures.Select(f => f.Family));
        Assert.Equal(new[] { "early", "late" }, beside.Failures.Select(f => f.Read));
    }

    /// <summary>
    /// The family deadline stops the family that runs under it and not the anomaly read: the anomaly read holds the pass's
    /// token, which is still live after the cut, and it finishes with its facts.
    /// </summary>
    [Fact]
    public async Task AFamilyDeadlineCut_StopsTheFamily_AndNotTheAnomalyRead()
    {
        var ct = TestContext.Current.CancellationToken;
        using var pass = new CancellationTokenSource();
        using var deadline = new CancellationTokenSource();
        var releaseAnomalyRead = Hook();
        CancellationToken anomalyReadToken = default;

        var context = Observed();
        context.CancellationToken = pass.Token;
        context.FamilyDeadline = deadline.Token;
        var beside = new AnomalyReadBesideFamilies(async c =>
        {
            anomalyReadToken = c.CancellationToken;
            await releaseAnomalyRead.Task.WaitAsync(Limit, ct);
            c.CancellationToken.ThrowIfCancellationRequested();
            return [new Fact { Source = "test", Key = "ANOMALY" }];
        });
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact>();

        var stage = PgFactCollector.RunFamiliesAsync(context, facts,
            [Quick("A"), CollectSlowThingFactsAsync, Quick("B")],
            family => family.Method.Name == nameof(CollectSlowThingFactsAsync));

        await _slowReadStarted.Task.WaitAsync(Limit, ct);
        deadline.Cancel();
        await _slowReadStopped.Task.WaitAsync(Limit, ct);

        Assert.Equal(pass.Token, anomalyReadToken);
        Assert.False(anomalyReadToken.IsCancellationRequested);
        Assert.False(stage.IsCompleted);

        releaseAnomalyRead.SetResult();
        await stage.WaitAsync(Limit, ct);

        Assert.Equal(new[] { "ANOMALY" }, (await beside.Run!).Select(f => f.Key));
        Assert.Single(context.FamiliesLeftOutForTime);
        Assert.Equal(new[] { "A", "B" }, facts.Select(f => f.Key));
    }

    /// <summary>An anomaly read that fails does not end the stage: the family facts are kept and the failure waits in its task for the pass.</summary>
    [Fact]
    public async Task AnAnomalyReadThatFails_LeavesItsFailureForThePass_AndTheStageKeepsTheFamilyFacts()
    {
        var context = Observed();
        var beside = new AnomalyReadBesideFamilies(_ => Task.FromException<List<Fact>>(new OperationCanceledException("stopped")));
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact>();

        await PgFactCollector.RunFamiliesAsync(context, facts, [Quick("A"), Quick("B"), Quick("C")]);

        Assert.Equal(new[] { "A", "B", "C" }, facts.Select(f => f.Key));
        Assert.NotNull(beside.Run);
        await Assert.ThrowsAsync<OperationCanceledException>(() => beside.Run);
    }
}

/// <summary>
/// #9879, the service's side on a real store: which passes hand the detector to the family stage, and where the pass puts
/// what the detector produced. The fact collector and the detector are stand-ins, so the family stage is held on a hook
/// and the window's coverage is stated, and the family reads still run through the real family stage with the context
/// the service built.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalyzeServerDetectorBesideFamiliesLiveTests
{
    private const string ServerName = "darling-analyze-detector-beside-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(15);
    private const double FourHoursMs = 4 * 3_600_000.0;

    private static WindowCoverage ObservedWindow() => new() { NominalMs = FourHoursMs, ObservedMs = FourHoursMs, SampleCount = 17 };

    private static Func<AnalysisContext, List<Fact>, Task> Quick(string key) =>
        (_, facts) =>
        {
            facts.Add(new Fact { Source = "test", Key = key });
            return Task.CompletedTask;
        };

    private static Func<AnalysisContext, List<Fact>, Task> Failing(string family) =>
        (context, _) =>
        {
            context.RecordCollectionFailure(family, "read", CollectionFailureOutcome.Timeout, new TimeoutException());
            return Task.CompletedTask;
        };

    /// <summary>
    /// Stands in for the SQL Server fact collector: states the window's coverage, runs the given reads through the real
    /// family stage with the context the service built, and says when that stage has ended.
    /// </summary>
    private sealed class StageProbeCollector(WindowCoverage coverage, params Func<AnalysisContext, List<Fact>, Task>[] families) : IFactCollector
    {
        private int _stageEnded;

        public bool StageEnded => Volatile.Read(ref _stageEnded) != 0;

        public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
        {
            context.CollectionFamilyCount = families.Length;
            context.Coverage = coverage;
            var facts = new List<Fact>();
            await PgFactCollector.RunFamiliesAsync(context, facts, families);
            Volatile.Write(ref _stageEnded, 1);
            return facts;
        }
    }

    /// <summary>Stands in for the SQL Server anomaly detector and records the state of the pass at the moment it starts.</summary>
    private sealed class ProbeDetector : IAnomalyDetector
    {
        public int Calls;
        public bool? StageHadEnded;
        public bool TokenWasSignalled;
        public bool DeadlineHadFired;
        public bool RecordsAFailure;
        public StageProbeCollector? Collector;
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
        {
            Interlocked.Increment(ref Calls);
            StageHadEnded = Collector?.StageEnded;
            TokenWasSignalled = context.CancellationToken.IsCancellationRequested;
            DeadlineHadFired = context.FamilyDeadline.IsCancellationRequested;
            if (RecordsAFailure)
                context.RecordCollectionFailure("anomaly_probe", nameof(DetectAnomaliesAsync), CollectionFailureOutcome.Error, new InvalidOperationException());
            Started.TrySetResult();
            return Task.FromResult(new List<Fact> { new() { Source = "test", Key = "FB9879A_ANOMALY" } });
        }
    }

    private static DarlingAnalysisService ServiceOver(NpgsqlDataSource postgres, StageProbeCollector collector, ProbeDetector detector)
    {
        detector.Collector = collector;
        return new DarlingAnalysisService(postgres, sqlServerCollectorWrapper: _ => collector, sqlServerDetectorWrapper: _ => detector);
    }

    /// <summary>
    /// An on-demand pass starts the detector while a family read is still held, and a scheduled pass starts it after the
    /// family stage has ended. The held family waits for the detector itself, so a pass that runs the detector after the
    /// families cannot get past it.
    /// </summary>
    [Fact]
    public async Task AnOnDemandPass_StartsTheDetectorWhileAFamilyIsHeld_AndAScheduledPassStartsItAfterTheFamilies()
    {
        await OnALiveStoreAsync(async (postgres, ct) =>
        {
            var detector = new ProbeDetector();
            Func<AnalysisContext, List<Fact>, Task> held = async (_, facts) =>
            {
                await detector.Started.Task.WaitAsync(Limit, ct);
                facts.Add(new Fact { Source = "test", Key = "FB9879A_HELD" });
            };
            var onDemand = ServiceOver(postgres, new StageProbeCollector(ObservedWindow(), held, Quick("FB9879A_QUICK")), detector);

            await onDemand.AnalyzeAsync(ServerId, ServerName, 4, ct, persistCollectionCaveats: false);

            Assert.Equal(1, detector.Calls);
            Assert.False(detector.StageHadEnded);
            Assert.Equal(3, onDemand.LastFactCount);

            var scheduledDetector = new ProbeDetector();
            var scheduled = ServiceOver(postgres,
                new StageProbeCollector(ObservedWindow(), Quick("FB9879A_FIRST"), Quick("FB9879A_SECOND")), scheduledDetector);

            await scheduled.AnalyzeAsync(ServerId, ServerName, 4, ct, persistCollectionCaveats: true);

            Assert.Equal(1, scheduledDetector.Calls);
            Assert.True(scheduledDetector.StageHadEnded);
            Assert.Equal(3, scheduled.LastFactCount);
        });
    }

    /// <summary>An on-demand pass over a window with no observed collection never starts the detector.</summary>
    [Fact]
    public async Task AnOnDemandPassOverAnUnobservedWindow_NeverStartsTheDetector()
    {
        await OnALiveStoreAsync(async (postgres, ct) =>
        {
            var detector = new ProbeDetector();
            var service = ServiceOver(postgres,
                new StageProbeCollector(WindowCoverage.Unobserved(FourHoursMs), Quick("FB9879A_FIRST"), Quick("FB9879A_SECOND")), detector);

            await service.AnalyzeAsync(ServerId, ServerName, 4, ct, persistCollectionCaveats: false);

            Assert.Equal(0, detector.Calls);
            Assert.NotNull(service.WindowEmptyMessage);
        });
    }

    /// <summary>
    /// A failure the detector records beside the families lands after every family failure in the pass's list, and the
    /// ledger's record of the pass holds the family failures alone. The family budget of this pass has already ended, and
    /// the detector still runs once, through a token that is not signalled.
    /// </summary>
    [Fact]
    public async Task ADetectorFailure_FollowsTheFamilyFailures_StaysOutOfTheLedger_AndAnEndedFamilyBudgetDoesNotStopTheDetector()
    {
        await OnALiveStoreAsync(async (postgres, ct) =>
        {
            var detector = new ProbeDetector { RecordsAFailure = true };
            var service = ServiceOver(postgres,
                new StageProbeCollector(ObservedWindow(), Failing("fb9879a_first"), Failing("fb9879a_second")), detector);
            service.OnDemandFamilyBudget = TimeSpan.Zero;

            await service.AnalyzeAsync(ServerId, ServerName, 4, ct, persistCollectionCaveats: false);

            Assert.Equal(1, detector.Calls);
            Assert.False(detector.StageHadEnded);
            Assert.True(detector.DeadlineHadFired);
            Assert.False(detector.TokenWasSignalled);
            Assert.Equal(1, service.LastFactCount);
            Assert.Equal(new[] { "fb9879a_first", "fb9879a_second", "anomaly_probe" }, service.LastCollectionFailures.Select(f => f.Family));

            Assert.True(CollectionCaveatLedger.Shared.TryGetLatest(ServerId, out var snapshot));
            Assert.Equal(new[] { "fb9879a_first", "fb9879a_second" }, snapshot!.Failures.Select(f => f.Family));
        });
    }

    /// <summary>
    /// Registers the test's server with more than a day of wait history and a reading every fifteen minutes across the
    /// last four hours, so a pass gets past its history gate, runs the body, and removes the rows again.
    /// </summary>
    private static async Task OnALiveStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live detector-beside-families test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);

            var now = DateTime.UtcNow;
            await PlantWaitAsync(connection, Naive(now.AddHours(-30)), ct);
            for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
                await PlantWaitAsync(connection, Naive(now.AddMinutes(-minutesAgo)), ct);

            await body(postgres, ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Naive-UTC, the Kind every timestamp column in this store is bound with.</summary>
    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, 'FB9879A_BENIGN_WAIT', 50, 100)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.analysis_collection_caveats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9972: a baseline compute started for a pass with a detector deadline runs on the provider's own token. The deadline
/// stops the caller's wait, the compute finishes and is stored, and the next read is a hit. Every compute here is held by
/// a hook the test releases, and the test waits on the compute's own task. Nothing waits on a clock.
/// </summary>
public sealed class BaselineComputeKeptPastDeadlineTests
{
    private const int ServerId = -997201;
    private static readonly DateTime Key = new(2026, 11, 2, 10, 0, 0);
    private static readonly TimeSpan OwnLimit = TimeSpan.FromMinutes(1);

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /* Never connected to: every compute in this class is passed in. */
    private static NpgsqlDataSource NoStore() => NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1;Command Timeout=1");

    private static PgBaselineProvider.CachedBaseline Computed() => new()
    {
        ComputedAt = Key,
        RealTime = DateTime.UtcNow,
        Buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>()
    };

    private static Task<PgBaselineProvider.CachedBaseline> MustNotCompute(CancellationToken _) =>
        throw new InvalidOperationException("The entry was stored, so this read computes nothing.");

    [Fact]
    public async Task ACallerItsDeadlineStops_LeavesTheComputeRunning_AndTheNextReadIsAHit()
    {
        await using var store = NoStore();
        var provider = new PgBaselineProvider(store);
        var (started, release) = (Hook(), Hook());
        var computes = 0;
        var ownToken = CancellationToken.None;

        using var deadline = new CancellationTokenSource();
        var read = provider.GetOrComputeKeptAsync(ServerId, "k", Key, async own =>
        {
            Interlocked.Increment(ref computes);
            ownToken = own;
            started.TrySetResult();
            await release.Task.WaitAsync(own);
            return Computed();
        }, OwnLimit, deadline.Token);

        await started.Task;
        deadline.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);

        /* The compute is not the caller's: its token is not the deadline's and is not signalled. */
        Assert.NotEqual(deadline.Token, ownToken);
        Assert.False(ownToken.IsCancellationRequested);
        var running = provider.KeptComputes.Running();
        Assert.Single(running);
        Assert.Equal(1, provider.KeptComputes.InFlight);

        release.SetResult();
        await Task.WhenAll(running);
        Assert.Equal(0, provider.KeptComputes.InFlight);
        Assert.Empty(provider.KeptComputes.Running());

        var again = await provider.GetOrComputeKeptAsync(ServerId, "k", Key, MustNotCompute, OwnLimit, CancellationToken.None);
        Assert.NotNull(again.Buckets);
        Assert.Equal(1, computes);

        /* The read a pass with no deadline makes finds the same entry. */
        var unshared = await provider.GetOrComputeUnsharedAsync(ServerId, "k", Key, () => MustNotCompute(CancellationToken.None));
        Assert.Same(again, unshared);
    }

    [Fact]
    public async Task TwoProvidersOfOneSharedTier_JoinOneCompute_AndBothGetItsAnswer()
    {
        await using var store = NoStore();
        var shared = new BaselineCache();
        var (first, second) = (new PgBaselineProvider(store, null, shared), new PgBaselineProvider(store, null, shared));
        var (started, release) = (Hook(), Hook());
        var computes = 0;

        async Task<PgBaselineProvider.CachedBaseline> Compute(CancellationToken own)
        {
            Interlocked.Increment(ref computes);
            started.TrySetResult();
            await release.Task.WaitAsync(own);
            return Computed();
        }

        var one = first.GetOrComputeKeptAsync(ServerId, "k", Key, Compute, OwnLimit, CancellationToken.None);
        await started.Task;
        var two = second.GetOrComputeKeptAsync(ServerId, "k", Key, Compute, OwnLimit, CancellationToken.None);
        Assert.Equal(1, shared.KeptComputes.InFlight);

        release.SetResult();
        Assert.Same(await one, await two);
        Assert.Equal(1, computes);
        Assert.Equal(0, shared.KeptComputes.InFlight);

        /* A provider with another data source (another role) starts its own compute for the same key. */
        await using var otherStore = NoStore();
        var other = new PgBaselineProvider(otherStore, null, new BaselineCache());
        await other.GetOrComputeKeptAsync(ServerId, "k", Key, Compute, OwnLimit, CancellationToken.None);
        Assert.Equal(2, computes);
    }

    [Fact]
    public async Task AComputePastTheLimit_RunsOnTheCallersToken_IsCancelledWithIt_AndStoresNothing()
    {
        await using var store = NoStore();
        var provider = new PgBaselineProvider(store);
        var release = Hook();

        /* As many held computes as the limit allows, each on its own key. */
        var kept = new List<Task<PgBaselineProvider.CachedBaseline>>();
        for (var i = 0; i < KeptBaselineComputes.MaxInFlight; i++)
        {
            var started = Hook();
            kept.Add(provider.GetOrComputeKeptAsync(ServerId, $"k{i}", Key, async own =>
            {
                started.TrySetResult();
                await release.Task.WaitAsync(own);
                return Computed();
            }, OwnLimit, CancellationToken.None));
            await started.Task;
        }

        Assert.Equal(KeptBaselineComputes.MaxInFlight, provider.KeptComputes.InFlight);

        /* One more key: no slot is free, so its compute is the caller's. */
        using var deadline = new CancellationTokenSource();
        var pastStarted = Hook();
        var pastToken = CancellationToken.None;
        var past = provider.GetOrComputeKeptAsync(ServerId, "past", Key, async token =>
        {
            pastToken = token;
            pastStarted.TrySetResult();
            await release.Task.WaitAsync(token);
            return Computed();
        }, OwnLimit, deadline.Token);
        await pastStarted.Task;
        Assert.Equal(deadline.Token, pastToken);
        Assert.Equal(KeptBaselineComputes.MaxInFlight, provider.KeptComputes.InFlight);

        deadline.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => past);

        release.SetResult();
        await Task.WhenAll(kept);
        Assert.Equal(0, provider.KeptComputes.InFlight);

        /* The kept computes are stored. The one past the limit is not, so its key computes again. */
        for (var i = 0; i < KeptBaselineComputes.MaxInFlight; i++)
            await provider.GetOrComputeKeptAsync(ServerId, $"k{i}", Key, MustNotCompute, OwnLimit, CancellationToken.None);
        var recomputed = 0;
        await provider.GetOrComputeKeptAsync(ServerId, "past", Key, _ =>
        {
            recomputed++;
            return Task.FromResult(Computed());
        }, OwnLimit, CancellationToken.None);
        Assert.Equal(1, recomputed);
    }

    [Fact]
    public async Task AComputeThatFaults_IsAnsweredToItsWaitingCaller_AndNotKeptForTheNextPass()
    {
        await using var store = NoStore();
        var shared = new BaselineCache();
        var provider = new PgBaselineProvider(store, null, shared);

        var faulted = await provider.GetOrComputeKeptAsync(ServerId, "k", Key, _ => Task.FromResult(new PgBaselineProvider.CachedBaseline
        {
            ComputedAt = Key,
            RealTime = DateTime.UtcNow,
            Faulted = true
        }), OwnLimit, CancellationToken.None);
        Assert.True(faulted.Faulted);
        Assert.Equal(0, shared.Count);

        /* The same provider's second read in the pass is a hit on the faulted answer. Another pass's provider computes. */
        Assert.Same(faulted, await provider.GetOrComputeKeptAsync(ServerId, "k", Key, MustNotCompute, OwnLimit, CancellationToken.None));
        var next = await new PgBaselineProvider(store, null, shared).GetOrComputeKeptAsync(
            ServerId, "k", Key, _ => Task.FromResult(Computed()), OwnLimit, CancellationToken.None);
        Assert.NotNull(next.Buckets);
        Assert.Equal(1, shared.Count);
    }
}

/// <summary>
/// #9972 against a real store: an on-demand pass whose wait detector's baseline compute is held past the detector
/// deadline, and the scheduled pass's cancel. The compute is held by the provider's own hook and the test waits on the
/// compute's task.
/// </summary>
[Collection("live-postgres")]
public sealed class BaselineComputeKeptPastDeadlineLiveTests
{
    private const int ServerId = -997202;
    private const string ServerName = "baseline-kept-9972";
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime at, long waitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, 'FB9972_TEST_WAIT', 50, $5)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A registered SQL Server target with more than a day of wait history in one hour-of-week bucket and three
    /// heavy collections a week later at the same hour: one ANOMALY_WAIT_PROFILE fact, which roots a finding. Returns
    /// the start of the incident's hour.</summary>
    private static async Task<DateTime> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        using (var register = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)", connection))
        {
            register.Parameters.AddWithValue(ServerId);
            register.Parameters.AddWithValue(ServerName);
            register.Parameters.AddWithValue(Naive(DateTime.UtcNow));
            await register.ExecuteNonQueryAsync(ct);
        }

        var day = DateTime.UtcNow.Date.AddDays(-8);
        while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
        var historyStart = Naive(day.AddHours(10));
        for (var i = 0; i < 12; i++)
            await PlantWaitAsync(connection, historyStart.AddMinutes(5 * i), (i == 5 || i == 6) ? 0L : 60_000L, ct);
        var incidentStart = historyStart.AddDays(7);
        for (var minutes = 5; minutes <= 15; minutes += 5)
            await PlantWaitAsync(connection, incidentStart.AddMinutes(minutes), 200_000L, ct);

        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        return incidentStart;
    }

    [Fact]
    public async Task ABaselineHeldPastTheDetectorDeadline_IsStoredWhenItFinishes_AndTheSameCallAgainComputesNoneAndHasTheFinding()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live kept-baseline test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var incidentStart = await SeedAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* An on-demand pass anchored on the incident's hour (an as_of call), one detector at a time. */
        AnalysisContext Pass(CancellationToken deadline) => new()
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = incidentStart,
            TimeRangeEnd = incidentStart.AddHours(1),
            AsOfUtc = incidentStart.AddHours(1),
            ServerUtcOffset = TimeSpan.Zero,
            DetectorParallelism = 1,
            DetectorDeadline = deadline,
            CancellationToken = ct
        };

        /* Each call gets its own service and provider over the process's one shared tier, as the MCP host builds them. */
        var shared = new BaselineCache();
        PgBaselineProvider? firstBaselines = null;
        var first = new DarlingAnalysisService(postgres, baselineCache: shared, sqlServerDetectorWrapper: inner =>
        {
            firstBaselines = ((PgAnomalyDetector)inner).Baselines;
            return inner;
        });

        /* The wait detector's baseline compute is held. The CPU detector before it finishes. */
        var (started, release) = (Hook(), Hook());
        var firstWaitComputes = 0;
        firstBaselines!.BeforeCompute = async (metric, token) =>
        {
            if (metric != MetricNames.WaitMsPerSec)
                return;
            Interlocked.Increment(ref firstWaitComputes);
            started.TrySetResult();
            await release.Task.WaitAsync(token);
        };

        using var deadline = new CancellationTokenSource();
        var cut = Pass(deadline.Token);
        var run = first.AnalyzeAsync(cut);
        await started.Task.WaitAsync(Limit, ct);
        deadline.Cancel();
        var cutFindings = await run.WaitAsync(Limit, ct);

        Assert.Contains("wait", cut.DetectorsLeftOutForTime);
        Assert.DoesNotContain("cpu", cut.DetectorsLeftOutForTime);
        Assert.DoesNotContain(cutFindings, finding => finding.RootFactKey == "ANOMALY_WAIT_PROFILE");

        /* The pass has returned and the hold is released: the compute it was waiting for ends, and is waited for by its task. */
        var running = shared.KeptComputes.Running();
        release.SetResult();
        await Task.WhenAll(running).WaitAsync(Limit, ct);
        Assert.Equal(0, shared.KeptComputes.InFlight);

        /* The same call again, from a new service: no wait baseline is computed, no detector is left out, and the
           wait detector's finding is there. */
        PgBaselineProvider? secondBaselines = null;
        var second = new DarlingAnalysisService(postgres, baselineCache: shared, sqlServerDetectorWrapper: inner =>
        {
            secondBaselines = ((PgAnomalyDetector)inner).Baselines;
            return inner;
        });
        var secondWaitComputes = 0;
        secondBaselines!.BeforeCompute = (metric, _) =>
        {
            if (metric == MetricNames.WaitMsPerSec)
                Interlocked.Increment(ref secondWaitComputes);
            return Task.CompletedTask;
        };

        using var notFired = new CancellationTokenSource();
        var again = Pass(notFired.Token);
        var againFindings = await second.AnalyzeAsync(again).WaitAsync(Limit, ct);

        Assert.Equal(0, secondWaitComputes);
        Assert.Equal(1, firstWaitComputes);
        Assert.Empty(again.DetectorsLeftOutForTime);
        Assert.Contains(againFindings, finding => finding.RootFactKey == "ANOMALY_WAIT_PROFILE");
    }

    [Fact]
    public async Task AScheduledPassStoppedDuringABaselineCompute_CancelsTheCompute_AndStoresNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live scheduled-pass baseline test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var incidentStart = await SeedAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var shared = new BaselineCache();
        var provider = new PgBaselineProvider(postgres, null, shared);
        var detector = new PgAnomalyDetector(postgres, provider);

        var started = Hook();
        var computes = 0;
        var computeToken = CancellationToken.None;
        provider.BeforeCompute = async (_, token) =>
        {
            Interlocked.Increment(ref computes);
            computeToken = token;
            started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        };

        /* One detector that reads its baseline the way the real ones do, on a pass with no detector deadline. */
        Task ReadWaitBaseline(AnalysisContext context, List<Fact> facts) => provider.GetBucketMapAsync(
            context.ServerId, MetricNames.WaitMsPerSec, context.TimeRangeStart, context.TimeRangeEnd, context.CancellationToken);

        using var pass = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var scheduled = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = incidentStart,
            TimeRangeEnd = incidentStart.AddHours(1),
            ServerUtcOffset = TimeSpan.Zero,
            CancellationToken = pass.Token
        };
        var run = detector.RunDetectorsAsync(scheduled, [], [ReadWaitBaseline], parallelism: 1);
        await started.Task.WaitAsync(Limit, ct);
        pass.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Limit, ct));

        /* The compute ran on the pass's token, no compute outlives the pass, and nothing was stored. */
        Assert.Equal(pass.Token, computeToken);
        Assert.Empty(shared.KeptComputes.Running());
        Assert.Equal(0, shared.KeptComputes.InFlight);
        Assert.Equal(0, shared.Count);

        /* So the next read computes it. */
        provider.BeforeCompute = (_, _) =>
        {
            Interlocked.Increment(ref computes);
            return Task.CompletedTask;
        };
        await provider.GetBucketMapAsync(ServerId, MetricNames.WaitMsPerSec, incidentStart, incidentStart.AddHours(1), ct);
        Assert.Equal(2, computes);
        Assert.Equal(1, shared.Count);
    }
}

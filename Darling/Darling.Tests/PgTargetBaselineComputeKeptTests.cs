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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9972: a keyed baseline set (the PostgreSQL-target detectors' candidate statements) asked for on a pass with a detector
/// deadline is one kept compute per chunk of misses. The deadline stops the caller's wait, the compute finishes and stores
/// every key's entry, and the same set again is a hit. Every compute here is passed in and held by a hook the test
/// releases, and the test waits on the compute's own task. Nothing waits on a clock.
/// </summary>
public sealed class KeyedBaselineComputeKeptPastDeadlineTests
{
    private const int ServerId = -997203;
    private static readonly DateTime Now = new(2026, 11, 2, 10, 30, 0);
    private static readonly TimeSpan OwnLimit = TimeSpan.FromMinutes(1);
    private static readonly string[] Candidates = ["101", "202"];

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /* Never connected to: every compute in this class is passed in. */
    private static NpgsqlDataSource NoStore() => NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1;Command Timeout=1");

    /// <summary>A chunk's answer with a bucket map for every member of the set.</summary>
    private static (Dictionary<long, Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>>? ByMember, LocalClockWindow Clock, int? UtcOffsetMinutes, string? TimeZoneId, bool Faulted) Rows(
        string[] set, DateTime key)
    {
        var byMember = new Dictionary<long, Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>>();
        for (var member = 1; member <= set.Length; member++)
            byMember[member] = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>();
        return (byMember, LocalClockWindow.Utc(key), null, null, false);
    }

    private static readonly PgBaselineProvider.KeyedChunkComputeOn MustNotCompute =
        (_, _) => throw new InvalidOperationException("Every key was stored, so this read computes nothing.");

    [Fact]
    public async Task AKeyedSetItsCallersDeadlineStops_GoesOnComputing_IsStoredForEveryKey_AndTheSameSetAgainIsAHit()
    {
        await using var store = NoStore();
        var shared = new BaselineCache();
        var provider = new PgTargetBaselineProvider(store, null, shared);
        var key = provider.RoundedKeyTime(MetricNames.PgStatementShare, Now);
        var (started, release) = (Hook(), Hook());
        var computes = 0;
        var ownToken = CancellationToken.None;
        string[]? computedSet = null;

        PgBaselineProvider.KeyedChunkComputeOn compute = async (set, own) =>
        {
            Interlocked.Increment(ref computes);
            ownToken = own;
            computedSet = set;
            started.TrySetResult();
            await release.Task.WaitAsync(own);
            return Rows(set, key);
        };

        using var deadline = new CancellationTokenSource();
        var read = provider.GetOrComputeKeyedBaselinesAsync(
            ServerId, MetricNames.PgStatementShare, Candidates, Now, compute, keep: true, OwnLimit, deadline.Token);
        await started.Task;
        deadline.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);

        /* The compute is not the caller's: its token is not the deadline's and is not signalled, and nothing is stored yet. */
        Assert.NotEqual(deadline.Token, ownToken);
        Assert.False(ownToken.IsCancellationRequested);
        var running = shared.KeptComputes.Running();
        Assert.Single(running);
        Assert.Equal(1, shared.KeptComputes.InFlight);
        Assert.Equal(0, shared.Count);

        release.SetResult();
        await Task.WhenAll(running);
        Assert.Equal(0, shared.KeptComputes.InFlight);
        Assert.Empty(shared.KeptComputes.Running());
        Assert.Equal(Candidates, computedSet);
        Assert.Equal(Candidates.Length, shared.Count);

        /* The same candidates again, from the next pass's provider over the same shared tier: every key is a hit. */
        var next = new PgTargetBaselineProvider(store, null, shared);
        var again = await next.GetOrComputeKeyedBaselinesAsync(
            ServerId, MetricNames.PgStatementShare, Candidates, Now, MustNotCompute, keep: true, OwnLimit, CancellationToken.None);
        Assert.Equal(Candidates, again.Keys.Order(StringComparer.Ordinal).ToArray());
        Assert.All(again.Values, entry => Assert.NotNull(entry.Buckets));
        Assert.Equal(1, computes);

        /* The read a pass with no deadline makes finds the same entries. */
        var unkept = await next.GetOrComputeKeyedBaselinesAsync(
            ServerId, MetricNames.PgStatementShare, Candidates, Now, MustNotCompute, keep: false, OwnLimit, CancellationToken.None);
        Assert.Same(again["101"], unkept["101"]);
    }

    [Fact]
    public async Task TheSameKeyedSetAskedAgainWhileItsComputeRuns_JoinsIt_AndBothCallersGetItsEntries()
    {
        await using var store = NoStore();
        var shared = new BaselineCache();
        var (first, second) = (new PgTargetBaselineProvider(store, null, shared), new PgTargetBaselineProvider(store, null, shared));
        var key = first.RoundedKeyTime(MetricNames.PgStatementShare, Now);
        var (started, release) = (Hook(), Hook());
        var computes = 0;

        PgBaselineProvider.KeyedChunkComputeOn compute = async (set, own) =>
        {
            Interlocked.Increment(ref computes);
            started.TrySetResult();
            await release.Task.WaitAsync(own);
            return Rows(set, key);
        };

        var one = first.GetOrComputeKeyedBaselinesAsync(
            ServerId, MetricNames.PgStatementShare, Candidates, Now, compute, keep: true, OwnLimit, CancellationToken.None);
        await started.Task;
        var two = second.GetOrComputeKeyedBaselinesAsync(
            ServerId, MetricNames.PgStatementShare, Candidates, Now, compute, keep: true, OwnLimit, CancellationToken.None);
        Assert.Equal(1, shared.KeptComputes.InFlight);

        release.SetResult();
        var (fromOne, fromTwo) = (await one, await two);
        Assert.Equal(1, computes);
        Assert.Same(fromOne["101"], fromTwo["101"]);
        Assert.Same(fromOne["202"], fromTwo["202"]);
        Assert.Equal(0, shared.KeptComputes.InFlight);

        /* Another set of the same series is another compute. */
        string[] others = ["303"];
        await second.GetOrComputeKeyedBaselinesAsync(
            ServerId, MetricNames.PgStatementShare, others, Now, compute, keep: true, OwnLimit, CancellationToken.None);
        Assert.Equal(2, computes);
    }

    [Fact]
    public async Task KeptComputesOfBothEnginesCountTowardOneLimit_AndAKeyedSetPastIt_StopsWithItsCallerAndStoresNothing()
    {
        await using var store = NoStore();
        var shared = new BaselineCache();
        var sqlServer = new PgBaselineProvider(store, null, shared);
        var pgTarget = new PgTargetBaselineProvider(store, null, shared);
        var key = pgTarget.RoundedKeyTime(MetricNames.PgStatementShare, Now);
        var release = Hook();

        /* As many held computes as the limit allows, each an unkeyed series of the SQL Server provider. */
        var kept = new List<Task<PgBaselineProvider.CachedBaseline>>();
        for (var i = 0; i < KeptBaselineComputes.MaxInFlight; i++)
        {
            var started = Hook();
            kept.Add(sqlServer.GetOrComputeKeptAsync(ServerId, $"k{i}", key, async own =>
            {
                started.TrySetResult();
                await release.Task.WaitAsync(own);
                return new PgBaselineProvider.CachedBaseline
                {
                    ComputedAt = key,
                    RealTime = DateTime.UtcNow,
                    Buckets = new Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>()
                };
            }, OwnLimit, CancellationToken.None));
            await started.Task;
        }

        Assert.Equal(KeptBaselineComputes.MaxInFlight, shared.KeptComputes.InFlight);

        /* A keyed set of the PostgreSQL-target provider: no slot is free, so its compute is the caller's. */
        using var deadline = new CancellationTokenSource();
        var pastStarted = Hook();
        var pastToken = CancellationToken.None;
        var past = pgTarget.GetOrComputeKeyedBaselinesAsync(ServerId, MetricNames.PgStatementShare, Candidates, Now, async (set, token) =>
        {
            pastToken = token;
            pastStarted.TrySetResult();
            await release.Task.WaitAsync(token);
            return Rows(set, key);
        }, keep: true, OwnLimit, deadline.Token);
        await pastStarted.Task;
        Assert.Equal(deadline.Token, pastToken);
        Assert.Equal(KeptBaselineComputes.MaxInFlight, shared.KeptComputes.InFlight);

        deadline.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => past);

        release.SetResult();
        await Task.WhenAll(kept);
        Assert.Equal(0, shared.KeptComputes.InFlight);
        Assert.Equal(KeptBaselineComputes.MaxInFlight, shared.Count);

        /* The set past the limit stored nothing, so it computes again. */
        var recomputed = 0;
        await pgTarget.GetOrComputeKeyedBaselinesAsync(ServerId, MetricNames.PgStatementShare, Candidates, Now, (set, _) =>
        {
            recomputed++;
            return Task.FromResult(Rows(set, key));
        }, keep: true, OwnLimit, CancellationToken.None);
        Assert.Equal(1, recomputed);
    }
}

/// <summary>
/// #9972 on a PostgreSQL target against a real store: an on-demand pass whose growth detector's baseline compute is held
/// past the detector deadline, a keyed set held past its caller's deadline, and the scheduled pass's cancel. A compute is
/// held by the provider's own hook and the test waits on the compute's task.
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetBaselineComputeKeptLiveTests
{
    private const int ServerId = -997204;
    private const string ServerName = "pg-target-baseline-kept-9972";
    private const long GiB = 1024L * 1024 * 1024;
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);
    private static readonly string[] Candidates = ["101", "202"];

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One collection of database sizes as the collector writes it when every database is sized: four databases,
    /// the instance total on each row.</summary>
    private static async Task PlantSizesAsync(NpgsqlConnection connection, DateTime at, long appdb, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        var total = appdb + 24 * GiB + 2 * GiB + GiB;
        foreach (var (name, size) in new (string, long)[] { ("appdb", appdb), ("reporting", 24 * GiB), ("scratch", 2 * GiB), ("vendor", GiB) })
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, size_bytes, total_bytes, is_template, allows_connections)
VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, TRUE)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(name);
            command.Parameters.AddWithValue(size);
            command.Parameters.AddWithValue(total);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// A migrated scratch store with one registered PostgreSQL target, a database stats row more than a day back and one
    /// a minute across the four-hour window. With <paramref name="withGrowth"/>, a month of hourly instance totals that
    /// never grow and then grow a gibibyte an hour across the window: one ANOMALY_PG_DATABASE_GROWTH fact. Returns the
    /// window's end.
    /// </summary>
    private static async Task<DateTime> SeedAsync(NpgsqlConnection connection, bool withGrowth, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);

        var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
        var windowStart = windowEnd.AddHours(-4);
        await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
        for (var minute = 0; minute <= 4 * 60 + 1; minute++)
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

        if (withGrowth)
        {
            for (var hoursBack = 30 * 24; hoursBack >= 0; hoursBack--)
                await PlantSizesAsync(connection, windowEnd.AddHours(-hoursBack), (100 + Math.Max(0, 4 - hoursBack)) * GiB, ct);
        }

        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        return windowEnd;
    }

    [Fact]
    public async Task ABaselineHeldPastTheDetectorDeadlineOnAPostgresTarget_IsStoredWhenItFinishes_AndTheSameCallAgainComputesNoneAndHasTheAnomaly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL-target kept-baseline test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var windowEnd = await SeedAsync(connection, withGrowth: true, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* An on-demand pass anchored on the window's end (an as_of call). */
        AnalysisContext Pass(CancellationToken deadline) => new()
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = windowEnd.AddHours(-4),
            TimeRangeEnd = windowEnd,
            AsOfUtc = windowEnd,
            ServerUtcOffset = TimeSpan.Zero,
            DetectorDeadline = deadline,
            CancellationToken = ct
        };

        /* Each call gets its own service and provider over the process's one shared tier, as the MCP host builds them. */
        var shared = new BaselineCache();
        var first = new DarlingAnalysisService(postgres, baselineCache: shared);

        /* The growth detector is the last of the fourteen. Its baseline compute is held, the thirteen before it finish. */
        var (started, release) = (Hook(), Hook());
        var firstGrowthComputes = 0;
        first.PgTargetBaselines.BeforeCompute = async (metric, token) =>
        {
            if (metric != MetricNames.PgDatabaseGrowthBytesPerDay)
                return;
            Interlocked.Increment(ref firstGrowthComputes);
            started.TrySetResult();
            await release.Task.WaitAsync(token);
        };

        using var deadline = new CancellationTokenSource();
        var cut = Pass(deadline.Token);
        var run = first.AnalyzeAsync(cut);
        await started.Task.WaitAsync(Limit, ct);
        deadline.Cancel();
        await run.WaitAsync(Limit, ct);

        Assert.Equal(new[] { "database_growth" }, cut.DetectorsLeftOutForTime);
        Assert.NotNull(cut.AnomalyReadBesideFamilies?.Run);
        Assert.DoesNotContain(await cut.AnomalyReadBesideFamilies!.Run!, anomaly => anomaly.Key == PgTargetFactKeys.AnomalyDatabaseGrowth);

        /* The pass has returned and the hold is released: the compute it left behind ends, and is waited for by its task. */
        var running = shared.KeptComputes.Running();
        release.SetResult();
        await Task.WhenAll(running).WaitAsync(Limit, ct);
        Assert.Equal(0, shared.KeptComputes.InFlight);

        /* The same call again, from a new service: no growth baseline is computed, no detector is left out, and the
           growth detector's anomaly is among the pass's. */
        var second = new DarlingAnalysisService(postgres, baselineCache: shared);
        var secondGrowthComputes = 0;
        second.PgTargetBaselines.BeforeCompute = (metric, _) =>
        {
            if (metric == MetricNames.PgDatabaseGrowthBytesPerDay)
                Interlocked.Increment(ref secondGrowthComputes);
            return Task.CompletedTask;
        };

        using var notFired = new CancellationTokenSource();
        var again = Pass(notFired.Token);
        await second.AnalyzeAsync(again).WaitAsync(Limit, ct);

        Assert.Equal(0, secondGrowthComputes);
        Assert.Equal(1, firstGrowthComputes);
        Assert.Empty(again.DetectorsLeftOutForTime);
        Assert.NotNull(again.AnomalyReadBesideFamilies?.Run);
        Assert.Contains(await again.AnomalyReadBesideFamilies!.Run!, anomaly => anomaly.Key == PgTargetFactKeys.AnomalyDatabaseGrowth);
    }

    [Fact]
    public async Task AKeyedSetHeldPastItsCallersDeadline_IsStoredForEveryKey_AndTheSameSetAgainFromTheNextProviderComputesNone()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live kept keyed-baseline test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var windowEnd = await SeedAsync(connection, withGrowth: false, ct);
        var windowStart = windowEnd.AddHours(-4);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var shared = new BaselineCache();
        var provider = new PgTargetBaselineProvider(postgres, null, shared);
        var (started, release) = (Hook(), Hook());
        var computes = 0;
        var computeToken = CancellationToken.None;
        provider.BeforeCompute = async (_, token) =>
        {
            Interlocked.Increment(ref computes);
            computeToken = token;
            started.TrySetResult();
            await release.Task.WaitAsync(token);
        };

        /* The flow of a pass with a detector deadline, as the detector sets it, and the statement detectors' keyed read. */
        provider.KeepComputesForThisFlow(true);
        using var deadline = new CancellationTokenSource();
        var read = provider.GetBaselinesAsync(ServerId, MetricNames.PgStatementShare, Candidates, windowStart, deadline.Token);
        await started.Task.WaitAsync(Limit, ct);
        deadline.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read.WaitAsync(Limit, ct));

        /* The compute runs on a token that is not the caller's, and goes on. */
        Assert.NotEqual(deadline.Token, computeToken);
        Assert.False(computeToken.IsCancellationRequested);
        var running = shared.KeptComputes.Running();
        Assert.Single(running);
        release.SetResult();
        await Task.WhenAll(running).WaitAsync(Limit, ct);
        Assert.Equal(0, shared.KeptComputes.InFlight);
        Assert.Equal(Candidates.Length, shared.Count);

        /* The same candidates again, from the next pass's provider: every key is a hit and nothing is computed. */
        var next = new PgTargetBaselineProvider(postgres, null, shared);
        var nextComputes = 0;
        next.BeforeCompute = (_, _) =>
        {
            Interlocked.Increment(ref nextComputes);
            return Task.CompletedTask;
        };
        next.KeepComputesForThisFlow(true);
        var buckets = await next.GetBaselinesAsync(ServerId, MetricNames.PgStatementShare, Candidates, windowStart, ct);
        Assert.Equal(0, nextComputes);
        Assert.Equal(1, computes);
        Assert.Equal(Candidates, buckets.Keys.Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task AScheduledPostgresTargetPassStoppedDuringABaselineCompute_CancelsTheCompute_AndStoresNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL-target scheduled-pass baseline test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var windowEnd = await SeedAsync(connection, withGrowth: false, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var shared = new BaselineCache();
        var provider = new PgTargetBaselineProvider(postgres, null, shared);
        var detector = new PgTargetAnomalyDetector(postgres, provider);

        /* The first baseline compute of the pass is held until its token is signalled. */
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

        /* A pass with no detector deadline, as the scheduled pass runs the detector. */
        using var pass = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var scheduled = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = windowEnd.AddHours(-4),
            TimeRangeEnd = windowEnd,
            ServerUtcOffset = TimeSpan.Zero,
            CancellationToken = pass.Token
        };
        var run = detector.DetectAnomaliesAsync(scheduled);
        await started.Task.WaitAsync(Limit, ct);
        pass.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Limit, ct));

        /* The compute ran on the pass's token, no compute outlives the pass, and nothing was stored. */
        Assert.Equal(pass.Token, computeToken);
        Assert.Equal(1, computes);
        Assert.Empty(shared.KeptComputes.Running());
        Assert.Equal(0, shared.KeptComputes.InFlight);
        Assert.Equal(0, shared.Count);

        /* So the next read computes it. */
        provider.BeforeCompute = (_, _) =>
        {
            Interlocked.Increment(ref computes);
            return Task.CompletedTask;
        };
        await provider.GetBucketMapAsync(ServerId, MetricNames.PgTps, windowEnd.AddHours(-4), windowEnd, ct);
        Assert.Equal(2, computes);
    }
}

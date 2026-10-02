/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Runner-level live pins for the per-database Query Store watermark cache (#4661), against a real store
/// (gated on DARLING_TEST_PG). Every read the cache answers is compared with the bounded store read it stands
/// in for, and statement counts come from <see cref="CommandCountingLoggerFactory"/>.
///
/// <para><b>#1776 own-store</b> - every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so none is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class DatabaseWatermarkCacheRunnerLiveTests
{
    private const int ServerId = -466100;
    private const string Db = "wmdb";
    private static readonly DateTime T0 = new(2026, 9, 28, 12, 0, 0, DateTimeKind.Utc);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static ServerRuntime MakeServer() => new()
    {
        Config = new MonitoredServer { Name = "wm-qs", Host = "wm-qs" },
        ConnectionString = "Server=wm-qs",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "wm-qs",
        ServerId = ServerId,
        EngineEdition = 3,
    };

    private static CollectorContext MakeContext(ServerRuntime server, DateTime collectionTime) => new()
    {
        ServerId = server.ServerId,
        ServerName = server.StorageName,
        CollectionTime = collectionTime,
        Deltas = new CollectorDeltaCalculator(),
        Target = server.Target,
    };

    private static QueryStoreCollector.Row Row(long id, DateTime? last, string db = Db) => new()
    {
        DatabaseName = db,
        QueryId = id,
        PlanId = id,
        ExecutionTypeDesc = "Regular",
        FirstExecutionTime = last,
        LastExecutionTime = last,
        QueryHash = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
        QueryPlanHash = "0x" + id.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
        ExecutionCount = 1,
        AvgCpuTimeUs = 10,
        AvgDurationUs = 20,
        RuntimeStatsIntervalId = id,
    };

    private static async Task WriteAsync(
        DarlingCollectorRunner runner, NpgsqlConnection connection, ServerRuntime server,
        List<QueryStoreCollector.Row> rows, DateTime ct, CancellationToken token)
    {
        var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(QueryStoreCollector.Row)).Invoke(
            runner, new object?[] { connection, QueryStoreCollector.Instance, rows, server, ct, MakeContext(server, ct), token })!;
        await task;
    }

    private static Task<DateTime?> ResolveAsync(DarlingCollectorRunner runner, ServerRuntime server, DateTime ct, CancellationToken token) =>
        runner.ResolveQueryStoreDatabaseWatermarkAsync(
            server, "query_store_stats", "last_execution_time", "database_name", Db, WatermarkPolicy.ReadFloor(ct)!.Value, ct, token);

    private static Task<DateTime?> StoreAsync(DarlingCollectorRunner runner, DateTime ct, CancellationToken token) =>
        runner.GetLastCollectedTimeForDatabaseAsync(
            ServerId, "query_store_stats", "last_execution_time", "database_name", Db, token, WatermarkPolicy.ReadFloor(ct));

    private static void Commit(DarlingCollectorRunner runner, ServerRuntime server, List<QueryStoreCollector.Row> rows, DateTime ct) =>
        runner.CommitQueryStoreDatabaseWatermark(
            server, Db, DarlingCollectorRunner.StageQueryStoreDatabaseWatermark(rows, Db, ct));

    private static long Reads(CommandCountingLoggerFactory f) => f.Provider.CountContaining("MAX(last_execution_time)");

    private sealed class Rig : IAsyncDisposable
    {
        public ScratchPostgres Scratch = null!;
        public NpgsqlConnection Connection = null!;
        public NpgsqlDataSource Source = null!;
        public CommandCountingLoggerFactory Logger = new();
        public DarlingCollectorRunner Runner = null!;

        public static async Task<Rig> OpenAsync(CancellationToken ct)
        {
            var rig = new Rig();
            rig.Scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            rig.Connection = new NpgsqlConnection(rig.Scratch.ConnectionString);
            await rig.Connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(rig.Connection, ct);
            rig.Source = new NpgsqlDataSourceBuilder(rig.Scratch.ConnectionString).UseLoggerFactory(rig.Logger).Build();
            rig.Runner = new DarlingCollectorRunner(rig.Source, new CollectorDeltaCalculator());
            return rig;
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_Equivalence_CacheMatchesTheBoundedStoreRead_AfterEveryBatchShape()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();
        var subMicro = new DateTime(2026, 9, 28, 11, 55, 0, DateTimeKind.Utc).AddTicks(1234567);

        var batches = new List<List<QueryStoreCollector.Row>>
        {
            new() { Row(1, subMicro) },
            new() { Row(2, subMicro.AddMinutes(9)), Row(3, subMicro.AddMinutes(4)) },
            new() { Row(4, subMicro.AddMinutes(1)) },
            new(),
            new() { Row(5, null) },
            new() { Row(6, DateTime.SpecifyKind(subMicro.AddMinutes(20), DateTimeKind.Unspecified)) },
        };

        var ct = T0;
        var sawHit = false;
        for (var i = 0; i < batches.Count; i++)
        {
            ct = ct.AddMinutes(1);
            await ResolveAsync(rig.Runner, server, ct, token);
            if (batches[i].Count > 0)
            {
                await WriteAsync(rig.Runner, rig.Connection, server, batches[i], ct, token);
            }

            Commit(rig.Runner, server, batches[i], ct);

            var next = ct.AddMinutes(1);
            rig.Logger.Provider.Reset();
            var cached = await ResolveAsync(rig.Runner, server, next, token);
            var reads = Reads(rig.Logger);
            sawHit |= reads == 0;
            var store = await StoreAsync(rig.Runner, next, token);
            Assert.Equal(store, cached);
            if (batches[i].Count > 0 && batches[i].Exists(r => r.LastExecutionTime is not null))
            {
                Assert.Equal(0, reads);
            }
        }

        Assert.True(sawHit, "no resolve was ever answered from the cache");
    }

    [Fact]
    public async Task B_Floor_HitsInsideTheReseedInterval_AndReadsTheStoreAtTheFloorBoundary()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();
        var ct0 = T0;
        var rows = new List<QueryStoreCollector.Row> { Row(1, ct0.AddMinutes(-5)) };

        await ResolveAsync(rig.Runner, server, ct0, token);
        await WriteAsync(rig.Runner, rig.Connection, server, rows, ct0, token);
        Commit(rig.Runner, server, rows, ct0);

        var inside = ct0 + TimeSpan.FromMinutes(59);
        rig.Logger.Provider.Reset();
        var cached = await ResolveAsync(rig.Runner, server, inside, token);
        Assert.Equal(0, Reads(rig.Logger));
        Assert.NotNull(cached);
        Assert.Equal(await StoreAsync(rig.Runner, inside, token), cached);

        var boundary = ct0 + TimeSpan.FromHours(3);
        rig.Logger.Provider.Reset();
        var atBoundary = await ResolveAsync(rig.Runner, server, boundary, token);
        Assert.True(Reads(rig.Logger) >= 1);
        Assert.Null(atBoundary);
        Assert.Equal(await StoreAsync(rig.Runner, boundary, token), atBoundary);

        var later = boundary.AddSeconds(1);
        Assert.Null(await ResolveAsync(rig.Runner, server, later, token));
        Assert.Equal(await StoreAsync(rig.Runner, later, token), await ResolveAsync(rig.Runner, server, later, token));
    }

    [Fact]
    public async Task C_NullSeed_IsCached_OnAnEmptyStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        rig.Logger.Provider.Reset();
        Assert.Null(await ResolveAsync(rig.Runner, server, T0, token));
        Assert.Equal(1, Reads(rig.Logger));

        rig.Logger.Provider.Reset();
        Assert.Null(await ResolveAsync(rig.Runner, server, T0.AddMinutes(1), token));
        Assert.Equal(0, Reads(rig.Logger));
    }

    [Fact]
    public async Task D_AFailedRead_IsNotCachedAsNoRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        await using (var rename = new NpgsqlCommand("ALTER TABLE collect.query_store_stats RENAME TO qss_x", rig.Connection))
        {
            await rename.ExecuteNonQueryAsync(token);
        }

        Assert.Null(await ResolveAsync(rig.Runner, server, T0, token));

        await using (var back = new NpgsqlCommand("ALTER TABLE collect.qss_x RENAME TO query_store_stats", rig.Connection))
        {
            await back.ExecuteNonQueryAsync(token);
        }

        var rows = new List<QueryStoreCollector.Row> { Row(1, T0.AddMinutes(-2)) };
        await WriteAsync(rig.Runner, rig.Connection, server, rows, T0, token);

        rig.Logger.Provider.Reset();
        var result = await ResolveAsync(rig.Runner, server, T0.AddMinutes(1), token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.NotNull(result);
        Assert.Equal(await StoreAsync(rig.Runner, T0.AddMinutes(1), token), result);
    }

    [Fact]
    public async Task E_Invalidation_Reconnect_AndBackfillWrite_ForceTheNextResolveToReadTheStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        async Task SeedNullAsync()
        {
            await ResolveAsync(rig.Runner, server, T0, token);
            rig.Logger.Provider.Reset();
            await ResolveAsync(rig.Runner, server, T0.AddMinutes(1), token);
            Assert.Equal(0, Reads(rig.Logger));
        }

        async Task AssertNextReadsStoreAsync()
        {
            rig.Logger.Provider.Reset();
            var result = await ResolveAsync(rig.Runner, server, T0.AddMinutes(2), token);
            Assert.Equal(1, Reads(rig.Logger));
            Assert.Equal(await StoreAsync(rig.Runner, T0.AddMinutes(2), token), result);
        }

        await SeedNullAsync();
        rig.Runner.OnServerReconnected(ServerId);
        await AssertNextReadsStoreAsync();

        var backfillRows = new List<QueryStoreCollector.Row> { Row(9, T0.AddMinutes(-30)) };
        await rig.Runner.WriteBackfillBatchAsync(
            QueryStoreCollector.Instance, backfillRows, server, T0.AddMinutes(-20), MakeContext(server, T0), token);
        await AssertNextReadsStoreAsync();
    }

    [Fact]
    public async Task Commit_FollowsTheFlush_ADriverRunWhoseWriteThrows_LeavesTheCacheEqualToTheStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        var staged = new Dictionary<string, StagedDatabaseWatermark>();
        await Assert.ThrowsAsync<InvalidOperationException>(() => EnumeratedCollectorDriver.RunAsync<QueryStoreCollector.Row>(
            new[] { Db },
            perItemWatermark: async (item, ct) => { await ResolveAsync(rig.Runner, server, T0, ct); },
            readItem: (item, ct) =>
            {
                var rows = new List<QueryStoreCollector.Row> { Row(1, T0.AddMinutes(-1)) };
                staged[item] = DarlingCollectorRunner.StageQueryStoreDatabaseWatermark(rows, item, T0);
                return Task.FromResult(rows);
            },
            writeBatch: (batch, ct) => throw new InvalidOperationException("flush failed"),
            onItemComplete: (item, rows, sqlMs, storageMs) =>
            {
                rig.Runner.CommitQueryStoreDatabaseWatermark(server, item, staged[item]);
            },
            onItemError: (item, ex) => { },
            token));

        var later = T0.AddMinutes(1);
        var cached = await ResolveAsync(rig.Runner, server, later, token);
        Assert.Null(cached);
        Assert.Equal(await StoreAsync(rig.Runner, later, token), cached);
    }

    [Fact]
    public async Task F_AfterTheReseedInterval_TheNextResolveReadsTheStoreOnce_AndEqualsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        await ResolveAsync(rig.Runner, server, T0, token);
        var rows = new List<QueryStoreCollector.Row> { Row(1, T0.AddMinutes(-1)) };
        await WriteAsync(rig.Runner, rig.Connection, server, rows, T0, token);
        Commit(rig.Runner, server, rows, T0);

        rig.Logger.Provider.Reset();
        var inside = T0.AddMinutes(59);
        var insideResult = await ResolveAsync(rig.Runner, server, inside, token);
        var insideReads = Reads(rig.Logger);
        Assert.Equal(await StoreAsync(rig.Runner, inside, token), insideResult);

        rig.Logger.Provider.Reset();
        var after = T0.AddMinutes(60);
        var result = await ResolveAsync(rig.Runner, server, after, token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.Equal(await StoreAsync(rig.Runner, after, token), result);
        Assert.Equal(0, insideReads);
    }

    private async Task SeedNullAsync(Rig rig, ServerRuntime server, CancellationToken token)
    {
        await ResolveAsync(rig.Runner, server, T0, token);
        rig.Logger.Provider.Reset();
        await ResolveAsync(rig.Runner, server, T0.AddMinutes(1), token);
        Assert.Equal(0, Reads(rig.Logger));
    }

    private async Task AssertNextReadsStoreOnceAsync(Rig rig, ServerRuntime server, CancellationToken token)
    {
        rig.Logger.Provider.Reset();
        var result = await ResolveAsync(rig.Runner, server, T0.AddMinutes(2), token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.Equal(await StoreAsync(rig.Runner, T0.AddMinutes(2), token), result);
    }

    [Fact]
    public async Task G_ARunAsyncFault_DropsTheServersEntries_SoTheNextResolveReadsTheStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();
        await SeedNullAsync(rig, server, token);

        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(token);
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<Exception>(
            () => rig.Runner.RunAsync(QueryStoreCollector.Instance, server, cancelled.Token));

        await AssertNextReadsStoreOnceAsync(rig, server, token);
    }

    [Fact]
    public async Task H_AnItemError_DropsThatDatabasesEntry_AndItsStagedContribution()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();
        await SeedNullAsync(rig, server, token);

        var staged = new Dictionary<string, StagedDatabaseWatermark>
        {
            [Db] = DarlingCollectorRunner.StageQueryStoreDatabaseWatermark(new List<QueryStoreCollector.Row>(), Db, T0),
        };
        rig.Runner.DiscardQueryStoreDatabaseWatermark(server, Db, staged);

        Assert.Empty(staged);
        await AssertNextReadsStoreOnceAsync(rig, server, token);
    }

    [Fact]
    public async Task I_AForeignRowBatch_InvalidatesInsteadOfAdvancing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();
        await SeedNullAsync(rig, server, token);

        var rows = new List<QueryStoreCollector.Row> { Row(1, T0.AddMinutes(-1), "otherdb") };
        var staged = DarlingCollectorRunner.StageQueryStoreDatabaseWatermark(rows, Db, T0);
        Assert.True(staged.Foreign);
        rig.Runner.CommitQueryStoreDatabaseWatermark(server, Db, staged);

        await AssertNextReadsStoreOnceAsync(rig, server, token);
    }

    [Fact]
    public async Task J_ABackfillThatCommitsBetweenTheReadAndTheSeed_RejectsTheSeed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4661 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();

        var cache = (DatabaseWatermarkCache)typeof(DarlingCollectorRunner)
            .GetField("_databaseWatermarkCache", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(rig.Runner)!;

        var captured = cache.TokenFor(ServerId, Db);
        var readBeforeBackfill = await StoreAsync(rig.Runner, T0, token);
        Assert.Null(readBeforeBackfill);

        var backfillRows = new List<QueryStoreCollector.Row> { Row(9, T0.AddMinutes(-30)) };
        await rig.Runner.WriteBackfillBatchAsync(
            QueryStoreCollector.Instance, backfillRows, server, T0.AddMinutes(-20), MakeContext(server, T0), token);

        cache.Seed(ServerId, Db, readBeforeBackfill, WatermarkPolicy.ReadFloor(T0)!.Value, T0, captured);
        await AssertNextReadsStoreOnceAsync(rig, server, token);
    }

    /// <summary>
    /// #4749: a value read from the store keeps its witness, so a database whose newest row is recent but has
    /// had no batch since the seed hits the cache until that row leaves the floor. The seed is taken 2h30m after
    /// the batch: the row leaves the 3-hour floor at ct0 + 3h, 30 minutes into the 1-hour reseed interval, so
    /// only the witness decides that miss. A seed taken at the batch itself would let the reseed interval end
    /// before the floor could.
    /// </summary>
    [Fact]
    public async Task K_ASeedReadFromTheStore_KeepsItsWitness_SoAQuietDatabaseHitsUntilItsRowLeavesTheFloor()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4749 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();
        var ct0 = T0;
        var rows = new List<QueryStoreCollector.Row> { Row(1, ct0.AddMinutes(-5)) };

        /* No cache entry exists yet, so no batch advances one: the seed read below is the cache's only source. */
        await WriteAsync(rig.Runner, rig.Connection, server, rows, ct0, token);

        var seededAt = ct0 + TimeSpan.FromMinutes(150);
        rig.Logger.Provider.Reset();
        var seeded = await ResolveAsync(rig.Runner, server, seededAt, token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.NotNull(seeded);
        Assert.Equal(await StoreAsync(rig.Runner, seededAt, token), seeded);

        var quiet = seededAt + TimeSpan.FromMinutes(5);
        rig.Logger.Provider.Reset();
        var cached = await ResolveAsync(rig.Runner, server, quiet, token);
        Assert.Equal(0, Reads(rig.Logger));
        Assert.Equal(seeded, cached);
        Assert.Equal(await StoreAsync(rig.Runner, quiet, token), cached);

        /* At ct0 + 3h the row's collection_time equals the floor, which the store's collection_time > floor leaves out. */
        var boundary = ct0 + TimeSpan.FromHours(3);
        rig.Logger.Provider.Reset();
        var atBoundary = await ResolveAsync(rig.Runner, server, boundary, token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.Null(atBoundary);
        Assert.Equal(await StoreAsync(rig.Runner, boundary, token), atBoundary);
    }

    /// <summary>
    /// #4749: the witness is the newest collection_time among the rows AT the maximum value. Batches A and B
    /// hold the same value and batch C, the newest, holds a lower one. At ct0 + 3h batch A is at the floor but
    /// batch B still holds the value, so a witness taken from the oldest such row would miss. At ct0 + 3h20m
    /// batch B is at the floor and the store's read sees only C's lower value, so a witness taken from the
    /// newest batch of any value would still hit and serve the stale one.
    /// </summary>
    [Fact]
    public async Task L_TheSeededWitness_IsTheNewestBatchAtTheMaximumValue_NotTheNewestBatchOrTheOldest()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the #4749 pins.");
        var token = TestContext.Current.CancellationToken;
        await using var rig = await Rig.OpenAsync(token);
        var server = MakeServer();
        var ct0 = T0;
        var value = ct0.AddMinutes(-5);

        await WriteAsync(rig.Runner, rig.Connection, server, new List<QueryStoreCollector.Row> { Row(1, value) }, ct0, token);
        await WriteAsync(rig.Runner, rig.Connection, server, new List<QueryStoreCollector.Row> { Row(2, value) }, ct0.AddMinutes(20), token);
        await WriteAsync(rig.Runner, rig.Connection, server, new List<QueryStoreCollector.Row> { Row(3, value.AddHours(-1)) }, ct0.AddMinutes(30), token);

        var seededAt = ct0 + TimeSpan.FromMinutes(150);
        rig.Logger.Provider.Reset();
        var seeded = await ResolveAsync(rig.Runner, server, seededAt, token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.NotNull(seeded);
        Assert.Equal(await StoreAsync(rig.Runner, seededAt, token), seeded);

        var oldestGone = ct0 + TimeSpan.FromHours(3);
        rig.Logger.Provider.Reset();
        var stillHeld = await ResolveAsync(rig.Runner, server, oldestGone, token);
        Assert.Equal(0, Reads(rig.Logger));
        Assert.Equal(seeded, stillHeld);
        Assert.Equal(await StoreAsync(rig.Runner, oldestGone, token), stillHeld);

        var newestGone = ct0 + TimeSpan.FromMinutes(200);
        rig.Logger.Provider.Reset();
        var lower = await ResolveAsync(rig.Runner, server, newestGone, token);
        Assert.Equal(1, Reads(rig.Logger));
        Assert.NotNull(lower);
        Assert.True(lower < seeded);
        Assert.Equal(await StoreAsync(rig.Runner, newestGone, token), lower);
    }
}

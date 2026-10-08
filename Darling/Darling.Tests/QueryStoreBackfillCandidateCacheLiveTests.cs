/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5518: the Query Store backfill's per-tick database list is read from the store (a skip scan per chunk of the
/// window, already the cheapest exact read: one index search per chunk per database) only when it could have
/// changed, instead of on every tick. The cache is exact under the store-write fence: every Query Store write names
/// its databases there before it opens its transaction, so a name outside the cached list invalidates it; a new cut
/// chunk changes the statement text; and an age bound covers a writer outside the fence.
///
/// <para>The answer is pinned against a FRESH read (a backfill with no fence, which reads every call), including a
/// database seen only once in the window.</para>
///
/// <para><b>#1776 own-store</b> - every test mints its own scratch database.</para>
/// </summary>
public sealed class QueryStoreBackfillCandidateCacheLiveTests
{
    private const int Server = -551801;
    private const int OtherServer = -551802;
    private static long s_id = 5518000;

    private static DateTime At(int day, int hour, int minute = 0)
        => new(2026, 6, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static readonly IReadOnlyDictionary<string, string> NoState = new Dictionary<string, string>(StringComparer.Ordinal);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5518 candidate-cache tests (each mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        return (scratch, connection);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, int serverId, string database, DateTime collectionTime, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us)
VALUES
    ($1, $2, $3, 'SQL01', $4, 'dbo.GetOrders', '0xABCD', 91, 111, 'Regular', 'Primary',
     1, $2, $2, $2, 1, 100, 100, 100, 100)";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(Interlocked.Increment(ref s_id));
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static QueryStoreBackfill Backfill(NpgsqlDataSource postgres, QueryStoreWriteFence? fence)
        => new(postgres, new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), new CollectorDeltaCalculator(), logger: null,
               hasContinuousAggregates: () => true, writeFence: fence);

    /// <summary>One completed fenced write that names <paramref name="databases"/>, as the runner's COPY chokepoint
    /// makes it.</summary>
    private static void FencedWrite(QueryStoreWriteFence fence, int serverId, params string[] databases)
    {
        fence.BeginWrite(serverId, databases);
        fence.EndWrite(serverId, succeeded: true);
    }

    /// <summary>Pin 1. The second tick is served from the cache (one store read for two calls) and both lists equal a
    /// fresh read's, so the cache changes the cost, not the answer. A write to a database the list already holds
    /// does not make the next tick read.</summary>
    [Fact]
    public async Task SecondTick_IsServedFromTheCache_AndEqualsAFreshRead()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(15, 9);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "busy", floor.AddHours(1), ct);
        await SeedAsync(connection, Server, "seen_once", At(15, 3), ct);
        await SeedAsync(connection, OtherServer, "other_server", floor.AddHours(1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var fence = new QueryStoreWriteFence();
        var cached = Backfill(postgres, fence);
        var fresh = await Backfill(postgres, fence: null).GetCandidateDatabasesAsync(Server, floor, NoState, ct);

        var first = await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);
        FencedWrite(fence, Server, "busy");
        var second = await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);

        Assert.Equal(new[] { "busy", "seen_once" }, fresh);
        Assert.Equal(fresh, first);
        Assert.Equal(fresh, second);
        Assert.Equal(1, cached.CandidateStoreReadsForTests);
    }

    /// <summary>Pin 2. A write naming a database the list does not hold invalidates it: the next tick reads, and the
    /// database - seen in one row in the window - is in the list, exactly as a fresh read has it. Another server's
    /// new name does not.</summary>
    [Fact]
    public async Task AWriteNamingAnUnknownDatabase_MakesTheNextTickRead_AndTheSingleRowDatabaseIsListed()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(15, 9);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "busy", floor.AddHours(1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var fence = new QueryStoreWriteFence();
        var cached = Backfill(postgres, fence);

        Assert.Equal(new[] { "busy" }, await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct));

        FencedWrite(fence, OtherServer, "somebody_elses");
        Assert.Equal(new[] { "busy" }, await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct));
        Assert.Equal(1, cached.CandidateStoreReadsForTests);

        await SeedAsync(connection, Server, "arrives_once", floor.AddHours(2), ct);
        FencedWrite(fence, Server, "arrives_once");
        var list = await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);

        Assert.Equal(new[] { "arrives_once", "busy" }, list);
        Assert.Equal(await Backfill(postgres, fence: null).GetCandidateDatabasesAsync(Server, floor, NoState, ct), list);
        Assert.Equal(2, cached.CandidateStoreReadsForTests);
    }

    /// <summary>Pin 3. A list read beside a write in flight is returned but not kept (the write may commit after the
    /// read), and neither is one read while the server is poisoned by an ambiguous write; the tick after a clean
    /// quiet read keeps it.</summary>
    [Fact]
    public async Task AListReadBesideAWriteInFlight_IsNotKept()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(15, 9);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "busy", floor.AddHours(1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var fence = new QueryStoreWriteFence();
        var cached = Backfill(postgres, fence);

        fence.BeginWrite(Server, new[] { "busy" });
        await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);
        await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);
        Assert.Equal(2, cached.CandidateStoreReadsForTests);

        fence.EndWrite(Server, succeeded: true);
        await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);
        await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);
        Assert.Equal(3, cached.CandidateStoreReadsForTests);
    }

    /// <summary>Pin 4. The age bound: past <see cref="QueryStoreBackfill.CandidateCacheMaxAge"/> the next tick reads
    /// and sees a row some writer outside the fence added.</summary>
    [Fact]
    public async Task PastTheAgeBound_TheNextTickReads_AndSeesAWriteFromOutsideTheFence()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(15, 9);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "busy", floor.AddHours(1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var now = new DateTime(2026, 6, 15, 12, 0, 0, DateTimeKind.Utc);
        var cached = Backfill(postgres, new QueryStoreWriteFence());
        cached.UtcNowForCandidateCache = () => now;

        await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct);
        await SeedAsync(connection, Server, "restored", floor.AddHours(2), ct);

        now += QueryStoreBackfill.CandidateCacheMaxAge - TimeSpan.FromSeconds(1);
        Assert.Equal(new[] { "busy" }, await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct));

        now += TimeSpan.FromSeconds(1);
        Assert.Equal(new[] { "busy", "restored" }, await cached.GetCandidateDatabasesAsync(Server, floor, NoState, ct));
        Assert.Equal(2, cached.CandidateStoreReadsForTests);
    }

    /// <summary>Pin 5. The cut moving to another chunk changes the statement, so the cached list is not served: the
    /// floor in the next day's chunk drops a name that only the previous chunk held.</summary>
    [Fact]
    public async Task AFloorInAnotherChunk_IsNotServedTheCachedList()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "day15_only", At(15, 5), ct);
        await SeedAsync(connection, Server, "busy", At(15, 20), ct);
        await SeedAsync(connection, Server, "busy", At(16, 10), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var cached = Backfill(postgres, new QueryStoreWriteFence());

        Assert.Equal(new[] { "busy", "day15_only" }, await cached.GetCandidateDatabasesAsync(Server, At(15, 9), NoState, ct));
        Assert.Equal(new[] { "busy" }, await cached.GetCandidateDatabasesAsync(Server, At(16, 9), NoState, ct));
        Assert.Equal(2, cached.CandidateStoreReadsForTests);
    }

    /// <summary>Pin 6. A host with no fence never caches: every call reads, as before the cache.</summary>
    [Fact]
    public async Task WithoutAFence_EveryTickReads()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(15, 9);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "busy", floor.AddHours(1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var unfenced = Backfill(postgres, fence: null);
        await unfenced.GetCandidateDatabasesAsync(Server, floor, NoState, ct);
        await unfenced.GetCandidateDatabasesAsync(Server, floor, NoState, ct);

        Assert.Equal(2, unfenced.CandidateStoreReadsForTests);
    }
}

/// <summary>#5518: the fence's name ledger, and the two places that feed and read it.</summary>
public sealed class QueryStoreWriteFenceNameTests
{
    [Fact]
    public void UnknownNameSince_SeesOnlyNamesOutsideTheListWrittenAfterTheSnapshot()
    {
        var fence = new QueryStoreWriteFence();
        var known = new HashSet<string>(new[] { "a", "b" }, StringComparer.Ordinal);

        fence.BeginWrite(1, new[] { "zz_before_snapshot" });
        fence.EndWrite(1, succeeded: true);
        var snapshot = fence.NameSnapshot(1);
        Assert.True(snapshot.Quiet);
        Assert.False(fence.WroteUnknownNameSince(1, snapshot.Sequence, known));

        fence.BeginWrite(1, new[] { "a", "b" });
        fence.EndWrite(1, succeeded: true);
        fence.BeginWrite(2, new[] { "other_server_name" });
        fence.EndWrite(2, succeeded: true);
        fence.BeginWrite(1);
        fence.EndWrite(1, succeeded: true);
        Assert.False(fence.WroteUnknownNameSince(1, snapshot.Sequence, known));

        fence.BeginWrite(1, new[] { "a", "c" });
        Assert.True(fence.WroteUnknownNameSince(1, snapshot.Sequence, known));
        Assert.False(fence.NameSnapshot(1).Quiet);
    }

    [Fact]
    public void AFailedWrite_StillRecordsItsNames_AndPoisonsTheSnapshot()
    {
        var fence = new QueryStoreWriteFence();
        var snapshot = fence.NameSnapshot(1);
        fence.BeginWrite(1, new[] { "new_db" });
        fence.EndWrite(1, succeeded: false);

        Assert.True(fence.WroteUnknownNameSince(1, snapshot.Sequence, new HashSet<string>(StringComparer.Ordinal)));
        Assert.False(fence.NameSnapshot(1).Quiet);
    }

    [Fact]
    public void TheRunnerNamesItsDatabases_AndTheWorkerHandsTheBackfillTheFence()
    {
        var runner = File.ReadAllText(Path.Combine(ServiceDirectory(), "DarlingCollectorRunner.cs"));
        var worker = File.ReadAllText(Path.Combine(ServiceDirectory(), "DarlingWorker.cs"));

        Assert.Contains("_queryStoreWriteFence?.BeginWrite(beginServerId, queryStoreDatabases);", runner, StringComparison.Ordinal);
        var backfill = worker.IndexOf("new QueryStoreBackfill(", StringComparison.Ordinal);
        Assert.True(backfill > 0);
        Assert.Contains("_queryStoreWriteFence);", worker.Substring(backfill, 1800), StringComparison.Ordinal);
    }

    private static string ServiceDirectory([System.Runtime.CompilerServices.CallerFilePath] string here = "")
        => Path.Combine(Path.GetDirectoryName(here)!, "..", "PerformanceMonitor.Darling.Service");
}

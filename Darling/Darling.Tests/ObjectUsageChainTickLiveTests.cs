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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9349 part C2: the hourly tick that runs the chain builder, and the runs purge. Two servers hold daily snapshots of several databases. One tick
/// backfills every pair, a second tick with no new raw rows calls the builder for nothing, and a new raw snapshot on one pair builds only that
/// pair. A step cap smaller than the backlog finishes the backfill in two ticks with the rows an uncapped build stores. A builder that throws for
/// one pair fails that pair only. The purge deletes closed runs past the horizon and keeps open ones.
///
/// <para>#9350 review round 1, part T: the tick's listings read the newest two days of raw rows, so the fixture's snapshots are hours apart
/// ending a few hours ago (<see cref="Day"/>), and a pair whose rows are all older than the window is planted with <see cref="OldDay"/>. A
/// dead pair is never handed to the builder, a never-built server with old rows is still backfilled, the builder stops at its probe when
/// the pair has no new raw rows, a capped tick resumes after the pair it stopped at, and two ticks on one store build each pair once.</para>
/// </summary>
public sealed class ObjectUsageChainTickLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9349 chain tick tests.";
    private const int ServerOne = -935001;
    private const int ServerTwo = -935002;
    private const int Capped = -935003;
    private const int Uncapped = -935004;

    private static readonly DateTime Start = new(2026, 9, 1, 8, 0, 0);

    /* Through LiveClock (#5608): a fixed point inside one chunk, twelve hours before the anchored end, so the hourly seeds stay inside the
       tick's two-day listing window (which the product reads from its own clock) at any hour. */
    private static readonly DateTime Anchor = LiveClock.AnchoredEndNotAfterNow().AddHours(-12);

    /// <summary>The fixture's snapshots are an hour apart and inside the tick's two-day listing window.</summary>
    private static DateTime Day(int day) => Anchor.AddHours(day);

    /// <summary>The same spacing, ten days back: a pair whose raw rows are all this old is outside the window.</summary>
    private static DateTime OldDay(int day) => Anchor.AddDays(-10).AddHours(day);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task<ObjectUsageChainTick.TickResult> TickAsync(
        NpgsqlDataSource dataSource, int maxSteps, ObjectUsageChainTick.BuildPair build, CancellationToken ct) =>
        ObjectUsageChainTick.RunTickAsync(dataSource, maxSteps, TimeSpan.FromMinutes(5), build, NullLogger.Instance, ct);

    /// <summary>The snapshot of one day for one database: two indexes whose counters grow by day.</summary>
    private static async Task PlantAsync(NpgsqlConnection connection, int serverId, string database, int day, CancellationToken ct, Func<int, DateTime>? time = null)
    {
        time ??= Day;
        foreach (var (indexId, factor) in new[] { (1, 10L), (2, 3L) })
        {
            await using var command = new NpgsqlCommand(
                @"INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_type_desc, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_update, is_indexed_view, answering_server_name, is_memory_optimized)
VALUES ($1,$2,$3,'tick-sql1',$4,$5,7,'dbo',100,'Orders',$6,'CLUSTERED',$7,0,0,$8,$9,$9,false,'n1',false)", connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(time(day));
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(Start);
            command.Parameters.AddWithValue(database);
            command.Parameters.AddWithValue(indexId);
            command.Parameters.AddWithValue(factor * day);
            command.Parameters.AddWithValue(factor * day);
            command.Parameters.AddWithValue(time(day).AddHours(-1));
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task PlantServerAsync(
        NpgsqlConnection connection, int serverId, string[] databases, int days, CancellationToken ct, Func<int, DateTime>? time = null)
    {
        /* The server's registry row, which the backfill reads to find a server whose raw rows are all older than the listing window. */
        await using (var register = new NpgsqlCommand(
            "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
            + "VALUES ($1, 'tick-' || $1::text, 'tick', TRUE, 15, now(), now()) ON CONFLICT DO NOTHING", connection))
        {
            register.Parameters.AddWithValue(serverId);
            await register.ExecuteNonQueryAsync(ct);
        }

        foreach (var database in databases)
        {
            for (var day = 1; day <= days; day++)
            {
                await PlantAsync(connection, serverId, database, day, ct, time);
            }
        }
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static Task<long> SnapshotsAsync(NpgsqlConnection connection, int serverId, string database, CancellationToken ct) =>
        CountAsync(connection, $"SELECT count(*) FROM collect.object_usage_snapshots WHERE server_id = {serverId} AND database_name = '{database}'", ct);

    /// <summary>The pair's three tables as text, without server_id and built_at.</summary>
    private static async Task<string> FingerprintAsync(NpgsqlConnection connection, int serverId, string database, CancellationToken ct)
    {
        var parts = new List<string>();
        foreach (var (table, order) in new[]
        {
            (ObjectUsageChain.SnapshotsTable, "collection_time"),
            (ObjectUsageChain.DeltasTable, "collection_time, object_id, index_id"),
            (ObjectUsageChain.RunsTable, "object_id, index_id, first_time"),
        })
        {
            await using var command = new NpgsqlCommand(
                $"SELECT coalesce(string_agg((to_jsonb(t) - 'server_id' - 'built_at')::text, E'\\n' ORDER BY {order}), '') FROM {table} AS t WHERE server_id = $1 AND database_name = $2",
                connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(database);
            parts.Add((string)(await command.ExecuteScalarAsync(ct))!);
        }

        return string.Join("\n--\n", parts);
    }

    [Fact]
    public async Task OneTickBackfillsEveryPair_ASecondTickBuildsNothing_AndANewSnapshotBuildsOnlyThatPair()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantServerAsync(connection, ServerOne, ["Alpha", "Beta", "Gamma"], 5, ct);
        await PlantServerAsync(connection, ServerTwo, ["Alpha", "Delta"], 5, ct);

        var calls = new List<(int Server, string Database, int Built)>();
        async Task<int> Recording(NpgsqlDataSource ds, int server, string database, int maxSteps, CancellationToken token)
        {
            var built = await ObjectUsageChainBuilder.BuildPairAsync(ds, server, database, maxSteps, token);
            calls.Add((server, database, built));
            return built;
        }

        var first = await TickAsync(dataSource, int.MaxValue, Recording, ct);
        Assert.Equal(new ObjectUsageChainTick.TickResult(5, 25, 0, false), first);
        foreach (var (server, database) in new[] { (ServerOne, "Alpha"), (ServerOne, "Beta"), (ServerOne, "Gamma"), (ServerTwo, "Alpha"), (ServerTwo, "Delta") })
        {
            Assert.Equal(5L, await SnapshotsAsync(connection, server, database, ct));
        }

        calls.Clear();
        var second = await TickAsync(dataSource, int.MaxValue, Recording, ct);
        Assert.Equal(new ObjectUsageChainTick.TickResult(0, 0, 0, false), second);
        Assert.Empty(calls);

        await PlantAsync(connection, ServerOne, "Beta", 6, ct);
        var third = await TickAsync(dataSource, int.MaxValue, Recording, ct);
        Assert.Equal(new ObjectUsageChainTick.TickResult(1, 1, 0, false), third);
        Assert.Equal(new[] { (ServerOne, "Beta") }, calls.Where(c => c.Built > 0).Select(c => (c.Server, c.Database)).ToArray());
        Assert.DoesNotContain(calls, c => c.Server == ServerTwo);
        Assert.Equal(6L, await SnapshotsAsync(connection, ServerOne, "Beta", ct));
        Assert.Equal(5L, await SnapshotsAsync(connection, ServerOne, "Alpha", ct));
        Assert.Equal(5L, await SnapshotsAsync(connection, ServerTwo, "Delta", ct));
    }

    [Fact]
    public async Task AStepCapSmallerThanTheBacklog_FinishesTheBackfillInTwoTicks_WithTheRowsAnUncappedBuildStores()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var databases = new[] { "Alpha", "Beta", "Gamma" };

        await PlantServerAsync(connection, Capped, databases, 6, ct);

        // 18 snapshots to fold, 10 a tick: the first tick stops inside a pair, and the second tick resumes it.
        var first = await TickAsync(dataSource, 10, ObjectUsageChainBuilder.BuildPairAsync, ct);
        Assert.Equal(10, first.Steps);
        Assert.True(first.Deferred);
        var second = await TickAsync(dataSource, 10, ObjectUsageChainBuilder.BuildPairAsync, ct);
        Assert.Equal(8, second.Steps);
        Assert.False(second.Deferred);
        Assert.Equal(0, (await TickAsync(dataSource, 10, ObjectUsageChainBuilder.BuildPairAsync, ct)).Steps);

        await PlantServerAsync(connection, Uncapped, databases, 6, ct);
        var uncapped = await TickAsync(dataSource, int.MaxValue, ObjectUsageChainBuilder.BuildPairAsync, ct);
        Assert.Equal(18, uncapped.Steps);

        foreach (var database in databases)
        {
            var expected = await FingerprintAsync(connection, Uncapped, database, ct);
            Assert.Contains("\"collection_time\"", expected, StringComparison.Ordinal);
            Assert.Equal(expected, await FingerprintAsync(connection, Capped, database, ct));
        }
    }

    [Fact]
    public async Task ABuilderThatThrowsForOnePair_DoesNotStopTheOtherPairs_OrFailTheTick()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantServerAsync(connection, ServerOne, ["Alpha", "Beta", "Gamma"], 4, ct);
        await PlantServerAsync(connection, ServerTwo, ["Alpha"], 4, ct);

        Task<int> Failing(NpgsqlDataSource ds, int server, string database, int maxSteps, CancellationToken token) =>
            server == ServerOne && database == "Beta"
                ? throw new InvalidOperationException("planted failure")
                : ObjectUsageChainBuilder.BuildPairAsync(ds, server, database, maxSteps, token);

        var result = await TickAsync(dataSource, int.MaxValue, Failing, ct);
        Assert.Equal(new ObjectUsageChainTick.TickResult(3, 12, 1, false), result);
        Assert.Equal(4L, await SnapshotsAsync(connection, ServerOne, "Alpha", ct));
        Assert.Equal(4L, await SnapshotsAsync(connection, ServerOne, "Gamma", ct));
        Assert.Equal(4L, await SnapshotsAsync(connection, ServerTwo, "Alpha", ct));
        Assert.Equal(0L, await SnapshotsAsync(connection, ServerOne, "Beta", ct));

        // The next tick, with the real builder, builds the pair that failed and nothing else.
        var retry = await TickAsync(dataSource, int.MaxValue, ObjectUsageChainBuilder.BuildPairAsync, ct);
        Assert.Equal(new ObjectUsageChainTick.TickResult(1, 4, 0, false), retry);
        Assert.Equal(4L, await SnapshotsAsync(connection, ServerOne, "Beta", ct));
    }

    [Fact]
    public async Task TheRunsPurge_DeletesClosedRunsPastTheHorizon_AndKeepsOpenRunsAndRecentOnes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        Func<string, int> retentionDaysFor = _ => 10;
        var horizon = DarlingRetention.ObjectUsageChainRetentionDays(retentionDaysFor);
        var now = new DateTime(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc);

        async Task Run(int objectId, DateTime first, DateTime? last)
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO collect.object_usage_runs (server_id, database_name, object_id, index_id, first_time, last_time, schema_name, object_name, is_indexed_view) VALUES ($1, 'Alpha', $2, 1, $3, $4, 'dbo', 'T', false)",
                connection);
            command.Parameters.AddWithValue(ServerOne);
            command.Parameters.AddWithValue(objectId);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(first, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(last is { } l ? DateTime.SpecifyKind(l, DateTimeKind.Unspecified) : DBNull.Value);
            await command.ExecuteNonQueryAsync(ct);
        }

        /* Alpha still has a raw row, so its open runs stay (an open run of a pair with no raw row is purged: ObjectUsageChainRunRetentionLiveTests). */
        await using (var raw = new NpgsqlCommand(
            "INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name, database_name) VALUES (934950, $1, $2, 'tick-test', 'Alpha')", connection))
        {
            raw.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddDays(-1), DateTimeKind.Unspecified));
            raw.Parameters.AddWithValue(ServerOne);
            await raw.ExecuteNonQueryAsync(ct);
        }

        /* No hypertable here, so a closed run is kept the default seven days past the horizon: run 1 is past that, run 2 is not. */
        await Run(1, now.AddDays(-horizon - 40), now.AddDays(-horizon - 20));
        await Run(2, now.AddDays(-horizon - 5), now.AddDays(-horizon - 1));
        await Run(3, now.AddDays(-horizon - 40), null);
        await Run(4, now.AddDays(-horizon - 40), now.AddDays(-horizon + 2));
        await Run(5, now.AddDays(-2), null);

        var summary = await DarlingRetention.PurgeObjectUsageChainAsync(dataSource, false, retentionDaysFor, now, NullLogger.Instance, ct);
        Assert.Equal(0, summary.TablesFailed);

        await using var read = new NpgsqlCommand("SELECT object_id FROM collect.object_usage_runs ORDER BY object_id", connection);
        var kept = new List<int>();
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                kept.Add(reader.GetInt32(0));
            }
        }

        Assert.Equal(new[] { 2, 3, 4, 5 }, kept.ToArray());
    }

    [Fact]
    public async Task ADeadPair_IsNeverHandedToTheBuilder_AndANeverBuiltServerWithOldRowsIsStillBackfilled()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        // Server one is live: Alpha has fresh rows, Dead has only old ones and is already built up to its last snapshot. Server two was
        // never built and has old rows only.
        await PlantServerAsync(connection, ServerOne, ["Alpha"], 3, ct);
        await PlantServerAsync(connection, ServerOne, ["Dead"], 3, ct, OldDay);
        await PlantServerAsync(connection, ServerTwo, ["Old"], 3, ct, OldDay);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerOne, "Dead", int.MaxValue, ct);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerOne, "Alpha", int.MaxValue, ct);
        await PlantAsync(connection, ServerOne, "Alpha", 4, ct);

        // A later raw row for the dead pair that the chain has not folded: the pair is dead because it has no row in the window, so the
        // steady tick leaves it alone. Its watermark is behind its newest row, which is what a listing without the bound would hand over.
        await PlantAsync(connection, ServerOne, "Dead", 4, ct, OldDay);

        var calls = new List<(int Server, string Database, int Built)>();
        async Task<int> Recording(NpgsqlDataSource ds, int server, string database, int maxSteps, CancellationToken token)
        {
            var built = await ObjectUsageChainBuilder.BuildPairAsync(ds, server, database, maxSteps, token);
            calls.Add((server, database, built));
            return built;
        }

        var tick = await TickAsync(dataSource, int.MaxValue, Recording, ct);
        Assert.DoesNotContain(calls, c => c.Server == ServerOne && c.Database == "Dead");
        Assert.Equal(new[] { (ServerOne, "Alpha"), (ServerTwo, "Old") }, calls.Where(c => c.Built > 0).Select(c => (c.Server, c.Database)).OrderBy(c => c.Database, StringComparer.Ordinal).ToArray());
        Assert.Equal(new ObjectUsageChainTick.TickResult(2, 4, 0, false), tick);
        Assert.Equal(3L, await SnapshotsAsync(connection, ServerTwo, "Old", ct));
        Assert.Equal(3L, await SnapshotsAsync(connection, ServerOne, "Dead", ct));
    }

    [Fact]
    public async Task ADeadServersBackfill_ThatABoundStopsAfterOneStep_IsListedOnTheNextTicks_AndFinishes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        // The server is dead: all of its raw rows are older than the listing window, and it has no snapshots row until its first step.
        await PlantServerAsync(connection, ServerTwo, ["Old"], 3, ct, OldDay);

        var state = new ObjectUsageChainTick.TickState();
        async Task<ObjectUsageChainTick.TickResult> OneStepAsync() =>
            await ObjectUsageChainTick.RunTickAsync(dataSource, 1, TimeSpan.FromMinutes(5), ObjectUsageChainBuilder.BuildPairAsync, NullLogger.Instance, ct, state);

        // The first tick folds one snapshot and the bound stops it. The server now has a snapshots row, so only its pending backfill lists it.
        Assert.Equal(1, (await OneStepAsync()).Steps);
        Assert.Equal(1L, await SnapshotsAsync(connection, ServerTwo, "Old", ct));
        Assert.Equal(1, (await OneStepAsync()).Steps);
        Assert.Equal(2L, await SnapshotsAsync(connection, ServerTwo, "Old", ct));
        Assert.Equal(1, (await OneStepAsync()).Steps);
        Assert.Equal(3L, await SnapshotsAsync(connection, ServerTwo, "Old", ct));

        // The backfill is done: the server leaves the pending list and the next tick has nothing to build.
        Assert.Equal(0, (await OneStepAsync()).Steps);
        Assert.Empty(state.PendingBackfill);

        // The stepwise build stores what one uncapped build of the same history stores.
        await PlantServerAsync(connection, ServerOne, ["Old"], 3, ct, OldDay);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerOne, "Old", int.MaxValue, ct);
        Assert.Equal(await FingerprintAsync(connection, ServerOne, "Old", ct), await FingerprintAsync(connection, ServerTwo, "Old", ct));
    }

    [Fact]
    public async Task AServerWithNoRawIndexRows_IsNotListedForABackfill_AndAServerWithOldRowsIs()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        // Server two has old raw rows. Capped is registered and never collected, as a PostgreSQL target or a new server is.
        await PlantServerAsync(connection, ServerTwo, ["Old"], 2, ct, OldDay);
        await PlantServerAsync(connection, Capped, [], 0, ct);

        var listed = new List<int>();
        await using (var command = new NpgsqlCommand(ObjectUsageChainTick.UnbuiltServersSql, connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                listed.Add(reader.GetInt32(0));
            }
        }

        Assert.Equal([ServerTwo], listed);

        // The tick builds the dead server's history once and finds nothing to build for the other server, with no failure.
        var tick = await TickAsync(dataSource, int.MaxValue, ObjectUsageChainBuilder.BuildPairAsync, ct);
        Assert.Equal(new ObjectUsageChainTick.TickResult(1, 2, 0, false), tick);
    }

    [Fact]
    public async Task ABuiltLivePairWithNoNewRawRows_StopsAtTheProbeAndReadsNoChainRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantServerAsync(connection, ServerOne, ["Alpha"], 4, ct);
        var reads = new int[1];
        ObjectUsageChainBuilder.ChainReadCount.Value = reads;
        try
        {
            // The backfill (no watermark) skips the probe and reads the chain rows once.
            Assert.Equal(4, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerOne, "Alpha", int.MaxValue, ct));
            Assert.Equal(1, reads[0]);

            // A built pair with nothing new stops at the probe.
            Assert.Equal(0, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerOne, "Alpha", int.MaxValue, ct));
            Assert.Equal(1, reads[0]);

            // A new raw row passes the probe and reads the chain.
            await PlantAsync(connection, ServerOne, "Alpha", 5, ct);
            Assert.Equal(1, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerOne, "Alpha", int.MaxValue, ct));
            Assert.Equal(2, reads[0]);
        }
        finally
        {
            ObjectUsageChainBuilder.ChainReadCount.Value = null;
        }
    }

    [Fact]
    public async Task AStepCapOfOne_BuildsADifferentPairEachTick_ByResumingAfterThePairItStoppedAt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantServerAsync(connection, ServerOne, ["Alpha", "Beta", "Gamma"], 3, ct);

        var state = new ObjectUsageChainTick.TickState();
        var order = new List<string>();
        async Task<int> Recording(NpgsqlDataSource ds, int server, string database, int maxSteps, CancellationToken token)
        {
            var built = await ObjectUsageChainBuilder.BuildPairAsync(ds, server, database, maxSteps, token);
            if (built > 0)
            {
                order.Add(database);
            }

            return built;
        }

        // Three snapshots a pair and a cap of one step: without a cursor every tick would start at Alpha and fold only Alpha.
        for (var tick = 0; tick < 3; tick++)
        {
            Assert.Equal(1, (await ObjectUsageChainTick.RunTickAsync(dataSource, 1, TimeSpan.FromMinutes(5), Recording, NullLogger.Instance, ct, state)).Steps);
        }

        Assert.Equal(new[] { "Alpha", "Beta", "Gamma" }, order.ToArray());
    }

    [Fact]
    public async Task TwoTicksAtOnceOnOneStore_BuildEachPairOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantServerAsync(connection, ServerOne, ["Alpha", "Beta", "Gamma"], 3, ct);

        var calls = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();
        async Task<int> Slow(NpgsqlDataSource ds, int server, string database, int maxSteps, CancellationToken token)
        {
            calls.AddOrUpdate(database, 1, (_, n) => n + 1);
            await Task.Delay(400, token);
            return await ObjectUsageChainBuilder.BuildPairAsync(ds, server, database, maxSteps, token);
        }

        var first = ObjectUsageChainTick.RunTickAsync(dataSource, int.MaxValue, TimeSpan.FromMinutes(5), Slow, NullLogger.Instance, ct, new ObjectUsageChainTick.TickState());
        await Task.Delay(150, ct);
        var second = ObjectUsageChainTick.RunTickAsync(dataSource, int.MaxValue, TimeSpan.FromMinutes(5), Slow, NullLogger.Instance, ct, new ObjectUsageChainTick.TickState());
        var results = await Task.WhenAll(first, second);

        Assert.Equal(new ObjectUsageChainTick.TickResult(3, 9, 0, false), results[0]);
        Assert.Equal(new ObjectUsageChainTick.TickResult(0, 0, 0, false), results[1]);
        Assert.Equal(new[] { 1, 1, 1 }, calls.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => c.Value).ToArray());

        // The claim is released when the tick ends: the next tick takes it.
        await PlantAsync(connection, ServerOne, "Beta", 4, ct);
        Assert.Equal(1, (await TickAsync(dataSource, int.MaxValue, ObjectUsageChainBuilder.BuildPairAsync, ct)).Steps);
    }

    /// <summary>EXPLAIN (ANALYZE, BUFFERS) of one planner statement with its server id, on a store with sequential scans switched off so a missing index shows as an error text.</summary>
    private static async Task<string> PlanOfAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] args)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) " + sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg);
        }

        var lines = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(reader.GetString(0));
        }

        return string.Join("\n", lines);
    }

    private static async Task<List<string>> ListDatabasesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var names = new List<string>();
        await using var command = new NpgsqlCommand(ObjectUsageChainTick.DatabasesSql, connection);
        command.Parameters.AddWithValue(ServerOne);
        command.Parameters.AddWithValue(ObjectUsageChainTick.ListingWindowStart());
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    [Fact]
    public async Task ThePlannerStatements_ReadIndexEntriesAndNeverScanTheTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantServerAsync(connection, ServerOne, ["Alpha", "Beta", "Gamma"], 5, ct);
        await PlantServerAsync(connection, ServerTwo, ["Alpha", "Delta"], 5, ct);

        // Old chunks: a database and a server whose rows are all ten days old, outside the listing window.
        await PlantServerAsync(connection, ServerOne, ["Dead"], 5, ct, OldDay);
        await PlantServerAsync(connection, Capped, ["Gone"], 5, ct, OldDay);
        Assert.Equal(7, (await TickAsync(dataSource, int.MaxValue, ObjectUsageChainBuilder.BuildPairAsync, ct)).PairsBuilt);
        await using (var analyze = new NpgsqlCommand("ANALYZE collect.index_object_stats; ANALYZE collect.object_usage_snapshots; SET enable_seqscan = off", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        var servers = await PlanOfAsync(connection, ObjectUsageChainTick.ServersSql, ct, ObjectUsageChainTick.ListingWindowStart());
        var databases = await PlanOfAsync(connection, ObjectUsageChainTick.DatabasesSql, ct, ServerOne, ObjectUsageChainTick.ListingWindowStart());
        var watermarks = await PlanOfAsync(connection, ObjectUsageChainTick.WatermarksSql, ct, ServerOne);

        Assert.DoesNotContain("Dead", string.Join(",", await ListDatabasesAsync(connection, ct)), StringComparison.Ordinal);
        Assert.DoesNotContain("Seq Scan", servers, StringComparison.Ordinal);
        Assert.Contains("Index Only Scan using idx_index_object_stats_server_time", servers, StringComparison.Ordinal);
        Assert.DoesNotContain("Seq Scan", databases, StringComparison.Ordinal);
        Assert.Contains("Index Only Scan using idx_index_object_stats_object", databases, StringComparison.Ordinal);
        Assert.DoesNotContain("Seq Scan", watermarks, StringComparison.Ordinal);
        Assert.Contains("Index Only Scan using pk_object_usage_snapshots", watermarks, StringComparison.Ordinal);
    }
}

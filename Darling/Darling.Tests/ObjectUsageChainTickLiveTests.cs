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
/// </summary>
public sealed class ObjectUsageChainTickLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9349 chain tick tests.";
    private const int ServerOne = -935001;
    private const int ServerTwo = -935002;
    private const int Capped = -935003;
    private const int Uncapped = -935004;

    private static readonly DateTime Start = new(2026, 9, 1, 8, 0, 0);

    private static DateTime Day(int day) => new DateTime(2026, 10, 1, 2, 0, 0).AddDays(day - 1);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task<ObjectUsageChainTick.TickResult> TickAsync(
        NpgsqlDataSource dataSource, int maxSteps, ObjectUsageChainTick.BuildPair build, CancellationToken ct) =>
        ObjectUsageChainTick.RunTickAsync(dataSource, DateTime.UtcNow, maxSteps, TimeSpan.FromMinutes(5), build, NullLogger.Instance, ct);

    /// <summary>The snapshot of one day for one database: two indexes whose counters grow by day.</summary>
    private static async Task PlantAsync(NpgsqlConnection connection, int serverId, string database, int day, CancellationToken ct)
    {
        foreach (var (indexId, factor) in new[] { (1, 10L), (2, 3L) })
        {
            await using var command = new NpgsqlCommand(
                @"INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_type_desc, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_update, is_indexed_view, answering_server_name, is_memory_optimized)
VALUES ($1,$2,$3,'tick-sql1',$4,$5,7,'dbo',100,'Orders',$6,'CLUSTERED',$7,0,0,$8,$9,$9,false,'n1',false)", connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(Day(day));
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(Start);
            command.Parameters.AddWithValue(database);
            command.Parameters.AddWithValue(indexId);
            command.Parameters.AddWithValue(factor * day);
            command.Parameters.AddWithValue(factor * day);
            command.Parameters.AddWithValue(Day(day).AddHours(-1));
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task PlantServerAsync(NpgsqlConnection connection, int serverId, string[] databases, int days, CancellationToken ct)
    {
        foreach (var database in databases)
        {
            for (var day = 1; day <= days; day++)
            {
                await PlantAsync(connection, serverId, database, day, ct);
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

        Assert.Equal(new[] { 3, 4, 5 }, kept.ToArray());
    }
}

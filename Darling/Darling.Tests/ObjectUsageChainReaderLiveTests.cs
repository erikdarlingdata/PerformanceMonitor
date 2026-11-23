/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9349 part D1: <c>get_object_usage</c>'s persisted read path over a real store. Eight raw snapshots of one answering instance that restarts once,
/// the three chain tables filled by hand with the rows the design says the builder writes, and three window starts: the persisted path's answer is
/// record-equal to the full fold's. A stale step version sends the read to the full fold. The generated equivalence proof is part D2, after the
/// builder lands.
/// </summary>
public sealed class ObjectUsageChainReaderLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9349 object usage reader tests.";
    private const string Node = "chain-sql1";
    private const string Db = "Sales";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(Node);

    /* A sub-second value on purpose: PostgreSQL keeps microseconds, so the replay's exact comparisons have to hold at that precision. */
    private static readonly DateTime Day0 = new DateTime(2026, 10, 1, 6, 0, 0).AddTicks(1_234_560);
    private static readonly DateTime StartOne = new(2026, 8, 1, 3, 0, 0);
    private static readonly DateTime StartTwo = Day0.AddDays(4).AddHours(-2);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime T(int day) => Day0.AddDays(day);

    /* Object 100 index 1 (clustered) restarts at snapshot 4; index 2 is a nonclustered index whose updates the window never counts. Object 200 is
       never used. Snapshot 6 changes nothing on index 1, so its row stores no delta. */
    private static readonly long[] Seeks1 = [10, 20, 35, 50, 5, 12, 12, 30];
    private static readonly long[] Updates1 = [1, 2, 3, 4, 1, 2, 2, 3];
    private static readonly long[] Seeks2 = [0, 1, 1, 3, 2, 2, 4, 4];
    private static readonly long[] Updates2 = [5, 6, 6, 7, 1, 1, 2, 2];

    private static DateTime Stamp1(int day) => (day == 6 ? T(5) : T(day)).AddHours(-3);

    [Fact]
    public async Task ThePersistedPath_IsRecordEqualToTheFullFold_ForThreeWindowStarts_AndAStaleVersionTakesTheFullFold()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Node, ct);

        await SeedRawAsync(connection, ct);
        await SeedChainAsync(connection, ct);

        foreach (var windowStart in new[] { T(0), T(2).AddHours(1), T(5).AddHours(1) })
        {
            var expected = await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Db, windowStart, ct);
            var (persisted, reason) = await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, windowStart, ct);

            Assert.Null(reason);
            Assert.NotNull(persisted);
            AssertSame(expected, persisted);
            AssertSame(expected, await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Db, windowStart, ct));
        }

        /* The window from snapshot 0 crosses the restart: the reset, its blind span and the whole-value counts are in the answer. */
        var whole = await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Db, T(0), ct);
        var reset = Assert.Single(whole.Resets);
        Assert.Equal(ObjectUsageResetReasons.Restart, reset.Reasons);
        Assert.Equal(T(3), reset.BlindFrom);
        Assert.Equal(8, whole.SnapshotCount);
        Assert.Equal(70L + 7L, whole.Objects.Single(o => o.ObjectId == 100).Reads);

        /* A stale step version on any row the window reads: the persisted path gives up and the read is the full fold's. The window that starts after
           the stale row does not read it and is still served from the chain. */
        await using (var stale = new NpgsqlCommand(
            $"UPDATE collect.object_usage_snapshots SET step_version = {ObjectUsageChainVersion.Current - 1} WHERE collection_time = $1", connection))
        {
            stale.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = T(3) });
            Assert.Equal(1, await stale.ExecuteNonQueryAsync(ct));
        }

        var (none, staleReason) = await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, T(0), ct);
        Assert.Null(none);
        Assert.Equal("a facts row has an older step version", staleReason);
        AssertSame(whole, await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Db, T(0), ct));
        Assert.NotNull((await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, T(5).AddHours(1), ct)).Result);

        /* No facts row at the baseline, and a newest facts row before the newest snapshot, each fall back too. */
        await ExecAsync(connection, "DELETE FROM collect.object_usage_snapshots WHERE collection_time = " + Literal(T(7)), ct);
        Assert.Equal("the newest facts row is before the newest snapshot",
            (await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, T(5).AddHours(1), ct)).Reason);
        await ExecAsync(connection, "DELETE FROM collect.object_usage_snapshots WHERE collection_time = " + Literal(T(5)), ct);
        Assert.Equal("no facts row at the baseline",
            (await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, T(5).AddHours(1), ct)).Reason);
        AssertSame(
            await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Db, T(5).AddHours(1), ct),
            await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Db, T(5).AddHours(1), ct));
    }

    [Fact]
    public async Task AnIndexWithRawRowsButNoRun_TakesTheFullFold_EvenWhenAnotherIndexOfTheObjectHasARun()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Node, ct);
        await SeedRawAsync(connection, ct);
        await SeedChainAsync(connection, ct);

        var windowStart = T(2).AddHours(1);
        Assert.NotNull((await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, windowStart, ct)).Result);

        /* Object 100 keeps its clustered index's run; its nonclustered index loses the run while its raw rows stay. */
        await ExecAsync(connection, "DELETE FROM collect.object_usage_runs WHERE object_id = 100 AND index_id = 2", ct);

        var (persisted, reason) = await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, windowStart, ct);
        Assert.Null(persisted);
        Assert.NotNull(reason);
        var expected = await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Db, windowStart, ct);
        Assert.Equal(2, expected.Objects.Single(o => o.ObjectId == 100).IndexCount);
        AssertSame(expected, await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Db, windowStart, ct));
    }

    [Fact]
    public async Task AnIndexWhoseOnlyRunStartsAfterTheBaselineRow_TakesTheFullFold_AndTheAnswerIsTheFullFolds()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Node, ct);
        await SeedRawAsync(connection, ct);
        await SeedChainAsync(connection, ct);

        Assert.NotNull((await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, T(0), ct)).Result);

        /* Object 100's nonclustered index has a run that overlaps the window and starts after the baseline snapshot, whose raw row the
           replay reads: the run overlaps the window and does not cover that row. */
        await ExecAsync(connection, "UPDATE collect.object_usage_runs SET first_time = " + Literal(T(1)) + " WHERE object_id = 100 AND index_id = 2", ct);

        var (persisted, reason) = await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Db, T(0), ct);
        Assert.Null(persisted);
        Assert.NotNull(reason);
        AssertSame(
            await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Db, T(0), ct),
            await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Db, T(0), ct));
    }

    [Fact]
    public async Task AnUnreadableChainTable_TakesTheFullFoldWithAWarning_AndACancelledCallerStillThrows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Node, ct);
        await SeedRawAsync(connection, ct);
        await SeedChainAsync(connection, ct);

        var windowStart = T(2).AddHours(1);
        var expected = await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Db, windowStart, ct);
        await ExecAsync(connection, "ALTER TABLE collect.object_usage_deltas RENAME TO object_usage_deltas_gone", ct);

        var logger = new CapturingTestLogger();
        AssertSame(expected, await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Db, windowStart, ct, logger));
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("deltas", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("42P01", logger.Joined, StringComparison.Ordinal);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Db, windowStart, cancelled.Token, logger));
    }

    private static void AssertSame(ObjectUsageWindowResult expected, ObjectUsageWindowResult actual)
    {
        Assert.Equal(expected.CoveredFrom, actual.CoveredFrom);
        Assert.Equal(expected.CoveredTo, actual.CoveredTo);
        Assert.Equal(expected.SnapshotCount, actual.SnapshotCount);
        Assert.Equal(expected.LargestGap, actual.LargestGap);
        Assert.Equal(expected.IndexResets, actual.IndexResets);
        Assert.Equal(expected.BlindTime, actual.BlindTime);
        Assert.Equal(expected.Resets, actual.Resets);
        Assert.Equal(expected.AnsweringServerNames, actual.AnsweringServerNames);
        Assert.Equal(expected.Objects, actual.Objects);
    }

    private static string Literal(DateTime value) => $"'{value:yyyy-MM-dd HH:mm:ss.ffffff}'::timestamp";

    private static async Task SeedRawAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        for (var day = 0; day < 8; day++)
        {
            var start = day >= 4 ? StartTwo : StartOne;
            await PlantRawAsync(connection, T(day), start, 100, "Orders", 1, "CLUSTERED", Seeks1[day], Updates1[day], Stamp1(day), ct);
            await PlantRawAsync(connection, T(day), start, 100, "Orders", 2, "NONCLUSTERED", Seeks2[day], Updates2[day], null, ct);
            await PlantRawAsync(connection, T(day), start, 200, "Quiet", 1, "CLUSTERED", 0, 0, null, ct);
        }
    }

    private static Task PlantRawAsync(
        NpgsqlConnection connection, DateTime at, DateTime start, int objectId, string table, int indexId, string type, long seeks, long updates,
        DateTime? lastSeek, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_type_desc, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, is_indexed_view, answering_server_name, is_memory_optimized)
VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,$9,$10,$11,0,0,$12,$13,false,$14,false)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Node, DarlingMcpTestData.Naive(start), Db, objectId, table, indexId, type, seeks,
            updates, lastSeek is { } s ? DarlingMcpTestData.Naive(s) : null, Node);

    /* The rows the design says the builder writes for this history. A step's delta rows are the indexes that were counted whole, new to the
       compared snapshot, or changed in any counter or stamp; the first snapshot of a node writes none. */
    private static async Task SeedChainAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var builtAt = DarlingMcpTestData.Naive(T(8));
        for (var day = 0; day < 8; day++)
        {
            var restart = day == 4;
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.object_usage_snapshots (server_id, database_name, collection_time, step_version, answering_server_name, sqlserver_start_time, node_key, compared_with, kept_start, reasons, counted, index_resets, blind_from, built_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,0,$12,$13)",
                ServerId, Db, DarlingMcpTestData.Naive(T(day)), (short)ObjectUsageChainVersion.Current, Node,
                DarlingMcpTestData.Naive(day >= 4 ? StartTwo : StartOne), Node,
                day == 0 ? null : DarlingMcpTestData.Naive(T(day - 1)),
                DarlingMcpTestData.Naive(day >= 4 ? StartTwo : StartOne),
                restart ? (int)ObjectUsageResetReasons.Restart : 0,
                (short)(day == 0 ? 0 : restart ? 2 : 1),
                restart ? DarlingMcpTestData.Naive(T(3)) : null, builtAt);
        }

        await PlantDeltaAsync(connection, 1, 100, 1, 10, 1, Stamp1(1), ct);
        await PlantDeltaAsync(connection, 1, 100, 2, 1, 1, null, ct);
        await PlantDeltaAsync(connection, 2, 100, 1, 15, 1, Stamp1(2), ct);
        await PlantDeltaAsync(connection, 3, 100, 1, 15, 1, Stamp1(3), ct);
        await PlantDeltaAsync(connection, 3, 100, 2, 2, 1, null, ct);
        await PlantDeltaAsync(connection, 4, 100, 1, 5, 1, Stamp1(4), ct);
        await PlantDeltaAsync(connection, 4, 100, 2, 2, 1, null, ct);
        await PlantDeltaAsync(connection, 4, 200, 1, 0, 0, null, ct);
        await PlantDeltaAsync(connection, 5, 100, 1, 7, 1, Stamp1(5), ct);
        await PlantDeltaAsync(connection, 6, 100, 2, 2, 1, null, ct);
        await PlantDeltaAsync(connection, 7, 100, 1, 18, 1, Stamp1(7), ct);

        foreach (var (objectId, indexId, name) in new[] { (100, 1, "Orders"), (100, 2, "Orders"), (200, 1, "Quiet") })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.object_usage_runs (server_id, database_name, object_id, index_id, first_time, last_time, schema_name, object_name, is_indexed_view, is_memory_optimized)
VALUES ($1,$2,$3,$4,$5,NULL,'dbo',$6,false,false)",
                ServerId, Db, objectId, indexId, DarlingMcpTestData.Naive(T(0)), name);
        }
    }

    private static Task PlantDeltaAsync(
        NpgsqlConnection connection, int day, int objectId, int indexId, long reads, long updates, DateTime? lastRead, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.object_usage_deltas (server_id, database_name, collection_time, object_id, index_id, reads, updates, last_read, last_update)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULL)",
            ServerId, Db, DarlingMcpTestData.Naive(T(day)), objectId, indexId, reads, updates, lastRead is { } r ? DarlingMcpTestData.Naive(r) : null);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}

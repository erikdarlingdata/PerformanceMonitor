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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9349 review round 1, part P: how long the retention pass keeps <c>collect.object_usage_runs</c>. <c>drop_chunks</c> keeps a raw chunk of
/// <c>index_object_stats</c> until all of its range is past the horizon, so the full fold still reads raw rows in the chunk that straddles the horizon.
/// A closed run purged at the horizon itself leaves those rows with no run and the persisted path answers differently from the full fold, with no
/// fallback. These facts run the pass over a real store with a straddling chunk and read both paths afterwards.
/// </summary>
public sealed class ObjectUsageChainRunRetentionLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG (with TimescaleDB installed) to run the live #9349 run retention tests.";
    private const string Node = "run-ret-sql1";
    private const string Live = "Sales";
    private const string Dead = "Retired";
    private const int HorizonDays = 10;
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(Node);
    private static readonly DateTime Started = new(2026, 1, 1, 8, 0, 0);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Func<string, int> Horizon => name =>
        string.Equals(name, ObjectUsageChain.SourceCollector, StringComparison.Ordinal) ? HorizonDays : CollectorScheduleDefaults.All[name].RetentionDays;

    /// <summary>
    /// The snapshot times, built from the start of the day the raw horizon falls in. The raw table's chunks are one day wide in a test store, so the
    /// chunk that starts at <see cref="Straddle"/> holds the horizon: a row in it is older than the horizon and still kept.
    /// </summary>
    private sealed record Times(DateTime Floor)
    {
        /// <summary>A snapshot in a chunk that is wholly past the horizon, and past it by more than a chunk.</summary>
        public DateTime Old => Floor.AddDays(-2).AddHours(1);

        /// <summary>A snapshot in the chunk the horizon falls in: older than the horizon, and still in the raw table.</summary>
        public DateTime Straddle => Floor.AddSeconds(1);

        public DateTime Newer1 => Floor.AddDays(1).AddHours(1);

        public DateTime Newer2 => Floor.AddDays(1).AddHours(2);
    }

    private static Times Now() => new(DateTime.UtcNow.AddDays(-HorizonDays).Date);

    /// <summary>
    /// Live pair: index A in every snapshot, B in the old and the straddling snapshot only (its run closes at the straddling one), C in the old
    /// snapshot only (its run closes there), D from the first newer snapshot on (open). Retired pair: one index, only in snapshots that are gone after the pass.
    /// </summary>
    private static async Task<(NpgsqlDataSource DataSource, NpgsqlConnection Connection, Times T)> SeedAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Node, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureObjectUsageHypertablesAsync(connection, NullLogger.Instance, ct));

        var t = Now();
        var step = 0;
        async Task Plant(string database, DateTime at, int objectId)
        {
            step++;
            await PlantAsync(connection, database, at, objectId, 10 * step, ct);
        }

        foreach (var obj in new[] { 100, 200, 300 })
        {
            await Plant(Live, t.Old, obj);
        }

        foreach (var obj in new[] { 100, 200 })
        {
            await Plant(Live, t.Straddle, obj);
        }

        foreach (var at in new[] { t.Newer1, t.Newer2 })
        {
            foreach (var obj in new[] { 100, 400 })
            {
                await Plant(Live, at, obj);
            }
        }

        await Plant(Dead, t.Old, 900);
        await Plant(Dead, t.Old.AddHours(1), 900);

        var dataSource = NpgsqlDataSource.Create(connectionString);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerId, Live, int.MaxValue, ct);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerId, Dead, int.MaxValue, ct);
        return (dataSource, connection, t);
    }

    /// <summary>One <c>index_object_stats</c> row for <see cref="Node"/>: the index (object, 1) of <paramref name="database"/> at <paramref name="at"/>, with a counter that grows with <paramref name="seeks"/>.</summary>
    private static async Task PlantAsync(NpgsqlConnection connection, string database, DateTime at, int objectId, long seeks, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_type_desc, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, is_indexed_view, answering_server_name, is_memory_optimized)
VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,1,'CLUSTERED',$9,0,0,0,$10,false,$4,false)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, Node, DarlingMcpTestData.Naive(Started), database, objectId, "T" + objectId,
            seeks, DarlingMcpTestData.Naive(at.AddMinutes(-5)));
    }

    private static async Task<List<string>> RunsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT database_name || '/' || object_id || '/' || CASE WHEN last_time IS NULL THEN 'open' ELSE 'closed' END FROM collect.object_usage_runs WHERE server_id = $1 ORDER BY 1",
            connection);
        command.Parameters.AddWithValue(ServerId);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
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

    [Fact]
    public async Task ARawChunkThatStraddlesTheHorizon_KeepsTheRunOfEveryRawRowTheFullFoldCanStillSee()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for this test");

        var bodySucceeded = false;
        try
        {
            var (dataSource, connection, t) = await SeedAsync(scratch.ConnectionString, ct);
            await using var _ = dataSource;
            await using var __ = connection;

            await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: true, NullLogger.Instance, ct, Horizon);

            /* The chunk of the old snapshot is gone from the raw table and the straddling chunk is not: that is the state under test. */
            Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM collect.index_object_stats WHERE server_id = {ServerId} AND collection_time < '{t.Floor:yyyy-MM-dd}'::timestamp", ct));
            Assert.Equal(2L, await CountAsync(connection,
                $"SELECT count(*) FROM collect.index_object_stats WHERE server_id = {ServerId} AND database_name = '{Live}' AND collection_time < '{t.Floor.AddDays(1):yyyy-MM-dd}'::timestamp", ct));

            /* Every raw row the full fold can still read has a run that covers its snapshot. */
            Assert.Equal(0L, await CountAsync(connection, $@"
SELECT count(*)
FROM collect.index_object_stats AS s
WHERE s.server_id = {ServerId}
AND   NOT EXISTS
(
    SELECT 1
    FROM collect.object_usage_runs AS r
    WHERE r.server_id = s.server_id
    AND   r.database_name = s.database_name
    AND   r.object_id = s.object_id
    AND   r.index_id = s.index_id
    AND   r.first_time <= s.collection_time
    AND   (r.last_time IS NULL OR r.last_time >= s.collection_time)
)", ct));

            foreach (var windowStart in new[] { t.Straddle, t.Newer1 })
            {
                var full = await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Live, windowStart, ct);
                var (persisted, reason) = await DarlingObjectUsageReader.TryReadPersistedAsync(dataSource, ServerId, Live, windowStart, ct);
                Assert.Null(reason);
                Assert.NotNull(persisted);
                AssertSame(full, persisted);
                AssertSame(full, await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Live, windowStart, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AClosedRunOlderThanEveryRawRowGoes_AnOpenRunOfALivePairStays_AnOpenRunOfAPairWithNoRawRowsGoes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for this test");

        var bodySucceeded = false;
        try
        {
            var (dataSource, connection, _) = await SeedAsync(scratch.ConnectionString, ct);
            await using var _ = dataSource;
            await using var __ = connection;

            Assert.Equal(
                [$"{Dead}/900/open", $"{Live}/100/open", $"{Live}/200/closed", $"{Live}/300/closed", $"{Live}/400/open"],
                await RunsAsync(connection, ct));

            await DarlingRetention.PurgeAsync(dataSource, timescaleAvailable: true, NullLogger.Instance, ct, Horizon);

            /* C closed in the old chunk, which the raw drop took: older than every surviving raw row, gone. B closed in the straddling chunk, which is still stored: kept. */
            Assert.Equal([$"{Live}/100/open", $"{Live}/200/closed", $"{Live}/400/open"], await RunsAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A table narrowed from 24 h to 6 h chunks keeps its older 24 h chunks. The oldest surviving chunk is one of those and straddles the horizon by
    /// more than 6 h, so the raw rows it keeps run past the horizon by more than the current interval. Every one of them still has its run.
    /// The clock is fixed so the straddle does not depend on the hour the test runs.
    /// </summary>
    [Fact]
    public async Task AfterTheChunkIntervalIsNarrowed_AnOlderWideChunkThatStraddlesTheHorizonStillKeepsTheRunOfEveryRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for this test");

        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, Node, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureObjectUsageHypertablesAsync(connection, NullLogger.Instance, ct));
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT set_chunk_time_interval('collect.index_object_stats', INTERVAL '24 hours')");

            /* A fixed clock: the horizon is day 11 at 10:00, so the 24 h chunk of day 11 (00:00 to 24:00) holds it and runs 14 h past it. */
            var day0 = DateTime.UtcNow.Date.AddDays(-60);
            var now = day0.AddDays(HorizonDays + 11).AddHours(10);
            var horizon = now.AddDays(-HorizonDays);
            var old = day0.AddHours(1);
            var straddleBefore = day0.AddDays(11).AddHours(2);
            var straddleAfter = day0.AddDays(11).AddHours(20);
            Assert.True(straddleBefore < horizon && horizon.AddHours(6) < straddleAfter);

            await PlantAsync(connection, Live, old, 100, 10, ct);
            await PlantAsync(connection, Live, old, 200, 20, ct);
            await PlantAsync(connection, Live, old, 300, 30, ct);
            await PlantAsync(connection, Live, straddleBefore, 100, 40, ct);
            await PlantAsync(connection, Live, straddleBefore, 200, 50, ct);
            await PlantAsync(connection, Live, straddleAfter, 100, 60, ct);

            /* The reconciler's narrowing: only chunks created from here on are 6 h wide. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT set_chunk_time_interval('collect.index_object_stats', INTERVAL '6 hours')");
            var newer1 = day0.AddDays(13).AddHours(1);
            var newer2 = day0.AddDays(13).AddHours(8);
            foreach (var at in new[] { newer1, newer2 })
            {
                await PlantAsync(connection, Live, at, 100, 70, ct);
                await PlantAsync(connection, Live, at, 400, 80, ct);
            }

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ObjectUsageChainBuilder.BuildPairAsync(dataSource, ServerId, Live, int.MaxValue, ct);
            Assert.Equal(
                [$"{Live}/100/open", $"{Live}/200/closed", $"{Live}/300/closed", $"{Live}/400/open"],
                await RunsAsync(connection, ct));

            /* The raw drop at the fixed horizon: the 24 h chunk of day 0 goes, the 24 h chunk of day 11 stays. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"SELECT drop_chunks('collect.index_object_stats', older_than => '{horizon:yyyy-MM-dd HH:mm:ss}'::timestamp)");
            Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM collect.index_object_stats WHERE server_id = {ServerId} AND collection_time < '{day0.AddDays(11):yyyy-MM-dd}'::timestamp", ct));
            Assert.Equal(3L, await CountAsync(connection, $"SELECT count(*) FROM collect.index_object_stats WHERE server_id = {ServerId} AND collection_time < '{day0.AddDays(12):yyyy-MM-dd}'::timestamp", ct));

            await DarlingRetention.PurgeObjectUsageChainAsync(dataSource, timescaleAvailable: true, Horizon, now, NullLogger.Instance, ct);

            /* B closed at straddleBefore: older than the horizon by less than the 6 h width the table has now, but its 24 h chunk is still stored. C is older than every surviving raw row. */
            Assert.Equal([$"{Live}/100/open", $"{Live}/200/closed", $"{Live}/400/open"], await RunsAsync(connection, ct));

            Assert.Equal(0L, await CountAsync(connection, $@"
SELECT count(*)
FROM collect.index_object_stats AS s
WHERE s.server_id = {ServerId}
AND   NOT EXISTS
(
    SELECT 1
    FROM collect.object_usage_runs AS r
    WHERE r.server_id = s.server_id
    AND   r.database_name = s.database_name
    AND   r.object_id = s.object_id
    AND   r.index_id = s.index_id
    AND   r.first_time <= s.collection_time
    AND   (r.last_time IS NULL OR r.last_time >= s.collection_time)
)", ct));

            foreach (var windowStart in new[] { straddleBefore, straddleAfter, newer1 })
            {
                AssertSame(
                    await DarlingObjectUsageReader.ReadTablesFullAsync(dataSource, ServerId, Live, windowStart, ct),
                    await DarlingObjectUsageReader.ReadTablesAsync(dataSource, ServerId, Live, windowStart, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>On a store with no TimescaleDB there is no chunk to read, so the margin is the fallback of seven days, on the safe side.</summary>
    [Fact]
    public async Task WithNoHypertable_AClosedRunIsKeptSevenDaysPastTheHorizon()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var horizon = now.AddDays(-HorizonDays);
            var rows = new (int ObjectId, DateTime Last)[]
            {
                (1, horizon.AddDays(-3)),
                (2, horizon.AddDays(-6)),
                (3, horizon.AddDays(-8)),
                (4, horizon.AddHours(1)),
            };
            foreach (var (objectId, last) in rows)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collect.object_usage_runs (server_id, database_name, object_id, index_id, first_time, last_time, schema_name, object_name, is_indexed_view)
VALUES ($1,$2,$3,1,$4,$5,'dbo','T',false)",
                    ServerId, Live, objectId, DarlingMcpTestData.Naive(last.AddDays(-20)), DarlingMcpTestData.Naive(last));
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            Assert.Null(await DarlingRetention.ReadOldestRawChunkStartAsync(postgres, timescaleAvailable: false, NullLogger.Instance, ct));
            await DarlingRetention.PurgeObjectUsageChainAsync(postgres, timescaleAvailable: false, Horizon, now, NullLogger.Instance, ct);

            /* 1 and 2 are past the horizon by less than seven days and stay, 3 is past it by more and goes, 4 is inside the horizon. */
            Assert.Equal([$"{Live}/1/closed", $"{Live}/2/closed", $"{Live}/4/closed"], await RunsAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}

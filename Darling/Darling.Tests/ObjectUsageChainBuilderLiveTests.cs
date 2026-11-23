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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #9349 part C1: the chain builder. Three servers hold the same seeded history for three databases (daily growth, a restart, a database-wide
/// clear, a lone index clear, an index dropped and recreated after a gap, an object dropped, a rename, duplicate rows for one index, two named
/// nodes answering in turn, and a store that gained the answering name). The first is built in one call (a backfill), the second one snapshot
/// per call (steady state), the third in a backfill read in tiny batches, and all three must store the same rows in all three tables, apart
/// from built_at. Then what the rows say: which indexes get delta rows, the runs, a stale step version and an up to date chain.
/// </summary>
public sealed class ObjectUsageChainBuilderLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #9349 chain builder tests.";
    private const string Sales = "Sales";
    private const string Mix = "Mix";
    private const string Named = "Named";
    private const int Backfilled = -934911;
    private const int OneByOne = -934912;
    private const int TinyBatches = -934913;

    private const string SnapshotOrder = "collection_time";
    private const string DeltaOrder = "collection_time, object_id, index_id";
    private const string RunOrder = "object_id, index_id, first_time";

    private static readonly DateTime FirstStart = new(2026, 9, 1, 8, 0, 0);
    private static readonly DateTime SecondStart = new(2026, 10, 5, 8, 0, 0);

    private static DateTime Day(int day) => new DateTime(2026, 10, 1, 2, 0, 0).AddDays(day - 1);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One raw row to plant.</summary>
    private sealed record Raw(
        int Day, string Database, string? Node, DateTime Start, int ObjectId, int IndexId, string Schema, string Name, long Seeks, long Updates,
        DateTime? LastSeek, DateTime? LastUpdate);

    /// <summary>The counter of the Sales history: grows by a step a day, restarts on day 5 and is cleared on day 8.</summary>
    private static long Growth(int day) => day <= 4 ? 10 * day : day <= 7 ? 3 + 10 * (day - 5) : 2 + 10 * (day - 8);

    private static IEnumerable<Raw> SalesRows()
    {
        for (var d = 1; d <= 14; d++)
        {
            var start = d < 5 ? FirstStart : SecondStart;
            var stamp = Day(d).AddHours(-1);

            Raw Use(int obj, int idx, string name, long seeks) =>
                new(d, Sales, "n1", start, obj, idx, "dbo", name, seeks, idx == 1 ? seeks : 0, stamp, idx == 1 ? stamp : null);

            yield return Use(100, 1, "Orders", Growth(d));

            /* The same index twice in one snapshot: the larger reading counts. */
            if (d == 3)
            {
                yield return Use(100, 1, "Orders", Growth(d) + 5);
            }

            /* Cleared on its own on day 10 while the rest keep growing. */
            yield return Use(100, 2, "Orders", d >= 10 ? 1 + 10 * (d - 10) : Growth(d));

            /* Never used since the restart. */
            yield return new Raw(d, Sales, "n1", start, 200, 1, "dbo", "Dim", d < 5 ? 7 : 0, 0, d < 5 ? new DateTime(2026, 9, 30) : null, null);

            /* Dropped on day 4 and created again on day 7. */
            if (d is <= 3 or >= 7)
            {
                yield return Use(300, 1, "Temp", Growth(d) * 2);
            }

            /* Dropped for good after day 8. */
            if (d <= 8)
            {
                yield return Use(400, 1, "Gone", Growth(d) * 3);
            }

            /* Renamed on day 7. */
            yield return Use(500, 1, d < 7 ? "Old" : "New", Growth(d) * 4);
        }
    }

    private static IEnumerable<Raw> MixRows()
    {
        /* Odd days n1, even days n2. Each started within a day of the first snapshot, so a first sight counts whole. */
        for (var d = 1; d <= 6; d++)
        {
            var odd = d % 2 == 1;
            yield return new Raw(
                d, Mix, odd ? "n1" : "n2", odd ? new DateTime(2026, 9, 30, 12, 0, 0) : new DateTime(2026, 9, 30, 13, 0, 0), 10, 1, "dbo", "T",
                (odd ? 10 : 100) * d, 0, Day(d).AddHours(-1), null);
        }
    }

    private static IEnumerable<Raw> NamedRows()
    {
        for (var d = 1; d <= 6; d++)
        {
            yield return new Raw(d, Named, d <= 3 ? null : "srv", FirstStart, 10, 1, "dbo", "T", 10 * d, 0, Day(d).AddHours(-1), null);
        }
    }

    private static async Task PlantAsync(NpgsqlConnection connection, int serverId, IEnumerable<Raw> rows, CancellationToken ct)
    {
        foreach (var r in rows)
        {
            await using var command = new NpgsqlCommand(
                @"INSERT INTO collect.index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_type_desc, user_seeks, user_scans, user_lookups, user_updates, last_user_seek, last_user_update, is_indexed_view, answering_server_name, is_memory_optimized)
VALUES ($1,$2,$3,'chain-sql1',$4,$5,7,$6,$7,$8,$9,'CLUSTERED',$10,0,0,$11,$12,$13,false,$14,false)", connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(Day(r.Day));
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(r.Start);
            command.Parameters.AddWithValue(r.Database);
            command.Parameters.AddWithValue(r.Schema);
            command.Parameters.AddWithValue(r.ObjectId);
            command.Parameters.AddWithValue(r.Name);
            command.Parameters.AddWithValue(r.IndexId);
            command.Parameters.AddWithValue(r.Seeks);
            command.Parameters.AddWithValue(r.Updates);
            command.Parameters.AddWithValue((object?)r.LastSeek ?? DBNull.Value);
            command.Parameters.AddWithValue((object?)r.LastUpdate ?? DBNull.Value);
            command.Parameters.AddWithValue((object?)r.Node ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>The pair's three tables as text, without server_id and built_at.</summary>
    private static async Task<(string Snapshots, string Deltas, string Runs)> FingerprintAsync(
        NpgsqlConnection connection, int serverId, string database, CancellationToken ct)
    {
        async Task<string> Text(string table, string order)
        {
            await using var command = new NpgsqlCommand(
                $"SELECT coalesce(string_agg((to_jsonb(t) - 'server_id' - 'built_at')::text, E'\\n' ORDER BY {order}), '') FROM {table} AS t WHERE server_id = $1 AND database_name = $2",
                connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(database);
            return (string)(await command.ExecuteScalarAsync(ct))!;
        }

        return (
            await Text(ObjectUsageChain.SnapshotsTable, SnapshotOrder),
            await Text(ObjectUsageChain.DeltasTable, DeltaOrder),
            await Text(ObjectUsageChain.RunsTable, RunOrder));
    }

    private static async Task<List<object[]>> RowsAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg);
        }

        var rows = new List<object[]>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new object[reader.FieldCount];
            reader.GetValues(row);
            rows.Add(row);
        }

        return rows;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        (long)(await RowsAsync(connection, sql, ct))[0][0];

    [Fact]
    public async Task ABackfill_AndOneSnapshotPerCall_AndABackfillInTinyBatches_StoreTheSameRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        foreach (var server in new[] { Backfilled, OneByOne, TinyBatches })
        {
            await PlantAsync(connection, server, SalesRows(), ct);
            await PlantAsync(connection, server, MixRows(), ct);
            await PlantAsync(connection, server, NamedRows(), ct);
        }

        foreach (var db in new[] { Sales, Mix, Named })
        {
            await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, db, int.MaxValue, ct);
            await ObjectUsageChainBuilder.BuildPairAsync(dataSource, TinyBatches, db, int.MaxValue, 3, ct);
            int built;
            do
            {
                built = await ObjectUsageChainBuilder.BuildPairAsync(dataSource, OneByOne, db, 1, ct);
            }
            while (built > 0);
        }

        foreach (var db in new[] { Sales, Mix, Named })
        {
            var expected = await FingerprintAsync(connection, Backfilled, db, ct);
            Assert.NotEqual("", expected.Snapshots);
            Assert.NotEqual("", expected.Runs);
            Assert.Equal(expected, await FingerprintAsync(connection, OneByOne, db, ct));
            Assert.Equal(expected, await FingerprintAsync(connection, TinyBatches, db, ct));
        }

        // Up to date: a further call builds nothing and changes nothing.
        Assert.Equal(0, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, 10, ct));
        Assert.Equal(14L, await CountAsync(connection, $"SELECT count(*) FROM collect.object_usage_snapshots WHERE server_id = {Backfilled} AND database_name = 'Sales'", ct));
    }

    [Fact]
    public async Task TheSalesChain_StoresWhatEachStepDecided_AndOnlyTheIndexesThatMoved()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantAsync(connection, Backfilled, SalesRows(), ct);
        Assert.Equal(14, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, int.MaxValue, ct));

        async Task<object[]> Step(int day) => (await RowsAsync(
            connection,
            "SELECT reasons, counted, index_resets, compared_with, blind_from FROM collect.object_usage_snapshots WHERE server_id = $1 AND database_name = 'Sales' AND collection_time = $2",
            ct, Backfilled, Day(day)))[0];

        async Task<List<object[]>> Deltas(int day) => await RowsAsync(
            connection,
            "SELECT object_id, index_id, reads, updates FROM collect.object_usage_deltas WHERE server_id = $1 AND database_name = 'Sales' AND collection_time = $2 ORDER BY object_id, index_id",
            ct, Backfilled, Day(day));

        static long Reads(List<object[]> rows, int obj, int idx) => (long)rows.Single(r => (int)r[0] == obj && (int)r[1] == idx)[2];

        // Day 1 is the baseline: nothing counted, no delta rows.
        var first = await Step(1);
        Assert.Equal(0, (int)first[0]);
        Assert.Equal((short)0, (short)first[1]);
        Assert.Equal(DBNull.Value, first[3]);
        Assert.Empty(await Deltas(1));

        // Day 2: counted against day 1, one delta row per index that grew. The unchanged index has none.
        var second = await Step(2);
        Assert.Equal(0, (int)second[0]);
        Assert.Equal((short)1, (short)second[1]);
        Assert.Equal(Day(1), (DateTime)second[3]);
        var day2 = await Deltas(2);
        Assert.Equal(10L, Reads(day2, 100, 1));
        Assert.DoesNotContain(day2, r => (int)r[0] == 200);

        // Day 3 holds two rows for one index: the larger reading counts.
        Assert.Equal(15L, Reads(await Deltas(3), 100, 1));
        Assert.Equal(5L, Reads(await Deltas(4), 100, 1));

        // Day 5 is a restart: every index counted whole, the unchanged one included, and the day after it moves nothing for that index.
        var restart = await Step(5);
        Assert.Equal((int)ObjectUsageResetReasons.Restart, (int)restart[0]);
        Assert.Equal((short)2, (short)restart[1]);
        Assert.Equal(Day(4), (DateTime)restart[3]);
        var day5 = await Deltas(5);
        Assert.Equal(3L, Reads(day5, 100, 1));
        Assert.Equal(0L, Reads(day5, 200, 1));
        Assert.DoesNotContain(await Deltas(6), r => (int)r[0] == 200);

        // Day 8 clears every counter without a restart: counted whole, and the clear hides what the last snapshot before it had not seen.
        var clear = await Step(8);
        Assert.Equal((int)ObjectUsageResetReasons.CounterDecreased, (int)clear[0]);
        Assert.Equal((short)2, (short)clear[1]);
        Assert.Equal(Day(7), (DateTime)clear[3]);
        Assert.Equal(Day(7), (DateTime)clear[4]);

        // Day 10 clears one index: counted per index, whole for that one alone.
        var lone = await Step(10);
        Assert.Equal(0, (int)lone[0]);
        Assert.Equal((short)1, (short)lone[1]);
        Assert.Equal(1, (int)lone[2]);
        var day10 = await Deltas(10);
        Assert.Equal(1L, Reads(day10, 100, 2));
        Assert.Equal(10L, Reads(day10, 100, 1));

        // The recreated index is new to the compared snapshot on day 7: counted whole.
        Assert.Equal(Growth(7) * 2, Reads(await Deltas(7), 300, 1));

        // The unchanged index has delta rows only where a step counted everything whole: the restart and the clear.
        var dim = await RowsAsync(
            connection,
            "SELECT collection_time FROM collect.object_usage_deltas WHERE server_id = $1 AND database_name = 'Sales' AND object_id = 200 ORDER BY collection_time",
            ct, Backfilled);
        Assert.Equal(new[] { Day(5), Day(8) }, dim.Select(r => (DateTime)r[0]).ToArray());
    }

    [Fact]
    public async Task Runs_CloseWhenAnIndexIsMissing_StartAgainOnReturnOrRename_AndStayOpenWhilePresent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantAsync(connection, Backfilled, SalesRows(), ct);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, int.MaxValue, ct);

        async Task<string> Runs(int obj, int idx) => string.Join(
            ";",
            (await RowsAsync(
                connection,
                "SELECT first_time, last_time, object_name FROM collect.object_usage_runs WHERE server_id = $1 AND database_name = 'Sales' AND object_id = $2 AND index_id = $3 ORDER BY first_time",
                ct, Backfilled, obj, idx))
            .Select(r => $"{r[2]} {((DateTime)r[0]).Day}-{(r[1] is DateTime last ? last.Day.ToString() : "open")}"));

        // Dropped after day 3 and back on day 7: two runs, the second still open.
        Assert.Equal("Temp 1-3;Temp 7-open", await Runs(300, 1));

        // Gone after day 8: one closed run.
        Assert.Equal("Gone 1-8", await Runs(400, 1));

        // A rename starts a new run under the new name.
        Assert.Equal("Old 1-6;New 7-open", await Runs(500, 1));

        // Present throughout: one open run.
        Assert.Equal("Orders 1-open", await Runs(100, 1));
    }

    [Fact]
    public async Task TwoNodesInTurn_AndAStoreThatGainedTheName_KeepEachNodesOwnComparison()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantAsync(connection, Backfilled, MixRows(), ct);
        await PlantAsync(connection, Backfilled, NamedRows(), ct);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Mix, int.MaxValue, ct);
        await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Named, int.MaxValue, ct);

        async Task<string> Chain(string db) => string.Join(
            ";",
            (await RowsAsync(
                connection,
                "SELECT node_key, compared_with, counted FROM collect.object_usage_snapshots WHERE server_id = $1 AND database_name = $2 ORDER BY collection_time",
                ct, Backfilled, db))
            .Select(r => $"{r[0]}:{(r[1] is DateTime cmp ? ((cmp - Day(1)).Days + 1).ToString() : "-")}:{r[2]}"));

        // The second node is new on day 2 (counted whole, no delta rows); on day 3 the first node is compared with day 1, not day 2.
        Assert.Equal("n1:-:0;n2:-:2;n1:1:1;n2:2:1;n1:3:1;n2:4:1", await Chain(Mix));
        var mix = await RowsAsync(
            connection,
            "SELECT collection_time, reads FROM collect.object_usage_deltas WHERE server_id = $1 AND database_name = 'Mix' ORDER BY collection_time",
            ct, Backfilled);
        Assert.Equal(new[] { (Day(3), 20L), (Day(4), 200L), (Day(5), 20L), (Day(6), 200L) }, mix.Select(r => ((DateTime)r[0], (long)r[1])).ToArray());

        // The unnamed snapshots are one node, and the name that follows them takes it over: day 4 compares with day 3.
        Assert.Equal(":-:0;:1:1;:2:1;srv:3:1;srv:4:1;srv:5:1", await Chain(Named));
    }

    [Fact]
    public async Task AnOlderStepVersion_DeletesThePairsRows_AndBuildsThemAgain_AndASmallerMaxStepsBuildsOnFromTheWatermark()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantAsync(connection, Backfilled, SalesRows(), ct);
        await PlantAsync(connection, OneByOne, SalesRows(), ct);

        // Five at a time reaches the same chain as one call.
        Assert.Equal(5, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, 5, ct));
        Assert.Equal(5, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, 5, ct));
        Assert.Equal(4, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, 5, ct));
        Assert.Equal(14, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, OneByOne, Sales, int.MaxValue, ct));
        var before = await FingerprintAsync(connection, OneByOne, Sales, ct);
        Assert.Equal(before, await FingerprintAsync(connection, Backfilled, Sales, ct));

        // One row from an older version, and a leftover delta and run no rebuild would write: the next call deletes the pair's rows and builds all 14 again.
        await ExecAsync(connection, $"UPDATE collect.object_usage_snapshots SET step_version = {ObjectUsageChainVersion.Current - 1} WHERE server_id = {OneByOne} AND collection_time = '{Day(3):yyyy-MM-dd HH:mm:ss}'", ct);
        await ExecAsync(connection, $"INSERT INTO collect.object_usage_deltas VALUES ({OneByOne}, 'Sales', '{Day(2):yyyy-MM-dd HH:mm:ss}', 999, 1, 5, 5, NULL, NULL)", ct);
        await ExecAsync(connection, $"INSERT INTO collect.object_usage_runs VALUES ({OneByOne}, 'Sales', 999, 1, '{Day(2):yyyy-MM-dd HH:mm:ss}', NULL, 'dbo', 'Stale', false, false)", ct);
        Assert.Equal(14, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, OneByOne, Sales, int.MaxValue, ct));
        Assert.Equal(before, await FingerprintAsync(connection, OneByOne, Sales, ct));
        Assert.Equal(0L, await CountAsync(connection, $"SELECT count(*) FROM collect.object_usage_snapshots WHERE server_id = {OneByOne} AND step_version <> {ObjectUsageChainVersion.Current}", ct));
    }

    [Fact]
    public async Task ANewRawSnapshot_IsBuiltOntoTheChain_AndTheBuilderNeverWritesTheRawTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await PlantAsync(connection, Backfilled, SalesRows().Where(r => r.Day <= 10), ct);
        await PlantAsync(connection, OneByOne, SalesRows(), ct);
        var rawBefore = await CountAsync(connection, "SELECT count(*) FROM collect.index_object_stats", ct);
        Assert.Equal(10, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, int.MaxValue, ct));
        Assert.Equal(rawBefore, await CountAsync(connection, "SELECT count(*) FROM collect.index_object_stats", ct));

        // The collector adds days 11 to 14 later: the next call builds exactly those, onto the restored state.
        await PlantAsync(connection, Backfilled, SalesRows().Where(r => r.Day > 10), ct);
        Assert.Equal(4, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, Backfilled, Sales, int.MaxValue, ct));
        Assert.Equal(14, await ObjectUsageChainBuilder.BuildPairAsync(dataSource, OneByOne, Sales, int.MaxValue, ct));
        Assert.Equal(await FingerprintAsync(connection, OneByOne, Sales, ct), await FingerprintAsync(connection, Backfilled, Sales, ct));
    }
}

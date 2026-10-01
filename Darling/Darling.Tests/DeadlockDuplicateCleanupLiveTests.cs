/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live, end-to-end proof of the one-time cleanup that removes EXACT duplicate rows already stored in
/// <c>collect.deadlocks</c> (one Azure deadlock stored twice, once from a database's own session and once
/// from the server's telemetry). It is a DELETE path, so the pins are written against data loss first:
/// only a row whose server, <c>deadlock_time</c> and full graph text all match an earlier row goes; the
/// earliest <c>collection_time</c> stays; a NULL or empty graph, a graph that differs by one character, and
/// the same graph on another server are never touched; a second run removes nothing.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (<see cref="ScratchPostgres"/>) because it
/// builds its own hypertable and compression shape on <c>collect.deadlocks</c>, which the shared fixture
/// must never inherit from a test.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DeadlockDuplicateCleanupLiveTests
{
    private const int ServerA = -445201;
    private const int ServerB = -445202;

    private static readonly DateTime Day1 = DateTime.SpecifyKind(new DateTime(2026, 3, 10, 0, 0, 0), DateTimeKind.Unspecified);
    private static readonly DateTime Day2 = Day1.AddDays(1);

    private static readonly DateTime EventOne = Day1.AddHours(9).AddMinutes(30);
    private static readonly DateTime EventTwo = Day1.AddHours(11);
    private static readonly DateTime EventThree = Day1.AddHours(23).AddMinutes(57);
    private static readonly DateTime EventNull = Day1.AddHours(13);

    private const string GraphOne = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";
    private const string GraphOneOffByOne = "<deadlock><victim-list><victimProcess id=\"process2\"/></victim-list></deadlock>";
    private const string GraphTwo = "<deadlock><victim-list><victimProcess id=\"process7\"/></victim-list></deadlock>";
    private const string GraphThree = "<deadlock><victim-list><victimProcess id=\"process9\"/></victim-list></deadlock>";

    // Seeded ids. Kept rows are listed in KeptIds; every other id is a duplicate that must go.
    private const long D1Earliest = 101;
    private const long D1Later1 = 102;
    private const long D1Later2 = 103;
    private const long D2Only = 201;
    private const long D3BeforeMidnight = 301;
    private const long D3AfterMidnight = 302;
    private const long D1OtherServer = 401;
    private const long D1DifferentGraph = 402;
    private const long NullGraphA = 501;
    private const long NullGraphB = 502;
    private const long EmptyGraphA = 503;
    private const long EmptyGraphB = 504;
    private const long NullTimeA = 601;
    private const long NullTimeB = 602;

    private static readonly long[] RemovedIds = { D1Later1, D1Later2, D3AfterMidnight };

    private static readonly long[] KeptIds =
    {
        D1Earliest, D2Only, D3BeforeMidnight, D1OtherServer, D1DifferentGraph,
        NullGraphA, NullGraphB, EmptyGraphA, EmptyGraphB, NullTimeA, NullTimeB,
    };

    [Fact]
    public async Task OneRowSurvivesPerExactDuplicate_TheEarliestIsKept_AndTheSecondRunRemovesNothing_OnACompressedHypertable()
    {
        await RunScenarioAsync(withTimescale: true);
    }

    [Fact]
    public async Task OneRowSurvivesPerExactDuplicate_TheEarliestIsKept_AndTheSecondRunRemovesNothing_OnAStoreWithoutTimescaleDB()
    {
        await RunScenarioAsync(withTimescale: false);
    }

    private static async Task RunScenarioAsync(bool withTimescale)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed for the hypertable case) to run the live deadlock duplicate cleanup (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
            {
                await setup.OpenAsync(ct);
                await PgMigrations.MigrateAsync(setup, ct);

                if (withTimescale)
                {
                    Assert.True(await TimescaleSupport.TryEnableAsync(setup, null, ct),
                        "the dev fixture is expected to have TimescaleDB installed");
                    await ExecAsync(setup, "SELECT create_hypertable('collect.deadlocks', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
                    await ExecAsync(setup, "ALTER TABLE collect.deadlocks SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);
                    await ExecAsync(setup, "SELECT _timescaledb_functions.stop_background_workers()", ct);
                }

                // D1: three copies, the earliest first collected at 09:31, then +5 and +10 minutes.
                await InsertAsync(setup, D1Earliest, ServerA, Day1.AddHours(9).AddMinutes(31), EventOne, GraphOne, ct);
                await InsertAsync(setup, D1Later1, ServerA, Day1.AddHours(9).AddMinutes(36), EventOne, GraphOne, ct);
                await InsertAsync(setup, D1Later2, ServerA, Day1.AddHours(9).AddMinutes(41), EventOne, GraphOne, ct);

                // D2: a single copy.
                await InsertAsync(setup, D2Only, ServerA, Day1.AddHours(11).AddMinutes(1), EventTwo, GraphTwo, ct);

                // D3: stored at 23:58 and again at 00:03 the next day. The later copy is in the next day's batch;
                // its keeper is in the previous day, so the read window must reach back across midnight.
                await InsertAsync(setup, D3BeforeMidnight, ServerA, Day1.AddHours(23).AddMinutes(58), EventThree, GraphThree, ct);
                await InsertAsync(setup, D3AfterMidnight, ServerA, Day2.AddMinutes(3), EventThree, GraphThree, ct);

                // Never touched: the same event on ANOTHER server, the same time with a graph that differs by
                // one character, NULL graphs, empty graphs and NULL times.
                await InsertAsync(setup, D1OtherServer, ServerB, Day1.AddHours(9).AddMinutes(32), EventOne, GraphOne, ct);
                await InsertAsync(setup, D1DifferentGraph, ServerA, Day1.AddHours(9).AddMinutes(33), EventOne, GraphOneOffByOne, ct);
                await InsertAsync(setup, NullGraphA, ServerA, Day1.AddHours(13).AddMinutes(1), EventNull, null, ct);
                await InsertAsync(setup, NullGraphB, ServerA, Day1.AddHours(13).AddMinutes(2), EventNull, null, ct);
                await InsertAsync(setup, EmptyGraphA, ServerA, Day1.AddHours(13).AddMinutes(3), EventNull, string.Empty, ct);
                await InsertAsync(setup, EmptyGraphB, ServerA, Day1.AddHours(13).AddMinutes(4), EventNull, string.Empty, ct);
                await InsertAsync(setup, NullTimeA, ServerA, Day1.AddHours(14).AddMinutes(1), null, GraphTwo, ct);
                await InsertAsync(setup, NullTimeB, ServerA, Day1.AddHours(14).AddMinutes(2), null, GraphTwo, ct);

                if (withTimescale)
                {
                    await ExecAsync(setup, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.deadlocks') c", ct);
                    Assert.True(await ScalarLongAsync(setup,
                        "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'deadlocks' AND is_compressed", ct) >= 2,
                        "seeding failed to compress both days' chunks");
                }

                Assert.Equal(KeptIds.Length + RemovedIds.Length, await ScalarLongAsync(setup, "SELECT count(*) FROM collect.deadlocks", ct));
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var first = await DeadlockDuplicateCleanup.RunAsync(postgres, logger: null, ct);
            Assert.False(first.AlreadyDone);
            Assert.Equal(RemovedIds.Length, first.RowsRemoved);

            await using var verify = new NpgsqlConnection(scratch.ConnectionString);
            await verify.OpenAsync(ct);

            var remaining = await ReadIdsAsync(verify, ct);
            Assert.Equal(new SortedSet<long>(KeptIds), remaining);

            // The keeper of the three-copy event is the earliest collection_time, not just any one row.
            Assert.Equal(Day1.AddHours(9).AddMinutes(31),
                await ScalarTimeAsync(verify, "SELECT collection_time FROM collect.deadlocks WHERE server_id = " + ServerA + " AND deadlock_time = '" + EventOne.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "' AND deadlock_graph_xml = '" + GraphOne + "'", ct));

            Assert.Equal(CleanupVersionText(),
                await ScalarTextAsync(verify,
                    "SELECT state_value FROM collect.collector_state WHERE server_id = " + DarlingObservability.FleetServerId.ToString(CultureInfo.InvariantCulture) +
                    " AND collector_name = '" + DeadlockDuplicateCleanup.StateCollectorName + "' AND state_key = '" + DeadlockDuplicateCleanup.CleanupVersionStateKey + "'", ct));

            var second = await DeadlockDuplicateCleanup.RunAsync(postgres, logger: null, ct);
            Assert.True(second.AlreadyDone);
            Assert.Equal(0, second.RowsRemoved);
            Assert.Equal(new SortedSet<long>(KeptIds), await ReadIdsAsync(verify, ct));

            // With the marker gone the walk runs again for real, and still removes nothing.
            await ExecAsync(verify, "DELETE FROM collect.collector_state WHERE collector_name = '" + DeadlockDuplicateCleanup.StateCollectorName + "'", ct);
            var third = await DeadlockDuplicateCleanup.RunAsync(postgres, logger: null, ct);
            Assert.False(third.AlreadyDone);
            Assert.Equal(0, third.RowsRemoved);
            Assert.Equal(new SortedSet<long>(KeptIds), await ReadIdsAsync(verify, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static string CleanupVersionText() => DeadlockDuplicateCleanup.CleanupVersion.ToString(CultureInfo.InvariantCulture);

    private static async Task<SortedSet<long>> ReadIdsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = new SortedSet<long>();
        await using var command = new NpgsqlCommand("SELECT deadlock_id FROM collect.deadlocks", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, long id, int serverId, DateTime collectionTime, DateTime? deadlockTime, string? graph,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = "server-" + serverId.ToString(CultureInfo.InvariantCulture) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)deadlockTime ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)graph ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<string?> ScalarTextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    private static async Task<DateTime> ScalarTimeAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }
}

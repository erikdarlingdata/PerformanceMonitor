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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter, collection of deadlocks): a deadlock graph the filter withheld WHOLE is stored as the
/// marker text, the same for every such graph, so its identity is built from the columns the collector parsed before
/// judging it: the event time, the victim process id and the database. The one-time duplicate cleanup must read two
/// different deadlocks at the same time as two, a re-read of one as a copy, and a real copy as a copy.
///
/// <para><b>#1776 own-store</b> - mints its own scratch database (<see cref="ScratchPostgres"/>) because it runs a
/// delete path over <c>collect.deadlocks</c>, which the shared fixture must never inherit.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class StatementScrubDeadlockLiveTests
{
    private const int Server = -445301;
    private static readonly DateTime EventTime = DateTime.SpecifyKind(new DateTime(2026, 3, 10, 9, 30, 0), DateTimeKind.Unspecified);
    private const string Plain = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list></deadlock>";

    [Fact]
    public async Task WholeMarkerDeadlocks_ReReadsAreRemoved_DifferentVictimsAndTimesAreKept_AndARealCopyStillGoes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock duplicate cleanup (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
            {
                await setup.OpenAsync(ct);
                await PgMigrations.MigrateAsync(setup, ct);

                /* 1, 2 and 3 are three reads of one whole-marker deadlock; 4 is a different victim at the same time;
                   5 the same victim at another time; 6 and 7 carry no victim id, so nothing tells them apart. */
                await InsertAsync(setup, 1, EventTime.AddMinutes(1), SensitiveStatements.PlaceholderText, ct, "process1", "db1");
                await InsertAsync(setup, 2, EventTime.AddMinutes(2), SensitiveStatements.PlaceholderText, ct, "process1", "db1");
                await InsertAsync(setup, 3, EventTime.AddMinutes(3), SensitiveStatements.PlaceholderText, ct, "process1", "db1");
                await InsertAsync(setup, 4, EventTime.AddMinutes(4), SensitiveStatements.PlaceholderText, ct, "process9", "db1");
                await InsertAsync(setup, 5, EventTime.AddMinutes(5), SensitiveStatements.PlaceholderText, ct, "process1", "db1", EventTime.AddSeconds(30));
                await InsertAsync(setup, 6, EventTime.AddMinutes(6), SensitiveStatements.PlaceholderText, ct, null, "db1");
                await InsertAsync(setup, 7, EventTime.AddMinutes(7), SensitiveStatements.PlaceholderText, ct, null, "db1");
                await InsertAsync(setup, 8, EventTime.AddMinutes(8), Plain, ct, "process1", "db1");
                await InsertAsync(setup, 9, EventTime.AddMinutes(9), Plain, ct, "process9", "db2");
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var summary = await DeadlockDuplicateCleanup.RunAsync(postgres, logger: null, ct);

            Assert.Equal(3, summary.RowsRemoved);

            await using var verify = new NpgsqlConnection(scratch.ConnectionString);
            await verify.OpenAsync(ct);
            var ids = new List<long>();
            await using (var command = new NpgsqlCommand("SELECT deadlock_id FROM collect.deadlocks ORDER BY 1", verify))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    ids.Add(reader.GetInt64(0));
                }
            }

            Assert.Equal(new List<long> { 1, 4, 5, 6, 7, 8 }, ids);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /* The stored-copy read the pre-insert dedupe runs must hand back, for a whole-marker row, exactly the identity
       the collector computes for the row it is about to write. */
    [Fact]
    public async Task TheStoredIdentityRead_GivesAWholeMarkerRowTheIdentityTheCollectorComputes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live deadlock duplicate cleanup (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var setup = new NpgsqlConnection(scratch.ConnectionString);
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);

            await InsertAsync(setup, 1, EventTime.AddMinutes(1), SensitiveStatements.PlaceholderText, ct, "process1", "db1");
            await InsertAsync(setup, 2, EventTime.AddMinutes(2), SensitiveStatements.PlaceholderText, ct, "process9", null);
            await InsertAsync(setup, 3, EventTime.AddMinutes(3), SensitiveStatements.PlaceholderText, ct, null, "db1");
            await InsertAsync(setup, 4, EventTime.AddMinutes(4), Plain, ct, "process1", "db1");

            var stored = new HashSet<(DateTime Time, string Graph)>();
            await using (var command = new NpgsqlCommand(DarlingCollectorRunner.StoredDeadlockIdentitySql, setup))
            {
                command.Parameters.AddWithValue(Server);
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = new[] { EventTime } });
                command.Parameters.AddWithValue(EventTime.AddDays(-1));
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    stored.Add((reader.GetDateTime(0), reader.GetString(1)));
            }

            static DeadlocksCollector.Row Row(string graph, string? victim, string? db) =>
                new() { DeadlockTime = EventTime, GraphXml = graph, VictimProcessId = victim, DatabaseName = db };

            /* A re-read of a stored whole-marker row is dropped; another victim or database is not; a row with no
               victim id has no identity; real text still matches by graph text alone. */
            var rows = new List<DeadlocksCollector.Row>
            {
                Row(SensitiveStatements.PlaceholderText, "process1", "db1"),
                Row(SensitiveStatements.PlaceholderText, "process9", null),
                Row(SensitiveStatements.PlaceholderText, "process1", "db2"),
                Row(SensitiveStatements.PlaceholderText, "process7", "db1"),
                Row(SensitiveStatements.PlaceholderText, null, "db1"),
                Row(Plain, "anything", "elsewhere"),
            };

            var kept = DeadlocksCollector.Instance.DropAlreadyStored(rows, stored);

            Assert.Equal(new[] { 2, 3, 4 }, kept.Select(r => rows.IndexOf(r)).ToArray());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, long id, DateTime collectionTime, string graph, CancellationToken ct,
        string? victim = null, string? database = null, DateTime? deadlockTime = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, victim_process_id, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Server });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = "server-" + Server.ToString(CultureInfo.InvariantCulture) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = deadlockTime ?? EventTime });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = graph });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)victim ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }
}

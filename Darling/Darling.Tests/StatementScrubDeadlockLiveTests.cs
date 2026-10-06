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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 (statement filter, collection of deadlocks): a deadlock graph the filter withheld WHOLE is stored as the
/// marker text, the same for every such graph. The one-time duplicate cleanup must not read two different deadlocks
/// at the same time as exact copies, and must still remove a real copy.
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
    public async Task TwoWholeMarkerDeadlocksAtTheSameTime_AreBothKept_AndARealCopyStillGoes()
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

                await InsertAsync(setup, 1, EventTime.AddMinutes(1), SensitiveStatements.PlaceholderText, ct);
                await InsertAsync(setup, 2, EventTime.AddMinutes(2), SensitiveStatements.PlaceholderText, ct);
                await InsertAsync(setup, 3, EventTime.AddMinutes(3), Plain, ct);
                await InsertAsync(setup, 4, EventTime.AddMinutes(4), Plain, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var summary = await DeadlockDuplicateCleanup.RunAsync(postgres, logger: null, ct);

            Assert.Equal(1, summary.RowsRemoved);

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

            Assert.Equal(new List<long> { 1, 2, 3 }, ids);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, long id, DateTime collectionTime, string graph, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = collectionTime });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Server });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = "server-" + Server.ToString(CultureInfo.InvariantCulture) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = EventTime });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = graph });
        await command.ExecuteNonQueryAsync(ct);
    }
}

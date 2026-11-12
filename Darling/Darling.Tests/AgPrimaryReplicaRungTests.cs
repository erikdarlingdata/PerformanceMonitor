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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact here mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so none is [Collection("live-postgres")]. */

/// <summary>
/// #6159, V202: <c>collect.ag_replica_states.primary_replica</c>, the primary's name read from
/// <c>sys.dm_hadr_availability_group_states</c>, which a secondary can read. A row written through the collector's own
/// payload lands in it; a store upgraded from the prior rung gets the same column in the same place as a fresh one, and its old rows
/// read NULL; and the stored name reaches the web card and the Viewer card, by display name when the primary is a monitored
/// server, with the "not visible from this secondary" note kept for a row that carries none.
/// </summary>
public sealed class AgPrimaryReplicaRungTests
{
    private const int TestServerId = -563212;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AFreshStore_AndOneUpgradedFromThePriorRung_StoreAndReadBackThePrimary_WithTheColumnLast()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V202 rung pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Fresh: the generated table already carries the column (from the collector's payload) and the rung's ALTER no-ops. */
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            var freshColumns = await ColumnOrderAsync(connection, ct);
            Assert.Equal("primary_replica", freshColumns[^1]);
            Assert.Equal("group_id", freshColumns[^2]);
            await WriteAsync(connection, "NODE1", ct);
            Assert.Equal("NODE1", (string)(await ScalarAsync(connection, "SELECT primary_replica FROM collect.ag_replica_states WHERE server_id = " + TestServerId, ct))!);
            await ExecAsync(connection, "DELETE FROM collect.ag_replica_states WHERE server_id = " + TestServerId, ct);

            /* Upgraded: a store at V201 (the rung just below this one) has no such column, and the probe says so. A row collected then is kept through the rung. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 201, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 202", ct);
            Assert.DoesNotContain("primary_replica", await ColumnOrderAsync(connection, ct));
            Assert.Equal(201, await ProbedVersionAsync(connection, ct));
            await ExecAsync(connection,
                "INSERT INTO collect.ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc) "
                + "VALUES (" + CollectionIdGenerator.Next() + ", timestamp '2026-10-19 10:00:00', " + TestServerId + ", 'primary-test', 'AG1', 'NODE2', 'SECONDARY')", ct);

            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(freshColumns, await ColumnOrderAsync(connection, ct));

            /* The row collected before the rung reads NULL, not an empty name and not a default. */
            Assert.Equal(DBNull.Value, await ScalarAsync(connection, "SELECT primary_replica FROM collect.ag_replica_states WHERE server_id = " + TestServerId, ct));
            await WriteAsync(connection, "NODE1", ct);
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT COUNT(*) FROM collect.ag_replica_states WHERE server_id = " + TestServerId + " AND primary_replica = 'NODE1'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheStoredPrimary_ReachesTheWebCardAndTheViewerCard_ByDisplayName_AndTheNoteStaysForARowWithoutOne()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V202 reader pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* Three monitored servers: the primary (which reports its own PRIMARY row and has a display name), a secondary whose row carries
               the stored primary, and a secondary of another AG whose row predates the column. */
            var primaryId = ServerIdHelper.GetDeterministicHashCode("darling-ag-primary-p");
            var withId = ServerIdHelper.GetDeterministicHashCode("darling-ag-primary-s1");
            var withoutId = ServerIdHelper.GetDeterministicHashCode("darling-ag-primary-s2");
            await DarlingMcpTestData.RegisterServerAsync(connection, primaryId, "darling-ag-primary-p", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, withId, "darling-ag-primary-s1", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, withoutId, "darling-ag-primary-s2", ct);
            await ExecAsync(connection, "UPDATE servers SET display_name = 'Orders primary' WHERE server_id = " + primaryId, ct);

            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            await PlantReplicaAsync(connection, primaryId, "darling-ag-primary-p", "AG1", "AGNODE-P", "PRIMARY", true, "AGNODE-P", when, ct);
            await PlantReplicaAsync(connection, withId, "darling-ag-primary-s1", "AG1", "AGNODE-S1", "SECONDARY", true, "AGNODE-P", when, ct);
            await PlantReplicaAsync(connection, withoutId, "darling-ag-primary-s2", "AG2", "AGNODE-S2", "SECONDARY", true, null, when, ct);

            /* The web read (get_ag_health and /api/ag). */
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var web = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, cancellationToken: ct);
            var webWith = Assert.Single(web.AvailabilityGroups, g => g.ServerId == withId);
            Assert.Equal("Orders primary", webWith.PrimaryReplica);
            Assert.False(webWith.PrimaryNotVisible);
            Assert.Equal(HealthSeverity.Healthy, webWith.Severity);
            var webWithout = Assert.Single(web.AvailabilityGroups, g => g.ServerId == withoutId);
            Assert.Null(webWithout.PrimaryReplica);
            Assert.True(webWithout.PrimaryNotVisible);

            /* The Viewer's read, the same call path the AG tab uses. */
            var viewer = new ViewerDataService(scratch.ConnectionString);
            try
            {
                var cards = await viewer.GetAvailabilityGroupsAsync(ct);
                Assert.Equal("Primary: Orders primary", Assert.Single(cards, c => c.ServerId == withId).PrimaryDisplay);
                Assert.Equal("Primary not visible from this secondary", Assert.Single(cards, c => c.ServerId == withoutId).PrimaryDisplay);
            }
            finally
            {
                await viewer.DisposeAsync();
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task PlantReplicaAsync(
        NpgsqlConnection connection, int serverId, string serverName, string agName, string replica, string role, bool isLocal, string? primary, DateTime when, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, "
            + "operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, is_local, primary_replica) "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, 'ONLINE', 'CONNECTED', 'ONLINE', 'HEALTHY', 'SYNCHRONOUS_COMMIT', 'AUTOMATIC', $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(when));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(agName);
        command.Parameters.AddWithValue(replica);
        command.Parameters.AddWithValue(role);
        command.Parameters.AddWithValue(isLocal);
        command.Parameters.AddWithValue(primary is null ? DBNull.Value : primary);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Writes one row through the collector's own payload, so the column list the writer sends is the one under test.</summary>
    private static async Task WriteAsync(NpgsqlConnection connection, string primary, CancellationToken ct)
    {
        var definition = AgReplicaStatesCollector.Instance;
        var runTime = new DateTime(2026, 10, 19, 12, 0, 0, DateTimeKind.Unspecified);
        var row = new AgReplicaStatesCollector.Row(
            "AG1", "NODE2", "SECONDARY", "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", null, true,
            "11111111-1111-1111-1111-111111111111", primary);
        var context = new CollectorContext { ServerId = TestServerId, ServerName = "primary-test", CollectionTime = runTime, Deltas = null! };

        var writer = new PgCollectorRowWriter();
        await using var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        await importer.StartRowAsync(ct);
        writer.Value(CollectionIdGenerator.Next());
        writer.Value(runTime).Value(TestServerId).Value("primary-test");
        writer.BeginPayload();
        definition.WritePayload(row, writer, context);
        writer.EndPayload(definition.PayloadColumns.Count);
        await importer.CompleteAsync(ct);
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }

    private static async Task<string[]> ColumnOrderAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = 'collect.ag_replica_states'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum", connection)
        { CommandTimeout = 60 };
        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names.ToArray();
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}

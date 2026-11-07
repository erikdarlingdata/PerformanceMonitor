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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #7643: the store rung that holds versioned table and view definitions (V254). The seven tables exist, a history key is exactly one of
/// a server or an availability group, deleting a version deletes its child rows, the pass table is the generator's output for its
/// catalog definition, and running the rung again changes nothing.
/// </summary>
[Collection("live-postgres")]
public sealed class SchemaTrackingRungTests
{
    private static readonly string[] Tables =
    {
        "schema_objects", "schema_columns", "schema_indexes", "schema_constraints", "schema_triggers", "schema_capture_state", "schema_capture_passes",
    };

    private static readonly string[] ChildTables = { "schema_columns", "schema_indexes", "schema_constraints", "schema_triggers" };

    private const string SomeGroup = "'00000000-0000-0000-0000-000000000001'::uuid";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> OpenMigratedAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return (scratch, connection);
    }

    private static async Task<long> InsertVersionAsync(NpgsqlConnection connection, string serverId, string agGroupId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $@"INSERT INTO collect.schema_objects
    (server_id, ag_group_id, database_name, object_id, schema_name, object_name, object_type, valid_from, change_kind, detected_by, detected_on_server_id, definition_hash)
VALUES ({serverId}, {agGroupId}, 'SalesDb', 101, 'dbo', 'Orders', 'U', now() AT TIME ZONE 'UTC', 'baseline', 'baseline', 1, decode('00', 'hex'))
RETURNING version_id", connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM collect.{table}", connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task V254_CreatesTheSevenTables()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the schema tracking rung tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenMigratedAsync(ct);
        await using var _ = scratch;
        await using var __ = connection;

        foreach (var table in Tables)
        {
            await using var command = new NpgsqlCommand("SELECT to_regclass(@name) IS NOT NULL", connection);
            command.Parameters.AddWithValue("name", "collect." + table);
            Assert.True((bool)(await command.ExecuteScalarAsync(ct))!, table);
        }
    }

    [Fact]
    public async Task V254_RejectsARowWithBothOrNeitherHistoryKeys()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the schema tracking rung tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenMigratedAsync(ct);
        await using var _ = scratch;
        await using var __ = connection;

        var both = await Assert.ThrowsAsync<PostgresException>(() => InsertVersionAsync(connection, "1", SomeGroup, ct));
        Assert.Equal("23514", both.SqlState);
        var neither = await Assert.ThrowsAsync<PostgresException>(() => InsertVersionAsync(connection, "NULL", "NULL", ct));
        Assert.Equal("23514", neither.SqlState);

        /* Exactly one key is accepted, either way. */
        await InsertVersionAsync(connection, "1", "NULL", ct);
        await InsertVersionAsync(connection, "NULL", SomeGroup, ct);
    }

    [Fact]
    public async Task V254_DeletingAVersionCascadesToItsChildRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the schema tracking rung tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenMigratedAsync(ct);
        await using var _ = scratch;
        await using var __ = connection;

        var versionId = await InsertVersionAsync(connection, "1", "NULL", ct);
        await using (var children = new NpgsqlCommand(
            $@"INSERT INTO collect.schema_columns (version_id, is_current, ordinal, name, type_name, is_nullable) VALUES ({versionId}, true, 1, 'OrderId', 'int', false);
INSERT INTO collect.schema_indexes (version_id, is_current, index_id, name, type_desc, is_unique, is_primary_key) VALUES ({versionId}, true, 1, 'PK_Orders', 'CLUSTERED', true, true);
INSERT INTO collect.schema_constraints (version_id, is_current, kind, name) VALUES ({versionId}, true, 'C', 'CK_Orders_Total');
INSERT INTO collect.schema_triggers (version_id, is_current, name, is_instead_of, events) VALUES ({versionId}, true, 'TR_Orders', false, 'INSERT');", connection))
        {
            await children.ExecuteNonQueryAsync(ct);
        }

        foreach (var table in ChildTables)
        {
            Assert.Equal(1, await CountAsync(connection, table, ct));
        }

        await using (var delete = new NpgsqlCommand($"DELETE FROM collect.schema_objects WHERE version_id = {versionId}", connection))
        {
            await delete.ExecuteNonQueryAsync(ct);
        }

        foreach (var table in ChildTables)
        {
            Assert.Equal(0, await CountAsync(connection, table, ct));
        }
    }

    [Fact]
    public async Task V254_RunningTheRungAgain_ChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the schema tracking rung tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenMigratedAsync(ct);
        await using var _ = scratch;
        await using var __ = connection;

        await InsertVersionAsync(connection, "1", "NULL", ct);
        await using (var rerun = new NpgsqlCommand(PgMigrations.Scripts.Single(m => m.Version == 254).Sql, connection))
        {
            await rerun.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(1, await CountAsync(connection, "schema_objects", ct));
    }

    [Fact]
    public void V254_PassTable_IsTheGeneratorsOutput()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == 254).Sql.Replace("collect.", "", StringComparison.Ordinal);
        var generated = PgSchemaGenerator.CreateTable(SchemaObjectsCollector.Instance) + "\n\n" + PgSchemaGenerator.CreateIndex(SchemaObjectsCollector.Instance);

        static string Norm(string s) => string.Join(' ', s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        Assert.StartsWith(Norm(generated).TrimEnd(';'), Norm(rung), StringComparison.Ordinal);
    }
}

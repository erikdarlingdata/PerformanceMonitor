/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// V247 (#7639): <c>config.config_monitored_servers.xe_session_databases</c>, the per-server list of databases that get Extended Events
/// sessions on Azure SQL Database. The column exists on a fresh store and on a store upgraded from the rung below, a server registered
/// before the rung reads an empty list (every database), and the viewer probe reads the rung as applied.
/// </summary>
public sealed class XeSessionDatabasesRungTests
{
    private const int RungVersion = 247;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OnALiveStore_AFreshAndAnUpgradedStore_EndWithTheColumnAndAnEmptyList()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V247 rung's live test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* Fresh: the column, its type and its empty default. */
            await AssertColumnAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            /* One rung behind, holding a server registered before the rung. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion, ct);
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (9, 'srv-9', 'host-9')", ct);
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_attribute WHERE attrelid = to_regclass('config.config_monitored_servers') AND attname = 'xe_session_databases' AND NOT attisdropped", ct));

            await PgMigrations.MigrateAsync(connection, ct);

            await AssertColumnAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(0, await ScalarAsync(connection, "SELECT coalesce(cardinality(xe_session_databases), -1) FROM config.config_monitored_servers WHERE server_id = 9", ct));

            /* The rung runs again on a store that already has the column. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await AssertColumnAsync(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task AssertColumnAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        Assert.Equal("ARRAY|NO|'{}'::text[]", await ScalarAsync(connection,
            "SELECT concat_ws('|', data_type, is_nullable, column_default) FROM information_schema.columns "
            + "WHERE table_schema = 'config' AND table_name = 'config_monitored_servers' AND column_name = 'xe_session_databases'", ct));
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

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}

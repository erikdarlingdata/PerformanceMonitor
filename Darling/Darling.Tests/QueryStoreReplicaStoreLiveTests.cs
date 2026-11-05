/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254: the store half of the Query Store replica setting, against a live store. V213 adds
/// <c>config.config_monitored_servers.query_store_replica</c> and <c>collect.collection_log.read_source</c> to a store at V212 and
/// every row that was there reads NULL for both (an older row means "read from the server itself"), the collection-log view
/// shows the new column, and an upgraded store ends holding the 18-argument edit function and its two wrappers (R9-18).
/// </summary>
/* #1776 own-store: each live test mints its own scratch database through ScratchPostgres, so no other class's rows are read. */
[Collection("live-postgres")]
public sealed class QueryStoreReplicaStoreLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static Task<bool> ColumnExistsAsync(NpgsqlConnection connection, string table, string column, CancellationToken ct) =>
        ScalarAsync<bool>(connection,
            $"SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('{table}') AND attname = '{column}' AND NOT attisdropped)", ct);

    /// <summary>
    /// A store at V212 holding a server row and a collection-log row climbs to V213: both columns appear, the rows read NULL for
    /// them (through the table and through <c>collect.v_collection_log</c>, whose column list is frozen when it is created), the
    /// rung re-applied over itself changes nothing, and a new row takes a value in each column.
    /// </summary>
    [Fact]
    public async Task AStoreAtV212WithRows_TakesV213_AndTheOldRowsReadNullForBothColumns_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V213 rung test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.True(await ColumnExistsAsync(connection, "config.config_monitored_servers", "query_store_replica", ct));
            Assert.True(await ColumnExistsAsync(connection, "collect.collection_log", "read_source", ct));

            /* Back to V212: both columns gone, the version stamps above it removed. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 212, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 213;", ct);
            Assert.False(await ColumnExistsAsync(connection, "config.config_monitored_servers", "query_store_replica", ct));
            Assert.False(await ColumnExistsAsync(connection, "collect.collection_log", "read_source", ct));

            /* The rows an upgrade finds: a registered server and a collection-log row, written the V212 way. */
            await ExecAsync(connection,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, database, auth, username, encrypted_password, engine, port) " +
                "VALUES (6254, 'old-row', 'primary-host', 'master', 'sql', 'monitor', 'sealed-blob', 'sqlserver', 0);", ct);
            await ExecAsync(connection,
                "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) " +
                "VALUES (6254001, 6254, 'old-row', 'query_store', now() AT TIME ZONE 'utc', 20, 'SUCCESS', 3);", ct);

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version;", ct));
            Assert.True(await ColumnExistsAsync(connection, "config.config_monitored_servers", "query_store_replica", ct));
            Assert.True(await ColumnExistsAsync(connection, "collect.collection_log", "read_source", ct));
            Assert.True(await ColumnExistsAsync(connection, "collect.v_collection_log", "read_source", ct));

            Assert.True(await ScalarAsync<bool>(connection,
                "SELECT query_store_replica IS NULL FROM config.config_monitored_servers WHERE server_id = 6254;", ct));
            Assert.True(await ScalarAsync<bool>(connection, "SELECT read_source IS NULL FROM collect.collection_log WHERE log_id = 6254001;", ct));
            Assert.True(await ScalarAsync<bool>(connection, "SELECT read_source IS NULL FROM collect.v_collection_log WHERE log_id = 6254001;", ct));
            Assert.Equal("sealed-blob", await ScalarAsync<string>(connection, "SELECT encrypted_password FROM config.config_monitored_servers WHERE server_id = 6254;", ct));

            /* A new row can take a value in each column, and the view carries it. */
            await ExecAsync(connection, "UPDATE config.config_monitored_servers SET query_store_replica = 'replica-host' WHERE server_id = 6254;", ct);
            await ExecAsync(connection, "UPDATE collect.collection_log SET read_source = 'replica-host' WHERE log_id = 6254001;", ct);
            Assert.Equal("replica-host", await ScalarAsync<string>(connection, "SELECT read_source FROM collect.v_collection_log WHERE log_id = 6254001;", ct));

            /* Re-applied over its own columns (stamps rewound, columns kept): nothing changes and nothing throws. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 213;", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal("replica-host", await ScalarAsync<string>(connection, "SELECT query_store_replica FROM config.config_monitored_servers WHERE server_id = 6254;", ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ScalarAsync<int>(connection, "SELECT max(version) FROM darling_schema_version;", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}

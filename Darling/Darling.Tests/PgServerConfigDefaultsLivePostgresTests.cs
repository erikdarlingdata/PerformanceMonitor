/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7409 against a live, migrated store: <c>get_pg_server_config</c> (and the reader the web page and the desktop viewer
/// share) counts a setting as non-default only when its value differs from the default, shows the default in the same
/// base as the value, and lists the settings pinned in a file at their default after the real differences.
/// </summary>
[Collection("live-postgres")]
public sealed class PgServerConfigDefaultsLivePostgresTests
{
    private const int ServerId = -740901;
    private const string ServerName = "pg-7409-defaults";

    [Fact]
    public async Task ADefaultPinnedInAFileIsNotADifference_AndTheOctalDefaultsReadInTheBaseOfTheValue()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7409 configuration round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var snapshot = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-3);

            /* initdb writes these into the file at the compiled-in value: source is the file, the value is the default. */
            await SeedAsync(connection, ct, snapshot, "port", "5432", "configuration file", "5432", false);
            await SeedAsync(connection, ct, snapshot, "max_connections", "100", "configuration file", "100", false);
            /* A real difference, an empty default left empty, and an empty default given a command. */
            await SeedAsync(connection, ct, snapshot, "work_mem", "65536", "configuration file", "4096", false);
            await SeedAsync(connection, ct, snapshot, "archive_command", "", "configuration file", "", false);
            await SeedAsync(connection, ct, snapshot, "archive_cleanup_command", "cp %p /x", "configuration file", "", false);
            /* The octal settings: the value is octal, the stored default is decimal (384 is 0600, 448 is 0700). */
            await SeedAsync(connection, ct, snapshot, "log_file_mode", "0640", "configuration file", "384", false);
            await SeedAsync(connection, ct, snapshot, "data_directory_mode", "0700", "configuration file", "448", false);
            await SeedAsync(connection, ct, snapshot, "unix_socket_permissions", "0777", "default", "511", false);
            /* A setting nobody touched, and a pending restart row at its default. */
            await SeedAsync(connection, ct, snapshot, "wal_level", "replica", "default", "replica", false);
            await SeedAsync(connection, ct, snapshot, "shared_buffers", "16384", "default", "16384", true);

            var root = JsonDocument.Parse(await DarlingMcpPgServerStateTools.GetPgServerConfig(postgres, ServerName, 50)).RootElement;
            var settings = root.GetProperty("settings").EnumerateArray().ToList();
            var names = settings.Select(s => s.GetProperty("name").GetString()!).ToArray();

            /* The real differences: work_mem, the cp command and log_file_mode (0640 against 0600). */
            Assert.Equal(3, root.GetProperty("non_default_count").GetInt32());
            Assert.Equal(3, root.GetProperty("non_default_returned").GetInt32());

            /* pending first, then what differs, then what is pinned at the default, in name order inside each. */
            Assert.Equal(
                new[] { "shared_buffers", "archive_cleanup_command", "log_file_mode", "work_mem", "archive_command", "data_directory_mode", "max_connections", "port" },
                names);

            var row = (string name) => settings.Single(s => s.GetProperty("name").GetString() == name);
            Assert.False(row("log_file_mode").GetProperty("is_default").GetBoolean());
            Assert.Equal("0600", row("log_file_mode").GetProperty("default_value").GetString());
            Assert.True(row("data_directory_mode").GetProperty("is_default").GetBoolean());
            Assert.Equal("0700", row("data_directory_mode").GetProperty("default_value").GetString());
            Assert.True(row("port").GetProperty("is_default").GetBoolean());
            Assert.True(row("max_connections").GetProperty("is_default").GetBoolean());
            Assert.True(row("archive_command").GetProperty("is_default").GetBoolean());
            Assert.False(row("archive_cleanup_command").GetProperty("is_default").GetBoolean());
            Assert.False(row("work_mem").GetProperty("is_default").GetBoolean());

            /* include_defaults still reaches the rest, and the octal default of a default-source row reads right too. */
            var all = JsonDocument.Parse(await DarlingMcpPgServerStateTools.GetPgServerConfig(postgres, ServerName, 50, include_defaults: true)).RootElement;
            var socket = all.GetProperty("settings").EnumerateArray().Single(s => s.GetProperty("name").GetString() == "unix_socket_permissions");
            Assert.Equal("0777", socket.GetProperty("default_value").GetString());
            Assert.Equal(3, all.GetProperty("non_default_count").GetInt32());
            Assert.Equal(10, all.GetProperty("settings_returned").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime,
        string name, string setting, string source, string bootVal, bool pendingRestart)
    {
        using var insert = new NpgsqlCommand(
            """
            INSERT INTO collect.pg_server_config
                (collection_id, collection_time, server_id, server_name, name, setting, source, boot_val,
                 reset_val, sourceline, pending_restart)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $6, 0, $9)
            """, connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.Naive(collectionTime));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(name);
        insert.Parameters.AddWithValue(setting);
        insert.Parameters.AddWithValue(source);
        insert.Parameters.AddWithValue(bootVal);
        insert.Parameters.AddWithValue(pendingRestart);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM collect.pg_server_config WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one. It seeds fixed server ids and reads no wall clock. */

/// <summary><c>remove_server</c> against a name that is one server's storage name and another server's display
/// name (#6230): it answers ambiguous with both listed and removes neither, and a name that is exactly one
/// server's still resolves.</summary>
public sealed class ServerNameCollisionRemoveLiveTests
{
    [Fact]
    public async Task RemoveServer_ANameThatIsOneServersStorageNameAndAnothersDisplayName_IsAmbiguous_AndRemovesNeither()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-name test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        await using var source = NpgsqlDataSource.Create(connectionString);
        /* Server 7201's storage name is the text server 7202 shows as its display name. */
        await ExecAsync(source, @"INSERT INTO config_monitored_servers (server_id, name, host) VALUES
            (7201, 'name-collide-a', 'name-collide.example.test'),
            (7202, 'name-collide.example.test', 'name-collide-b.example.test')", ct);

        using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(source, "name-collide.example.test")))
        {
            Assert.Equal("ambiguous", doc.RootElement.GetProperty("status").GetString());
            var listed = doc.RootElement.GetProperty("candidates").EnumerateArray()
                .Select(c => c.GetProperty("server").GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToArray();
            Assert.Equal(new[] { "name-collide-b.example.test", "name-collide.example.test" }, listed);
        }

        Assert.Equal(2L, await CountAsync(source, "SELECT count(*) FROM config_monitored_servers WHERE server_id IN (7201, 7202)", ct));

        /* An exact single match still resolves: the other server's own storage name removes only that server. */
        using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(source, "name-collide-b.example.test")))
        {
            Assert.Equal("removed", doc.RootElement.GetProperty("status").GetString());
        }

        Assert.Equal(0L, await CountAsync(source, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 7202", ct));
        Assert.Equal(1L, await CountAsync(source, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 7201", ct));
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource source, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }
}

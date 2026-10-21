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

    /// <summary>A READ tool given the same clashing name resolves the server whose storage name it is (#6230, #5633): only a
    /// write refuses, so a saved scope and a caller that was handed the stored name keep reading that server.</summary>
    [Theory]
    [InlineData("name-collide.example.test")]
    [InlineData("  name-collide.example.test  ")]
    [InlineData("NAME-COLLIDE.EXAMPLE.TEST")]
    public async Task ReadTool_ANameThatIsOneServersStorageNameAndAnothersDisplayName_ResolvesTheStoredName(string typed)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-name test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var when = DateTime.SpecifyKind(new DateTime(2026, 10, 20, 12, 0, 0), DateTimeKind.Unspecified);
        await using (var seed = new NpgsqlConnection(connectionString))
        {
            await seed.OpenAsync(ct);
            await PgMigrations.MigrateAsync(seed, null, ct);

            /* Server 7301's storage name is the text server 7302 shows as its display name; each has its own edition so the answer says which one it read. */
            await DarlingMcpTestData.ExecAsync(seed, ct,
                @"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, sql_engine_edition, created_date, modified_date)
VALUES (7301, 'name-collide.example.test', 'Stored name server', TRUE, 15, 3, $1, $1),
       (7302, 'name-collide-b.example.test', 'name-collide.example.test', TRUE, 15, 3, $1, $1)", when);
            foreach (var (id, key, edition) in new[] { (7301, "name-collide.example.test", "Enterprise Edition (64-bit)"), (7302, "name-collide-b.example.test", "Standard Edition (64-bit)") })
            {
                await DarlingMcpTestData.ExecAsync(seed, ct,
                    @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
    cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, is_hadr_enabled, is_clustered, service_objective, vcore_count)
VALUES ($1,$2,$3,$4,$5,'15.0.2000.5','RTM',3,2,64,933888,2,16,false,false,NULL,NULL)",
                    PerformanceMonitor.Collectors.CollectionIdGenerator.Next(), when, id, key, edition);
            }
        }

        await using var source = NpgsqlDataSource.Create(connectionString);
        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetServerProperties(source, typed, ct));
        Assert.Equal("Stored name server", doc.RootElement.GetProperty("server").GetString());
        Assert.Contains("Enterprise", doc.RootElement.GetProperty("edition").GetString(), StringComparison.Ordinal);
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

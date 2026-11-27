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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9087: <c>list_servers</c> lists every configured server. A server whose collection was turned off (<c>disable_server</c> sets
/// <c>config_monitored_servers.is_enabled</c> false) is a row that reads Disabled, with the reason, and no stored connect failure.
/// The web read route keeps listing only the servers that are collecting. A store with nothing configured answers <c>empty</c>.
/// #1776 own-store: each test mints its own database through ScratchPostgres, so it cannot race the shared one.
/// </summary>
public sealed class ListServersDisabledServerLiveTests
{
    private static readonly DateTime DownSince = new(2026, 11, 4, 11, 56, 0, DateTimeKind.Unspecified);

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, int id, string name, bool enabled)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, $3) ON CONFLICT (server_id) DO UPDATE SET is_enabled = $3", id, name, enabled);
    }

    [Fact]
    public async Task A_server_with_collection_off_is_listed_Disabled_and_the_web_route_leaves_it_out()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live list_servers test.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: a clone of a migrated template, so the two servers seeded here are the only rows. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await SeedAsync(connection, ct, 91, "ls-on", enabled: true);
        await SeedAsync(connection, ct, 92, "ls-off", enabled: false);
        /* A connect failure stored before the server was turned off must not reach its row. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET connect_down_since = $2, last_connect_error = 'old failure' WHERE server_id = $1", 92, DownSince);

        using (var doc = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(postgres, ct)))
        {
            Assert.Equal(2, doc.RootElement.GetProperty("server_count").GetInt32());
            var rows = doc.RootElement.GetProperty("servers").EnumerateArray().ToDictionary(r => r.GetProperty("server_name").GetString()!);
            Assert.NotEqual("Disabled", rows["ls-on"].GetProperty("status").GetString());
            var off = rows["ls-off"];
            Assert.Equal("Disabled", off.GetProperty("status").GetString());
            Assert.Equal(JsonValueKind.String, off.GetProperty("reason").ValueKind);
            Assert.Equal(JsonValueKind.Null, off.GetProperty("connect_down_since").ValueKind);
            Assert.Equal(JsonValueKind.Null, off.GetProperty("last_connect_error").ValueKind);
        }

        using (var web = JsonDocument.Parse(await DarlingMcpDataTools.ListServersCore(postgres, includeDisabled: false, ct)))
        {
            Assert.Equal(1, web.RootElement.GetProperty("server_count").GetInt32());
            Assert.Equal("ls-on", Assert.Single(web.RootElement.GetProperty("servers").EnumerateArray()).GetProperty("server_name").GetString());
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE config.config_monitored_servers SET is_enabled = FALSE");
        using (var allOff = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(postgres, ct)))
        {
            Assert.False(allOff.RootElement.TryGetProperty("message", out _));
            Assert.Equal(2, allOff.RootElement.GetProperty("server_count").GetInt32());
        }
    }

    [Fact]
    public async Task A_store_with_nothing_configured_answers_empty()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live list_servers test.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(postgres, ct));
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }
}

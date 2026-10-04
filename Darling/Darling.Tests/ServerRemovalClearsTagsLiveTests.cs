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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one. It seeds fixed server ids and reads no wall clock. */

/// <summary>Removing a server also removes its fleet-tag assignments (#5085). <c>server_id</c> is a fixed hash of
/// the connection, so assignments left behind would come back when the same server is added again and would put
/// it back inside tag-scoped alert rules. Both remove paths are driven through their product entry points: the
/// MCP <c>remove_server</c> tool and the Viewer's <c>DeleteMonitoredServerAsync</c>.</summary>
public sealed class ServerRemovalClearsTagsLiveTests
{
    private const int KeptId = 7101;
    private const int RemovedId = 7102;

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source, string ConnectionString)?> SeedAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-removal tag test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var source = NpgsqlDataSource.Create(connectionString);
        await ExecAsync(source, $@"INSERT INTO config_monitored_servers (server_id, name, host) VALUES
            ({KeptId}, 'tagkeep-host', 'tagkeep-host'), ({RemovedId}, 'tagdrop-host', 'tagdrop-host')");
        var store = new ServerTagStore(source, 30);
        var ct2 = TestContext.Current.CancellationToken;
        var prod = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Prod", null, null, ct2)).Tag!;
        var east = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("East", null, null, ct2)).Tag!;
        foreach (var tag in new[] { prod, east })
        {
            await store.AssignAsync(tag.Id, [KeptId, RemovedId], ct2);
        }

        return (scratch, source, connectionString);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<int> MapRowsAsync(NpgsqlDataSource source, int serverId)
    {
        await using var command = source.CreateCommand($"SELECT count(*) FROM server_tag_map WHERE server_id = {serverId}");
        return Convert.ToInt32(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TheMcpRemoveServerTool_DropsTheServersTagAssignments_AndNoOtherServers()
    {
        var seeded = await SeedAsync();
        var (scratch, source, _) = seeded!.Value;
        await using var _s = scratch;
        await using var _d = source;
        Assert.Equal(2, await MapRowsAsync(source, RemovedId));

        using var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(source, "tagdrop-host"));
        Assert.Equal("removed", doc.RootElement.GetProperty("status").GetString());

        Assert.Equal(0, await MapRowsAsync(source, RemovedId));
        Assert.Equal(2, await MapRowsAsync(source, KeptId));
    }

    [Fact]
    public async Task TheViewersRemove_DropsTheServersTagAssignments_AndNoOtherServers()
    {
        var seeded = await SeedAsync();
        var (scratch, source, connectionString) = seeded!.Value;
        await using var _s = scratch;
        await using var _d = source;
        Assert.Equal(2, await MapRowsAsync(source, RemovedId));

        await using (var viewer = new ViewerDataService(connectionString))
        {
            await viewer.DeleteMonitoredServerAsync(RemovedId, TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, await MapRowsAsync(source, RemovedId));
        Assert.Equal(2, await MapRowsAsync(source, KeptId));
        Assert.Equal(0L, Convert.ToInt64(await (source.CreateCommand($"SELECT count(*) FROM config_monitored_servers WHERE server_id = {RemovedId}")).ExecuteScalarAsync(TestContext.Current.CancellationToken)));
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #6277: a store with no servers registered (a fresh install) answers <c>GET /api/read/list_servers</c> with 200 and
/// the <c>empty</c> status envelope, not a 400. Alert History, FinOps and Job History read this call on load, and a 400
/// put an error in the browser console on a store that was simply new.
/// </summary>
public sealed class ListServersEmptyRegistryRouteLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to run the live list_servers route test.";

    [Fact]
    public async Task EmptyStore_ThroughTheReadRoute_AnswersOkWithTheEmptyEnvelope()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var (status, body) = await FinOpsWebReadParityLiveTests.GetAsync(postgres, "/api/read/list_servers?", ct);

        Assert.Equal(StatusCodes.Status200OK, status);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.StartsWith("No servers are registered yet.", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

        /* The tool says the same thing, so an MCP client reads the identical true negative. */
        using var viaTool = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(postgres, ct));
        Assert.Equal("empty", viaTool.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task StoreWithAServer_ThroughTheReadRoute_StillListsIt()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FinOpsHighImpactReaderLiveTests.SeedAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        /* #6454: list_servers lists a registered server only while its definition exists. */
        await using (var define = postgres.CreateCommand(
            "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) SELECT server_id, server_name, server_name, TRUE FROM servers WHERE is_enabled ON CONFLICT (server_id) DO NOTHING"))
        {
            await define.ExecuteNonQueryAsync(ct);
        }

        var (status, body) = await FinOpsWebReadParityLiveTests.GetAsync(postgres, "/api/read/list_servers?", ct);

        Assert.Equal(StatusCodes.Status200OK, status);
        using var doc = JsonDocument.Parse(body);
        Assert.Equal(1, doc.RootElement.GetProperty("server_count").GetInt32());
        Assert.False(doc.RootElement.TryGetProperty("status", out _));
    }
}

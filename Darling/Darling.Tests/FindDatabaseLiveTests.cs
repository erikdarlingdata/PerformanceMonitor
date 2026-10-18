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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #6074 part 2 (gated, DARLING_TEST_PG): <c>find_database</c> names each monitored server whose NEWEST database list holds a database,
/// exact and case-insensitive, with the newest list's time and whether the server is still monitored. A database only an OLDER list
/// names (dropped since) is not found; a server with no recent list is not searched.
/// </summary>
public sealed class FindDatabaseLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the find_database live test.";

    private static async Task SnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, TimeSpan age, System.Threading.CancellationToken ct, params string[] databases)
    {
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow - age);
        foreach (var database in databases)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,'ONLINE',FALSE)",
                CollectionIdGenerator.Next(), at, serverId, serverName, database);
        }
    }

    [Fact]
    public async Task FindDatabase_NamesEachServerWhoseNewestListHoldsIt_AndOnlyThose()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (a, b, c, d) = (ServerIdHelper.GetDeterministicHashCode("fd-a"), ServerIdHelper.GetDeterministicHashCode("fd-b"),
            ServerIdHelper.GetDeterministicHashCode("fd-c"), ServerIdHelper.GetDeterministicHashCode("fd-d"));
        foreach (var (id, name) in new[] { (a, "fd-a"), (b, "fd-b"), (c, "fd-c"), (d, "fd-d") })
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        /* a: holds Tenant42 in its newest list; b: only an OLDER list held it (dropped since); c: holds it, but the server is disabled;
           d: its only list is older than the look-back, so it is not searched. */
        await SnapshotAsync(connection, a, "fd-a", TimeSpan.FromMinutes(10), ct, "master", "TENANT42");
        await SnapshotAsync(connection, b, "fd-b", TimeSpan.FromHours(3), ct, "Tenant42", "other");
        await SnapshotAsync(connection, b, "fd-b", TimeSpan.FromMinutes(20), ct, "other");
        await SnapshotAsync(connection, c, "fd-c", TimeSpan.FromHours(9), ct, "tenant42");
        await SnapshotAsync(connection, d, "fd-d", TimeSpan.FromDays(10), ct, "Tenant42");
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET is_enabled = FALSE WHERE server_id = $1", c);

        var found = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, " tenant42 ", ct)).RootElement;
        var matches = found.GetProperty("matches").EnumerateArray().ToList();
        Assert.Equal(new[] { "fd-a", "fd-c" }, matches.Select(m => m.GetProperty("server_name").GetString()).ToArray());
        Assert.Equal("TENANT42", matches[0].GetProperty("database_name").GetString());
        Assert.True(matches[0].GetProperty("is_monitored").GetBoolean());
        Assert.True(matches[0].GetProperty("snapshot_current").GetBoolean());
        Assert.False(matches[1].GetProperty("is_monitored").GetBoolean());
        Assert.False(matches[1].GetProperty("snapshot_current").GetBoolean());
        Assert.Equal(3, found.GetProperty("servers_searched").GetInt32());

        var none = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, "NoSuchDb", ct)).RootElement;
        Assert.Equal("empty", none.GetProperty("status").GetString());
        Assert.Contains("NoSuchDb", none.GetProperty("message").GetString(), StringComparison.Ordinal);

        var blank = JsonDocument.Parse(await DarlingMcpFindDatabaseTools.FindDatabase(dataSource, "  ", ct)).RootElement;
        Assert.Equal("invalid", blank.GetProperty("status").GetString());
    }
}

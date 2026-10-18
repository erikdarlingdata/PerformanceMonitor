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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #6074 part 4 (gated, DARLING_TEST_PG): <c>get_collection_health</c> with <c>server_name</c> "*" and a <c>collector_name</c> answers one row per
/// enabled server with the newest success and newest failure of that collector in the window, a server with no run says so, and a
/// disabled server is not listed.
/// </summary>
public sealed class CollectionHealthFleetLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the fleet collection-health live test.";

    private static Task RunAsync(NpgsqlConnection connection, int serverId, string serverName, string collector, string status, TimeSpan age, string? message, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected) VALUES ($1,$2,$3,$4,$5,0,$6,$7,0)",
            CollectionIdGenerator.Next(), serverId, serverName, collector, DarlingMcpTestData.Naive(DateTime.UtcNow - age), status, message);

    [Fact]
    public async Task TheFleetForm_GivesEachServersNewestSuccessAndFailure_AndNamesTheServerWithNoRun()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var ids = new[] { "fl-a", "fl-b", "fl-c", "fl-off" }.ToDictionary(n => n, ServerIdHelper.GetDeterministicHashCode);
        foreach (var (name, id) in ids)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET is_enabled = FALSE WHERE server_id = $1", ids["fl-off"]);

        await RunAsync(connection, ids["fl-a"], "fl-a", "query_store", "SUCCESS", TimeSpan.FromHours(3), null, ct);
        await RunAsync(connection, ids["fl-a"], "fl-a", "query_store", "ERROR", TimeSpan.FromHours(1), "denied", ct);
        await RunAsync(connection, ids["fl-b"], "fl-b", "query_store", "SUCCESS", TimeSpan.FromHours(2), null, ct);
        await RunAsync(connection, ids["fl-b"], "fl-b", "query_store", "SKIPPED", TimeSpan.FromMinutes(5), null, ct);
        await RunAsync(connection, ids["fl-c"], "fl-c", "wait_stats", "SUCCESS", TimeSpan.FromHours(1), null, ct);
        await RunAsync(connection, ids["fl-off"], "fl-off", "query_store", "SUCCESS", TimeSpan.FromHours(1), null, ct);

        var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: "query_store", cancellationToken: ct)).RootElement;
        var servers = root.GetProperty("servers").EnumerateArray().ToDictionary(s => s.GetProperty("server_name").GetString()!);
        Assert.Equal(new[] { "fl-a", "fl-b", "fl-c" }, servers.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.Equal("failing_since_last_success", servers["fl-a"].GetProperty("outcome").GetString());
        Assert.Equal("denied", servers["fl-a"].GetProperty("last_failure_message").GetString());
        Assert.Equal("succeeding", servers["fl-b"].GetProperty("outcome").GetString());
        Assert.Equal(JsonValueKind.Null, servers["fl-b"].GetProperty("last_failure_at").ValueKind);
        Assert.Equal("no_run_in_window", servers["fl-c"].GetProperty("outcome").GetString());

        var refused = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", cancellationToken: ct)).RootElement;
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        var unknown = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: "no_such_collector", cancellationToken: ct)).RootElement;
        Assert.Equal("invalid", unknown.GetProperty("status").GetString());
    }
}

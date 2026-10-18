/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6074 part 4, Lite half: <c>get_collection_health</c> with <c>server_name</c> "*" and a <c>collector_name</c> answers one row per enabled
/// server with the newest success and newest failure of that collector in the window, and a server with no run says so.
/// </summary>
public sealed class CollectionHealthFleetTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private long _nextId = 1;

    public CollectionHealthFleetTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    private async Task RunAsync(ServerConnection server, string collector, string status, TimeSpan age, string? message = null)
    {
        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
                            VALUES ($1, $2, $3, $4, $5, 0, $6, $7, 0, 0, 0)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(_nextId++); P(serverId); P(server.ServerName); P(collector); P(DateTime.UtcNow - age); P(status); P(message);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TheFleetForm_GivesEachServersNewestSuccessAndFailure_AndNamesTheServerWithNoRun()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var a = new ServerConnection { ServerName = "fl-a", DisplayName = "Alpha" };
        var b = new ServerConnection { ServerName = "fl-b", DisplayName = "Beta" };
        var c = new ServerConnection { ServerName = "fl-c", DisplayName = "Gamma" };
        serverManager.AddServer(a);
        serverManager.AddServer(b);
        serverManager.AddServer(c);

        /* a: success then a NEWER failure; b: success only (plus a SKIPPED run, which is no failure); c: no run of this collector. */
        await RunAsync(a, "query_store", "SUCCESS", TimeSpan.FromHours(3));
        await RunAsync(a, "query_store", "ERROR", TimeSpan.FromHours(1), "denied");
        await RunAsync(b, "query_store", "SUCCESS", TimeSpan.FromHours(2));
        await RunAsync(b, "query_store", "SKIPPED", TimeSpan.FromMinutes(5));
        await RunAsync(c, "wait_stats", "SUCCESS", TimeSpan.FromHours(1));

        var service = new LocalDataService(initializer);
        var root = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*", collector_name: "query_store")).RootElement;
        var servers = root.GetProperty("servers").EnumerateArray().ToDictionary(s => s.GetProperty("server_name").GetString()!);
        Assert.Equal("fleet", root.GetProperty("scope").GetString());
        Assert.Equal("failing_since_last_success", servers["fl-a"].GetProperty("outcome").GetString());
        Assert.Equal("ERROR", servers["fl-a"].GetProperty("last_failure_status").GetString());
        Assert.Equal("denied", servers["fl-a"].GetProperty("last_failure_message").GetString());
        Assert.NotEqual(JsonValueKind.Null, servers["fl-a"].GetProperty("last_success_at").ValueKind);
        Assert.Equal("succeeding", servers["fl-b"].GetProperty("outcome").GetString());
        Assert.Equal(JsonValueKind.Null, servers["fl-b"].GetProperty("last_failure_at").ValueKind);
        Assert.Equal("no_run_in_window", servers["fl-c"].GetProperty("outcome").GetString());
        Assert.Equal(2, root.GetProperty("servers_with_a_run").GetInt32());
        Assert.Equal(1, root.GetProperty("servers_with_no_run").GetInt32());

        var refused = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*")).RootElement;
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        var unknown = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*", collector_name: "no_such_collector")).RootElement;
        Assert.Equal("invalid", unknown.GetProperty("status").GetString());
    }
}

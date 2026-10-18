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
/// #6074 part 2, Lite half: <c>find_database</c> names each monitored server whose NEWEST <c>database_states</c> list holds a database,
/// exact and case-insensitive, with the newest list's time and whether the server is still enabled. A database only an older list names
/// (dropped since) is not found.
/// </summary>
public sealed class FindDatabaseTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private long _nextId = 1;

    public FindDatabaseTests()
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

    private async Task SnapshotAsync(ServerConnection server, TimeSpan age, params string[] databases)
    {
        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var at = DateTime.UtcNow - age;
        foreach (var database in databases)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ({_nextId++}, TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', {serverId}, '{server.ServerName}', '{database}', 5, 'ONLINE', false)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task FindDatabase_NamesEachServerWhoseNewestListHoldsIt_AndOnlyThose()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var a = new ServerConnection { ServerName = "fd-a", DisplayName = "Alpha" };
        var b = new ServerConnection { ServerName = "fd-b", DisplayName = "Beta" };
        var c = new ServerConnection { ServerName = "fd-c", DisplayName = "Gamma", IsEnabled = false };
        serverManager.AddServer(a);
        serverManager.AddServer(b);
        serverManager.AddServer(c);

        /* a holds it in its newest list; b only an OLDER list held it (dropped since); c holds it but is disabled. */
        await SnapshotAsync(a, TimeSpan.FromMinutes(10), "master", "TENANT42");
        await SnapshotAsync(b, TimeSpan.FromHours(3), "Tenant42", "other");
        await SnapshotAsync(b, TimeSpan.FromMinutes(20), "other");
        await SnapshotAsync(c, TimeSpan.FromHours(9), "tenant42");

        var service = new LocalDataService(initializer);
        var found = JsonDocument.Parse(await McpDiscoveryTools.FindDatabase(serverManager, service, " tenant42 ")).RootElement;
        var matches = found.GetProperty("matches").EnumerateArray().ToList();
        Assert.Equal(new[] { "fd-a", "fd-c" }, matches.Select(m => m.GetProperty("server_name").GetString()).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        var alpha = matches.Single(m => m.GetProperty("server_name").GetString() == "fd-a");
        Assert.Equal("Alpha", alpha.GetProperty("display_name").GetString());
        Assert.Equal("TENANT42", alpha.GetProperty("database_name").GetString());
        Assert.True(alpha.GetProperty("is_monitored").GetBoolean());
        Assert.True(alpha.GetProperty("snapshot_current").GetBoolean());
        var gamma = matches.Single(m => m.GetProperty("server_name").GetString() == "fd-c");
        Assert.False(gamma.GetProperty("is_monitored").GetBoolean());
        Assert.False(gamma.GetProperty("snapshot_current").GetBoolean());

        var none = JsonDocument.Parse(await McpDiscoveryTools.FindDatabase(serverManager, service, "NoSuchDb")).RootElement;
        Assert.Equal("empty", none.GetProperty("status").GetString());
        Assert.Contains("NoSuchDb", none.GetProperty("message").GetString(), StringComparison.Ordinal);

        var blank = JsonDocument.Parse(await McpDiscoveryTools.FindDatabase(serverManager, service, "  ")).RootElement;
        Assert.Equal("invalid", blank.GetProperty("status").GetString());
    }
}

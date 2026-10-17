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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Database;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The roll-up's top-level status rule (#5992, the twin of #5983): "unavailable" only when every section is, else "empty" when the
/// served sections are empty, else data. A section whose table is missing is unavailable, never a tool error. Own class so the table
/// this drops is not shared with another test's database.
/// </summary>
public sealed class LiteQueryTagRollupStatusTests : IDisposable
{
    private const string ServerName = "StatusServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;

    public LiteQueryTagRollupStatusTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryTagRollupStatus_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "status.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_tempDir);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>The deadlock source gone: its view first, then the table behind it.</summary>
    private async Task DropDeadlocksAsync()
    {
        await ExecAsync("DROP VIEW IF EXISTS v_deadlocks");
        await ExecAsync("DROP TABLE IF EXISTS deadlocks");
    }

    [Fact]
    public async Task OneSectionUnavailable_OthersWithData_IsDataWithAMessageNamingTheUnavailableSection()
    {
        await ExecAsync(
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', 'db1', '0x00000000000000AA', 1, 5000, 5000, 10, 1)",
            1L, DateTime.UtcNow.AddMinutes(-30), _serverId);
        await ExecAsync("ALTER TABLE deadlocks RENAME TO deadlocks_held");

        var json = await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, ServerName, 24);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.False(root.TryGetProperty("status", out _), "a mixed answer with data carries no top-level status");
        Assert.Contains("deadlocks is unavailable", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        var sections = root.GetProperty("sections").EnumerateArray().ToDictionary(e => e.GetProperty("section").GetString()!);
        Assert.Equal("unavailable", sections["deadlocks"].GetProperty("status").GetString());
        Assert.NotEqual("unavailable", sections["plan_cache"].GetProperty("status").GetString());
    }

    [Fact]
    public async Task OneSectionUnavailable_OthersEmpty_IsEmptyAndStillCarriesTheSections()
    {
        await ExecAsync("ALTER TABLE deadlocks RENAME TO deadlocks_held");

        var json = await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, ServerName, 24);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(6, root.GetProperty("sections").GetArrayLength());
        Assert.Contains("deadlocks is unavailable", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }
}

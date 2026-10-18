/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6074 part 3, Lite half: a Query Store read that finds nothing says so, and names <c>get_query_store_health</c>, when the
/// database's newest health row is not READ_WRITE. A READ_WRITE database keeps its answer exactly as it was.
/// </summary>
public sealed class QueryStoreReadOnlyHintTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;

    public QueryStoreReadOnlyHintTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _tempDir = Path.Combine(Path.GetTempPath(), "QsRoHint_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AnEmptyQueryStoreTop_OnAReadOnlyDatabase_NamesGetQueryStoreHealth_AndAReadWriteOneDoesNot()
    {
        await SeedHealthAsync("RoDb", "READ_ONLY");
        await SeedHealthAsync("RwDb", "READ_WRITE");

        var readOnly = JsonDocument.Parse(await McpQueryTools.GetQueryStoreTop(_dataService, _serverManager, database_name: "RoDb")).RootElement;
        var message = readOnly.GetProperty("message").GetString()!;
        Assert.Contains("get_query_store_health", message, StringComparison.Ordinal);
        Assert.Contains("RoDb", message, StringComparison.Ordinal);
        Assert.Contains("READ_ONLY", message, StringComparison.Ordinal);

        var readWrite = JsonDocument.Parse(await McpQueryTools.GetQueryStoreTop(_dataService, _serverManager, database_name: "RwDb")).RootElement;
        Assert.DoesNotContain("get_query_store_health", readWrite.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    private async Task SeedHealthAsync(string databaseName, string actualState)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_health
            (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state,
             readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode,
             stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 'READ_WRITE', 0, 0, 1000, 'AUTO', 30, 200, 60)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddMinutes(-5) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = actualState });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}

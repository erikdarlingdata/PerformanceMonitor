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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6226, Lite's twin of Darling's live test: a ranking over a window in which the collector ran successfully and stored no row
/// (nothing executed since its watermark) is <c>empty</c> with the run count; a window with no successful run stays
/// <c>unavailable</c>; #6127's snapshots-without-a-delta answer is unchanged.
/// </summary>
public sealed class McpTopReadsCollectorRanEmptyTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = 1;

    public McpTopReadsCollectorRanEmptyTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpRanEmpty_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task QueryStats_RanSuccessfullyAndStoredNothing_IsEmptyWithTheRunCount()
    {
        var now = DateTime.UtcNow;
        for (var minutes = 5; minutes <= 55; minutes += 10)
        {
            await SeedRunAsync("query_stats", now.AddMinutes(-minutes), "SUCCESS", 0);
        }

        var root = await TopQueriesAsync(hoursBack: 1);

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal("query_stats ran 6 time(s) in the last 1 hour(s) and found no executed statements to store. This is not a collection gap: collection health showing successful runs that stored no rows is consistent with it.",
            root.GetProperty("message").GetString());
        Assert.Equal(6, root.GetProperty("hints").GetProperty("successful_runs_in_window").GetInt64());
    }

    [Fact]
    public async Task QueryStats_WithNoSuccessfulRunInTheWindow_StaysUnavailable()
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync("query_stats", now.AddMinutes(-10), "ERROR", 0);
        await SeedRunAsync("query_stats", now.AddHours(-5), "SUCCESS", 0);

        var root = await TopQueriesAsync(hoursBack: 1);

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.Equal("No query stats available for the specified time range.", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task QueryStats_SnapshotsWithoutADelta_KeepTheSnapshotAnswerEvenWhenTheCollectorRan()
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync("query_stats", now.AddMinutes(-20), "SUCCESS", 1);
        await SeedSnapshotAsync(now.AddMinutes(-20), "0xFIRST");

        var root = await TopQueriesAsync(hoursBack: 1);

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("none carries a measured delta", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, root.GetProperty("hints").GetProperty("snapshots_in_window").GetInt64());
    }

    [Fact]
    public async Task QueryStats_ADatabaseFilter_ClaimsOnlyThatDatabase()
    {
        await SeedRunAsync("query_stats", DateTime.UtcNow.AddMinutes(-10), "SUCCESS", 0);

        var root = await TopQueriesAsync(hoursBack: 1, database: "TestDb");

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("stored no executed statements for database 'TestDb'", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task QueryStoreTop_RanSuccessfullyAndStoredNothing_IsEmptyWithTheRunCount_AndNoRunIsUnavailable()
    {
        var now = DateTime.UtcNow;
        await SeedRunAsync("query_store", now.AddHours(-5), "SUCCESS", 0);

        var unavailable = JsonDocument.Parse(await McpQueryTools.GetQueryStoreTop(
            new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 1)).RootElement;
        Assert.Equal("unavailable", unavailable.GetProperty("status").GetString());

        await SeedRunAsync("query_store", now.AddMinutes(-30), "SUCCESS", 0);
        await SeedRunAsync("query_store", now.AddMinutes(-15), "SUCCESS", 0);

        var empty = JsonDocument.Parse(await McpQueryTools.GetQueryStoreTop(
            new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 1)).RootElement;
        Assert.Equal("empty", empty.GetProperty("status").GetString());
        Assert.StartsWith("query_store ran 2 time(s) in the last 1 hour(s) and found no Query Store intervals to store.", empty.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal(2, empty.GetProperty("hints").GetProperty("successful_runs_in_window").GetInt64());
    }

    private async Task<JsonElement> TopQueriesAsync(int hoursBack, string? database = null) =>
        JsonDocument.Parse(await McpQueryTools.GetTopQueriesByCpu(
            new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: hoursBack, database_name: database)).RootElement;

    private async Task SeedRunAsync(string collector, DateTime atUtc, string status, long rows)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(atUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = rows });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedSnapshotAsync(DateTime collectedUtc, string queryHash)
    {
        var collected = DateTime.SpecifyKind(collectedUtc, DateTimeKind.Unspecified);
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 0, 0, $9)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xH" + queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 1" });
        await cmd.ExecuteNonQueryAsync();
    }
}

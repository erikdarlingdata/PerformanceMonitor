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
/// #6127, Lite's twin of Darling's live test: <c>get_top_queries_by_cpu</c> answered <c>unavailable</c> ("No query stats available
/// for the specified time range.") for a window that stored snapshots with no measured delta (first sightings and unmoved
/// repeats), which sent callers to collection health, where the rows show. The read must say what it found and which window
/// carries a delta.
/// </summary>
public sealed class McpTopQueriesSnapshotOnlyWindowTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = 1;

    public McpTopQueriesSnapshotOnlyWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpSnapshotOnly_" + Guid.NewGuid().ToString("N")[..8]);
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
    public async Task SnapshotsWithoutADelta_AreNotCalledNoData_AndNameTheWindowThatAnswers()
    {
        var now = DateTime.UtcNow;
        var measuredAt = DateTime.SpecifyKind(now.AddHours(-10), DateTimeKind.Unspecified);

        await SeedAsync(now.AddHours(-3), "0xFIRST1", 0, 0, 0);
        await SeedAsync(now.AddHours(-2), "0xFIRST2", 0, 0, 0);
        await SeedAsync(now.AddMinutes(-70), "0xFIRST2", 0, 0, 0);
        await SeedAsync(now.AddMinutes(-20), "0xFIRST3", 0, 0, 0);
        await SeedAsync(measuredAt, "0xBUSY", 40, 8_000_000, 9_000_000);

        var json = await McpQueryTools.GetTopQueriesByCpu(
            new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 4);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        var message = doc.RootElement.GetProperty("message").GetString();
        Assert.DoesNotContain("No query stats available", message, StringComparison.Ordinal);
        Assert.Contains("none carries a measured delta", message, StringComparison.Ordinal);
        var hints = doc.RootElement.GetProperty("hints");
        Assert.Equal(4, hints.GetProperty("snapshots_in_window").GetInt64());
        Assert.Equal(0, hints.GetProperty("snapshots_with_measured_delta").GetInt64());
        Assert.True(hints.GetProperty("suggested_hours_back").GetInt32() is 10 or 11);

        var wider = JsonDocument.Parse(await McpQueryTools.GetTopQueriesByCpu(
            new LocalDataService(_duckDb), _serverManager, "TestServer",
            hours_back: hints.GetProperty("suggested_hours_back").GetInt32() + 1)).RootElement;
        Assert.Equal("0xBUSY", wider.GetProperty("queries").EnumerateArray().Single().GetProperty("query_hash").GetString());
    }

    [Fact]
    public async Task AWindowWithNoSnapshotsAtAll_StaysUnavailable()
    {
        await SeedAsync(DateTime.UtcNow.AddHours(-10), "0xBUSY", 40, 8_000_000, 9_000_000);

        var json = await McpQueryTools.GetTopQueriesByCpu(
            new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 4);

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("No query stats available for the specified time range.", doc.RootElement.GetProperty("message").GetString());
    }

    private async Task SeedAsync(DateTime collectedUtc, string queryHash, long executions, long cpuUs, long elapsedUs)
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
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xH" + queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 1" });
        await cmd.ExecuteNonQueryAsync();
    }
}

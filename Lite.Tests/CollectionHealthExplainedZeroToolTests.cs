/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// #9086: get_collection_health said "2 need a look" over two HEALTHY rows whose only remark was a zero the output_finding explains (no job
/// running, no trace flag set). The note's count of collectors that need a look now agrees with the rows: a healthy collector that read and
/// found nothing is counted apart, and its row keeps the output_finding that says why.
/// </summary>
public sealed class CollectionHealthExplainedZeroToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ExplainedZeroSrv";
    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthExplainedZeroToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-explainedzero-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task ExplainedZeroCollector_IsNotCountedAsNeedingALook_AndKeepsItsOutputFinding()
    {
        await SeedRunsAsync("wait_stats", "SUCCESS", 40, null);
        await SeedRunsAsync("running_jobs", "SUCCESS", 0, null);
        await SeedRunsAsync("memory_stats", "ERROR", 0, "simulated failure");

        var json = await McpHealthTools.GetCollectionHealth(new LocalDataService(_duckDb), _serverManager, ServerName);
        var root = JsonDocument.Parse(json).RootElement;
        var rows = root.GetProperty("collectors").EnumerateArray().ToDictionary(c => c.GetProperty("collector").GetString()!);
        var note = root.GetProperty("collector_detail_note").GetString()!;

        var jobs = rows["running_jobs"];
        Assert.Equal("HEALTHY", jobs.GetProperty("status").GetString());
        Assert.False(string.IsNullOrWhiteSpace(jobs.GetProperty("output_finding").GetString()));
        Assert.NotEqual("HEALTHY", rows["memory_stats"].GetProperty("status").GetString());
        Assert.StartsWith("1 of 3 collectors below is HEALTHY", note, StringComparison.Ordinal);
        Assert.Contains(" 1 needs a look", note, StringComparison.Ordinal);
        Assert.Contains(" 1 is HEALTHY and stored nothing", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnexplainedZeroSnapshotCollector_StillNeedsALook()
    {
        await SeedRunsAsync("wait_stats", "SUCCESS", 40, null);
        await SeedRunsAsync("memory_stats", "SUCCESS", 0, null);

        var json = await McpHealthTools.GetCollectionHealth(new LocalDataService(_duckDb), _serverManager, ServerName);
        var note = JsonDocument.Parse(json).RootElement.GetProperty("collector_detail_note").GetString()!;

        Assert.Contains(" 1 needs a look", note, StringComparison.Ordinal);
        Assert.DoesNotContain("stored nothing", note, StringComparison.Ordinal);
    }

    private async Task SeedRunsAsync(string collector, string status, int rowsCollected, string? errorMessage)
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 6; i++)
        {
            await SeedLogAsync(collector, now.AddMinutes(-i * 30 - 2), status, rowsCollected, errorMessage);
        }
    }

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string status, int rowsCollected, string? errorMessage)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= await OpenAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 50, $6, $7, $8, 40, 10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)errorMessage ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = rowsCollected });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> OpenAsync()
    {
        var connection = new DuckDBConnection(_duckDb.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }
}

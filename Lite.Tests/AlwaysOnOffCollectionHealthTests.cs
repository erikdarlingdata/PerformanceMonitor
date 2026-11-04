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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7375: on a SQL Server with Always On off, Darling's get_collection_health lists the two Always On collectors as not_collected, and Lite
/// listed both as HEALTHY partial rows that "need a look". Lite now reads the newest server_properties row the way Darling does (a NULL
/// flag reads as off, #6838), turns a HEALTHY Always On collector into a not_collected row with Darling's sentence, leaves a FAILING one
/// alone, and stops counting the rows in collector_detail_note.
/// </summary>
public sealed class AlwaysOnOffCollectionHealthTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "AlwaysOnOffSrv";
    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AlwaysOnOffCollectionHealthTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-alwaysonoff-" + Guid.NewGuid().ToString("N"));
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

    /// <summary>Both Always On collectors HEALTHY with zero rows, plus wait_stats HEALTHY with rows.</summary>
    private async Task SeedHealthyAsync()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 6; i++)
        {
            await SeedLogAsync("ag_replica_states", now.AddMinutes(-i * 30 - 2), "SUCCESS", 0, null);
            await SeedLogAsync("ag_database_replica_states", now.AddMinutes(-i * 30 - 2), "SUCCESS", 0, null);
            await SeedLogAsync("wait_stats", now.AddMinutes(-i * 30 - 2), "SUCCESS", 40, null);
        }
    }

    private async Task<JsonElement> ReadAsync()
    {
        var json = await McpHealthTools.GetCollectionHealth(new LocalDataService(_duckDb), _serverManager, ServerName);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(null)]
    public async Task AlwaysOffNewestRow_ListsHealthyAlwaysOnCollectorsAsNotCollected_AndStopsCountingThem(bool? newestFlag)
    {
        await SeedHealthyAsync();
        /* An older row said Always On was on: the NEWEST row decides, and its NULL reads as off. */
        await SeedPropertiesAsync(DateTime.UtcNow.AddHours(-5), true);
        await SeedPropertiesAsync(DateTime.UtcNow.AddHours(-1), newestFlag);

        var root = await ReadAsync();
        var collectors = root.GetProperty("collectors").EnumerateArray().ToList();

        var ag = collectors.Where(c => c.GetProperty("collector").GetString()!.StartsWith("ag_", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, ag.Count);
        foreach (var row in ag)
        {
            Assert.Equal("not_collected", row.GetProperty("status").GetString());
            Assert.Equal(CollectorHealthClassifier.AlwaysOnOffMessage, row.GetProperty("message").GetString());
            Assert.False(row.TryGetProperty("partial_detail", out _));
        }

        var note = root.GetProperty("collector_detail_note").GetString()!;
        Assert.StartsWith("1 of 1 collector(s) below are HEALTHY", note, StringComparison.Ordinal);
        Assert.Contains(" 0 need a look", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AlwaysOnNewestRow_LeavesTheAlwaysOnCollectorsAsLogged()
    {
        await SeedHealthyAsync();
        await SeedPropertiesAsync(DateTime.UtcNow.AddHours(-1), true);

        var root = await ReadAsync();
        var ag = root.GetProperty("collectors").EnumerateArray()
            .Where(c => c.GetProperty("collector").GetString()!.StartsWith("ag_", StringComparison.Ordinal)).ToList();

        Assert.Equal(2, ag.Count);
        Assert.All(ag, row => Assert.Equal("HEALTHY", row.GetProperty("status").GetString()));
        Assert.StartsWith("1 of 3 collector(s) below are HEALTHY", root.GetProperty("collector_detail_note").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoPropertiesRow_MakesNoClaim()
    {
        await SeedHealthyAsync();

        var root = await ReadAsync();

        Assert.All(root.GetProperty("collectors").EnumerateArray(), row => Assert.NotEqual("not_collected", row.GetProperty("status").GetString()));
    }

    [Fact]
    public async Task AFailingAlwaysOnCollector_KeepsItsRowWithItsLastError_WhenAlwaysOnIsOff()
    {
        var now = DateTime.UtcNow;
        await SeedHealthyAsync();
        for (var i = 0; i < 4; i++)
            await SeedLogAsync("ag_replica_states", now.AddMinutes(-i * 5 - 1), "ERROR", null, "VIEW SERVER STATE permission was denied.");
        await SeedPropertiesAsync(now.AddHours(-1), false);

        var root = await ReadAsync();
        var agRows = root.GetProperty("collectors").EnumerateArray()
            .Where(c => c.GetProperty("collector").GetString() == "ag_replica_states").ToList();

        var band = Assert.Single(agRows, r => r.GetProperty("status").GetString() != "not_collected");
        Assert.NotEqual("HEALTHY", band.GetProperty("status").GetString());
        Assert.Contains("permission", band.GetProperty("last_error").GetString()!, StringComparison.OrdinalIgnoreCase);
        /* The other Always On collector stayed HEALTHY, so it alone is reported as not collected. */
        Assert.Single(root.GetProperty("collectors").EnumerateArray(), r => r.GetProperty("status").GetString() == "not_collected");
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedPropertiesAsync(DateTime collectionTimeUtc, bool? isHadrEnabled)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, is_hadr_enabled)
VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = -_nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)isHadrEnabled ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string status, int? rowsCollected, string? errorMessage)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
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
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rowsCollected ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}

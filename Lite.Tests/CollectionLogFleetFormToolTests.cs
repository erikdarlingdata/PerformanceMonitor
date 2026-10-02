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
/// #4199's stated pin, Lite's twin of <c>Darling.Tests/McpFleetResponseBudgetLivePostgresTests</c>: Lite's
/// <c>get_collection_log</c> fleet form (server_name omitted) merges rows across servers rather than
/// resolving to one, and <c>min_duration_ms</c> ranks the merged page fleet-wide rather than per server.
/// Two servers, matching <see cref="CollectionLogToolTests"/>'s per-server file but with the second server
/// this shape needs and that file never seeds.
/// </summary>
public sealed class CollectionLogFleetFormToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerAName = "FleetFormSrvA";
    private const string ServerBName = "FleetFormSrvB";

    private readonly int _serverAId;
    private readonly int _serverBId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionLogFleetFormToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-collogfleet-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var serverA = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerAName, IsEnabled = true };
        var serverB = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerBName, IsEnabled = true };
        _serverManager.AddServer(serverA);
        _serverManager.AddServer(serverB);
        _serverAId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(serverA));
        _serverBId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(serverB));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task FleetForm_MergesRowsAcrossServers_AndMinDurationMsRanksAcrossServers()
    {
        var service = new LocalDataService(_duckDb);

        /* The fleet's slowest run is on server B, inserted with an OLDER collection_time than server A's
           rows -- so newest-first ordering would surface server A first, and only a true duration-ranked
           fleet-wide sort puts server B's row on top. */
        await SeedLogAsync(_serverAId, ServerAName, "query_store", DateTime.UtcNow.AddMinutes(-5), 100);
        await SeedLogAsync(_serverBId, ServerBName, "query_store", DateTime.UtcNow.AddMinutes(-20), 9000);
        await SeedLogAsync(_serverAId, ServerAName, "query_store", DateTime.UtcNow.AddMinutes(-3), 150);

        var newestFirst = await McpHealthTools.GetCollectionLog(service, _serverManager, server_name: null, hours_back: 24, limit: 200);
        var newestRoot = JsonDocument.Parse(newestFirst).RootElement;
        Assert.Equal("fleet", newestRoot.GetProperty("scope").GetString());
        var names = newestRoot.GetProperty("runs").EnumerateArray()
            .Select(r => r.GetProperty("server_name").GetString()).Distinct().ToList();
        Assert.True(names.Count >= 2, $"Expected rows from >= 2 servers, got: {string.Join(", ", names)}");

        var slowestFirst = await McpHealthTools.GetCollectionLog(
            service, _serverManager, server_name: null, hours_back: 24, limit: 200, min_duration_ms: 0);
        var runs = JsonDocument.Parse(slowestFirst).RootElement.GetProperty("runs").EnumerateArray().ToList();
        Assert.True(runs.Count >= 3);
        Assert.Equal(ServerBName, runs[0].GetProperty("server_name").GetString());
        Assert.Equal(9000, runs[0].GetProperty("duration_ms").GetDouble());
    }

    [Fact]
    public async Task FleetForm_AlsoReachedByOmittingTheArgument_AndByPassingAsterisk()
    {
        var service = new LocalDataService(_duckDb);
        await SeedLogAsync(_serverAId, ServerAName, "query_store", DateTime.UtcNow.AddMinutes(-1), 50);
        await SeedLogAsync(_serverBId, ServerBName, "query_store", DateTime.UtcNow.AddMinutes(-1), 60);

        foreach (var name in new string?[] { null, "", "*" })
        {
            var json = await McpHealthTools.GetCollectionLog(service, _serverManager, server_name: name, hours_back: 24, limit: 200);
            Assert.Equal("fleet", JsonDocument.Parse(json).RootElement.GetProperty("scope").GetString());
        }
    }

    /// <summary>
    /// #4199's default-limit sizing, Lite's twin of the Darling live byte-budget test: a fleet-wide call at
    /// DEFAULT arguments (server_name AND limit both omitted) stays under
    /// <see cref="PerformanceMonitor.Common.McpResponseBudget.DefaultBytes"/> even seeded past the fleet
    /// default row count.
    /// </summary>
    [Fact]
    public async Task FleetFormDefaultCall_StaysUnderTheResponseBudget()
    {
        var service = new LocalDataService(_duckDb);

        for (var i = 0; i < 40; i++)
        {
            await SeedLogAsync(_serverAId, ServerAName, "query_store", DateTime.UtcNow.AddMinutes(-(i + 1)), 100 + i);
            await SeedLogAsync(_serverBId, ServerBName, "wait_stats", DateTime.UtcNow.AddMinutes(-(i + 1)), 200 + i);
        }

        var json = await McpHealthTools.GetCollectionLog(service, _serverManager, server_name: null, hours_back: 24);
        var bytes = System.Text.Encoding.UTF8.GetByteCount(json);
        Assert.True(bytes <= PerformanceMonitor.Common.McpResponseBudget.DefaultBytes,
            $"Fleet-form default call was {bytes:N0} bytes, over the {PerformanceMonitor.Common.McpResponseBudget.DefaultBytes:N0}-byte budget.");
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

    private async Task SeedLogAsync(int serverId, string serverName, string collector, DateTime collectionTimeUtc, double durationMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.8 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.2 });
        await cmd.ExecuteNonQueryAsync();
    }
}

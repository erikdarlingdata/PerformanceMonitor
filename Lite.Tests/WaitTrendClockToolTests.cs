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
/// get_wait_trend stamps each bucket in UTC and its description says so. The server is planted five hours behind
/// UTC, so a bucket stamped on its clock would read five hours earlier than the UTC one (the way get_cpu_utilization's
/// does); the stamp is the UTC bucket's start, which is what the description promises.
/// </summary>
public sealed class WaitTrendClockToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "WaitClockSrv";

    private static readonly DateTime AsOfUtc = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 850200;

    public WaitTrendClockToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-waitclock-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);

        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task WaitTrend_StampsBucketsInUtc_AndItsDescriptionSaysSo()
    {
        var description = Lite.Tests.McpToolGuideTests.Served("get_wait_trend").Served;
        Assert.Contains("Every stamp here is UTC, not the monitored server's clock", description, StringComparison.Ordinal);

        await SeedServerClockAsync();
        await SeedWaitAsync(new DateTime(2026, 11, 1, 5, 5, 0), 1_200);
        await SeedWaitAsync(new DateTime(2026, 11, 1, 5, 15, 0), 6_000);

        var service = new LocalDataService(_duckDb);
        var json = await McpWaitTools.GetWaitTrend(service, _serverManager, "LCK_M_X", ServerName, 4, AsOfUtc.ToString("o"), 60);
        var root = JsonDocument.Parse(json).RootElement;
        Assert.True(root.TryGetProperty("trend", out var trend), "get_wait_trend answered no trend: " + json);
        var stamps = trend.EnumerateArray()
            .Select(p => p.GetProperty("time").GetString()![..19])
            .ToArray();

        /* The hourly bucket both collections fall in starts at 05:00 UTC; on the server's clock it would be 00:00. */
        Assert.Equal(new[] { "2026-11-01T05:00:00" }, stamps);
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

    private async Task SeedServerClockAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, -300, 'Eastern Standard Time')";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId++);
        P(DateTime.SpecifyKind(AsOfUtc.AddMinutes(-1), DateTimeKind.Unspecified));
        P(_serverId);
        P(ServerName);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedWaitAsync(DateTime collectionTimeUtc, long deltaMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
            VALUES ($1, $2, $3, $4, 'LCK_M_X', 5000, $5, 0, 1, $5, 0, 600)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId++);
        P(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        P(_serverId);
        P(ServerName);
        P(deltaMs);
        await cmd.ExecuteNonQueryAsync();
    }
}

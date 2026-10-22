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
/// #6446: <c>get_wait_trend</c> on a wait type with no rows in the window. The status word tells a caller whether the gap
/// is permanent, so it has to follow the server's own rules: <c>empty</c> when the collector would store the wait type
/// and nothing waited, <c>not_collected</c> only for a wait type the collector never stores (the ignored list),
/// <c>unavailable</c> when no wait stats exist at all. Run through the real tool over a real DuckDB.
/// </summary>
public sealed class WaitTrendMissStatusTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string CollectedWait = "PAGEIOLATCH_SH";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;

    public WaitTrendMissStatusTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "WaitTrendMiss_" + Guid.NewGuid().ToString("N")[..8]);
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
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AStoredWaitTypeWithNoRowsInTheWindow_IsEmpty_AndKeepsTheTypoHint()
    {
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-30), CollectedWait);
        Assert.DoesNotContain("CXPACKET", IgnoredWaitTypes.Load());

        using var doc = JsonDocument.Parse(await McpWaitTools.GetWaitTrend(_dataService, _serverManager, "CXPACKET", "TestServer"));

        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        var hint = doc.RootElement.GetProperty("hints").GetProperty("collected_wait_types");
        Assert.Contains(CollectedWait, hint.EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public async Task AnIgnoredWaitType_IsNotCollected_AndSaysTheCollectorNeverStoresIt()
    {
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-30), CollectedWait);
        Assert.Contains("SLEEP_TASK", IgnoredWaitTypes.Load());

        using var doc = JsonDocument.Parse(await McpWaitTools.GetWaitTrend(_dataService, _serverManager, "SLEEP_TASK", "TestServer"));

        Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("never stores", doc.RootElement.GetProperty("message").GetString());
    }

    [Fact]
    public async Task NoWaitStatsAtAll_IsUnavailable()
    {
        using var doc = JsonDocument.Parse(await McpWaitTools.GetWaitTrend(_dataService, _serverManager, "CXPACKET", "TestServer"));

        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
    }

    private async Task SeedWaitAsync(DateTime at, string waitType)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
            VALUES (-1, $1, $2, 'TestServer', $3, 0, 0, 0, 50, 1000, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(at);
        P(_serverId);
        P(waitType);
        await cmd.ExecuteNonQueryAsync();
    }
}

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
/// and nothing waited, <c>not_collected</c> only for a wait type on the ignored list (the collector skips it, and rows
/// stored before it was ignored are hidden, #7880),
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
    public async Task AnIgnoredWaitType_IsNotCollected_AndSaysItIsOnTheIgnoredList()
    {
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-30), CollectedWait);
        Assert.Contains("SLEEP_TASK", IgnoredWaitTypes.Load());

        using var doc = JsonDocument.Parse(await McpWaitTools.GetWaitTrend(_dataService, _serverManager, "SLEEP_TASK", "TestServer"));

        Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
        var message = doc.RootElement.GetProperty("message").GetString()!;
        Assert.Contains("ignored (benign) wait list", message, StringComparison.Ordinal);
        /* Rows stored before a type was ignored stay in the file until retention removes them (#7880), so the
           answer must not claim the collector never stores the type. */
        Assert.DoesNotContain("never stores", message, StringComparison.Ordinal);
        Assert.Contains("not shown", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// #7880: rows the store already holds for an ignored wait type are hidden by get_wait_stats and get_wait_types at read
    /// time, and get_wait_trend hides them the same way, so the same server and window never show a wait in one read and
    /// not the other.
    /// </summary>
    [Fact]
    public async Task AnIgnoredWaitTypeWithStoredRows_IsNotCollected_AsGetWaitStatsAlsoHidesIt()
    {
        var t0 = DateTime.UtcNow.AddMinutes(-30);
        Assert.Contains("SLEEP_TASK", IgnoredWaitTypes.Load());
        for (var i = 0; i < 3; i++)
        {
            await SeedWaitAsync(t0.AddMinutes(i), "SLEEP_TASK");
            await SeedWaitAsync(t0.AddMinutes(i), CollectedWait);
        }

        var stats = await _dataService.GetWaitStatsAsync(_serverId, hoursBack: 3);
        Assert.DoesNotContain(stats, r => r.WaitType == "SLEEP_TASK");
        Assert.Contains(stats, r => r.WaitType == CollectedWait);

        using var doc = JsonDocument.Parse(await McpWaitTools.GetWaitTrend(_dataService, _serverManager, "SLEEP_TASK", "TestServer", hours_back: 3));
        Assert.True(doc.RootElement.TryGetProperty("status", out var status),
            "get_wait_trend served points for an ignored wait type that get_wait_stats hides: " + doc.RootElement.GetRawText());
        Assert.Equal("not_collected", status.GetString());

        /* The wait type the collector stores still answers with points over the same rows. */
        using var kept = JsonDocument.Parse(await McpWaitTools.GetWaitTrend(_dataService, _serverManager, CollectedWait, "TestServer", hours_back: 3));
        Assert.False(kept.RootElement.TryGetProperty("status", out _));
        Assert.NotEmpty(kept.RootElement.GetProperty("trend").EnumerateArray());
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

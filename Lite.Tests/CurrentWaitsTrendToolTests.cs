/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// Lite's get_current_waits_trend (#2484), twin of Darling's. Ported in the same change rather than parked
/// on the divergence ratchet.
///
/// <para>The empty case is the one that matters, and it is sharper here than for most reads: the WRONG
/// answer is the reassuring one. "Nothing was waiting" reads as an all-clear, and a caller who believes it
/// stops looking — so a server the collector has never sampled must not be described that way.</para>
/// </summary>
public sealed class CurrentWaitsTrendToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "CurrentWaitsSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 700000;

    public CurrentWaitsTrendToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-waits-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);

        /* Derived, not stored -- seeding under a hardcoded id would write rows the tool looks past. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task NeverSampled_IsNotReportedAsAnAllClear()
    {
        var service = new LocalDataService(_duckDb);

        var never = await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 4, null);
        var root = JsonDocument.Parse(never).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("NOT an all-clear", text, StringComparison.Ordinal);
        Assert.Contains("EVER", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SampledButQuiet_IsAGenuineAllClear_AndSaysSoDifferently()
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(DateTime.UtcNow.AddHours(-48), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");

        var clear = await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 1, null);
        var root = JsonDocument.Parse(clear).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("genuine all-clear", text, StringComparison.Ordinal);

        /* Same zero rows as the never-sampled case, and it must not reach for the same word. */
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BothSeriesComeBackTogether_AndOnlyBlockedRowsCountAsBlocked()
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-10), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-9), "PAGEIOLATCH_SH", 250, blockingSessionId: 0, database: "AppDb");

        var hit = await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 4, null);
        var root = JsonDocument.Parse(hit).RootElement;

        Assert.Equal(2, root.GetProperty("waiting_tasks").GetArrayLength());

        /*
            The PAGEIOLATCH row waits on IO and blocks on nothing, so it is in the wait series and NOT the
            blocked series. That difference is exactly why the two are returned together: the same wait
            spike means a resource problem without blocked sessions and contention with them.
        */
        var blocked = root.GetProperty("blocked_sessions");
        Assert.Equal(1, blocked.GetArrayLength());
        Assert.Equal("AppDb", blocked[0].GetProperty("database_name").GetString());
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

    private async Task SeedWaitAsync(
        DateTime collectionTimeUtc, string waitType, long waitMs, int blockingSessionId, string database)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = 55 });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockingSessionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        await cmd.ExecuteNonQueryAsync();
    }
}

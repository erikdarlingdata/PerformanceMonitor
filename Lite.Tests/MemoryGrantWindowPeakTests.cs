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
/// #6129: get_memory_grants window[] shaped like a real instance. One pool's workspace shrank until almost
/// nothing was available while every snapshot sampled zero granted and zero waiters (grants too short to land
/// on a snapshot); a second pool did catch a grant and a waiter at an older snapshot. A peak of zero has no
/// moment, so its *_at is null; a real peak keeps its own time (not the newest snapshot's); and the answer says
/// its peaks are sampled so an agent does not read 0 as "no grant pressure".
/// </summary>
public sealed class MemoryGrantWindowPeakTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "GrantPeakSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 930000;

    public MemoryGrantWindowPeakTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-grantpeak-" + Guid.NewGuid().ToString("N"));
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
    public async Task ZeroPeaks_HaveNoTime_RealPeaksKeepTheirOwn_AndTheAnswerSaysItIsSampled()
    {
        var t0 = DateTime.UtcNow.AddMinutes(-30);
        var t1 = t0.AddMinutes(10);
        var t2 = t0.AddMinutes(20);

        /* Pool 1: available workspace 157 -> 40 -> 6.45 MB, nothing granted, nobody waiting at any snapshot. */
        await SeedAsync(t0, poolId: 1, availableMb: 157.0, grantedMb: 0.0, waiters: 0);
        await SeedAsync(t1, poolId: 1, availableMb: 40.0, grantedMb: 0.0, waiters: 0);
        await SeedAsync(t2, poolId: 1, availableMb: 6.45, grantedMb: 0.0, waiters: 0);

        /* Pool 2: a grant of 300 MB and 4 waiters at t1 only; the newest snapshot (t2) is quiet. */
        await SeedAsync(t0, poolId: 2, availableMb: 500.0, grantedMb: 0.0, waiters: 0);
        await SeedAsync(t1, poolId: 2, availableMb: 200.0, grantedMb: 300.0, waiters: 4);
        await SeedAsync(t2, poolId: 2, availableMb: 500.0, grantedMb: 0.0, waiters: 0);

        var json = await McpMemoryTools.GetMemoryGrants(new LocalDataService(_duckDb), _serverManager, ServerName, 2);
        using var doc = JsonDocument.Parse(json);
        var window = doc.RootElement.GetProperty("window").EnumerateArray().ToDictionary(w => w.GetProperty("pool_id").GetInt32());

        var quiet = window[1];
        Assert.Equal(0, quiet.GetProperty("peak_waiter_count").GetInt64());
        Assert.Equal(JsonValueKind.Null, quiet.GetProperty("peak_waiters_at").ValueKind);
        Assert.Equal(0.0, quiet.GetProperty("peak_granted_memory_mb").GetDouble());
        Assert.Equal(JsonValueKind.Null, quiet.GetProperty("peak_granted_at").ValueKind);
        Assert.Equal(6.45, quiet.GetProperty("min_available_memory_mb").GetDouble(), 2);

        var busy = window[2];
        Assert.Equal(4, busy.GetProperty("peak_waiter_count").GetInt64());
        Assert.Equal(300.0, busy.GetProperty("peak_granted_memory_mb").GetDouble());
        var peakAt = DateTime.Parse(busy.GetProperty("peak_granted_at").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind);
        Assert.True(Math.Abs((peakAt - t1).TotalSeconds) < 2, "the peak keeps its own snapshot's time, not the newest snapshot's");
        Assert.NotEqual(JsonValueKind.Null, busy.GetProperty("peak_waiters_at").ValueKind);

        Assert.Contains("sampled", doc.RootElement.GetProperty("window_note").GetString(), StringComparison.Ordinal);
    }

    private async Task SeedAsync(DateTime collectionTimeUtc, int poolId, double availableMb, double grantedMb, int waiters)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name,
     resource_semaphore_id, pool_id, available_memory_mb, granted_memory_mb, waiter_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = (short)0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = poolId });
        cmd.Parameters.Add(new DuckDBParameter { Value = availableMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = grantedMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = waiters });
        await cmd.ExecuteNonQueryAsync();
    }
}

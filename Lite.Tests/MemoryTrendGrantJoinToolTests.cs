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
/// #3548: get_memory_trend joins the memory-grant series so total_granted_mb carries real data — the
/// complete fix #3529's null was the honest placeholder for. The join is nearest-match within 30 seconds
/// because the two collectors each stamp their own DateTime.UtcNow per run: same-cycle rows sit seconds
/// apart, so an equality join returns nothing, while a wider match would smear a slower grants cadence
/// across points it never measured.
///
/// <para>The three claims worth pinning are the three an agent acts on: a matched point carries the
/// pool-summed measurement, a matched point measuring NOTHING granted is a genuine 0.0 (a snapshot
/// existed — zero is a measurement here, not a fabrication), and an unmatched point is null with the
/// envelope's granted_note explaining the gap — which vanishes entirely when every point matched, so a
/// clean window is not captioned with an apology.</para>
/// </summary>
public sealed class MemoryTrendGrantJoinToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "GrantJoinSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 910000;

    public MemoryTrendGrantJoinToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-grantjoin-" + Guid.NewGuid().ToString("N"));
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
    public async Task JoinedPoints_CarryThePoolSum_AGenuineZero_AndANullForTheUncoveredPoint()
    {
        var t0 = DateTime.UtcNow.AddMinutes(-30);
        var t1 = t0.AddMinutes(1);
        var t2 = t0.AddMinutes(2);

        await SeedMemoryAsync(t0);
        await SeedMemoryAsync(t1);
        await SeedMemoryAsync(t2);

        /* One grants snapshot 4s after t0 with TWO pools (the SUM is the contract, not a row pick), one
           6s after t1 measuring nothing granted, and nothing anywhere near t2. The offsets are the real
           shape: each collector stamps its own UtcNow, so same-cycle rows land seconds apart. */
        var snap0 = t0.AddSeconds(4);
        await SeedGrantAsync(snap0, poolId: 1, grantedMb: 25.0);
        await SeedGrantAsync(snap0, poolId: 2, grantedMb: 100.0);
        await SeedGrantAsync(t1.AddSeconds(6), poolId: 2, grantedMb: 0.0);

        var payload = await McpMemoryTools.GetMemoryTrend(new LocalDataService(_duckDb), _serverManager, ServerName, 4);
        var root = JsonDocument.Parse(payload).RootElement;

        var trend = root.GetProperty("trend");
        Assert.Equal(3, trend.GetArrayLength());
        Assert.Equal(125.0, trend[0].GetProperty("total_granted_mb").GetDouble(), precision: 6);

        /* The genuine zero: a snapshot existed and measured nothing granted. Distinguishable from the
           uncovered point below only because the join keeps zero-vs-unknown apart. */
        Assert.Equal(JsonValueKind.Number, trend[1].GetProperty("total_granted_mb").ValueKind);
        Assert.Equal(0.0, trend[1].GetProperty("total_granted_mb").GetDouble(), precision: 6);

        Assert.Equal(JsonValueKind.Null, trend[2].GetProperty("total_granted_mb").ValueKind);

        var note = root.GetProperty("granted_note").GetString()!;
        Assert.Contains("get_memory_grants", note, StringComparison.Ordinal);
        Assert.Contains("30 seconds", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AFullyCoveredWindow_CarriesNoGrantedNoteAtAll()
    {
        var t0 = DateTime.UtcNow.AddMinutes(-20);
        await SeedMemoryAsync(t0);
        await SeedGrantAsync(t0.AddSeconds(3), poolId: 2, grantedMb: 50.0);

        var payload = await McpMemoryTools.GetMemoryTrend(new LocalDataService(_duckDb), _serverManager, ServerName, 4);
        var root = JsonDocument.Parse(payload).RootElement;

        Assert.Equal(50.0, root.GetProperty("trend")[0].GetProperty("total_granted_mb").GetDouble(), precision: 6);
        Assert.False(root.TryGetProperty("granted_note", out _),
            "a window the grants series fully covers must not be captioned with a gap note");
    }

    [Fact]
    public async Task TheToleranceBoundary_Is30Seconds_InAt30_OutAt31()
    {
        var tIn = DateTime.UtcNow.AddMinutes(-40);
        var tOut = tIn.AddMinutes(5);

        await SeedMemoryAsync(tIn);
        await SeedMemoryAsync(tOut);
        await SeedGrantAsync(tIn.AddSeconds(30), poolId: 2, grantedMb: 75.0);
        await SeedGrantAsync(tOut.AddSeconds(31), poolId: 2, grantedMb: 999.0);

        var payload = await McpMemoryTools.GetMemoryTrend(new LocalDataService(_duckDb), _serverManager, ServerName, 4);
        var trend = JsonDocument.Parse(payload).RootElement.GetProperty("trend");

        Assert.Equal(75.0, trend[0].GetProperty("total_granted_mb").GetDouble(), precision: 6);
        Assert.Equal(JsonValueKind.Null, trend[1].GetProperty("total_granted_mb").ValueKind);
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

    private async Task SeedMemoryAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = 65536.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 8192.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 49152.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 40000.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 35000.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5000.0 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedGrantAsync(DateTime collectionTimeUtc, int poolId, double grantedMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name,
     resource_semaphore_id, pool_id, granted_memory_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = (short)0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = poolId });
        cmd.Parameters.Add(new DuckDBParameter { Value = grantedMb });
        await cmd.ExecuteNonQueryAsync();
    }
}

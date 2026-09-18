/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Reflection;
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
/// The three tools where the two SKUs disagreed (#2485): <c>get_memory_trend</c>, <c>get_file_io_trend</c>
/// and <c>get_query_duration_trend</c> returned a BARE empty array on Lite while their Darling twins
/// returned a status envelope. Same tool name, same client, two different answers depending on which SKU it
/// was pointed at — a parity break independent of the issue's main point and arguably the more urgent half.
///
/// <para>Darling's envelope was not the target either. "No memory trend data available" is true both of a
/// window that was simply quiet and of a server the collector has never touched, and those want opposite
/// next moves — widen the window, versus go find out why collection is not running, which widening will
/// never fix. Both SKUs now make that distinction, in the same two sentences.</para>
/// </summary>
public sealed class TrendEmptyParityToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TrendEmptySrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 830000;

    public TrendEmptyParityToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-trendempty-" + Guid.NewGuid().ToString("N"));
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
    public async Task MemoryTrend_NeverCollected_AndAQuietWindow_AreDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        AssertNeverCollected(await McpMemoryTools.GetMemoryTrend(service, _serverManager, ServerName, 4));

        await SeedMemoryAsync(DateTime.UtcNow.AddHours(-48));
        AssertQuietWindow(await McpMemoryTools.GetMemoryTrend(service, _serverManager, ServerName, 1));

        await SeedMemoryAsync(DateTime.UtcNow.AddMinutes(-10));
        AssertPayload(await McpMemoryTools.GetMemoryTrend(service, _serverManager, ServerName, 4));
    }

    [Fact]
    public async Task FileIoTrend_NeverCollected_AndAQuietWindow_AreDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        AssertNeverCollected(await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 4));

        await SeedFileIoAsync(DateTime.UtcNow.AddHours(-48));
        AssertQuietWindow(await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 1));

        await SeedFileIoAsync(DateTime.UtcNow.AddMinutes(-10));
        AssertPayload(await McpIoTools.GetFileIoTrend(service, _serverManager, ServerName, 4));
    }

    [Fact]
    public async Task QueryDurationTrend_NeverCollected_AndAQuietWindow_AreDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        var never = await McpQueryTools.GetQueryDurationTrend(service, _serverManager, ServerName, 4);
        AssertNeverCollected(never);

        await SeedQueryAsync(DateTime.UtcNow.AddHours(-48));
        var quiet = await McpQueryTools.GetQueryDurationTrend(service, _serverManager, ServerName, 1);
        AssertQuietWindow(quiet);

        await SeedQueryAsync(DateTime.UtcNow.AddMinutes(-10));
        var payload = await McpQueryTools.GetQueryDurationTrend(service, _serverManager, ServerName, 4);
        AssertPayload(payload);

        /* #3541 A2: get_query_duration_trend's three answers all carry the same disclosure block as its
           Darling twin — raw, per-collection, the requested start standing (not truncated) on the two empty
           branches and the first point on the data one. */
        foreach (var envelope in new[] { never, quiet, payload })
        {
            var root = JsonDocument.Parse(envelope).RootElement;
            Assert.Equal("raw", root.GetProperty("source").GetString());
            Assert.Equal("per-collection", root.GetProperty("bucket").GetString());
            Assert.True(root.TryGetProperty("effective_start", out _));
            Assert.True(root.TryGetProperty("truncated", out _));
        }

        Assert.False(JsonDocument.Parse(never).RootElement.GetProperty("truncated").GetBoolean());
        Assert.False(JsonDocument.Parse(quiet).RootElement.GetProperty("truncated").GetBoolean());

        var data = JsonDocument.Parse(payload).RootElement;
        Assert.Equal(data.GetProperty("trend")[0].GetProperty("time").GetString(), data.GetProperty("effective_start").GetString());
        Assert.Equal(
            data.GetProperty("trend")[0].GetProperty("value").GetDouble(),
            data.GetProperty("trend")[0].GetProperty("elapsed_ms_per_second").GetDouble());

        /* And get_query_trend over the same seeded query carries the block too — the last tool where the two
           SKUs' envelopes disagreed (Darling's has had it since #2353). */
        var single = JsonDocument.Parse(
            await McpQueryTools.GetQueryTrend(service, _serverManager, "0xEMPTYTRENDHASH", "AppDb", ServerName, 4)).RootElement;
        Assert.Equal("raw", single.GetProperty("source").GetString());
        Assert.Equal("per-collection", single.GetProperty("bucket").GetString());
        Assert.Equal(single.GetProperty("trend")[0].GetProperty("collection_time").GetString(), single.GetProperty("effective_start").GetString());
        Assert.True(single.GetProperty("truncated").GetBoolean(), "a series beginning 10 minutes ago in a 4-hour window starts past the slack");
        Assert.Equal(1, single.GetProperty("data_points").GetInt32());
    }

    /// <summary>
    /// #3529: the payload used to carry a hardcoded total_granted_mb of 0.0 — an agent investigating
    /// RESOURCE_SEMAPHORE read "granted was 0 all window" and ruled out memory grants, the exact wrong
    /// turn. With the #3548 join this is now specifically the UNCOVERED window (no memory_grant_stats
    /// rows seeded anywhere near these points): every point stays an explicit null and the envelope's
    /// note names the real source — never a fabricated zero. The covered arms live in
    /// <see cref="MemoryTrendGrantJoinToolTests"/>.
    /// </summary>
    [Fact]
    public async Task MemoryTrend_GrantedMemoryIsNullWithANoteNamingTheGrantsTool_NeverALiteralZero()
    {
        await SeedMemoryAsync(DateTime.UtcNow.AddMinutes(-10));

        var payload = await McpMemoryTools.GetMemoryTrend(new LocalDataService(_duckDb), _serverManager, ServerName, 4);
        var root = JsonDocument.Parse(payload).RootElement;

        Assert.True(root.GetProperty("trend").GetArrayLength() > 0);
        Assert.Contains("get_memory_grants", root.GetProperty("granted_note").GetString(), StringComparison.Ordinal);
        foreach (var point in root.GetProperty("trend").EnumerateArray())
        {
            Assert.Equal(JsonValueKind.Null, point.GetProperty("total_granted_mb").ValueKind);
        }
    }

    /// <summary>#3529's description half, superseded by the #3548 join: the tool now DELIVERS granted
    /// memory (joined per point from the grants series), so the description may promise it again — but it
    /// must name the null gap rather than promising an always-filled field, and still point at
    /// get_memory_grants as the series' own tool.</summary>
    [Fact]
    public void MemoryTrend_Description_PromisesTheJoinedGrantSeries_AndNamesTheNullGap()
    {
        var description = typeof(McpMemoryTools).GetMethod(nameof(McpMemoryTools.GetMemoryTrend))!
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("granted memory joined per point", description, StringComparison.Ordinal);
        Assert.Contains("total_granted_mb is null", description, StringComparison.Ordinal);
        Assert.Contains("get_memory_grants", description, StringComparison.Ordinal);
    }

    /// <summary>Nothing has ever been stored for this server: NOT an empty window, and widening it would
    /// never help.</summary>
    private static void AssertNeverCollected(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("EVER", text, StringComparison.Ordinal);
        Assert.Contains("not an empty window", text, StringComparison.Ordinal);
        Assert.DoesNotContain("widen hours_back", text, StringComparison.Ordinal);
    }

    /// <summary>The server collected and this window is simply quiet — the opposite next move, and it must
    /// not share wording with the case above.</summary>
    private static void AssertQuietWindow(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("widen hours_back", text, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
    }

    private static void AssertPayload(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        Assert.False(root.TryGetProperty("status", out _));
        Assert.True(root.GetProperty("trend").GetArrayLength() > 0);
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

    /* delta_reads above zero on purpose: the trend's top_files CTE requires read or write activity, so a
       row with zero deltas would leave the window empty for a reason that has nothing to do with #2485. */
    private async Task SeedFileIoAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, file_name, file_type, size_mb,
     num_of_reads, num_of_writes, read_bytes, write_bytes,
     io_stall_read_ms, io_stall_write_ms,
     delta_reads, delta_writes, delta_stall_read_ms, delta_stall_write_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, 0,
        $8, $9, 0, 0, $10, $11, $12, $13, $14, $15)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "AppDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "app.mdf" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ROWS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 500L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 200L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 2500L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 400L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 500L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 200L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 2500L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 400L });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        var naive = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "AppDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xEMPTYTRENDHASH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xSQLH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM Orders" });
        await cmd.ExecuteNonQueryAsync();
    }
}

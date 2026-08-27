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
/// Lite's get_collection_log (#2484), the twin of Darling's. Lite gained the tool in the same change that
/// added it to Darling rather than being parked on the divergence ratchet, so its behaviour is pinned here
/// for the same reasons -- and because the SKUs make PROMISES to each other that only a test can hold:
/// the same two words for the two kinds of empty, and store_duration_ms as one field name over two different
/// storage engines.
///
/// <para>Written at the TOOL level, not the reader level. The reader is the easy half; the cap contract and
/// the truncation signal live in the tool, and a reader-level test would have missed both.</para>
/// </summary>
public sealed class CollectionLogToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "CollectionLogSrv";

    /*
        Lite does not store a server id -- ServerResolver DERIVES it from the storage name, so the seeded
        rows have to be written under the same derived value the tool will resolve to. Hardcoding an id
        here would seed rows the tool then looks straight past, and the test would pass its
        never-collected assertion for entirely the wrong reason.
    */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionLogToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-collog-" + Guid.NewGuid().ToString("N"));
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
    public async Task EmptyWindow_AndNeverCollected_AreDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        /* Registered, never collected: a fault, and it must not be described as an empty window. */
        var never = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 200);
        var neverRoot = JsonDocument.Parse(never).RootElement;
        Assert.Equal("unavailable", neverRoot.GetProperty("status").GetString());
        var neverText = neverRoot.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", neverText, StringComparison.OrdinalIgnoreCase);

        /* Collected, but outside the asked-for window: a true negative, and widening IS the move. */
        await SeedLogAsync("query_store", DateTime.UtcNow.AddHours(-48));

        var quiet = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 1, 200);
        var quietRoot = JsonDocument.Parse(quiet).RootElement;
        Assert.Equal("empty", quietRoot.GetProperty("status").GetString());
        var quietText = quietRoot.GetProperty("message").GetString()!;
        Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);
        Assert.Contains("widen", quietText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TruncationIsObserved_NotInferredFromTheRowCount()
    {
        var service = new LocalDataService(_duckDb);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-10));
        await SeedLogAsync("deadlocks", DateTime.UtcNow.AddMinutes(-5));

        /* Exactly the cap, nothing beyond it. Comparing count to limit reports truncated here, wrongly. */
        var exact = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 2);
        var exactRoot = JsonDocument.Parse(exact).RootElement;
        Assert.Equal(2, exactRoot.GetProperty("run_count").GetInt32());
        Assert.False(exactRoot.GetProperty("truncated").GetBoolean());

        /* Under the cap: there really is more, and it says so. */
        var capped = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 1);
        var cappedRoot = JsonDocument.Parse(capped).RootElement;
        Assert.Equal(1, cappedRoot.GetProperty("run_count").GetInt32());
        Assert.True(cappedRoot.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public async Task AnOutOfRangeCap_IsRefused_NotSilentlyClamped()
    {
        var service = new LocalDataService(_duckDb);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-10));

        var tooBig = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 5000);
        Assert.Contains("exceeds maximum of", tooBig, StringComparison.Ordinal);
        Assert.Contains("1000", tooBig, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ThePayloadSplitsDuration_AndNamesTheStoreFieldTheWayDarlingDoes()
    {
        var service = new LocalDataService(_duckDb);
        await SeedLogAsync("query_store", DateTime.UtcNow.AddMinutes(-10));

        var hit = await McpHealthTools.GetCollectionLog(service, _serverManager, ServerName, 24, 200);
        var run = JsonDocument.Parse(hit).RootElement.GetProperty("runs")[0];

        Assert.Equal(80, run.GetProperty("sql_duration_ms").GetInt32());

        /* Lite's column is duckdb_duration_ms. The FIELD is store_duration_ms on both SKUs, because the
           storage engine differs and the question the caller is asking does not. */
        Assert.Equal(20, run.GetProperty("store_duration_ms").GetInt32());
        Assert.False(run.TryGetProperty("duckdb_duration_ms", out _));
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

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }
}

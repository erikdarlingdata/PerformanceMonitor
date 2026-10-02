/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
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
/// #4198's per-server pass, Lite's twin of <c>Darling.Tests/McpCollectionLogPerServerResponseBudgetLivePostgresTests</c>:
/// <c>get_collection_log</c>'s ONE-SERVER form at DEFAULT arguments stays under
/// <see cref="McpResponseBudget.DefaultBytes"/> on a store with more rows than the new default row count.
/// No rig: Lite's store is local DuckDB (<see cref="SharedDuckDbFixture"/>).
/// </summary>
public sealed class CollectionLogPerServerResponseBudgetToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "PerServerBudgetSrv";

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionLogPerServerResponseBudgetToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-collogperserver-" + Guid.NewGuid().ToString("N"));
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
    public async Task PerServerDefaultCall_StaysUnderTheResponseBudget()
    {
        var service = new LocalDataService(_duckDb);

        for (var i = 0; i < 200; i++)
        {
            var when = DateTime.UtcNow.AddMinutes(-(i + 1));
            /* One in ten carries a moderate error_message, the same mix as Darling's twin. */
            var error = i % 10 == 0
                ? "Timeout expired. The timeout period elapsed prior to completion of the operation or the monitored-server round trip is not responding."
                : null;
            await SeedLogAsync(_serverId, ServerName, i % 3 == 0 ? "query_store" : "wait_stats", when, 100 + i, error);
        }

        var json = await McpHealthTools.GetCollectionLog(service, _serverManager, server_name: ServerName, hours_back: 24);
        var bytes = Encoding.UTF8.GetByteCount(json);
        Assert.True(bytes <= McpResponseBudget.DefaultBytes,
            $"Per-server default call was {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
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

    private async Task SeedLogAsync(int serverId, string serverName, string collector, DateTime collectionTimeUtc, double durationMs, string? errorMessage)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = errorMessage is null ? "SUCCESS" : "ERROR" });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)errorMessage ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.8 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.2 });
        await cmd.ExecuteNonQueryAsync();
    }
}

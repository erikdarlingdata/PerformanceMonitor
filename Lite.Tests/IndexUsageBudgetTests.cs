/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
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
/// #4198, Lite parity for <c>Darling.Tests/IndexUsageBudgetLiveTests</c>: <c>get_index_usage</c> measured
/// 69,290 bytes at default arguments on a busy production store (200 rows, <c>IndexUsageTop</c>'s old default,
/// #2636's <c>limit</c> parameter left at its hardcoded-era value) -- more than double
/// <see cref="McpResponseBudget.DefaultBytes"/>. Not on #4224's <c>McpReadToolBudgetLiveTests</c> roster and
/// that fixture doesn't seed <c>index_object_stats</c>, so this file seeds it directly on Lite's shared DuckDB
/// fixture (no rig needed) and measures the same call.
/// </summary>
public sealed class IndexUsageBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "IndexUsageBudgetSrv";
    private const int SeededRowCount = 200;
    private const int DatabaseCount = 10;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly DateTime _startTime = DateTime.UtcNow.AddDays(-10);
    private DuckDBConnection? _seedConn;
    private long _nextId = -1;

    public IndexUsageBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-indexusagebudget-" + Guid.NewGuid().ToString("N"));
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

    /// <summary>
    /// The regression pin: a default call (no <c>limit</c> passed) stays under
    /// <see cref="McpResponseBudget.DefaultBytes"/>, is marked <c>truncated</c>, and an explicit <c>limit</c>
    /// covering the whole seeded set still gets every row.
    /// </summary>
    [Fact]
    public async Task DefaultCallStaysUnderBudget_AndAnExplicitLimitStillGetsEveryRow()
    {
        await SeedAsync();

        var defaultJson = await McpObjectStatsTools.GetIndexUsage(_dataService, _serverManager, ServerName);
        var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"get_index_usage, default args, {SeededRowCount} rows seeded: {defaultBytes:N0} bytes, budget={McpResponseBudget.DefaultBytes:N0} bytes.");
        Assert.True(defaultBytes <= McpResponseBudget.DefaultBytes,
            $"get_index_usage default call was {defaultBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        var defaultRoot = JsonDocument.Parse(defaultJson).RootElement;
        var returned = defaultRoot.GetProperty("returned_index_count").GetInt32();
        Assert.True(returned < SeededRowCount,
            $"expected the default page to be narrower than the seeded {SeededRowCount} rows; got {returned}.");
        Assert.True(defaultRoot.GetProperty("truncated").GetBoolean());
        Assert.Equal(SeededRowCount, defaultRoot.GetProperty("matching_index_count").GetInt32());
        Assert.Equal(returned, defaultRoot.GetProperty("indexes").GetArrayLength());

        var fullJson = await McpObjectStatsTools.GetIndexUsage(_dataService, _serverManager, ServerName, limit: SeededRowCount);
        var fullRoot = JsonDocument.Parse(fullJson).RootElement;
        Assert.Equal(SeededRowCount, fullRoot.GetProperty("returned_index_count").GetInt32());
        Assert.False(fullRoot.GetProperty("truncated").GetBoolean());
        Assert.Equal(SeededRowCount, fullRoot.GetProperty("indexes").GetArrayLength());
    }

    /// <summary>200 rows across 10 databases, table/index names sized like a real composite-index schema, all
    /// sharing one capture so "latest snapshot" keeps every row.</summary>
    private async Task SeedAsync()
    {
        var capture = DateTime.UtcNow;
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        for (var i = 0; i < SeededRowCount; i++)
        {
            var dbIndex = i % DatabaseCount;
            var databaseName = $"TenantDb{dbIndex:D2}";
            var tableName = $"OrderLineItems{(i % 30):D2}";
            var indexName = $"IX_OrderLineItems{(i % 30):D2}_TenantId_OrderId";
            var indexType = i % 5 == 0 ? "CLUSTERED" : "NONCLUSTERED";
            var reservedMb = 100m + i * 7.31m;
            var totalRows = 10_000L + i * 997L;
            var seeks = i % 3 == 0 ? 0L : 500L + i * 11L;
            var scans = i % 3 == 0 ? 0L : 10L + i % 40L;
            var lookups = i % 3 == 0 ? 0L : 5L + i % 20L;
            var updates = i % 3 == 1 ? 0L : 50L + i % 90L;

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO index_object_stats
                (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
                 schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                 user_seeks, user_scans, user_lookups, user_updates)
                VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,$9,$10,$11,$12,$12,$13,$14,$15,$16,$17)";
            void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
            P(_nextId--); P(capture); P(_serverId); P(ServerName); P(_startTime); P(databaseName);
            P(1000 + i); P(tableName); P(1 + i % 3); P(indexName); P(indexType); P(reservedMb); P(totalRows);
            P(seeks); P(scans); P(lookups); P(updates);
            await cmd.ExecuteNonQueryAsync();
        }
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
}

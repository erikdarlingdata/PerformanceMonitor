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
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4198, Lite twin of Darling's CollectionHealthPayloadBudgetLiveTests: get_collection_health has no row to
/// drop (every collector is one row, and a health read must never hide a failing/stale/disabled/erroring one),
/// so the default-size cut is per-field. Seeds every SQL Server catalog collector, mostly boring-healthy plus
/// four rows that must never compact, and measures McpHealthTools.GetCollectionHealth's own UTF-8 bytes.
/// </summary>
public sealed class CollectionHealthPayloadBudgetToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "CollHealthBudgetSrv";
    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthPayloadBudgetToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-collhealthbudget-" + Guid.NewGuid().ToString("N"));
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
    public async Task DefaultCall_StaysUnderBudget_AndNeverCompactsANonBoringCollector()
    {
        var service = new LocalDataService(_duckDb);
        var now = DateTime.UtcNow;
        var sqlServerCollectors = CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer)
            .Select(d => d.Name)
            .ToArray();
        var neverCompact = new[] { "wait_stats", "memory_grant_stats", "query_store_health", "database_scoped_config" };

        foreach (var name in sqlServerCollectors)
        {
            switch (name)
            {
                case "wait_stats":
                    for (var i = 0; i < 5; i++)
                        await SeedLogAsync(name, now.AddHours(-i * 6), "ERROR", 120, null, "Login failed for user 'darling_monitor'.");
                    break;
                case "memory_grant_stats":
                    for (var i = 0; i < 7; i++)
                        await SeedLogAsync(name, now.AddHours(-i * 20 - 1), "SUCCESS", 80, 40, null);
                    for (var i = 0; i < 3; i++)
                        await SeedLogAsync(name, now.AddHours(-i * 30 - 2), "ERROR", 90, null, "Timeout expired.");
                    break;
                case "query_store_health":
                    await SeedLogAsync(name, now.AddDays(-6), "PERMISSIONS", 50, null, "permission denied for function pg_read_file");
                    await SeedLogAsync(name, now.AddDays(-6).AddHours(-1), "PERMISSIONS", 50, null, "permission denied for function pg_read_file");
                    for (var i = 0; i < 4; i++)
                        await SeedLogAsync(name, now.AddHours(-i * 12), "SUCCESS", 60, 12, null);
                    break;
                case "database_scoped_config":
                    for (var i = 0; i < 8; i++)
                        await SeedLogAsync(name, now.AddHours(-i * 18), "SUCCESS", 30, 0, null);
                    break;
                case "deadlocks":
                    for (var i = 0; i < 8; i++)
                        await SeedLogAsync(name, now.AddHours(-i * 18), "SUCCESS", 15, 0, null);
                    break;
                default:
                    for (var i = 0; i < 6; i++)
                        await SeedLogAsync(name, now.AddHours(-i * 24 - 1), "SUCCESS", 100 + i * 15, 50 + i * 5, null);
                    break;
            }
        }

        var defaultJson = await McpHealthTools.GetCollectionHealth(service, _serverManager, ServerName);
        var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
        var fullJson = await McpHealthTools.GetCollectionHealth(service, _serverManager, ServerName, full_detail: true);
        var fullBytes = Encoding.UTF8.GetByteCount(fullJson);

        Assert.True(defaultBytes <= McpResponseBudget.DefaultBytes,
            $"default get_collection_health is {defaultBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
        Assert.True(defaultBytes < fullBytes);

        using var defaultDoc = JsonDocument.Parse(defaultJson);
        var defaultRows = defaultDoc.RootElement.GetProperty("collectors").EnumerateArray()
            .ToDictionary(r => r.GetProperty("collector").GetString()!, r => r);
        Assert.Equal(sqlServerCollectors.Length, defaultRows.Count);

        foreach (var name in neverCompact)
        {
            Assert.True(defaultRows[name].TryGetProperty("errors", out _), $"{name} must keep full detail by default.");
            Assert.False(defaultRows[name].TryGetProperty("compact", out _));
        }
        Assert.True(defaultRows["deadlocks"].TryGetProperty("compact", out var deadlocksCompact) && deadlocksCompact.GetBoolean());

        using var fullDoc = JsonDocument.Parse(fullJson);
        Assert.All(fullDoc.RootElement.GetProperty("collectors").EnumerateArray(),
            r => Assert.False(r.TryGetProperty("compact", out _)));
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

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string status, double durationMs, int? rowsCollected, string? errorMessage)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)errorMessage ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rowsCollected ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.8 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.2 });
        await cmd.ExecuteNonQueryAsync();
    }
}

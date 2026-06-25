/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// Behavioral tests for the structured non-data envelope (McpHelpers.Status) added to Lite's MCP
/// tools. These invoke the real tool methods against a seeded temp DuckDB + a real ServerManager,
/// asserting that a legitimate miss is machine-readable: an uncollected perfmon counter returns
/// "not_collected" with the list of counters that ARE collected, while a genuine absence (no
/// deadlocks) returns "empty". Successful, data-bearing results are intentionally not exercised
/// here — this change does not touch them.
/// </summary>
public class McpStatusEnvelopeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;

    public McpStatusEnvelopeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpStatusTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);

        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _dataService = new LocalDataService(_duckDb);

        /* A real ServerManager with one enabled, Windows-auth server. Windows auth means AddServer
           never touches the credential store, so no DPAPI / OS keychain side effects in the test. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);

        /* The server_id the tools resolve to — same derivation ServerResolver uses. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    // ── helpers ──

    private async Task SeedPerfmonCountersAsync(params string[] counterNames)
    {
        await _duckDb.InitializeAsync();

        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();

        foreach (var name in counterNames)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO perfmon_stats
                (collection_id, collection_time, server_id, server_name, object_name, counter_name,
                 instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
                VALUES ($1, $2, $3, 'TestServer', 'SQLServer:Buffer Manager', $4, '', 100, 5, 15)";
            void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
            P(_nextId--);
            P(DateTime.UtcNow.AddMinutes(-5));
            P(_serverId);
            P(name);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task SeedWaitStatsAsync(params string[] waitTypes)
    {
        await _duckDb.InitializeAsync();

        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();

        foreach (var wt in waitTypes)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO wait_stats
                (collection_id, collection_time, server_id, server_name, wait_type,
                 waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
                 delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
                VALUES ($1, $2, $3, 'TestServer', $4, 10, 1000, 100, 5, 500, 50)";
            void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
            P(_nextId--);
            P(DateTime.UtcNow.AddMinutes(-5));
            P(_serverId);
            P(wt);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static List<string> CollectedCounters(JsonElement root) =>
        root.GetProperty("hints").GetProperty("collected_counters")
            .EnumerateArray().Select(e => e.GetString() ?? "").ToList();

    // ── tests ──

    [Fact]
    public async Task GetPerfmonTrend_UncollectedCounter_ReturnsNotCollectedWithCollectedList()
    {
        await SeedPerfmonCountersAsync("Batch Requests/sec", "SQL Compilations/sec");

        var json = await McpPerfmonTools.GetPerfmonTrend(_dataService, _serverManager, "Buffer cache hit ratio");
        var root = Parse(json);

        Assert.Equal("not_collected", root.GetProperty("status").GetString());

        var collected = CollectedCounters(root);
        Assert.Contains("Batch Requests/sec", collected);
        Assert.Contains("SQL Compilations/sec", collected);
    }

    [Fact]
    public async Task GetPerfmonTrend_PageLifeExpectancy_ReturnsNotCollectedWithLegacyNote()
    {
        await SeedPerfmonCountersAsync("Batch Requests/sec");

        var json = await McpPerfmonTools.GetPerfmonTrend(_dataService, _serverManager, "Page life expectancy");
        var root = Parse(json);

        Assert.Equal("not_collected", root.GetProperty("status").GetString());

        var message = root.GetProperty("message").GetString() ?? "";
        Assert.Contains("legacy", message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("get_memory_stats", message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GetPerfmonTrend_NoCountersCollectedAtAll_ReturnsUnavailable()
    {
        await _duckDb.InitializeAsync(); // schema + views exist, but no perfmon rows seeded

        var json = await McpPerfmonTools.GetPerfmonTrend(_dataService, _serverManager, "Batch Requests/sec");
        var root = Parse(json);

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
    }

    [Fact]
    public async Task GetWaitTrend_UncollectedWaitType_ReturnsNotCollectedWithCollectedList()
    {
        await SeedWaitStatsAsync("CXPACKET", "PAGEIOLATCH_SH");

        var json = await McpWaitTools.GetWaitTrend(_dataService, _serverManager, "SOS_SCHEDULER_YIELD");
        var root = Parse(json);

        Assert.Equal("not_collected", root.GetProperty("status").GetString());

        var collected = root.GetProperty("hints").GetProperty("collected_wait_types")
            .EnumerateArray().Select(e => e.GetString() ?? "").ToList();
        Assert.Contains("CXPACKET", collected);
        Assert.Contains("PAGEIOLATCH_SH", collected);
    }

    [Fact]
    public async Task GetDeadlocks_NoDeadlocks_ReturnsEmpty()
    {
        await _duckDb.InitializeAsync(); // no deadlock rows seeded => a true negative

        var json = await McpBlockingTools.GetDeadlocks(_dataService, _serverManager);
        var root = Parse(json);

        Assert.Equal("empty", root.GetProperty("status").GetString());
    }
}

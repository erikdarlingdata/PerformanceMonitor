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
public class McpStatusEnvelopeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public McpStatusEnvelopeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        /* The temp dir stays test-local for the ServerManager's config directory;
           only the database is shared through the class fixture. */
        _tempDir = Path.Combine(Path.GetTempPath(), "McpStatusTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);

        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

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
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    // ── helpers ──

    private async Task SeedPerfmonCountersAsync(params string[] counterNames)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

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
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

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
        // Schema + views come from the class fixture; no perfmon rows seeded.

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
        // Schema comes from the class fixture; no deadlock rows seeded => a true negative.

        var json = await McpBlockingTools.GetDeadlocks(_dataService, _serverManager);
        var root = Parse(json);

        Assert.Equal("empty", root.GetProperty("status").GetString());
    }

    /// <summary>
    /// #2546, Lite's side, through the real tool: the deadlock table is empty in BOTH states, and the only
    /// thing that differs is what the last <c>running_jobs</c> run recorded. That single difference has to
    /// turn "no running SQL Agent jobs found" — an affirmative claim about the server's Agent — into "we
    /// were denied msdb, here is the grant".
    /// </summary>
    [Fact]
    public async Task GetRunningJobs_AfterADeniedRun_ReturnsPreconditionNamingTheDenial()
    {
        await SeedCollectionLogAsync("running_jobs", "PERMISSIONS",
            "The server principal is not able to access the database \"msdb\" under the current security context.");

        var root = Parse(await McpJobTools.GetRunningJobs(_dataService, _serverManager));

        Assert.Equal("precondition", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("msdb", message, StringComparison.Ordinal);
        Assert.Contains("the SQL Agent running-job snapshot", message, StringComparison.Ordinal);
        Assert.Contains("re-derives it on EVERY call", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The other direction, and the one that keeps the branch from becoming a blanket rule: a collector that
    /// ran and found nothing keeps the read's own <c>empty</c>. Without this, a precondition answer that had
    /// stopped distinguishing anything would still pass the assertion above.
    /// </summary>
    [Fact]
    public async Task GetRunningJobs_AfterASuccessfulRun_KeepsItsEmptyMiss()
    {
        await SeedCollectionLogAsync("running_jobs", "SUCCESS", null);

        var root = Parse(await McpJobTools.GetRunningJobs(_dataService, _serverManager));

        Assert.Equal("empty", root.GetProperty("status").GetString());
    }

    /// <summary>
    /// The Query Store half, whose evidence is a collected SNAPSHOT rather than a log status. Both SKUs used
    /// to say "Query Store may not be enabled on target databases" — a guess, and equally true of a server
    /// where it IS enabled. The hourly health collector has recorded the answer all along.
    /// </summary>
    [Fact]
    public async Task GetQueryStoreTop_WithQueryStoreOff_StatesItFromTheSnapshot()
    {
        await SeedQueryStoreHealthAsync("OffDb", "OFF");

        var root = Parse(await McpQueryTools.GetQueryStoreTop(_dataService, _serverManager, database_name: "OffDb"));

        Assert.Equal("precondition", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("OffDb", message, StringComparison.Ordinal);
        Assert.Contains("SET QUERY_STORE = ON", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Scope is part of the evidence: a read narrowed to a database whose Query Store IS collecting must keep
    /// its own miss, or the branch would claim a precondition about every empty window on the server.
    /// </summary>
    [Fact]
    public async Task GetQueryStoreTop_WithQueryStoreOn_KeepsItsUnavailableMiss()
    {
        await SeedQueryStoreHealthAsync("OnDb", "READ_WRITE");

        var root = Parse(await McpQueryTools.GetQueryStoreTop(_dataService, _serverManager, database_name: "OnDb"));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
    }

    private async Task SeedCollectionLogAsync(string collectorName, string status, string? errorMessage)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO collection_log
            (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status,
             error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
            VALUES ($1, $2, 'TestServer', $3, $4, 0, $5, $6, 0, 0, 0)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(_nextId--);
        P(_serverId);
        P(collectorName);
        P(DateTime.UtcNow.AddMinutes(-1));
        P(status);
        P(errorMessage);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStoreHealthAsync(string databaseName, string actualState)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_health
            (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state,
             readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode,
             stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 'READ_WRITE', 0, 0, 1000, 'AUTO', 30, 200, 60)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(DateTime.UtcNow.AddMinutes(-5));
        P(_serverId);
        P(databaseName);
        P(actualState);
        await cmd.ExecuteNonQueryAsync();
    }
}

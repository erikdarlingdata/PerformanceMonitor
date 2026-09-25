/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
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
/// #4231: Lite's twin of Darling's #2364. <c>query_stats</c>, <c>procedure_stats</c> and
/// <c>query_store_stats</c> are raw-only (no rollup fallback), and Lite's default 30-day
/// <c>retention_days</c> is per-collector and user-settable — lower it, or run a young install, and a
/// "Last 7 days" ask can be served from far less. Pins the shared floor helper
/// (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the three MCP tools' disclosure.
/// Own <see cref="DuckDbInitializer"/> per test (not <c>SharedDuckDbFixture</c>) because the archive
/// tests need control of the database's archive directory, to COPY hot rows out to parquet exactly like
/// <c>ArchiveViewDedupTests</c> does.
/// </summary>
public sealed class QueryWindowTruncationTests : IDisposable
{
    private readonly int ServerId;
    private readonly string _tempDir;
    private readonly string _archivePath;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    public QueryWindowTruncationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryWindowTrunc_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _archivePath = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archivePath);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        ServerId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private async Task<DuckDBConnection> OpenSeedConnectionAsync()
    {
        var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        return connection;
    }

    private async Task SeedQueryStatsAsync(DuckDBConnection connection, DateTime collected, string queryHash)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xH" + queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT " + queryHash });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureStatsAsync(DuckDBConnection connection, DateTime collected, string objectName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     schema_name, object_name, object_type, last_execution_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "dbo" });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SQL_STORED_PROCEDURE" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStoreStatsAsync(DuckDBConnection connection, DateTime collected, long queryId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     module_name, query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId * 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Adhoc" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT " + queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xQ" + queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xP" + queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = false });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task GetTopQueriesByCpu_ReportsTruncation_WhenRawStartsAfterTheWindow()
    {
        await _duckDb.InitializeAsync();
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedQueryStatsAsync(connection, collected, "0xTRUNC");

        var json = await McpQueryTools.GetTopQueriesByCpu(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 168);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStart = DateTime.Parse(root.GetProperty("effective_start").GetString()!).ToUniversalTime();
        Assert.True(Math.Abs((effectiveStart - collected.ToUniversalTime()).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should track the seeded floor {collected:o}");
        Assert.InRange(root.GetProperty("effective_hours_back").GetDouble(), 46, 50);
        Assert.NotEqual(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    [Fact]
    public async Task GetTopProceduresByCpu_ReportsTruncation_WhenRawStartsAfterTheWindow()
    {
        await _duckDb.InitializeAsync();
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedProcedureStatsAsync(connection, collected, "usp_Trunc");

        var json = await McpQueryTools.GetTopProceduresByCpu(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 168);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStart = DateTime.Parse(root.GetProperty("effective_start").GetString()!).ToUniversalTime();
        Assert.True(Math.Abs((effectiveStart - collected.ToUniversalTime()).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should track the seeded floor {collected:o}");
        Assert.NotEqual(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    [Fact]
    public async Task GetQueryStoreTop_ReportsTruncation_WhenRawStartsAfterTheWindow()
    {
        await _duckDb.InitializeAsync();
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedQueryStoreStatsAsync(connection, collected, 900001);

        var json = await McpQueryTools.GetQueryStoreTop(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 168);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStart = DateTime.Parse(root.GetProperty("effective_start").GetString()!).ToUniversalTime();
        Assert.True(Math.Abs((effectiveStart - collected.ToUniversalTime()).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should track the seeded floor {collected:o}");
        Assert.NotEqual(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    /// <summary>
    /// #4231 ruling: "a floor inside the slack shows no note." The seeded floor sits 60 minutes after the
    /// requested start — inside McpQueryTools.TruncationSlack's 90-minute allowance — so this is a normal raw
    /// series opening a collection cadence or two late, not a retention cut.
    /// </summary>
    [Fact]
    public async Task GetTopQueriesByCpu_NoNote_WhenFloorIsInsideTheSlack()
    {
        await _duckDb.InitializeAsync();
        var requestedStart = DateTime.UtcNow.AddHours(-24);
        var collected = DateTime.SpecifyKind(requestedStart.AddMinutes(60), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedQueryStatsAsync(connection, collected, "0xINSLACK");

        var json = await McpQueryTools.GetTopQueriesByCpu(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 24);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    /// <summary>
    /// Ruling 1: the floor "comes from that view" when the grid/tool reads a view over the hot table and the
    /// archived parquet files — never just the hot table. Archives an OLDER set to parquet (mirroring
    /// ArchiveViewDedupTests' staging), keeps a NEWER set in the hot table, and confirms the probe returns the
    /// archived (older) floor. Also times the probe, per the #4231 ruling to measure it on a store that has
    /// archived files.
    /// </summary>
    [Fact]
    public async Task FloorHelper_ReadsTheArchivedFloor_NotJustTheHotTable()
    {
        await _duckDb.InitializeAsync();
        var archivedFloor = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-6), DateTimeKind.Unspecified);
        var hotStart = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-12), DateTimeKind.Unspecified);

        using (var connection = await OpenSeedConnectionAsync())
        {
            for (var i = 0; i < 500; i++)
                await SeedQueryStatsAsync(connection, archivedFloor.AddMinutes(i), $"0xARCH{i}");

            var parquetPath = Path.Combine(_archivePath, "20260101_0000_query_stats.parquet").Replace("\\", "/");
            using (var readLock = _duckDb.AcquireReadLock())
            using (var copyCmd = connection.CreateCommand())
            {
                copyCmd.CommandText = $"COPY query_stats TO '{parquetPath}' (FORMAT PARQUET)";
                await copyCmd.ExecuteNonQueryAsync();
            }
            using (var readLock = _duckDb.AcquireReadLock())
            using (var deleteCmd = connection.CreateCommand())
            {
                deleteCmd.CommandText = "DELETE FROM query_stats";
                await deleteCmd.ExecuteNonQueryAsync();
            }

            for (var i = 0; i < 500; i++)
                await SeedQueryStatsAsync(connection, hotStart.AddMinutes(i), $"0xHOT{i}");
        }

        await _duckDb.CreateArchiveViewsAsync();

        var service = new LocalDataService(_duckDb);
        var requestedStart = DateTime.UtcNow.AddDays(-7);
        var windowEnd = DateTime.UtcNow;

        var stopwatch = Stopwatch.StartNew();
        var floor = await service.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);
        stopwatch.Stop();
        Console.WriteLine($"#4231 GetQueryWindowFloorAsync (500 hot + 500 archived rows): {stopwatch.ElapsedMilliseconds} ms");

        Assert.NotNull(floor);
        Assert.True(Math.Abs((floor!.Value - archivedFloor).TotalMinutes) < 2,
            $"floor {floor:o} should be the ARCHIVED start {archivedFloor:o}, not the hot table's {hotStart:o}");
    }
}

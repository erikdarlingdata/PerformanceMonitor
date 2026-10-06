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
/// #5244 (PR4, lane L1): <c>database_name</c> on Lite's get_query_duration_trend, get_procedure_duration_trend and
/// get_query_store_duration_trend, the twins of Darling's list-taking trend readers. Each tool gets a DuckDB fixture
/// with three databases where the filter returns only the chosen one's rate and a blank name returns all of them, plus
/// the filtered empty answer: it names the database the way Darling's <c>ScopedServer</c> does and keeps the
/// <c>empty</c> status word for a collector that ran, while the server-wide never-sampled answer stays
/// <c>unavailable</c> on the bare server name.
/// </summary>
public sealed class DurationTrendDatabaseFilterToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TrendDbFilterSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly LocalDataService _service;
    private DuckDBConnection? _seedConn;
    private long _nextId = 940000;

    public DurationTrendDatabaseFilterToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-trenddbfilter-" + Guid.NewGuid().ToString("N"));
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

    private static DateTime Minute(int minutesAgo)
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc).AddMinutes(-minutesAgo);
    }

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    /// <summary>The summed executions_per_second of every point in the answer.</summary>
    private static double ExecutionsPerSecond(JsonElement root) =>
        root.GetProperty("trend").EnumerateArray().Sum(p => p.GetProperty("executions_per_second").GetDouble());

    /* ───────────────────────── get_query_duration_trend ───────────────────────── */

    /// <summary>One collection per database at the same minute, each carrying a stored 60 s interval: 60, 120 and 240
    /// executions are 1, 2 and 4 per second, so the rate says exactly which databases were summed.</summary>
    private async Task SeedThreeDatabaseQueryStatsAsync()
    {
        var when = Minute(10);
        await SeedQueryStatsAsync(when, "DbA", 60);
        await SeedQueryStatsAsync(when, "DbB", 120);
        await SeedQueryStatsAsync(when, "DbC", 240);
    }

    [Fact]
    public async Task QueryDurationTrend_OneName_ReturnsOnlyThatDatabasesRate()
    {
        await SeedThreeDatabaseQueryStatsAsync();

        var root = Parse(await McpQueryTools.GetQueryDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: "DbB"));

        Assert.Equal(2.0, ExecutionsPerSecond(root), 3);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task QueryDurationTrend_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabaseQueryStatsAsync();

        var root = Parse(await McpQueryTools.GetQueryDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: blank));

        Assert.Equal(7.0, ExecutionsPerSecond(root), 3);
    }

    [Fact]
    public async Task QueryDurationTrend_FilteredEmpty_NamesTheDatabase_AndStaysEmpty()
    {
        await SeedThreeDatabaseQueryStatsAsync();

        var root = Parse(await McpQueryTools.GetQueryDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains($"{ServerName} (database_name 'NoSuchDb')", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task QueryDurationTrend_NeverCollected_IsUnavailable_OnTheBareServerName_EvenWithAFilter()
    {
        var root = Parse(await McpQueryTools.GetQueryDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: "DbA"));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.DoesNotContain("database_name", root.GetProperty("message").GetString());
    }

    /* ───────────────────────── get_procedure_duration_trend ───────────────────────── */

    private async Task SeedThreeDatabaseProcedureStatsAsync()
    {
        var when = Minute(10);
        await SeedProcedureStatsAsync(when, "DbA", 60);
        await SeedProcedureStatsAsync(when, "DbB", 120);
        await SeedProcedureStatsAsync(when, "DbC", 240);
    }

    [Fact]
    public async Task ProcedureDurationTrend_OneName_ReturnsOnlyThatDatabasesRate()
    {
        await SeedThreeDatabaseProcedureStatsAsync();

        var root = Parse(await McpQueryTools.GetProcedureDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: "DbC"));

        Assert.Equal(4.0, ExecutionsPerSecond(root), 3);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ProcedureDurationTrend_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabaseProcedureStatsAsync();

        var root = Parse(await McpQueryTools.GetProcedureDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: blank));

        Assert.Equal(7.0, ExecutionsPerSecond(root), 3);
    }

    [Fact]
    public async Task ProcedureDurationTrend_FilteredEmpty_NamesTheDatabase_AndStaysEmpty()
    {
        await SeedThreeDatabaseProcedureStatsAsync();

        var root = Parse(await McpQueryTools.GetProcedureDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains($"{ServerName} (database_name 'NoSuchDb')", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task ProcedureDurationTrend_NeverCollected_IsUnavailable_OnTheBareServerName_EvenWithAFilter()
    {
        var root = Parse(await McpQueryTools.GetProcedureDurationTrend(
            _service, _serverManager, ServerName, 4, database_name: "DbA"));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.DoesNotContain("database_name", root.GetProperty("message").GetString());
    }

    /* ───────────────────────── get_query_store_duration_trend ───────────────────────── */

    /// <summary>One closed hour per database at the same interval start: 3,600 s each, so 3,600 / 7,200 / 14,400
    /// executions are 1, 2 and 4 per second.</summary>
    private async Task SeedThreeDatabaseQueryStoreAsync()
    {
        var start = Minute(180);
        var hour = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0, DateTimeKind.Utc);
        await SeedQueryStoreAsync(hour, "DbA", 3_600);
        await SeedQueryStoreAsync(hour, "DbB", 7_200);
        await SeedQueryStoreAsync(hour, "DbC", 14_400);
    }

    [Fact]
    public async Task QueryStoreDurationTrend_OneName_ReturnsOnlyThatDatabasesRate()
    {
        await SeedThreeDatabaseQueryStoreAsync();

        var root = Parse(await McpQueryTools.GetQueryStoreDurationTrend(
            _service, _serverManager, ServerName, 6, database_name: "DbB"));

        Assert.Equal(2.0, ExecutionsPerSecond(root), 3);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task QueryStoreDurationTrend_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabaseQueryStoreAsync();

        var root = Parse(await McpQueryTools.GetQueryStoreDurationTrend(
            _service, _serverManager, ServerName, 6, database_name: blank));

        Assert.Equal(7.0, ExecutionsPerSecond(root), 3);
    }

    [Fact]
    public async Task QueryStoreDurationTrend_FilteredEmpty_NamesTheDatabase_AndStaysEmpty()
    {
        await SeedThreeDatabaseQueryStoreAsync();

        var root = Parse(await McpQueryTools.GetQueryStoreDurationTrend(
            _service, _serverManager, ServerName, 6, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains($"{ServerName} (database_name 'NoSuchDb')", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task QueryStoreDurationTrend_NeverCollected_IsUnavailable_OnTheBareServerName_EvenWithAFilter()
    {
        var root = Parse(await McpQueryTools.GetQueryStoreDurationTrend(
            _service, _serverManager, ServerName, 6, database_name: "DbA"));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.DoesNotContain("database_name", root.GetProperty("message").GetString());
    }

    /* ───────────────────────── seeding ───────────────────────── */

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedQueryStatsAsync(DateTime collectionTimeUtc, string database, long executions)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 60)";
        var naive = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xTRENDDBFILTER" + database });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xSQLH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 1000 });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 2000 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 1" });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureStatsAsync(DateTime collectionTimeUtc, string database, long executions)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, schema_name, object_name,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'dbo', 'usp_Trend', $6, $7, $8, 0, 60)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 1000 });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions * 2000 });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>A closed hour: it starts at <paramref name="hour"/>, ends an hour later, and was fetched when it
    /// closed, so its rate is its executions over 3,600 s.</summary>
    private async Task SeedQueryStoreAsync(DateTime hour, string database, long executions)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, execution_count, avg_duration_us,
     runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, 1000, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(hour.AddMinutes(61), DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = 7L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 9L });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(hour, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(hour.AddHours(1), DateTimeKind.Unspecified) });
        await cmd.ExecuteNonQueryAsync();
    }
}

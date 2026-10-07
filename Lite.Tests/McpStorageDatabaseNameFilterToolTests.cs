/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
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
/// #5244 (PR5, Lite twins): <c>database_name</c>, appended LAST, on <c>get_database_sizes</c>, <c>get_table_index_sizes</c>,
/// <c>get_pvs_stats</c> and <c>get_file_io_stats</c>. Each tool is read through a real DuckDB store holding three databases (A, B, C):
/// the chosen name returns only that database's rows, an omitted, empty or whitespace name returns every database (a blank is "no
/// filter", never an error), the newest snapshot never moves with the filter, the cap follows the filter, and a name nothing was
/// recorded for is an <c>empty</c> status that names the database (Darling's words) while a server with no data at all keeps its
/// <c>unavailable</c> answer.
/// </summary>
public sealed class McpStorageDatabaseNameFilterToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "StorageNameFilterSrv";

    private static readonly string?[] BlankNames = [null, "", "   "];

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public McpStorageDatabaseNameFilterToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-storage-name-filter-" + Guid.NewGuid().ToString("N"));
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

    // ── get_database_sizes ──

    [Fact]
    public async Task DatabaseSizes_DatabaseName_LimitsTheFilesToThatDatabaseAtTheSameSnapshot()
    {
        var older = Truncate(DateTime.UtcNow.AddHours(-3));
        var newest = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedDatabaseSizeAsync("DbC", older);
        foreach (var db in new[] { "DbA", "DbB", "DbC" })
            await SeedDatabaseSizeAsync(db, newest);

        var all = await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName);
        var one = await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName, database_name: "DbA");
        using var allDoc = JsonDocument.Parse(all);
        using var oneDoc = JsonDocument.Parse(one);
        Assert.Equal(3, allDoc.RootElement.GetProperty("databases").GetArrayLength());
        Assert.Equal("DbA", Assert.Single(oneDoc.RootElement.GetProperty("databases").EnumerateArray()).GetProperty("database_name").GetString());
        Assert.Equal(1, oneDoc.RootElement.GetProperty("file_count").GetInt32());
        Assert.Equal(allDoc.RootElement.GetProperty("captured_at").GetString(), oneDoc.RootElement.GetProperty("captured_at").GetString());

        foreach (var blank in BlankNames)
            Assert.Equal(all, await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName, database_name: blank));
    }

    [Fact]
    public async Task DatabaseSizes_DatabaseName_NoRowsInTheNewestSnapshotIsEmptyAndNamesTheDatabase()
    {
        /* DbC is only in an OLDER snapshot: the snapshot never moves with the filter, so it is not found. */
        await SeedDatabaseSizeAsync("DbC", Truncate(DateTime.UtcNow.AddHours(-3)));
        await SeedDatabaseSizeAsync("DbA", Truncate(DateTime.UtcNow.AddHours(-1)));

        var none = await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName, database_name: "DbC");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        using var doc = JsonDocument.Parse(none);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("for database 'DbC'", doc.RootElement.GetProperty("message").GetString());
    }

    [Fact]
    public async Task DatabaseSizes_DatabaseName_ServerWithNoSnapshotStaysUnavailable()
    {
        var none = await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName, database_name: "DbA");
        using var doc = JsonDocument.Parse(none);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
    }

    // ── get_table_index_sizes ──

    [Fact]
    public async Task TableIndexSizes_DatabaseName_LimitsTheTablesAndKeepsTheServersHistory()
    {
        /* The oldest snapshot holds only DbC, so the history block proves the span is not narrowed by the filter. */
        var oldest = Truncate(DateTime.UtcNow.AddDays(-9));
        var newest = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedTableAsync("DbC", "tc", oldest, 5m);
        foreach (var db in new[] { "DbA", "DbB", "DbC" })
            await SeedTableAsync(db, "t" + db[^1], newest, 10m + (db[^1] - 'A'));

        var all = await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName);
        var one = await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: "DbA");
        using var allDoc = JsonDocument.Parse(all);
        using var oneDoc = JsonDocument.Parse(one);
        Assert.Equal(3, allDoc.RootElement.GetProperty("tables_returned").GetInt32());
        Assert.Equal("DbA", Assert.Single(oneDoc.RootElement.GetProperty("tables").EnumerateArray()).GetProperty("database_name").GetString());
        Assert.Equal(allDoc.RootElement.GetProperty("history").GetProperty("earliest_snapshot").GetString(),
            oneDoc.RootElement.GetProperty("history").GetProperty("earliest_snapshot").GetString());
        Assert.Equal(allDoc.RootElement.GetProperty("history").GetProperty("latest_snapshot").GetString(),
            oneDoc.RootElement.GetProperty("history").GetProperty("latest_snapshot").GetString());

        foreach (var blank in BlankNames)
            Assert.Equal(all, await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: blank));
    }

    [Fact]
    public async Task TableIndexSizes_DatabaseName_TheCapFollowsTheFilter()
    {
        var newest = Truncate(DateTime.UtcNow.AddHours(-1));
        for (var i = 0; i < 105; i++)
            await SeedTableAsync("DbC", $"big{i}", newest, 1000m + i);
        await SeedTableAsync("DbA", "small", newest, 1m);

        using var all = JsonDocument.Parse(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName));
        Assert.True(all.RootElement.GetProperty("truncated").GetBoolean());
        Assert.DoesNotContain(all.RootElement.GetProperty("tables").EnumerateArray(), t => t.GetProperty("database_name").GetString() == "DbA");

        using var one = JsonDocument.Parse(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: "DbA"));
        Assert.False(one.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal("small", Assert.Single(one.RootElement.GetProperty("tables").EnumerateArray()).GetProperty("table_name").GetString());
    }

    [Fact]
    public async Task TableIndexSizes_DatabaseName_NoMatchIsEmptyAndNoDataStaysUnavailable()
    {
        var unavailable = await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: "DbA");
        using (var doc = JsonDocument.Parse(unavailable))
            Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());

        await SeedTableAsync("DbB", "tb", Truncate(DateTime.UtcNow.AddHours(-1)), 10m);
        var none = await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: "DbA");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        using var noneDoc = JsonDocument.Parse(none);
        Assert.Equal("empty", noneDoc.RootElement.GetProperty("status").GetString());
        Assert.Contains("for database 'DbA'", noneDoc.RootElement.GetProperty("message").GetString());
    }

    // ── get_pvs_stats ──

    [Fact]
    public async Task PvsStats_DatabaseName_LimitsTheRowsAndTheTrendToThatDatabase()
    {
        foreach (var minutes in new[] { 30, 5 })
            foreach (var db in new[] { "DbA", "DbB", "DbC" })
                await SeedPvsAsync(db, minutes, 10);

        var all = await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, trend_hours_back: 1);
        var one = await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, trend_hours_back: 1, database_name: "DbB");
        using var allDoc = JsonDocument.Parse(all);
        using var oneDoc = JsonDocument.Parse(one);
        Assert.Equal(3, allDoc.RootElement.GetProperty("databases").GetArrayLength());
        Assert.Equal("DbB", Assert.Single(oneDoc.RootElement.GetProperty("databases").EnumerateArray()).GetProperty("database_name").GetString());
        Assert.Equal("DbB", Assert.Single(oneDoc.RootElement.GetProperty("trend").EnumerateArray()).GetProperty("database_name").GetString());
        Assert.Equal(allDoc.RootElement.GetProperty("as_of").GetString(), oneDoc.RootElement.GetProperty("as_of").GetString());

        foreach (var blank in BlankNames)
            Assert.Equal(all, await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, trend_hours_back: 1, database_name: blank));
    }

    [Fact]
    public async Task PvsStats_DatabaseName_NoMatchIsEmptyForTheDatabaseAndNoDataKeepsTheServerSentence()
    {
        var noData = await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, database_name: "DbA");
        Assert.Contains("No PVS data collected for this server", noData);

        await SeedPvsAsync("DbB", 5, 10);
        await SeedPvsAsync("DbC", 5, 10);
        var none = await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, database_name: "DbA");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        using var doc = JsonDocument.Parse(none);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        var message = doc.RootElement.GetProperty("message").GetString();
        Assert.Contains("No PVS rows for database 'DbA'", message);
        Assert.Contains("2 other database(s)", message);
        Assert.DoesNotContain("No PVS data collected for this server", none);
    }

    // ── get_file_io_stats ──

    [Fact]
    public async Task FileIoStats_DatabaseName_LimitsTheFilesToThatDatabaseAtTheSameCapture()
    {
        await SeedFileIoAsync("DbC", "c.mdf", 30);
        foreach (var db in new[] { "DbA", "DbB", "DbC" })
            await SeedFileIoAsync(db, db + ".mdf", 5);

        var all = await McpIoTools.GetFileIoStats(_dataService, _serverManager, ServerName);
        var one = await McpIoTools.GetFileIoStats(_dataService, _serverManager, ServerName, database_name: "DbA");
        using var allDoc = JsonDocument.Parse(all);
        using var oneDoc = JsonDocument.Parse(one);
        Assert.Equal(3, allDoc.RootElement.GetProperty("files").GetArrayLength());
        var file = Assert.Single(oneDoc.RootElement.GetProperty("files").EnumerateArray());
        Assert.Equal("DbA", file.GetProperty("database_name").GetString());
        Assert.Equal("DbA", oneDoc.RootElement.GetProperty("database_name").GetString());
        Assert.Equal(allDoc.RootElement.GetProperty("captured_at").GetString(), oneDoc.RootElement.GetProperty("captured_at").GetString());

        foreach (var blank in BlankNames)
            Assert.Equal(all, await McpIoTools.GetFileIoStats(_dataService, _serverManager, ServerName, database_name: blank));
    }

    [Fact]
    public async Task FileIoStats_DatabaseName_NewestCaptureWithoutTheDatabaseIsEmptyNotUnavailable()
    {
        await SeedFileIoAsync("DbC", "c.mdf", 30);
        await SeedFileIoAsync("DbA", "a.mdf", 5);

        /* DbC was only in an older capture: the capture never moves with the filter. */
        var none = await McpIoTools.GetFileIoStats(_dataService, _serverManager, ServerName, database_name: "DbC");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        using var doc = JsonDocument.Parse(none);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("DbC", doc.RootElement.GetProperty("database_name").GetString());
        Assert.Contains("holds no files for the database DbC", none);
    }

    [Fact]
    public async Task FileIoStats_DatabaseName_NeverCollectedStaysUnavailableAndEchoesTheName()
    {
        var none = await McpIoTools.GetFileIoStats(_dataService, _serverManager, ServerName, database_name: "DbA");
        using var doc = JsonDocument.Parse(none);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("DbA", doc.RootElement.GetProperty("database_name").GetString());
    }

    [Fact]
    public async Task FileIoTrend_DatabaseName_IsNotTrimmed_AndABlankIsEveryDatabase()
    {
        foreach (var minutes in new[] { 90, 60, 30 })
            foreach (var db in new[] { "DbA", "DbB" })
                await SeedFileIoAsync(db, db + ".mdf", minutes);

        var all = await McpIoTools.GetFileIoTrend(_dataService, _serverManager, ServerName);
        foreach (var blank in BlankNames)
            Assert.Equal(all, await McpIoTools.GetFileIoTrend(_dataService, _serverManager, ServerName, database_name: blank));

        var exact = await McpIoTools.GetFileIoTrend(_dataService, _serverManager, ServerName, database_name: "DbA");
        Assert.DoesNotContain("\"status\"", exact);

        /* A padded name is a different name: it matches nothing and says so, as on Darling. */
        var padded = await McpIoTools.GetFileIoTrend(_dataService, _serverManager, ServerName, database_name: " DbA ");
        using var doc = JsonDocument.Parse(padded);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }

    /// <summary>#5244 PR5: every answer shape of the storage tools says which database it was limited to (null for every database).</summary>
    [Fact]
    public async Task TheStorageAnswers_EchoDatabaseName_OnTheDataTheEmptyAndTheNoDataShapes()
    {
        static string? Echo(string json) =>
            JsonDocument.Parse(json).RootElement.TryGetProperty("database_name", out var e) && e.ValueKind == JsonValueKind.String ? e.GetString() : "<absent>";
        static bool HasKey(string json) => JsonDocument.Parse(json).RootElement.TryGetProperty("database_name", out _);

        /* No data at all: the unavailable answers carry the key (null for every database, the name for one). */
        Assert.True(HasKey(await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName)));
        Assert.Equal("DbA", Echo(await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName, database_name: "DbA")));
        Assert.True(HasKey(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName)));
        Assert.Equal("DbA", Echo(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: "DbA")));
        Assert.True(HasKey(await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName)));
        Assert.Equal("DbA", Echo(await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, database_name: "DbA")));

        /* Data: the answer carries the echo, and an empty filtered answer names the database. */
        await SeedDatabaseSizeAsync("DbA", Truncate(DateTime.UtcNow.AddHours(-1)));
        await SeedTableAsync("DbA", "ta", Truncate(DateTime.UtcNow.AddHours(-1)), 10m);
        await SeedPvsAsync("DbA", 5, 10);
        Assert.True(HasKey(await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName)));
        Assert.Equal("DbA", Echo(await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName, database_name: "DbA")));
        Assert.Equal("DbA", Echo(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: "DbA")));
        Assert.Equal("DbA", Echo(await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, database_name: "DbA")));
        Assert.Equal("NoSuch", Echo(await McpServerInfoTools.GetDatabaseSizes(_dataService, _serverManager, ServerName, database_name: "NoSuch")));
        Assert.Equal("NoSuch", Echo(await McpObjectStatsTools.GetTableIndexSizes(_dataService, _serverManager, ServerName, database_name: "NoSuch")));
        Assert.Equal("NoSuch", Echo(await McpPvsTools.GetPvsStats(_dataService, _serverManager, ServerName, database_name: "NoSuch")));
    }

    [Fact]
    public void TheFourStorageTools_TakeDatabaseNameLast_WithTheSharedSentence()
    {
        const string Sentence = "Limit to one database. Omit for all databases.";
        foreach (var (type, method) in new[]
        {
            (typeof(McpServerInfoTools), nameof(McpServerInfoTools.GetDatabaseSizes)),
            (typeof(McpObjectStatsTools), nameof(McpObjectStatsTools.GetTableIndexSizes)),
            (typeof(McpPvsTools), nameof(McpPvsTools.GetPvsStats)),
            (typeof(McpIoTools), nameof(McpIoTools.GetFileIoStats)),
        })
        {
            var parameters = type.GetMethod(method)!.GetParameters();
            var last = parameters[^1];
            Assert.Equal("database_name", last.Name);
            Assert.Equal(Sentence, ((System.ComponentModel.DescriptionAttribute)last.GetCustomAttributes(typeof(System.ComponentModel.DescriptionAttribute), false).Single()).Description);
        }
    }

    // ── Seeding helpers (raw INSERT through one shared connection) ──

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private static DateTime Minute(int minutesAgo)
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Unspecified).AddMinutes(-minutesAgo);
    }

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedDatabaseSizeAsync(string databaseName, DateTime collectionTime) =>
        ExecAsync(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb)
VALUES ($1, $2, $3, $4, $5, 5, 1, 'ROWS', 'data', 'D:\data\x.mdf', 1024.00)",
            _nextId++, collectionTime, _serverId, ServerName, databaseName);

    private Task SeedTableAsync(string databaseName, string tableName, DateTime collectionTime, decimal reservedMb) =>
        ExecAsync(@"
INSERT INTO index_object_stats
    (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
     schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
     user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, $4, $5, $6, 7, 'dbo', 100, $7, 1, 'IX', 'NONCLUSTERED', $8, $8, 10, 0, 0, 0, 0, 0, 0)",
            _nextId++, collectionTime, _serverId, ServerName, Minute(60 * 24 * 12), databaseName, tableName, reservedMb);

    private Task SeedPvsAsync(string databaseName, int minutesAgo, double pvsMb) =>
        ExecAsync(@"
INSERT INTO pvs_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1, $2, $3, $4, $5, 5, true, $6, 1000)",
            _nextId++, Minute(minutesAgo), _serverId, ServerName, databaseName, pvsMb);

    private Task SeedFileIoAsync(string databaseName, string fileName, int minutesAgo) =>
        ExecAsync(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
     sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, 'ROWS', '', 100, 10, 10, 81920, 81920, 20, 30, 60)",
            _nextId++, Minute(minutesAgo), _serverId, ServerName, databaseName, fileName);
}

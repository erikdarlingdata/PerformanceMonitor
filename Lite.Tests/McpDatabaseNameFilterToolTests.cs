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
/// #5244 (PR6, Lite twins): <c>database_name</c>, appended LAST, on <c>get_database_config_changes</c>,
/// <c>get_default_trace_events</c> and <c>get_health_parser_severe_errors</c>. Each tool is read through a real DuckDB store holding
/// three databases: the chosen name returns only that database's rows, an omitted, empty or whitespace name returns every database
/// (a blank is "no filter", never an error), and a name nothing was recorded for is an <c>empty</c> status that names the database
/// rather than a verdict on the server. The severe-errors tool filters on the name the event's database_id resolves to (the store has
/// no name for those events), so an id the collected map lacks is in no chosen database, ordinal as on Darling's reader.
/// </summary>
public sealed class McpDatabaseNameFilterToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "DbNameFilterSrv";

    private static readonly string?[] BlankNames = [null, "", "   "];

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public McpDatabaseNameFilterToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-dbname-filter-" + Guid.NewGuid().ToString("N"));
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
    public async Task DatabaseConfigChanges_DatabaseName_LimitsTheChangesToThatDatabase()
    {
        var older = Truncate(DateTime.UtcNow.AddHours(-3));
        var newer = Truncate(DateTime.UtcNow.AddHours(-2));
        foreach (var db in new[] { "DbA", "DbB", "DbC" })
        {
            await SeedDatabaseConfigAsync(older, db, "FULL");
            await SeedDatabaseConfigAsync(newer, db, "SIMPLE");
        }

        var one = await McpConfigHistoryTools.GetDatabaseConfigChanges(
            _dataService, _serverManager, ServerName, database_name: "DbB");
        using (var doc = JsonDocument.Parse(one))
        {
            Assert.Equal(1, doc.RootElement.GetProperty("change_count").GetInt32());
            Assert.Equal("DbB", Assert.Single(doc.RootElement.GetProperty("changes").EnumerateArray())
                .GetProperty("database_name").GetString());
        }

        foreach (var blank in BlankNames)
        {
            var all = await McpConfigHistoryTools.GetDatabaseConfigChanges(
                _dataService, _serverManager, ServerName, database_name: blank);
            using var allDoc = JsonDocument.Parse(all);
            Assert.Equal(3, allDoc.RootElement.GetProperty("change_count").GetInt32());
        }

        var none = await McpConfigHistoryTools.GetDatabaseConfigChanges(
            _dataService, _serverManager, ServerName, database_name: "NoSuchDb");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        using var noneDoc = JsonDocument.Parse(none);
        Assert.Equal("empty", noneDoc.RootElement.GetProperty("status").GetString());
        Assert.Contains("for database NoSuchDb", none);
    }

    [Fact]
    public async Task DefaultTraceEvents_DatabaseName_LimitsTheEventsToThatDatabase()
    {
        /* No server clock is collected, so the tool reads event_time as UTC (McpServerLocalWindow.ClockForAsync). */
        var eventTime = Truncate(DateTime.UtcNow.AddHours(-1));
        foreach (var db in new[] { "SalesDB", "HrDB", "OpsDB" })
            await SeedDefaultTraceAsync(eventTime, "Object:Altered", db);

        var one = await McpDefaultTraceTools.GetDefaultTraceEvents(
            _dataService, _serverManager, ServerName, database_name: "HrDB");
        using (var doc = JsonDocument.Parse(one))
        {
            Assert.Equal(1, doc.RootElement.GetProperty("total_events").GetInt32());
            Assert.Equal("HrDB", Assert.Single(doc.RootElement.GetProperty("events").EnumerateArray())
                .GetProperty("database_name").GetString());
        }

        foreach (var blank in BlankNames)
        {
            var all = await McpDefaultTraceTools.GetDefaultTraceEvents(
                _dataService, _serverManager, ServerName, database_name: blank);
            using var allDoc = JsonDocument.Parse(all);
            Assert.Equal(3, allDoc.RootElement.GetProperty("total_events").GetInt32());
        }

        var none = await McpDefaultTraceTools.GetDefaultTraceEvents(
            _dataService, _serverManager, ServerName, database_name: "NoSuchDb");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        using var noneDoc = JsonDocument.Parse(none);
        Assert.Equal("empty", noneDoc.RootElement.GetProperty("status").GetString());
        Assert.Contains("for database NoSuchDb", none);
    }

    [Fact]
    public async Task SevereErrors_DatabaseName_FiltersOnTheResolvedName_BeforeTheCountsAndTheCap()
    {
        var eventTime = Truncate(DateTime.UtcNow.AddHours(-1));
        var names = new[] { (Id: 6, Name: "ProdDB"), (Id: 7, Name: "StageDB"), (Id: 8, Name: "DevDB") };
        foreach (var (id, name) in names)
            await SeedDatabaseSizeAsync(id, name, eventTime);

        /* Two errors in ProdDB, one each in StageDB and DevDB, and one in an id the collected map lacks. */
        var seconds = 0;
        foreach (var id in new[] { 6, 6, 7, 8, 99 })
            await SeedSevereErrorAsync(id, eventTime.AddSeconds(seconds++)); /* the archive view dedups same-instant events */

        var one = await McpHealthParserTools.GetSevereErrors(
            _dataService, _serverManager, ServerName, database_name: "ProdDB");
        using (var doc = JsonDocument.Parse(one))
        {
            Assert.Equal(2, doc.RootElement.GetProperty("error_count").GetInt32());
            var errors = doc.RootElement.GetProperty("errors").EnumerateArray().ToList();
            Assert.All(errors, e => Assert.Equal("ProdDB", e.GetProperty("database_name").GetString()));
        }

        /* The count is the chosen database's, not the server's: one StageDB error with a cap of 1 is complete, not cut. */
        var capped = await McpHealthParserTools.GetSevereErrors(
            _dataService, _serverManager, ServerName, limit: 1, database_name: "StageDB");
        using (var doc = JsonDocument.Parse(capped))
        {
            Assert.Equal(1, doc.RootElement.GetProperty("error_count").GetInt32());
            Assert.Equal(1, doc.RootElement.GetProperty("shown").GetInt32());
        }

        foreach (var blank in BlankNames)
        {
            var all = await McpHealthParserTools.GetSevereErrors(
                _dataService, _serverManager, ServerName, database_name: blank);
            using var allDoc = JsonDocument.Parse(all);
            Assert.Equal(5, allDoc.RootElement.GetProperty("error_count").GetInt32());
        }

        /* Darling's reader keeps an id the map lacks out of every chosen database, so its "database_id 99" label is not a name. */
        var unmapped = await McpHealthParserTools.GetSevereErrors(
            _dataService, _serverManager, ServerName, database_name: "database_id 99");
        using (var doc = JsonDocument.Parse(unmapped))
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());

        /* Ordinal, as on Darling: a different case is a different name. */
        var cased = await McpHealthParserTools.GetSevereErrors(
            _dataService, _serverManager, ServerName, database_name: "proddb");
        using (var doc = JsonDocument.Parse(cased))
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());

        var none = await McpHealthParserTools.GetSevereErrors(
            _dataService, _serverManager, ServerName, database_name: "NoSuchDb");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        using var noneDoc = JsonDocument.Parse(none);
        Assert.Equal("empty", noneDoc.RootElement.GetProperty("status").GetString());
        Assert.Contains("in database NoSuchDb", none);
    }

    // ── Seeding helpers (raw INSERT through one shared connection) ──

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedDatabaseConfigAsync(DateTime captureTimeUtc, string databaseName, string recoveryModel)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name, recovery_model)
VALUES ($1, $2, $3, $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = captureTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = recoveryModel });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDefaultTraceAsync(DateTime eventTime, string eventName, string databaseName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, database_name, text_data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventName });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ALTER" });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDatabaseSizeAsync(int databaseId, string databaseName, DateTime collectionTime)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ROWS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "data" });
        cmd.Parameters.Add(new DuckDBParameter { Value = @"D:\data\prod.mdf" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1024.00m });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One severity-24 error_reported event for <paramref name="databaseId"/>: the fixture's database_id action (6) swapped.</summary>
    private async Task SeedSevereErrorAsync(int databaseId, DateTime eventTime)
    {
        const string DatabaseIdValue = "<action name=\"database_id\" package=\"sqlserver\"><type name=\"uint16\" package=\"package0\"/><value>6</value>";
        var fixture = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", "error_reported.xml"));
        Assert.Contains(DatabaseIdValue, fixture);
        var xml = fixture.Replace(DatabaseIdValue, DatabaseIdValue.Replace("<value>6</value>", $"<value>{databaseId}</value>"));

        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = SystemHealthParser.ErrorReportedEvent });
        cmd.Parameters.Add(new DuckDBParameter { Value = xml });
        await cmd.ExecuteNonQueryAsync();
    }
}

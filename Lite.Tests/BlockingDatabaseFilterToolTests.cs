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
/// #5244 (PR3, lane L1): <c>database_name</c> on Lite's get_blocking_trend, get_blocked_process_xml and
/// get_blocking_stats, the twins of Darling's list-taking readers. The readers already took a database list (the
/// Blocking tab's filter); what was missing was the MCP parameter. Each tool gets a DuckDB fixture with three
/// databases where the filter returns only the chosen one and a blank name returns all of them, plus the filtered
/// empty answer, which says "for the database X" the way Darling's does and keeps the <c>empty</c> status word for a
/// collector that ran.
/// </summary>
public sealed class BlockingDatabaseFilterToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "BlockingDbFilterSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly LocalDataService _service;
    private DuckDBConnection? _seedConn;
    private long _nextId = 910000;

    public BlockingDatabaseFilterToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-blockdbfilter-" + Guid.NewGuid().ToString("N"));
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

    private async Task SeedThreeDatabaseReportsAsync()
    {
        await SeedReportAsync(Minute(30), "DbA", 11);
        await SeedReportAsync(Minute(20), "DbB", 22);
        await SeedReportAsync(Minute(10), "DbC", 33);
    }

    /* ───────────────────────── get_blocked_process_xml ───────────────────────── */

    [Fact]
    public async Task BlockedProcessXml_OneName_ReturnsOnlyThatDatabasesReports()
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpBlockingTools.GetBlockedProcessXml(
            _service, _serverManager, ServerName, database_name: "DbB"));

        var reports = root.GetProperty("reports").EnumerateArray().ToList();
        var only = Assert.Single(reports);
        Assert.Equal("DbB", only.GetProperty("database_name").GetString());
        Assert.Equal(22, only.GetProperty("blocked_spid").GetInt32());
        /* #5244 L3: the page says it was filtered. */
        Assert.Equal("DbB", root.GetProperty("database_name").GetString());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task BlockedProcessXml_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpBlockingTools.GetBlockedProcessXml(
            _service, _serverManager, ServerName, database_name: blank));

        Assert.Equal(3, root.GetProperty("reports").GetArrayLength());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("database_name").ValueKind);
    }

    /// <summary>The filter is in the SQL before the limit + 1 fetch, so limit counts the CHOSEN database's reports: a
    /// newer report in another database neither takes a slot nor makes the page look truncated.</summary>
    [Fact]
    public async Task BlockedProcessXml_Limit_CountsTheChosenDatabasesReports()
    {
        await SeedReportAsync(Minute(40), "DbA", 11);
        await SeedReportAsync(Minute(30), "DbA", 12);
        await SeedReportAsync(Minute(5), "DbB", 22);
        await SeedReportAsync(Minute(4), "DbB", 23);

        var whole = Parse(await McpBlockingTools.GetBlockedProcessXml(
            _service, _serverManager, ServerName, limit: 2, database_name: "DbA"));
        Assert.Equal(2, whole.GetProperty("reports_returned").GetInt32());
        Assert.False(whole.GetProperty("truncated").GetBoolean());

        var cut = Parse(await McpBlockingTools.GetBlockedProcessXml(
            _service, _serverManager, ServerName, limit: 1, database_name: "DbA"));
        Assert.Equal(1, cut.GetProperty("reports_returned").GetInt32());
        Assert.True(cut.GetProperty("truncated").GetBoolean());
        Assert.Equal(12, cut.GetProperty("reports")[0].GetProperty("blocked_spid").GetInt32());
    }

    [Fact]
    public async Task BlockedProcessXml_FilteredEmpty_SaysForTheDatabase_AndStaysEmpty()
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpBlockingTools.GetBlockedProcessXml(
            _service, _serverManager, ServerName, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains(
            "No blocked process report XML available in the specified time range for the database NoSuchDb.",
            root.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    /* ───────────────────────── get_blocking_trend ───────────────────────── */

    [Fact]
    public async Task BlockingTrend_OneName_CountsOnlyThatDatabase()
    {
        await SeedReportAsync(Minute(30), "DbA", 11);
        await SeedReportAsync(Minute(30), "DbB", 21);
        await SeedReportAsync(Minute(30), "DbB", 22);
        await SeedReportAsync(Minute(20), "DbC", 33);

        var all = Parse(await McpBlockingTools.GetBlockingTrend(_service, _serverManager, ServerName));
        Assert.Equal(new[] { 3, 1 }, all.GetProperty("trend").EnumerateArray().Select(p => p.GetProperty("count").GetInt32()).ToArray());

        var one = Parse(await McpBlockingTools.GetBlockingTrend(_service, _serverManager, ServerName, database_name: "DbB"));
        var point = Assert.Single(one.GetProperty("trend").EnumerateArray());
        Assert.Equal(2, point.GetProperty("count").GetInt32());
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public async Task BlockingTrend_BlankName_ReturnsEveryDatabase(string blank)
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpBlockingTools.GetBlockingTrend(
            _service, _serverManager, ServerName, database_name: blank));

        Assert.Equal(3, root.GetProperty("trend").GetArrayLength());
    }

    /// <summary>The DMV-snapshot arm still fills in only where the FILTERED report arm has no rows, so a database that has
    /// snapshots but no report in the window is answered from them, and one that has reports is not doubled.</summary>
    [Fact]
    public async Task BlockingTrend_DmvArm_IsUsedOnlyWhenTheFilteredReportArmIsEmpty()
    {
        await SeedReportAsync(Minute(30), "DbA", 11);
        await SeedSnapshotAsync(Minute(30), "DbA", 71);
        await SeedSnapshotAsync(Minute(20), "DbB", 72);
        await SeedSnapshotAsync(Minute(19), "DbB", 73);

        var a = Parse(await McpBlockingTools.GetBlockingTrend(_service, _serverManager, ServerName, database_name: "DbA"));
        Assert.Equal(1, Assert.Single(a.GetProperty("trend").EnumerateArray()).GetProperty("count").GetInt32());
        /* The answer names its source (#5244 M1): A has a report, so the reports answered. */
        Assert.Equal("blocked-process-report", a.GetProperty("source").GetString());
        Assert.Equal("DbA", a.GetProperty("database_name").GetString());

        var b = Parse(await McpBlockingTools.GetBlockingTrend(_service, _serverManager, ServerName, database_name: "DbB"));
        Assert.Equal(2, b.GetProperty("trend").GetArrayLength());
        /* B has snapshots and no report, so the DMV snapshot answered. */
        Assert.Equal("DMV snapshot", b.GetProperty("source").GetString());

        /* Adding B to A: the reports answer (A's row), so the source is the reports and B's snapshots are not mixed in. */
        var unfiltered = Parse(await McpBlockingTools.GetBlockingTrend(_service, _serverManager, ServerName));
        Assert.Equal("blocked-process-report", unfiltered.GetProperty("source").GetString());
        Assert.Equal(JsonValueKind.Null, unfiltered.GetProperty("database_name").ValueKind);
    }

    /// <summary>The severity read names its source too (#5244 M1): the DMV snapshot answers only where the reports have no rows.</summary>
    [Fact]
    public async Task BlockingStats_Source_IsTheDmvSnapshotOnlyWhenTheFilteredReportArmIsEmpty()
    {
        await SeedReportAsync(Minute(30), "DbA", 11, waitMs: 1000);
        await SeedSnapshotAsync(Minute(20), "DbB", 72);

        var a = Parse(await McpHealthTools.GetBlockingStats(_service, _serverManager, ServerName, database_name: "DbA"));
        Assert.Equal("blocked-process-report", a.GetProperty("source").GetString());
        var b = Parse(await McpHealthTools.GetBlockingStats(_service, _serverManager, ServerName, database_name: "DbB"));
        Assert.Equal("DMV snapshot", b.GetProperty("source").GetString());
    }

    [Fact]
    public async Task BlockingTrend_FilteredEmpty_SaysForTheDatabase_AndKeepsTheEmptyStatus()
    {
        await SeedRunAsync("blocked_process_report", Minute(10));
        await SeedReportAsync(Minute(30), "DbA", 11);

        var root = Parse(await McpBlockingTools.GetBlockingTrend(
            _service, _serverManager, ServerName, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("No blocking was recorded for " + ServerName + " for the database NoSuchDb in the last 24 hour(s).", text, StringComparison.Ordinal);
        /* The run counts behind the answer are the server's own: a collector looks at every database. */
        Assert.Equal(1, root.GetProperty("hints").GetProperty("capture_count").GetInt64());
    }

    [Fact]
    public async Task BlockingTrend_UnfilteredEmpty_KeepsItsWords()
    {
        await SeedRunAsync("blocked_process_report", Minute(10));

        var root = Parse(await McpBlockingTools.GetBlockingTrend(_service, _serverManager, ServerName));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("No blocking was recorded for " + ServerName + " in the last 24 hour(s).",
            root.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    /* ───────────────────────── get_blocking_stats ───────────────────────── */

    [Fact]
    public async Task BlockingStats_OneName_LimitsTheBlockingSeries_AndLeavesDeadlocksWhole()
    {
        await SeedReportAsync(Minute(30), "DbA", 11, waitMs: 1000);
        await SeedReportAsync(Minute(30), "DbB", 21, waitMs: 2000);
        await SeedReportAsync(Minute(30), "DbB", 22, waitMs: 6000);
        await SeedReportAsync(Minute(20), "DbC", 33, waitMs: 9000);
        await SeedDeadlockAsync(Minute(25));

        var all = Parse(await McpHealthTools.GetBlockingStats(_service, _serverManager, ServerName));
        Assert.Equal(JsonValueKind.Null, all.GetProperty("database_name").ValueKind);
        Assert.Equal(2, all.GetProperty("blocking_duration").GetArrayLength());

        var one = Parse(await McpHealthTools.GetBlockingStats(_service, _serverManager, ServerName, database_name: "DbB"));
        Assert.Equal("DbB", one.GetProperty("database_name").GetString());
        Assert.Equal("blocked-process-report", one.GetProperty("source").GetString());
        var bucket = Assert.Single(one.GetProperty("blocking_duration").EnumerateArray());
        Assert.Equal(2, bucket.GetProperty("event_count").GetInt32());
        Assert.Equal(8000, bucket.GetProperty("total_duration_ms").GetInt64());
        Assert.Equal(6000, bucket.GetProperty("max_duration_ms").GetInt64());
        /* Deadlock graphs carry no database column here, so the deadlock series is the server's. */
        Assert.Equal(1, one.GetProperty("deadlock_severity").GetArrayLength());
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public async Task BlockingStats_BlankName_ReturnsEveryDatabase(string blank)
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpHealthTools.GetBlockingStats(
            _service, _serverManager, ServerName, database_name: blank));

        Assert.Equal(JsonValueKind.Null, root.GetProperty("database_name").ValueKind);
        Assert.Equal(3, root.GetProperty("blocking_duration").GetArrayLength());
    }

    [Fact]
    public async Task BlockingStats_FilteredEmpty_SaysBlockingForTheDatabase_AndKeepsTheEmptyStatus()
    {
        await SeedRunAsync("blocked_process_report", Minute(10));
        await SeedReportAsync(Minute(30), "DbA", 11);

        var root = Parse(await McpHealthTools.GetBlockingStats(
            _service, _serverManager, ServerName, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains(
            "No blocking for the database NoSuchDb (and no deadlocks, which are not limited by database) recorded for " + ServerName,
            root.GetProperty("message").GetString()!, StringComparison.Ordinal);
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

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedReportAsync(DateTime at, string database, int blockedSpid, long waitMs = 8000L) => ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text,
     blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        _nextId++, Naive(at), _serverId, ServerName, Naive(at), database, blockedSpid, 90, waitMs, "X", "SELECT 1",
        "UPDATE T SET x = 1", "<blocked-process-report/>", "dbo.T");

    private Task SeedSnapshotAsync(DateTime at, string database, int blockedSpid) => ExecAsync(@"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        _nextId++, Naive(at), _serverId, ServerName, -1, Naive(at), database, blockedSpid, 80, 3000L, "S", "suspended",
        "dbo.U", "SELECT 2", "WAITFOR DELAY '00:01'");

    private Task SeedDeadlockAsync(DateTime at) => ExecAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        _nextId++, Naive(at), _serverId, ServerName, Naive(at), "process1", "DELETE FROM Posts",
        "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list><process id=\"process1\"><inputbuf>DELETE FROM Posts</inputbuf></process></process-list></deadlock>");

    /// <summary>A collector run that SUCCEEDED and stored nothing, the row that makes an empty answer an all-clear.</summary>
    private Task SeedRunAsync(string collector, DateTime collectionTimeUtc) => ExecAsync(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        _nextId++, _serverId, ServerName, collector, Naive(collectionTimeUtc), 100, "SUCCESS", null, 0, 80, 20);
}

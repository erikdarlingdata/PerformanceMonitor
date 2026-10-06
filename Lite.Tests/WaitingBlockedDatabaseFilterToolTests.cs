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
/// #5244 (PR3, lane L2): <c>database_name</c> on Lite's get_waiting_tasks and get_blocked_process_reports, the twins of
/// Darling's list-taking readers. Both Lite readers already took a database list; what was missing was the MCP
/// parameter. Each tool gets a DuckDB fixture with three databases where the filter returns only the chosen one, a blank
/// name returns all of them, the filter runs BEFORE the row cap, and the filtered empty answer says the CHOSEN database
/// had none while keeping the <c>empty</c> status word.
/// </summary>
public sealed class WaitingBlockedDatabaseFilterToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "WaitBlockDbFilterSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly LocalDataService _service;
    private DuckDBConnection? _seedConn;
    private long _nextId = 920000;

    public WaitingBlockedDatabaseFilterToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-waitblockdbfilter-" + Guid.NewGuid().ToString("N"));
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

    /* ───────────────────────── get_waiting_tasks ───────────────────────── */

    private async Task SeedThreeDatabaseTasksAsync()
    {
        await SeedTaskAsync(Minute(30), "DbA", 11);
        await SeedTaskAsync(Minute(20), "DbB", 22);
        await SeedTaskAsync(Minute(10), "DbC", 33);
    }

    [Fact]
    public async Task WaitingTasks_OneName_ReturnsOnlyThatDatabasesTasks_AndEchoesTheName()
    {
        await SeedThreeDatabaseTasksAsync();

        var root = Parse(await McpWaitTools.GetWaitingTasks(
            _service, _serverManager, ServerName, database_name: "DbB"));

        var only = Assert.Single(root.GetProperty("tasks").EnumerateArray());
        Assert.Equal("DbB", only.GetProperty("database_name").GetString());
        Assert.Equal(22, only.GetProperty("blocking_session_id").GetInt32());
        Assert.Equal("DbB", root.GetProperty("database_name").GetString());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task WaitingTasks_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabaseTasksAsync();

        var root = Parse(await McpWaitTools.GetWaitingTasks(
            _service, _serverManager, ServerName, database_name: blank));

        Assert.Equal(3, root.GetProperty("tasks").GetArrayLength());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("database_name").ValueKind);
    }

    /// <summary>The filter is in the SQL before the limit + 1 fetch, so limit counts the CHOSEN database's tasks: newer
    /// tasks in another database neither take a slot nor make the page look truncated.</summary>
    [Fact]
    public async Task WaitingTasks_Limit_CountsTheChosenDatabasesTasks()
    {
        await SeedTaskAsync(Minute(40), "DbA", 11);
        await SeedTaskAsync(Minute(30), "DbA", 12);
        await SeedTaskAsync(Minute(5), "DbB", 22);
        await SeedTaskAsync(Minute(4), "DbB", 23);

        var whole = Parse(await McpWaitTools.GetWaitingTasks(
            _service, _serverManager, ServerName, limit: 2, database_name: "DbA"));
        Assert.Equal(2, whole.GetProperty("tasks_returned").GetInt32());
        Assert.False(whole.GetProperty("truncated").GetBoolean());

        var cut = Parse(await McpWaitTools.GetWaitingTasks(
            _service, _serverManager, ServerName, limit: 1, database_name: "DbA"));
        Assert.Equal(1, cut.GetProperty("tasks_returned").GetInt32());
        Assert.True(cut.GetProperty("truncated").GetBoolean());
        Assert.Equal(12, cut.GetProperty("tasks")[0].GetProperty("blocking_session_id").GetInt32());
    }

    [Fact]
    public async Task WaitingTasks_FilteredEmpty_SaysTheChosenDatabaseHadNone_AndStaysEmpty()
    {
        await SeedThreeDatabaseTasksAsync();

        var root = Parse(await McpWaitTools.GetWaitingTasks(
            _service, _serverManager, ServerName, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("No waiting tasks captured in the specified time range for the database NoSuchDb. ", text, StringComparison.Ordinal);
        Assert.Contains("waiting tasks of other databases may well exist", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WaitingTasks_UnfilteredEmpty_KeepsItsWords()
    {
        var root = Parse(await McpWaitTools.GetWaitingTasks(_service, _serverManager, ServerName));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal("No waiting tasks captured in the specified time range.", root.GetProperty("message").GetString());
    }

    /* ───────────────────────── get_blocked_process_reports ───────────────────────── */

    private async Task SeedThreeDatabaseReportsAsync()
    {
        await SeedReportAsync(Minute(30), "DbA", 11);
        await SeedReportAsync(Minute(20), "DbB", 22);
        await SeedReportAsync(Minute(10), "DbC", 33);
    }

    [Fact]
    public async Task BlockedProcessReports_OneName_ReturnsOnlyThatDatabasesReports()
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpBlockingTools.GetBlockedProcessReports(
            _service, _serverManager, ServerName, database_name: "DbB"));

        var only = Assert.Single(root.GetProperty("reports").EnumerateArray());
        Assert.Equal("DbB", only.GetProperty("database_name").GetString());
        Assert.Equal(22, only.GetProperty("blocked_spid").GetInt32());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task BlockedProcessReports_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpBlockingTools.GetBlockedProcessReports(
            _service, _serverManager, ServerName, database_name: blank));

        Assert.Equal(3, root.GetProperty("reports").GetArrayLength());
    }

    /// <summary>Both arms, the XE reports and the DMV fallback, keep only the chosen database: a snapshot of another
    /// database does not appear, and one whose database has no report is answered from the DMV arm.</summary>
    [Fact]
    public async Task BlockedProcessReports_DmvArm_FollowsTheFilterToo()
    {
        await SeedReportAsync(Minute(30), "DbA", 11);
        await SeedSnapshotAsync(Minute(20), "DbB", 72);
        await SeedSnapshotAsync(Minute(10), "DbC", 73);

        var root = Parse(await McpBlockingTools.GetBlockedProcessReports(
            _service, _serverManager, ServerName, database_name: "DbB"));

        var only = Assert.Single(root.GetProperty("reports").EnumerateArray());
        Assert.Equal("DbB", only.GetProperty("database_name").GetString());
        Assert.Equal(72, only.GetProperty("blocked_spid").GetInt32());
    }

    [Fact]
    public async Task BlockedProcessReports_Limit_CountsTheChosenDatabasesReports()
    {
        await SeedReportAsync(Minute(40), "DbA", 11);
        await SeedReportAsync(Minute(30), "DbA", 12);
        await SeedReportAsync(Minute(5), "DbB", 22);
        await SeedReportAsync(Minute(4), "DbB", 23);

        var whole = Parse(await McpBlockingTools.GetBlockedProcessReports(
            _service, _serverManager, ServerName, limit: 2, database_name: "DbA"));
        Assert.Equal(2, whole.GetProperty("reports_returned").GetInt32());
        Assert.False(whole.GetProperty("truncated").GetBoolean());

        var cut = Parse(await McpBlockingTools.GetBlockedProcessReports(
            _service, _serverManager, ServerName, limit: 1, database_name: "DbA"));
        Assert.Equal(1, cut.GetProperty("reports_returned").GetInt32());
        Assert.True(cut.GetProperty("truncated").GetBoolean());
        Assert.Equal(12, cut.GetProperty("reports")[0].GetProperty("blocked_spid").GetInt32());
    }

    [Fact]
    public async Task BlockedProcessReports_FilteredEmpty_SaysForTheDatabase_AndStaysEmpty()
    {
        await SeedThreeDatabaseReportsAsync();

        var root = Parse(await McpBlockingTools.GetBlockedProcessReports(
            _service, _serverManager, ServerName, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("No blocked process reports found in the specified time range for the database NoSuchDb.",
            root.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BlockedProcessReports_UnfilteredEmpty_KeepsItsWords()
    {
        var root = Parse(await McpBlockingTools.GetBlockedProcessReports(_service, _serverManager, ServerName));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal("No blocked process reports found in the specified time range.", root.GetProperty("message").GetString());
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

    /// <summary>The blocking session id doubles as the row's identity in the assertions.</summary>
    private Task SeedTaskAsync(DateTime at, string database, int blockingSessionId) => ExecAsync(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        _nextId++, Naive(at), _serverId, ServerName, 55, "LCK_M_X", 5000L, blockingSessionId, "keylock", database);

    private Task SeedReportAsync(DateTime at, string database, int blockedSpid) => ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text,
     blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        _nextId++, Naive(at), _serverId, ServerName, Naive(at), database, blockedSpid, 90, 8000L, "X", "SELECT 1",
        "UPDATE T SET x = 1", "<blocked-process-report/>", "dbo.T");

    private Task SeedSnapshotAsync(DateTime at, string database, int blockedSpid) => ExecAsync(@"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocking_status, contentious_object, blocked_sql_text, blocking_sql_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        _nextId++, Naive(at), _serverId, ServerName, -1, Naive(at), database, blockedSpid, 80, 3000L, "S", "suspended",
        "dbo.U", "SELECT 2", "WAITFOR DELAY '00:01'");
}

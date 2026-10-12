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

    // One clock read per test instance (cut to the minute). Reading the clock on every call split rows
    // seeded together across two minutes whenever a minute boundary passed mid-seed.
    private readonly DateTime _minuteAnchor = MinuteOf(DateTime.UtcNow);

    private static DateTime MinuteOf(DateTime now) =>
        new(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);

    private DateTime Minute(int minutesAgo) => _minuteAnchor.AddMinutes(-minutesAgo);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    /* ───────────────────────── get_waiting_tasks ───────────────────────── */

    private async Task SeedThreeDatabaseTasksAsync()
    {
        await SeedTaskAsync(Minute(30), "DbA", 11);
        await SeedTaskAsync(Minute(20), "DbB", 22);
        await SeedTaskAsync(Minute(10), "DbC", 33);
    }

    /// <summary>#5633 item 18: a parallel query's threads are ONE row with a thread_count, and a blocker equal to the session itself
    /// is flagged intra_query_parallelism and never reported as a blocker (it used to be 30 rows, 29 "blocked by" session 97).</summary>
    [Fact]
    public async Task WaitingTasks_ParallelQueryThreads_AreOneRow_WithAThreadCount_AndNoSelfBlocker()
    {
        for (var thread = 0; thread < 30; thread++)
        {
            await ExecAsync(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                _nextId++, Naive(Minute(5)), _serverId, ServerName, 97, "CXCONSUMER", 100L + thread, thread < 29 ? 97 : null, null, "DbA");
        }

        await ExecAsync(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            _nextId++, Naive(Minute(5)), _serverId, ServerName, 55, "LCK_M_S", 5000L, 60, null, "DbA");

        var root = Parse(await McpWaitTools.GetWaitingTasks(_service, _serverManager, ServerName));
        var tasks = root.GetProperty("tasks").EnumerateArray().ToList();
        Assert.Equal(2, tasks.Count);

        var parallel = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 97);
        Assert.Equal(30, parallel.GetProperty("thread_count").GetInt32());
        Assert.True(parallel.GetProperty("intra_query_parallelism").GetBoolean());
        Assert.Equal(JsonValueKind.Null, parallel.GetProperty("blocking_session_id").ValueKind);
        Assert.Equal(129, parallel.GetProperty("wait_duration_ms").GetInt64());

        var locked = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 55);
        Assert.Equal(1, locked.GetProperty("thread_count").GetInt32());
        Assert.False(locked.GetProperty("intra_query_parallelism").GetBoolean());
        Assert.Equal(60, locked.GetProperty("blocking_session_id").GetInt32());
    }

    /// <summary>#5633 review L-a: threads of one session that DISAGREE. The row is the longest-waiting thread's wait, resource and
    /// blocker (it used to be three independent MAXes: the alphabetically greatest resource beside another thread's duration, and the
    /// highest-numbered blocker alone), and every other distinct blocker is listed in blocking_session_ids.</summary>
    [Fact]
    public async Task WaitingTasks_DisagreeingThreads_TakeResourceAndBlockerFromTheLongestWaitingThread()
    {
        await SeedThreadAsync(70, "LCK_M_S", 100L, 40, "zzz-shortest-wait");
        await SeedThreadAsync(70, "LCK_M_S", 900L, 20, "aaa-longest-wait");
        await SeedThreadAsync(70, "LCK_M_S", 500L, 30, "mmm-middle");
        /* Session 71: the longest thread waits on its own session (parallelism); a shorter one is blocked by 12. */
        await SeedThreadAsync(71, "CXPACKET", 800L, 71, "res-self");
        await SeedThreadAsync(71, "CXPACKET", 300L, 12, "res-external");

        var root = Parse(await McpWaitTools.GetWaitingTasks(_service, _serverManager, ServerName));
        var tasks = root.GetProperty("tasks").EnumerateArray().ToList();

        var disagree = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 70);
        Assert.Equal(900, disagree.GetProperty("wait_duration_ms").GetInt64());
        Assert.Equal("aaa-longest-wait", disagree.GetProperty("resource_description").GetString());
        Assert.Equal(20, disagree.GetProperty("blocking_session_id").GetInt32());
        Assert.Equal(new[] { 20, 30, 40 }, disagree.GetProperty("blocking_session_ids").EnumerateArray().Select(e => e.GetInt32()).ToArray());

        var selfFirst = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 71);
        Assert.Equal(800, selfFirst.GetProperty("wait_duration_ms").GetInt64());
        Assert.Equal("res-self", selfFirst.GetProperty("resource_description").GetString());
        Assert.True(selfFirst.GetProperty("intra_query_parallelism").GetBoolean());
        Assert.Equal(12, selfFirst.GetProperty("blocking_session_id").GetInt32());
        Assert.Equal(JsonValueKind.Null, selfFirst.GetProperty("blocking_session_ids").ValueKind);
    }

    /// <summary>Review round 2 Low-2: the fallback blocker (the longest thread waits on its own session) and the resource follow the SAME
    /// ordering as Darling's ordered array_agg on equal waits and NULL waits. Darling's twin
    /// (<c>WaitingTasksRollup5633Tests.TiedAndNullWaitThreads_...</c>) seeds the same threads and asserts the same rows.</summary>
    [Fact]
    public async Task WaitingTasks_TiedAndNullWaitThreads_PickTheSameRowAsDarling()
    {
        /* Session 80: the longest thread is self-blocked, two others tie at 300 ms (the higher blocker is stored first): the lowest blocker wins. */
        await SeedThreadAsync(80, "CXPACKET", 800L, 80, "res-self");
        await SeedThreadAsync(80, "CXPACKET", 300L, 50, "res-50");
        await SeedThreadAsync(80, "CXPACKET", 300L, 40, "res-40");
        /* Session 81: the longest thread is self-blocked and the only other-blocked thread has a NULL wait: it still supplies the blocker. */
        await SeedThreadAsync(81, "CXPACKET", 800L, 81, "res-self");
        await SeedThreadAsync(81, "CXPACKET", null, 70, "res-null-wait");
        /* Session 82: equal waits, one self-blocked: the lowest other blocker wins, and the resource follows the lowest raw blocker. */
        await SeedThreadAsync(82, "LCK_M_S", 500L, 33, "res-33");
        await SeedThreadAsync(82, "LCK_M_S", 500L, 22, "res-22");
        await SeedThreadAsync(82, "LCK_M_S", 500L, 82, "res-self");

        var root = Parse(await McpWaitTools.GetWaitingTasks(_service, _serverManager, ServerName));
        var tasks = root.GetProperty("tasks").EnumerateArray().ToList();

        var tied = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 80);
        Assert.Equal(800, tied.GetProperty("wait_duration_ms").GetInt64());
        Assert.Equal("res-self", tied.GetProperty("resource_description").GetString());
        Assert.Equal(40, tied.GetProperty("blocking_session_id").GetInt32());
        Assert.Equal(new[] { 40, 50 }, tied.GetProperty("blocking_session_ids").EnumerateArray().Select(e => e.GetInt32()).ToArray());

        var nullWait = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 81);
        Assert.Equal(800, nullWait.GetProperty("wait_duration_ms").GetInt64());
        Assert.Equal("res-self", nullWait.GetProperty("resource_description").GetString());
        Assert.True(nullWait.GetProperty("intra_query_parallelism").GetBoolean());
        Assert.Equal(70, nullWait.GetProperty("blocking_session_id").GetInt32());
        Assert.Equal(JsonValueKind.Null, nullWait.GetProperty("blocking_session_ids").ValueKind);

        var equal = tasks.Single(t => t.GetProperty("session_id").GetInt32() == 82);
        Assert.Equal(500, equal.GetProperty("wait_duration_ms").GetInt64());
        Assert.Equal("res-22", equal.GetProperty("resource_description").GetString());
        Assert.Equal(22, equal.GetProperty("blocking_session_id").GetInt32());
        Assert.Equal(new[] { 22, 33 }, equal.GetProperty("blocking_session_ids").EnumerateArray().Select(e => e.GetInt32()).ToArray());
    }

    private Task SeedThreadAsync(int session, string waitType, long? waitMs, int? blocker, string? resource) => ExecAsync(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        _nextId++, Naive(Minute(5)), _serverId, ServerName, session, waitType, waitMs, blocker, resource, "DbA");

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
        /* N2: a blank is "no filter", and the answer echoes it as null rather than the blank that was sent. */
        Assert.Equal(JsonValueKind.Null, root.GetProperty("database_name").ValueKind);
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

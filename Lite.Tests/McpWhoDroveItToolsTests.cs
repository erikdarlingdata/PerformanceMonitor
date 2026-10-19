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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Behaviour of Lite's <c>get_cpu_by_database</c>, <c>get_load_by_caller</c> and <c>get_query_pileups</c> (#6114), the twins of Darling's
/// #6072 reads, over rows seeded into a real DuckDB: the ranking, a share of the whole window rather than of the page, a request counted
/// once however many captures saw it, a pile-up with and without a blocking snapshot within 60 seconds, microseconds kept, and the empty
/// answers.
/// </summary>
public sealed class McpWhoDroveItToolsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly LocalDataService _data;
    private readonly int _serverId;
    private long _nextId = 1;

    public McpWhoDroveItToolsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _data = new LocalDataService(_duckDb);

        _tempDir = Path.Combine(Path.GetTempPath(), "McpWhoDroveIt_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /* DuckDB keeps microseconds, so the seeded instants are whole microseconds (a .NET tick is 100 ns). */
    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc.AddTicks(-(utc.Ticks % 10)), DateTimeKind.Unspecified);

    /* ───────────────────────── get_cpu_by_database ───────────────────────── */

    [Fact]
    public async Task CpuByDatabase_RanksByCpu_ShareIsOfTheServer_AndProceduresSitBesideNeverAdded()
    {
        var t = Naive(DateTime.UtcNow.AddMinutes(-30));
        /* DbA: two statements (one of them across two collections, so it is one statement), 400k + 100k us. DbB: one, 100k us. */
        await SeedQueryStatsAsync(t, "DbA", "0xA1", 300_000, 5, "SELECT a1");
        await SeedQueryStatsAsync(t.AddMinutes(1), "DbA", "0xA1", 100_000, 3, "SELECT a1 newer");
        await SeedQueryStatsAsync(t, "DbA", "0xA2", 100_000, 1, "SELECT a2");
        await SeedQueryStatsAsync(t, "DbB", "0xB1", 100_000, 2, "SELECT b1");
        await SeedProcedureStatsAsync(t, "DbA", "dbo", "usp_heavy", 250_000);
        await SeedProcedureStatsAsync(t, "DbA", "dbo", "usp_light", 50_000);

        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetCpuByDatabase(_data, _serverManager, "TestServer"));
        var root = doc.RootElement;
        Assert.Equal(600.0, root.GetProperty("server_cpu_ms").GetDouble());
        var dbs = root.GetProperty("databases").EnumerateArray().ToList();
        Assert.Equal(new[] { "DbA", "DbB" }, dbs.Select(d => d.GetProperty("database_name").GetString()).ToArray());

        var a = dbs[0];
        Assert.Equal(500.0, a.GetProperty("cpu_ms").GetDouble());
        Assert.Equal(0.8333, a.GetProperty("share").GetDouble());
        Assert.Equal(2, a.GetProperty("statements").GetInt64());
        Assert.Equal(9, a.GetProperty("executions").GetInt64());
        var top = a.GetProperty("top_statement");
        Assert.Equal("0xA1", top.GetProperty("query_hash").GetString());
        Assert.Equal(400.0, top.GetProperty("cpu_ms").GetDouble());
        Assert.Equal(0.8, top.GetProperty("share_of_database").GetDouble());
        Assert.Equal("SELECT a1 newer", top.GetProperty("query_text").GetString());
        /* Procedure CPU is its own figure: it is not folded into cpu_ms. */
        Assert.Equal(300.0, a.GetProperty("procedure_cpu_ms").GetDouble());
        Assert.Equal("dbo.usp_heavy", a.GetProperty("top_procedure").GetProperty("name").GetString());

        Assert.Equal(JsonValueKind.Null, dbs[1].GetProperty("procedure_cpu_ms").ValueKind);
        Assert.Equal(0.1667, dbs[1].GetProperty("share").GetDouble());
    }

    [Fact]
    public async Task CpuByDatabase_ShareStaysOfTheServerWhenTopCutsThePage()
    {
        var t = Naive(DateTime.UtcNow.AddMinutes(-30));
        await SeedQueryStatsAsync(t, "DbA", "0xA1", 300_000, 1, "SELECT a");
        await SeedQueryStatsAsync(t, "DbB", "0xB1", 100_000, 1, "SELECT b");

        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetCpuByDatabase(_data, _serverManager, "TestServer", top: 1));
        var root = doc.RootElement;
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, root.GetProperty("databases_returned").GetInt32());
        Assert.Equal(400.0, root.GetProperty("server_cpu_ms").GetDouble());
        Assert.Equal(0.75, root.GetProperty("databases")[0].GetProperty("share").GetDouble());
    }

    [Fact]
    public async Task CpuByDatabase_TiesBreakOnDatabaseName()
    {
        var t = Naive(DateTime.UtcNow.AddMinutes(-30));
        await SeedQueryStatsAsync(t, "Zeta", "0xZ", 100_000, 1, "SELECT z");
        await SeedQueryStatsAsync(t, "Alpha", "0xA", 100_000, 1, "SELECT a");

        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetCpuByDatabase(_data, _serverManager, "TestServer"));
        Assert.Equal(new[] { "Alpha", "Zeta" },
            doc.RootElement.GetProperty("databases").EnumerateArray().Select(d => d.GetProperty("database_name").GetString()).ToArray());
    }

    [Fact]
    public async Task CpuByDatabase_EmptyWindow_SaysEmpty()
    {
        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetCpuByDatabase(_data, _serverManager, "TestServer"));
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }

    /* ───────────────────────── get_load_by_caller ───────────────────────── */

    [Fact]
    public async Task LoadByCaller_CountsEachRequestOnce_AndSharesAreOfTheWindow()
    {
        var t0 = Naive(DateTime.UtcNow.AddMinutes(-20));
        /* Session 55 is ONE request seen at three captures: its cumulative figures grow, its start (capture minus elapsed) does not. */
        await SeedSnapshotAsync(t0, 55, "hostA", "appA", "loginA", "0xQ1", 100, 1_000, "SELECT q1");
        await SeedSnapshotAsync(t0.AddSeconds(10), 55, "hostA", "appA", "loginA", "0xQ1", 200, 11_000, "SELECT q1");
        await SeedSnapshotAsync(t0.AddSeconds(20), 55, "hostA", "appA", "loginA", "0xQ1", 300, 21_000, "SELECT q1 newest");
        /* Session 56, same caller, another request, one capture. */
        await SeedSnapshotAsync(t0.AddSeconds(20), 56, "hostA", "appA", "loginA", "0xQ2", 50, 500, "SELECT q2");
        /* A different caller. */
        await SeedSnapshotAsync(t0.AddSeconds(20), 70, "hostB", "appB", "loginB", "0xQ3", 150, 900, "SELECT q3");
        /* A WAITFOR shell is dropped. */
        await SeedSnapshotAsync(t0.AddSeconds(20), 71, "hostB", "appB", "loginB", "0xW", 9_999, 9_999, "WAITFOR DELAY '00:00:30'");

        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetLoadByCaller(_data, _serverManager, "TestServer"));
        var root = doc.RootElement;
        Assert.Equal(3, root.GetProperty("total_requests").GetInt64());
        Assert.Equal(5, root.GetProperty("total_samples").GetInt64());
        Assert.Equal(500.0, root.GetProperty("total_cpu_ms").GetDouble());

        var callers = root.GetProperty("callers").EnumerateArray().ToList();
        Assert.Equal(new[] { "hostA", "hostB" }, callers.Select(c => c.GetProperty("host_name").GetString()).ToArray());
        var a = callers[0];
        Assert.Equal(2, a.GetProperty("requests").GetInt64());
        Assert.Equal(4, a.GetProperty("samples").GetInt64());
        Assert.Equal(3, a.GetProperty("captures").GetInt64());
        Assert.Equal(350.0, a.GetProperty("cpu_ms").GetDouble());
        Assert.Equal(0.7, a.GetProperty("cpu_share").GetDouble());
        Assert.Equal(0.8, a.GetProperty("sample_share").GetDouble());
        var stmts = a.GetProperty("top_statements").EnumerateArray().ToList();
        Assert.Equal("0xQ1", stmts[0].GetProperty("query_hash").GetString());
        Assert.Equal(1, stmts[0].GetProperty("requests").GetInt64());
        Assert.Equal(3, stmts[0].GetProperty("samples").GetInt64());
        Assert.Equal(300.0, stmts[0].GetProperty("cpu_ms").GetDouble());
        Assert.Equal("SELECT q1 newest", stmts[0].GetProperty("query_text").GetString());
        Assert.Equal(t0.ToString("o").Replace("Z", ""), a.GetProperty("first_seen").GetString()!.TrimEnd('Z'));

        var b = callers[1];
        Assert.Equal(1, b.GetProperty("requests").GetInt64());
        Assert.Equal(150.0, b.GetProperty("cpu_ms").GetDouble());
    }

    [Fact]
    public async Task LoadByCaller_SharesStayOfTheWindowWhenTopCutsThePage_AndAbsentCallerFieldsAreNull()
    {
        var t0 = Naive(DateTime.UtcNow.AddMinutes(-20));
        await SeedSnapshotAsync(t0, 55, "hostA", "appA", "loginA", "0xQ1", 300, 1_000, "SELECT q1");
        await SeedSnapshotAsync(t0, 56, null, null, null, "0xQ2", 100, 1_000, "SELECT q2");

        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetLoadByCaller(_data, _serverManager, "TestServer", top: 1));
        var root = doc.RootElement;
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(0.75, root.GetProperty("callers")[0].GetProperty("cpu_share").GetDouble());

        using var all = JsonDocument.Parse(await McpWhoDroveItTools.GetLoadByCaller(_data, _serverManager, "TestServer"));
        var anonymous = all.RootElement.GetProperty("callers")[1];
        Assert.Equal(JsonValueKind.Null, anonymous.GetProperty("host_name").ValueKind);
        Assert.Equal(JsonValueKind.Null, anonymous.GetProperty("login_name").ValueKind);
    }

    [Fact]
    public async Task LoadByCaller_EmptyWindow_SaysEmpty()
    {
        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetLoadByCaller(_data, _serverManager, "TestServer"));
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }

    /* ───────────────────────── get_query_pileups ───────────────────────── */

    [Fact]
    public async Task Pileups_NameTheHeadBlockerFromTheNearestSnapshotWithinSixtySeconds_KeepMicroseconds_AndListAPileWithoutOne()
    {
        /* 123456 microseconds past a whole second: 1,234,560 ticks. */
        var whole = DateTime.UtcNow.AddMinutes(-40);
        var t1 = new DateTime(whole.Year, whole.Month, whole.Day, whole.Hour, whole.Minute, whole.Second, DateTimeKind.Unspecified).AddTicks(1_234_560);
        var t2 = Naive(DateTime.UtcNow.AddHours(-3));

        /* Pile 1: three sessions on one hash, two of them blocked. A second hash has one session and is no pile. */
        await SeedSnapshotAsync(t1, 51, "h", "a", "l", "0xPILE", 10, 100, "SELECT pile", waitType: "LCK_M_S", waitMs: 5_000, blockedBy: 70);
        await SeedSnapshotAsync(t1, 52, "h", "a", "l", "0xPILE", 10, 100, "SELECT pile", waitType: "LCK_M_S", waitMs: 9_000, blockedBy: 70);
        await SeedSnapshotAsync(t1, 53, "h", "a", "l", "0xPILE", 10, 100, "SELECT pile");
        await SeedSnapshotAsync(t1, 54, "h", "a", "l", "0xOTHER", 10, 100, "SELECT other");
        /* Pile 2, three hours back, no blocking snapshot within 60 seconds (one 90 seconds out). */
        await SeedSnapshotAsync(t2, 61, "h", "a", "l", "0xOLD", 10, 100, "SELECT old");
        await SeedSnapshotAsync(t2, 62, "h", "a", "l", "0xOLD", 10, 100, "SELECT old");
        await SeedSnapshotAsync(t2, 63, "h", "a", "l", "0xOLD", 10, 100, "SELECT old");

        /* Pile 1: a snapshot 50 seconds before is farther than one 10 seconds after; the nearer one names spid 70 (blocked by nobody). */
        await SeedBlockingAsync(t1.AddSeconds(-50), blocked: 80, blocking: 99, waitMs: 1_000, login: "farther");
        await SeedBlockingAsync(t1.AddSeconds(10), blocked: 51, blocking: 70, waitMs: 5_000, login: "loginHead", host: "hostHead", app: "appHead", sql: "UPDATE t SET x = 1");
        await SeedBlockingAsync(t1.AddSeconds(10), blocked: 52, blocking: 70, waitMs: 9_000, login: "loginHead");
        /* spid 70 is itself behind nothing; spid 51 blocks 53 in turn, but 51 is blocked so it is not a head. */
        await SeedBlockingAsync(t1.AddSeconds(10), blocked: 53, blocking: 51, waitMs: 100, login: "mid");
        await SeedBlockingAsync(t2.AddSeconds(90), blocked: 61, blocking: 99, waitMs: 1_000, login: "toofar");

        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetQueryPileups(_data, _serverManager, "TestServer", min_sessions: 3));
        var root = doc.RootElement;
        Assert.Equal("dmv_blocking_snapshots", root.GetProperty("blocking_source").GetString());
        Assert.Equal(60, root.GetProperty("blocking_tolerance_seconds").GetInt32());
        var piles = root.GetProperty("pileups").EnumerateArray().ToList();
        Assert.Equal(2, piles.Count);

        var p1 = piles[0];
        Assert.Equal("0xPILE", p1.GetProperty("query_hash").GetString());
        Assert.Equal(3, p1.GetProperty("sessions").GetInt64());
        Assert.Equal(2, p1.GetProperty("blocked_sessions").GetInt64());
        Assert.Equal(9_000, p1.GetProperty("max_wait_ms").GetInt64());
        Assert.EndsWith(".1234560Z", p1.GetProperty("collection_time").GetString());
        var waits = p1.GetProperty("waits").EnumerateArray().Select(w => (w.GetProperty("wait_type").GetString()!, w.GetProperty("sessions").GetInt64())).ToList();
        Assert.Equal(new[] { ("LCK_M_S", 2L), ("(none)", 1L) }, waits);
        Assert.Equal(10.0, p1.GetProperty("blocking_snapshot_offset_seconds").GetDouble());
        var head = p1.GetProperty("head_blocker");
        Assert.Equal(70, head.GetProperty("spid").GetInt32());
        Assert.Equal(3, head.GetProperty("blocked_sessions").GetInt32());
        Assert.Equal(9_000, head.GetProperty("wait_time_ms").GetInt64());
        Assert.Equal("loginHead", head.GetProperty("login_name").GetString());

        var p2 = piles[1];
        Assert.Equal("0xOLD", p2.GetProperty("query_hash").GetString());
        Assert.Equal(JsonValueKind.Null, p2.GetProperty("head_blocker").ValueKind);
        Assert.Equal(JsonValueKind.Null, p2.GetProperty("blocking_snapshot_time").ValueKind);
        Assert.Contains("within 60 seconds", p2.GetProperty("head_blocker_note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Pileups_MinSessionsFilters_AndMostSessionsComesFirst()
    {
        var t = Naive(DateTime.UtcNow.AddMinutes(-30));
        for (var s = 1; s <= 3; s++) await SeedSnapshotAsync(t, s, "h", "a", "l", "0xTHREE", 1, 1, "SELECT three");
        for (var s = 11; s <= 15; s++) await SeedSnapshotAsync(t.AddMinutes(-5), s, "h", "a", "l", "0xFIVE", 1, 1, "SELECT five");

        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetQueryPileups(_data, _serverManager, "TestServer", min_sessions: 3));
        Assert.Equal(new[] { "0xFIVE", "0xTHREE" },
            doc.RootElement.GetProperty("pileups").EnumerateArray().Select(p => p.GetProperty("query_hash").GetString()).ToArray());

        using var strict = JsonDocument.Parse(await McpWhoDroveItTools.GetQueryPileups(_data, _serverManager, "TestServer", min_sessions: 4));
        Assert.Single(strict.RootElement.GetProperty("pileups").EnumerateArray());
    }

    [Fact]
    public async Task Pileups_EmptyWindow_SaysEmpty()
    {
        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetQueryPileups(_data, _serverManager, "TestServer"));
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task Pileups_RefuseAMinSessionsBelowTwo()
    {
        using var doc = JsonDocument.Parse(await McpWhoDroveItTools.GetQueryPileups(_data, _serverManager, "TestServer", min_sessions: 1));
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("min_sessions", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
    }

    /* ───────────────────────── seeding ───────────────────────── */

    private async Task SeedQueryStatsAsync(DateTime collected, string database, string queryHash, long cpuUs, long executions, string queryText)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, last_execution_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xH" + queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryText });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureStatsAsync(DateTime collected, string database, string schema, string name, long cpuUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, delta_execution_count, delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = schema });
        cmd.Parameters.Add(new DuckDBParameter { Value = name });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedSnapshotAsync(
        DateTime collected, int session, string? host, string? program, string? login, string queryHash, long cpuMs, long elapsedMs, string queryText,
        string? waitType = null, long waitMs = 0, int blockedBy = 0)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, query_hash, wait_type, wait_time_ms,
     blocking_session_id, cpu_time_ms, total_elapsed_time_ms, host_name, program_name, login_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = session });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryText });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)waitType ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockedBy });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)host ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)program ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)login ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedBlockingAsync(
        DateTime collected, int blocked, int blocking, long waitMs, string login, string host = "h", string app = "a", string sql = "SELECT 1")
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms,
     blocking_login_name, blocking_host_name, blocking_client_app, blocking_sql_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = blocked });
        cmd.Parameters.Add(new DuckDBParameter { Value = blocking });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = login });
        cmd.Parameters.Add(new DuckDBParameter { Value = host });
        cmd.Parameters.Add(new DuckDBParameter { Value = app });
        cmd.Parameters.Add(new DuckDBParameter { Value = sql });
        await cmd.ExecuteNonQueryAsync();
    }
}

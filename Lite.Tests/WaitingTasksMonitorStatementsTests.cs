/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
/// #7573, Lite half: the collector now stores the waiting task's session program name (v80, a nullable VARCHAR appended last). The read and the
/// <c>get_waiting_tasks</c> tool leave out a row whose stored <c>program_name</c> starts with the monitor's prefix, keep a user's application
/// that only contains the words and a row with no program name (every row stored before v80), and bring them back when asked.
/// </summary>
public sealed class WaitingTasksMonitorStatementsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly string _configDir;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public WaitingTasksMonitorStatementsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        _configDir = Path.Combine(Path.GetTempPath(), "WaitingTasksMonitor_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        _serverManager.AddServer(server);
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_configDir)) Directory.Delete(_configDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /* Sessions 60 to 65, in order: three monitor names, a similar user name, an ordinary application, and a NULL program name. */
    private static readonly string?[] Apps =
    {
        "PerformanceMonitorDarling", "PerformanceMonitorLite", "PerformanceMonitorDarling-Remediation",
        "MyPerformanceMonitorTool", "Orders.Api", null,
    };

    private async Task SeedAsync()
    {
        var now = DateTime.UtcNow;
        var at = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddMinutes(-5);
        for (var i = 0; i < Apps.Length; i++)
        {
            await ExecAsync(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, database_name, program_name)
VALUES ($1, $2, $3, $4, $5, 'PAGEIOLATCH_SH', $6, 0, 'TestDb', $7)",
                _nextId--, DateTime.SpecifyKind(at, DateTimeKind.Unspecified), _serverId, ServerName, 60 + i, 500L + i, Apps[i]);
        }
    }

    /* The old read had no program predicate and returned all six rows, so the default assertions fail on it. */
    [Fact]
    public async Task TheReader_LeavesTheMonitorOut_ByDefault_AndKeepsAUsersSimilarNameAndANull()
    {
        await SeedAsync();

        var def = await _dataService.GetWaitingTasksAsync(_serverId, hoursBack: 1);
        Assert.Equal(new[] { 63, 64, 65 }, def.Select(r => r.SessionId).OrderBy(s => s).ToArray());

        var all = await _dataService.GetWaitingTasksAsync(_serverId, hoursBack: 1, includeMonitor: true);
        Assert.Equal(6, all.Count);
    }

    [Fact]
    public async Task TheTool_LeavesTheMonitorOut_ByDefault_AndShowsItWithTheSwitch()
    {
        await SeedAsync();

        var def = Sessions(await McpWaitTools.GetWaitingTasks(_dataService, _serverManager, ServerName, 1, 30));
        Assert.Equal(new[] { 63, 64, 65 }, def);

        var all = Sessions(await McpWaitTools.GetWaitingTasks(_dataService, _serverManager, ServerName, 1, 30, include_monitor: true));
        Assert.Equal(new[] { 60, 61, 62, 63, 64, 65 }, all);
    }

    [Fact]
    public async Task OnlyMonitorWaits_AnswerEmpty_AndTheTextSaysTheyAreLeftOut()
    {
        await ExecAsync(@"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, database_name, program_name)
VALUES ($1, $2, $3, $4, 60, 'PAGEIOLATCH_SH', 500, 0, 'TestDb', 'PerformanceMonitorLite')",
            _nextId--, DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-5), DateTimeKind.Unspecified), _serverId, ServerName);

        using var def = JsonDocument.Parse(await McpWaitTools.GetWaitingTasks(_dataService, _serverManager, ServerName, 1, 30));
        Assert.Equal("empty", def.RootElement.GetProperty("status").GetString());
        Assert.Contains("monitor's own statements are left out", def.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

        Assert.Single(Sessions(await McpWaitTools.GetWaitingTasks(_dataService, _serverManager, ServerName, 1, 30, include_monitor: true)));
    }

    private static int[] Sessions(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("tasks").EnumerateArray()
            .Select(r => r.GetProperty("session_id").GetInt32())
            .OrderBy(s => s)
            .ToArray();

    private async Task ExecAsync(string sql, params object?[] values)
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
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}

/// <summary>
/// Lite v80 / Darling V243 (#7573): <c>waiting_tasks</c> gains <c>program_name</c>, VARCHAR, nullable, the LAST payload column (the DuckDB appender is
/// positional). A fresh file gets it from the generator, an existing file from the shared added-columns list, and the row stored before it reads NULL.
/// </summary>
public sealed class WaitingTasksProgramNameRungTests : IDisposable
{
    private const string Column = "program_name";
    private readonly string _tempDir;

    public WaitingTasksProgramNameRungTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "WaitingTasksProgramNameRungTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AFreshFile_EndsWaitingTasksWithTheProgramName()
    {
        var dbPath = Path.Combine(_tempDir, "lite-fresh.duckdb");
        using (var initializer = new DuckDbInitializer(dbPath))
        {
            await initializer.InitializeAsync();
        }

        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync();

        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 80);
        var rows = await ColumnsAsync(conn);
        Assert.Equal((Column, "VARCHAR", "YES"), rows[^1]);
        Assert.Equal("database_name", rows[^2].Name);
        var entry = Assert.Single(DuckDbInitializer.AddedColumnsForVersion(80));
        Assert.Equal(("waiting_tasks", Column, "VARCHAR"), (entry.Table, entry.Column, entry.Type));
    }

    [Fact]
    public async Task AV79Database_ClimbsToTheNewestVersion_AndItsOldRowsReadNull()
    {
        var dbPath = Path.Combine(_tempDir, "lite-v79.duckdb");

        using (var initializer = new DuckDbInitializer(dbPath))
        {
            await initializer.InitializeAsync();
        }

        string[] freshOrder;
        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            freshOrder = (await ColumnsAsync(conn)).Select(c => c.Name).ToArray();

            /* DuckDB refuses to DROP a column a view or index depends on; the dependents go first for the fixture's sake. */
            foreach (var view in await ListAsync(conn, "SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%waiting_tasks%'"))
                await ExecAsync(conn, $"DROP VIEW IF EXISTS {view}");
            foreach (var index in await ListAsync(conn, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'waiting_tasks'"))
                await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");

            await ExecAsync(conn, $"ALTER TABLE waiting_tasks DROP COLUMN {Column}");
            await ExecAsync(conn, "INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms) "
                + "VALUES (-1, TIMESTAMP '2026-10-19 12:00:00', 1, 'srv', 60, 'LCK_M_S', 100)");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (79)");

            Assert.Equal(0L, await CountAsync(conn, $"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'waiting_tasks' AND column_name = '{Column}'"));
        }

        using var upgraded = new DuckDbInitializer(dbPath);
        await upgraded.InitializeAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(conn, "SELECT MAX(version) FROM schema_version"));

            /* The upgraded table has the same columns in the same order as a fresh one. */
            Assert.Equal(freshOrder, (await ColumnsAsync(conn)).Select(c => c.Name).ToArray());

            /* The pre-rung row reads NULL, and the view the readers use still returns it; a post-rung row keeps its value. */
            await ExecAsync(conn, "INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, program_name) "
                + "VALUES (-2, TIMESTAMP '2026-10-19 13:00:00', 1, 'srv', 61, 'LCK_M_S', 100, 'Orders.Api')");
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, $"SELECT {Column} FROM waiting_tasks WHERE collection_id = -1"));
            Assert.Equal("Orders.Api", await ScalarAsync(conn, $"SELECT {Column} FROM waiting_tasks WHERE collection_id = -2"));
            Assert.Equal(2L, await CountAsync(conn, "SELECT COUNT(*) FROM v_waiting_tasks WHERE server_id = 1"));
        }
    }

    private static async Task<List<(string Name, string Type, string Nullable)>> ColumnsAsync(DuckDBConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'waiting_tasks' ORDER BY ordinal_position";
        using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<(string Name, string Type, string Nullable)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        return rows;
    }

    private static async Task<List<string>> ListAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        var values = new List<string>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }

    private static async Task<long> CountAsync(DuckDBConnection conn, string sql) =>
        Convert.ToInt64(await ScalarAsync(conn, sql));
}

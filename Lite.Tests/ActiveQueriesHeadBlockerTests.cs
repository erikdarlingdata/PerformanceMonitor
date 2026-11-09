/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Active Queries grid keeps a head blocker whose current statement is a <c>WAITFOR</c>: the classic head blocker is a session sitting in
/// <c>WAITFOR</c> with an open transaction, and a grid that trims every <c>WAITFOR</c> row shows the victim with a blocking session id that is
/// on no row. The grid read keeps the row when another row of the SAME capture names it as its blocker, as the MCP page read does, and still
/// drops an idle <c>WAITFOR</c> shell nothing waits on.
/// </summary>
public sealed class ActiveQueriesHeadBlockerTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public ActiveQueriesHeadBlockerTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    private const int Head = 60;
    private const int Victim = 61;
    private const int IdleShell = 62;
    private const int Reader = 63;

    /* One capture five minutes ago: the WAITFOR head holding the lock, its victim, an idle WAITFOR shell nothing waits on, and a plain reader.
       An older capture holds the same shell session id blocking nobody, so a head is judged within its own capture and not across the window. */
    private async Task SeedAsync()
    {
        var now = DateTime.UtcNow;
        var at = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddMinutes(-5);
        await InsertAsync(at, Head, "WAITFOR DELAY '00:10:00'", blockedBy: 0);
        await InsertAsync(at, Victim, "SELECT [v] FROM [dbo].[walk_rows] WHERE [id]=@1", blockedBy: Head);
        await InsertAsync(at, IdleShell, "WAITFOR DELAY '00:10:00'", blockedBy: 0);
        await InsertAsync(at, Reader, "SELECT 1", blockedBy: 0);
        await InsertAsync(at.AddMinutes(-1), Head, "WAITFOR DELAY '00:10:00'", blockedBy: 0);
    }

    private Task InsertAsync(DateTime at, int session, string text, int blockedBy) => ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, program_name, request_id)
VALUES ($1, $2, $3, $4, $5, 'TestDb', $6, 'suspended', $7, $8, 10, 300000, 'Orders.Api', 0)",
        _nextId--, DateTime.SpecifyKind(at, DateTimeKind.Unspecified), _serverId, ServerName, session, text, blockedBy, blockedBy > 0 ? "LCK_M_S" : null);

    [Fact]
    public async Task TheGridRead_KeepsTheWaitforHeadBlocker_AndDropsTheIdleShell()
    {
        await SeedAsync();

        var rows = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack: 1);
        var newest = rows.Max(r => r.CollectionTime);
        var sessions = rows.Where(r => r.CollectionTime == newest).Select(r => r.SessionId).OrderBy(s => s).ToArray();

        Assert.Equal(new[] { Head, Victim, Reader }, sessions);
        Assert.DoesNotContain(rows, r => r.CollectionTime < newest);
    }

    [Fact]
    public async Task TheGridRead_ShowsTheSameRowsAsThePageRead()
    {
        await SeedAsync();

        var grid = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack: 1);
        var (page, _) = await _dataService.GetActiveQueriesPageAsync(_serverId, 1, 100);

        Assert.Equal(
            page.Select(r => (r.CollectionTime, r.SessionId)).OrderBy(k => k).ToArray(),
            grid.Select(r => (r.CollectionTime, r.SessionId)).OrderBy(k => k).ToArray());
    }

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

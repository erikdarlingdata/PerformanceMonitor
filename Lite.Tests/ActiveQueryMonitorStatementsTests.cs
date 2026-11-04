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
/// #7573, Lite half: on a quiet server every Active Queries row was one of the monitor's own statements. The rows stay stored. The grid read,
/// the MCP page read and the strip's buckets leave out a row whose stored <c>program_name</c> starts with the monitor's prefix, keep a user's
/// application that only contains the words and a row with no program name, and bring them back when asked.
/// </summary>
public sealed class ActiveQueryMonitorStatementsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public ActiveQueryMonitorStatementsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly string?[] Apps =
    {
        "PerformanceMonitorDarling", "PerformanceMonitorLite", "PerformanceMonitorDarling-Remediation",
        "MyPerformanceMonitorTool", "Orders.Api", null,
    };

    private static readonly string?[] UserApps = { null, "MyPerformanceMonitorTool", "Orders.Api" };

    private async Task SeedAsync()
    {
        var now = DateTime.UtcNow;
        var at = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddMinutes(-5);
        for (var i = 0; i < Apps.Length; i++)
        {
            await ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, cpu_time_ms, total_elapsed_time_ms, program_name, request_id)
VALUES ($1, $2, $3, $4, $5, 'TestDb', $6, 'running', 0, 1000, 1500, $7, 0)",
                _nextId--, DateTime.SpecifyKind(at, DateTimeKind.Unspecified), _serverId, ServerName, 60 + i, "SELECT " + i, Apps[i]);
        }
    }


    /* The old reads had no program predicate and returned all six rows, so the default assertions fail on them. */
    [Fact]
    public async Task TheGridRead_LeavesTheMonitorOut_ByDefault_AndKeepsAUsersSimilarName()
    {
        await SeedAsync();

        var def = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack: 1);
        Assert.Equal(UserApps.Select(a => a ?? "").ToArray(), def.Select(r => r.ProgramName).OrderBy(a => a, StringComparer.Ordinal).ToArray());

        var all = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack: 1, includeMonitor: true);
        Assert.Equal(6, all.Count);
    }

    [Fact]
    public async Task TheMcpPageRead_LeavesTheMonitorOut_ByDefault_AndCountsOnlyWhatItShows()
    {
        await SeedAsync();

        var (rows, population) = await _dataService.GetActiveQueriesPageAsync(_serverId, 1, 100);
        Assert.Equal(3, rows.Count);
        Assert.Equal(3, population);
        Assert.DoesNotContain(rows, r => r.ProgramName.StartsWith("PerformanceMonitor", StringComparison.Ordinal));

        var (allRows, allPopulation) = await _dataService.GetActiveQueriesPageAsync(_serverId, 1, 100, includeMonitor: true);
        Assert.Equal(6, allRows.Count);
        Assert.Equal(6, allPopulation);
    }

    [Fact]
    public async Task TheStrip_CountsOnlyTheKeptRows_ByDefault()
    {
        await SeedAsync();

        var def = await _dataService.GetActiveQuerySlicerDataAsync(_serverId, 1);
        Assert.Equal(3, def.Sum(b => b.SessionCount));

        var all = await _dataService.GetActiveQuerySlicerDataAsync(_serverId, 1, includeMonitor: true);
        Assert.Equal(6, all.Sum(b => b.SessionCount));
    }

    /* The live bar runs the collector's own query, which no longer drops the monitor's other sessions, so it follows the page's switch. Three live rows:
       the monitor's, an application's and one with no program name (never hidden). Program name is column 22 of the snapshot query. */
    [Fact]
    public async Task TheLiveBar_LeavesTheMonitorOut_ByDefault_AndKeepsThemWhenAsked()
    {
        static object[] Live(int session, string? program)
        {
            var row = new object[35];
            Array.Fill(row, DBNull.Value);
            row[0] = session;
            row[22] = program ?? (object)DBNull.Value;
            return row;
        }

        object[][] Rows() => new[] { Live(61, "PerformanceMonitorLite"), Live(62, "Orders.Api"), Live(63, null) };

        using var def = new Lite.Tests.Helpers.FakeCollectorDataReader(Rows());
        var kept = await PerformanceMonitorLite.Controls.ServerTab.ReadLiveSnapshotRowsAsync(def, new DateTime(2026, 11, 4, 12, 0, 0));
        Assert.Equal(new[] { 62, 63 }, kept.Select(r => r.SessionId).ToArray());

        using var asked = new Lite.Tests.Helpers.FakeCollectorDataReader(Rows());
        var all = await PerformanceMonitorLite.Controls.ServerTab.ReadLiveSnapshotRowsAsync(asked, new DateTime(2026, 11, 4, 12, 0, 0), includeMonitor: true);
        Assert.Equal(3, all.Count);
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

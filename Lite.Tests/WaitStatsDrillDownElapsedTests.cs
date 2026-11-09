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
/// The Wait Stats drill-down reads (the queries with one wait, and every snapshot in the range) show elapsed time the way Active Queries
/// does: the stored <c>00 00:05:12.345</c> reads <c>00:05:12.345</c>, in the same Elapsed column.
/// </summary>
public sealed class WaitStatsDrillDownElapsedTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;

    public WaitStatsDrillDownElapsedTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task SeedAsync()
    {
        var now = DateTime.UtcNow;
        var at = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddMinutes(-5);
        await ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status,
     blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, program_name, request_id)
VALUES ($1, $2, $3, $4, 61, 'TestDb', '00 00:05:12.345', 'SELECT 1', 'suspended', 60, 'LCK_M_S', 5000, 10, 312345, 'Orders.Api', 0)",
            -1L, DateTime.SpecifyKind(at, DateTimeKind.Unspecified), _serverId, ServerName);
    }

    [Fact]
    public async Task TheQueriesWithOneWait_ShowElapsedWithoutAZeroDayCount()
    {
        await SeedAsync();

        var rows = await _dataService.GetQuerySnapshotsByWaitTypeAsync(_serverId, "LCK_M_S", hoursBack: 1);

        Assert.Equal("00:05:12.345", Assert.Single(rows).ElapsedTimeFormatted);
    }

    [Fact]
    public async Task EverySnapshotInTheRange_ShowsElapsedWithoutAZeroDayCount()
    {
        await SeedAsync();

        var rows = await _dataService.GetAllQuerySnapshotsInRangeAsync(_serverId, hoursBack: 1);

        Assert.Equal("00:05:12.345", Assert.Single(rows).ElapsedTimeFormatted);
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

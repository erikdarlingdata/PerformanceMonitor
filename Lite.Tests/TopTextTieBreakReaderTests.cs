/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5299 round 2 (N3): the newest-text pick behind <c>GetTopQueriesByCpuAsync</c> and <c>GetQueryStoreTopQueriesAsync</c> breaks a tie on
/// <c>collection_time</c> on <c>collection_id DESC</c>, the Lite twin of the Darling statements. Two rows of one key at one time with
/// different texts (the one with the lower collection id is inserted first, which is the one an unordered pick returns) give the text of
/// the row collected last on every read.
/// </summary>
public sealed class TopTextTieBreakReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8816;
    private static readonly DateTime Collected = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-90), DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public TopTextTieBreakReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

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
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TopQueriesByCpu_PicksTheTextOfTheRowCollectedLast_WhenTwoRowsShareATime()
    {
        foreach (var text in new[] { "tie text A (lower collection id)", "tie text B (higher collection id)" })
        {
            await ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, 'TestSrv', 'TestDb', '0xTIE', '0xHTIE', $2, 10, 1000, 1000, $4)", _nextId++, Collected, ServerId, text);
        }

        var service = new LocalDataService(_duckDb);
        for (var run = 0; run < 3; run++)
        {
            var rows = await service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: 5);
            Assert.Equal("tie text B (higher collection id)", Assert.Single(rows).QueryText);
        }
    }

    [Fact]
    public async Task QueryStoreTop_PicksTheTextOfTheRowCollectedLast_WhenTwoRowsShareATime()
    {
        foreach (var text in new[] { "store tie text A (lower collection id)", "store tie text B (higher collection id)" })
        {
            await ExecAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, 'TestSrv', 'TestDb', 77, 7, 'Regular', $2, $2, $4, '0xQTIE', 10, 1000, 5000, 100, 0, 0, 1, $2)", _nextId++, Collected, ServerId, text);
        }

        var service = new LocalDataService(_duckDb);
        for (var run = 0; run < 3; run++)
        {
            var rows = await service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24, top: 5);
            Assert.Equal("store tie text B (higher collection id)", Assert.Single(rows).QueryText);
        }
    }
}

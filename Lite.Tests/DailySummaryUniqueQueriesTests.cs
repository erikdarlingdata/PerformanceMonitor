/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8651: the daily summary's unique queries count queries with credited executions in the window. A restart row
/// (<c>sample_interval_seconds = 0</c>: a first sighting, an uncredited counter reset or a gap) credits nothing, so a hash whose
/// only rows that day are restart rows is left out. A hash with any credited row counts once, and a row from before the
/// column (NULL interval) counts as it always did.
/// </summary>
public class DailySummaryUniqueQueriesTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private DuckDBConnection? _seedConn;

    private const int ServerId = -8651;
    private static readonly DateTime _day = new DateTime(2026, 6, 15, 0, 0, 0, DateTimeKind.Utc);
    private long _nextId = -1;

    public DailySummaryUniqueQueriesTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task SeedAsync(DateTime time, string hash, int? interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_stats
            (collection_id, collection_time, server_id, server_name, query_hash,
             execution_count, total_elapsed_time, total_worker_time,
             total_logical_reads, total_logical_writes, total_physical_reads,
             delta_execution_count, delta_elapsed_time, delta_worker_time,
             delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
             sample_interval_seconds)
            VALUES ($1, $2, $3, 'TestServer', $4, 5, 50, 0, 0, 0, 0, $5, $5, 0, 0, 0, 0, 0, 0, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = hash });
        cmd.Parameters.Add(new DuckDBParameter { Value = interval == 0 ? 0L : 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)interval ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task UniqueQueries_LeavesOutAHashWhoseOnlyRowsAreRestartRows_AndCountsAHashWithAnyCreditedRowOnce()
    {
        await SeedAsync(_day.AddHours(1), "0xCREDITED", 120);
        await SeedAsync(_day.AddHours(2), "0xCREDITED", 120);
        await SeedAsync(_day.AddHours(3), "0xRESTART_THEN_CREDITED", 0);
        await SeedAsync(_day.AddHours(4), "0xRESTART_THEN_CREDITED", 120);
        await SeedAsync(_day.AddHours(5), "0xPRE_COLUMN", null);
        await SeedAsync(_day.AddHours(6), "0xRESTART_ONLY", 0);
        await SeedAsync(_day.AddHours(7), "0xRESTART_ONLY", 0);

        var summary = await _dataService.GetDailySummaryAsync(ServerId, _day);

        Assert.NotNull(summary);
        Assert.Equal(3L, summary!.UniqueQueries);
    }
}

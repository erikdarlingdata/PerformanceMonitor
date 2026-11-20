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
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9100: a Query Store window is the half-open range from its start to its end, placed by the interval's own start. The interval
/// that starts at the window's end belongs to the next window, and the interval that starts exactly at the window's start stays in.
/// Three intervals start at the end minus an hour, at the end and at the end plus an hour; the top read, the slicer and the
/// duration trend each count only the first.
/// </summary>
public sealed class QueryStoreWindowEndIsExclusiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9100;
    private const string Db = "EndDb";

    private static readonly DateTime AsOf = new(2026, 11, 13, 1, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = AsOf.AddHours(-1);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public QueryStoreWindowEndIsExclusiveTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task TheTopRead_CountsOnlyTheIntervalStartedBeforeTheEnd_AndKeepsTheOneAtTheStart()
    {
        await SeedAsync();

        var rows = await new LocalDataService(_duckDb).GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 1, asOfUtc: AsOf);

        Assert.Equal(new long[] { 1 }, rows.Select(r => r.QueryId).OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task TheSlicer_CountsOnlyTheIntervalStartedBeforeTheEnd()
    {
        await SeedAsync();

        var buckets = await new LocalDataService(_duckDb).GetQueryStoreSlicerDataAsync(ServerId, 1, fromDate: WindowStart, toDate: AsOf);

        Assert.Equal(1, buckets.Sum(b => b.SessionCount));
    }

    [Fact]
    public async Task TheDurationTrend_PlacesOnlyTheIntervalStartedBeforeTheEnd()
    {
        await SeedAsync();

        var points = await new LocalDataService(_duckDb).GetQueryStoreDurationTrendAsync(ServerId, hoursBack: 1, asOfUtc: AsOf);

        var point = Assert.Single(points);
        Assert.Equal(WindowStart, point.CollectionTime);
    }

    [Fact]
    public async Task TheItemTimeline_PlotsOnlyTheIntervalStartedBeforeTheEnd()
    {
        /* One query and plan, an interval at the window's start and one at its end. */
        await SeedAsync((7L, WindowStart), (7L, AsOf));

        var points = await new LocalDataService(_duckDb).GetQueryStoreItemTimelineAsync(ServerId, Db, 7, 7, hoursBack: 1, fromDate: WindowStart, toDate: AsOf);

        var point = Assert.Single(points);
        Assert.Equal(WindowStart, point.PointTime);
    }

    /// <summary>One interval at each of the window's start, its end and an hour past it, each collected half an hour after it starts.</summary>
    private Task SeedAsync() => SeedAsync((1L, AsOf.AddHours(-1)), (2L, AsOf), (3L, AsOf.AddHours(1)));

    private async Task SeedAsync(params (long Query, DateTime Started)[] intervals)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        long id = 1;
        foreach (var (query, started) in intervals)
        {
            using var cmd = _seedConn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc)
VALUES ($1, $2, $3, 'EndSrv', $4, $5, $5, 'Regular', $6, $7, $8, $9, 10, 100, 1000, 5, 0, 0, $10, false, 0, $1, $11, $12)";
            cmd.Parameters.Add(new DuckDBParameter { Value = id++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = started.AddMinutes(30) });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = Db });
            cmd.Parameters.Add(new DuckDBParameter { Value = query });
            cmd.Parameters.Add(new DuckDBParameter { Value = started.AddMinutes(5) });
            cmd.Parameters.Add(new DuckDBParameter { Value = started.AddMinutes(25) });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"SELECT {query}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"0xH{query}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"0xPLAN{query}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = started });
            cmd.Parameters.Add(new DuckDBParameter { Value = started.AddHours(1) });
            await cmd.ExecuteNonQueryAsync();
        }
    }
}

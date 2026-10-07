/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5449: the procedure_stats collector stores no row for a procedure that did no work in a cycle. A missing minute is
/// "no work", not "no data", and old stores still hold the idle rows (deltas 0, interval 60). Every reader that touched
/// the table must give the same answer on a store that has the idle rows and one that does not. Each test seeds two
/// servers with the same work: <see cref="OldServer"/> keeps the idle rows, <see cref="NewServer"/> leaves them out.
/// </summary>
public sealed class ProcedureStatsIdleRowsReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int OldServer = -5449;
    private const int NewServer = -5450;

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private DuckDBConnection? _seedConn;
    private long _nextId = -5_000_000;

    public ProcedureStatsIdleRowsReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime TenMinuteFloor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.FromMinutes(10).Ticks)), DateTimeKind.Unspecified);

    /// <summary>The same twenty collections, one a minute, on both servers: work in minutes 1, 2, 15 and 20, and an idle row
    /// (deltas 0, interval 60) for every other minute on the old server only.</summary>
    private async Task<DateTime> SeedTwentyMinutesAsync()
    {
        var t0 = TenMinuteFloor(DateTime.UtcNow.AddMinutes(-40));
        var workMinutes = new HashSet<int> { 1, 2, 15, 20 };
        for (var k = 1; k <= 20; k++)
        {
            var at = t0.AddMinutes(k);
            if (workMinutes.Contains(k))
            {
                await SeedAsync(OldServer, at, "usp_Work", executions: 10, elapsedUs: 600_000, interval: 60);
                await SeedAsync(NewServer, at, "usp_Work", executions: 10, elapsedUs: 600_000, interval: 60);
            }
            else
            {
                await SeedAsync(OldServer, at, "usp_Work", executions: 0, elapsedUs: 0, interval: 60);
            }
        }
        return t0;
    }

    [Fact]
    public async Task BucketedProcedureTrend_IdleMinutesNotStored_ReadsTheSameAsStoredIdleRows()
    {
        var t0 = await SeedTwentyMinutesAsync();
        var asOf = t0.AddMinutes(25);

        var oldPoints = await _dataService.GetBucketedProcedureDurationTrendAsync(OldServer, 1, asOf, 10);
        var newPoints = await _dataService.GetBucketedProcedureDurationTrendAsync(NewServer, 1, asOf, 10);

        Assert.Equal(3, oldPoints.Count);
        Assert.Equal(oldPoints.Count, newPoints.Count);
        for (var i = 0; i < oldPoints.Count; i++)
        {
            Assert.Equal(oldPoints[i].CollectionTime, newPoints[i].CollectionTime);
            Assert.Equal(oldPoints[i].Value!.Value, newPoints[i].Value!.Value, precision: 6);
            Assert.Equal(oldPoints[i].ExecutionsPerSecond!.Value, newPoints[i].ExecutionsPerSecond!.Value, precision: 6);
        }

        /* 1,200 ms of work over the bucket's 600 s, not over the two stored one-minute collections' 120 s (which read 10). */
        Assert.Equal(2.0, newPoints[0].Value!.Value, precision: 6);
        Assert.Equal(1.0, newPoints[1].Value!.Value, precision: 6);
        /* The last bucket holds only the series' final collection: its own 60 s. */
        Assert.Equal(10.0, newPoints[2].Value!.Value, precision: 6);
    }

    [Fact]
    public async Task ProcedureSlicer_ProcedureCountIsOfProceduresWithWork_OnBothStores()
    {
        var t0 = await SeedTwentyMinutesAsync();
        /* A second procedure that never worked: a row of zeros on the old store, nothing on the new. */
        await SeedAsync(OldServer, t0.AddMinutes(3), "usp_NeverWorked", executions: 0, elapsedUs: 0, interval: 60);

        var oldBuckets = await _dataService.GetProcStatsSlicerDataAsync(OldServer, hoursBack: 24);
        var newBuckets = await _dataService.GetProcStatsSlicerDataAsync(NewServer, hoursBack: 24);

        Assert.Equal(oldBuckets.Select(b => (b.BucketTime, b.SessionCount)), newBuckets.Select(b => (b.BucketTime, b.SessionCount)));
        Assert.All(newBuckets, b => Assert.Equal(1, b.SessionCount));
    }

    [Fact]
    public async Task ProcedureWindowFloor_AnIdleStartIsCoveredByTheCollectorsRuns()
    {
        var start = TenMinuteFloor(DateTime.UtcNow.AddHours(-3));
        var end = start.AddHours(3);
        /* The collector ran every minute from the window's start, and its first stored row is an hour in. */
        await SeedAsync(NewServer, start.AddHours(1), "usp_Work", executions: 10, elapsedUs: 600_000, interval: 60);
        await SeedRunsAsync(NewServer, "procedure_stats", start, end);

        var floor = await _dataService.GetQueryWindowFloorAsync(QueryWindowRelation.ProcedureStats, NewServer, start, end);

        Assert.Equal(start, floor);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedAsync(int serverId, DateTime at, string objectName, long executions, long elapsedUs, int interval)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO procedure_stats
            (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
             execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes,
             delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
            VALUES ($1, $2, $3, 'idle-rows', 'AppDb', 'dbo', $4, 'PROCEDURE', 0, 0, 0, 0, 0, 0, $5, $6, $6, $7)";
        foreach (var v in new object[] { _nextId--, at, serverId, objectName, executions, elapsedUs, interval })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedRunsAsync(int serverId, string collector, DateTime firstUtc, DateTime lastUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT {_nextId} - row_number() OVER (), {serverId}, 'idle-rows', '{collector}', g.t, 12, 'SUCCESS', 0
FROM generate_series(TIMESTAMP '{firstUtc:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{lastUtc:yyyy-MM-dd HH:mm:ss}', INTERVAL 1 MINUTE) AS g(t)";
        _nextId -= await cmd.ExecuteNonQueryAsync() + 1;
    }
}

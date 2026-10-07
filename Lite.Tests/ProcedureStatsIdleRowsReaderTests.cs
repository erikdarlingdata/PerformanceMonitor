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
        /* The collector ran every minute on the new server, which is how its quiet minutes are known; the old server's
           stored idle rows are its own record of them. */
        await SeedRunsAsync(NewServer, "procedure_stats", t0.AddMinutes(1), t0.AddMinutes(20));
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

        /* 1,200 ms of work over the bucket's nine one-minute points (minutes 1-9, 540 s), not over the two stored
           collections' 120 s (which read 10). The first point's own interval is the store's, 60 s. */
        Assert.Equal(1200.0 / 540.0, newPoints[0].Value!.Value, precision: 6);
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

    [Fact]
    public async Task ProcedureChartPoints_AQuietCollectionPlotsZero_OnBothStores()
    {
        var t0 = await SeedTwentyMinutesAsync();
        /* The collector ran every minute on both servers; only the old one kept a row for the quiet minutes. */
        await SeedRunsAsync(OldServer, "procedure_stats", t0.AddMinutes(1), t0.AddMinutes(20));
        await SeedRunsAsync(NewServer, "procedure_stats", t0.AddMinutes(1), t0.AddMinutes(20));

        var oldPoints = await _dataService.GetProcedureDurationTrendAsync(OldServer, 1, t0, t0.AddMinutes(25));
        var newPoints = await _dataService.GetProcedureDurationTrendAsync(NewServer, 1, t0, t0.AddMinutes(25));

        Assert.Equal(20, oldPoints.Count);
        Assert.Equal(oldPoints.Select(p => p.CollectionTime), newPoints.Select(p => p.CollectionTime));
        Assert.Equal(oldPoints.Select(p => p.Value), newPoints.Select(p => p.Value));
        /* Minute 3 is quiet: a plotted 0, where the line used to be drawn straight from minute 2 to minute 15. */
        Assert.Equal(0.0, newPoints.Single(p => p.CollectionTime == t0.AddMinutes(3)).Value!.Value, precision: 6);
        Assert.Equal(10.0, newPoints.Single(p => p.CollectionTime == t0.AddMinutes(15)).Value!.Value, precision: 6);
    }

    [Fact]
    public async Task BucketedProcedureTrend_AQuietBucketIsAZeroPoint_OnBothStores()
    {
        var t0 = await SeedTwentyMinutesAsync();
        await SeedRunsAsync(OldServer, "procedure_stats", t0.AddMinutes(1), t0.AddMinutes(20));
        await SeedRunsAsync(NewServer, "procedure_stats", t0.AddMinutes(1), t0.AddMinutes(20));
        var asOf = t0.AddMinutes(25);

        /* One-minute buckets: the minutes between the work are whole buckets with no stored row. */
        var oldPoints = await _dataService.GetBucketedProcedureDurationTrendAsync(OldServer, 1, asOf, 1);
        var newPoints = await _dataService.GetBucketedProcedureDurationTrendAsync(NewServer, 1, asOf, 1);

        Assert.Equal(20, oldPoints.Count);
        Assert.Equal(oldPoints.Select(p => p.CollectionTime), newPoints.Select(p => p.CollectionTime));
        Assert.Equal(oldPoints.Select(p => p.Value), newPoints.Select(p => p.Value));
    }

    [Fact]
    public async Task ProcedureChartPoints_AFailedRunIsNotAQuietPoint()
    {
        var t0 = TenMinuteFloor(DateTime.UtcNow.AddMinutes(-40));
        await SeedAsync(NewServer, t0.AddMinutes(1), "usp_Work", executions: 10, elapsedUs: 600_000, interval: 60);
        await SeedAsync(NewServer, t0.AddMinutes(5), "usp_Work", executions: 10, elapsedUs: 600_000, interval: 240);
        await SeedRunsAsync(NewServer, "procedure_stats", t0.AddMinutes(1), t0.AddMinutes(5));
        await SetRunStatusAsync(NewServer, t0.AddMinutes(3), "ERROR");

        var points = await _dataService.GetProcedureDurationTrendAsync(NewServer, 1, t0, t0.AddMinutes(25));

        /* The runs at minutes 2 and 4 stored nothing and succeeded: zero points. Minute 3 failed: no point at all. */
        Assert.Equal(new[] { 1, 2, 4, 5 }, points.Select(p => (int)(p.CollectionTime - t0).TotalMinutes).ToArray());
    }

    [Fact]
    public async Task ProcedureHistoryChart_AQuietRunPlotsZero_OnBothStores()
    {
        var t0 = await SeedTwentyMinutesAsync();
        /* The runs reach past the procedure's first and last row: a 0 there would be invented (it was not cached yet, or had left). */
        await SeedRunsAsync(OldServer, "procedure_stats", t0.AddMinutes(-5), t0.AddMinutes(30));
        await SeedRunsAsync(NewServer, "procedure_stats", t0.AddMinutes(-5), t0.AddMinutes(30));

        var oldChart = await ChartRowsAsync(OldServer, t0);
        var newChart = await ChartRowsAsync(NewServer, t0);

        Assert.Equal(20, oldChart.Count);
        Assert.Equal(oldChart.Select(r => r.CollectionTime), newChart.Select(r => r.CollectionTime));
        foreach (var metric in new Func<ProcedureStatsHistoryRow, double>[] { r => r.DeltaExecutions, r => r.DeltaCpuMs, r => r.AvgCpuMs, r => r.DeltaLogicalReads })
        {
            Assert.Equal(oldChart.Select(metric), newChart.Select(metric));
        }
        Assert.Equal(0, newChart.Single(r => r.CollectionTime == t0.AddMinutes(3)).DeltaExecutions);
        Assert.Equal(10, newChart.Single(r => r.CollectionTime == t0.AddMinutes(15)).DeltaExecutions);
        /* The grid still lists only the minutes with work. */
        var grid = await _dataService.GetProcedureStatsHistoryAsync(NewServer, "AppDb", "dbo", "usp_Work", 1, t0, t0.AddMinutes(25));
        Assert.Equal(4, grid.Count);
    }

    [Fact]
    public async Task ProcedureHistoryChart_AFailedRunIsNotAQuietPoint_AndAnotherProceduresRowDoesNotFillTheMinute()
    {
        var t0 = TenMinuteFloor(DateTime.UtcNow.AddMinutes(-40));
        await SeedAsync(NewServer, t0.AddMinutes(1), "usp_Work", executions: 10, elapsedUs: 600_000, interval: 60);
        await SeedAsync(NewServer, t0.AddMinutes(5), "usp_Work", executions: 10, elapsedUs: 600_000, interval: 240);
        /* Another procedure worked at minute 2: the run at minute 2 stored a row, but not for usp_Work, so it is still a 0 for usp_Work. */
        await SeedAsync(NewServer, t0.AddMinutes(2), "usp_Other", executions: 3, elapsedUs: 100_000, interval: 60);
        await SeedRunsAsync(NewServer, "procedure_stats", t0.AddMinutes(1), t0.AddMinutes(5));
        await SetRunStatusAsync(NewServer, t0.AddMinutes(3), "ERROR");

        var chart = await ChartRowsAsync(NewServer, t0);

        Assert.Equal(new[] { 1, 2, 4, 5 }, chart.Select(r => (int)(r.CollectionTime - t0).TotalMinutes).ToArray());
    }

    /// <summary>#5449 M2: one collection holds a procedure that was measured over 60 s and one that came back after 1,800 s. The whole
    /// delta lands in the run that stored it, over that run's gap to the previous point (60 s), never over the longer stored interval:
    /// the MAX of the collection's stored intervals (1,800 s) read the minute 30 times too low.</summary>
    [Fact]
    public async Task ProcedureChartPoints_AReturningProceduresDeltaIsRatedOverTheRunsGap()
    {
        var t = TenMinuteFloor(DateTime.UtcNow.AddMinutes(-40)).AddMinutes(5);
        await SeedAsync(NewServer, t.AddMinutes(-1), "usp_Prev", executions: 0, elapsedUs: 0, interval: 60);
        await SeedAsync(NewServer, t, "usp_P1", executions: 10, elapsedUs: 600_000, interval: 60);
        await SeedAsync(NewServer, t, "usp_P2", executions: 20, elapsedUs: 1_200_000, interval: 1800);
        await SeedRunsAsync(NewServer, "procedure_stats", t.AddMinutes(-1), t);

        var points = await _dataService.GetProcedureDurationTrendAsync(NewServer, 1, t.AddMinutes(-2), t.AddMinutes(10));
        var bucketed = await _dataService.GetBucketedProcedureDurationTrendAsync(NewServer, 1, t.AddMinutes(10), 1);

        /* (600 + 1,200) ms and 30 executions over the run's own 60 s gap; the head read 1,800 s (MAX) and said 1 and 0.0167. */
        var point = points.Single(p => p.CollectionTime == t);
        Assert.Equal(30.0, point.Value!.Value, precision: 6);
        Assert.Equal(0.5, point.ExecutionsPerSecond!.Value, precision: 6);
        var bucket = bucketed.Single(p => p.CollectionTime == t);
        Assert.Equal(30.0, bucket.Value!.Value, precision: 6);
        Assert.Equal(30.0, bucket.PeakElapsedMsPerSecond!.Value, precision: 6);
    }

    /// <summary>#5449 M1: a 240-minute bucket with an outage inside it. The collector's runs are the axis, so the outage is not
    /// seconds the bucket was observed for: the first collection after it (past the collector's 3,600 s gap limit, here a store row
    /// of zero work) is unrated, and the bucket's seconds are the 119 one-minute points on either side, 7,140 s. The head
    /// stretched the denominator over the bucket's whole span (14,400 s) and read the busy hours at half their rate.</summary>
    [Fact]
    public async Task BucketedProcedureTrend_AnOutageIsNotSeconds_AndTheCollectionAfterItIsUnrated()
    {
        var t0 = BucketFloor(240, DateTime.UtcNow.AddHours(-9));
        /* A point a minute before the window (it supplies the first gap), busy minutes 0-59 and 181-239, an outage 60-179, and a
           zero-work collection at minute 180 right after it. Runs stamp at t, rows land 200 ms later. */
        await SeedBusyMinutesAsync(NewServer, t0, -1, 59);
        await SeedBusyMinutesAsync(NewServer, t0, 181, 239);
        await SeedAsync(NewServer, t0.AddMinutes(180).AddMilliseconds(200), "usp_Work", executions: 0, elapsedUs: 0, interval: 60);
        await SeedRunsAsync(NewServer, "procedure_stats", t0.AddMinutes(180), t0.AddMinutes(180));

        var points = await _dataService.GetBucketedProcedureDurationTrendAsync(NewServer, 4, t0.AddHours(4), 240);

        var point = Assert.Single(points);
        /* 119 busy minutes of 1,000 ms and 1 execution each, over 60 + 59 rated minutes. */
        Assert.Equal(119_000.0 / 7140.0, point.Value!.Value, precision: 6);
        Assert.Equal(119.0 / 7140.0, point.ExecutionsPerSecond!.Value, precision: 6);
        Assert.Equal(1, point.UnratedInBucket);
    }

    /// <summary>The 4-hour-aligned (any whole-hour width that divides a day) floor of <paramref name="from"/>, the bucket origin being midnight.</summary>
    private static DateTime BucketFloor(int minutes, DateTime from) =>
        DateTime.SpecifyKind(new DateTime(from.Ticks - (from.Ticks % TimeSpan.FromMinutes(minutes).Ticks)), DateTimeKind.Unspecified);

    /// <summary>A busy collection every minute from <paramref name="firstMinute"/> to <paramref name="lastMinute"/> after <paramref name="t0"/>:
    /// the log stamps the run at the minute, the rows land 200 ms later (1,000 ms, one execution, a 60 s interval).</summary>
    private async Task SeedBusyMinutesAsync(int serverId, DateTime t0, int firstMinute, int lastMinute)
    {
        await SeedRunsAsync(serverId, "procedure_stats", t0.AddMinutes(firstMinute), t0.AddMinutes(lastMinute));
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
     execution_count, total_worker_time, total_elapsed_time, total_logical_reads, total_physical_reads, total_logical_writes,
     delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
SELECT {_nextId} - row_number() OVER (), g.t + INTERVAL 200 MILLISECOND, {serverId}, 'idle-rows', 'AppDb', 'dbo', 'usp_Work', 'PROCEDURE',
       0, 0, 0, 0, 0, 0, 1, 1000000, 1000000, 60
FROM generate_series(TIMESTAMP '{t0.AddMinutes(firstMinute):yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{t0.AddMinutes(lastMinute):yyyy-MM-dd HH:mm:ss}', INTERVAL 1 MINUTE) AS g(t)";
        _nextId -= await cmd.ExecuteNonQueryAsync() + 1;
    }

    [Fact]
    public void IdleRunTimes_ARunOwnsTheRowsUpToTheNextRun_WhetherStampedBeforeOrAtTheRows()
    {
        var t = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var runs = Enumerable.Range(0, 8).Select(m => t.AddMinutes(m)).ToList();
        /* Rows land two seconds after their run's stamp (minutes 1 and 6) or on it (minute 4). */
        var rows = new List<DateTime> { t.AddMinutes(1).AddSeconds(2), t.AddMinutes(4), t.AddMinutes(6).AddSeconds(2) };

        var idle = ProcedureHistoryIdleRuns.IdleRunTimes(rows, runs);

        /* Minutes 2, 3 and 5 own no row. Minute 1 owns the first row, 4 and 6 their rows; 0 and 7 lie outside the first and last row. */
        Assert.Equal(new[] { 2, 3, 5 }, idle.Select(i => (int)(i - t).TotalMinutes).ToArray());
        Assert.Empty(ProcedureHistoryIdleRuns.IdleRunTimes(new List<DateTime>(), runs));
    }

    private async Task<List<ProcedureStatsHistoryRow>> ChartRowsAsync(int serverId, DateTime t0)
    {
        var history = await _dataService.GetProcedureStatsHistoryAsync(serverId, "AppDb", "dbo", "usp_Work", 1, t0, t0.AddMinutes(40));
        return await _dataService.GetProcedureStatsHistoryChartRowsAsync(serverId, history);
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

    private async Task SetRunStatusAsync(int serverId, DateTime at, string status)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"UPDATE collection_log SET status = '{status}' WHERE server_id = {serverId} AND collection_time = TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}'";
        await cmd.ExecuteNonQueryAsync();
    }
}

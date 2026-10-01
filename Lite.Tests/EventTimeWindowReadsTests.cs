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
/// A windowed blocked-process-report / deadlock read answers "what happened in this window", so it windows on
/// the EVENT's own time, not on when the collector stored the row. Three rows per table pin it:
/// (a) event 2h before the window, collected inside it; (b) both inside; (c) event inside, collected 30 min
/// after the window end. Reads return b and c, never a. The alert engine's reads are the exception: they are a
/// delivery cursor on collection time, so a late-collected event still alerts (pinned here too).
/// </summary>
public sealed class EventTimeWindowReadsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9931;
    private const string Graph = "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list><process id=\"p1\" spid=\"55\" waittime=\"1000\"><inputbuf>x</inputbuf></process></process-list><resource-list/></deadlock>";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;
    private long _id = 1;

    private static readonly DateTime Start = Floor(DateTime.UtcNow.AddHours(-10));
    private static readonly DateTime End = Start.AddHours(4);

    public EventTimeWindowReadsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    private static DateTime Floor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerHour)), DateTimeKind.Unspecified);

    private async Task SeedAbcAsync()
    {
        foreach (var (evt, coll) in new[]
        {
            (Start.AddHours(-2), Start.AddHours(1)),       // a
            (Start.AddHours(1), Start.AddHours(1).AddMinutes(1)),   // b
            (End.AddMinutes(-10), End.AddMinutes(30)),      // c
        })
        {
            await SeedDeadlockAsync(evt, coll);
            await SeedBprAsync(evt, coll);
        }
    }

    private async Task SeedDeadlockAsync(DateTime evt, DateTime coll)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _conn ??= await OpenAsync();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _id++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = coll });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = evt });
        cmd.Parameters.Add(new DuckDBParameter { Value = Graph });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedBprAsync(DateTime evt, DateTime coll)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _conn ??= await OpenAsync();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_last_tran_started, blocking_spid, blocking_last_tran_started,
     wait_time_ms, lock_mode, blocked_status, blocking_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _id++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = coll });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = evt });
        cmd.Parameters.Add(new DuckDBParameter { Value = "DB1" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 55 });
        cmd.Parameters.Add(new DuckDBParameter { Value = evt });
        cmd.Parameters.Add(new DuckDBParameter { Value = 66 });
        cmd.Parameters.Add(new DuckDBParameter { Value = evt });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "X" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "suspended" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "running" });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> OpenAsync()
    {
        var c = _duckDb.CreateConnection();
        await c.OpenAsync();
        return c;
    }

    [Fact]
    public async Task DeadlockGrid_WindowsOnEventTime()
    {
        await SeedAbcAsync();
        var rows = await _service.GetRecentDeadlocksAsync(ServerId, 0, Start, End);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.True(r.DeadlockTime >= Start));
    }

    [Fact]
    public async Task BprGrid_WindowsOnEventTime()
    {
        await SeedAbcAsync();
        var rows = await _service.GetRecentBlockedProcessReportsAsync(ServerId, 0, Start, End);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.True(r.EventTime >= Start));
    }

    [Fact]
    public async Task Slicers_WindowOnEventTime()
    {
        await SeedAbcAsync();
        var bpr = await _service.GetBlockingSlicerDataAsync(ServerId, 0, Start, End);
        var dl = await _service.GetDeadlockSlicerDataAsync(ServerId, 0, Start, End);
        Assert.Equal(2, bpr.Sum(b => b.SessionCount));
        Assert.Equal(2, dl.Sum(b => b.SessionCount));
        /* b and c land in the buckets their events fall in, not their collection hour. */
        Assert.Contains(dl, b => b.BucketTime == Start.AddHours(1));
        Assert.Contains(dl, b => b.BucketTime == Start.AddHours(3));
    }

    [Fact]
    public async Task DeadlockTrend_WindowsOnEventTime()
    {
        await SeedAbcAsync();
        var trend = await _service.GetDeadlockTrendAsync(ServerId, 0, Start, End);
        Assert.Equal(2, trend.Sum(t => t.Count));
        Assert.All(trend, t => Assert.True(t.Time >= Start));
    }

    [Fact]
    public async Task DeadlockSeverityStats_WindowsOnEventTime()
    {
        await SeedAbcAsync();
        var points = await _service.GetDeadlockSeverityStatsAsync(ServerId, 0, Start, End);
        Assert.Equal(2, points.Count);
        Assert.All(points, p => Assert.True(p.Time >= Start));
    }

    [Fact]
    public async Task AlertCounts_WindowOnEventTime()
    {
        await SeedAbcAsync();
        var (blocking, deadlock, latest) = await _service.GetAlertCountsAsync(ServerId, 0, Start, End);
        Assert.Equal(2, blocking);
        Assert.Equal(2, deadlock);
        Assert.Equal(End.AddMinutes(-10), latest);
    }

    [Fact]
    public async Task OverviewDeadlockCount_AgreesWithGrid_SameWindow()
    {
        var now = DateTime.UtcNow;
        await SeedDeadlockAsync(now.AddHours(-2), now.AddMinutes(-10));   // old event, collected recently
        await SeedDeadlockAsync(now.AddMinutes(-30), now.AddMinutes(-29));
        var grid = await _service.GetRecentDeadlocksAsync(ServerId, 1);
        var summary = await _service.GetServerSummaryAsync(ServerId, "TestSrv", null);
        Assert.NotNull(summary);
        Assert.Single(grid);
        Assert.Equal(grid.Count, summary!.DeadlockCount);
    }

    [Fact]
    public async Task DailySummary_CountsDeadlockOnTheDayItHappened()
    {
        var day = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
        await SeedDeadlockAsync(day.AddMinutes(-10), day.AddMinutes(10));   // 23:50 D-1, collected 00:10 D
        var rows = await _service.GetDailySummaryRangeAsync(ServerId, day.AddDays(-1), day.AddDays(1));
        var prev = Assert.Single(rows, r => r.SummaryDate.Date == day.AddDays(-1).Date);
        Assert.Equal(1, prev.DeadlockCount);
        Assert.DoesNotContain(rows, r => r.SummaryDate.Date == day.Date && r.DeadlockCount > 0);
    }

    [Fact]
    public async Task AlertReads_StayOnCollectionTime_LateCollectedEventStillAlerts()
    {
        var now = DateTime.UtcNow;
        await SeedDeadlockAsync(now.AddHours(-6), now.AddHours(-1));   // event long ago, collected inside 4h
        await SeedBprAsync(now.AddHours(-6), now.AddHours(-1));
        var adapter = new LiteAlertReadAdapter(_service);
        var dl = await adapter.GetRecentDeadlocksAsync(ServerId.ToString(), 4);
        var bpr = await adapter.GetRecentBlockedProcessReportsAsync(ServerId.ToString(), 4);
        Assert.Single(dl);
        Assert.Single(bpr);
    }
}

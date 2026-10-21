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
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6284, Lite's side: one deadlock and one blocked-process report from yesterday are listed by the grids and
/// counted by the Overview's Blocking &amp; Deadlocking lane over the same window, and the geometry the lane
/// draws with makes the one bucket a visible bar that reads 1 on hover. Before, the bar was a fixed 30 seconds wide
/// (under a pixel across 24 hours) and the hover reached one minute.
/// </summary>
public sealed class OverviewEventLaneTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9932;
    private const string Graph = "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list><process id=\"p1\" spid=\"55\" waittime=\"1000\"><inputbuf>x</inputbuf></process></process-list><resource-list/></deadlock>";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;

    public OverviewEventLaneTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn == null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task OneDeadlockAndOneBlockedReportYesterday_AreListed_Charted_AndReadOneOnHover()
    {
        var end = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        var start = end.AddDays(-1);
        var happened = start.AddHours(14).AddMinutes(7).AddSeconds(31);
        var collected = happened.AddMinutes(1);

        await ExecAsync(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml) VALUES ($1, $2, $3, $4, $5, $6)",
            1L, collected, ServerId, "TestSrv", happened, Graph);
        await ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_last_tran_started, blocking_spid, blocking_last_tran_started,
     wait_time_ms, lock_mode, blocked_status, blocking_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            2L, collected, ServerId, "TestSrv", happened, "DB1", 55, happened, 66, happened, 1000L, "X", "suspended", "running");

        var listed = await _service.GetRecentDeadlocksAsync(ServerId, 0, start, end);
        var deadlockBars = await _service.GetDeadlockTrendAsync(ServerId, 0, start, end);
        var blockingBars = await _service.GetBlockingTrendAsync(ServerId, 0, start, end);

        Assert.Single(listed);
        var deadlockBar = Assert.Single(deadlockBars);
        Assert.Equal(listed.Count, deadlockBars.Sum(b => b.Count));
        Assert.Equal(1, deadlockBar.Count);
        Assert.Equal(1, Assert.Single(blockingBars).Count);

        var span = (end - start).TotalDays;
        var pixelsPerDay = 1000 / span;
        var x = deadlockBar.Time.ToOADate();
        Assert.True(EventLaneGeometry.BarWidthDays(span) * pixelsPerDay >= 5);

        var tolerance = EventLaneGeometry.HoverToleranceDays(span);
        double[] times = [x];
        double[] values = [1];
        foreach (var pixelsAway in new[] { 0, 2, -2, 4, -4 })
            Assert.Equal(1, EventLaneGeometry.NearestValue(times, values, x + pixelsAway / pixelsPerDay, true, tolerance));
        Assert.Equal(0, EventLaneGeometry.NearestValue(times, values, x + 60 / pixelsPerDay, true, tolerance));
    }
}

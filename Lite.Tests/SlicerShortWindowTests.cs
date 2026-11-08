/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Shapes;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The time-range strip over a short window. Over the past hour, hourly buckets are one or two points, and the strip's line ends at the last
/// whole hour while the grid below holds rows up to the window's end. The Active Queries and Blocked Process Reports reads bucket a one-hour
/// window by the minute, and the strip, given a window and per-minute buckets, draws its line out to the right edge.
/// </summary>
public sealed class SlicerShortWindowTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public SlicerShortWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime MinuteFloor(DateTime at) => new(at.Ticks - at.Ticks % TimeSpan.TicksPerMinute, at.Kind);

    /* Two captures, six and three minutes ago, each a few seconds past the minute so the bucket stamp is the minute and not the row's time. */
    private static (DateTime Older, DateTime Newer) TwoMinutes()
    {
        var now = DateTime.UtcNow;
        var floor = MinuteFloor(now);
        var newer = floor.AddMinutes(-3).AddSeconds(7);
        var older = floor.AddMinutes(-6).AddSeconds(7);
        return (DateTime.SpecifyKind(older, DateTimeKind.Unspecified), DateTime.SpecifyKind(newer, DateTimeKind.Unspecified));
    }

    [Fact]
    public async Task TheActiveQueriesStrip_BucketsAOneHourWindowByTheMinute()
    {
        var (older, newer) = TwoMinutes();
        await SeedSnapshotAsync(older, 51);
        await SeedSnapshotAsync(newer, 52);
        await SeedSnapshotAsync(newer, 53);

        var buckets = await _dataService.GetActiveQuerySlicerDataAsync(_serverId, hoursBack: 1);

        Assert.Equal(new[] { MinuteFloor(older), MinuteFloor(newer) }, buckets.Select(b => b.BucketTime).ToArray());
        Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
    }

    [Fact]
    public async Task TheBlockedProcessReportsStrip_BucketsAOneHourWindowByTheMinute()
    {
        var (older, newer) = TwoMinutes();
        await SeedReportAsync(older);
        await SeedReportAsync(newer);
        await SeedReportAsync(newer);

        var buckets = await _dataService.GetBlockingSlicerDataAsync(_serverId, hoursBack: 1);

        Assert.Equal(new[] { MinuteFloor(older), MinuteFloor(newer) }, buckets.Select(b => b.BucketTime).ToArray());
        Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
    }

    /// <summary>
    /// A one-hour window from 12:25 to 13:25 with a bucket every minute: the strip's filled area reaches the right edge of the strip, where
    /// the last bucket sits. The same buckets taken by the hour end the area at 13:00, less than two thirds of the way across.
    /// </summary>
    [Fact]
    public void TheStrip_DrawsAOneHourWindowOutToItsRightEdge()
    {
        var start = new DateTime(2026, 11, 7, 12, 25, 0, DateTimeKind.Utc);
        var end = start.AddHours(1);
        var buckets = Enumerable.Range(0, 61).Select(i => new TimeSliceBucket { BucketTime = start.AddMinutes(i), Value = 1 + (i % 3) }).ToList();

        var (areaRight, stripWidth, drawnBuckets) = StaTestThread.Run(() =>
        {
            var slicer = new TimeRangeSlicerControl { Width = 800, Height = 130 };
            slicer.Measure(new Size(800, 130));
            slicer.Arrange(new Rect(0, 0, 800, 130));
            slicer.UpdateLayout();
            slicer.LoadData(buckets, "Sessions", start, end);
            slicer.Redraw();

            var area = slicer.SlicerCanvas.Children.OfType<Path>().Single(p => p.Fill is not null && p.Stroke is null);
            return (area.Data.Bounds.Right, slicer.SlicerBorder.ActualWidth, slicer.DrawnRuns.Sum(r => r.Count));
        });

        Assert.True(stripWidth > 100, $"the strip laid out {stripWidth} px wide");
        /* Every minute bucket is a point of its own; folded into hours the strip would be two points, one at 12:00 and one at 13:00. */
        Assert.Equal(61, drawnBuckets);
        Assert.True(areaRight >= stripWidth - 1, $"the area ends at {areaRight:F1} of {stripWidth:F1} px");
    }

    /// <summary>
    /// The Deadlocks strip keeps its hourly buckets over a short window. Past hour at 13:25 with two deadlocks at 12:40 and 12:50: the read
    /// returns one bucket stamped 12:00 with a count of 2, and the strip draws it (two hourly stamps, 12:00 and 13:00, and an area with
    /// height) instead of walking minute stamps from 12:25 and dropping the bucket stamped before the window.
    /// </summary>
    [Fact]
    public void TheDeadlocksStrip_KeepsAnHourlyBucketStampedBeforeAOneHourWindow()
    {
        var start = new DateTime(2026, 11, 7, 12, 25, 0, DateTimeKind.Utc);
        var end = start.AddHours(1);
        var buckets = new List<TimeSliceBucket> { new() { BucketTime = new DateTime(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc), SessionCount = 2, Value = 2 } };

        var (drawnBuckets, areaHeight) = StaTestThread.Run(() =>
        {
            var slicer = new TimeRangeSlicerControl { Width = 800, Height = 130 };
            slicer.Measure(new Size(800, 130));
            slicer.Arrange(new Rect(0, 0, 800, 130));
            slicer.UpdateLayout();
            slicer.LoadData(buckets, "Deadlocks", start, end);
            slicer.Redraw();

            var area = slicer.SlicerCanvas.Children.OfType<Path>().Single(p => p.Fill is not null && p.Stroke is null);
            return (slicer.DrawnRuns.Sum(r => r.Count), area.Data.Bounds.IsEmpty ? 0 : area.Data.Bounds.Height);
        });

        Assert.Equal(2, drawnBuckets);
        Assert.True(areaHeight > 1, $"the strip is flat: the area is {areaHeight:F1} px tall");
    }

    private Task SeedSnapshotAsync(DateTime at, int session) => ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, cpu_time_ms, total_elapsed_time_ms, program_name, request_id)
VALUES ($1, $2, $3, $4, $5, 'TestDb', 'SELECT 1', 'running', 0, 10, 20, 'Orders.Api', 0)",
        _nextId--, at, _serverId, ServerName, session);

    private Task SeedReportAsync(DateTime at) => ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms)
VALUES ($1, $2, $3, $4, $2, 'TestDb', 71, 72, 5000)",
        _nextId--, at, _serverId, ServerName);

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

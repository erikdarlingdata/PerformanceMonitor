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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A read that places a row by an instant takes the window (start, end]: a row stamped exactly at the start belongs to
/// the window before, a row stamped exactly at the end belongs to this one, and two windows that share an edge return
/// each row once. Three reads of different kinds pin it: an event list (deadlocks, by deadlock_time), a snapshot read
/// (waiting tasks, by collection_time, on the hours-back and as-of path the MCP tool takes) and a summed delta read
/// (wait stats). Five rows sit at 08:00, 08:30, 09:00, 09:30 and 10:00; the windows are (08:00, 09:00] and
/// (09:00, 10:00]. A time strip selection, whose first bar counts the row on its left edge, reads that row through
/// <see cref="SlicerRangeEventArgs.ReadStartUtc"/>.
/// </summary>
public sealed class WindowStartEdgeReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9941;
    private const string Graph = "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list><process id=\"p1\" spid=\"55\" waittime=\"1000\"><inputbuf>x</inputbuf></process></process-list><resource-list/></deadlock>";

    private static readonly DateTime Start = new(2026, 3, 10, 8, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Middle = Start.AddHours(1);
    private static readonly DateTime End = Start.AddHours(2);

    /* The five stamps: on the first start, inside the first window, on the shared edge, inside the second window, on the last end. */
    private static readonly DateTime[] Stamps = { Start, Start.AddMinutes(30), Middle, Middle.AddMinutes(30), End };

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;
    private long _id = 1;

    public WindowStartEdgeReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    private async Task ExecuteAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One deadlock at <paramref name="deadlockTime"/>, collected thirty seconds later.</summary>
    private Task SeedDeadlockAsync(DateTime deadlockTime) => ExecuteAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, 'TestSrv', $4, $5)",
        _id++, deadlockTime.AddSeconds(30), ServerId, deadlockTime, Graph);

    private Task SeedWaitingTaskAsync(DateTime collectionTime) => ExecuteAsync(@"
INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, database_name)
VALUES ($1, $2, $3, 'TestSrv', 55, 'LCK_M_X', 3000, 60, 'Db')",
        _id++, collectionTime, ServerId);

    private Task SeedWaitDeltaAsync(DateTime collectionTime, long deltaMs) => ExecuteAsync(@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
    delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, 'TestSrv', 'PAGEIOLATCH_SH', 5000, $4, 0, 1, $4, 0)",
        _id++, collectionTime, ServerId, deltaMs);

    private async Task<List<DateTime>> DeadlockTimesAsync(DateTime from, DateTime to)
        => (await _service.GetRecentDeadlocksAsync(ServerId, 0, from, to)).Select(r => Naive(r.DeadlockTime!.Value)).OrderBy(t => t).ToList();

    private async Task<List<DateTime>> WaitingTaskTimesAsync(int hoursBack, DateTime asOf)
        => (await _service.GetWaitingTasksAsync(ServerId, hoursBack, asOfUtc: asOf)).Select(r => Naive(r.CollectionTime)).OrderBy(t => t).ToList();

    private async Task<long> WaitMsAsync(DateTime from, DateTime to)
        => (await _service.GetWaitStatsAsync(ServerId, 0, from, to)).Where(r => r.WaitType == "PAGEIOLATCH_SH").Sum(r => r.TotalWaitTimeMs);

    [Fact]
    public async Task TheDeadlockList_LeavesOutTheRowAtItsStart_AndReturnsTheRowAtItsEnd()
    {
        foreach (var stamp in Stamps) await SeedDeadlockAsync(stamp);

        Assert.Equal(new[] { Start.AddMinutes(30), Middle }, await DeadlockTimesAsync(Start, Middle));
    }

    [Fact]
    public async Task TwoDeadlockWindowsBackToBack_ReturnEachRowOnce()
    {
        foreach (var stamp in Stamps) await SeedDeadlockAsync(stamp);

        var first = await DeadlockTimesAsync(Start, Middle);
        var second = await DeadlockTimesAsync(Middle, End);

        Assert.Equal(new[] { Middle.AddMinutes(30), End }, second);
        Assert.Equal(Stamps.Skip(1), first.Concat(second));
        Assert.Equal(Stamps.Skip(1), await DeadlockTimesAsync(Start, End));
    }

    [Fact]
    public async Task TheWaitingTasksRead_LeavesOutTheSnapshotAtItsStart_AndReturnsTheSnapshotAtItsEnd()
    {
        foreach (var stamp in Stamps) await SeedWaitingTaskAsync(stamp);

        Assert.Equal(new[] { Start.AddMinutes(30), Middle }, await WaitingTaskTimesAsync(1, Middle));
    }

    [Fact]
    public async Task TwoWaitingTasksWindowsBackToBack_ReturnEachSnapshotOnce()
    {
        foreach (var stamp in Stamps) await SeedWaitingTaskAsync(stamp);

        var first = await WaitingTaskTimesAsync(1, Middle);
        var second = await WaitingTaskTimesAsync(1, End);

        Assert.Equal(new[] { Middle.AddMinutes(30), End }, second);
        Assert.Equal(Stamps.Skip(1), first.Concat(second));
        Assert.Equal(Stamps.Skip(1), await WaitingTaskTimesAsync(2, End));
    }

    /// <summary>The deltas are 1, 10, 100, 1000 and 10000 ms in stamp order, so a sum names the rows it holds.</summary>
    private async Task SeedWaitDeltasAsync()
    {
        long delta = 1;
        foreach (var stamp in Stamps)
        {
            await SeedWaitDeltaAsync(stamp, delta);
            delta *= 10;
        }
    }

    [Fact]
    public async Task TheWaitStatsSum_LeavesOutTheDeltaAtItsStart_AndCountsTheDeltaAtItsEnd()
    {
        await SeedWaitDeltasAsync();

        Assert.Equal(110L, await WaitMsAsync(Start, Middle));
    }

    [Fact]
    public async Task TwoWaitStatsWindowsBackToBack_CountEachDeltaOnce()
    {
        await SeedWaitDeltasAsync();

        var first = await WaitMsAsync(Start, Middle);
        var second = await WaitMsAsync(Middle, End);

        Assert.Equal(11000L, second);
        Assert.Equal(11110L, first + second);
        Assert.Equal(11110L, await WaitMsAsync(Start, End));
    }

    private static readonly DateTime StripStart = new(2026, 10, 22, 10, 37, 20, DateTimeKind.Utc);
    private static readonly DateTime StripEnd = StripStart.AddHours(24);

    /// <summary>The range the real time strip sends when <paramref name="chip"/> is clicked, loaded with hourly bars over a day that does not start on the hour.</summary>
    private static SlicerRangeEventArgs ClickChip(string chip)
        => StaTestThread.Run(() =>
        {
            var slicer = new TimeRangeSlicerControl();
            slicer.Measure(new Size(800, 130));
            slicer.Arrange(new Rect(0, 0, 800, 130));
            var bars = Enumerable.Range(0, 23).Select(i => new TimeSliceBucket { BucketTime = new DateTime(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc).AddHours(i), Value = 10 + i }).ToList();
            slicer.LoadData(bars, "Deadlocks", StripStart, StripEnd);

            SlicerRangeEventArgs? sent = null;
            slicer.RangeChanged += (_, e) => sent = e;

            var chips = (StackPanel)slicer.FindName("QuickRangeChips");
            var button = chips.Children.OfType<Button>().Single(b => Equals(b.Content, chip));
            button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

            Assert.NotNull(sent);
            return sent!;
        });

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>The one-hour chip starts on the 09:00 bar. That bar counts a deadlock stamped 09:00:00 exactly, and the grid read
    /// the selection drives returns it, with the deadlock stamped at the selection's end.</summary>
    [Fact]
    public async Task ASelectionInsideTheStrip_ReadsTheDeadlockOnItsFirstBarsLeftEdge()
    {
        var sent = ClickChip("1h");
        Assert.True(sent.StartUtc > StripStart);
        await SeedDeadlockAsync(Naive(sent.StartUtc).AddMinutes(-1));
        await SeedDeadlockAsync(Naive(sent.StartUtc));
        await SeedDeadlockAsync(Naive(sent.EndUtc));

        Assert.Equal(new[] { Naive(sent.StartUtc), Naive(sent.EndUtc) }, await DeadlockTimesAsync(sent.ReadStartUtc, sent.EndUtc));
    }

    /// <summary>"All" selects the loaded window, whose bars and grid load both read (start, end]: its read start is the window's
    /// start, so the selection reads the rows the load reads and not a row stamped on the window's start.</summary>
    [Fact]
    public async Task ASelectionOfTheWholeStrip_ReadsWhatTheLoadReads()
    {
        var sent = ClickChip("All");
        await SeedDeadlockAsync(Naive(StripStart));
        await SeedDeadlockAsync(Naive(StripStart).AddMinutes(1));

        Assert.Equal(StripStart, sent.ReadStartUtc);
        Assert.Equal(new[] { Naive(StripStart).AddMinutes(1) }, await DeadlockTimesAsync(sent.ReadStartUtc, sent.EndUtc));
    }
}

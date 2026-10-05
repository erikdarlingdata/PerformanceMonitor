/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Overview blocking chart's "Showing since" note (#4966) against a real store, through the same step the control runs
/// (<see cref="LiteBlockingLaneDataStart.ShowAsync"/>): the notice, the quiet start, one series answering, a failed probe and a short window.
/// Anchors are fixed so the tests never straddle a day boundary.
/// </summary>
[Collection("server-time-helper")]
public sealed class OverviewBlockingLaneDataStartLiveTests : IDisposable
{
    private const int ServerId = 4966;
    private const string ServerName = "OverviewBlockingServer";
    private static readonly DateTime End = new(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified);
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public OverviewBlockingLaneDataStartLiveTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "OverviewBlockingLane_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static string CollectorOf(QueryWindowRelation relation) =>
        relation == QueryWindowRelation.BlockedProcessReports ? "blocked_process_report" : "deadlocks";

    private async Task SeedLogRunsAsync(QueryWindowRelation relation, DateTime firstUtc, DateTime lastUtc)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, $4, g.t, 12, 'SUCCESS', 0
FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL 30 MINUTE) AS g(t)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = CollectorOf(relation) });
        cmd.Parameters.Add(new DuckDBParameter { Value = firstUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastUtc });
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    private static string SinceText(DateTime instant) =>
        "Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(instant, TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss");

    private static TrendPoint Bar(DateTime at, int count = 1) => new() { Time = at, Count = count };

    private async Task<(bool Visible, string Text)> NoteAsync(
        DateTime startUtc, DateTime endUtc, IEnumerable<TrendPoint> blockingBars, IEnumerable<TrendPoint> deadlockBars,
        Func<QueryWindowRelation, Task<DateTime?>>? floorOf = null)
    {
        var service = new LocalDataService(_duckDb);
        floorOf ??= relation => service.GetQueryWindowFloorAsync(relation, ServerId, startUtc, endUtc);
        /* The real note step, end to end, on an STA thread: the banner is a WPF object. No sync context there, so the blocking wait is safe. */
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            LiteBlockingLaneDataStart.ShowAsync(banner, floorOf, startUtc, endUtc, blockingBars, deadlockBars, TimeZoneInfo.Utc).GetAwaiter().GetResult();
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });
    }

    /// <summary>Blocking was collected from 2 days back, deadlocks from 3: the one note names the later start, in the chart's zone.</summary>
    [Fact]
    public async Task Notice_NamesTheLaterOfTheTwoSeriesStarts()
    {
        await _duckDb.InitializeAsync();
        var blockingFrom = End.AddDays(-2);
        await SeedLogRunsAsync(QueryWindowRelation.BlockedProcessReports, blockingFrom, End);
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, End.AddDays(-3), End);

        var (visible, text) = await NoteAsync(End.AddDays(-7), End, [], []);

        Assert.True(visible);
        Assert.Equal(SinceText(blockingFrom), text);
    }

    /// <summary>Both collectors ran across the whole range and the first bars come late: no note, and a zero bucket changes nothing.</summary>
    [Fact]
    public async Task QuietStart_BothCollectorsCoveredTheRange_ShowsNoNote()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync(QueryWindowRelation.BlockedProcessReports, End.AddDays(-9), End);
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, End.AddDays(-9), End);

        var (visible, text) = await NoteAsync(
            End.AddDays(-7), End, [Bar(End.AddDays(-7).AddHours(5)), Bar(End.AddHours(-1))], [Bar(End.AddDays(-7), 0), Bar(End.AddHours(-2))]);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>One collector covers the range and the other began 2 days ago: the later start is named, not the covered one.</summary>
    [Fact]
    public async Task OneSeriesCovered_TheOtherStartedLate_NamesTheLateOne()
    {
        await _duckDb.InitializeAsync();
        var lateFrom = End.AddDays(-2);
        await SeedLogRunsAsync(QueryWindowRelation.BlockedProcessReports, End.AddDays(-9), End);
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, lateFrom, End);

        var (visible, text) = await NoteAsync(End.AddDays(-7), End, [], []);

        Assert.True(visible);
        Assert.Equal(SinceText(lateFrom), text);
    }

    /// <summary>A probe that throws costs only its own series' start; the other still names the note.</summary>
    [Fact]
    public async Task AFailedProbe_CostsOnlyItsSeries()
    {
        await _duckDb.InitializeAsync();
        var from = End.AddDays(-2);
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, from, End);
        var service = new LocalDataService(_duckDb);

        var (visible, text) = await NoteAsync(End.AddDays(-7), End, [], [],
            relation => relation == QueryWindowRelation.BlockedProcessReports
                ? Task.FromException<DateTime?>(new InvalidOperationException("probe failed"))
                : service.GetQueryWindowFloorAsync(relation, ServerId, End.AddDays(-7), End));

        Assert.True(visible);
        Assert.Equal(SinceText(from), text);
    }

    /// <summary>A failed blocking probe with a late bar names no start; the covered deadlock series starts at the window start, so no note.</summary>
    [Fact]
    public async Task AFailedProbe_WithALateBar_AndACoveredOtherSeries_ShowsNoNote()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, End.AddDays(-9), End);
        var service = new LocalDataService(_duckDb);

        var (visible, text) = await NoteAsync(End.AddDays(-7), End, [Bar(End.AddHours(-1))], [],
            relation => relation == QueryWindowRelation.BlockedProcessReports
                ? Task.FromException<DateTime?>(new InvalidOperationException("probe failed"))
                : service.GetQueryWindowFloorAsync(relation, ServerId, End.AddDays(-7), End));

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    private Task<(bool Visible, string Text)> LaneNoteWithSourceCheckAsync(DateTime startUtc, DateTime endUtc)
    {
        var service = new LocalDataService(_duckDb);
        return Task.FromResult(OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            LiteBlockingLaneDataStart.ShowAsync(
                banner, relation => service.GetQueryWindowFloorAsync(relation, ServerId, startUtc, endUtc), startUtc, endUtc, [], [], TimeZoneInfo.Utc,
                () => service.HasBlockedProcessReportsInWindowAsync(ServerId, startUtc, endUtc),
                () => service.GetQueryWindowFloorAsync(QueryWindowRelation.BlockedProcessReports, ServerId, startUtc, endUtc, includeAlsoCovered: false)).GetAwaiter().GetResult();
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        }));
    }

    private async Task SeedDmvCoverageAsync(DateTime firstUtc, DateTime lastUtc)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, 'dmv_blocking_snapshot', g.t, 12, 'SUCCESS', 0
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL 30 MINUTE) AS g(t)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = firstUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = lastUtc });
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    private async Task SeedXeReportAsync(DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time)
VALUES ($1, $2, $3, $4, $2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>#5098: the DMV covers the range, the XE collector began 2 days back and its reports are in the window: the lane names the XE start.</summary>
    [Fact]
    public async Task DmvCoversTheRange_XeStartsMidway_NamesTheXeStart()
    {
        await _duckDb.InitializeAsync();
        var xeFrom = End.AddDays(-2);
        await SeedDmvCoverageAsync(End.AddDays(-9), End);
        await SeedLogRunsAsync(QueryWindowRelation.BlockedProcessReports, xeFrom, End);
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, End.AddDays(-9), End);
        await SeedXeReportAsync(xeFrom.AddHours(3));

        var (visible, text) = await LaneNoteWithSourceCheckAsync(End.AddDays(-7), End);

        Assert.True(visible);
        Assert.Equal(SinceText(xeFrom), text);
    }

    /// <summary>#5098: the XE collector covers the whole range: no note.</summary>
    [Fact]
    public async Task XeCoversTheRange_ShowsNoNote()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync(QueryWindowRelation.BlockedProcessReports, End.AddDays(-9), End);
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, End.AddDays(-9), End);
        await SeedXeReportAsync(End.AddDays(-7).AddHours(5));

        var (visible, _) = await LaneNoteWithSourceCheckAsync(End.AddDays(-7), End);

        Assert.False(visible);
    }

    /// <summary>#5098: no XE report in the window (DMV only): the two-source probe, as before, so a covering DMV means no note.</summary>
    [Fact]
    public async Task DmvOnly_IsAsBefore_ShowsNoNote()
    {
        await _duckDb.InitializeAsync();
        await SeedDmvCoverageAsync(End.AddDays(-9), End);
        await SeedLogRunsAsync(QueryWindowRelation.Deadlocks, End.AddDays(-9), End);

        var (visible, _) = await LaneNoteWithSourceCheckAsync(End.AddDays(-7), End);

        Assert.False(visible);
    }

    /// <summary>A window of 90 minutes or less starts no probe, even on a store that would earn a note.</summary>
    [Fact]
    public async Task AShortWindow_StartsNoProbe_AndShowsNoNote()
    {
        await _duckDb.InitializeAsync();
        var probes = 0;

        var (visible, _) = await NoteAsync(End.AddMinutes(-90), End, [], [], relation => { probes++; return Task.FromResult<DateTime?>(End); });

        Assert.False(visible);
        Assert.Equal(0, probes);
    }

    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }
}

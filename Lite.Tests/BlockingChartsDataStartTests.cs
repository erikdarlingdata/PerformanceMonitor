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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The "Showing since" note (#4966) on the Blocking tab's charts that draw zeros: the Lock Wait, Blocking and Deadlock
/// Trend charts and the two Blocking Stats chart pairs. Each surface gets a notice test (the data starts inside the window,
/// so the note names the right instant) and a quiet-start test (the window is covered but its first point comes late, so
/// there is no note). Each runs the same steps the tab runs: the shared probe, <see cref="ServerTab.EarlierOfFloorAndRowShown"/>
/// with the earliest point the chart draws, then <see cref="ServerTab.ApplyWindowFloorToBanner"/>.
/// </summary>
[Collection("server-time-helper")]
public sealed class BlockingChartsDataStartTests : IDisposable
{
    private const int ServerId = 4966;
    private const string ServerName = "BlockingChartsServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public BlockingChartsDataStartTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "BlockingChartsDataStart_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static string SinceText(DateTime instant) =>
        "Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(instant), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss");

    private static string CollectorOf(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.WaitStats => "wait_stats",
        QueryWindowRelation.BlockedProcessReports => "blocked_process_report",
        QueryWindowRelation.Deadlocks => "deadlocks",
        _ => throw new ArgumentOutOfRangeException(nameof(relation))
    };

    private async Task ExecAsync(string sql, params object[] args)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var a in args)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = a });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedRowAsync(QueryWindowRelation relation, DateTime at) => relation switch
    {
        QueryWindowRelation.WaitStats => ExecAsync(@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, 'LCK_M_X', 0, 0, 0, 10, 5, 1)", _nextId++, Naive(at), ServerId, ServerName),
        QueryWindowRelation.BlockedProcessReports => ExecAsync(@"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time)
VALUES ($1, $2, $3, $4, $2)", _nextId++, Naive(at), ServerId, ServerName),
        _ => ExecAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time)
VALUES ($1, $2, $3, $4, $2)", _nextId++, Naive(at), ServerId, ServerName)
    };

    private async Task SeedRunsAsync(QueryWindowRelation relation, DateTime firstUtc, DateTime lastUtc)
    {
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, $4, g.t, 12, 'SUCCESS', 0
FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL 30 MINUTE) AS g(t)",
            _nextId, ServerId, ServerName, CollectorOf(relation), Naive(firstUtc), Naive(lastUtc));
        _nextId += 100000;
    }

    /// <summary>The probe, the earlier-of step with the earliest point drawn, then the banner step: (visible, text).</summary>
    private async Task<(bool Visible, string Text)> NoteAsync(QueryWindowRelation relation, DateTime? earliestDrawn, DateTime startUtc, DateTime endUtc)
    {
        await Task.CompletedTask;
        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(relation, ServerId, startUtc, endUtc);
        floor = ServerTab.EarlierOfFloorAndRowShown(floor, earliestDrawn);
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.ApplyWindowFloorToBanner(banner, floor, startUtc, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });
    }

    /* The chart's own points, as the read returns them, and the helper the tab uses to find the earliest one drawn. */
    private static DateTime? DrawnFor(string surface, DateTime at) => surface switch
    {
        "BlockingTrend" or "DeadlockTrend" => ServerTab.EarliestBlockingTrendPointDrawn(new List<TrendPoint> { new() { Time = at, Count = 2 }, new() { Time = at.AddHours(-30), Count = 0 } }),
        "BlockingStats" => ServerTab.EarliestBlockingStatsPointDrawn(new List<BlockingDurationStatsPoint> { new(at, 1, 10, 10, 10) }),
        "DeadlockStats" => ServerTab.EarliestDeadlockStatsPointDrawn(new List<DeadlockSeverityStatsPoint> { new(at, 1, 10, 10, 10) }),
        _ => null /* Lock Wait is a rate series: coverage alone */
    };

    public static IEnumerable<object[]> Surfaces() => new[]
    {
        new object[] { "LockWait", QueryWindowRelation.WaitStats },
        new object[] { "BlockingTrend", QueryWindowRelation.BlockedProcessReports },
        new object[] { "DeadlockTrend", QueryWindowRelation.Deadlocks },
        new object[] { "BlockingStats", QueryWindowRelation.BlockedProcessReports },
        new object[] { "DeadlockStats", QueryWindowRelation.Deadlocks },
    };

    /// <summary>The store's data starts two days ago inside a seven-day window: the note names that coverage start.</summary>
    [Theory]
    [MemberData(nameof(Surfaces))]
    public async Task DataStartsInsideTheWindow_ShowsTheNote_AtTheCoverageStart(string surface, QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddDays(-2);
        await SeedRunsAsync(relation, added, end);
        await SeedRowAsync(relation, end.AddDays(-1));

        var (visible, text) = await NoteAsync(relation, DrawnFor(surface, end.AddDays(-1)), end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(SinceText(added), text);
    }

    /// <summary>The collector ran across the whole window and the first point came five hours in: the store covered the window, so no note.</summary>
    [Theory]
    [MemberData(nameof(Surfaces))]
    public async Task QuietStart_WindowCovered_FirstPointComesLate_ShowsNoNote(string surface, QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedRunsAsync(relation, end.AddDays(-9), end);
        await SeedRowAsync(relation, end.AddDays(-7).AddHours(5));

        var (visible, text) = await NoteAsync(relation, DrawnFor(surface, end.AddDays(-7).AddHours(5)), end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>The event-time rule: a drawn point earlier than the coverage start is the instant the note names.</summary>
    [Fact]
    public async Task APointDrawnBeforeTheCoverageStart_IsWhatTheNoteNames()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var pointAt = end.AddDays(-3);
        await SeedRunsAsync(QueryWindowRelation.Deadlocks, end.AddDays(-2), end);
        await SeedRowAsync(QueryWindowRelation.Deadlocks, end.AddDays(-1));

        var (visible, text) = await NoteAsync(QueryWindowRelation.Deadlocks, DrawnFor("DeadlockTrend", pointAt), end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(SinceText(pointAt), text);
    }

    /// <summary>A zero bucket is the chart's baseline, not a point drawn: the trend helper ignores it.</summary>
    [Fact]
    public void ZeroBuckets_AreNotPointsDrawn()
    {
        var at = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        Assert.Null(ServerTab.EarliestBlockingTrendPointDrawn(new List<TrendPoint> { new() { Time = at, Count = 0 } }));
        Assert.Null(ServerTab.EarliestBlockingStatsPointDrawn(new List<BlockingDurationStatsPoint> { new(at, 0, 0, 0, 0) }));
    }

    /// <summary>Wiring pin: every refresh path that draws these charts also runs the note step, after the charts, and the xaml carries the five notes.</summary>
    [Fact]
    public void EveryRefreshPath_RunsTheNoteSteps_AndTheXamlCarriesTheNotes()
    {
        var refresh = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")).ReplaceLineEndings("\n");
        Assert.Equal(2, Count(refresh, "await RefreshBlockingTrendsBannersAsync("));
        Assert.Equal(2, Count(refresh, "await RefreshBlockingStatsBannersAsync("));
        Assert.True(refresh.IndexOf("UpdateDeadlockTrendChart(dt.Result", StringComparison.Ordinal)
            < refresh.IndexOf("await RefreshBlockingTrendsBannersAsync(lwt.Result", StringComparison.Ordinal));
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        foreach (var name in new[] { "LockWaitTrendTruncationBanner", "BlockingTrendTruncationBanner", "DeadlockTrendTruncationBanner",
                     "BlockingStatsBlockingTruncationBanner", "BlockingStatsDeadlockTruncationBanner" })
        {
            Assert.Contains($"x:Name=\"{name}\"", xaml, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void WaitStatsRelation_NamesItsViewAndCollector()
    {
        Assert.Equal("v_wait_stats", LocalDataService.QueryWindowRelationView(QueryWindowRelation.WaitStats));
        Assert.Equal("wait_stats", LocalDataService.QueryWindowRelationCollector(QueryWindowRelation.WaitStats));
    }

    private static int Count(string haystack, string needle)
    {
        var n = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = haystack.IndexOf(needle, i + 1, StringComparison.Ordinal)) { n++; }
        return n;
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

    private static string ControlsFile(string name, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", name));
}

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

    private static readonly DateTime FixedEnd = new(2026, 9, 20, 12, 0, 0, DateTimeKind.Unspecified);

    private Task SeedDmvRowAsync(DateTime at) => ExecAsync(@"
INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, database_name, monitor_loop, wait_time_ms)
VALUES ($1, $2, $2, $3, $4, 'db1', 1, 100)", _nextId++, Naive(at), ServerId, ServerName);

    /// <summary>The tab's note for the blocking charts, as the step runs it: the source check, then the probe with its flag.</summary>
    private async Task<(bool Visible, string Text)> BlockingNoteAsync(DateTime? earliestDrawn, DateTime startUtc, DateTime endUtc)
    {
        var service = new LocalDataService(_duckDb);
        var fromXe = await service.HasBlockedProcessReportsInWindowAsync(ServerId, startUtc, endUtc);
        /* #5098: the XE branch names the combined start (collector floor, earliest report, threshold history); the DMV branch is the shared probe. */
        var floor = fromXe
            ? await service.GetBlockingXeDataStartAsync(ServerId, startUtc, endUtc)
            : await service.GetQueryWindowFloorAsync(QueryWindowRelation.BlockedProcessReports, ServerId, startUtc, endUtc, includeAlsoCovered: true);
        floor = ServerTab.EarlierOfFloorAndRowShown(floor, earliestDrawn);
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.ApplyWindowFloorToBanner(banner, floor, startUtc, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });
    }

    /// <summary>#5098: the DMV covers the whole window and the XE collector began mid-window; the read drew the XE rows, so the note names the XE start.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task DmvCoversTheWindow_XeStartsMidway_NamesTheXeStart(string surface)
    {
        await _duckDb.InitializeAsync();
        var xeFrom = FixedEnd.AddDays(-3);
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-9), FixedEnd);
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, 'dmv_blocking_snapshot', g.t, 12, 'SUCCESS', 0
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL 30 MINUTE) AS g(t)", _nextId, ServerId, ServerName, FixedEnd.AddDays(-9), FixedEnd);
        _nextId += 100000;
        await ExecAsync("DELETE FROM collection_log WHERE collector_name = 'blocked_process_report' AND collection_time < $1", xeFrom);
        await SeedDmvRowAsync(FixedEnd.AddDays(-6));
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, xeFrom.AddHours(2));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, xeFrom.AddHours(2)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.True(visible);
        Assert.Equal(SinceText(xeFrom), text);
    }

    /// <summary>#5098: no XE row in the window, so the read drew the DMV rows and the note is as before (the DMV covers the window: quiet).</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task DmvOnly_NoteIsAsBefore(string surface)
    {
        await _duckDb.InitializeAsync();
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, 'dmv_blocking_snapshot', g.t, 12, 'SUCCESS', 0
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL 30 MINUTE) AS g(t)", _nextId, ServerId, ServerName, FixedEnd.AddDays(-9), FixedEnd);
        _nextId += 100000;
        await SeedDmvRowAsync(FixedEnd.AddDays(-6));

        Assert.False(await new LocalDataService(_duckDb).HasBlockedProcessReportsInWindowAsync(ServerId, FixedEnd.AddDays(-7), FixedEnd));
        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-6)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>#5098: the XE collector covers the whole window: no note, with or without the DMV beside it.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task XeCoversTheWindow_ShowsNoNote(string surface)
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-9), FixedEnd);
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-7).AddHours(5));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-7).AddHours(5)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    private async Task SeedThresholdAsync(DateTime at, int valueInUse) =>
        await ExecAsync(@"
INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, 'blocked process threshold (s)', $5, $5, true, true)", _nextId++, Naive(at), ServerId, ServerName, valueInUse);

    /// <summary>#5098: the threshold went on midway; the collector covers the window and the first report is later. The note names the first nonzero snapshot.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task ThresholdWentOnMidway_NamesTheFirstNonzeroSnapshot(string surface)
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-9), FixedEnd);
        await SeedThresholdAsync(FixedEnd.AddDays(-8), 0);
        await SeedThresholdAsync(FixedEnd.AddDays(-4), 5);
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-2));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-2)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.True(visible);
        Assert.Equal(SinceText(FixedEnd.AddDays(-4)), text);
    }

    /// <summary>#5098: a report earlier than the first nonzero snapshot is proof the threshold was on: the note names the report.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task AReportBeforeTheFirstNonzeroSnapshot_NamesTheReport(string surface)
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-9), FixedEnd);
        await SeedThresholdAsync(FixedEnd.AddDays(-8), 0);
        await SeedThresholdAsync(FixedEnd.AddDays(-4), 5);
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-5));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-5)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.True(visible);
        Assert.Equal(SinceText(FixedEnd.AddDays(-5)), text);
    }

    /// <summary>#5098: the threshold was already on before the window: covered, no note.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task ThresholdOnBeforeTheWindow_IsCovered(string surface)
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-9), FixedEnd);
        await SeedThresholdAsync(FixedEnd.AddDays(-8), 5);
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-5));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-5)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>#5098: nonzero snapshots inside the window, none before it, and no zero ever read: the earlier snapshots may be gone, so the start is unknown, not late. Covered, no note.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task NonzeroSnapshotsInTheWindowAndNoZeroSeen_IsCovered(string surface)
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-9), FixedEnd);
        await SeedThresholdAsync(FixedEnd.AddDays(-4), 5);
        await SeedThresholdAsync(FixedEnd.AddDays(-3), 5);
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-2));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-2)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>#5098: the threshold read 5 and then 0 with no earlier snapshot: it was turned off later, which is not a late start. Covered, no note.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task AZeroSnapshotAfterTheFirstNonzeroOne_IsNotALateStart(string surface)
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-9), FixedEnd);
        await SeedThresholdAsync(FixedEnd.AddDays(-6), 5);
        await SeedThresholdAsync(FixedEnd.AddDays(-3), 0);
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-5));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-5)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>#5098: no server_config rows at all: the answer is the collector's start, as before the threshold was read.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task NoServerConfigRows_NamesTheCollectorStart_AsBefore(string surface)
    {
        await _duckDb.InitializeAsync();
        var xeFrom = FixedEnd.AddDays(-3);
        await SeedRunsAsync(QueryWindowRelation.BlockedProcessReports, xeFrom, FixedEnd);
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, xeFrom.AddHours(2));

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, xeFrom.AddHours(2)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.True(visible);
        Assert.Equal(SinceText(xeFrom), text);
    }

    /// <summary>#5098: the DMV branch never reads the threshold: a midway-on threshold changes nothing there.</summary>
    [Theory]
    [InlineData("BlockingTrend")]
    [InlineData("BlockingStats")]
    public async Task DmvBranch_IgnoresTheThreshold(string surface)
    {
        await _duckDb.InitializeAsync();
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, 'dmv_blocking_snapshot', g.t, 12, 'SUCCESS', 0
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL 30 MINUTE) AS g(t)", _nextId, ServerId, ServerName, FixedEnd.AddDays(-9), FixedEnd);
        _nextId += 100000;
        await SeedDmvRowAsync(FixedEnd.AddDays(-6));
        await SeedThresholdAsync(FixedEnd.AddDays(-8), 0);
        await SeedThresholdAsync(FixedEnd.AddDays(-4), 5);

        var (visible, text) = await BlockingNoteAsync(DrawnFor(surface, FixedEnd.AddDays(-6)), FixedEnd.AddDays(-7), FixedEnd);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>#5098: the tab's XE branch goes through the guarded probe with the combined start; the DMV branch keeps the shared step.</summary>
    [Fact]
    public void TheXeBranch_CombinesThroughTheGuardedProbe_AndTheDmvBranchKeepsTheSharedStep()
    {
        var src = File.ReadAllText(ControlsFile("ServerTab.BlockingChartsDataStart.cs")).Replace("\r\n", "\n");
        var start = src.IndexOf("private async System.Threading.Tasks.Task RefreshBlockingBannerAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = src[start..src.IndexOf("\n    }\n", start, StringComparison.Ordinal)];
        Assert.Contains("if (!blockingFromXe)", body, StringComparison.Ordinal);
        Assert.Contains("await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.BlockedProcessReports, banner, start, end, earliestDrawn);", body, StringComparison.Ordinal);
        Assert.Contains("await ProbeWindowFloorOrNullAsync(", body, StringComparison.Ordinal);
        Assert.Contains("_dataService.GetBlockingXeDataStartAsync(_serverId, start, end, databaseNames)", body, StringComparison.Ordinal);
    }

    /// <summary>#5098: the source check honors the database filter like the reads do.</summary>
    [Fact]
    public async Task SourceCheck_HonorsTheDatabaseFilter()
    {
        await _duckDb.InitializeAsync();
        await SeedRowAsync(QueryWindowRelation.BlockedProcessReports, FixedEnd.AddDays(-1));
        var service = new LocalDataService(_duckDb);
        var window = (FixedEnd.AddDays(-7), FixedEnd);

        Assert.True(await service.HasBlockedProcessReportsInWindowAsync(ServerId, window.Item1, window.Item2));
        Assert.False(await service.HasBlockedProcessReportsInWindowAsync(ServerId, window.Item1, window.Item2, ["no_such_db"]));
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

    /// <summary>
    /// Wiring pin: the Trends read and the Stats read take the database filter, so the XE source check the note runs must take the same one.
    /// All four refresh calls hand the tab's <c>SelectedDatabaseFilter</c> to their step, each step hands its <c>databaseNames</c> to
    /// <c>BlockingReadTookXeAsync(start, end, databaseNames)</c>, and that check hands it on to the source query.
    /// </summary>
    [Fact]
    public void TheNoteSteps_ReceiveTheDatabaseFilter_AndHandItToTheXeSourceCheck()
    {
        var refresh = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")).ReplaceLineEndings("\n");
        foreach (var call in new[] { "await RefreshBlockingTrendsBannersAsync(", "await RefreshBlockingStatsBannersAsync(" })
        {
            var at = 0;
            var seen = 0;
            while ((at = refresh.IndexOf(call, at, StringComparison.Ordinal)) >= 0)
            {
                var line = refresh[at..refresh.IndexOf('\n', at)];
                Assert.EndsWith("hoursBack, fromDate, toDate, SelectedDatabaseFilter);", line, StringComparison.Ordinal);
                seen++;
                at += call.Length;
            }

            Assert.Equal(2, seen);
        }

        var src = File.ReadAllText(ControlsFile("ServerTab.BlockingChartsDataStart.cs")).Replace("\r\n", "\n");
        foreach (var step in new[] { "RefreshBlockingTrendsBannersAsync", "RefreshBlockingStatsBannersAsync" })
        {
            var start = src.IndexOf($"private async System.Threading.Tasks.Task {step}(", StringComparison.Ordinal);
            Assert.True(start >= 0, $"{step} is missing");
            var body = src[start..src.IndexOf("\n    }\n", start, StringComparison.Ordinal)];
            Assert.Contains("IReadOnlyList<string>? databaseNames = null)", body, StringComparison.Ordinal);
            Assert.Contains("await BlockingReadTookXeAsync(start, end, databaseNames);", body, StringComparison.Ordinal);
        }

        Assert.Contains("_dataService.HasBlockedProcessReportsInWindowAsync(_serverId, start, end, databaseNames)", src, StringComparison.Ordinal);
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

    /// <summary>The wiring: each step calls each note with its relation, banner and drawn-point argument.</summary>
    [Theory]
    [InlineData("RefreshBlockingTrendsBannersAsync", "WaitStats", "LockWaitTrendTruncationBanner", "")]
    [InlineData("RefreshBlockingTrendsBannersAsync", "BlockedProcessReports", "BlockingTrendTruncationBanner", ", EarliestBlockingTrendPointDrawn(blocking), databaseNames")]
    [InlineData("RefreshBlockingTrendsBannersAsync", "Deadlocks", "DeadlockTrendTruncationBanner", ", EarliestBlockingTrendPointDrawn(deadlocks)")]
    [InlineData("RefreshBlockingStatsBannersAsync", "BlockedProcessReports", "BlockingStatsBlockingTruncationBanner", ", EarliestBlockingStatsPointDrawn(durationStats), databaseNames")]
    [InlineData("RefreshBlockingStatsBannersAsync", "Deadlocks", "BlockingStatsDeadlockTruncationBanner", ", EarliestDeadlockStatsPointDrawn(deadlockSeverity)")]
    public void BlockingBannerSteps_CallEachNote_WithItsRelation(string step, string relation, string banner, string drawn)
    {
        var src = File.ReadAllText(ControlsFile("ServerTab.BlockingChartsDataStart.cs")).Replace("\r\n", "\n");
        var start = src.IndexOf($"private async System.Threading.Tasks.Task {step}(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{step} is missing");
        var body = src[start..src.IndexOf("\n    }\n", start, StringComparison.Ordinal)];

        /* #5098: the blocking note goes through RefreshBlockingBannerAsync (the XE branch combines the start, the DMV branch is the shared step). */
        var call = relation == "BlockedProcessReports"
            ? $"await RefreshBlockingBannerAsync(blockingFromXe, {banner}, start, end{drawn});"
            : $"await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.{relation}, {banner}, start, end{drawn});";
        Assert.True(body.Contains(call, StringComparison.Ordinal), $"{step} must contain: {call}");
    }

    private static string ControlsFile(string name, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", name));
}

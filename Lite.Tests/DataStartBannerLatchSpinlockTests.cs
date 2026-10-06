/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The "where the data starts" notice (#4966) on the Latches &amp; Spinlocks tab's two trend charts. Each draws a flat zero
/// over an empty stretch, so each says where its collector's coverage starts: latch_stats (collector latch_stats) and
/// spinlock_stats (collector spinlock_stats), through the shared probe and banner step.
/// </summary>
[Collection("server-time-helper")]
public sealed class DataStartBannerLatchSpinlockTests : IDisposable
{
    private const int ServerId = 4966;
    private const string ServerName = "LatchSpinlockServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DataStartBannerLatchSpinlockTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DataStartBannerLatch_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static string CollectorOf(QueryWindowRelation relation) =>
        relation == QueryWindowRelation.LatchStats ? "latch_stats" : "spinlock_stats";

    private async Task SeedRowAsync(QueryWindowRelation relation, DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = relation == QueryWindowRelation.LatchStats
            ? @"
INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms)
VALUES ($1, $2, $3, $4, 'ACCESS_METHODS_DATASET_PARENT', 1, 1, 1)"
            : @"
INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs)
VALUES ($1, $2, $3, $4, 'LOCK_HASH', 1, 1, 1, 0, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedLogRunsAsync(QueryWindowRelation relation, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, $4, g.t, 12, 'SUCCESS', 0
FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL {everyMinutes} MINUTE) AS g(t)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = CollectorOf(relation) });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(firstUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(lastUtc) });
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    private async Task<(bool Visible, string Text)> BannerForAsync(QueryWindowRelation relation, DateTime startUtc, DateTime endUtc)
    {
        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(relation, ServerId, startUtc, endUtc);
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.ApplyWindowFloorToBanner(banner, floor, startUtc, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text);
        });
    }

    private static string SinceText(DateTime instant) =>
        "Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(instant), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss");

    [Fact]
    public void Relations_NameTheirArchiveViewCollectorAndTimeColumn()
    {
        Assert.Equal("v_latch_stats", LocalDataService.QueryWindowRelationView(QueryWindowRelation.LatchStats));
        Assert.Equal("v_spinlock_stats", LocalDataService.QueryWindowRelationView(QueryWindowRelation.SpinlockStats));
        Assert.Equal("latch_stats", LocalDataService.QueryWindowRelationCollector(QueryWindowRelation.LatchStats));
        Assert.Equal("spinlock_stats", LocalDataService.QueryWindowRelationCollector(QueryWindowRelation.SpinlockStats));
        Assert.Equal("collection_time", LocalDataService.QueryWindowRelationTimeColumn(QueryWindowRelation.LatchStats));
        Assert.Equal("collection_time", LocalDataService.QueryWindowRelationTimeColumn(QueryWindowRelation.SpinlockStats));
    }

    /// <summary>The server was added 2 days ago, so a 7-day chart starts before its coverage: the note names the first collection.</summary>
    [Theory]
    [InlineData(QueryWindowRelation.LatchStats)]
    [InlineData(QueryWindowRelation.SpinlockStats)]
    public async Task DataStartsInsideTheWindow_ShowsTheNote_AtTheFirstCollection(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddDays(-2);
        await SeedLogRunsAsync(relation, added, end, 30);
        await SeedRowAsync(relation, added.AddMinutes(1));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(SinceText(added), text);
    }

    /// <summary>The store covered the whole window (runs for two days before it) and the first row inside it is late: no note.</summary>
    [Theory]
    [InlineData(QueryWindowRelation.LatchStats)]
    [InlineData(QueryWindowRelation.SpinlockStats)]
    public async Task QuietStart_WindowIsCovered_FirstPointComesLate_ShowsNoNote(QueryWindowRelation relation)
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync(relation, end.AddDays(-9), end, 30);
        await SeedRowAsync(relation, end.AddDays(-7).AddHours(5));

        var (visible, text) = await BannerForAsync(relation, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>The wiring: each chart has its banner, refreshed through the shared helper after the charts are bound.</summary>
    [Theory]
    [InlineData("LatchStats", "LatchStatsWindowTruncatedBanner", "UpdateLatchStatsChart(")]
    [InlineData("SpinlockStats", "SpinlockStatsWindowTruncatedBanner", "UpdateSpinlockStatsChart(")]
    public void RefreshLatchSpinlockAsync_RefreshesEachBanner_AfterTheChartIsDrawn(string relation, string banner, string drawCall)
    {
        Assert.Contains($"x:Name=\"{banner}\"", File.ReadAllText(ControlsFile("ServerTab.xaml")), StringComparison.Ordinal);

        var lf = File.ReadAllText(ControlsFile("ServerTab.LatchSpinlock.cs")).Replace("\r\n", "\n");
        var code = Regex.Replace(Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);
        var start = code.IndexOf("private async System.Threading.Tasks.Task RefreshLatchSpinlockAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = code[start..code.IndexOf("\n    }\n", start, StringComparison.Ordinal)];

        var call = body.IndexOf($"await RefreshStoredWindowBannerAsync(QueryWindowRelation.{relation}, {banner}, hoursBack, fromDate, toDate);", StringComparison.Ordinal);
        Assert.True(call > body.IndexOf(drawCall, StringComparison.Ordinal) && body.Contains(drawCall, StringComparison.Ordinal),
            "the banner must be refreshed after the chart is drawn");
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
        if (error is not null) { throw error; }
        return result;
    }

    private static string ControlsFile(string name, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", name));
}

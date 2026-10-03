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
using System.Linq;
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
/// #4966: "where the data starts" on the Queries tab's Plan Corrections grid and Query Heatmap, and the result for
/// every other surface in the same group that carries no notice. Plan Corrections reads <c>v_plan_correction</c>,
/// which holds a row only while the engine has a recommendation, so its coverage comes from the
/// <c>plan_correction</c> collector's runs, like Active Queries and Current Waits. The heatmap reads
/// <c>v_query_stats</c>, the table behind the Top Queries grid, so it asks the same
/// <see cref="QueryWindowRelation.QueryStats"/> question. Both go through the ONE probe
/// (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the ONE banner step
/// (<see cref="ServerTab.ApplyWindowFloorToBanner"/>). A quiet start (the store covered the range, its first row comes
/// late) shows no banner.
/// </summary>
[Collection("server-time-helper")]
public sealed class DataStartBannerQueriesTabTests : IDisposable
{
    private const int ServerId = 4343;
    private const string ServerName = "QueriesTabServer";
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public DataStartBannerQueriesTabTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DataStartQueriesTab_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private async Task SeedPlanCorrectionAsync(DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
VALUES ($1, $2, $3, $4, 'Db', 'PR_1', 'Active', 50)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// One query_stats row collected at <paramref name="at"/>. <c>last_execution_time</c> is NOT the collection time:
    /// it sits <see cref="LastExecutionOffsetDays"/> days earlier, far outside any range these tests ask for, so a
    /// read or a probe that windows on that column instead of <c>collection_time</c> finds nothing and the test fails
    /// (#4966: the seeds used to set the two equal, which no wrong-column read could be told apart from).
    /// </summary>
    private async Task SeedQueryStatsAsync(DateTime at)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, 'Db', '0xAB', '0xH0xAB', $5, 10, 5000, 5000, 'SELECT 1')";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(at.AddDays(LastExecutionOffsetDays)) });
        await cmd.ExecuteNonQueryAsync();
    }

    private const int LastExecutionOffsetDays = -30;

    /// <summary>The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes, whether or not anything was stored.</summary>
    private async Task SeedLogRunsAsync(string collector, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(firstUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(lastUtc) });
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    /// <summary>The probe, then the banner step the surface runs on the result: (visible, text).</summary>
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

    /// <summary>A range that starts before the oldest stored plan correction, with no run logged before it, gets the banner worded at that row.</summary>
    [Fact]
    public async Task PlanCorrections_RangeStartsBeforeTheOldestStoredRow_ShowsTheBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-2);
        await SeedPlanCorrectionAsync(oldest);
        await SeedPlanCorrectionAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>
    /// A server added 2 days ago whose first recommendation came a day later: a 7-day range starts before its
    /// coverage, and the banner names where coverage starts (the collector's first run), not the first row.
    /// </summary>
    [Fact]
    public async Task PlanCorrections_ServerAddedTwoDaysAgo_SaysSinceItsFirstCollection_NotItsFirstRow()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddDays(-2);
        await SeedLogRunsAsync("plan_correction", added, end, 30);
        await SeedPlanCorrectionAsync(end.AddDays(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal("Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(added), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss"), text);
    }

    /// <summary>
    /// The quiet-start guard: the collector ran for 30 days and the engine's first recommendation in all that time
    /// came 3 days ago, so a 7-day range is covered whole and nothing is missing. No banner.
    /// </summary>
    [Fact]
    public async Task PlanCorrections_QuietStart_OnAServerCollectedForThirtyDays_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("plan_correction", end.AddDays(-30), end, 360);
        await SeedPlanCorrectionAsync(end.AddDays(-3));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>The same guard with only stored rows to go on: an older row before the range proves it was served whole.</summary>
    [Fact]
    public async Task PlanCorrections_QuietStartInsideTheRange_WithAnOlderRowBeforeIt_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedPlanCorrectionAsync(end.AddDays(-27));
        await SeedPlanCorrectionAsync(end.AddHours(-2));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>A server with no plan correction and no logged run in the range has nothing to say about where its data starts.</summary>
    [Fact]
    public async Task PlanCorrections_NoRowAndNoRunInTheRange_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedPlanCorrectionAsync(end.AddDays(-20));

        var (visible, _) = await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end);

        Assert.False(visible);
    }

    /// <summary>
    /// The grid, read at its cap (<see cref="LocalDataService.PlanCorrectionGridCap"/>), then the capped-grid banner
    /// step itself (<see cref="ServerTab.CappedGridBannerAsync{T}"/>, the decision inside the tab's
    /// <c>RefreshCappedGridBannerAsync</c>): (visible, text, oldest row the grid returned, rows it returned, whether
    /// the step handed the banner to the probing step). The probing step stands in for the shared
    /// <c>RefreshWindowTruncatedBannerAsync</c>: it words the banner from the real probe's answer, which is awaited
    /// BEFORE the STA block, because a continuation after an await runs on another thread and a WPF banner can only be
    /// written by the thread that made it.
    /// </summary>
    private async Task<(bool Visible, string Text, DateTime Oldest, int Rows, bool Probed)> CappedPlanCorrectionBannerAsync(DateTime startUtc, DateTime endUtc)
    {
        var service = new LocalDataService(_duckDb);
        var rows = await service.GetPlanCorrectionsAsync(ServerId, fromDate: startUtc, toDate: endUtc);
        var probedFloor = await service.GetQueryWindowFloorAsync(QueryWindowRelation.PlanCorrection, ServerId, startUtc, endUtc);
        var (visible, text, probed) = OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            var probeStepRan = false;
            ServerTab.CappedGridBannerAsync(rows, LocalDataService.PlanCorrectionGridCap, row => row.CollectionTime,
                oldestRowShown => ServerTab.ApplyWindowFloorToBanner(banner, oldestRowShown, startUtc, TimeZoneInfo.Utc),
                () =>
                {
                    probeStepRan = true;
                    ServerTab.ApplyWindowFloorToBanner(banner, probedFloor, startUtc, TimeZoneInfo.Utc);
                    return Task.CompletedTask;
                }).GetAwaiter().GetResult();
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text, probeStepRan);
        });
        return (visible, text, rows.Min(row => row.CollectionTime), rows.Count, probed);
    }

    /// <summary>
    /// The capped-grid banner step on rows in memory, at a cap of three, over a range that starts 2026-06-01, in UTC.
    /// The step's two ways out are the banner worded from the oldest row (through the real
    /// <see cref="ServerTab.ApplyWindowFloorToBanner"/>) and a probing step that records its calls and hands back a task
    /// of its own, so they can be told apart: (visible, text, probing-step calls, whether the step returned the probing
    /// step's task).
    /// </summary>
    private static (bool Visible, string Text, int ProbeCalls, bool ReturnedProbeTask) CappedGridBannerStep(DateTime[] rows)
    {
        return OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            var probeTask = new TaskCompletionSource().Task;
            var probeCalls = 0;
            var step = ServerTab.CappedGridBannerAsync(rows, 3, row => row,
                oldestRowShown => ServerTab.ApplyWindowFloorToBanner(banner, oldestRowShown, new DateTime(2026, 6, 1), TimeZoneInfo.Utc),
                () =>
                {
                    probeCalls++;
                    return probeTask;
                });
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text, probeCalls, ReferenceEquals(step, probeTask));
        });
    }

    /// <summary>
    /// A read that came back at its cap words the banner from its oldest row, by time and not by position, and does not
    /// call the probing step: its reach is what the grid shows, whatever the store holds.
    /// </summary>
    [Fact]
    public void CappedGridBannerStep_AReadAtItsCap_WordsTheBannerFromItsOldestRow_AndDoesNotProbe()
    {
        var (visible, text, probeCalls, returnedProbeTask) = CappedGridBannerStep(
            new[] { new DateTime(2026, 6, 5), new DateTime(2026, 6, 3), new DateTime(2026, 6, 9) });

        Assert.True(visible);
        Assert.Equal(Since(new DateTime(2026, 6, 3)), text);
        Assert.Equal(0, probeCalls);
        Assert.False(returnedProbeTask);
    }

    /// <summary>
    /// A read under its cap holds everything the store has in the range: the step hands the banner to the probing step,
    /// returns its task, and words nothing itself.
    /// </summary>
    [Fact]
    public void CappedGridBannerStep_AReadUnderItsCap_FallsThroughToTheProbingStep()
    {
        var (_, text, probeCalls, returnedProbeTask) = CappedGridBannerStep(
            new[] { new DateTime(2026, 6, 5), new DateTime(2026, 6, 3) });

        Assert.Equal(1, probeCalls);
        Assert.True(returnedProbeTask);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// The tab's <c>RefreshCappedGridBannerAsync</c> is the thin step a test cannot run without building the
    /// UserControl: it hands the decision (<see cref="ServerTab.CappedGridBannerAsync{T}"/>, run by the tests above and
    /// by the Plan Corrections tests) the banner worded in the tab's picker zone and the shared probing step over its own
    /// relation, banner and window. Pinned, as <c>RefreshWindowTruncatedBannerAsync</c>'s own body is, so a step that
    /// drops either fails here.
    /// </summary>
    [Fact]
    public void RefreshCappedGridBannerAsync_HandsTheDecisionThePickerZone_AndTheSharedProbingStep()
    {
        Assert.Matches(
            @"private System\.Threading\.Tasks\.Task RefreshCappedGridBannerAsync<T>\(\s*QueryWindowRelation relation, TextBlock banner, DateTime startUtc, DateTime endUtc,\s*IReadOnlyCollection<T> rows, int rowCap, Func<T, DateTime> rowTimeUtc\) =>\s*CappedGridBannerAsync\(rows, rowCap, rowTimeUtc,\s*oldestRowShown => ApplyWindowFloorToBanner\(banner, oldestRowShown, startUtc, GetPickerZone\(\)\),\s*\(\) => RefreshWindowTruncatedBannerAsync\(relation, banner, startUtc, endUtc\)\);",
            CodeOf("ServerTab.Refresh.cs"));
    }

    private static string Since(DateTime instant) =>
        "Showing since " + PerformanceMonitor.Ui.DisplayZone.Format(Naive(instant), TimeZoneInfo.Utc, "yyyy-MM-dd HH:mm:ss");

    /// <summary>
    /// A server added 48 hours ago, one recommendation row every 5 minutes, a 7-day range. The grid reads the newest
    /// 200 rows, which reach back about 16.6 hours, and the banner said 48 hours (where the store starts). It says
    /// where the GRID starts: the oldest row it returned.
    /// </summary>
    [Fact]
    public async Task PlanCorrections_ServerAddedTwoDaysAgo_WhoseGridHitsItsCap_SaysSinceTheOldestRowTheGridShows()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddHours(-48);
        await SeedLogRunsAsync("plan_correction", added, end, 5);
        await SeedPlanCorrectionEveryAsync(added, end, 5);

        var (visible, text, oldest, rows, probed) = await CappedPlanCorrectionBannerAsync(end.AddDays(-7), end);

        Assert.Equal(LocalDataService.PlanCorrectionGridCap, rows);
        Assert.InRange((end - oldest).TotalHours, 16, 17);
        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
        Assert.NotEqual(Since(added), text);
        Assert.False(probed);
    }

    /// <summary>
    /// The store covers the whole range (a row 20 days back, collector runs for 30 days), so the probe alone shows
    /// nothing; the grid still reads only the newest 200 rows. The banner shows even though the store covers the
    /// range, because the grid does not.
    /// </summary>
    [Fact]
    public async Task PlanCorrections_StoreCoversTheRange_ButTheGridHitsItsCap_StillSaysWhereTheGridStarts()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("plan_correction", end.AddDays(-30), end, 360);
        await SeedPlanCorrectionAsync(end.AddDays(-20));
        await SeedPlanCorrectionEveryAsync(end.AddDays(-3), end, 5);

        Assert.False((await BannerForAsync(QueryWindowRelation.PlanCorrection, end.AddDays(-7), end)).Visible);

        var (visible, text, oldest, rows, probed) = await CappedPlanCorrectionBannerAsync(end.AddDays(-7), end);

        Assert.Equal(LocalDataService.PlanCorrectionGridCap, rows);
        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
        Assert.False(probed);
    }

    /// <summary>A read that came back under its cap holds everything the store has in the range, so the store's own floor is the answer, as before.</summary>
    [Fact]
    public async Task PlanCorrections_GridUnderItsCap_KeepsTheStoreFloor()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var added = end.AddHours(-48);
        await SeedLogRunsAsync("plan_correction", added, end, 5);
        await SeedPlanCorrectionEveryAsync(end.AddMinutes(-5 * 149), end, 5);

        var (visible, text, _, rows, probed) = await CappedPlanCorrectionBannerAsync(end.AddDays(-7), end);

        Assert.Equal(150, rows);
        Assert.True(probed);
        Assert.True(visible);
        Assert.Equal(Since(added), text);
    }

    /// <summary>
    /// The one decision every capped grid's banner path takes: a read that returned as many rows as its cap is cut
    /// short, so its reach is its oldest row, whatever the probe found. Oldest by time, not by position.
    /// </summary>
    [Fact]
    public void CapAwareWindowFloor_ACappedRead_IsTheOldestRowReturned_NotTheProbedFloor()
    {
        var probed = new DateTime(2026, 6, 1, 0, 0, 0);
        var rows = new[] { new DateTime(2026, 6, 5), new DateTime(2026, 6, 3), new DateTime(2026, 6, 9) };

        Assert.Equal(new DateTime(2026, 6, 3), ServerTab.CapAwareWindowFloor(probed, rows, 3, row => row));
        Assert.Equal(new DateTime(2026, 6, 3), ServerTab.CapAwareWindowFloor(probed, rows, 2, row => row));
        Assert.Equal(new DateTime(2026, 6, 3), ServerTab.CapAwareWindowFloor(null, rows, 3, row => row));
    }

    /// <summary>Below its cap, or with no cap, a read is not cut short: the probed floor stands, null included.</summary>
    [Fact]
    public void CapAwareWindowFloor_ABelowCapRead_KeepsTheProbedFloor()
    {
        var probed = new DateTime(2026, 6, 1, 0, 0, 0);
        var rows = new[] { new DateTime(2026, 6, 5), new DateTime(2026, 6, 3) };

        Assert.Equal(probed, ServerTab.CapAwareWindowFloor(probed, rows, 3, row => row));
        Assert.Equal(probed, ServerTab.CapAwareWindowFloor(probed, rows, 0, row => row));
        Assert.Null(ServerTab.CapAwareWindowFloor(null, rows, 3, row => row));
        Assert.Equal(probed, ServerTab.CapAwareWindowFloor(probed, Array.Empty<DateTime>(), 3, row => row));
    }

    /// <summary>A recommendation re-captured every <paramref name="everyMinutes"/> minutes, from <paramref name="firstUtc"/> to <paramref name="lastUtc"/>.</summary>
    private async Task SeedPlanCorrectionEveryAsync(DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score)
SELECT $1 + row_number() OVER (), g.t, $2, $3, 'Db', 'PR_1', 'Active', 50
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL {everyMinutes} MINUTE) AS g(t)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(firstUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(lastUtc) });
        _nextId += await cmd.ExecuteNonQueryAsync() + 1;
    }

    /// <summary>
    /// One ring-buffer event sampled at <paramref name="sampleAt"/>. <c>collection_time</c> is three days LATER (the
    /// first collection of a server added after the event, or any collector lag), so a probe that reads
    /// <c>collection_time</c> instead of <c>sample_time</c>, the column the chart windows on, sees the event outside
    /// the range and fails the test.
    /// </summary>
    private async Task SeedMemoryPressureEventAsync(DateTime sampleAt)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
VALUES ($1, $2, $3, $4, $5, 'RESOURCE_MEMPHYSICAL_LOW', 1, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(sampleAt.AddDays(3)) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Naive(sampleAt) });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Memory Pressure Events draws bars only where pressure was recorded, so a span with no data looked like a span
    /// with no pressure. A range that starts before the oldest stored event gets the banner, worded at that event's
    /// SAMPLE time (the column the chart filters on), not the later time it was collected.
    /// </summary>
    [Fact]
    public async Task MemoryPressureEvents_RangeStartsBeforeTheOldestStoredEvent_ShowsTheBanner_AtItsSampleTime()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-2);
        await SeedMemoryPressureEventAsync(oldest);
        await SeedMemoryPressureEventAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.MemoryPressureEvents, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.Equal(Since(oldest), text);
    }

    /// <summary>The quiet start: the collector ran for 30 days and its first event in all that time came 3 days ago, so a 7-day range is covered whole. No banner.</summary>
    [Fact]
    public async Task MemoryPressureEvents_QuietStart_OnAServerCollectedForThirtyDays_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedLogRunsAsync("memory_pressure_events", end.AddDays(-30), end, 360);
        await SeedMemoryPressureEventAsync(end.AddDays(-3));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.MemoryPressureEvents, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>The same guard with only stored events to go on: an older event before the range proves it was served whole.</summary>
    [Fact]
    public async Task MemoryPressureEvents_QuietStartInsideTheRange_WithAnOlderEventBeforeIt_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedMemoryPressureEventAsync(end.AddDays(-27));
        await SeedMemoryPressureEventAsync(end.AddHours(-2));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.MemoryPressureEvents, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>A range that starts before the oldest stored query_stats row gets the banner the heatmap shows.</summary>
    [Fact]
    public async Task QueryHeatmap_RangeStartsBeforeTheOldestStoredRow_ShowsTheBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        var oldest = end.AddDays(-2);
        await SeedQueryStatsAsync(oldest);
        await SeedQueryStatsAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QueryStats, end.AddDays(-7), end);

        Assert.True(visible);
        Assert.StartsWith("Showing since ", text, StringComparison.Ordinal);
        Assert.Contains(Naive(oldest).ToString("yyyy-MM-dd HH:"), text, StringComparison.Ordinal);
    }

    /// <summary>The quiet start: a row before the range proves it was served whole, however late the first row inside it comes.</summary>
    [Fact]
    public async Task QueryHeatmap_QuietStartInsideTheRange_WithAnOlderRowBeforeIt_ShowsNoBanner()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedQueryStatsAsync(end.AddDays(-27));
        await SeedQueryStatsAsync(end.AddHours(-2));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QueryStats, end.AddDays(-7), end);

        Assert.False(visible);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>
    /// One column per 5-minute bucket across the ASKED range, empty ones included, so a gap in the data draws as a
    /// gap. Rows 30 minutes after the start of a 7-day range and an hour before its end used to draw two ADJACENT
    /// columns 6.9 days apart (the columns were only the buckets that hold a row, and the X axis counts columns).
    /// The range is bucket-aligned so the count is exact: 7 days of 5-minute buckets, both ends included.
    /// </summary>
    [Fact]
    public async Task QueryHeatmap_DrawsOneColumnPerBucketAcrossTheAskedRange_SoAGapShowsAsEmptyColumns()
    {
        await _duckDb.InitializeAsync();
        var start = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddDays(7);
        await SeedQueryStatsAsync(start.AddMinutes(30));
        await SeedQueryStatsAsync(end.AddHours(-1));

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(ServerId, HeatmapMetric.Duration, hoursBack: 24, fromDate: start, toDate: end);

        const int buckets = 7 * 24 * 12 + 1;
        Assert.Equal(buckets, result.TimeBuckets.Length);
        Assert.Equal(buckets, result.Intensities.GetLength(1));
        Assert.Equal(buckets, result.CellDetails.GetLength(1));
        Assert.Equal(start, result.TimeBuckets[0]);
        Assert.Equal(end, result.TimeBuckets[^1]);
        for (var i = 1; i < result.TimeBuckets.Length; i++)
        {
            Assert.Equal(TimeSpan.FromMinutes(5), result.TimeBuckets[i] - result.TimeBuckets[i - 1]);
        }

        /* The two stored rows sit in their own columns, a week of empty columns apart; every other column is empty. */
        var firstColumn = Array.IndexOf(result.TimeBuckets, start.AddMinutes(30));
        var lastColumn = Array.IndexOf(result.TimeBuckets, end.AddHours(-1));
        Assert.Equal(6, firstColumn);
        Assert.Equal(buckets - 1 - 12, lastColumn);
        double total = 0;
        for (var row = 0; row < result.Intensities.GetLength(0); row++)
        {
            for (var col = 0; col < result.Intensities.GetLength(1); col++)
            {
                total += result.Intensities[row, col];
                if (result.Intensities[row, col] > 0)
                {
                    Assert.True(col == firstColumn || col == lastColumn, $"column {col} should be empty");
                }
            }
        }

        Assert.Equal(2d, total);
    }

    /// <summary>
    /// A range whose ends are not on a bucket boundary starts at the bucket that CONTAINS its start and ends at the
    /// one that contains its end: the same 5-minute buckets the read's <c>time_bucket</c> puts a row in, so the
    /// stored row lands in the column of its own bucket.
    /// </summary>
    [Fact]
    public async Task QueryHeatmap_AnUnalignedRange_StartsAtTheBucketHoldingItsStart()
    {
        await _duckDb.InitializeAsync();
        var start = new DateTime(2026, 6, 1, 10, 2, 30, DateTimeKind.Unspecified);
        var end = new DateTime(2026, 6, 1, 10, 27, 30, DateTimeKind.Unspecified);
        await SeedQueryStatsAsync(new DateTime(2026, 6, 1, 10, 13, 10, DateTimeKind.Unspecified));

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(ServerId, HeatmapMetric.Duration, hoursBack: 24, fromDate: start, toDate: end);

        Assert.Equal(
            new[] { 0, 5, 10, 15, 20, 25 }.Select(minute => new DateTime(2026, 6, 1, 10, minute, 0)).ToArray(),
            result.TimeBuckets);
        Assert.Equal(1d, result.Intensities[0, 2]);
    }

    /// <summary>
    /// #4991: a range that starts before the data shows the "Showing since" notice, and its columns start at the
    /// 5-minute bucket that holds the notice's time, not at the range start: the span before the data is what the
    /// notice already explains, and a year-wide range over a few days of data would otherwise draw ~105,000 columns,
    /// nearly all of them empty. The end of the range is still the last column. The first row is at 10:17:40 on the
    /// 4th, so its bucket is 10:15.
    /// </summary>
    [Fact]
    public async Task QueryHeatmap_ARangeThatStartsBeforeTheData_StartsItsColumnsAtTheBucketHoldingTheNoticesTime()
    {
        await _duckDb.InitializeAsync();
        var start = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddDays(7);
        var firstRow = new DateTime(2026, 6, 4, 10, 17, 40, DateTimeKind.Unspecified);
        await SeedQueryStatsAsync(firstRow);
        await SeedQueryStatsAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QueryStats, start, end);
        Assert.True(visible);
        Assert.Equal("Showing since 2026-06-04 10:17:40", text);

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(ServerId, HeatmapMetric.Duration, hoursBack: 24, fromDate: start, toDate: end);

        var noticeBucket = new DateTime(2026, 6, 4, 10, 15, 0, DateTimeKind.Unspecified);
        var buckets = (int)((end - noticeBucket).TotalMinutes / 5) + 1;
        Assert.Equal(1030, buckets);
        Assert.Equal(buckets, result.TimeBuckets.Length);
        Assert.Equal(buckets, result.Intensities.GetLength(1));
        Assert.Equal(buckets, result.CellDetails.GetLength(1));
        Assert.Equal(noticeBucket, result.TimeBuckets[0]);
        Assert.Equal(end, result.TimeBuckets[^1]);
        Assert.Equal(1d, result.Intensities[0, 0]);
        Assert.Equal(1d, result.Intensities[0, buckets - 1 - 12]);
    }

    /// <summary>
    /// #4991: a range the store covered shows no notice and keeps its columns from the range start, however late its
    /// first row inside the range comes. An older row before the range proves the store reached back to it, so the
    /// first row inside it (3 days in here) is a quiet start, not where the data begins. A guard against trimming
    /// the columns to the first row whatever the notice says: the old code always started at the range start, so
    /// this one passes on it too.
    /// </summary>
    [Fact]
    public async Task QueryHeatmap_ACoveredRange_StillStartsItsFirstColumnAtTheRangeStart()
    {
        await _duckDb.InitializeAsync();
        var start = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddDays(7);
        await SeedQueryStatsAsync(start.AddDays(-20));
        await SeedQueryStatsAsync(start.AddDays(3).AddMinutes(17));
        await SeedQueryStatsAsync(end.AddHours(-1));

        var (visible, text) = await BannerForAsync(QueryWindowRelation.QueryStats, start, end);
        Assert.False(visible);
        Assert.Equal(string.Empty, text);

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(ServerId, HeatmapMetric.Duration, hoursBack: 24, fromDate: start, toDate: end);

        const int buckets = 7 * 24 * 12 + 1;
        Assert.Equal(buckets, result.TimeBuckets.Length);
        Assert.Equal(start, result.TimeBuckets[0]);
        Assert.Equal(end, result.TimeBuckets[^1]);
    }

    /// <summary>
    /// #4991: starting the columns at the data start does not close the gaps INSIDE the data: every bucket from the
    /// notice's bucket to the range end still has a column, so the two days with no rows between the first row and
    /// the next draw as 575 empty columns (the 576 5-minute buckets from the first row's bucket to the next row's,
    /// less the next row's own).
    /// </summary>
    [Fact]
    public async Task QueryHeatmap_AGapInsideTheData_StillDrawsEmptyColumns_WhenTheRangeStartsBeforeTheData()
    {
        await _duckDb.InitializeAsync();
        var start = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddDays(7);
        var dataStart = new DateTime(2026, 6, 3, 8, 0, 0, DateTimeKind.Unspecified);
        await SeedQueryStatsAsync(dataStart.AddMinutes(2));
        await SeedQueryStatsAsync(dataStart.AddDays(2).AddMinutes(3));
        await SeedQueryStatsAsync(end.AddHours(-1));

        var (visible, _) = await BannerForAsync(QueryWindowRelation.QueryStats, start, end);
        Assert.True(visible);

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(ServerId, HeatmapMetric.Duration, hoursBack: 24, fromDate: start, toDate: end);

        const int buckets = 4 * 24 * 12 + 16 * 12 + 1;
        Assert.Equal(buckets, result.TimeBuckets.Length);
        Assert.Equal(dataStart, result.TimeBuckets[0]);
        Assert.Equal(end, result.TimeBuckets[^1]);
        for (var i = 1; i < result.TimeBuckets.Length; i++)
        {
            Assert.Equal(TimeSpan.FromMinutes(5), result.TimeBuckets[i] - result.TimeBuckets[i - 1]);
        }

        /* The three stored rows sit in their own columns (the first, 2 days later, and the hour before the end); the 575 columns
           between the first two, and every other one, are empty. */
        var secondColumn = Array.IndexOf(result.TimeBuckets, dataStart.AddDays(2));
        var lastColumn = Array.IndexOf(result.TimeBuckets, end.AddHours(-1));
        Assert.Equal(2 * 24 * 12, secondColumn);
        Assert.Equal(buckets - 1 - 12, lastColumn);
        double total = 0;
        for (var col = 0; col < result.Intensities.GetLength(1); col++)
        {
            for (var row = 0; row < result.Intensities.GetLength(0); row++)
            {
                total += result.Intensities[row, col];
                if (result.Intensities[row, col] > 0)
                {
                    Assert.True(col == 0 || col == secondColumn || col == lastColumn, $"column {col} should be empty");
                }
            }
        }

        Assert.Equal(3d, total);
    }

    /// <summary>A range that holds no row at all still answers the empty result, so the chart says it has no data rather than drawing a blank grid.</summary>
    [Fact]
    public async Task QueryHeatmap_ARangeWithNoRows_DrawsNoColumns()
    {
        await _duckDb.InitializeAsync();
        var end = DateTime.UtcNow;
        await SeedQueryStatsAsync(end.AddDays(-20));

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(ServerId, HeatmapMetric.Duration, hoursBack: 7 * 24);

        Assert.Empty(result.TimeBuckets);
    }

    /// <summary>
    /// The three surfaces have their banner TextBlocks, collapsed until a read raises them, and their helpers hand the
    /// shared step the SAME UTC pair the surface's read takes (<c>GetQueriesTabWindowUtc</c>, #4279: the one
    /// <c>GetTimeRange</c> call, which the Memory Pressure Events read makes too) with the right relation: Plan
    /// Corrections its own, through the cap-aware step because its grid reads the newest 200 rows; the heatmap the Top
    /// Queries relation, because both read query_stats; Memory Pressure Events its own.
    /// </summary>
    [Fact]
    public void Surfaces_HaveTheirBanner_AndTheirHelpersTakeTheReadsUtcWindow()
    {
        var xaml = File.ReadAllText(ControlsFile("ServerTab.xaml"));
        Assert.Matches(@"<TextBlock[^>]*x:Name=""PlanCorrectionsWindowTruncatedBanner""[^>]*Visibility=""Collapsed""", xaml);
        Assert.Matches(@"<TextBlock[^>]*x:Name=""QueryHeatmapWindowTruncatedBanner""[^>]*Visibility=""Collapsed""", xaml);
        Assert.Matches(@"<TextBlock[^>]*x:Name=""MemoryPressureEventsWindowTruncatedBanner""[^>]*Visibility=""Collapsed""", xaml);

        var helpers = CodeOf("ServerTab.QueriesDataStart.cs");
        Assert.Matches(
            @"RefreshPlanCorrectionsBannerAsync\(IReadOnlyCollection<PlanCorrectionRow> planCorrections, int hoursBack, DateTime\? fromDate, DateTime\? toDate\)\s*\{\s*var \(windowStart, windowEnd\) = LocalDataService\.GetQueriesTabWindowUtc\(hoursBack, fromDate, toDate\);\s*return RefreshCappedGridBannerAsync\(QueryWindowRelation\.PlanCorrection, PlanCorrectionsWindowTruncatedBanner, windowStart, windowEnd, planCorrections, LocalDataService\.PlanCorrectionGridCap, row => row\.CollectionTime\);",
            helpers);
        Assert.Matches(
            @"RefreshQueryHeatmapBannerAsync\(int hoursBack, DateTime\? fromDate, DateTime\? toDate\)\s*\{\s*var \(windowStart, windowEnd\) = LocalDataService\.GetQueriesTabWindowUtc\(hoursBack, fromDate, toDate\);\s*return RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.QueryStats, QueryHeatmapWindowTruncatedBanner, windowStart, windowEnd\);",
            helpers);
        Assert.Matches(
            @"RefreshMemoryPressureEventsBannerAsync\(int hoursBack, DateTime\? fromDate, DateTime\? toDate\)\s*\{\s*var \(windowStart, windowEnd\) = LocalDataService\.GetQueriesTabWindowUtc\(hoursBack, fromDate, toDate\);\s*return RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.MemoryPressureEvents, MemoryPressureEventsWindowTruncatedBanner, windowStart, windowEnd\);",
            helpers);
    }

    /// <summary>
    /// Every read path of Plan Corrections (the sub-tab switch and the full refresh) refreshes its banner after the
    /// grid is bound, handing it the rows the grid read (the cap-aware step needs them). A census over every
    /// ServerTab file, so a read path added later without the call fails here.
    /// </summary>
    [Fact]
    public void PlanCorrections_EveryReadPathRefreshesTheBanner_AfterTheGridIsBound()
    {
        var refresh = Body(CodeOf("ServerTab.Refresh.cs"), "private async System.Threading.Tasks.Task RefreshQueriesAsync(");
        var bound = Positions(refresh, "_planCorrectionFilterMgr!.UpdateData(");
        var banner = Positions(refresh, "await RefreshPlanCorrectionsBannerAsync(");

        Assert.Equal(2, bound.Count);
        AssertEachFollowsItsBind(bound, banner, "Plan Corrections");
        Assert.Contains("await RefreshPlanCorrectionsBannerAsync(planCorrections, hoursBack, fromDate, toDate);", refresh, StringComparison.Ordinal);
        Assert.Contains("await RefreshPlanCorrectionsBannerAsync(planCorrectionTask.Result, hoursBack, fromDate, toDate);", refresh, StringComparison.Ordinal);

        Assert.Equal(
            AllServerTabCode().Sum(code => Positions(code, "_dataService.GetPlanCorrectionsAsync(").Count),
            AllServerTabCode().Sum(code => Positions(code, "await RefreshPlanCorrectionsBannerAsync(").Count));
    }

    /// <summary>
    /// Every read path of Memory Pressure Events (the timer's sub-tab refresh and the full refresh) refreshes its
    /// banner after the chart is drawn. The same census.
    /// </summary>
    [Fact]
    public void MemoryPressureEvents_EveryReadPathRefreshesTheBanner_AfterTheChartIsDrawn()
    {
        var memory = Body(CodeOf("ServerTab.Refresh.cs"), "private async System.Threading.Tasks.Task RefreshMemoryAsync(");
        var drawn = Positions(memory, "UpdateMemoryPressureEventsChart(");
        var banner = Positions(memory, "await RefreshMemoryPressureEventsBannerAsync(hoursBack, fromDate, toDate);");

        Assert.Equal(2, drawn.Count);
        AssertEachFollowsItsBind(drawn, banner, "Memory Pressure Events");

        Assert.Equal(
            AllServerTabCode().Sum(code => Positions(code, "_dataService.GetMemoryPressureEventsAsync(").Count),
            AllServerTabCode().Sum(code => Positions(code, "await RefreshMemoryPressureEventsBannerAsync(").Count));
    }

    /// <summary>
    /// #4966 item 6: the doc comments that say which surfaces read the probe and share the banner step name the ones
    /// this group added. They had listed only Active Queries and Current Waits as the coverage readers, and only the
    /// three Queries grids, Active Queries and Current Waits as the banner's surfaces.
    /// </summary>
    [Fact]
    public void TheProbeAndBannerStepDocs_NameEverySurfaceThatUsesThem()
    {
        var floorSource = File.ReadAllText(ServicesFile("LocalDataService.QueryWindowFloor.cs")).Replace("\r\n", "\n");
        var probeDoc = DocBefore(floorSource, "public async Task<DateTime?> GetQueryWindowFloorAsync(");
        var collectorDoc = DocBefore(floorSource, "internal static string? QueryWindowRelationCollector(");
        foreach (var surface in new[] { "Active Queries", "Current Waits", "Plan Corrections", "Memory Pressure Events" })
        {
            Assert.True(probeDoc.Contains(surface, StringComparison.Ordinal), $"the probe's doc comment does not name '{surface}' among the surfaces that read it by coverage");
        }

        Assert.Contains("PlanCorrection", collectorDoc, StringComparison.Ordinal);
        Assert.Contains("MemoryPressureEvents", collectorDoc, StringComparison.Ordinal);

        var refreshSource = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs")).Replace("\r\n", "\n");
        var stepDoc = DocBefore(refreshSource, "internal static bool ApplyWindowFloorToBanner(");
        foreach (var surface in new[] { "Plan Corrections", "Query Heatmap", "Memory Pressure Events" })
        {
            Assert.True(stepDoc.Contains(surface, StringComparison.Ordinal), $"the banner step's doc comment does not name '{surface}'");
        }

        /* The shared probing step's own summary names every surface that reaches it (the capped Plan Corrections grid
           included, through its fall-through), so a surface added later without a mention fails here. */
        var sharedStepDoc = DocBefore(refreshSource, "private async System.Threading.Tasks.Task RefreshWindowTruncatedBannerAsync(");
        foreach (var surface in new[] { "Queries grids", "Active Queries", "Current Waits", "Query Heatmap", "Memory Pressure Events", "Plan Corrections" })
        {
            Assert.True(sharedStepDoc.Contains(surface, StringComparison.Ordinal), $"the shared probing step's doc comment does not name '{surface}'");
        }
    }

    /// <summary>
    /// Every read path of the Query Heatmap (the sub-tab switch, the full refresh and the metric picker) refreshes its
    /// banner after the chart is drawn. The same census.
    /// </summary>
    [Fact]
    public void QueryHeatmap_EveryReadPathRefreshesTheBanner_AfterTheChartIsDrawn()
    {
        var refresh = Body(CodeOf("ServerTab.Refresh.cs"), "private async System.Threading.Tasks.Task RefreshQueriesAsync(");
        var drawn = Positions(refresh, "UpdateQueryHeatmapChart(");
        var banner = Positions(refresh, "await RefreshQueryHeatmapBannerAsync(hoursBack, fromDate, toDate);");
        Assert.Equal(2, drawn.Count);
        AssertEachFollowsItsBind(drawn, banner, "Query Heatmap (sub-tab switch and full refresh)");

        var picker = Body(CodeOf("ServerTab.Charts.cs"), "private async void HeatmapMetric_SelectionChanged(");
        var pickerDrawn = Positions(picker, "UpdateQueryHeatmapChart(result);");
        var pickerBanner = Positions(picker, "await RefreshQueryHeatmapBannerAsync(hoursBack, fromDate, toDate);");
        Assert.Single(pickerDrawn);
        AssertEachFollowsItsBind(pickerDrawn, pickerBanner, "Query Heatmap (metric picker)");

        Assert.Equal(
            AllServerTabCode().Sum(code => Positions(code, "_dataService.GetQueryHeatmapAsync(").Count),
            AllServerTabCode().Sum(code => Positions(code, "await RefreshQueryHeatmapBannerAsync(").Count));
    }

    /// <summary>
    /// The chart surfaces of the same group that carry NO notice, and why. A chart whose time axis is pinned to the
    /// asked range already draws the empty span before the first stored row, so a reader cannot take it for a quiet
    /// period; this holds each of them to that: every one of these methods pins the X axis to the window the read took
    /// (<c>GetChartWindow</c>, or <c>GetXAxisWindow</c> for the Overview's five timeline charts), so one that moves to an axis the data
    /// decides fails here and needs a notice instead. The Memory Overview summary reads only the newest snapshot, which
    /// is not a window at all. Memory Pressure Events is on this list for its axis only: its bars exist only where
    /// pressure was recorded, so its empty span still reads as no pressure, which is why it carries the notice too.
    ///
    /// <para>The pin is the DATA path's own call, not any call: most of these methods also set the limits on their
    /// empty path (<c>if (data.Count == 0) { ... SetLimitsX ...; return; }</c>), and that call alone satisfied the old
    /// check when the data path's was deleted. So, for each chart in <paramref name="charts"/> (the controls a method
    /// plots, separated by <c>|</c>; the loop variable for the Overview's five lanes), the call must come after the
    /// method's last <c>return;</c>.</para>
    /// </summary>
    [Theory]
    [InlineData("ServerTab.Charts.cs", "private void UpdateCpuChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "CpuChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateMemoryChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "MemoryChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateMemoryGrantCharts(", "GetChartWindow(hoursBack, fromDate, toDate)", "MemoryGrantSizingChart|MemoryGrantActivityChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateMemoryPressureEventsChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "MemoryPressureEventsChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateTempDbChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "TempDbChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateTempDbSizeChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "TempDbSizeChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateTempDbFileIoChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "TempDbFileIoChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateFileIoCharts(", "GetChartWindow(hoursBack, fromDate, toDate)", "FileIoReadChart|FileIoWriteChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateFileIoThroughputCharts(", "GetChartWindow(hoursBack, fromDate, toDate)", "FileIoReadThroughputChart|FileIoWriteThroughputChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateQueryDurationTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "QueryDurationTrendChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateProcDurationTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "ProcDurationTrendChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateQueryStoreDurationTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "QueryStoreDurationTrendChart")]
    [InlineData("ServerTab.Charts.cs", "private void UpdateExecutionCountTrendChart(", "GetChartWindow(hoursBack, fromDate, toDate)", "ExecutionCountTrendChart")]
    [InlineData("ServerTab.Pickers.cs", "private async System.Threading.Tasks.Task UpdateMemoryClerksChartFromPickerAsync(", "GetChartWindow(hoursBack, fromDate, toDate)", "MemoryClerksChart")]
    [InlineData("ServerTab.Pickers.cs", "private async System.Threading.Tasks.Task UpdateWaitStatsChartFromPickerAsync(", "GetChartWindow(hoursBack, null, null)", "WaitStatsChart")]
    [InlineData("ServerTab.Pickers.cs", "private async System.Threading.Tasks.Task UpdatePerfmonChartFromPickerAsync(", "GetChartWindow(hoursBack, null, null)", "PerfmonChart")]
    [InlineData("CorrelatedTimelineLanesControl.xaml.cs", "private void SyncXAxes(", "GetXAxisWindow(hoursBack, fromDate, toDate, DateTime.UtcNow)", "chart")]
    public void ChartSurfaces_PinTheirXAxisToTheRequestedRange_OnTheDataPathToo(string file, string signature, string window, string charts)
    {
        var body = Body(CodeOf(file), signature);
        Assert.Contains(window, body, StringComparison.Ordinal);
        Assert.Contains(".SetLimitsX(", body, StringComparison.Ordinal);

        /* Everything after the last `return;` is the data path (a method with no early return is all data path). */
        var dataPath = body[(body.LastIndexOf("return;", StringComparison.Ordinal) + 1)..];
        foreach (var chart in charts.Split('|'))
        {
            Assert.True(
                dataPath.Contains($"{chart}.Plot.Axes.SetLimitsX(", StringComparison.Ordinal),
                $"{signature}: the data path no longer pins {chart}'s X axis to the requested range (an empty-path call does not count)");
        }
    }

    /// <summary>
    /// The heatmap's X axis counts its drawn columns (<c>SetLimitsX(-0.5, numCols - 0.5)</c>, never the chart window).
    /// The columns now span the asked range, one per 5-minute bucket, so a gap in the data draws as empty columns; the
    /// notice stays because an empty column cannot tell a server that did not exist yet from one that ran nothing
    /// (see the column tests above). Plan Corrections is a grid.
    /// </summary>
    [Fact]
    public void QueryHeatmap_XAxisCountsDrawnColumns_SoItCarriesTheNotice()
    {
        var body = Body(CodeOf("ServerTab.Charts.cs"), "private void UpdateQueryHeatmapChart(");
        Assert.Contains("SetLimitsX(-0.5, numCols - 0.5)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("GetChartWindow(", body, StringComparison.Ordinal);
    }

    /// <summary>The Memory Overview summary reads the newest snapshot only: no window, so nothing to say about where one starts.</summary>
    [Fact]
    public void MemoryOverviewSummary_ReadsTheNewestSnapshotOnly()
    {
        var refresh = CodeOf("ServerTab.Refresh.cs");
        Assert.Equal(2, Positions(refresh, "_dataService.GetLatestMemoryStatsAsync(_serverId)").Count);
        Assert.DoesNotContain("GetLatestMemoryStatsAsync(_serverId, hoursBack", refresh, StringComparison.Ordinal);
    }

    private static void AssertEachFollowsItsBind(List<int> binds, List<int> banners, string what)
    {
        Assert.True(banners.Count == binds.Count,
            $"{what}: expected one banner refresh per read path ({binds.Count}), found {banners.Count}");
        for (var i = 0; i < binds.Count; i++)
        {
            Assert.True(banners[i] > binds[i], $"{what}: the banner refresh must follow the read it describes");
            if (i + 1 < binds.Count)
            {
                Assert.True(banners[i] < binds[i + 1], $"{what}: the banner refresh must sit with the read it describes, not the next one");
            }
        }
    }

    private static List<int> Positions(string text, string token)
    {
        var found = new List<int>();
        for (var at = text.IndexOf(token, StringComparison.Ordinal); at >= 0; at = text.IndexOf(token, at + token.Length, StringComparison.Ordinal))
        {
            found.Add(at);
        }

        return found;
    }

    /// <summary>The method that starts at <paramref name="signature"/>, up to the first closing brace at member indentation.</summary>
    private static string Body(string code, string signature)
    {
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' was not found; update this pin.");
        var end = code.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found.");
        return code[start..end];
    }

    /// <summary>A Controls source file with line endings folded to LF and comments removed, so a sentence naming a call cannot satisfy a pin.</summary>
    private static string CodeOf(string name)
    {
        var lf = File.ReadAllText(ControlsFile(name)).Replace("\r\n", "\n");
        return Regex.Replace(Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"(?<!:)//[^\n]*", string.Empty);
    }

    private static IEnumerable<string> AllServerTabCode() =>
        Directory.EnumerateFiles(ControlsDir(), "ServerTab*.cs").Select(path => CodeOf(Path.GetFileName(path)));

    /// <summary>WPF objects require STA; same shape as DataStartBannerTests.</summary>
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

    /// <summary>
    /// The <c>///</c> block (and attribute lines) directly above the member that starts at <paramref name="signature"/>,
    /// in an LF-only source. It collects a doc run by line prefix, which is what defines the run: the surfaces a pin
    /// reads out of it live only in the comment, so a walk that strips comments would leave nothing to read. The bound
    /// it rests on: the walk stops at the first line that is neither <c>///</c> nor an attribute, so a block comment, a
    /// <c>//</c> line or a blank line between the run and its member truncates it and the pin reads the surfaces as
    /// missing, which fails loudly rather than passing on half a comment. <c>CommentFilterAdoptionTests</c> lists this
    /// site with the same bound.
    /// </summary>
    private static string DocBefore(string lfSource, string signature)
    {
        var at = lfSource.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"'{signature}' was not found; update this pin.");
        var lines = lfSource[..at].Split('\n');
        var doc = new List<string>();
        for (var i = lines.Length - 2; i >= 0; i--)
        {
            var line = lines[i].Trim();
            if (!line.StartsWith("///", StringComparison.Ordinal) && !line.StartsWith('['))
            {
                break;
            }

            doc.Add(line);
        }

        return string.Join("\n", doc);
    }

    private static string ServicesFile(string name) =>
        Path.GetFullPath(Path.Combine(ControlsDir(), "..", "Services", name));

    private static string ControlsFile(string name) =>
        Path.GetFullPath(Path.Combine(ControlsDir(), name));

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: where the data starts, on four more time-ranged Lite tools. <c>get_active_queries</c> and
/// <c>get_waiting_tasks</c> (read by coverage: the collector's logged runs count as well as rows),
/// <c>get_query_store_regressions</c> (checked against the baseline's start, the earlier of its two windows) and
/// <c>get_query_heatmap</c> each publish <c>effective_start</c>, <c>window_truncated</c> and <c>truncation_note</c>
/// beside <c>hours_back</c>, from the same probe the Queries tools use
/// (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>). All four carry a page cut of their own
/// (<c>truncated</c>), so none writes <c>effective_hours_back</c>: Darling's census holds that key apart for the
/// window floor. Own <see cref="DuckDbInitializer"/> per test, like <see cref="QueryWindowTruncationTests"/>.
/// </summary>
public sealed class McpWindowNoticeToolTests : IDisposable
{
    private const string ServerName = "NoticeServer";
    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    public McpWindowNoticeToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpWindowNotice_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    /// <summary>Reads a payload's instant as UTC whether or not it carries a trailing Z (the store's own frame has none).</summary>
    private static DateTime ParseUtc(string text) =>
        DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private LocalDataService Service() => new(_duckDb);

    /* ───────────────────────── the helper ───────────────────────── */

    /// <summary>
    /// #4966: the zone marker on effective_start does not depend on the flag beside it. The floor comes off the store as
    /// a naive instant and the requested start is already UTC, and only the second used to print a trailing Z, so a cut
    /// window read without one and a covered window with one. Both end in Z now and name the same instants as before.
    /// </summary>
    [Fact]
    public void WindowNotice_EffectiveStart_EndsInZ_WhetherTheWindowWasCutOrCovered()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

        var cut = McpQueryTools.WindowNotice(Naive(start.AddDays(2)), start, "query_snapshots");
        var covered = McpQueryTools.WindowNotice(null, start, "query_snapshots");
        var insideSlack = McpQueryTools.WindowNotice(Naive(start.AddMinutes(60)), start, "query_snapshots");

        Assert.True(cut.WindowTruncated);
        Assert.False(covered.WindowTruncated);
        Assert.False(insideSlack.WindowTruncated);
        Assert.Equal("2026-09-03T00:00:00.0000000Z", cut.EffectiveStart);
        Assert.Equal("2026-09-01T00:00:00.0000000Z", covered.EffectiveStart);
        /* No cut, but the served start is the floor, which used to print with no Z beside the flag's false. */
        Assert.Equal("2026-09-01T01:00:00.0000000Z", insideSlack.EffectiveStart);
    }

    /// <summary>The one formatter every window-floor payload writes effective_start through, in both apps (now in the
    /// shared McpHelpers), sets the kind and never shifts the instant.</summary>
    [Fact]
    public void FormatEffectiveStart_NamesTheInstantAsUtc_WhicheverKindItCameWith()
    {
        var instant = new DateTime(2026, 9, 3, 4, 5, 6, 789, DateTimeKind.Unspecified);

        Assert.Equal("2026-09-03T04:05:06.7890000Z", PerformanceMonitor.Common.McpHelpers.FormatEffectiveStart(instant));
        Assert.Equal("2026-09-03T04:05:06.7890000Z", PerformanceMonitor.Common.McpHelpers.FormatEffectiveStart(DateTime.SpecifyKind(instant, DateTimeKind.Utc)));
    }

    [Fact]
    public void WindowNotice_AFloorPastTheSlack_IsTruncated_AtTheFloor_NamingTheTable()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var floor = Naive(start.AddDays(2));

        var notice = McpQueryTools.WindowNotice(floor, start, "query_snapshots");

        Assert.True(notice.WindowTruncated);
        /* #4966: this assertion pinned the floor's own naive spelling, with no zone marker. The payload now names it as
           UTC, with the trailing Z a covered window's requested start has always carried; the instant is unchanged. */
        Assert.Equal(DateTime.SpecifyKind(floor, DateTimeKind.Utc).ToString("o"), notice.EffectiveStart);
        Assert.EndsWith("Z", notice.EffectiveStart, StringComparison.Ordinal);
        Assert.Equal(floor, ParseUtc(notice.EffectiveStart!));
        Assert.NotNull(notice.TruncationNote);
        Assert.Contains("raw query_snapshots retains", notice.TruncationNote, StringComparison.Ordinal);
        Assert.EndsWith("so the older part of it was not read.", notice.TruncationNote, StringComparison.Ordinal);
    }

    [Fact]
    public void WindowNotice_ACoveredWindow_IsNotTruncated_AndTheNoteIsNull()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

        /* No row in the window, a floor at the start (the probe's "served whole"), and a floor inside the
           ninety-minute slack: none of them is a cut, and each keeps the requested start. */
        foreach (var floor in new DateTime?[] { null, start, Naive(start.AddMinutes(60)) })
        {
            var notice = McpQueryTools.WindowNotice(floor, start, "waiting_tasks");
            Assert.False(notice.WindowTruncated);
            Assert.Null(notice.TruncationNote);
            Assert.EndsWith("Z", notice.EffectiveStart, StringComparison.Ordinal);
        }

        /* A floor inside the slack is no cut, but it still moves the served start onto the floor (the clamp only stops
           it going earlier than asked), and that instant carries the Z too. */
        var insideSlack = Naive(start.AddMinutes(60));
        Assert.Equal(insideSlack, ParseUtc(McpQueryTools.WindowNotice(insideSlack, start, "waiting_tasks").EffectiveStart!));

        Assert.Equal(start.ToString("o"), McpQueryTools.WindowNotice(null, start, "waiting_tasks").EffectiveStart);
        Assert.Equal(start.ToString("o"), McpQueryTools.WindowNotice(start, start, "waiting_tasks").EffectiveStart);
    }

    [Fact]
    public void WindowNotice_AToolsOwnSentence_RidesOnlyOnATruncatedNote()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

        var truncated = McpQueryTools.WindowNotice(Naive(start.AddDays(1)), start, "query_store_stats", "Its own sentence.");
        var covered = McpQueryTools.WindowNotice(start, start, "query_store_stats", "Its own sentence.");

        Assert.EndsWith(" Its own sentence.", truncated.TruncationNote, StringComparison.Ordinal);
        Assert.Null(covered.TruncationNote);
    }

    /* ───────────────────────── get_active_queries (query_snapshots, by coverage) ───────────────────────── */

    [Fact]
    public async Task GetActiveQueries_RowsStartInsideTheWindow_ReportTheFloor()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-2);
        await SeedSnapshotAsync(floor);
        await SeedSnapshotAsync(now.AddDays(-1));

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 168));

        AssertTruncatedAt(root, floor, "query_snapshots");
    }

    [Fact]
    public async Task GetActiveQueries_AnOlderRowBeforeTheWindow_IsCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedSnapshotAsync(now.AddDays(-8));
        await SeedSnapshotAsync(now.AddDays(-2));

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 168));

        AssertCovered(root, now.AddHours(-168));
    }

    [Fact]
    public async Task GetActiveQueries_AQuietStart_TheCollectorRanFromTheWindowsStart_IsCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        /* The collector ran every half hour from before the window began; the first snapshot is two days in.
           A server idle overnight looks exactly like this, and a rows-only probe would call it truncated. */
        await SeedLogRunsAsync("query_snapshots", now.AddHours(-168 - 1), now, everyMinutes: 30);
        await SeedSnapshotAsync(now.AddDays(-2));

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 168));

        AssertCovered(root, now.AddHours(-168));
    }

    [Fact]
    public async Task GetActiveQueries_AFilledPage_StillReportsThePageCut_BesideTheCoverage()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedSnapshotAsync(now.AddHours(-30));
        await SeedSnapshotAsync(now.AddHours(-3));
        await SeedSnapshotAsync(now.AddHours(-2));
        await SeedSnapshotAsync(now.AddHours(-1));

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 24, limit: 2));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(2, root.GetProperty("snapshots_returned").GetInt32());
        AssertCovered(root, now.AddHours(-24));
    }

    /* ───────────────────────── get_waiting_tasks (waiting_tasks, by coverage) ───────────────────────── */

    [Fact]
    public async Task GetWaitingTasks_RowsStartInsideTheWindow_ReportTheFloor()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-2);
        await SeedWaitingTaskAsync(floor);
        await SeedWaitingTaskAsync(now.AddDays(-1));

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 168));

        AssertTruncatedAt(root, floor, "waiting_tasks");
    }

    [Fact]
    public async Task GetWaitingTasks_AnOlderRowBeforeTheWindow_IsCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedWaitingTaskAsync(now.AddDays(-8));
        await SeedWaitingTaskAsync(now.AddDays(-2));

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 168));

        AssertCovered(root, now.AddHours(-168));
    }

    [Fact]
    public async Task GetWaitingTasks_AQuietStart_TheCollectorRanFromTheWindowsStart_IsCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        /* Nothing waits for hours on a quiet server: the first row comes two days into the window while the
           collector has been running since before it. */
        await SeedLogRunsAsync("waiting_tasks", now.AddHours(-168 - 1), now, everyMinutes: 30);
        await SeedWaitingTaskAsync(now.AddDays(-2));

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 168));

        AssertCovered(root, now.AddHours(-168));
    }

    [Fact]
    public async Task GetWaitingTasks_AFilledPage_StillReportsThePageCut_BesideTheCoverage()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedWaitingTaskAsync(now.AddHours(-30));
        await SeedWaitingTaskAsync(now.AddHours(-3));
        await SeedWaitingTaskAsync(now.AddHours(-2));
        await SeedWaitingTaskAsync(now.AddHours(-1));

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 24, limit: 2));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(2, root.GetProperty("tasks_returned").GetInt32());
        AssertCovered(root, now.AddHours(-24));
    }

    /* ───────────────────────── get_query_store_regressions (query_store_stats, against the baseline's start) ───────────────────────── */

    [Fact]
    public async Task GetQueryStoreRegressions_TheBaselineStartsInsideTheSevenDays_ReportsTheFloor()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        /* The baseline is the seven days before the 24-hour window: it starts 8 days back, and the store's first
           row is 3 days back, so the baseline holds two of its seven days and nothing else says so. */
        var floor = now.AddDays(-3);
        await SeedRegressionAsync(floor, queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 3000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24));

        Assert.Equal(1, root.GetProperty("regression_count").GetInt32());
        AssertTruncatedAt(root, floor, "query_store_stats");
        Assert.Contains("baseline_start", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetQueryStoreRegressions_AnOlderRowBeforeTheBaseline_IsCovered_AtTheBaselinesStart()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedRegressionAsync(now.AddDays(-9), queryId: 2, avgUs: 1000, intervalId: 3);
        await SeedRegressionAsync(now.AddDays(-3), queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 3000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24));

        Assert.Equal(1, root.GetProperty("regression_count").GetInt32());
        AssertCovered(root, now.AddHours(-24 - 7 * 24));
        /* Checked against the EARLIER window's start: the covered answer is the baseline's own start. */
        Assert.Equal(root.GetProperty("baseline_start").GetString(), root.GetProperty("effective_start").GetString());
    }

    [Fact]
    public async Task GetQueryStoreRegressions_AFilledPage_StillReportsThePageCut_BesideTheCoverage()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedRegressionAsync(now.AddDays(-9), queryId: 9, avgUs: 1000, intervalId: 9);
        foreach (var queryId in new long[] { 1, 2 })
        {
            await SeedRegressionAsync(now.AddDays(-3), queryId, avgUs: 1000, intervalId: 10 + queryId);
            await SeedRegressionAsync(now.AddHours(-1), queryId, avgUs: 3000, intervalId: 20 + queryId);
        }

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24, limit: 1));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, root.GetProperty("regression_count").GetInt32());
        AssertCovered(root, now.AddHours(-24 - 7 * 24));
    }

    /* ───────────────────────── get_query_heatmap (query_stats) ───────────────────────── */

    /// <summary>#4966: an idle grid over a cut window carries the shared cut sentence, not the "idle" reading.</summary>
    [Fact]
    public async Task GetQueryHeatmap_AnIdleGrid_OverACutWindow_CarriesTheCutSentence()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedIdleQueryStatsAsync(now.AddDays(-2), "0xI1");
        await SeedIdleQueryStatsAsync(now.AddDays(-1), "0xI2");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 168));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.True(EmptyHints(root).GetProperty("window_truncated").GetBoolean());
        Assert.Equal(McpHelpers.CutWindowNothingMessage, root.GetProperty("message").GetString());
    }

    /// <summary>#4966: the same idle grid over a covered window keeps its "idle" sentence.</summary>
    [Fact]
    public async Task GetQueryHeatmap_AnIdleGrid_OverACoveredWindow_KeepsTheIdleSentence()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedIdleQueryStatsAsync(now.AddHours(-167.9), "0xI1");
        await SeedIdleQueryStatsAsync(now.AddDays(-1), "0xI2");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 168));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.False(EmptyHints(root).GetProperty("window_truncated").GetBoolean());
        Assert.Contains("WERE collected", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetQueryHeatmap_RowsStartInsideTheWindow_ReportTheFloor()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-2);
        await SeedQueryStatsAsync(floor, "0xH1");
        await SeedQueryStatsAsync(now.AddDays(-1), "0xH2");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 168));

        Assert.True(root.GetProperty("cell_count").GetInt32() > 0);
        AssertTruncatedAt(root, floor, "query_stats");
    }

    [Fact]
    public async Task GetQueryHeatmap_AnOlderRowBeforeTheWindow_IsCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedQueryStatsAsync(now.AddDays(-8), "0xH0");
        await SeedQueryStatsAsync(now.AddDays(-2), "0xH1");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 168));

        Assert.True(root.GetProperty("cell_count").GetInt32() > 0);
        AssertCovered(root, now.AddHours(-168));
    }

    [Fact]
    public async Task GetQueryHeatmap_AFilledGrid_StillReportsTheCellCap_BesideTheCoverage()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedQueryStatsAsync(now.AddHours(-30), "0xH0");
        /* Three captures in three different bins: three cells, against a cap of two. */
        await SeedQueryStatsAsync(now.AddHours(-3), "0xH1");
        await SeedQueryStatsAsync(now.AddHours(-2), "0xH2");
        await SeedQueryStatsAsync(now.AddHours(-1), "0xH3");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 24, limit: 2));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        AssertCovered(root, now.AddHours(-24));
    }

    /* ───────────────────────── a window the slack covers needs no probe ───────────────────────── */

    /// <summary>
    /// #4966: a window of 90 minutes or less can never be called cut by the store (the probe cannot find a floor later
    /// than the start by more than the slack), so the helper does not start the probe for one, as the Lite tabs' banner
    /// step does not. The answer is the covered one, at the start that was asked for.
    /// </summary>
    [Theory]
    [InlineData(60)]
    [InlineData(90)]
    public async Task WindowNoticeAsync_AWindowOf90MinutesOrLess_MakesNoProbeCall_AndIsCovered(int minutes)
    {
        var end = DateTime.UtcNow;
        var start = end.AddMinutes(-minutes);
        var probes = 0;

        var notice = await McpQueryTools.WindowNoticeAsync(
            () => { probes++; return Task.FromResult<DateTime?>(Naive(start.AddMinutes(30))); },
            start, end, "query_snapshots");

        Assert.Equal(0, probes);
        Assert.False(notice.WindowTruncated);
        Assert.Null(notice.TruncationNote);
        Assert.Equal(PerformanceMonitor.Common.McpHelpers.FormatEffectiveStart(start), notice.EffectiveStart);
    }

    [Fact]
    public async Task WindowNoticeAsync_AWindowPastTheSlack_AsksTheProbeOnce_AndReadsItsFloor()
    {
        var end = DateTime.UtcNow;
        var start = end.AddHours(-2);
        var floor = Naive(start.AddMinutes(100));
        var probes = 0;

        var notice = await McpQueryTools.WindowNoticeAsync(
            () => { probes++; return Task.FromResult<DateTime?>(floor); },
            start, end, "query_snapshots");

        Assert.Equal(1, probes);
        Assert.True(notice.WindowTruncated);
        Assert.Equal(PerformanceMonitor.Common.McpHelpers.FormatEffectiveStart(floor), notice.EffectiveStart);
    }

    /// <summary>
    /// The first row sits 35 minutes into the hour: the probe would have answered that row and put effective_start 25
    /// minutes after the window's start. With no probe the answer is the window's own start. The page cut never used
    /// the probe: the newest two rows, with the older of them named by oldest_returned_collection_time (UTC, with the Z).
    /// </summary>
    [Fact]
    public async Task GetActiveQueries_AOneHourWindow_MakesNoProbeCall_AndACappedPageStillNamesItsOldestRow()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedSnapshotAsync(now.AddMinutes(-35));
        await SeedSnapshotAsync(now.AddMinutes(-25));
        await SeedSnapshotAsync(now.AddMinutes(-15));
        await SeedSnapshotAsync(now.AddMinutes(-5));

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 1, limit: 2));

        AssertCovered(root, now.AddHours(-1));
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(2, root.GetProperty("snapshots_returned").GetInt32());
        AssertOldestReturned(root, now.AddMinutes(-15));
    }

    [Fact]
    public async Task GetWaitingTasks_AOneHourWindow_MakesNoProbeCall_AndACappedPageStillNamesItsOldestRow()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedWaitingTaskAsync(now.AddMinutes(-35));
        await SeedWaitingTaskAsync(now.AddMinutes(-25));
        await SeedWaitingTaskAsync(now.AddMinutes(-15));
        await SeedWaitingTaskAsync(now.AddMinutes(-5));

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 1, limit: 2));

        AssertCovered(root, now.AddHours(-1));
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(2, root.GetProperty("tasks_returned").GetInt32());
        AssertOldestReturned(root, now.AddMinutes(-15));
    }

    [Fact]
    public async Task GetQueryHeatmap_AOneHourWindow_MakesNoProbeCall_AndIsCoveredAtTheWindowsStart()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedQueryStatsAsync(now.AddMinutes(-35), "0xH1");
        await SeedQueryStatsAsync(now.AddMinutes(-15), "0xH2");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 1));

        Assert.True(root.GetProperty("cell_count").GetInt32() > 0);
        AssertCovered(root, now.AddHours(-1));
    }

    /* ───────────────────────── an empty answer says where the data starts too ───────────────────────── */

    /// <summary>
    /// #4966: an empty answer over a window the store does not reach back to is NOT a true negative: nothing in the
    /// part of the window the store holds says nothing about the part it does not. The four tools wrote the three
    /// window keys on a data answer only, so the empty one read as "nothing happened". An <c>empty</c> status now
    /// carries <c>effective_start</c>, <c>window_truncated</c> and <c>truncation_note</c> under <c>hints</c>, the shape
    /// <c>get_query_store_top</c>'s module miss already uses, with the data answer's values and wording. A status that
    /// says the collector never ran (<c>not_collected</c>) keeps its shape.
    /// </summary>
    [Fact]
    public async Task GetActiveQueries_AnEmptyWindowTheCollectorRanIn_PastCoverage_CarriesTheFloorAndTheNote()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        /* The collector ran every half hour for two days and nothing was running at any of them: the store holds
           the last two days of a 7-day ask, and nothing but the three keys says the older five were never read. */
        await SeedLogRunsAsync("query_snapshots", now.AddDays(-2), now, everyMinutes: 30);

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 168));

        AssertTruncatedAt(EmptyHints(root), now.AddDays(-2), "query_snapshots");
    }

    [Fact]
    public async Task GetActiveQueries_AFilteredMiss_PastCoverage_CarriesTheSameKeys()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-2);
        await SeedSnapshotAsync(floor);
        await SeedSnapshotAsync(now.AddDays(-1));

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 168, database_name: "NoSuchDb"));

        Assert.Contains("matched database_name 'NoSuchDb'", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        /* The probe is deliberately unfiltered: the floor is a property of the table, not of the filter that missed. */
        AssertTruncatedAt(EmptyHints(root), floor, "query_snapshots");
    }

    [Fact]
    public async Task GetActiveQueries_AFilteredMiss_OverACoveredRange_SaysCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedSnapshotAsync(now.AddDays(-8));
        await SeedSnapshotAsync(now.AddDays(-2));

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 168, database_name: "NoSuchDb"));

        AssertCovered(EmptyHints(root), now.AddHours(-168));
    }

    /// <summary>
    /// #5015: an EMPTY answer is probed whatever the window's length, because nothing was read and the skip would answer
    /// "covered" for it. A run 35 minutes into the hour is the floor, inside the slack: covered, and effective_start is
    /// the run.
    /// </summary>
    [Fact]
    public async Task GetActiveQueries_AnEmptyOneHourWindow_IsProbed_AndNamesTheFirstRunInIt()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedLogRunsAsync("query_snapshots", now.AddMinutes(-35), now.AddMinutes(-35), everyMinutes: 30);

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 1));

        AssertCoveredFrom(EmptyHints(root), now.AddMinutes(-35));
    }

    /// <summary>
    /// #5015, the ruling's RED: an empty answer over a 60-minute window with no run in it never says "covered". Nothing was
    /// read, so the answer is NOT covered: no effective_start, and a note that the store holds no collection in the window.
    /// </summary>
    [Fact]
    public async Task GetActiveQueries_AnEmptyOneHourWindow_WithNoRunInIt_IsNotCovered()
    {
        await _duckDb.InitializeAsync();

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 1));

        AssertNothingHeld(EmptyHints(root), "query_snapshots");
    }

    /// <summary>#5015, RED: a server that was never collected has no row and no run in any window, so an empty answer is NOT covered.</summary>
    [Fact]
    public async Task GetActiveQueries_AnEmptyAnswer_OnAServerNeverCollected_IsNotCovered()
    {
        await _duckDb.InitializeAsync();

        var root = Root(await McpSessionTools.GetActiveQueries(Service(), _serverManager, ServerName, hours_back: 168));

        AssertNothingHeld(EmptyHints(root), "query_snapshots");
    }

    /// <summary>#5015, RED: runs 9 and 10 days old are not in a 168-hour window, so nothing waiting there is not a verdict on it.</summary>
    [Fact]
    public async Task GetWaitingTasks_AnEmptyWideWindow_WhoseRunsAreAllOlder_IsNotCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedLogRunsAsync("waiting_tasks", now.AddDays(-10), now.AddDays(-9), everyMinutes: 60);

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 168));

        AssertNothingHeld(EmptyHints(root), "waiting_tasks");
    }

    [Fact]
    public async Task GetWaitingTasks_AnEmptyOneHourWindow_WithNoRunInIt_IsNotCovered()
    {
        await _duckDb.InitializeAsync();

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 1));

        AssertNothingHeld(EmptyHints(root), "waiting_tasks");
    }

    /// <summary>
    /// #5015: the helper's own rule. An empty answer asks the probe even for a 60-minute window, and a null floor then reads
    /// as NOT covered; the same null floor on an answer WITH rows keeps its old meaning (covered, at the asked-for start).
    /// </summary>
    [Fact]
    public async Task WindowNoticeAsync_AnEmptyAnswerOver60Minutes_AsksTheProbe_AndANullFloorIsNotCovered()
    {
        var end = DateTime.UtcNow;
        var start = end.AddMinutes(-60);
        var probes = 0;

        var empty = await McpQueryTools.WindowNoticeAsync(
            () => { probes++; return Task.FromResult<DateTime?>(null); }, start, end, "query_snapshots", emptyAnswer: true);
        var withRows = McpQueryTools.WindowNotice(null, start, "query_snapshots");

        Assert.Equal(1, probes);
        Assert.True(empty.WindowTruncated);
        Assert.Null(empty.EffectiveStart);
        Assert.Contains("holds no collection of query_snapshots", empty.TruncationNote, StringComparison.Ordinal);
        Assert.False(withRows.WindowTruncated);
        Assert.Equal(PerformanceMonitor.Common.McpHelpers.FormatEffectiveStart(start), withRows.EffectiveStart);
    }

    [Fact]
    public async Task GetWaitingTasks_AnEmptyWindowTheCollectorRanIn_PastCoverage_CarriesTheFloorAndTheNote()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        /* Nothing waits for hours on a quiet server: the collector ran, and no task was waiting at any of its runs. */
        await SeedLogRunsAsync("waiting_tasks", now.AddDays(-2), now, everyMinutes: 30);

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 168));

        AssertTruncatedAt(EmptyHints(root), now.AddDays(-2), "waiting_tasks");
    }

    [Fact]
    public async Task GetWaitingTasks_AnEmptyWindow_OverACoveredRange_SaysCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedLogRunsAsync("waiting_tasks", now.AddHours(-168 - 1), now, everyMinutes: 30);

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 168));

        AssertCovered(EmptyHints(root), now.AddHours(-168));
    }

    [Fact]
    public async Task GetWaitingTasks_AnEmptyOneHourWindow_IsProbed_AndNamesTheFirstRunInIt()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedLogRunsAsync("waiting_tasks", now.AddMinutes(-35), now.AddMinutes(-35), everyMinutes: 30);

        var root = Root(await McpWaitTools.GetWaitingTasks(Service(), _serverManager, ServerName, hours_back: 1));

        AssertCoveredFrom(EmptyHints(root), now.AddMinutes(-35));
    }

    [Fact]
    public async Task GetQueryStoreRegressions_AnAllClear_TheBaselineStartsInsideTheSevenDays_CarriesTheFloorAndTheNote()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-3);
        /* The same average on both sides: no regression, so the answer is the all-clear. The baseline holds three of
           its seven days, and a bare "no query regressed" would read as a verdict on all seven. */
        await SeedRegressionAsync(floor, queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 1000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24));

        /* #4966: the window is cut, so the all-clear claim gives way to the shared cut sentence. */
        Assert.Equal(McpHelpers.CutWindowNothingMessage, root.GetProperty("message").GetString());
        AssertTruncatedAt(EmptyHints(root), floor, "query_store_stats");
        Assert.Contains("baseline_start", EmptyHints(root).GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
    }

    /// <summary>#4966: the same all-clear over a baseline the store fully covers keeps its text, with no cut in the hints.</summary>
    [Fact]
    public async Task GetQueryStoreRegressions_AnAllClear_OverACoveredBaseline_KeepsTheAllClearSentence()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedRegressionAsync(now.AddDays(-9), queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddDays(-3), queryId: 1, avgUs: 1000, intervalId: 3);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 1000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24));

        Assert.Contains("this IS the all-clear", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.False(EmptyHints(root).GetProperty("window_truncated").GetBoolean());
    }

    [Fact]
    public async Task GetQueryStoreRegressions_AFilteredMiss_CarriesTheSameKeys_AndTheFloorIsTheTablesNotTheFilters()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-3);
        await SeedRegressionAsync(floor, queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 3000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24, database_name: "NoSuchDb"));

        AssertTruncatedAt(EmptyHints(root), floor, "query_store_stats");
        /* #5015: the probe is the table's, so the filter that matched nothing used to get the all-clear text. It says the filter missed. */
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("database_name 'NoSuchDb'", message, StringComparison.Ordinal);
        Assert.Contains("the filter matched nothing and this is NOT the all-clear", message, StringComparison.Ordinal);
        Assert.DoesNotContain("this IS the all-clear", message, StringComparison.Ordinal);
    }

    /// <summary>#5015: a filter that matches, and finds nothing wrong, is still the all-clear.</summary>
    [Fact]
    public async Task GetQueryStoreRegressions_AFilterThatMatches_AndFindsNothingWrong_IsTheAllClear()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedRegressionAsync(now.AddDays(-9), queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddDays(-3), queryId: 1, avgUs: 1000, intervalId: 3);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 1000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24, database_name: "Db"));

        Assert.Contains("this IS the all-clear", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// #5015: a filter whose database has captures in the baseline and none in the window is a missing recent side FOR THAT
    /// DATABASE, not the all-clear: the server-wide probe saw both sides (another database wrote the window), and no query of
    /// the filtered database was compared. The answer is the server-wide "nothing collected IN the last N hour(s)" one, with
    /// the window hints, naming the database.
    /// </summary>
    [Fact]
    public async Task GetQueryStoreRegressions_AFilteredDatabase_WithBaselineCapturesOnly_SaysItsWindowIsMissing_NotTheAllClear()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-3);
        await SeedRegressionAsync(floor, queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 2, avgUs: 1000, intervalId: 2, database: "Other");

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24, database_name: "Db"));

        AssertTruncatedAt(EmptyHints(root), floor, "query_store_stats");
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("database_name 'Db'", message, StringComparison.Ordinal);
        Assert.Contains("nothing of it collected IN the last 24 hour(s)", message, StringComparison.Ordinal);
        Assert.Contains("NOT the all-clear", message, StringComparison.Ordinal);
        Assert.DoesNotContain("this IS the all-clear", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5015: a filter whose database has captures in the window and none in the baseline has nothing to compare against FOR
    /// THAT DATABASE: <c>unavailable</c>, worded like the server-wide no-baseline answer and naming the database, not the
    /// all-clear.
    /// </summary>
    [Fact]
    public async Task GetQueryStoreRegressions_AFilteredDatabase_WithWindowCapturesOnly_SaysItHasNoBaseline_NotTheAllClear()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedRegressionAsync(now.AddDays(-3), queryId: 2, avgUs: 1000, intervalId: 1, database: "Other");
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 3000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24, database_name: "Db"));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("database_name 'Db'", message, StringComparison.Ordinal);
        Assert.Contains("no baseline", message, StringComparison.Ordinal);
        Assert.Contains("NOT a clean bill of health", message, StringComparison.Ordinal);
        Assert.DoesNotContain("this IS the all-clear", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetQueryStoreRegressions_AnEmptyRecentSide_CarriesTheSameKeys()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-3);
        /* History from before the recent window and nothing inside it: the answer names the missing recent side. */
        await SeedRegressionAsync(floor, queryId: 1, avgUs: 1000, intervalId: 1);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24));

        Assert.Contains("nothing collected IN the last 24 hour(s)", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        AssertTruncatedAt(EmptyHints(root), floor, "query_store_stats");
    }

    [Fact]
    public async Task GetQueryStoreRegressions_AnAllClear_OverACoveredBaseline_SaysCovered_AtTheBaselinesStart()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedRegressionAsync(now.AddDays(-9), queryId: 2, avgUs: 1000, intervalId: 3);
        await SeedRegressionAsync(now.AddDays(-3), queryId: 1, avgUs: 1000, intervalId: 1);
        await SeedRegressionAsync(now.AddHours(-1), queryId: 1, avgUs: 1000, intervalId: 2);

        var root = Root(await McpQueryTools.GetQueryStoreRegressions(Service(), _serverManager, ServerName, hours_back: 24));

        AssertCovered(EmptyHints(root), now.AddHours(-24 - 7 * 24));
    }

    [Fact]
    public async Task GetQueryHeatmap_AFilteredMiss_PastCoverage_CarriesTheFloorAndTheNote()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var floor = now.AddDays(-2);
        await SeedQueryStatsAsync(floor, "0xH1");
        await SeedQueryStatsAsync(now.AddDays(-1), "0xH2");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 168, database_name: "NoSuchDb"));

        Assert.Contains("a database_name filter matching nothing collected", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        AssertTruncatedAt(EmptyHints(root), floor, "query_stats");
    }

    [Fact]
    public async Task GetQueryHeatmap_AFilteredMiss_OverACoveredRange_SaysCovered()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedQueryStatsAsync(now.AddDays(-8), "0xH0");
        await SeedQueryStatsAsync(now.AddDays(-2), "0xH1");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 168, database_name: "NoSuchDb"));

        AssertCovered(EmptyHints(root), now.AddHours(-168));
    }

    [Fact]
    public async Task GetQueryHeatmap_AnEmptyOneHourWindow_IsProbed_AndNamesTheFirstRowInIt()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedQueryStatsAsync(now.AddMinutes(-35), "0xH1");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 1, database_name: "NoSuchDb"));

        AssertCoveredFrom(EmptyHints(root), now.AddMinutes(-35));
    }

    /// <summary>
    /// #5015, RED: rows only from 30 days ago leave a 168-hour grid with no columns, and the answer used to say "nothing
    /// collected IN the last 168 hour(s)" beside <c>window_truncated: false</c>, a contradiction. The store holds no
    /// collection in the window, so the keys now say so.
    /// </summary>
    [Fact]
    public async Task GetQueryHeatmap_RowsOnlyOutsideTheWindow_SaysTheStoreHoldsNothingInIt()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        await SeedQueryStatsAsync(now.AddDays(-30), "0xH0");

        var root = Root(await McpQueryTools.GetQueryHeatmap(Service(), _serverManager, ServerName, hours_back: 168));

        Assert.Contains("nothing collected IN the last 168 hour(s)", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        AssertNothingHeld(EmptyHints(root), "query_stats");
    }

    /* ───────────────────────── assertions ───────────────────────── */

    /// <summary>
    /// The window read of an <c>empty</c> status: the three keys sit under <c>hints</c> (the shape the Query Store module
    /// miss uses), so the assertions below read them off that object, exactly as they read a data answer's root.
    /// </summary>
    private static JsonElement EmptyHints(JsonElement root)
    {
        Assert.Equal("empty", root.GetProperty("status").GetString());
        return root.GetProperty("hints");
    }

    /// <summary>The store's data starts at <paramref name="floor"/>, later than the window asked for.</summary>
    private static void AssertTruncatedAt(JsonElement root, DateTime floor, string table)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStartText = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", effectiveStartText, StringComparison.Ordinal);
        var effectiveStart = ParseUtc(effectiveStartText);
        Assert.True(Math.Abs((effectiveStart - floor).TotalSeconds) < 5,
            $"effective_start {effectiveStart:o} should be the seeded floor {floor:o}");
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"raw {table} retains", note, StringComparison.Ordinal);
        AssertNoReachKey(root);
    }

    /// <summary>
    /// #5015: an empty answer over a window the store holds no collection in. Not covered: <c>window_truncated</c> true, no
    /// <c>effective_start</c> (null, there is no start to name) and a note that says so.
    /// </summary>
    private static void AssertNothingHeld(JsonElement root, string table)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("effective_start").ValueKind);
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"holds no collection of {table}", note, StringComparison.Ordinal);
        AssertNoReachKey(root);
    }

    /// <summary>The probe found data inside the slack: covered, and <c>effective_start</c> is where the data starts.</summary>
    private static void AssertCoveredFrom(JsonElement root, DateTime firstInstant)
    {
        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - firstInstant).TotalSeconds) < 5,
            $"effective_start {text} should be the first collection {firstInstant:o}");
        AssertNoReachKey(root);
    }

    /// <summary>The store held the whole window: the keys are present, false and null, at the requested start.</summary>
    private static void AssertCovered(JsonElement root, DateTime requestedStart)
    {
        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var effectiveStartText = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", effectiveStartText, StringComparison.Ordinal);
        var effectiveStart = ParseUtc(effectiveStartText);
        Assert.True(Math.Abs((effectiveStart - requestedStart).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should be the requested start {requestedStart:o}");
        AssertNoReachKey(root);
    }

    /// <summary>#4966: where the page's rows stop names the oldest row it returned, as UTC with the Z.</summary>
    private static void AssertOldestReturned(JsonElement root, DateTime oldestRow)
    {
        var text = root.GetProperty("oldest_returned_collection_time").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - oldestRow).TotalSeconds) < 2,
            $"oldest_returned_collection_time {text} should be the oldest returned row {oldestRow:o}");
    }

    /// <summary>
    /// All four payloads carry a page cut under <c>truncated</c>, and Darling's census fails
    /// <c>effective_hours_back</c> in a block that holds one, so the reach is the instant alone.
    /// </summary>
    private static void AssertNoReachKey(JsonElement root) =>
        Assert.False(root.TryGetProperty("effective_hours_back", out _));

    /* ───────────────────────── seeding ───────────────────────── */

    private async Task ExecuteAsync(string sql, params object[] values)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedSnapshotAsync(DateTime at) => ExecuteAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, cpu_time_ms, total_elapsed_time_ms)
VALUES ($1, $2, $3, $4, 55, 'Db', 'SELECT 1', 'running', 0, 10, 20)",
        _nextId++, Naive(at), _serverId, ServerName);

    private Task SeedWaitingTaskAsync(DateTime at) => ExecuteAsync(@"
INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, database_name)
VALUES ($1, $2, $3, $4, 55, 'LCK_M_X', 3000, 60, 'Db')",
        _nextId++, Naive(at), _serverId, ServerName);

    /// <summary>One capture of <paramref name="queryHash"/>, with executions, so the heatmap has a cell for it.</summary>
    private Task SeedIdleQueryStatsAsync(DateTime at, string queryHash) => ExecuteAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, 'Db', $5, $6, $2, 0, 0, 0, $7)",
        _nextId++, Naive(at), _serverId, ServerName, queryHash, "0xS" + queryHash, "SELECT " + queryHash);

    private Task SeedQueryStatsAsync(DateTime at, string queryHash) => ExecuteAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, 'Db', $5, $6, $2, 10, 5000, 5000, $7)",
        _nextId++, Naive(at), _serverId, ServerName, queryHash, "0xS" + queryHash, "SELECT " + queryHash);

    /// <summary>
    /// One Query Store interval of a query at <paramref name="avgUs"/> CPU and duration: a baseline row or a regressed one.
    /// <paramref name="database"/> is the database it belongs to ("Db" unless a test needs a second one to filter on).
    /// </summary>
    private Task SeedRegressionAsync(DateTime at, long queryId, long avgUs, long intervalId, string database = "Db") => ExecuteAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $8, $5, 9, 'Regular', 100, $6, $6, 100, $7, 'SELECT * FROM dbo.Widgets', $2)",
        _nextId++, Naive(at), _serverId, ServerName, queryId, avgUs, intervalId, database);

    /// <summary>The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes, whether or not anything ran or waited.</summary>
    private async Task SeedLogRunsAsync(string collector, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        await ExecuteAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, $4, g.t, 12, 'SUCCESS', 0
FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL {everyMinutes} MINUTE) AS g(t)",
            _nextId, _serverId, ServerName, collector, Naive(firstUtc), Naive(lastUtc));
        _nextId += 100_000;
    }
}

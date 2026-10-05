/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// #4966: where the data starts, on Lite's three aggregate tools: <c>get_default_trace_events</c>, <c>get_wait_stats</c> and
/// <c>get_blocking_stats</c>. Each publishes <c>effective_start</c>, <c>window_truncated</c> and <c>truncation_note</c> right
/// after <c>hours_back</c> on a data answer, from the coverage probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>).
/// An <c>empty</c> answer carries the same three keys under <c>hints</c>; <c>unavailable</c> and <c>not_collected</c> keep their
/// shape. <c>get_blocking_stats</c> reads two collectors and says ONE thing, from the EARLIER of the blocked process reports'
/// coverage (with the DMV blocking snapshots beside it) and the deadlocks'. <c>get_daily_summary_range</c> writes no keys,
/// because every row it returns carries its own data state and the store's retention horizon (pinned at the end). Every window
/// is anchored at a fixed <c>as_of</c>, never the clock. Own <see cref="DuckDbInitializer"/> per test, like
/// <see cref="McpWindowNoticeConfigAndLogToolTests"/>.
/// </summary>
public sealed class McpWindowNoticeAggregateToolTests : IDisposable
{
    private const string ServerName = "AggregateNoticeServer";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private const int HoursBack = 168;
    private const int ServerUtcOffsetMinutes = -300;
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = Anchor.AddHours(-HoursBack);

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    public McpWindowNoticeAggregateToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpWindowNoticeAggregate_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _seed = new PendingSeedSession(_duckDb);

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seed.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static DateTime ParseUtc(string text) =>
        DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private LocalDataService Service()
    {
        _seed.Flush();
        return new(_duckDb);
    }

    private Task<string> TraceAsync(int hoursBack = HoursBack, int limit = 100) =>
        McpDefaultTraceTools.GetDefaultTraceEvents(Service(), _serverManager, ServerName, hoursBack, limit, AsOf);

    private Task<string> WaitsAsync(int hoursBack = HoursBack, int limit = 20) =>
        McpWaitTools.GetWaitStats(Service(), _serverManager, ServerName, hoursBack, limit, AsOf);

    private Task<string> BlockingAsync(int hoursBack = HoursBack) =>
        McpHealthTools.GetBlockingStats(Service(), _serverManager, ServerName, hoursBack, AsOf);

    /* ───────────────────────── get_default_trace_events ───────────────────────── */

    [Fact]
    public async Task DefaultTrace_CollectionStartsInsideTheWindow_ReportsTheFloorInUtc_AndTheNote()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();
        var floor = Anchor.AddDays(-2);
        await SeedRunsAsync("default_trace_events", floor, Anchor, everyMinutes: 30);
        await SeedTraceEventAsync(floor);
        await SeedTraceEventAsync(Anchor.AddDays(-1));

        var root = Root(await TraceAsync());

        /* The store holds the event in the server's wall clock (UTC-5 here); the floor is named in UTC, as the read names it. */
        AssertTruncatedAt(root, floor, "default_trace_events");
    }

    [Fact]
    public async Task DefaultTrace_AQuietStart_CollectedFromBeforeTheWindow_IsCovered()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();
        await SeedRunsAsync("default_trace_events", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedTraceEventAsync(Anchor.AddDays(-2));

        var root = Root(await TraceAsync());

        AssertCovered(root, WindowStart);
    }

    /// <summary>
    /// The Default Trace stores its times in the server's wall clock (five hours behind UTC here). The first event happened a day
    /// into the window; it is stored five hours earlier. The floor is named at the UTC instant of the event, not at the stored
    /// wall-clock time the column holds.
    /// </summary>
    [Fact]
    public async Task DefaultTrace_TheFloorIsConvertedFromTheServersWallClock()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();
        var floorUtc = WindowStart.AddDays(1);
        await SeedTraceEventAsync(floorUtc);
        await SeedTraceEventAsync(Anchor.AddDays(-1));

        var root = Root(await TraceAsync());

        AssertTruncatedAt(root, floorUtc, "default_trace_events");
        var named = ParseUtc(root.GetProperty("effective_start").GetString()!);
        Assert.True(Math.Abs((named - floorUtc.AddMinutes(ServerUtcOffsetMinutes)).TotalHours) > 1, "the floor must not be the stored wall-clock time");
    }

    [Fact]
    public async Task DefaultTrace_ADataAnswer_CarriesTheKeysRightAfterHoursBack()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();
        await SeedRunsAsync("default_trace_events", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedTraceEventAsync(Anchor.AddDays(-2));

        AssertKeysFollowHoursBack(Root(await TraceAsync()));
    }

    [Fact]
    public async Task DefaultTrace_AShortWindowWithRows_StartsNoProbe_AndIsCoveredAtTheRequestedStart()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();
        await SeedTraceEventAsync(Anchor.AddMinutes(-30));

        var root = Root(await TraceAsync(hoursBack: 1));

        /* A probe would have named the row, 30 minutes in. */
        AssertCovered(root, Anchor.AddHours(-1));
    }

    [Fact]
    public async Task DefaultTrace_AnEmptyWindow_PastCoverage_CarriesTheFloorAndTheNote_UnderHints()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();
        await SeedRunsAsync("default_trace_events", Anchor.AddDays(-2), Anchor, everyMinutes: 30);

        var root = Root(await TraceAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertTruncatedAt(root.GetProperty("hints"), Anchor.AddDays(-2), "default_trace_events");
    }

    [Fact]
    public async Task DefaultTrace_AnEmptyWindow_TheStoreHoldsNothingIn_SaysNothingWasRead()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();
        await SeedRunsAsync("default_trace_events", WindowStart.AddDays(-9), WindowStart.AddDays(-8), everyMinutes: 60);

        var root = Root(await TraceAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), "default_trace_events");
    }

    [Fact]
    public async Task DefaultTrace_AnEmptyShortWindow_IsAlwaysProbed()
    {
        await _duckDb.InitializeAsync();
        await SeedServerClockAsync();

        var root = Root(await TraceAsync(hoursBack: 1));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), "default_trace_events");
    }

    /* ───────────────────────── get_wait_stats ───────────────────────── */

    [Fact]
    public async Task WaitStats_CollectionStartsInsideTheWindow_ReportsTheFloor_AndTheNote()
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedRunsAsync("wait_stats", floor, Anchor, everyMinutes: 30);
        await SeedWaitAsync(floor);
        await SeedWaitAsync(Anchor.AddDays(-1));

        var root = Root(await WaitsAsync());

        AssertTruncatedAt(root, floor, "wait_stats");
    }

    [Fact]
    public async Task WaitStats_AQuietStart_CollectedFromBeforeTheWindow_IsCovered()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("wait_stats", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedWaitAsync(Anchor.AddDays(-2));

        AssertCovered(Root(await WaitsAsync()), WindowStart);
    }

    [Fact]
    public async Task WaitStats_ADataAnswer_CarriesTheKeysRightAfterHoursBack()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("wait_stats", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedWaitAsync(Anchor.AddDays(-2));

        AssertKeysFollowHoursBack(Root(await WaitsAsync()));
    }

    [Fact]
    public async Task WaitStats_AShortWindowWithRows_StartsNoProbe_AndIsCoveredAtTheRequestedStart()
    {
        await _duckDb.InitializeAsync();
        await SeedWaitAsync(Anchor.AddMinutes(-30));

        AssertCovered(Root(await WaitsAsync(hoursBack: 1)), Anchor.AddHours(-1));
    }

    /// <summary>A capped page (<c>limit: 1</c> over two wait types) sits next to a window floor: the page cut does not change the notice.</summary>
    [Fact]
    public async Task WaitStats_ACappedPage_NextToAWindowFloor_LeavesTheNoticeUnchanged()
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedRunsAsync("wait_stats", floor, Anchor, everyMinutes: 30);
        await SeedWaitAsync(floor, "PAGEIOLATCH_SH");
        await SeedWaitAsync(Anchor.AddDays(-1), "WRITELOG");

        var root = Root(await WaitsAsync(limit: 1));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        AssertTruncatedAt(root, floor, "wait_stats");
    }

    /// <summary>The no-rows answer is <c>unavailable</c> and stays bare, even beside collector runs: it keys on the data answer only.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WaitStats_NoRows_IsUnavailable_AndStaysBare(bool collectorRan)
    {
        await _duckDb.InitializeAsync();
        if (collectorRan)
        {
            await SeedRunsAsync("wait_stats", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        }

        var root = Root(await WaitsAsync());

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        AssertBare(root);
    }

    /* ───────────────────────── get_blocking_stats ───────────────────────── */

    [Fact]
    public async Task BlockingStats_CollectionStartsInsideTheWindow_ReportsTheFloor_AndTheNote()
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedRunsAsync("blocked_process_report", floor, Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", floor, Anchor, everyMinutes: 30);
        await SeedBprAsync(floor);
        await SeedBprAsync(Anchor.AddDays(-1));

        var root = Root(await BlockingAsync());

        AssertTruncatedAt(root, floor, "blocked_process_report and deadlocks");
    }

    [Fact]
    public async Task BlockingStats_AQuietStart_CollectedFromBeforeTheWindow_IsCovered()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedBprAsync(Anchor.AddDays(-2));

        AssertCovered(Root(await BlockingAsync()), WindowStart);
    }

    /// <summary>
    /// One notice, from the LATER of the two series' floors, whichever collector is later: blocking and deadlocks are separate
    /// series in one answer, and the series that began later has an empty head that would read as "none happened". Both start
    /// inside the window, three days and two days in, in both orders: the notice is at the two-day one.
    /// </summary>
    [Theory]
    [InlineData(-3, -2)]
    [InlineData(-2, -3)]
    public async Task BlockingStats_TheLaterOfTheTwoSeriesFloors_Wins(int blockedProcessDays, int deadlockDays)
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", Anchor.AddDays(blockedProcessDays), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", Anchor.AddDays(deadlockDays), Anchor, everyMinutes: 30);
        await SeedBprAsync(Anchor.AddDays(-1));

        var root = Root(await BlockingAsync());

        AssertTruncatedAt(root, Anchor.AddDays(Math.Max(blockedProcessDays, deadlockDays)), "blocked_process_report and deadlocks");
    }

    /// <summary>
    /// One series covers the window from before it and the other began inside it: the notice names the later start, in both
    /// orders. An uncovered deadlock half beside covered blocking reads as "no deadlocks" otherwise, and the reverse.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BlockingStats_OneSeriesCoveredFromBeforeTheWindow_TheOtherStartingInside_NoticeIsAtTheLaterStart(bool deadlocksCoverFirst)
    {
        await _duckDb.InitializeAsync();
        var early = deadlocksCoverFirst ? "deadlocks" : "blocked_process_report";
        var late = deadlocksCoverFirst ? "blocked_process_report" : "deadlocks";
        var lateStart = Anchor.AddDays(-2);
        await SeedRunsAsync(early, WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedRunsAsync(late, lateStart, Anchor, everyMinutes: 30);
        await SeedBprAsync(Anchor.AddDays(-1));

        AssertTruncatedAt(Root(await BlockingAsync()), lateStart, "blocked_process_report and deadlocks");
    }

    /// <summary>Deadlock rows and no blocking rows: a data answer from the second series alone, with that series' floor.</summary>
    [Fact]
    public async Task BlockingStats_DeadlockRowsOnly_IsADataAnswer_WithTheDeadlockFloor()
    {
        await _duckDb.InitializeAsync();
        var deadlockStart = Anchor.AddDays(-2);
        await SeedRunsAsync("blocked_process_report", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", deadlockStart, Anchor, everyMinutes: 30);
        await SeedDeadlockAsync(Anchor.AddDays(-1));

        var root = Root(await BlockingAsync());

        Assert.False(root.TryGetProperty("status", out _));
        Assert.Equal(0, root.GetProperty("blocking_duration").GetArrayLength());
        Assert.Equal(1, root.GetProperty("deadlock_severity").GetArrayLength());
        AssertTruncatedAt(root, deadlockStart, "blocked_process_report and deadlocks");
    }

    /// <summary>
    /// Inside the blocking series the EARLIER source wins: the XE reports start three days in and the DMV snapshots one day in,
    /// the deadlocks are covered, so the blocking floor (and the notice) is the XE start. The reverse order is the DMV test below.
    /// </summary>
    [Fact]
    public async Task BlockingStats_TheXeReportsStartingEarly_TheDmvSnapshotsLate_BlockingFloorIsTheXeStart()
    {
        await _duckDb.InitializeAsync();
        var xeStart = Anchor.AddDays(-3);
        await SeedRunsAsync("blocked_process_report", xeStart, Anchor, everyMinutes: 30);
        await SeedRunsAsync("dmv_blocking_snapshot", Anchor.AddDays(-1), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedBprAsync(Anchor.AddHours(-20));

        AssertTruncatedAt(Root(await BlockingAsync()), xeStart, "blocked_process_report and deadlocks");
    }

    /// <summary>Both series covered from before the window, with rows in each: no notice.</summary>
    [Fact]
    public async Task BlockingStats_BothSeriesCoveredFromBeforeTheWindow_HaveNoNotice()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedBprAsync(Anchor.AddDays(-1));
        await SeedDeadlockAsync(Anchor.AddDays(-1));

        var root = Root(await BlockingAsync());

        Assert.Equal(1, root.GetProperty("deadlock_severity").GetArrayLength());
        AssertCovered(root, WindowStart);
    }

    /// <summary>The DMV blocking snapshots are the report grid's also-covered source: a server with no XE session is covered by them.</summary>
    [Fact]
    public async Task BlockingStats_TheDmvCollectorCoveringFromBeforeTheWindow_CountsAsTheBlockingCoverage()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("dmv_blocking_snapshot", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedRunsAsync("blocked_process_report", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedDmvAsync(Anchor.AddDays(-1));

        AssertCovered(Root(await BlockingAsync()), WindowStart);
    }

    [Fact]
    public async Task BlockingStats_ADataAnswer_CarriesTheKeysRightAfterHoursBack_AndOneNoticeOnly()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedBprAsync(Anchor.AddDays(-1));

        var root = Root(await BlockingAsync());

        AssertKeysFollowHoursBack(root);
        /* One notice for the payload, none inside either series. */
        foreach (var series in new[] { "blocking_duration", "deadlock_severity" })
        {
            foreach (var point in root.GetProperty(series).EnumerateArray())
            {
                Assert.False(point.TryGetProperty("effective_start", out _));
                Assert.False(point.TryGetProperty("window_truncated", out _));
            }
        }
    }

    [Fact]
    public async Task BlockingStats_AShortWindowWithRows_StartsNoProbe_AndIsCoveredAtTheRequestedStart()
    {
        await _duckDb.InitializeAsync();
        await SeedBprAsync(Anchor.AddMinutes(-30));

        AssertCovered(Root(await BlockingAsync(hoursBack: 1)), Anchor.AddHours(-1));
    }

    [Fact]
    public async Task BlockingStats_AnEmptyWindow_PastCoverage_CarriesTheFloorAndTheNote_UnderHints()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", Anchor.AddDays(-3), Anchor, everyMinutes: 30);

        var root = Root(await BlockingAsync());

        /* The later of the two series' starts: before day -2 the blocking series was not collected, so its empty head says nothing. */
        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertTruncatedAt(root.GetProperty("hints"), Anchor.AddDays(-2), "blocked_process_report and deadlocks");

        /* #4966: cut at a start, so the "genuinely clear" claim gives way to the cut sentence that points at effective_start. */
        Assert.Equal($"No blocking or deadlocks recorded for {ServerName} in the last {HoursBack} hour(s). {McpHelpers.CutWindowNothingMessage}", root.GetProperty("message").GetString());
    }

    /// <summary>#4966: both blocking collectors have run from before the window: the covered text keeps its "genuinely clear" claim.</summary>
    [Fact]
    public async Task BlockingStats_AnEmptyWindow_Covered_KeepsTheGenuinelyClearClaim()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedRunsAsync("deadlocks", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);

        var root = Root(await BlockingAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertCovered(root.GetProperty("hints"), WindowStart);
        Assert.Equal(
            $"No blocking or deadlocks recorded for {ServerName} in the last {HoursBack} hour(s). The blocking collectors HAVE run successfully for this server, so the window is genuinely clear rather than blind.",
            root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task BlockingStats_AnEmptyWindow_TheStoreHoldsNothingIn_SaysNothingWasRead()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", WindowStart.AddDays(-9), WindowStart.AddDays(-8), everyMinutes: 60);

        var root = Root(await BlockingAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), "blocked_process_report and deadlocks");
        /* #4966: no start to name, so the sentence says nothing was read. */
        Assert.Equal($"No blocking or deadlocks recorded for {ServerName} in the last {HoursBack} hour(s). {McpHelpers.CutWindowNothingReadMessage}", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task BlockingStats_AnEmptyShortWindow_IsAlwaysProbed()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("blocked_process_report", WindowStart.AddDays(-9), WindowStart.AddDays(-8), everyMinutes: 60);

        var root = Root(await BlockingAsync(hoursBack: 1));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), "blocked_process_report and deadlocks");
    }

    [Fact]
    public async Task BlockingStats_NeverCollected_IsUnavailable_AndStaysBare()
    {
        await _duckDb.InitializeAsync();

        var root = Root(await BlockingAsync());

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        AssertBare(root);
    }

    /* ───────────────────────── get_daily_summary_range: no notice, by design ───────────────────────── */

    /// <summary>
    /// The tool writes no data-start keys, because each day it returns carries its own state (<c>data_state</c>, with a
    /// <c>data_note</c> before the horizon) and the payload names the store's <c>retention_horizon</c>: a day before the horizon
    /// is never Healthy, and a day the store does not hold is absent, which the tool's contract calls a collection gap. A notice
    /// would repeat that. This pins both halves, so a later edit that drops the per-day state has to add a notice.
    /// </summary>
    [Fact]
    public async Task DailySummaryRange_CarriesItsOwnDataState_AndNoWindowKeys()
    {
        await _duckDb.InitializeAsync();
        await SeedRunsAsync("wait_stats", Anchor.AddDays(-1), Anchor, everyMinutes: 60);

        var root = Root(await McpHealthTools.GetDailySummaryRange(Service(), _serverManager, ServerName, 7, AsOf));

        Assert.False(root.TryGetProperty("effective_start", out _));
        Assert.False(root.TryGetProperty("window_truncated", out _));
        Assert.False(root.TryGetProperty("truncation_note", out _));
        Assert.True(root.TryGetProperty("retention_horizon", out _));
        Assert.True(root.TryGetProperty("days_before_horizon", out _));
        Assert.True(root.GetProperty("days").GetArrayLength() >= 1);
        foreach (var day in root.GetProperty("days").EnumerateArray())
        {
            Assert.True(day.TryGetProperty("data_state", out var state) && !string.IsNullOrEmpty(state.GetString()));
        }
    }

    /* ───────────────────────── assertions ───────────────────────── */

    private static void AssertKeysFollowHoursBack(JsonElement root)
    {
        var names = new List<string>();
        foreach (var property in root.EnumerateObject())
        {
            names.Add(property.Name);
        }

        var hoursBack = names.IndexOf("hours_back");
        Assert.True(hoursBack >= 0);
        Assert.Equal(new[] { "effective_start", "window_truncated", "truncation_note" }, names.GetRange(hoursBack + 1, 3));
    }

    /// <summary>The store's data starts at <paramref name="floor"/>, later than the window asked for.</summary>
    private static void AssertTruncatedAt(JsonElement root, DateTime floor, string table)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - floor).TotalSeconds) < 5,
            $"effective_start {text} should be the seeded floor {floor:o}");
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"raw {table} retains", note, StringComparison.Ordinal);
        Assert.False(root.TryGetProperty("effective_hours_back", out _));
    }

    /// <summary>An empty answer over a window the store holds no collection in: not covered, and no start to name.</summary>
    private static void AssertNothingHeld(JsonElement root, string table)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("effective_start").ValueKind);
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"holds no collection of {table}", note, StringComparison.Ordinal);
        Assert.False(root.TryGetProperty("effective_hours_back", out _));
    }

    /// <summary>The store held the whole window: the keys are present, false and null, at the requested start.</summary>
    private static void AssertCovered(JsonElement root, DateTime requestedStart)
    {
        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - requestedStart).TotalMinutes) < 2,
            $"effective_start {text} should be the requested start {requestedStart:o}");
        Assert.False(root.TryGetProperty("effective_hours_back", out _));
    }

    private static void AssertBare(JsonElement root)
    {
        Assert.False(root.TryGetProperty("hints", out _));
        Assert.False(root.TryGetProperty("window_truncated", out _));
        Assert.False(root.TryGetProperty("effective_start", out _));
        Assert.False(root.TryGetProperty("truncation_note", out _));
    }

    /* ───────────────────────── seeding ───────────────────────── */

    /* #5208: every seed statement joins one open transaction (PendingSeedSession) instead of committing on its own;
       Service() commits it before anything reads, so the code under test sees the same rows in the same order. */
    private Task ExecuteAsync(string sql, params object?[] values) => _seed.ExecuteAsync(sql, values);

    /// <summary>The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes, whether or not it stored anything.</summary>
    private async Task SeedRunsAsync(string collector, DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        await ExecuteAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, $6, g.t, 12, 'SUCCESS', 0
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL {everyMinutes} MINUTE) AS g(t)",
            _nextId, _serverId, ServerName, Naive(firstUtc), Naive(lastUtc), collector);
        _nextId += 100_000;
    }

    /// <summary>A server whose wall clock is five hours behind UTC all year: the Default Trace stores its times in it.</summary>
    private Task SeedServerClockAsync() => ExecuteAsync(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, utc_offset_minutes)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5)",
        _nextId++, Naive(WindowStart.AddDays(-1)), _serverId, ServerName, ServerUtcOffsetMinutes);

    /// <summary>A default trace event at <paramref name="atUtc"/>, stored as the trace stores it: in the server's wall clock.</summary>
    private Task SeedTraceEventAsync(DateTime atUtc) => ExecuteAsync(@"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name,
     database_name, duration_us, integer_data, severity, error_number, text_data)
VALUES ($1, $2, $3, $4, $5, 'Server Memory Change', NULL, NULL, NULL, NULL, NULL, 'memory')",
        _nextId++, Naive(atUtc), _serverId, ServerName, Naive(atUtc.AddMinutes(ServerUtcOffsetMinutes)));

    private Task SeedWaitAsync(DateTime at, string waitType = "PAGEIOLATCH_SH") => ExecuteAsync(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, 10, 1000, 100, 5, 500, 50)",
        _nextId++, Naive(at), _serverId, ServerName, waitType);

    private Task SeedBprAsync(DateTime at) => ExecuteAsync(@"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms)
VALUES ($1, $2, $3, $4, $2, 4000)",
        _nextId++, Naive(at), _serverId, ServerName);

    private Task SeedDeadlockAsync(DateTime at) => ExecuteAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $2, $5)",
        _nextId++, Naive(at), _serverId, ServerName,
        "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>"
        + "<process id=\"p1\" spid=\"55\" waittime=\"1000\"><inputbuf>x</inputbuf></process>"
        + "<process id=\"p2\" spid=\"66\" waittime=\"3000\"><inputbuf>x</inputbuf></process></process-list></deadlock>");

    private Task SeedDmvAsync(DateTime at) => ExecuteAsync(@"
INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop, event_time, wait_time_ms)
VALUES ($1, $2, $3, $4, -1, $2, 3000)",
        _nextId++, Naive(at), _serverId, ServerName);
}

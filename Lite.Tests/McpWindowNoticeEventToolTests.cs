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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: where the data starts, on the five Lite event-list tools: <c>get_deadlocks</c>,
/// <c>get_deadlock_detail</c>, <c>get_blocked_process_reports</c>, <c>get_blocked_process_xml</c> and
/// <c>get_long_query_completions</c>. Each publishes <c>effective_start</c>, <c>window_truncated</c> and
/// <c>truncation_note</c> beside <c>hours_back</c> on a data answer, and the same three keys under <c>hints</c> on an
/// <c>empty</c> one, from the coverage probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) the other
/// window-floor tools use. They are EVENT lists, so on a data answer the floor is the EARLIER of the probe's and
/// the oldest event the page shows: a first run of a collector can store events from before itself. Every window is
/// anchored at a fixed <c>as_of</c>, never the clock. Own <see cref="DuckDbInitializer"/> per test, like
/// <see cref="McpWindowNoticeToolTests"/>.
/// </summary>
public sealed class McpWindowNoticeEventToolTests : IDisposable
{
    private const string ServerName = "EventNoticeServer";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private const int HoursBack = 168;
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = Anchor.AddHours(-HoursBack);

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    /// <summary>The five tools under test.</summary>
    public enum EventTool
    {
        Deadlocks,
        DeadlockDetail,
        BlockedProcessReports,
        BlockedProcessXml,
        LongQueryCompletions
    }

    public McpWindowNoticeEventToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpWindowNoticeEvent_" + Guid.NewGuid().ToString("N")[..8]);
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

    private static DateTime ParseUtc(string text) =>
        DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private LocalDataService Service() => new(_duckDb);

    /* ───────────────────────── per-tool wiring ───────────────────────── */

    private Task<string> CallAsync(EventTool tool) => tool switch
    {
        EventTool.Deadlocks => McpBlockingTools.GetDeadlocks(Service(), _serverManager, ServerName, HoursBack, as_of: AsOf),
        EventTool.DeadlockDetail => McpBlockingTools.GetDeadlockDetail(Service(), _serverManager, ServerName, HoursBack, as_of: AsOf),
        EventTool.BlockedProcessReports => McpBlockingTools.GetBlockedProcessReports(Service(), _serverManager, ServerName, HoursBack, as_of: AsOf),
        EventTool.BlockedProcessXml => McpBlockingTools.GetBlockedProcessXml(Service(), _serverManager, ServerName, HoursBack, as_of: AsOf),
        EventTool.LongQueryCompletions => McpLongQueryTools.GetLongQueryCompletions(Service(), _serverManager, ServerName, HoursBack, as_of: AsOf),
        _ => throw new ArgumentOutOfRangeException(nameof(tool))
    };

    /// <summary>The collector whose logged runs the tool's coverage is read from.</summary>
    private static string CollectorOf(EventTool tool) => tool switch
    {
        EventTool.Deadlocks or EventTool.DeadlockDetail => "deadlocks",
        EventTool.BlockedProcessReports or EventTool.BlockedProcessXml => "blocked_process_report",
        _ => "long_query_completions"
    };

    /// <summary>The table name the truncation note names.</summary>
    private static string TableOf(EventTool tool) => tool switch
    {
        EventTool.Deadlocks or EventTool.DeadlockDetail => "deadlocks",
        EventTool.BlockedProcessReports or EventTool.BlockedProcessXml => "blocked_process_report",
        _ => "long_query_completions"
    };

    /// <summary>One stored event that happened at <paramref name="eventTime"/> and was collected at <paramref name="collectedAt"/>
    /// (earlier events than the collection are what a first run backfills). The graph and report XML are set so the
    /// two XML tools count the row.</summary>
    private Task SeedEventAsync(EventTool tool, DateTime eventTime, DateTime? collectedAt = null)
    {
        var collected = Naive(collectedAt ?? eventTime);
        var happened = Naive(eventTime);
        return tool switch
        {
            EventTool.Deadlocks or EventTool.DeadlockDetail => ExecuteAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1, $2, $3, $4, $5, 'process1', 'DELETE FROM dbo.Posts', '<deadlock/>')",
                _nextId++, collected, _serverId, ServerName, happened),
            EventTool.BlockedProcessReports or EventTool.BlockedProcessXml => ExecuteAsync(@"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, blocked_spid, blocking_spid, blocked_process_report_xml)
VALUES ($1, $2, $3, $4, $5, 55, 60, '<blocked-process-report/>')",
                _nextId++, collected, _serverId, ServerName, happened),
            _ => ExecuteAsync(@"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, statement_text, duration_microseconds)
VALUES ($1, $2, $3, $4, $5, 'rpc_completed', 'Db', 'SELECT 1', 5000000)",
                _nextId++, collected, _serverId, ServerName, happened)
        };
    }

    /* ───────────────────────── a data answer ───────────────────────── */

    [Theory]
    [InlineData(EventTool.Deadlocks)]
    [InlineData(EventTool.DeadlockDetail)]
    [InlineData(EventTool.BlockedProcessReports)]
    [InlineData(EventTool.BlockedProcessXml)]
    [InlineData(EventTool.LongQueryCompletions)]
    public async Task EventsAndRunsStartInsideTheWindow_ReportTheFloor_AndTheNote(EventTool tool)
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedLogRunsAsync(CollectorOf(tool), floor, Anchor, everyMinutes: 30);
        await SeedEventAsync(tool, floor);
        await SeedEventAsync(tool, Anchor.AddDays(-1));

        var root = Root(await CallAsync(tool));

        AssertTruncatedAt(root, floor, TableOf(tool));
    }

    [Theory]
    [InlineData(EventTool.Deadlocks)]
    [InlineData(EventTool.DeadlockDetail)]
    [InlineData(EventTool.BlockedProcessReports)]
    [InlineData(EventTool.BlockedProcessXml)]
    [InlineData(EventTool.LongQueryCompletions)]
    public async Task AQuietStart_TheCollectorRanFromBeforeTheWindow_FirstEventTwoDaysIn_IsCovered(EventTool tool)
    {
        await _duckDb.InitializeAsync();
        /* The collector ran every half hour from before the window began, and nothing happened until two days in.
           A server that is quiet for days looks exactly like this, and a rows-only probe would call it truncated. */
        await SeedLogRunsAsync(CollectorOf(tool), WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedEventAsync(tool, Anchor.AddDays(-2));

        var root = Root(await CallAsync(tool));

        AssertCovered(root, WindowStart);
    }

    /// <summary>
    /// The event-time rule: a first run stores events from before itself, so the oldest event on the page can be older
    /// than the probe's floor. The collector's first run is 100 minutes into the window (past the 90-minute slack, so the
    /// probe alone would call the window cut), but it stored an event from 30 minutes in. The notice names the earlier of
    /// the two, which is inside the slack: no cut, and <c>effective_start</c> is that event.
    /// </summary>
    [Theory]
    [InlineData(EventTool.Deadlocks)]
    [InlineData(EventTool.DeadlockDetail)]
    [InlineData(EventTool.BlockedProcessReports)]
    [InlineData(EventTool.BlockedProcessXml)]
    [InlineData(EventTool.LongQueryCompletions)]
    public async Task AnEventOlderThanItsRun_FirstRunBackfill_NamesTheEvent_AndIsNotACut(EventTool tool)
    {
        await _duckDb.InitializeAsync();
        var firstRun = WindowStart.AddMinutes(100);
        var backfilled = WindowStart.AddMinutes(30);
        await SeedLogRunsAsync(CollectorOf(tool), firstRun, Anchor, everyMinutes: 30);
        await SeedEventAsync(tool, backfilled, collectedAt: firstRun);

        var root = Root(await CallAsync(tool));

        AssertCoveredFrom(root, backfilled);
    }

    /// <summary>The page cut and the window floor are separate: <c>truncated</c> is the page's, <c>window_truncated</c> the store's.</summary>
    [Fact]
    public async Task ADataAnswer_CarriesTheKeysRightAfterHoursBack()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync("deadlocks", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedEventAsync(EventTool.Deadlocks, Anchor.AddDays(-2));

        var names = new System.Collections.Generic.List<string>();
        foreach (var property in Root(await CallAsync(EventTool.Deadlocks)).EnumerateObject())
        {
            names.Add(property.Name);
        }

        var hoursBack = names.IndexOf("hours_back");
        Assert.True(hoursBack >= 0);
        Assert.Equal(new[] { "effective_start", "window_truncated", "truncation_note" }, names.GetRange(hoursBack + 1, 3));
    }

    /* ───────────────────────── the blocked process reports' second collector ───────────────────────── */

    /// <summary>
    /// The Blocked Process Reports relation is also covered by the always-on DMV blocking snapshots
    /// (<see cref="LocalDataService.QueryWindowRelationAlsoCoveredBy"/>), and <see cref="LocalDataService.GetQueryWindowFloorAsync"/>
    /// takes the earlier of the two itself, so the tools apply nothing more: with the XE collector off and the DMV collector running
    /// from before the window, the first report two days in is not a cut.
    /// </summary>
    [Theory]
    [InlineData(EventTool.BlockedProcessReports)]
    [InlineData(EventTool.BlockedProcessXml)]
    public async Task BlockedProcessReports_TheDmvCollectorCoveredTheWindow_IsCovered(EventTool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync("dmv_blocking_snapshot", WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedEventAsync(tool, Anchor.AddDays(-2));

        var root = Root(await CallAsync(tool));

        AssertCovered(root, WindowStart);
    }

    /* ───────────────────────── an empty answer says where the data starts too ───────────────────────── */

    [Theory]
    [InlineData(EventTool.Deadlocks)]
    [InlineData(EventTool.DeadlockDetail)]
    [InlineData(EventTool.BlockedProcessReports)]
    [InlineData(EventTool.BlockedProcessXml)]
    [InlineData(EventTool.LongQueryCompletions)]
    public async Task AnEmptyWindow_PastCoverage_CarriesTheFloorAndTheNote_UnderHints(EventTool tool)
    {
        await _duckDb.InitializeAsync();
        /* The collector ran for two days of a 7-day ask and nothing happened: only the keys say the older five were never read. */
        await SeedLogRunsAsync(CollectorOf(tool), Anchor.AddDays(-2), Anchor, everyMinutes: 30);

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertTruncatedAt(root.GetProperty("hints"), Anchor.AddDays(-2), TableOf(tool));
    }

    [Theory]
    [InlineData(EventTool.Deadlocks)]
    [InlineData(EventTool.DeadlockDetail)]
    [InlineData(EventTool.BlockedProcessReports)]
    [InlineData(EventTool.BlockedProcessXml)]
    [InlineData(EventTool.LongQueryCompletions)]
    public async Task AnEmptyWindow_TheStoreHoldsNothingIn_SaysNothingWasRead(EventTool tool)
    {
        await _duckDb.InitializeAsync();

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), TableOf(tool));
    }

    [Theory]
    [InlineData(EventTool.Deadlocks)]
    [InlineData(EventTool.DeadlockDetail)]
    [InlineData(EventTool.BlockedProcessReports)]
    [InlineData(EventTool.BlockedProcessXml)]
    [InlineData(EventTool.LongQueryCompletions)]
    public async Task AnEmptyWindow_TheCollectorCoveredFromBeforeIt_SaysCovered(EventTool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync(CollectorOf(tool), Anchor.AddDays(-9), Anchor, everyMinutes: 30);

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertCovered(root.GetProperty("hints"), WindowStart);
    }

    /* ───────────────────────── assertions ───────────────────────── */

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
        AssertNoReachKey(root);
    }

    /// <summary>An empty answer over a window the store holds no collection in: not covered, and no start to name.</summary>
    private static void AssertNothingHeld(JsonElement root, string table)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("effective_start").ValueKind);
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"holds no collection of {table}", note, StringComparison.Ordinal);
        AssertNoReachKey(root);
    }

    /// <summary>Data inside the slack: covered, and <c>effective_start</c> is where the data starts.</summary>
    private static void AssertCoveredFrom(JsonElement root, DateTime firstInstant)
    {
        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - firstInstant).TotalSeconds) < 5,
            $"effective_start {text} should be the first event {firstInstant:o}");
        AssertNoReachKey(root);
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
        AssertNoReachKey(root);
    }

    /// <summary>All five payloads carry a page cut under <c>truncated</c>, so the reach is the instant alone.</summary>
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

    /// <summary>The collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes, whether or not anything happened.</summary>
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

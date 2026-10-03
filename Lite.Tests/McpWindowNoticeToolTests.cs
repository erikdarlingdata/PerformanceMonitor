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

    [Fact]
    public void WindowNotice_AFloorPastTheSlack_IsTruncated_AtTheFloor_NamingTheTable()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var floor = Naive(start.AddDays(2));

        var notice = McpQueryTools.WindowNotice(floor, start, "query_snapshots");

        Assert.True(notice.WindowTruncated);
        Assert.Equal(floor.ToString("o"), notice.EffectiveStart);
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
        }

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

    /* ───────────────────────── assertions ───────────────────────── */

    /// <summary>The store's data starts at <paramref name="floor"/>, later than the window asked for.</summary>
    private static void AssertTruncatedAt(JsonElement root, DateTime floor, string table)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStart = ParseUtc(root.GetProperty("effective_start").GetString()!);
        Assert.True(Math.Abs((effectiveStart - floor).TotalSeconds) < 5,
            $"effective_start {effectiveStart:o} should be the seeded floor {floor:o}");
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"raw {table} retains", note, StringComparison.Ordinal);
        AssertNoReachKey(root);
    }

    /// <summary>The store held the whole window: the keys are present, false and null, at the requested start.</summary>
    private static void AssertCovered(JsonElement root, DateTime requestedStart)
    {
        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var effectiveStart = ParseUtc(root.GetProperty("effective_start").GetString()!);
        Assert.True(Math.Abs((effectiveStart - requestedStart).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should be the requested start {requestedStart:o}");
        AssertNoReachKey(root);
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
    private Task SeedQueryStatsAsync(DateTime at, string queryHash) => ExecuteAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, 'Db', $5, $6, $2, 10, 5000, 5000, $7)",
        _nextId++, Naive(at), _serverId, ServerName, queryHash, "0xS" + queryHash, "SELECT " + queryHash);

    /// <summary>One Query Store interval of a query at <paramref name="avgUs"/> CPU and duration: a baseline row or a regressed one.</summary>
    private Task SeedRegressionAsync(DateTime at, long queryId, long avgUs, long intervalId) => ExecuteAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, 'Db', $5, 9, 'Regular', 100, $6, $6, 100, $7, 'SELECT * FROM dbo.Widgets', $2)",
        _nextId++, Naive(at), _serverId, ServerName, queryId, avgUs, intervalId);

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

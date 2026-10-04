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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4966: where the data starts, on six Lite tools: the three config-change tools (<c>get_server_config_changes</c>,
/// <c>get_database_config_changes</c>, <c>get_trace_flag_changes</c>), the one-server form of <c>get_collection_log</c>,
/// <c>get_plan_corrections</c> and <c>get_memory_pressure_events</c>. Each publishes <c>effective_start</c>,
/// <c>window_truncated</c> and <c>truncation_note</c> beside <c>hours_back</c> on a data answer, and the same three keys
/// under <c>hints</c> on an <c>empty</c> one, from the coverage probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>)
/// the other window-floor tools use. None of them uses the event-time form (<c>McpQueryTools.EventWindowNoticeAsync</c>): a
/// config change is stamped with the capture time of the snapshot that showed it, a plan-correction row and a log run with the
/// run's own time, and the memory pressure probe reads <c>sample_time</c>, the column the list is windowed on, so the oldest row
/// shown can never be older than the probe's floor. Every window is anchored at a fixed <c>as_of</c>, never the clock. Own
/// <see cref="DuckDbInitializer"/> per test, like <see cref="McpWindowNoticeEventToolTests"/>.
/// </summary>
public sealed class McpWindowNoticeConfigAndLogToolTests : IDisposable
{
    private const string ServerName = "ConfigLogNoticeServer";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private const int HoursBack = 168;
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = Anchor.AddHours(-HoursBack);

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    /// <summary>The tools under test. The collection log is the one-server form.</summary>
    public enum Tool
    {
        ServerConfig,
        DatabaseConfig,
        TraceFlags,
        CollectionLog,
        PlanCorrections,
        MemoryPressure
    }

    public McpWindowNoticeConfigAndLogToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpWindowNoticeConfigLog_" + Guid.NewGuid().ToString("N")[..8]);
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

    private Task<string> CallAsync(Tool tool, int hoursBack = HoursBack, int? limit = null) => tool switch
    {
        Tool.ServerConfig => McpConfigHistoryTools.GetServerConfigChanges(Service(), _serverManager, ServerName, hoursBack, as_of: AsOf),
        Tool.DatabaseConfig => McpConfigHistoryTools.GetDatabaseConfigChanges(Service(), _serverManager, ServerName, hoursBack, as_of: AsOf),
        Tool.TraceFlags => McpConfigHistoryTools.GetTraceFlagChanges(Service(), _serverManager, ServerName, hoursBack, as_of: AsOf),
        Tool.CollectionLog => McpHealthTools.GetCollectionLog(Service(), _serverManager, ServerName, hoursBack, limit: limit, as_of: AsOf),
        Tool.PlanCorrections => McpPlanCorrectionTools.GetPlanCorrections(Service(), _serverManager, ServerName, hoursBack, limit: limit ?? 25, as_of: AsOf),
        Tool.MemoryPressure => McpMemoryTools.GetMemoryPressureEvents(Service(), _serverManager, ServerName, hoursBack, as_of: AsOf),
        _ => throw new ArgumentOutOfRangeException(nameof(tool))
    };

    /// <summary>The collector whose logged runs the tool's coverage is read from. The collection log is the run log itself: its
    /// own rows are the coverage, and the collector it is seeded under is any one.</summary>
    private static string CollectorOf(Tool tool) => tool switch
    {
        Tool.ServerConfig => "server_config",
        Tool.DatabaseConfig => "database_config",
        Tool.TraceFlags => "trace_flags",
        Tool.PlanCorrections => "plan_correction",
        Tool.MemoryPressure => "memory_pressure_events",
        _ => "wait_stats"
    };

    /// <summary>The table name the truncation note names.</summary>
    private static string TableOf(Tool tool) => tool switch
    {
        Tool.ServerConfig => "server_config",
        Tool.DatabaseConfig => "database_config",
        Tool.TraceFlags => "trace_flags",
        Tool.CollectionLog => "collection_log",
        Tool.PlanCorrections => "plan_correction",
        _ => "memory_pressure_events"
    };

    /// <summary>
    /// Data for the tool at <paramref name="at"/>, which is what its answer lists. A config tool needs a CHANGE, so this seeds
    /// the baseline snapshot at <paramref name="baselineAt"/> and the changed one at <paramref name="at"/> (the change is stamped
    /// with the later capture time). The others seed one row stamped <paramref name="at"/>.
    /// </summary>
    private async Task SeedDataAsync(Tool tool, DateTime at, DateTime? baselineAt = null)
    {
        var baseline = baselineAt ?? at.AddMinutes(-20);
        switch (tool)
        {
            case Tool.ServerConfig:
                await SeedServerConfigAsync(baseline, 0);
                await SeedServerConfigAsync(at, 4);
                break;
            case Tool.DatabaseConfig:
                await SeedDatabaseConfigAsync(baseline, "FULL");
                await SeedDatabaseConfigAsync(at, "SIMPLE");
                break;
            case Tool.TraceFlags:
                await SeedTraceFlagAsync(baseline, 1117);
                await SeedTraceFlagAsync(at, 1117);
                await SeedTraceFlagAsync(at, 3226);
                break;
            case Tool.CollectionLog:
                await SeedLogRunsAsync("wait_stats", at, at, everyMinutes: 30);
                break;
            case Tool.PlanCorrections:
                await SeedPlanCorrectionAsync(at, recommendation: "PR_1");
                break;
            default:
                await SeedMemoryPressureAsync(sampleTime: at, collectedAt: at);
                break;
        }
    }

    /// <summary>
    /// Two stretches of data: the first at <paramref name="first"/> (the data's start) and a later one a day before the anchor. A config
    /// tool's first stretch is its baseline snapshot, so the later change is the one the answer lists.
    /// </summary>
    private async Task SeedFirstAndLaterAsync(Tool tool, DateTime first)
    {
        if (tool is Tool.ServerConfig or Tool.DatabaseConfig or Tool.TraceFlags)
        {
            await SeedDataAsync(tool, Anchor.AddDays(-1), baselineAt: first);
        }
        else
        {
            await SeedDataAsync(tool, first);
            await SeedDataAsync(tool, Anchor.AddDays(-1));
        }
    }

    /// <summary>The collector's runs, for every tool but the collection log (whose log rows ARE the data).</summary>
    private Task SeedCoverageAsync(Tool tool, DateTime first, DateTime last) =>
        tool == Tool.CollectionLog ? Task.CompletedTask : SeedLogRunsAsync(CollectorOf(tool), first, last, everyMinutes: 30);

    /* ───────────────────────── a data answer ───────────────────────── */

    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    [InlineData(Tool.CollectionLog)]
    [InlineData(Tool.PlanCorrections)]
    [InlineData(Tool.MemoryPressure)]
    public async Task DataAndRunsStartInsideTheWindow_ReportTheFloor_AndTheNote(Tool tool)
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedCoverageAsync(tool, floor, Anchor);
        await SeedFirstAndLaterAsync(tool, floor);

        var root = Root(await CallAsync(tool));

        AssertTruncatedAt(root, floor, TableOf(tool));
    }

    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    [InlineData(Tool.CollectionLog)]
    [InlineData(Tool.PlanCorrections)]
    [InlineData(Tool.MemoryPressure)]
    public async Task AQuietStart_CollectedFromBeforeTheWindow_FirstDataTwoDaysIn_IsCovered(Tool tool)
    {
        await _duckDb.InitializeAsync();
        /* The store holds this tool's data from before the window began, and nothing was stored until two days in: a server that
           is quiet for days looks exactly like this, and a probe that read only the window's rows would call it truncated. A config
           tool's older evidence is its baseline snapshot; the collection log's is an older run. */
        await SeedCoverageAsync(tool, WindowStart.AddHours(-1), Anchor);
        if (tool == Tool.CollectionLog)
        {
            await SeedLogRunsAsync("wait_stats", WindowStart.AddHours(-1), WindowStart.AddHours(-1), everyMinutes: 30);
        }

        await SeedDataAsync(tool, Anchor.AddDays(-2), baselineAt: WindowStart.AddHours(-1));

        var root = Root(await CallAsync(tool));

        AssertCovered(root, WindowStart);
    }

    /// <summary>The three keys sit right after <c>hours_back</c>, in this order, on every tool.</summary>
    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    [InlineData(Tool.CollectionLog)]
    [InlineData(Tool.PlanCorrections)]
    [InlineData(Tool.MemoryPressure)]
    public async Task ADataAnswer_CarriesTheKeysRightAfterHoursBack(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedCoverageAsync(tool, Anchor.AddDays(-2), Anchor);
        await SeedFirstAndLaterAsync(tool, Anchor.AddDays(-2));

        var names = new List<string>();
        foreach (var property in Root(await CallAsync(tool)).EnumerateObject())
        {
            names.Add(property.Name);
        }

        var hoursBack = names.IndexOf("hours_back");
        Assert.True(hoursBack >= 0);
        Assert.Equal(new[] { "effective_start", "window_truncated", "truncation_note" }, names.GetRange(hoursBack + 1, 3));
    }

    /// <summary>A window of 90 minutes or less with rows starts no probe: the answer is covered at the requested start, not at the data.</summary>
    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    [InlineData(Tool.CollectionLog)]
    [InlineData(Tool.PlanCorrections)]
    [InlineData(Tool.MemoryPressure)]
    public async Task AShortWindowWithRows_StartsNoProbe_AndIsCoveredAtTheRequestedStart(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedDataAsync(tool, Anchor.AddMinutes(-30), baselineAt: Anchor.AddMinutes(-50));

        var root = Root(await CallAsync(tool, hoursBack: 1));

        AssertCovered(root, Anchor.AddHours(-1));
    }

    /// <summary>A capped page (<c>limit: 1</c> of two rows) sits next to a window floor: the page cut does not change the notice. These two tools page; the others return the whole window.</summary>
    [Theory]
    [InlineData(Tool.CollectionLog)]
    [InlineData(Tool.PlanCorrections)]
    public async Task ACappedPage_NextToAWindowFloor_LeavesTheNoticeUnchanged(Tool tool)
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedCoverageAsync(tool, floor, Anchor);
        await SeedFirstAndLaterAsync(tool, floor);

        var root = Root(await CallAsync(tool, limit: 1));

        Assert.True(root.GetProperty("truncated").GetBoolean());
        AssertTruncatedAt(root, floor, TableOf(tool));
    }

    /* ───────────────────────── what each probe reads ───────────────────────── */

    /// <summary>
    /// A config read keeps the snapshot BEFORE the window as the diff's baseline and the collectors capture on connect, so a
    /// server that has stayed connected holds its last snapshot days before the window and none inside it. That is a window the
    /// store covers, with no change in it; the plain probe finds nothing inside the window and would say the store holds no
    /// collection of it. The empty answer's hints say covered.
    /// </summary>
    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    public async Task AConfigWindow_HoldsNoSnapshotButOneBeforeIt_IsCoveredAndEmpty(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedDataAsync(tool, WindowStart.AddDays(-9), baselineAt: WindowStart.AddDays(-10));
        await SeedLogRunsAsync("wait_stats", Anchor.AddDays(-3), Anchor, everyMinutes: 60);

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertCovered(root.GetProperty("hints"), WindowStart);
    }

    /// <summary>
    /// The memory pressure list is windowed on <c>sample_time</c>, the ring-buffer event's own time, which can sit before the
    /// first run that stored it; the probe reads that same column beside the collector's runs. So a first-run backfill needs no
    /// event-time form: the collector's first run is 100 minutes into the window (past the 90-minute slack), it stored an event
    /// from 30 minutes in, and the probe alone already names the event and calls the window covered.
    /// </summary>
    [Fact]
    public async Task MemoryPressure_AnEventOlderThanItsRun_FirstRunBackfill_NamesTheEvent_AndIsNotACut()
    {
        await _duckDb.InitializeAsync();
        var firstRun = WindowStart.AddMinutes(100);
        var backfilled = WindowStart.AddMinutes(30);
        await SeedLogRunsAsync("memory_pressure_events", firstRun, Anchor, everyMinutes: 30);
        await SeedMemoryPressureAsync(sampleTime: backfilled, collectedAt: firstRun);

        var root = Root(await CallAsync(Tool.MemoryPressure));

        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - backfilled).TotalSeconds) < 5, $"effective_start {text} should be the event {backfilled:o}");
        Assert.False(root.TryGetProperty("effective_hours_back", out _));
    }

    /// <summary>
    /// A plan-correction answer can hold only the automatic-tuning snapshot (a database the engine has nothing to recommend for):
    /// the recommendation list is empty but the answer is a data answer, so it carries the keys at the top level and is probed
    /// whatever the window's length.
    /// </summary>
    [Fact]
    public async Task PlanCorrections_AnAutomaticTuningOnlyAnswer_CarriesTheKeysAtTheTopLevel_AndIsProbed()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync("plan_correction", Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedPlanCorrectionAsync(Anchor.AddDays(-1), recommendation: null);

        var root = Root(await CallAsync(Tool.PlanCorrections));

        Assert.False(root.TryGetProperty("status", out _));
        Assert.Equal(0, root.GetProperty("recommendations_returned").GetInt32());
        AssertTruncatedAt(root, Anchor.AddDays(-2), "plan_correction");
    }

    /// <summary>A one-hour, tuning-only answer is probed too (short window, empty page): effective_start is the probe's floor, not the asked start.</summary>
    [Fact]
    public async Task PlanCorrections_AOneHourTuningOnlyAnswer_NamesTheProbesFloor_NotTheAskedStart()
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddMinutes(-20);
        await SeedLogRunsAsync("plan_correction", floor, Anchor, everyMinutes: 5);
        await SeedPlanCorrectionAsync(Anchor.AddDays(-1), recommendation: null);

        var root = Root(await CallAsync(Tool.PlanCorrections, hoursBack: 1));

        Assert.False(root.TryGetProperty("status", out _));
        AssertTruncatedAt(root, floor, "plan_correction");
    }

    /// <summary>A status filter that matches nothing, over a window with runs, is a filtered-empty answer, not a quiet window.</summary>
    [Fact]
    public async Task CollectionLog_AStatusFilterThatMatchesNothing_TakesTheFilteredEmptyBranch()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync("wait_stats", Anchor.AddHours(-20), Anchor, everyMinutes: 30);

        var root = Root(await McpHealthTools.GetCollectionLog(
            Service(), _serverManager, ServerName, 24, as_of: AsOf, status: "FAILED"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.DoesNotContain("genuinely quiet", message, StringComparison.Ordinal);
        Assert.Contains("matched", message, StringComparison.Ordinal);
    }

    /// <summary>A snapshot before the window with NO run in the window proves nothing was read: not covered.</summary>
    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    public async Task AConfigWindow_OneSnapshotBeforeIt_ButNoRunInIt_SaysNothingWasRead(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedDataAsync(tool, WindowStart.AddDays(-9), baselineAt: WindowStart.AddDays(-10));
        await SeedLogRunsAsync("wait_stats", WindowStart.AddDays(-9), WindowStart.AddDays(-8), everyMinutes: 60);

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), TableOf(tool));
    }

    /// <summary>The collection log's probe is the log itself, unfiltered: a collector filter that matches nothing still says where the log starts.</summary>
    [Fact]
    public async Task CollectionLog_AFilterThatMatchesNothing_StillSaysWhereTheLogStarts_UnderHints()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync("wait_stats", Anchor.AddDays(-2), Anchor, everyMinutes: 30);

        var root = Root(await McpHealthTools.GetCollectionLog(
            Service(), _serverManager, ServerName, HoursBack, as_of: AsOf, collector_name: "no_such_collector"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertTruncatedAt(root.GetProperty("hints"), Anchor.AddDays(-2), "collection_log");
    }

    /// <summary>A server that has NEVER logged a run is a fault, not an empty window: <c>unavailable</c> keeps its shape and carries no keys.</summary>
    [Fact]
    public async Task CollectionLog_ANeverCollectedServer_IsUnavailable_AndStaysBare()
    {
        await _duckDb.InitializeAsync();

        var root = Root(await CallAsync(Tool.CollectionLog));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.False(root.TryGetProperty("hints", out _));
        Assert.False(root.TryGetProperty("window_truncated", out _));
    }

    /// <summary>The fleet form names no server, so there is no one server's start to give: it carries none of the three keys.</summary>
    [Fact]
    public async Task CollectionLog_TheFleetForm_CarriesNoWindowFloorKeys()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync("wait_stats", Anchor.AddDays(-2), Anchor, everyMinutes: 30);

        var json = await McpHealthTools.GetCollectionLog(Service(), _serverManager, server_name: null, hours_back: HoursBack, as_of: AsOf);

        var root = Root(json);
        Assert.True(root.TryGetProperty("run_count", out _));
        Assert.False(root.TryGetProperty("effective_start", out _));
        Assert.False(root.TryGetProperty("window_truncated", out _));
        Assert.False(root.TryGetProperty("truncation_note", out _));
    }

    /* ───────────────────────── an empty answer says where the data starts too ───────────────────────── */

    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    [InlineData(Tool.PlanCorrections)]
    [InlineData(Tool.MemoryPressure)]
    public async Task AnEmptyWindow_PastCoverage_CarriesTheFloorAndTheNote_UnderHints(Tool tool)
    {
        await _duckDb.InitializeAsync();
        /* The collector ran for two days of a 7-day ask and stored nothing a change or a row would show: only the keys say the
           older five were never read. A config tool holds one snapshot, which is no change. */
        await SeedCoverageAsync(tool, Anchor.AddDays(-2), Anchor);
        if (tool is Tool.ServerConfig)
        {
            await SeedServerConfigAsync(Anchor.AddDays(-2), 0);
        }
        else if (tool is Tool.DatabaseConfig)
        {
            await SeedDatabaseConfigAsync(Anchor.AddDays(-2), "FULL");
        }
        else if (tool is Tool.TraceFlags)
        {
            await SeedTraceFlagAsync(Anchor.AddDays(-2), 1117);
        }

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertTruncatedAt(root.GetProperty("hints"), Anchor.AddDays(-2), TableOf(tool));
    }

    [Theory]
    [InlineData(Tool.ServerConfig)]
    [InlineData(Tool.DatabaseConfig)]
    [InlineData(Tool.TraceFlags)]
    [InlineData(Tool.PlanCorrections)]
    [InlineData(Tool.MemoryPressure)]
    public async Task AnEmptyWindow_TheStoreHoldsNothingIn_SaysNothingWasRead(Tool tool)
    {
        await _duckDb.InitializeAsync();

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), TableOf(tool));
    }

    [Theory]
    [InlineData(Tool.PlanCorrections)]
    [InlineData(Tool.MemoryPressure)]
    public async Task AnEmptyWindow_TheCollectorCoveredFromBeforeIt_SaysCovered(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedCoverageAsync(tool, Anchor.AddDays(-9), Anchor);

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertCovered(root.GetProperty("hints"), WindowStart);
    }

    /// <summary>The collection log's one empty window with older runs: this server HAS collected, but nothing in the window, so the hints say the store holds no collection of the window.</summary>
    [Fact]
    public async Task CollectionLog_AQuietWindowWithOlderRuns_SaysNothingInTheWindowWasRead()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync("wait_stats", Anchor.AddDays(-20), Anchor.AddDays(-19), everyMinutes: 30);

        var root = Root(await CallAsync(Tool.CollectionLog, hoursBack: 24));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"), "collection_log");
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

    /// <summary>None of the six writes a reach: the instant alone, as on the other window-floor payloads.</summary>
    private static void AssertNoReachKey(JsonElement root) =>
        Assert.False(root.TryGetProperty("effective_hours_back", out _));

    /* ───────────────────────── seeding ───────────────────────── */

    private async Task ExecuteAsync(string sql, params object?[] values)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedServerConfigAsync(DateTime captureTime, long value) => ExecuteAsync(@"
INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, 'max degree of parallelism', $5, $5, TRUE, TRUE)",
        _nextId++, Naive(captureTime), _serverId, ServerName, value);

    private Task SeedDatabaseConfigAsync(DateTime captureTime, string recoveryModel) => ExecuteAsync(@"
INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, recovery_model)
VALUES ($1, $2, $3, $4, 'Db', $5)",
        _nextId++, Naive(captureTime), _serverId, ServerName, recoveryModel);

    private Task SeedTraceFlagAsync(DateTime captureTime, int traceFlag) => ExecuteAsync(@"
INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, TRUE, TRUE, FALSE)",
        _nextId++, Naive(captureTime), _serverId, ServerName, traceFlag);

    private Task SeedPlanCorrectionAsync(DateTime collectedAt, string? recommendation) => ExecuteAsync(@"
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, score,
                             force_last_good_plan_desired_state, force_last_good_plan_actual_state)
VALUES ($1, $2, $3, $4, 'Db', $5, 'Active', 50, 'Enabled', 'Enabled')",
        _nextId++, Naive(collectedAt), _serverId, ServerName, recommendation);

    private Task SeedMemoryPressureAsync(DateTime sampleTime, DateTime collectedAt) => ExecuteAsync(@"
INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
VALUES ($1, $2, $3, $4, $5, 'RESOURCE_MEMPHYSICAL_LOW', 1, 1)",
        _nextId++, Naive(collectedAt), _serverId, ServerName, Naive(sampleTime));

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

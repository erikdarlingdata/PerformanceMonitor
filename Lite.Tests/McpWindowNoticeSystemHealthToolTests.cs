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
/// #4966: where the data starts, on the nine <c>get_health_parser_*</c> tools. Each publishes <c>effective_start</c>,
/// <c>window_truncated</c> and <c>truncation_note</c> beside <c>hours_back</c> on a data answer, and the same three keys under
/// <c>hints</c> on an <c>empty</c> one, from the coverage probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) over
/// <c>v_system_health_events</c>. That is the earlier of the first stored event and the collector's first logged run inside
/// the window, both on <c>event_time</c>, the column the nine reads window on. It counts events of every type, because one
/// collector stores them all. None uses the event-time form (<c>McpQueryTools.EventWindowNoticeAsync</c>): the probe already
/// reads <c>event_time</c>, so a first-run backfill is named by the probe itself (see the backfill theory). An <c>unavailable</c>
/// answer keeps its shape. Every window is anchored at a fixed <c>as_of</c>, never the clock. Own
/// <see cref="DuckDbInitializer"/> per test, like <see cref="McpWindowNoticeConfigAndLogToolTests"/>.
/// </summary>
public sealed class McpWindowNoticeSystemHealthToolTests : IDisposable
{
    private const string ServerName = "SystemHealthNoticeServer";
    private const string Table = "system_health_events";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private const int HoursBack = 168;
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = Anchor.AddHours(-HoursBack);

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    /// <summary>The nine tools under test, in the order the source declares them.</summary>
    public enum Tool
    {
        SystemHealth,
        SevereErrors,
        IoIssues,
        SchedulerIssues,
        MemoryConditions,
        CpuTasks,
        MemoryBroker,
        MemoryNodeOom,
        SignificantWaits
    }

    public McpWindowNoticeSystemHealthToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpWindowNoticeSystemHealth_" + Guid.NewGuid().ToString("N")[..8]);
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

    private Task<string> CallAsync(Tool tool, int hoursBack = HoursBack, int limit = 50)
    {
        var service = Service();
        return tool switch
        {
            Tool.SystemHealth => McpHealthParserTools.GetSystemHealth(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.SevereErrors => McpHealthParserTools.GetSevereErrors(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.IoIssues => McpHealthParserTools.GetIOIssues(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.SchedulerIssues => McpHealthParserTools.GetSchedulerIssues(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.MemoryConditions => McpHealthParserTools.GetMemoryConditions(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.CpuTasks => McpHealthParserTools.GetCPUTasks(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.MemoryBroker => McpHealthParserTools.GetMemoryBroker(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.MemoryNodeOom => McpHealthParserTools.GetMemoryNodeOOM(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            Tool.SignificantWaits => McpHealthParserTools.GetSignificantWaits(service, _serverManager, ServerName, hoursBack, limit, AsOf),
            _ => throw new ArgumentOutOfRangeException(nameof(tool))
        };
    }

    /// <summary>The event type the tool reads, and a fixture that passes its significance gate.</summary>
    private static (string EventType, string Fixture, string? Replace) SourceOf(Tool tool) => tool switch
    {
        Tool.SystemHealth => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_system.xml", null),
        Tool.SevereErrors => (SystemHealthParser.ErrorReportedEvent, "error_reported.xml", null),
        Tool.IoIssues => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_io_subsystem.xml", null),
        Tool.SchedulerIssues => (SystemHealthParser.SchedulerMonitorEvent, "scheduler_monitor_high_sql_cpu.xml", null),
        /* The fixtures carry the HIGH notification, which the memory gates drop; LOW is the one they keep. */
        Tool.MemoryConditions => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_resource.xml", "LOW"),
        Tool.CpuTasks => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_query_processing_warning.xml", null),
        Tool.MemoryBroker => (SystemHealthParser.MemoryBrokerEvent, "memory_broker.xml", "LOW"),
        Tool.MemoryNodeOom => (SystemHealthParser.MemoryNodeOomEvent, "memory_node_oom.xml", null),
        Tool.SignificantWaits => (SystemHealthParser.WaitInfoEvent, "wait_info.xml", null),
        _ => throw new ArgumentOutOfRangeException(nameof(tool))
    };

    /// <summary>The key that counts the tool's rows before the <c>limit</c> cut.</summary>
    private static string CountKey(Tool tool) => tool switch
    {
        Tool.SystemHealth => "total_entries",
        Tool.SevereErrors => "error_count",
        Tool.IoIssues or Tool.SchedulerIssues => "issue_count",
        Tool.SignificantWaits => "wait_count",
        _ => "event_count"
    };

    /// <summary>An event of a different type than the tool reads, so the tool's window is empty beside a collector that IS read.</summary>
    private static Tool OtherTypeOf(Tool tool) => tool == Tool.SignificantWaits ? Tool.SevereErrors : Tool.MemoryNodeOom;

    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    private Task SeedEventAsync(Tool tool, DateTime eventTime, DateTime? collectedAt = null)
    {
        var (eventType, fixture, replace) = SourceOf(tool);
        var xml = LoadFixture(fixture);
        if (replace is not null)
        {
            xml = xml.Replace("RESOURCE_MEMPHYSICAL_HIGH", "RESOURCE_MEMPHYSICAL_" + replace, StringComparison.Ordinal);
        }

        return ExecuteAsync(@"
INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            _nextId++, Naive(collectedAt ?? eventTime), _serverId, ServerName, Naive(eventTime), eventType, xml);
    }

    /// <summary>Two stretches of data: the first at <paramref name="first"/> (the data's start) and a later one a day before the anchor.</summary>
    private async Task SeedFirstAndLaterAsync(Tool tool, DateTime first)
    {
        await SeedEventAsync(tool, first);
        await SeedEventAsync(tool, Anchor.AddDays(-1));
    }

    /* ───────────────────────── a data answer ───────────────────────── */

    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task CollectionStartsInsideTheWindow_ReportsTheFloor_AndTheNote(Tool tool)
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedLogRunsAsync(floor, Anchor, everyMinutes: 30);
        await SeedFirstAndLaterAsync(tool, floor);

        var root = Root(await CallAsync(tool));

        AssertTruncatedAt(root, floor);
    }

    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task AQuietStart_CollectedFromBeforeTheWindow_FirstEventTwoDaysIn_IsCovered(Tool tool)
    {
        await _duckDb.InitializeAsync();
        /* The collector has run since before the window and stored nothing until two days in: a quiet server looks exactly like
           this, and a probe that read only the window's events would call it truncated. */
        await SeedLogRunsAsync(WindowStart.AddHours(-1), Anchor, everyMinutes: 30);
        await SeedEventAsync(tool, Anchor.AddDays(-2));

        var root = Root(await CallAsync(tool));

        AssertCovered(root, WindowStart);
    }

    /// <summary>The three keys sit right after <c>hours_back</c>, in this order, on every tool.</summary>
    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task ADataAnswer_CarriesTheKeysRightAfterHoursBack(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync(Anchor.AddDays(-2), Anchor, everyMinutes: 30);
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

    /// <summary>A window of 90 minutes or less with rows starts no probe: covered at the requested start, not at the data.</summary>
    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task AShortWindowWithRows_StartsNoProbe_AndIsCoveredAtTheRequestedStart(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedEventAsync(tool, Anchor.AddMinutes(-30));

        var root = Root(await CallAsync(tool, hoursBack: 1));

        AssertCovered(root, Anchor.AddHours(-1));
    }

    /// <summary>
    /// A capped page (<c>limit: 1</c> over at least two rows) sits next to a window floor: the page cut does not change the notice.
    /// All nine tools page on <c>limit</c>, newest first, and the oldest row is the one the cut drops.
    /// </summary>
    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task ACappedPage_NextToAWindowFloor_LeavesTheNoticeUnchanged(Tool tool)
    {
        await _duckDb.InitializeAsync();
        var floor = Anchor.AddDays(-2);
        await SeedLogRunsAsync(floor, Anchor, everyMinutes: 30);
        await SeedFirstAndLaterAsync(tool, floor);

        var root = Root(await CallAsync(tool, limit: 1));

        Assert.Equal(1, root.GetProperty("shown").GetInt32());
        Assert.True(root.GetProperty(CountKey(tool)).GetInt32() >= 2);
        AssertTruncatedAt(root, floor);
    }

    /// <summary>
    /// Why the event-time form is not used: the collector's first run stores events from before itself (an event is stamped
    /// with its own <c>event_time</c>, the run's <c>collection_time</c> is later), and the probe reads <c>event_time</c>. The first
    /// run is 100 minutes into the window (past the 90-minute slack), it stored an event from 30 minutes in, and the probe alone
    /// already names that event and calls the window covered. This holds for every tool because the nine share one relation.
    /// </summary>
    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task AnEventOlderThanItsRun_FirstRunBackfill_NamesTheEvent_AndIsNotACut(Tool tool)
    {
        await _duckDb.InitializeAsync();
        var firstRun = WindowStart.AddMinutes(100);
        var backfilled = WindowStart.AddMinutes(30);
        await SeedLogRunsAsync(firstRun, Anchor, everyMinutes: 30);
        await SeedEventAsync(tool, backfilled, collectedAt: firstRun);

        var root = Root(await CallAsync(tool));

        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - backfilled).TotalSeconds) < 5, $"effective_start {text} should be the event {backfilled:o}");
        Assert.False(root.TryGetProperty("effective_hours_back", out _));
    }

    /* ───────────────────────── an empty answer says where the data starts too ───────────────────────── */

    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task AnEmptyWindow_PastCoverage_CarriesTheFloorAndTheNote_UnderHints(Tool tool)
    {
        await _duckDb.InitializeAsync();
        /* The collector ran for two days of a 7-day ask and the session stored an event of another type: the session IS read, so
           the answer is a measured empty, and only the keys say the older five days were never read. */
        await SeedLogRunsAsync(Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedEventAsync(OtherTypeOf(tool), Anchor.AddDays(-2));

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.True(root.GetProperty("source_observed").GetBoolean());
        AssertTruncatedAt(root.GetProperty("hints"), Anchor.AddDays(-2));
    }

    /// <summary>Events of the tool's own type were captured and gated out (the first rung): still an empty answer, still the keys.</summary>
    [Fact]
    public async Task AnEmptyWindow_EventsCapturedButGatedOut_CarriesTheKeysUnderHints()
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync(Anchor.AddDays(-2), Anchor, everyMinutes: 30);
        /* A memory-broker event with the HIGH notification, which the gate drops. */
        await ExecuteAsync(@"
INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $2, $5, $6)",
            _nextId++, Naive(Anchor.AddDays(-1)), _serverId, ServerName, SystemHealthParser.MemoryBrokerEvent, LoadFixture("memory_broker.xml"));

        var root = Root(await CallAsync(Tool.MemoryBroker));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(1, root.GetProperty("events_in_window").GetInt32());
        AssertTruncatedAt(root.GetProperty("hints"), Anchor.AddDays(-2));
    }

    /// <summary>The tool's type was captured before the window and not in it (the second rung): the keys say nothing in the window was read.</summary>
    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task AnEmptyWindow_TheStoreHoldsNothingIn_SaysNothingWasRead(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedEventAsync(tool, WindowStart.AddDays(-9));
        await SeedLogRunsAsync(WindowStart.AddDays(-9), WindowStart.AddDays(-8), everyMinutes: 60);

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"));
    }

    /// <summary>An empty answer over a short window is probed too: the skip that is right beside rows is not a claim an empty answer can make.</summary>
    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task AnEmptyShortWindow_IsAlwaysProbed(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedEventAsync(OtherTypeOf(tool), WindowStart.AddDays(-9));

        var root = Root(await CallAsync(tool, hoursBack: 1));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertNothingHeld(root.GetProperty("hints"));
    }

    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SevereErrors)]
    [InlineData(Tool.IoIssues)]
    [InlineData(Tool.SchedulerIssues)]
    [InlineData(Tool.MemoryConditions)]
    [InlineData(Tool.CpuTasks)]
    [InlineData(Tool.MemoryBroker)]
    [InlineData(Tool.MemoryNodeOom)]
    [InlineData(Tool.SignificantWaits)]
    public async Task AnEmptyWindow_TheCollectorCoveredFromBeforeIt_SaysCovered(Tool tool)
    {
        await _duckDb.InitializeAsync();
        await SeedLogRunsAsync(WindowStart.AddDays(-2), Anchor, everyMinutes: 30);
        await SeedEventAsync(OtherTypeOf(tool), WindowStart.AddDays(-1));

        var root = Root(await CallAsync(tool));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        AssertCovered(root.GetProperty("hints"), WindowStart);
    }

    /// <summary>A server whose session has NEVER been read is <c>unavailable</c>: that answer keeps its shape and carries no keys.</summary>
    [Theory]
    [InlineData(Tool.SystemHealth)]
    [InlineData(Tool.SignificantWaits)]
    public async Task ANeverCapturedServer_IsUnavailable_AndStaysBare(Tool tool)
    {
        await _duckDb.InitializeAsync();

        var root = Root(await CallAsync(tool));

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.False(root.GetProperty("source_observed").GetBoolean());
        Assert.False(root.TryGetProperty("hints", out _));
        Assert.False(root.TryGetProperty("window_truncated", out _));
        Assert.False(root.TryGetProperty("effective_start", out _));
    }

    /* ───────────────────────── assertions ───────────────────────── */

    /// <summary>The store's data starts at <paramref name="floor"/>, later than the window asked for.</summary>
    private static void AssertTruncatedAt(JsonElement root, DateTime floor)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        Assert.True(Math.Abs((ParseUtc(text) - floor).TotalSeconds) < 5,
            $"effective_start {text} should be the seeded floor {floor:o}");
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"raw {Table} retains", note, StringComparison.Ordinal);
        AssertNoReachKey(root);
    }

    /// <summary>An empty answer over a window the store holds no collection in: not covered, and no start to name.</summary>
    private static void AssertNothingHeld(JsonElement root)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("effective_start").ValueKind);
        var note = root.GetProperty("truncation_note").GetString();
        Assert.NotNull(note);
        Assert.Contains($"holds no collection of {Table}", note, StringComparison.Ordinal);
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

    /// <summary>None of the nine writes a reach: the instant alone, as on the other window-floor payloads.</summary>
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

    /// <summary>The system_health collector's runs in collection_log, every <paramref name="everyMinutes"/> minutes, whether or not it stored anything.</summary>
    private async Task SeedLogRunsAsync(DateTime firstUtc, DateTime lastUtc, int everyMinutes)
    {
        await ExecuteAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + row_number() OVER (), $2, $3, 'system_health_events', g.t, 12, 'SUCCESS', 0
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL {everyMinutes} MINUTE) AS g(t)",
            _nextId, _serverId, ServerName, Naive(firstUtc), Naive(lastUtc));
        _nextId += 100_000;
    }
}

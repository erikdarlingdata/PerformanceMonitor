/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4966: where the data starts, on the nine <c>get_health_parser_*</c> tools. Each writes <c>effective_start</c>,
/// <c>window_truncated</c> and <c>truncation_note</c> right after <c>hours_back</c> on a data answer, and the same three keys under
/// <c>hints</c> on an <c>empty</c> one. The coverage is the <c>system_health_events</c> table's (events of EVERY type plus the
/// collector's logged runs), never one event type's first hit, and a first collection stores the ring buffer's history, so an event
/// can be older than the coverage: the notice names the earlier of the two. <c>unavailable</c> answers stay bare. Every window is
/// anchored by <c>as_of</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpHealthParserToolsWindowNoticeLiveTests
{
    private const string Collector = "system_health_events";
    private static readonly string[] HealthTables = ["system_health_events"];

    public enum HealthTool { SystemHealth, SevereErrors, IoIssues, SchedulerIssues, MemoryConditions, CpuTasks, MemoryBroker, MemoryNodeOom, SignificantWaits }

    private static string HealthName(string scenario) => "darling-mcp-health-notice-" + scenario;

    private static Task<string> CallHealthAsync(HealthTool tool, NpgsqlDataSource ds, string scenario, int hours, DateTime end, int limit = 50)
    {
        var name = HealthName(scenario);
        var asOf = WebDataStartNote.FormatWindowEnd(end);
        return tool switch
        {
            HealthTool.SystemHealth => DarlingMcpHealthParserTools.GetSystemHealth(ds, name, hours, limit, asOf),
            HealthTool.SevereErrors => DarlingMcpHealthParserTools.GetSevereErrors(ds, name, hours, limit, asOf),
            HealthTool.IoIssues => DarlingMcpHealthParserTools.GetIOIssues(ds, name, hours, limit, asOf),
            HealthTool.SchedulerIssues => DarlingMcpHealthParserTools.GetSchedulerIssues(ds, name, hours, limit, asOf),
            HealthTool.MemoryConditions => DarlingMcpHealthParserTools.GetMemoryConditions(ds, name, hours, limit, asOf),
            HealthTool.CpuTasks => DarlingMcpHealthParserTools.GetCPUTasks(ds, name, hours, limit, asOf),
            HealthTool.MemoryBroker => DarlingMcpHealthParserTools.GetMemoryBroker(ds, name, hours, limit, asOf),
            HealthTool.MemoryNodeOom => DarlingMcpHealthParserTools.GetMemoryNodeOOM(ds, name, hours, limit, asOf),
            _ => DarlingMcpHealthParserTools.GetSignificantWaits(ds, name, hours, limit, asOf),
        };
    }

    /// <summary>The event type a tool reads, a fixture that passes its significance gate, and the edit that makes a memory fixture one the gate keeps.</summary>
    private static (string EventType, string Fixture, bool Low) SourceOf(HealthTool tool) => tool switch
    {
        HealthTool.SystemHealth => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_system.xml", false),
        HealthTool.SevereErrors => (SystemHealthParser.ErrorReportedEvent, "error_reported.xml", false),
        HealthTool.IoIssues => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_io_subsystem.xml", false),
        HealthTool.SchedulerIssues => (SystemHealthParser.SchedulerMonitorEvent, "scheduler_monitor_high_sql_cpu.xml", false),
        HealthTool.MemoryConditions => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_resource.xml", true),
        HealthTool.CpuTasks => (SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_query_processing_warning.xml", false),
        HealthTool.MemoryBroker => (SystemHealthParser.MemoryBrokerEvent, "memory_broker.xml", true),
        HealthTool.MemoryNodeOom => (SystemHealthParser.MemoryNodeOomEvent, "memory_node_oom.xml", false),
        _ => (SystemHealthParser.WaitInfoEvent, "wait_info.xml", false),
    };

    /// <summary>A tool whose event type differs from <paramref name="tool"/>'s, so its window is empty beside a collector that IS read.</summary>
    private static HealthTool OtherTypeOf(HealthTool tool) =>
        tool is HealthTool.SignificantWaits or HealthTool.MemoryNodeOom ? HealthTool.SevereErrors : HealthTool.MemoryNodeOom;

    /// <summary>A gate-passing event of <paramref name="tool"/>'s type at <paramref name="eventTime"/>, stored by a collection at <paramref name="collectedAt"/>.</summary>
    private static Task SeedHealthEventAsync(NpgsqlConnection c, string scenario, HealthTool tool, DateTime eventTime, DateTime? collectedAt = null)
    {
        var (eventType, fixture, low) = SourceOf(tool);
        var xml = System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", fixture));
        if (low)
        {
            xml = xml.Replace("RESOURCE_MEMPHYSICAL_HIGH", "RESOURCE_MEMPHYSICAL_LOW", StringComparison.Ordinal);
        }

        /* The row's event time is the event's own timestamp, as a real capture has it; the read windows on the column. */
        xml = Regex.Replace(xml, "timestamp=\"[^\"]*\"", $"timestamp=\"{eventTime:yyyy-MM-dd'T'HH:mm:ss.fff}Z\"");
        var name = HealthName(scenario);
        return DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectedAt ?? eventTime), ServerIdHelper.GetDeterministicHashCode(name), name,
            DarlingMcpTestData.Naive(eventTime), eventType, xml);
    }

    private static Task RunHealthAsync(string scenario, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionStringForNotices, [Collector], [HealthName(scenario)], HealthTables, body);

    private static string? ConnectionStringForNotices => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task SeedHealthServerAsync(NpgsqlConnection c, string scenario, DateTime created, DateTime? runsFrom, DateTime end) =>
        WindowFloorLiveHarness.SeedServerAsync(c, HealthName(scenario), created, Collector, runsFrom, 30, end, HealthTables, TestContext.Current.CancellationToken);

    private static void AssertTruncatedAt(JsonElement root, DateTime expected, DateTime windowStart)
    {
        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(McpHelpers.FormatEffectiveStart(expected), root.GetProperty("effective_start").GetString());
        Assert.Equal(DarlingMcpWindowNotice.Build(expected, windowStart, Collector).TruncationNote, root.GetProperty("truncation_note").GetString());
    }

    private static void AssertCovered(JsonElement root, DateTime windowStart)
    {
        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var effective = DateTime.Parse(root.GetProperty("effective_start").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
        Assert.InRange((effective - windowStart).TotalSeconds, 0, 120);
    }

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task CollectionStartsInsideTheWindow_ReportsTheFloor_AndTheNote_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("inside-" + tool, async (c, ds, end) =>
        {
            var floor = end.AddDays(-2);
            await SeedHealthServerAsync(c, "inside-" + tool, floor, floor, end);
            await SeedHealthEventAsync(c, "inside-" + tool, tool, floor);
            await SeedHealthEventAsync(c, "inside-" + tool, tool, end.AddDays(-1));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "inside-" + tool, 168, end));

            AssertTruncatedAt(root, floor, end.AddHours(-168));
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task AQuietStart_RunsFromBeforeTheWindow_FirstEventTwoDaysIn_IsCovered_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("quiet-" + tool, async (c, ds, end) =>
        {
            await SeedHealthServerAsync(c, "quiet-" + tool, end.AddDays(-30), end.AddDays(-8), end);
            await SeedHealthEventAsync(c, "quiet-" + tool, tool, end.AddDays(-2));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "quiet-" + tool, 168, end));

            AssertCovered(root, end.AddHours(-168));
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task ABackfilledEvent_OlderThanTheCoverage_NamesTheEventNotTheCoverage_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("backfill-" + tool, async (c, ds, end) =>
        {
            /* Collection began a day ago, and its first run stored the ring buffer's history: an event from three days ago. */
            var began = end.AddDays(-1);
            var backfilled = end.AddDays(-3);
            await SeedHealthServerAsync(c, "backfill-" + tool, began, began, end);
            await SeedHealthEventAsync(c, "backfill-" + tool, tool, backfilled, collectedAt: began);
            await SeedHealthEventAsync(c, "backfill-" + tool, tool, end.AddHours(-6));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "backfill-" + tool, 168, end));

            AssertTruncatedAt(root, backfilled, end.AddHours(-168));
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task ASparseEventType_OnAnOldServer_UsesTheTablesCoverage_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("sparse-" + tool, async (c, ds, end) =>
        {
            /* An event of another type sits inside the window, two days before the first of this type, with no collector run logged:
               the table is covered from before the window whatever this type's first hit says, and a probe filtered to this type
               would not see the other type's event at all. */
            await SeedHealthServerAsync(c, "sparse-" + tool, end.AddDays(-30), null, end);
            await SeedHealthEventAsync(c, "sparse-" + tool, OtherTypeOf(tool), end.AddDays(-3));
            await SeedHealthEventAsync(c, "sparse-" + tool, tool, end.AddDays(-1));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "sparse-" + tool, 168, end));

            AssertCovered(root, end.AddHours(-168));
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task AnEmptyAnswer_PastCoverage_CarriesTheKeysUnderHints_AndStaysTopLevelAround_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("empty-" + tool, async (c, ds, end) =>
        {
            /* The session IS read (an event of another type is stored), the server was added a day ago, and this type never fired. */
            var added = end.AddDays(-1);
            await SeedHealthServerAsync(c, "empty-" + tool, added, added, end);
            await SeedHealthEventAsync(c, "empty-" + tool, OtherTypeOf(tool), end.AddHours(-6));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "empty-" + tool, 168, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.True(root.GetProperty("source_observed").GetBoolean());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), hints.GetProperty("effective_start").GetString());
            Assert.False(root.TryGetProperty("window_truncated", out _));
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task AServerWithNoCollectionAtAll_StaysBare_NoKeysNoHints_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("none-" + tool, async (c, ds, end) =>
        {
            await SeedHealthServerAsync(c, "none-" + tool, end.AddDays(-30), null, end);

            var json = await CallHealthAsync(tool, ds, "none-" + tool, 168, end);

            Assert.DoesNotContain("effective_start", json, StringComparison.Ordinal);
            Assert.DoesNotContain("window_truncated", json, StringComparison.Ordinal);
            Assert.DoesNotContain("\"hints\"", json, StringComparison.Ordinal);
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task ADataAnswer_CarriesTheKeysRightAfterHoursBack_AndALimitedPageKeepsThem_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("order-" + tool, async (c, ds, end) =>
        {
            var floor = end.AddDays(-2);
            await SeedHealthServerAsync(c, "order-" + tool, floor, floor, end);
            await SeedHealthEventAsync(c, "order-" + tool, tool, end.AddDays(-1));
            await SeedHealthEventAsync(c, "order-" + tool, tool, end.AddHours(-5));

            var json = await CallHealthAsync(tool, ds, "order-" + tool, 168, end, limit: 1);
            var root = WindowFloorLiveHarness.Parse(json);

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
            Assert.Equal(1, root.GetProperty("shown").GetInt32());
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.IoIssues)]
    [InlineData(HealthTool.SchedulerIssues)]
    [InlineData(HealthTool.MemoryConditions)]
    [InlineData(HealthTool.CpuTasks)]
    [InlineData(HealthTool.MemoryBroker)]
    [InlineData(HealthTool.MemoryNodeOom)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task AShortWindow_WithRows_StartsNoProbe_AndAFailedProbe_CostsTheNoticeNotTheRows_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("probe-" + tool, async (c, ds, end) =>
        {
            await SeedHealthServerAsync(c, "probe-" + tool, end.AddDays(-30), end.AddDays(-30), end);
            await SeedHealthEventAsync(c, "probe-" + tool, tool, end.AddMinutes(-20));

            var probes = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { probes++; throw new TimeoutException("the probe's deadline passed"); };

            /* One hour, rows: no probe starts, so the throwing stand-in is never reached and the keys are present and covered. */
            var shortRoot = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "probe-" + tool, 1, end));
            Assert.Equal(0, probes);
            Assert.False(shortRoot.GetProperty("window_truncated").GetBoolean());

            /* A week: the probe runs and fails. The rows stay, the three keys go. */
            var failed = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "probe-" + tool, 168, end));
            Assert.Equal(1, probes);
            Assert.False(failed.TryGetProperty("effective_start", out _));
            Assert.False(failed.TryGetProperty("window_truncated", out _));
            Assert.False(failed.TryGetProperty("truncation_note", out _));
            Assert.True(failed.GetProperty("shown").GetInt32() >= 1);
        });

    [Fact]
    public async Task ALimitedPage_StillNamesTheEarliestEvent_OfEveryQualifyingRow_AgainstDevPostgres() =>
        await RunHealthAsync("page-cap", async (c, ds, end) =>
        {
            /* The read has no SQL cap, so the store reaches the backfilled event even though limit 1 returns only the newest row. */
            var began = end.AddDays(-1);
            var backfilled = end.AddDays(-3);
            await SeedHealthServerAsync(c, "page-cap", began, began, end);
            await SeedHealthEventAsync(c, "page-cap", HealthTool.SystemHealth, backfilled, collectedAt: began);
            await SeedHealthEventAsync(c, "page-cap", HealthTool.SystemHealth, end.AddHours(-6));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(HealthTool.SystemHealth, ds, "page-cap", 168, end, limit: 1));

            Assert.Equal(1, root.GetProperty("shown").GetInt32());
            Assert.Equal(2, root.GetProperty("total_entries").GetInt32());
            AssertTruncatedAt(root, backfilled, end.AddHours(-168));
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task NoCoverageInTheWindow_WithEventsLateInIt_NamesTheEarliestEvent_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("nofloor-late-" + tool, async (c, ds, end) =>
        {
            /* The server was registered, and its runs logged, only after as_of: the probe finds no coverage in the window, yet backfilled events sit in it. */
            var late = end.AddHours(-6);
            await WindowFloorLiveHarness.SeedServerAsync(c, HealthName("nofloor-late-" + tool), end.AddHours(1), Collector, end.AddHours(1), 30, end.AddHours(3), HealthTables, TestContext.Current.CancellationToken);
            await SeedHealthEventAsync(c, "nofloor-late-" + tool, tool, late, collectedAt: end.AddHours(1));
            await SeedHealthEventAsync(c, "nofloor-late-" + tool, tool, end.AddHours(-2), collectedAt: end.AddHours(1));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "nofloor-late-" + tool, 168, end));

            AssertTruncatedAt(root, late, end.AddHours(-168));
        });

    [Theory]
    [InlineData(HealthTool.SystemHealth)]
    [InlineData(HealthTool.SevereErrors)]
    [InlineData(HealthTool.SignificantWaits)]
    public async Task NoCoverageInTheWindow_WithAnEventNearTheWindowStart_IsCovered_AgainstDevPostgres(HealthTool tool) =>
        await RunHealthAsync("nofloor-near-" + tool, async (c, ds, end) =>
        {
            /* Same shape, but the earliest event is 30 minutes after the window start: within the 90-minute slack, so no notice. */
            await WindowFloorLiveHarness.SeedServerAsync(c, HealthName("nofloor-near-" + tool), end.AddHours(1), Collector, end.AddHours(1), 30, end.AddHours(3), HealthTables, TestContext.Current.CancellationToken);
            await SeedHealthEventAsync(c, "nofloor-near-" + tool, tool, end.AddHours(-168).AddMinutes(30), collectedAt: end.AddHours(1));
            await SeedHealthEventAsync(c, "nofloor-near-" + tool, tool, end.AddHours(-2), collectedAt: end.AddHours(1));

            var root = WindowFloorLiveHarness.Parse(await CallHealthAsync(tool, ds, "nofloor-near-" + tool, 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        });
}

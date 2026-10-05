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
/// #4966: where the data starts, on <c>get_default_trace_events</c>. The trace stores each event in the monitored server's LOCAL
/// clock; the read converts it to UTC, and the coverage probe reads <c>collection_time</c>, the collector's own UTC clock, so the
/// notice compares UTC with UTC. The first collection of a server stores the trace's history, so an event can be older than the
/// coverage: the notice names the earlier of the two. Every window is anchored by <c>as_of</c>.
/// </summary>
public sealed partial class DarlingMcpDefaultTraceToolsLivePostgresTests
{
    private const string TraceTable = "default_trace_events";
    private const int OffsetMinutes = -300;
    private static readonly string[] TraceTables = ["default_trace_events", "server_properties"];

    private static string TraceName(string scenario) => "darling-mcp-deftrace-notice-" + scenario;

    private static Task<string> CallTraceAsync(NpgsqlDataSource ds, string scenario, int hours, DateTime end, int limit = 100) =>
        DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(ds, TraceName(scenario), hours, limit, WebDataStartNote.FormatWindowEnd(end));

    private static Task RunTraceAsync(string scenario, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG"), [TraceTable], [TraceName(scenario)], TraceTables, body);

    private static async Task SeedTraceServerAsync(NpgsqlConnection c, string scenario, DateTime created, DateTime? runsFrom, DateTime end)
    {
        var ct = TestContext.Current.CancellationToken;
        await WindowFloorLiveHarness.SeedServerAsync(c, TraceName(scenario), created, TraceTable, runsFrom, 30, end, TraceTables, ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, utc_offset_minutes)
VALUES ($1,$2,$3,$4,'Enterprise Edition','16.0.4085.2','RTM',3,$5)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(created.AddDays(-60)), ServerIdHelper.GetDeterministicHashCode(TraceName(scenario)), TraceName(scenario), OffsetMinutes);
    }

    /// <summary>A significant event at the UTC instant <paramref name="eventUtc"/>, stored in the server's local clock, collected at <paramref name="collectedAt"/>.</summary>
    private static Task SeedTraceEventAsync(NpgsqlConnection c, string scenario, DateTime eventUtc, DateTime? collectedAt = null) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO default_trace_events (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, duration_us, integer_data)
VALUES ($1,$2,$3,$4,$5,'Data File Auto Grow',1500000,256)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectedAt ?? eventUtc), ServerIdHelper.GetDeterministicHashCode(TraceName(scenario)), TraceName(scenario),
            DarlingMcpTestData.Naive(eventUtc.AddMinutes(OffsetMinutes)));

    [Fact]
    public async Task CollectionStartsInsideTheWindow_ReportsTheFloor_AndTheNote_AgainstDevPostgres() =>
        await RunTraceAsync("inside", async (c, ds, end) =>
        {
            var floor = end.AddDays(-2);
            await SeedTraceServerAsync(c, "inside", floor, floor, end);
            await SeedTraceEventAsync(c, "inside", end.AddDays(-1));

            var root = WindowFloorLiveHarness.Parse(await CallTraceAsync(ds, "inside", 168, end));

            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(floor), root.GetProperty("effective_start").GetString());
            Assert.Equal(DarlingMcpWindowNotice.Build(floor, end.AddHours(-168), TraceTable).TruncationNote, root.GetProperty("truncation_note").GetString());
            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
        });

    [Fact]
    public async Task AQuietStart_RunsFromBeforeTheWindow_FirstEventLate_IsCovered_AgainstDevPostgres() =>
        await RunTraceAsync("quiet", async (c, ds, end) =>
        {
            await SeedTraceServerAsync(c, "quiet", end.AddDays(-30), end.AddDays(-8), end);
            await SeedTraceEventAsync(c, "quiet", end.AddDays(-2));

            var root = WindowFloorLiveHarness.Parse(await CallTraceAsync(ds, "quiet", 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        });

    [Fact]
    public async Task ABackfilledEvent_OlderThanTheCoverage_NamesTheEventInUtc_NotTheCoverage_AgainstDevPostgres() =>
        await RunTraceAsync("backfill", async (c, ds, end) =>
        {
            /* The server began a day ago; its first run stored the trace's history. The stored event time is local (UTC-5), so a start
               read off the raw column would be five hours off. */
            var began = end.AddDays(-1);
            var backfilled = end.AddDays(-3);
            await SeedTraceServerAsync(c, "backfill", began, began, end);
            await SeedTraceEventAsync(c, "backfill", backfilled, collectedAt: began);
            await SeedTraceEventAsync(c, "backfill", end.AddHours(-6));

            var root = WindowFloorLiveHarness.Parse(await CallTraceAsync(ds, "backfill", 168, end));

            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(backfilled), root.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task AnEmptyAnswer_PastCoverage_CarriesTheKeysUnderHints_AgainstDevPostgres() =>
        await RunTraceAsync("empty", async (c, ds, end) =>
        {
            var added = end.AddDays(-1);
            await SeedTraceServerAsync(c, "empty", added, added, end);

            var root = WindowFloorLiveHarness.Parse(await CallTraceAsync(ds, "empty", 168, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), hints.GetProperty("effective_start").GetString());
            Assert.False(root.TryGetProperty("window_truncated", out _));
        });

    [Fact]
    public async Task AShortWindow_WithRows_StartsNoProbe_AndAFailedProbe_CostsTheNoticeNotTheRows_AgainstDevPostgres() =>
        await RunTraceAsync("probe", async (c, ds, end) =>
        {
            await SeedTraceServerAsync(c, "probe", end.AddDays(-30), end.AddDays(-30), end);
            await SeedTraceEventAsync(c, "probe", end.AddMinutes(-20));

            var probes = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { probes++; throw new TimeoutException("the probe's deadline passed"); };

            var shortRoot = WindowFloorLiveHarness.Parse(await CallTraceAsync(ds, "probe", 1, end));
            Assert.Equal(0, probes);
            Assert.False(shortRoot.GetProperty("window_truncated").GetBoolean());

            var failed = WindowFloorLiveHarness.Parse(await CallTraceAsync(ds, "probe", 168, end));
            Assert.Equal(1, probes);
            Assert.False(failed.TryGetProperty("effective_start", out _));
            Assert.False(failed.TryGetProperty("window_truncated", out _));
            Assert.False(failed.TryGetProperty("truncation_note", out _));
            Assert.True(failed.GetProperty("shown").GetInt32() >= 1);
        });

    [Fact]
    public async Task ALimitedPage_KeepsTheNotice_OfTheWholeWindow_AgainstDevPostgres() =>
        await RunTraceAsync("cap", async (c, ds, end) =>
        {
            var floor = end.AddDays(-2);
            await SeedTraceServerAsync(c, "cap", floor, floor, end);
            await SeedTraceEventAsync(c, "cap", end.AddDays(-1));
            await SeedTraceEventAsync(c, "cap", end.AddHours(-5));

            var root = WindowFloorLiveHarness.Parse(await CallTraceAsync(ds, "cap", 168, end, limit: 1));

            Assert.Equal(1, root.GetProperty("shown").GetInt32());
            Assert.Equal(2, root.GetProperty("total_events").GetInt32());
            Assert.Equal(McpHelpers.FormatEffectiveStart(floor), root.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task AServerWithNoCollectionAtAll_StaysNotCollectedOrBare_AgainstDevPostgres() =>
        await RunTraceAsync("none", async (c, ds, end) =>
        {
            await SeedTraceServerAsync(c, "none", end.AddDays(-30), null, end);
            await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken, "DELETE FROM server_properties WHERE server_id = $1", ServerIdHelper.GetDeterministicHashCode(TraceName("none")));

            var json = await CallTraceAsync(ds, "none", 168, end);

            /* An empty store with no capability verdict is `empty`, which always carries the not-covered notice. */
            var root = WindowFloorLiveHarness.Parse(json);
            if (root.GetProperty("status").GetString() == "empty")
            {
                Assert.True(root.GetProperty("hints").GetProperty("window_truncated").GetBoolean());
            }
            else
            {
                Assert.DoesNotContain("effective_start", json, StringComparison.Ordinal);
            }
        });
}

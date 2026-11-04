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
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7516: <c>get_memory_pressure_events</c> pages its rows. A server under memory pressure writes thousands of samples a day, and
/// the unpaged 24-hour read answered with more than half a million characters. The tool now returns the newest <c>limit</c>
/// events (default 200), oldest first, with a summary of the whole window: the total, the count per notification, and the
/// samples with either indicator at 2 or more. The desktop chart still reads every row through
/// <see cref="LocalDataService.GetMemoryPressureEventsAsync"/>. 205 samples, one a minute, g = 1 the newest; even g is a LOW
/// notification, odd g is STEADY, every fifth has the process indicator at 3 and every seventh the system indicator at 2.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class McpMemoryPressureEventsPagingTests : IDisposable
{
    private const string ServerName = "MemoryPressurePagingServer";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private const int Samples = 205;
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;

    public McpMemoryPressureEventsPagingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "McpMemoryPressurePaging_" + Guid.NewGuid().ToString("N")[..8]);
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
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private static int MediumOrWorse(Func<int, bool> include) =>
        Enumerable.Range(1, Samples).Count(g => include(g) && (g % 5 == 0 || g % 7 == 0));

    private async Task SeedAsync()
    {
        await _duckDb.InitializeFromTemplateAsync();
        await _seed.ExecuteAsync(@"
INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
SELECT $1 + g, $2, $3, $4, $2 - (g * INTERVAL 1 MINUTE),
       CASE WHEN g % 2 = 0 THEN 'RESOURCE_MEMPHYSICAL_LOW' ELSE 'RESOURCE_MEM_STEADY' END,
       CASE WHEN g % 5 = 0 THEN 3 ELSE 0 END,
       CASE WHEN g % 7 = 0 THEN 2 ELSE 1 END
FROM generate_series(1, " + Samples + ") AS s(g)",
            1L, Naive(Anchor), _serverId, ServerName);
        _seed.Flush();
    }

    private Task<string> CallAsync(int? limit = null) =>
        limit is { } l
            ? McpMemoryTools.GetMemoryPressureEvents(new LocalDataService(_duckDb), _serverManager, ServerName, 24, l, AsOf)
            : McpMemoryTools.GetMemoryPressureEvents(new LocalDataService(_duckDb), _serverManager, ServerName, 24, as_of: AsOf);

    private static DateTime ParseUtc(string text) =>
        DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

    [Fact]
    public async Task AWindowOverTheLimit_ReturnsTheNewestRows_TheTrueTotal_AndTheTrueCounts()
    {
        await SeedAsync();

        var root = JsonDocument.Parse(await CallAsync()).RootElement;
        var events = root.GetProperty("events").EnumerateArray().ToList();
        Assert.Equal(200, events.Count);
        Assert.Equal(200, root.GetProperty("limit").GetInt32());
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(200, root.GetProperty("events_returned").GetInt32());
        Assert.Contains("newest 200 of 205", root.GetProperty("limit_note").GetString(), StringComparison.Ordinal);

        var times = events.Select(e => ParseUtc(e.GetProperty("sample_time").GetString()!)).ToList();
        Assert.Equal(times.OrderBy(t => t).ToList(), times);
        Assert.Equal(Anchor.AddMinutes(-200), times[0]);
        Assert.Equal(Anchor.AddMinutes(-1), times[^1]);
        Assert.Equal(McpHelpers.FormatEffectiveStart(times[0]), root.GetProperty("oldest_returned_at").GetString());
        Assert.Equal(McpHelpers.FormatEffectiveStart(times[^1]), root.GetProperty("newest_returned_at").GetString());

        var summary = root.GetProperty("summary");
        Assert.Equal(Samples, summary.GetProperty("total_events").GetInt64());
        Assert.Equal(MediumOrWorse(_ => true), summary.GetProperty("medium_or_worse_events").GetInt64());
        var byNotification = summary.GetProperty("by_notification").EnumerateArray().ToDictionary(n => n.GetProperty("memory_notification").GetString()!);
        Assert.Equal(Enumerable.Range(1, Samples).Count(g => g % 2 == 0), byNotification["RESOURCE_MEMPHYSICAL_LOW"].GetProperty("events").GetInt64());
        Assert.Equal(Enumerable.Range(1, Samples).Count(g => g % 2 != 0), byNotification["RESOURCE_MEM_STEADY"].GetProperty("events").GetInt64());
        Assert.Equal(MediumOrWorse(g => g % 2 == 0), byNotification["RESOURCE_MEMPHYSICAL_LOW"].GetProperty("medium_or_worse_events").GetInt64());
        Assert.Equal(MediumOrWorse(g => g % 2 != 0), byNotification["RESOURCE_MEM_STEADY"].GetProperty("medium_or_worse_events").GetInt64());

        var small = JsonDocument.Parse(await CallAsync(3)).RootElement;
        Assert.Equal(3, small.GetProperty("events").GetArrayLength());
        Assert.True(small.GetProperty("truncated").GetBoolean());
        Assert.Equal(Samples, small.GetProperty("summary").GetProperty("total_events").GetInt64());
        Assert.Equal(Anchor.AddMinutes(-1), ParseUtc(small.GetProperty("events")[2].GetProperty("sample_time").GetString()!));
    }

    [Fact]
    public async Task AWindowUnderTheLimit_ReturnsEveryRow_NotTruncated_WithTheSummary()
    {
        await SeedAsync();

        var root = JsonDocument.Parse(await CallAsync(1000)).RootElement;

        Assert.Equal(Samples, root.GetProperty("events").GetArrayLength());
        Assert.False(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("limit_note").ValueKind);
        Assert.Equal(Samples, root.GetProperty("summary").GetProperty("total_events").GetInt64());
    }

    [Fact]
    public async Task TheChartRead_StillReturnsEveryRow()
    {
        await SeedAsync();

        var rows = await new LocalDataService(_duckDb).GetMemoryPressureEventsAsync(_serverId, 24, asOfUtc: Anchor);

        Assert.Equal(Samples, rows.Count);
    }

    [Fact]
    public async Task ALimitOutsideTheRange_IsRefusedByName()
    {
        await _duckDb.InitializeFromTemplateAsync();

        Assert.Contains("limit", await CallAsync(0), StringComparison.Ordinal);
        Assert.Contains("exceeds maximum", await CallAsync(1001), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABadLimit_IsRefusedBeforeTheServerIsResolved_AsOnDarling()
    {
        await _duckDb.InitializeFromTemplateAsync();

        /* Darling checks limit first, so a bad server name with a bad limit gets the limit refusal on both products. */
        var reply = await McpMemoryTools.GetMemoryPressureEvents(new LocalDataService(_duckDb), _serverManager, "NoSuchServer", 24, 0, AsOf);

        /* The reply is JSON, which escapes the quotes around the value: read the message, not the raw text. */
        var root = JsonDocument.Parse(reply).RootElement;
        Assert.Equal("invalid", root.GetProperty("status").GetString());
        Assert.Contains("Invalid limit value '0'", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }
}

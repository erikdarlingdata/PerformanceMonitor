/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
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
/// Lite's get_query_heatmap (#2484), the twin of Darling's.
///
/// <para>Two things are pinned here that no reader-level test would see. The three empty branches — the third
/// of which is unique to this read: collection ran, the window has captures, and every one recorded zero
/// executions, which is an IDLE server rather than a broken one. And the BUCKETING, which is the whole reason
/// the read exists in this shape: the desktop viewer hardcodes 5-minute bins, so an agent and a desktop
/// looking at the same server over the same window have to land on the same grid.</para>
/// </summary>
public sealed class QueryHeatmapToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "HeatmapSrv";
    private const string Db = "AppDb";

    /* Lite DERIVES its server id from the storage name; a hardcoded one would seed rows the tool looks
       straight past and pass the never-collected assertion for the wrong reason. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryHeatmapToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-heatmap-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task ThreeKindsOfNothing_AreThreeDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);

        /* 1. nothing ever collected. */
        var never = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24));
        Assert.Equal("unavailable", never.GetProperty("status").GetString());
        var neverText = never.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.Contains("PERIODIC table rather than an edge table", neverText, StringComparison.Ordinal);

        /*
            2. rows exist, but not in the window. Seeded FIRST, and the ordering is load-bearing: once a row
            exists 30 minutes ago no window a caller can legally ask for excludes it, so this branch is
            unreachable afterwards. The states are walked by moving hours_back over one growing set of rows.
        */
        await SeedAsync(baseNow.AddHours(-40), "0xOLD", deltaExec: 4, deltaElapsed: 200_000);

        var noWindow = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 2));
        Assert.Equal("empty", noWindow.GetProperty("status").GetString());
        var noWindowText = noWindow.GetProperty("message").GetString()!;
        Assert.Contains("Widen hours_back", noWindowText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", noWindowText, StringComparison.Ordinal);

        /*
            3. collected in the window, and every capture recorded ZERO executions. The branch only this read
            has: collection is healthy and the server is idle, so telling the caller to widen the window would
            point them at the wrong problem.
        */
        await SeedAsync(baseNow.AddMinutes(-30), "0xIDLE", deltaExec: 0, deltaElapsed: 999_000);

        var idle = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24));
        Assert.Equal("empty", idle.GetProperty("status").GetString());
        var idleText = idle.GetProperty("message").GetString()!;
        Assert.Contains("zero execution delta", idleText, StringComparison.Ordinal);
        Assert.DoesNotContain("Widen hours_back", idleText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", idleText, StringComparison.Ordinal);

        /* Three sentences, not one sentence three times. */
        var messages = new[] { neverText, noWindowText, idleText };
        Assert.Equal(3, messages.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// The grid itself, on the viewer's own 5-minute bins.
    /// <para>The seed is floored to the hour, and that is not tidiness: time_bucket aligns bins to the origin,
    /// so an unfloored "three hours ago" lands at an arbitrary minute and the three rows below straddle a
    /// 5-minute boundary roughly three times in five. It would fail on a clock, not on a defect.</para>
    /// </summary>
    [Fact]
    public async Task TheGrid_BinsAtFiveMinutes_BucketsByMagnitude_AndNamesTheHottestQuery()
    {
        var service = new LocalDataService(_duckDb);
        var t1 = FloorToHour(Truncate(DateTime.UtcNow).AddHours(-3));

        /* Two queries at ~50 ms/exec share bucket 2 of one bin; 0.5 ms/exec lands in bucket 0 of the SAME
           bin; a fourth 35 minutes later gets its own bin. The zero-execution row contributes to nothing. */
        await SeedAsync(t1, "0xHOT", deltaExec: 5, deltaElapsed: 250_000);
        await SeedAsync(t1.AddMinutes(1), "0xCOLD", deltaExec: 1, deltaElapsed: 50_000);
        await SeedAsync(t1.AddMinutes(2), "0xLOW", deltaExec: 2, deltaElapsed: 1_000);
        await SeedAsync(t1.AddMinutes(35), "0xNEXT", deltaExec: 3, deltaElapsed: 90_000);
        await SeedAsync(t1.AddMinutes(3), "0xZERO", deltaExec: 0, deltaElapsed: 999_000);

        var grid = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24));

        Assert.Equal(ServerName, grid.GetProperty("server").GetString());
        Assert.Equal("duration", grid.GetProperty("metric").GetString());
        Assert.Equal(5, grid.GetProperty("bucket_minutes").GetInt32());
        Assert.True(grid.GetProperty("bucket_minutes_matches_desktop_viewer").GetBoolean());
        Assert.Equal(2, grid.GetProperty("time_bin_count").GetInt32());
        Assert.Equal(3, grid.GetProperty("cell_count").GetInt32());
        Assert.False(grid.GetProperty("truncated").GetBoolean());
        Assert.Equal(7, grid.GetProperty("magnitude_buckets").GetArrayLength());

        var cells = grid.GetProperty("cells").EnumerateArray().ToArray();

        /* Chronological, whatever order the cap fetched them in. */
        var times = cells.Select(c => DateTime.Parse(c.GetProperty("time_bucket").GetString()!,
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind)).ToArray();
        Assert.Equal(times.OrderBy(t => t).ToArray(), times);

        /*
            The reported window has to be the window the read USED. It spans exactly hours_back and brackets
            every bin that came back. Taken after the read returns it drifts by the read's own duration —
            on the one read whose entire output is a time axis, so a window that disagrees with the bins
            under it is worse here than almost anywhere (review catch on the first cut of this file).
        */
        var windowStart = ParseUtc(grid.GetProperty("window_start").GetString()!);
        var windowEnd = ParseUtc(grid.GetProperty("window_end").GetString()!);
        Assert.Equal(24.0, (windowEnd - windowStart).TotalHours, 3);
        Assert.True(windowStart <= times[0], "window_start is after the first bin the read returned");
        Assert.True(windowEnd >= times[^1], "window_end is before the last bin the read returned");

        var bucket0 = cells.Single(c => c.GetProperty("bucket_index").GetInt32() == 0);
        Assert.Equal(1, bucket0.GetProperty("query_count").GetInt64());
        Assert.Equal("0xLOW", bucket0.GetProperty("top_query_hash").GetString());
        Assert.Equal("0-1ms", bucket0.GetProperty("bucket_label").GetString());

        /* Two DISTINCT queries in the cell, and the cell's top query is the most-EXECUTED rather than the
           slowest — 0xHOT ran five times, 0xCOLD once, and they are the same speed. */
        var bucket2 = cells.First(c => c.GetProperty("bucket_index").GetInt32() == 2);
        Assert.Equal(2, bucket2.GetProperty("query_count").GetInt64());
        Assert.Equal("0xHOT", bucket2.GetProperty("top_query_hash").GetString());
        Assert.Equal("10-100ms", bucket2.GetProperty("bucket_label").GetString());

        /* A coarser bin collapses the two columns into one — the parameter does something. */
        var coarse = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24, null, null, 60));
        Assert.Equal(60, coarse.GetProperty("bucket_minutes").GetInt32());
        Assert.False(coarse.GetProperty("bucket_minutes_matches_desktop_viewer").GetBoolean());
        Assert.Equal(1, coarse.GetProperty("time_bin_count").GetInt32());
        Assert.Equal(2, coarse.GetProperty("cell_count").GetInt32());

        /* Another metric is a different grid, not the same one relabelled. */
        var execCount = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24, "execution_count"));
        Assert.Equal("execution_count", execCount.GetProperty("metric").GetString());
        Assert.Equal("0-1", execCount.GetProperty("magnitude_buckets")[0].GetProperty("label").GetString());
        Assert.Contains("a total, not a per-execution average", execCount.GetProperty("metric_unit").GetString()!, StringComparison.Ordinal);
    }

    /// <summary>
    /// Truncation keeps the RECENT end of the window and hands back no partial column.
    /// <para>A cap of 2 against a 3-cell grid whose oldest bin holds 2 of those cells: the newest bin's one
    /// cell fits, the oldest bin does not fit whole, so it is dropped rather than shown with a hole. A column
    /// missing its low buckets reads as "nothing fast ran then" rather than "we stopped looking".</para>
    /// </summary>
    [Fact]
    public async Task ACappedGrid_KeepsTheRecentEnd_AndNoPartialColumn()
    {
        var service = new LocalDataService(_duckDb);
        var t1 = FloorToHour(Truncate(DateTime.UtcNow).AddHours(-3));

        await SeedAsync(t1, "0xHOT", deltaExec: 5, deltaElapsed: 250_000);
        await SeedAsync(t1.AddMinutes(2), "0xLOW", deltaExec: 2, deltaElapsed: 1_000);
        await SeedAsync(t1.AddMinutes(35), "0xNEXT", deltaExec: 3, deltaElapsed: 90_000);

        var capped = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24, null, null, 5, 2));

        Assert.True(capped.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, capped.GetProperty("time_bin_count").GetInt32());
        Assert.Equal(1, capped.GetProperty("cell_count").GetInt32());
        Assert.Equal("0xNEXT", capped.GetProperty("cells")[0].GetProperty("top_query_hash").GetString());

        /* first/last say which slice came back, so the missing part of the window is visible rather than
           inferred. */
        Assert.Equal(
            capped.GetProperty("first_time_bin").GetString(),
            capped.GetProperty("last_time_bin").GetString());

        /* An uncapped call over the same rows is the whole grid, so the cap above really was the cause. */
        var whole = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24));
        Assert.False(whole.GetProperty("truncated").GetBoolean());
        Assert.Equal(3, whole.GetProperty("cell_count").GetInt32());
    }

    /// <summary>
    /// The anchor moves the window, and the resolved instant reaches the QUERY.
    /// <para>#2495's own failure mode is a tool that takes <c>as_of</c>, validates it, refuses a bad one
    /// correctly, and then queries NOW — the validation succeeding is what makes the caller believe the
    /// window moved, and nothing in the result says otherwise. So this proves it by CONTENT: an incident 30
    /// hours old is outside every default window on the surface, and only the anchored call can see it.</para>
    /// </summary>
    [Fact]
    public async Task TheAnchor_MovesTheWindow_AndTheDefaultAnchorCannotSeeAPastIncident()
    {
        var service = new LocalDataService(_duckDb);
        var incident = FloorToHour(Truncate(DateTime.UtcNow).AddHours(-30));
        await SeedAsync(incident, "0xINCIDENT", deltaExec: 5, deltaElapsed: 250_000);

        var anchor = DateTime.SpecifyKind(incident.AddMinutes(30), DateTimeKind.Utc).ToString("o");
        var anchored = Root(await McpQueryTools.GetQueryHeatmap(
            service, _serverManager, ServerName, 1, null, null, 5, 500, anchor));

        Assert.Equal(1, anchored.GetProperty("cell_count").GetInt32());
        Assert.Equal("0xINCIDENT", anchored.GetProperty("cells")[0].GetProperty("top_query_hash").GetString());

        /* The reported window is the ANCHORED one. Taken from a fresh clock it would say "now" over bins
           that are 30 hours old. */
        Assert.Equal(anchor, anchored.GetProperty("window_end").GetString());
        Assert.Equal(
            ParseUtc(anchored.GetProperty("cells")[0].GetProperty("time_bucket").GetString()!),
            incident);

        /* The same LENGTH of window at the default anchor cannot reach it — so it is the anchor doing the
           work, not hours_back. */
        var unanchored = Root(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 1));
        Assert.Equal("empty", unanchored.GetProperty("status").GetString());
        Assert.Contains("Widen hours_back", unanchored.GetProperty("message").GetString()!, StringComparison.Ordinal);

        /* An anchor we cannot use is refused, never silently treated as now. */
        var badAnchor = await McpQueryTools.GetQueryHeatmap(
            service, _serverManager, ServerName, 1, null, null, 5, 500, "last tuesday");
        Assert.Contains("Invalid as_of", badAnchor, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OutOfRangeKnobs_AreRefused_NotSilentlyClamped()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(Truncate(DateTime.UtcNow).AddMinutes(-30), "0xANY", deltaExec: 3, deltaElapsed: 60_000);

        var tooManyCells = await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24, null, null, 5, 5000);
        Assert.Contains("exceeds maximum of", tooManyCells, StringComparison.Ordinal);
        Assert.Contains("1000", tooManyCells, StringComparison.Ordinal);

        var zeroBucket = await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24, null, null, 0);
        Assert.Contains("Must be between 1 and 1440", zeroBucket, StringComparison.Ordinal);

        var hugeBucket = await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24, null, null, 1441);
        Assert.Contains("Must be between 1 and 1440", hugeBucket, StringComparison.Ordinal);

        /* An unknown metric is REFUSED rather than turned into duration: a caller who asked for CPU and
           silently got elapsed time would read the wrong grid with nothing to tell them so. */
        var badMetric = await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 24, "reads");
        Assert.Contains("Invalid metric 'reads'", badMetric, StringComparison.Ordinal);
        Assert.Contains("logical_reads", badMetric, StringComparison.Ordinal);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime ParseUtc(string value) => DateTime.Parse(
        value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    /// <summary>Floors to the top of the hour, which is also a 5-minute boundary on the epoch grid the read
    /// bins against, so the seeded rows land where the test says they do whatever time CI runs at.</summary>
    private static DateTime FloorToHour(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour), value.Kind);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedAsync(DateTime collectionTime, string queryHash, long deltaExec, long deltaElapsed)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        var naive = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xSQLH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaExec });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaElapsed });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM Orders" });
        await cmd.ExecuteNonQueryAsync();
    }
}

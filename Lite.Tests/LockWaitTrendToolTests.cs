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
/// Lite's get_lock_wait_trend (#2484), the twin of Darling's. The viewer's Blocking-Trends lock-wait lane
/// had no read on either SKU: get_wait_trend charts ONE named wait type, and this is the whole LCK family
/// at once as a per-second rate.
///
/// <para>Since #3897 the family is the SERIES — every LCK type's wait summed per collection, bucketed — and the
/// types are a LEGEND of the ones that waited, with the idle ones counted. It was a row per (collection, type),
/// which grew with every lock type a server had ever waited on and was 96% zeros on DARLING01.</para>
///
/// <para>The properties that carry the weight, and none of them is "the SQL runs". The empty branch must NOT
/// be filtered the way the read is — a server collected for months that never took a lock wait is the
/// all-clear this branch exists to give, and an LCK-filtered probe would call it uncollected. A window of
/// collected, idle lock types is DATA — a measured zero — not an empty answer. The rate must survive being
/// fractional, because #2507 shipped an integer rate and a server at 0.4 a second reported zero. The family
/// has ONE interval per collection, so summing its types cannot divide it by their number. And the anchor
/// must reach the query, proven by CONTENT rather than by signature.</para>
/// </summary>
public sealed class LockWaitTrendToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "LockWaitSrv";

    /* Lite DERIVES its server id from the storage name; a hardcoded one would seed rows the tool looks
       straight past and pass the never-collected assertion for the wrong reason. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public LockWaitTrendToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-lockwait-" + Guid.NewGuid().ToString("N"));
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
    public async Task NoLockWaits_MeansQuietOnlyWhenWaitStatsWereCollected()
    {
        var service = new LocalDataService(_duckDb);

        /* 1. nothing sampled at all: NOT "this server has no lock contention". */
        var never = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 4));
        Assert.Equal("unavailable", never.GetProperty("status").GetString());
        var neverText = never.GetProperty("message").GetString()!;
        Assert.Contains("NOT a report of a server without lock contention", neverText, StringComparison.Ordinal);
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);

        /*
            2. wait stats collected, and not one LOCK wait among them. The single most important assertion
            here: this server is healthy and monitored, and the honest answer is a genuine all-clear. An
            existence probe carrying the read's own LIKE 'LCK%' filter would find nothing and report this
            server as uncollected, sending someone to fix collection that is working.
        */
        await SeedWaitAsync(Truncate(DateTime.UtcNow).AddMinutes(-30), "CXPACKET", 999_999);

        var noLocks = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 4));
        Assert.Equal("empty", noLocks.GetProperty("status").GetString());
        var noLocksText = noLocks.GetProperty("message").GetString()!;
        Assert.Contains("genuinely quiet rather than broken", noLocksText, StringComparison.Ordinal);

        /* Same zero rows as the branch above, and it must NOT reach for the same word. */
        Assert.DoesNotContain("EVER", noLocksText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheFamilyRate_IsPerSecond_Fractional_AndDropsCounterResets()
    {
        var service = new LocalDataService(_duckDb);
        var first = Truncate(DateTime.UtcNow).AddMinutes(-30);
        var second = first.AddSeconds(60);

        await SeedWaitAsync(first, "LCK_M_X", 1_200);
        await SeedWaitAsync(second, "LCK_M_X", 6_000);

        /* Three milliseconds over sixty seconds is 0.05 ms/sec — a real rate that integer division would
           report as zero, which is how a quiet server reads as an idle one. */
        await SeedWaitAsync(first, "LCK_M_S", 10);
        await SeedWaitAsync(second, "LCK_M_S", 3);

        /* A negative delta is the counter reset across a SQL Server restart, not a negative wait. */
        await SeedWaitAsync(second, "LCK_M_U", -500);

        /* Filtered out by the read even though it is the largest delta in the window. */
        await SeedWaitAsync(second, "CXPACKET", 999_999);

        var root = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 4));
        Assert.Equal(ServerName, root.GetProperty("server").GetString());

        /* The family: the FIRST collection of each type has no prior sample to difference against and (a pre-v60
           row) no stored interval, so it is left out — it used to be charted as 0.00, a fabricated idle point
           (#3540) — and the one rated collection is the second, where the family waited 6,000 + 3 ms over its ONE
           60-second interval: 100.05 ms/sec. CXPACKET is filtered by the read, and the reset row is dropped rather
           than charted as a negative wait. */
        var point = Assert.Single(root.GetProperty("trend").EnumerateArray().ToArray());
        Assert.Equal(100.05, point.GetProperty("wait_time_ms_per_second").GetDouble(), 3);
        Assert.Equal(100.05, point.GetProperty("peak_wait_time_ms_per_second").GetDouble(), 3);
        Assert.Equal("2 minutes", root.GetProperty("bucket").GetString());

        /* The legend names the types that waited, heaviest first, each with its own rate. The LAG is per wait
           type, which is what stops one type's cadence describing another. */
        var types = root.GetProperty("wait_types").EnumerateArray().ToArray();
        Assert.Equal(new[] { "LCK_M_X", "LCK_M_S" }, types.Select(t => t.GetProperty("wait_type").GetString()).ToArray());
        Assert.Equal(100d, types[0].GetProperty("wait_time_ms_per_second").GetDouble(), 4);
        Assert.DoesNotContain(types, t => t.GetProperty("wait_type").GetString() is "CXPACKET" or "LCK_M_U");
        Assert.Equal(0, root.GetProperty("wait_types_idle").GetInt32());

        /* The fractional rate. Asserted as > 0 as well as by value, because "0.05" and "0" differ by a cast
           and the point of the assertion is that the cast is there. */
        var tinyRate = types[1].GetProperty("wait_time_ms_per_second").GetDouble();
        Assert.True(tinyRate > 0, $"a 3 ms delta over 60 s must not truncate to zero, got {tinyRate}");
        Assert.Equal(0.05d, tinyRate, 4);
    }

    /// <summary>
    /// #3897: the family's denominator is each collection's ONE interval, and a window whose LCK types were
    /// collected and never waited is a measured zero, not an empty answer. Fixed instants and stored intervals
    /// (the v60 shape): two types in collection c1, one in c2, a third type idle throughout, all in one 60-minute
    /// bucket — Darling's DarlingLockWaitTrendTests plants the same rows and asserts the same figures.
    /// </summary>
    [Fact]
    public async Task TheFamily_SumsTypesOverOneIntervalPerCollection_AndAnIdleWindowIsAMeasuredZero()
    {
        var service = new LocalDataService(_duckDb);
        var c1 = new DateTime(2026, 3, 4, 10, 10, 0, DateTimeKind.Unspecified);
        var c2 = c1.AddMinutes(1);
        var c3 = c1.AddMinutes(2);

        /* c1: X 1,200 ms and S 600 ms over one 60 s sweep → 30 ms/s; c2: X 3,000 ms alone over a 120 s sweep →
           25 ms/s; c3: nothing waits → 0. IX is collected and idle throughout. */
        await SeedWaitAsync(c1, "LCK_M_X", 1_200, interval: 60);
        await SeedWaitAsync(c1, "LCK_M_S", 600, interval: 60);
        await SeedWaitAsync(c1, "LCK_M_IX", 0, interval: 60);
        await SeedWaitAsync(c2, "LCK_M_X", 3_000, interval: 120);
        await SeedWaitAsync(c2, "LCK_M_IX", 0, interval: 120);
        await SeedWaitAsync(c3, "LCK_M_X", 0, interval: 60);
        await SeedWaitAsync(c3, "LCK_M_IX", 0, interval: 60);

        var root = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 2, "2026-03-04T11:00:00Z", bucket_minutes: 60));

        /* One bucket: (1,200 + 600 + 3,000 + 0) ms over (60 + 120 + 60) s = 20 ms/s. Summing the types'
           INTERVALS would have divided c1 by 120; averaging the per-collection rates would have read (30 + 25 + 0)
           / 3. The peak is c1's family rate. */
        var point = Assert.Single(root.GetProperty("trend").EnumerateArray().ToArray());
        Assert.Equal(20d, point.GetProperty("wait_time_ms_per_second").GetDouble(), 6);
        Assert.Equal(30d, point.GetProperty("peak_wait_time_ms_per_second").GetDouble(), 6);
        Assert.StartsWith("2026-03-04T10:00:00", point.GetProperty("collection_time").GetString()!, StringComparison.Ordinal);
        Assert.Equal(1, root.GetProperty("wait_types_idle").GetInt32());
        Assert.Equal(new[] { "LCK_M_X", "LCK_M_S" }, root.GetProperty("wait_types").EnumerateArray().Select(t => t.GetProperty("wait_type").GetString()).ToArray());
        Assert.EndsWith("The width is the bucket_minutes you passed.", root.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);

        /* An hour later, every type collected and idle: data, not "widen hours_back". */
        var d1 = new DateTime(2026, 3, 4, 12, 10, 0, DateTimeKind.Unspecified);
        await SeedWaitAsync(d1, "LCK_M_X", 0, interval: 60);
        await SeedWaitAsync(d1.AddMinutes(1), "LCK_M_X", 0, interval: 60);
        await SeedWaitAsync(d1, "LCK_M_S", 0, interval: 60);

        var idle = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 1, "2026-03-04T13:00:00Z"));
        Assert.False(idle.TryGetProperty("status", out _), "collected-but-idle lock types are an answer, not an empty status");
        Assert.All(idle.GetProperty("trend").EnumerateArray(), p => Assert.Equal(0d, p.GetProperty("wait_time_ms_per_second").GetDouble()));
        Assert.Empty(idle.GetProperty("wait_types").EnumerateArray());
        Assert.Equal(2, idle.GetProperty("wait_types_idle").GetInt32());
        Assert.Contains("measured zero", idle.GetProperty("wait_types_note").GetString()!, StringComparison.Ordinal);
    }

    /// <summary>
    /// The anchor moves the window and the resolved instant reaches the QUERY.
    /// <para>#2495's own failure mode is a tool that takes <c>as_of</c>, validates it, refuses a bad one
    /// correctly, and then queries NOW. So this proves it by CONTENT: lock waits 30 hours old are outside
    /// every default window on the surface, and only the anchored call can see them.</para>
    /// </summary>
    [Fact]
    public async Task TheAnchor_MovesTheWindow_AndTheDefaultAnchorCannotSeeAPastIncident()
    {
        var service = new LocalDataService(_duckDb);
        var incident = Truncate(DateTime.UtcNow).AddHours(-30);

        await SeedWaitAsync(incident, "LCK_M_IX", 600);
        await SeedWaitAsync(incident.AddSeconds(60), "LCK_M_IX", 1_800);

        var anchor = DateTime.SpecifyKind(incident.AddSeconds(60), DateTimeKind.Utc).ToString("o");
        var anchored = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 1, anchor));

        /* One point: the first anchored collection has no prior and is not rated (#3540). */
        var point = Assert.Single(anchored.GetProperty("trend").EnumerateArray().ToArray());
        Assert.Equal(30d, point.GetProperty("wait_time_ms_per_second").GetDouble(), 3);
        Assert.Equal("LCK_M_IX", Assert.Single(anchored.GetProperty("wait_types").EnumerateArray().ToArray()).GetProperty("wait_type").GetString());

        /* The same LENGTH of window at the default anchor cannot reach it — so it is the anchor doing the
           work, not hours_back. */
        var unanchored = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 1));
        Assert.Equal("empty", unanchored.GetProperty("status").GetString());

        /* An anchor we cannot use is refused, never silently treated as now. */
        var bad = await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 1, "last tuesday");
        Assert.Contains("Invalid as_of", bad, StringComparison.Ordinal);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /// <summary>A wait_stats row; <paramref name="interval"/> null is the pre-v60 shape the read LAG-rates.</summary>
    private async Task SeedWaitAsync(DateTime collectionTime, string waitType, long deltaMs, int? interval = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 1, $6, 0, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)interval ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}

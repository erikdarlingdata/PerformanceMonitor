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
/// <para>Three properties carry the weight, and none of them is "the SQL runs". The empty branch must NOT
/// be filtered the way the read is — a server collected for months that never took a lock wait is the
/// all-clear this branch exists to give, and an LCK-filtered probe would call it uncollected. The rate must
/// survive being fractional, because #2507 shipped an integer rate and a server at 0.4 a second reported
/// zero. And the anchor must reach the query, proven by CONTENT rather than by signature.</para>
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
    public async Task TheRate_IsPerSecond_Fractional_AndDropsCounterResets()
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

        var trend = root.GetProperty("trend").EnumerateArray().ToArray();
        Assert.DoesNotContain(trend, r => r.GetProperty("wait_type").GetString() == "CXPACKET");
        Assert.DoesNotContain(trend, r => r.GetProperty("wait_type").GetString() == "LCK_M_U");

        /* Four rows: two wait types x two collections. The FIRST collection of each type has no prior
           sample to difference against, so its interval is NULL and its rate is 0 rather than the raw
           delta — the LAG is per wait type, which is what stops one type's cadence describing another. */
        Assert.Equal(4, trend.Length);

        Assert.Equal(100d, RateOf(trend, "LCK_M_X", second), 3);
        Assert.Equal(0d, RateOf(trend, "LCK_M_X", first), 3);

        /* The fractional rate. Asserted as > 0 as well as by value, because "0.05" and "0" differ by a cast
           and the point of the assertion is that the cast is there. */
        var tinyRate = RateOf(trend, "LCK_M_S", second);
        Assert.True(tinyRate > 0, $"a 3 ms delta over 60 s must not truncate to zero, got {tinyRate}");
        Assert.Equal(0.05d, tinyRate, 3);
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
        var rows = anchored.GetProperty("trend").EnumerateArray().ToArray();

        Assert.Equal(2, rows.Length);
        Assert.All(rows, r => Assert.Equal("LCK_M_IX", r.GetProperty("wait_type").GetString()));
        Assert.Equal(30d, RateOf(rows, "LCK_M_IX", incident.AddSeconds(60)), 3);

        /* The same LENGTH of window at the default anchor cannot reach it — so it is the anchor doing the
           work, not hours_back. */
        var unanchored = Root(await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 1));
        Assert.Equal("empty", unanchored.GetProperty("status").GetString());

        /* An anchor we cannot use is refused, never silently treated as now. */
        var bad = await McpBlockingTools.GetLockWaitTrend(service, _serverManager, ServerName, 1, "last tuesday");
        Assert.Contains("Invalid as_of", bad, StringComparison.Ordinal);
    }

    /// <summary>The rate the read returned for one (wait type, collection) pair, asserted to exist.</summary>
    private static double RateOf(JsonElement[] rows, string waitType, DateTime collectionTimeUtc)
    {
        var stamp = collectionTimeUtc.ToString("yyyy-MM-ddTHH:mm:ss");
        var row = rows.FirstOrDefault(r =>
            r.GetProperty("wait_type").GetString() == waitType &&
            r.GetProperty("collection_time").GetString()!.StartsWith(stamp, StringComparison.Ordinal));

        Assert.True(
            row.ValueKind == JsonValueKind.Object,
            $"no {waitType} row at {stamp} — the read returned [{string.Join(", ", rows.Select(r => r.GetProperty("wait_type").GetString() + "@" + r.GetProperty("collection_time").GetString()))}]");

        return row.GetProperty("wait_time_ms_per_second").GetDouble();
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

    private async Task SeedWaitAsync(DateTime collectionTime, string waitType, long deltaMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 1, $6, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaMs });
        await cmd.ExecuteNonQueryAsync();
    }
}

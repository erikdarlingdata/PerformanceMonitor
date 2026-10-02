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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_lock_wait_trend (#2484): the viewer's Blocking-Trends lock-wait lane, which had no endpoint on
/// either surface. get_wait_trend can chart ONE named LCK type; this is the whole family at once, as a
/// per-second rate.
///
/// <para>Since #3897 the family is the SERIES — every LCK type's wait summed per collection, bucketed — and the
/// types are a LEGEND of the ones that waited, with the ones that never did counted. It was a row per (collection,
/// type), which grew with every lock type a server had ever waited on and was 96% zeros on DARLING01.</para>
///
/// <para>The properties that carry the weight, none of them "the SQL runs".</para>
///
/// <para><b>The empty branch must not be filtered the way the read is.</b> The probe asks whether ANY wait
/// sample exists, not whether an LCK one does — a server collected for months that never took a lock wait
/// is precisely the all-clear this branch exists to give, and an LCK-filtered probe would report it as
/// never collected. That is the false alarm #2508 corrected in the other direction for the edge tables, and
/// this is the periodic-table case where the DATA is the right denominator.</para>
///
/// <para><b>A window of collected, idle LCK types is DATA, not an empty answer (#3897).</b> The collector writes
/// a row every cycle for every lock type the server has ever waited on, so the healthy state is rows of zeros:
/// the answer is a measured-zero family series and a legend that counts the idle types, never "widen
/// hours_back".</para>
///
/// <para><b>The rate must survive being fractional.</b> #2507 shipped an execution-count trend as an
/// integer, so a server at 0.4 executions a second reported zero and read as idle. The same shape is
/// available here: a small delta over a long interval is a real, tiny rate, and integer division would
/// erase it.</para>
///
/// <para><b>The family has ONE interval per collection.</b> Summing the types' deltas is right; summing their
/// intervals would divide the family's rate by the number of types collected with it.</para>
///
/// <para><b>The anchor must reach the query.</b> Proven by CONTENT — rows seeded two days back come back
/// when anchored there and the recent rows do not, which no signature check can see.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingLockWaitTrendTests
{
    private const int ServerId = -949577;
    private const string ServerName = "lock-wait-trend";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task LockWaitRates_AreFractional_Anchored_AndEmptyOnlyMeansQuietWhenWaitsWereCollected()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-wait trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* ── nothing sampled at all: NOT "this server has no lock contention" ── */
            var never = await DarlingMcpBlockingTools.GetLockWaitTrend(dataSource, ServerName, 4);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("NOT a report of a server without lock contention", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /*
                ── wait stats collected, but not one LOCK wait among them ──

                The single most important assertion in this file. This server is healthy and monitored, and
                the honest answer is a genuine all-clear. If the existence probe carried the read's own
                LIKE 'LCK%' filter it would find nothing and call this server uncollected, sending someone
                to fix collection that is working perfectly.
            */
            await SeedWaitAsync(connection, ct, MinutesAgo(30), "CXPACKET", 999_999);

            var noLocks = await DarlingMcpBlockingTools.GetLockWaitTrend(dataSource, ServerName, 4);
            var noLocksDoc = JsonDocument.Parse(noLocks);
            Assert.Equal("empty", noLocksDoc.RootElement.GetProperty("status").GetString());
            var noLocksText = noLocksDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("genuinely quiet rather than broken", noLocksText, StringComparison.Ordinal);

            /* Same zero rows as the branch above, and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", noLocksText, StringComparison.Ordinal);

            /* ── two LCK types, each sampled twice exactly 60 seconds apart ── */
            var first = MinutesAgo(30);
            var second = first.AddSeconds(60);

            await SeedWaitAsync(connection, ct, first, "LCK_M_X", 1_200);
            await SeedWaitAsync(connection, ct, second, "LCK_M_X", 6_000);

            /* Three milliseconds over sixty seconds is 0.05 ms/sec — a real rate that integer division would
               report as zero, which is exactly how a quiet server reads as an idle one. */
            await SeedWaitAsync(connection, ct, first, "LCK_M_S", 10);
            await SeedWaitAsync(connection, ct, second, "LCK_M_S", 3);

            /* A negative delta is the counter reset across a SQL Server restart, not a negative wait. */
            await SeedWaitAsync(connection, ct, second, "LCK_M_U", -500);

            var hit = await DarlingMcpBlockingTools.GetLockWaitTrend(dataSource, ServerName, 4);
            var root = JsonDocument.Parse(hit).RootElement;
            Assert.Equal(ServerName, root.GetProperty("server").GetString());

            /* The family: the FIRST collection of each type has no prior sample to difference against and (a
               pre-V127 row) no stored interval, so it is left out (#3540) — the one rated collection is `second`,
               where the family waited 6,000 + 3 ms over its ONE 60-second interval: 100.05 ms/sec. CXPACKET is
               filtered out by the read even though it is the largest delta in the window, and the reset row is
               dropped rather than charted as a negative wait. */
            var trend = root.GetProperty("trend").EnumerateArray().ToArray();
            var point = Assert.Single(trend);
            Assert.Equal(100.05, point.GetProperty("wait_time_ms_per_second").GetDouble(), 3);
            Assert.Equal(100.05, point.GetProperty("peak_wait_time_ms_per_second").GetDouble(), 3);
            Assert.Equal("2 minutes", root.GetProperty("bucket").GetString());

            /* The legend names the types that waited, heaviest first, each with its own rate — the fractional
               one kept fractional (four places: 0.05, not a rounded 0). */
            var types = root.GetProperty("wait_types").EnumerateArray().ToArray();
            Assert.Equal(new[] { "LCK_M_X", "LCK_M_S" }, types.Select(t => t.GetProperty("wait_type").GetString()).ToArray());
            Assert.Equal(6000d, types[0].GetProperty("total_wait_ms").GetDouble());
            Assert.Equal(100d, types[0].GetProperty("wait_time_ms_per_second").GetDouble(), 4);
            var tinyRate = types[1].GetProperty("wait_time_ms_per_second").GetDouble();
            Assert.True(tinyRate > 0, $"a 3 ms delta over 60 s must not truncate to zero, got {tinyRate}");
            Assert.Equal(0.05d, tinyRate, 4);
            Assert.DoesNotContain(types, t => t.GetProperty("wait_type").GetString() is "CXPACKET" or "LCK_M_U");
            Assert.Equal(0, root.GetProperty("wait_types_idle").GetInt32());

            /* ── the anchor, proven by CONTENT ── */
            var pastFirst = HoursAgo(48);
            var pastSecond = pastFirst.AddSeconds(60);
            await SeedWaitAsync(connection, ct, pastFirst, "LCK_M_IX", 600);
            await SeedWaitAsync(connection, ct, pastSecond, "LCK_M_IX", 1_800);

            /* Unanchored, the same call cannot see them at all — that is the whole reason as_of exists. */
            var unanchored = JsonDocument.Parse(
                await DarlingMcpBlockingTools.GetLockWaitTrend(dataSource, ServerName, 4)).RootElement;
            Assert.DoesNotContain(
                unanchored.GetProperty("wait_types").EnumerateArray(),
                t => t.GetProperty("wait_type").GetString() == "LCK_M_IX");

            var anchored = JsonDocument.Parse(await DarlingMcpBlockingTools.GetLockWaitTrend(
                dataSource, ServerName, 1, pastSecond.ToString("yyyy-MM-ddTHH:mm:ss") + "Z")).RootElement;

            /* One point: the first anchored collection has no prior and is not rated (#3540). */
            var anchoredPoint = Assert.Single(anchored.GetProperty("trend").EnumerateArray().ToArray());
            Assert.Equal(30d, anchoredPoint.GetProperty("wait_time_ms_per_second").GetDouble(), 3);
            Assert.Equal("LCK_M_IX", Assert.Single(anchored.GetProperty("wait_types").EnumerateArray().ToArray()).GetProperty("wait_type").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #3897: the family's denominator is each collection's ONE interval, and a window whose LCK types were
    /// collected and never waited is a measured zero, not an empty answer.
    ///
    /// <para>Fixed instants in March 2026 and stored intervals (the V127 shape), so every figure is exact and
    /// independent of the clock: two types in collection c1, only one of them in c2, a third type idle in both.
    /// One 60-minute bucket holds all three collections.</para>
    /// </summary>
    [Fact]
    public async Task TheFamily_SumsTypesOverOneIntervalPerCollection_AndAnIdleWindowIsAMeasuredZero()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-wait trend test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var c1 = new DateTime(2026, 3, 4, 10, 10, 0, DateTimeKind.Unspecified);
            var c2 = c1.AddMinutes(1);
            var c3 = c1.AddMinutes(2);

            /* c1: X waits 1,200 ms and S 600 ms over one 60 s sweep → 30 ms/s; c2: X 3,000 ms alone, over a
               120 s sweep → 25 ms/s; c3: nothing waits → 0. IX is collected and idle throughout. */
            await SeedWaitAsync(connection, ct, c1, "LCK_M_X", 1_200, interval: 60);
            await SeedWaitAsync(connection, ct, c1, "LCK_M_S", 600, interval: 60);
            await SeedWaitAsync(connection, ct, c1, "LCK_M_IX", 0, interval: 60);
            await SeedWaitAsync(connection, ct, c2, "LCK_M_X", 3_000, interval: 120);
            await SeedWaitAsync(connection, ct, c2, "LCK_M_IX", 0, interval: 120);
            await SeedWaitAsync(connection, ct, c3, "LCK_M_X", 0, interval: 60);
            await SeedWaitAsync(connection, ct, c3, "LCK_M_IX", 0, interval: 60);

            var root = JsonDocument.Parse(await DarlingMcpBlockingTools.GetLockWaitTrend(
                dataSource, ServerName, 2, "2026-03-04T11:00:00Z", bucket_minutes: 60)).RootElement;

            /* One bucket: (1,200 + 600 + 3,000 + 0) ms over (60 + 120 + 60) s = 20 ms/s. Summing the types'
               INTERVALS would have divided c1 by 120 and read 16 ms/s; averaging the per-collection rates would
               have read (30 + 25 + 0) / 3. The peak is c1's family rate, 30. */
            var point = Assert.Single(root.GetProperty("trend").EnumerateArray().ToArray());
            Assert.Equal(20d, point.GetProperty("wait_time_ms_per_second").GetDouble(), 6);
            Assert.Equal(30d, point.GetProperty("peak_wait_time_ms_per_second").GetDouble(), 6);
            Assert.StartsWith("2026-03-04T10:00:00", point.GetProperty("collection_time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(1, root.GetProperty("wait_types_idle").GetInt32());
            Assert.Equal(new[] { "LCK_M_X", "LCK_M_S" }, root.GetProperty("wait_types").EnumerateArray().Select(t => t.GetProperty("wait_type").GetString()).ToArray());
            Assert.EndsWith("The width is the bucket_minutes you passed.", root.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);

            /* ── an hour later, every type collected and idle: data, not "widen hours_back" ── */
            var d1 = new DateTime(2026, 3, 4, 12, 10, 0, DateTimeKind.Unspecified);
            await SeedWaitAsync(connection, ct, d1, "LCK_M_X", 0, interval: 60);
            await SeedWaitAsync(connection, ct, d1.AddMinutes(1), "LCK_M_X", 0, interval: 60);
            await SeedWaitAsync(connection, ct, d1, "LCK_M_S", 0, interval: 60);

            var idle = JsonDocument.Parse(await DarlingMcpBlockingTools.GetLockWaitTrend(
                dataSource, ServerName, 1, "2026-03-04T13:00:00Z")).RootElement;
            Assert.False(idle.TryGetProperty("status", out _), "collected-but-idle lock types are an answer, not an empty status");
            Assert.All(idle.GetProperty("trend").EnumerateArray(), p => Assert.Equal(0d, p.GetProperty("wait_time_ms_per_second").GetDouble()));
            Assert.Empty(idle.GetProperty("wait_types").EnumerateArray());
            Assert.Equal(2, idle.GetProperty("wait_types_idle").GetInt32());
            Assert.Contains("measured zero", idle.GetProperty("wait_types_note").GetString()!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    /// <summary>A wait_stats row; <paramref name="interval"/> null is the pre-V127 shape the read LAG-rates.</summary>
    private static async Task SeedWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string waitType, long deltaMs, int? interval = null) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, waitType, deltaMs, 0L, 1L, interval);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

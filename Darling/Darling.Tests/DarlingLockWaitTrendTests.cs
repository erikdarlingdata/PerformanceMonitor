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
/// <para>Three properties carry the weight here, and none of them is "the SQL runs".</para>
///
/// <para><b>The empty branch must not be filtered the way the read is.</b> The probe asks whether ANY wait
/// sample exists, not whether an LCK one does — a server collected for months that never took a lock wait
/// is precisely the all-clear this branch exists to give, and an LCK-filtered probe would report it as
/// never collected. That is the false alarm #2508 corrected in the other direction for the edge tables, and
/// this is the periodic-table case where the DATA is the right denominator.</para>
///
/// <para><b>The rate must survive being fractional.</b> #2507 shipped an execution-count trend as an
/// integer, so a server at 0.4 executions a second reported zero and read as idle. The same shape is
/// available here: a small delta over a long interval is a real, tiny rate, and integer division would
/// erase it.</para>
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

            /* Three milliseconds over sixty seconds is 0.05 ms/sec — a real rate that integer division
               would report as zero, which is exactly how a quiet server reads as an idle one. */
            await SeedWaitAsync(connection, ct, first, "LCK_M_S", 10);
            await SeedWaitAsync(connection, ct, second, "LCK_M_S", 3);

            /* A negative delta is the counter reset across a SQL Server restart, not a negative wait. */
            await SeedWaitAsync(connection, ct, second, "LCK_M_U", -500);

            var hit = await DarlingMcpBlockingTools.GetLockWaitTrend(dataSource, ServerName, 4);
            var root = JsonDocument.Parse(hit).RootElement;
            Assert.Equal(ServerName, root.GetProperty("server").GetString());

            var trend = root.GetProperty("trend").EnumerateArray().ToArray();

            /* CXPACKET is filtered out by the read even though it is the largest delta in the window. */
            Assert.DoesNotContain(trend, r => r.GetProperty("wait_type").GetString() == "CXPACKET");

            /* The reset row is dropped rather than charted as a negative wait. */
            Assert.DoesNotContain(trend, r => r.GetProperty("wait_type").GetString() == "LCK_M_U");

            /* Four rows: two wait types x two collections. The FIRST collection of each type has no prior
               sample to difference against, so its interval is NULL and its rate is 0 rather than the raw
               delta — the LAG is per wait type, which is what stops one type's cadence describing another. */
            Assert.Equal(4, trend.Length);

            Assert.Equal(100d, RateOf(trend, "LCK_M_X", second), 3);
            Assert.Equal(0d, RateOf(trend, "LCK_M_X", first), 3);

            /* The fractional rate. Asserted as > 0 as well as by value, because "0.05" and "0" differ by a
               cast and the point of the assertion is that the cast is there. */
            var tinyRate = RateOf(trend, "LCK_M_S", second);
            Assert.True(tinyRate > 0, $"a 3 ms delta over 60 s must not truncate to zero, got {tinyRate}");
            Assert.Equal(0.05d, tinyRate, 3);

            /* ── the anchor, proven by CONTENT ── */
            var pastFirst = HoursAgo(48);
            var pastSecond = pastFirst.AddSeconds(60);
            await SeedWaitAsync(connection, ct, pastFirst, "LCK_M_IX", 600);
            await SeedWaitAsync(connection, ct, pastSecond, "LCK_M_IX", 1_800);

            /* Unanchored, the same call cannot see them at all — that is the whole reason as_of exists. */
            var unanchored = JsonDocument.Parse(
                await DarlingMcpBlockingTools.GetLockWaitTrend(dataSource, ServerName, 4)).RootElement;
            Assert.DoesNotContain(
                unanchored.GetProperty("trend").EnumerateArray(),
                r => r.GetProperty("wait_type").GetString() == "LCK_M_IX");

            var anchored = JsonDocument.Parse(await DarlingMcpBlockingTools.GetLockWaitTrend(
                dataSource, ServerName, 1, pastSecond.ToString("yyyy-MM-ddTHH:mm:ss") + "Z")).RootElement;
            var anchoredRows = anchored.GetProperty("trend").EnumerateArray().ToArray();

            Assert.Equal(2, anchoredRows.Length);
            Assert.All(anchoredRows, r => Assert.Equal("LCK_M_IX", r.GetProperty("wait_type").GetString()));
            Assert.Equal(30d, RateOf(anchoredRows, "LCK_M_IX", pastSecond), 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
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

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string waitType, long deltaMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, waitType, deltaMs, 0L, 1L);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_daily_summary_range (#2484): the Performance Calendar's month grid, which had no endpoint on either
/// surface. <c>DarlingHealthReader.GetDailySummaryRangeAsync</c> already existed and nothing called it, so
/// get_daily_summary could only ever answer one day and the web page could only ever show today.
///
/// <para>What is actually worth pinning is not "the aggregate runs" — get_daily_summary already proves that,
/// off the same SQL. It is the three things only the RANGE form can get wrong.</para>
///
/// <para><b>A missing day means missing COLLECTION, not a quiet day.</b> The aggregate's day spine unions
/// nine sources and one of them is the collection log, where ANY run marks the day collected — so a
/// monitored day with nothing to report still appears, Healthy. That is what makes a hole in the returned
/// days diagnostic, and it is why the empty branch probes the collection log rather than the signals.</para>
///
/// <para><b>The band is per day, not per range.</b> Two days in one result must be able to disagree, or the
/// grid is a single verdict wearing a calendar's clothes. The fixture puts a clean day and a day with a
/// collection error in the SAME result for exactly that reason.</para>
///
/// <para><b>The anchor must reach the query, proven by CONTENT.</b> A day seeded ten days back comes back
/// when anchored there and does not on the default anchor.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingDailySummaryRangeTests
{
    private const int ServerId = -949578;
    private const string ServerName = "daily-summary-range";

    /// <summary>#4232: the "nothing ever collected" preamble below needs its OWN server — reading it through
    /// <see cref="ServerId"/> would populate <c>DarlingHealthReader.RangeCache</c>'s closed-day block for that
    /// exact range from an EMPTY store, and the seeded read moments later (same range, same cache key, same
    /// hour) would then see that stale empty block instead of the rows just seeded. That staleness is the
    /// closed-day cache working as designed against a store that already had data — not a substitute for a
    /// server nobody has ever collected from.</summary>
    private const int NeverCollectedServerId = -949579;
    private const string NeverCollectedServerName = "daily-summary-range-never-collected";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheCalendar_BandsEachDaySeparately_ShowsCollectionGaps_AndAnchors()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary range test.");

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
            await DarlingMcpTestData.RegisterServerAsync(connection, NeverCollectedServerId, NeverCollectedServerName, ct);

            var today = DateTime.UtcNow.Date;
            var twoDaysAgo = today.AddDays(-2);
            var tenDaysAgo = today.AddDays(-10);

            /* ── nothing collected at all: an empty calendar is a collection fault, not a quiet fortnight ──
               A dedicated server (see NeverCollectedServerId) so this empty read does not warm ServerId's
               closed-day cache block from a store that has not been seeded yet. */
            var never = await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, NeverCollectedServerName, 3);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("nothing has been collected", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /*
                ── two collected days with a hole between them ──

                Today collected cleanly. Two days ago collected AND recorded an ERROR run — one of two runs,
                a 50% error share past the 20% bar, which bands that day Warning (#3539 A2: the share's
                tier, never Critical). Yesterday is deliberately left alone: it is the gap, and the point of
                the read is that a gap is visible as an ABSENT day rather than as a quiet one.
            */
            await SeedRunAsync(connection, ct, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow), "wait_stats", "SUCCESS");
            await SeedRunAsync(connection, ct, twoDaysAgo.AddHours(12), "wait_stats", "SUCCESS");
            await SeedRunAsync(connection, ct, twoDaysAgo.AddHours(13), "query_store", "ERROR");

            var threeDays = JsonDocument.Parse(
                await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 3)).RootElement;

            Assert.Equal(ServerName, threeDays.GetProperty("server").GetString());
            Assert.Equal(3, threeDays.GetProperty("days_back").GetInt32());

            /* The bounds the read used, echoed back so a caller can tell which days they asked for from
               which days they got — the whole distinction this payload exists to make. */
            Assert.Equal(twoDaysAgo.ToString("yyyy-MM-dd"), threeDays.GetProperty("from_date").GetString());
            Assert.Equal(today.ToString("yyyy-MM-dd"), threeDays.GetProperty("to_date").GetString());

            var days = threeDays.GetProperty("days").EnumerateArray().ToArray();

            /* Two collected days out of three asked for. day_count counts days WITH data, and the gap
               between it and days_back is the useful number. */
            Assert.Equal(2, days.Length);
            Assert.Equal(2, threeDays.GetProperty("day_count").GetInt32());
            Assert.DoesNotContain(days, d => d.GetProperty("summary_date").GetString() == today.AddDays(-1).ToString("yyyy-MM-dd"));

            /* Ordered oldest-first, as the aggregate returns them — a calendar read backwards is a bug
               nobody would spot in a table. */
            Assert.Equal(twoDaysAgo.ToString("yyyy-MM-dd"), days[0].GetProperty("summary_date").GetString());
            Assert.Equal(today.ToString("yyyy-MM-dd"), days[1].GetProperty("summary_date").GetString());

            /* The two days disagree, which is what makes this a calendar rather than one verdict. */
            Assert.Equal("Warning", days[0].GetProperty("overall_health").GetString());
            Assert.Equal(1, days[0].GetProperty("collection_errors").GetInt64());
            /* #3539 A2/A3, additive: the share's denominator and the blocking rate ride the payload, so the
               band's figures are on the surface that bands — a finished day rates over 24 hours. */
            Assert.Equal(2, days[0].GetProperty("collection_runs").GetInt64());
            Assert.Equal(0.0, days[0].GetProperty("blocking_rate_per_hour").GetDouble());
            Assert.Equal("Healthy", days[1].GetProperty("overall_health").GetString());
            Assert.Equal(0, days[1].GetProperty("collection_errors").GetInt64());

            /* ── a range this server has no history for, on a server that HAS collected ── */
            var beforeHistory = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(
                dataSource, ServerName, 1, tenDaysAgo.ToString("yyyy-MM-dd"))).RootElement;

            Assert.Equal("empty", beforeHistory.GetProperty("status").GetString());
            var beforeText = beforeHistory.GetProperty("message").GetString()!;
            Assert.Contains("A day with ANY collection appears here even when every signal was quiet", beforeText, StringComparison.Ordinal);

            /* Same zero rows as the never-collected branch, and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", beforeText, StringComparison.Ordinal);

            /* ── the anchor, proven by CONTENT ── */
            await SeedRunAsync(connection, ct, tenDaysAgo.AddHours(12), "wait_stats", "SUCCESS");

            var anchored = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(
                dataSource, ServerName, 1, tenDaysAgo.ToString("yyyy-MM-dd"))).RootElement;
            var anchoredDay = Assert.Single(anchored.GetProperty("days").EnumerateArray().ToArray());
            Assert.Equal(tenDaysAgo.ToString("yyyy-MM-dd"), anchoredDay.GetProperty("summary_date").GetString());

            /* And the same one-day span unanchored answers about TODAY instead, so the anchor moved the
               range rather than widening it. */
            var unanchored = JsonDocument.Parse(
                await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 1)).RootElement;
            var unanchoredDay = Assert.Single(unanchored.GetProperty("days").EnumerateArray().ToArray());
            Assert.Equal(today.ToString("yyyy-MM-dd"), unanchoredDay.GetProperty("summary_date").GetString());

            /* ── the span is bounded, and refused rather than clamped ── */
            Assert.StartsWith(
                "Invalid days_back value '0'",
                McpHelpers.ErrorMessageOf(await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 0)),
                StringComparison.Ordinal);
            Assert.StartsWith(
                "Invalid days_back value '367'",
                McpHelpers.ErrorMessageOf(await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 367)),
                StringComparison.Ordinal);

            /* A bad anchor is refused too, rather than silently answered as of now — the failure the whole
               parameter exists to remove. */
            Assert.StartsWith(
                "Invalid as_of",
                McpHelpers.ErrorMessageOf(await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 30, "last tuesday")),
                StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task SeedRunAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string collector, string status) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, status,
            status == "ERROR" ? "seeded failure" : null, 10, 80, 20);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", NeverCollectedServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", NeverCollectedServerId);
    }

    /// <summary>
    /// #4232: the closed-day cache, end to end against a real store. A closed day (two days ago) is seeded,
    /// read once (populating the cache's block), then seeded with a SECOND wait row before a second read. If
    /// the closed day were re-read from the store, the second read would see the new row; it must not — that
    /// is exactly what "the closed portion is cached for an hour" means. Today (open) picks up its own second
    /// row every time, because it is never in the cached block.
    /// </summary>
    [Fact]
    public async Task GetDailySummaryRangeAsync_ClosedDayCache_SecondReadDoesNotSeeANewRowOnAClosedDay_ButDoesOnToday()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary range cache test.");

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

            var today = DateTime.UtcNow.Date;
            var twoDaysAgo = today.AddDays(-2); // well past the two-hour grace -- closed on every read below
            var fromDate = today.AddDays(-3);
            var toDate = today.AddDays(1);

            await PlantWaitAsync(connection, twoDaysAgo.AddHours(10), 1000, ct);
            await PlantWaitAsync(connection, today.AddHours(1), 500, ct);

            var first = await DarlingHealthReader.GetDailySummaryRangeAsync(dataSource, ServerId, fromDate, toDate, cancellationToken: ct);
            var firstClosedDay = first.Rows.Single(r => r.SummaryDate.Date == twoDaysAgo);
            var firstToday = first.Rows.Single(r => r.SummaryDate.Date == today);
            Assert.Equal(1000m, firstClosedDay.TotalWaitTimeSec * 1000m); // 1000 ms of wait, as seeded
            Assert.Equal(500m, firstToday.TotalWaitTimeSec * 1000m);

            /* Land a second row on EACH day after the first read -- the closed day's cached block must not
               see this; today, never cached, must. */
            await PlantWaitAsync(connection, twoDaysAgo.AddHours(11), 4000, ct);
            await PlantWaitAsync(connection, today.AddHours(2), 2000, ct);

            var second = await DarlingHealthReader.GetDailySummaryRangeAsync(dataSource, ServerId, fromDate, toDate, cancellationToken: ct);
            var secondClosedDay = second.Rows.Single(r => r.SummaryDate.Date == twoDaysAgo);
            var secondToday = second.Rows.Single(r => r.SummaryDate.Date == today);

            Assert.Equal(1000m, secondClosedDay.TotalWaitTimeSec * 1000m); // unchanged: served from the cached block
            Assert.Equal(2500m, secondToday.TotalWaitTimeSec * 1000m); // 500 + 2000: today is never cached

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime atUtc, long waitMs, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(atUtc), ServerId, ServerName, "CXPACKET", 5L, waitMs);
}

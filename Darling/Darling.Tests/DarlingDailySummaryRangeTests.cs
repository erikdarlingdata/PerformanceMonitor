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

            var today = DateTime.UtcNow.Date;
            var twoDaysAgo = today.AddDays(-2);
            var tenDaysAgo = today.AddDays(-10);

            /* ── nothing collected at all: an empty calendar is a collection fault, not a quiet fortnight ── */
            var never = await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 3);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("nothing has been collected", neverText, StringComparison.Ordinal);
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /*
                ── two collected days with a hole between them ──

                Today collected cleanly. Two days ago collected AND recorded an ERROR run, which bands that
                day Critical. Yesterday is deliberately left alone: it is the gap, and the point of the read
                is that a gap is visible as an ABSENT day rather than as a quiet one.
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
            Assert.Equal("Critical", days[0].GetProperty("overall_health").GetString());
            Assert.Equal(1, days[0].GetProperty("collection_errors").GetInt64());
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
                await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 0),
                StringComparison.Ordinal);
            Assert.StartsWith(
                "Invalid days_back value '367'",
                await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 367),
                StringComparison.Ordinal);

            /* A bad anchor is refused too, rather than silently answered as of now — the failure the whole
               parameter exists to remove. */
            Assert.StartsWith(
                "Invalid as_of",
                await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 30, "last tuesday"),
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
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

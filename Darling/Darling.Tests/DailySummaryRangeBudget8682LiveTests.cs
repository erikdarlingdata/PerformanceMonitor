/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
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
/// #8682: get_daily_summary_range grows with days_back and passed the 32,768-byte response budget at about 70 days without
/// saying so. It now keeps the newest days that fit and says what it left out, with the shared budget keys. A 30-day answer
/// fits and carries no cut. The web mirror passes no budget and keeps its whole span.
/// </summary>
[Collection("live-postgres")]
public sealed class DailySummaryRangeBudget8682LiveTests
{
    private const int ServerId = -868201;
    private const string ServerName = "daily-summary-range-budget";
    private const int SeededDays = 150;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Day(DateTime day) => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    [Fact]
    public async Task WideRange_FitsTheBudget_KeepsTheNewestDays_AndTheNoteReadsOnToTheRest_ButThirtyDaysAreWhole()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live daily-summary budget test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        DarlingHealthReader.ResetRangeCacheForTests();
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var today = DateTime.UtcNow.Date;
            for (var back = 0; back < SeededDays; back++)
            {
                /* One collector run on every day: any run puts the day on the spine, so every day is one row of the reply. */
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                    CollectionIdGenerator.Next(), ServerId, ServerName, "wait_stats",
                    DarlingMcpTestData.Naive(today.AddDays(-back).AddHours(1)), 100, "SUCCESS", null, 10, 80, 20);
            }

            /* The default span fits: whole, no cut, the keys present and quiet. */
            var thirty = await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 30);
            var thirtyRoot = JsonDocument.Parse(thirty).RootElement;
            Assert.True(Encoding.UTF8.GetByteCount(thirty) <= McpResponseBudget.DefaultBytes);
            Assert.Equal(30, thirtyRoot.GetProperty("day_count").GetInt32());
            Assert.Equal(30, thirtyRoot.GetProperty("days").GetArrayLength());
            Assert.False(thirtyRoot.GetProperty("budget_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, thirtyRoot.GetProperty("budget_note").ValueKind);
            Assert.Equal(Day(today.AddDays(-29)), thirtyRoot.GetProperty("from_date").GetString());

            /* The widest span is far over the budget on the old code. It now fits, says so, and keeps the newest days. */
            var wide = await DarlingMcpHealthTools.GetDailySummaryRange(dataSource, ServerName, 366);
            var wideRoot = JsonDocument.Parse(wide).RootElement;
            Assert.True(Encoding.UTF8.GetByteCount(wide) <= McpResponseBudget.DefaultBytes,
                $"get_daily_summary_range 366 was {Encoding.UTF8.GetByteCount(wide):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
            Assert.True(wideRoot.GetProperty("budget_truncated").GetBoolean());
            var days = wideRoot.GetProperty("days").EnumerateArray().Select(d => d.GetProperty("summary_date").GetString()!).ToList();
            Assert.Equal(days.Count, wideRoot.GetProperty("day_count").GetInt32());
            Assert.InRange(days.Count, 31, SeededDays - 1);
            Assert.Equal(Day(today), days[^1]); // the newest day rides
            for (var i = 0; i < days.Count; i++)
                Assert.Equal(Day(today.AddDays(i - (days.Count - 1))), days[i]); // whole, contiguous days

            var note = wideRoot.GetProperty("budget_note").GetString()!;
            Assert.Contains("32,768-byte response budget", note, StringComparison.Ordinal);
            Assert.Contains($"{SeededDays} days were read; only {days.Count} fit", note, StringComparison.Ordinal);

            /* The note's own call reads the older days. Following it page by page (a page of past-horizon days is wider
               and may be cut again) holds every seeded day exactly once. */
            var seen = days.ToList();
            var pageNote = note;
            var pages = 1;
            while (pageNote is not null)
            {
                var next = Regex.Match(pageNote, @"as_of (\d{4}-\d{2}-\d{2}) and days_back (\d+)");
                Assert.True(next.Success, pageNote);
                Assert.Equal(Day(DateTime.ParseExact(seen[0], "yyyy-MM-dd", CultureInfo.InvariantCulture).AddDays(-1)), next.Groups[1].Value);
                var older = await DarlingMcpHealthTools.GetDailySummaryRange(
                    dataSource, ServerName, int.Parse(next.Groups[2].Value, CultureInfo.InvariantCulture), next.Groups[1].Value);
                Assert.True(Encoding.UTF8.GetByteCount(older) <= McpResponseBudget.DefaultBytes);
                var olderRoot = JsonDocument.Parse(older).RootElement;
                var olderDays = olderRoot.GetProperty("days").EnumerateArray().Select(d => d.GetProperty("summary_date").GetString()!).ToList();
                seen.InsertRange(0, olderDays);
                pageNote = olderRoot.GetProperty("budget_note").GetString();
                Assert.InRange(++pages, 2, 6);
            }

            Assert.Equal(SeededDays, seen.Count);
            Assert.Equal(SeededDays, seen.Distinct().Count());
            Assert.Equal(seen.OrderBy(d => d, StringComparer.Ordinal).ToList(), seen);

            /* The web mirror passes no budget and keeps the whole span the Performance Calendar asked for. */
            var web = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(
                dataSource, ServerName, 366, null, null, null, responseByteBudget: null, ct)).RootElement;
            Assert.Equal(SeededDays, web.GetProperty("days").GetArrayLength());
            Assert.False(web.GetProperty("budget_truncated").GetBoolean());

            bodySucceeded = true;
        }
        finally
        {
            DarlingHealthReader.ResetRangeCacheForTests();
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

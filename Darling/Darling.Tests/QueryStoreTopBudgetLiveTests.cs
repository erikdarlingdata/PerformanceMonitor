/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
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
/// #4198: <c>get_query_store_top</c> at default arguments (top=20) measured 48 KB on a busy production
/// store's single server -- over the shared 32 KB response budget (<see cref="McpResponseBudget.DefaultBytes"/>).
/// The default page's <c>query_text</c> was truncated only at 2,000 characters, with no opt-in for the whole
/// statement and no disclosure that a row had been cut.
///
/// <para>Twenty-five rows across five query-text lengths (120 to 2,600 characters -- some land under the new
/// 400-character preview, some between it and the old 2,000-character cap, some past even that) reproduce the
/// shape here without a production store. Twenty-five so the tool's own top+5 over-fetch (for the WAITFOR
/// self-exclusion) has a full house to rank from at the default top=20.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreTopBudgetLiveTests
{
    private const string ServerName = "query-store-top-budget-4198";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>One length lands under the new preview, two land between it and the old cap, two land past
    /// the old cap entirely -- so both the new query_text_truncated boundary and the old blanket 2,000-char
    /// truncation it replaces are exercised.</summary>
    private static readonly int[] QueryTextLengths = { 120, 600, 1400, 2000, 2600 };

    private const int RowCount = 25;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DefaultCall_StaysUnderTheResponseBudget_AndFullTextOptInStillGetsTheWholeStatement()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-store-top budget census.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var longestQueryText = await SeedAsync(connection, ct);

            var defaultAnswer = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName);
            Assert.False(McpHelpers.IsErrorEnvelope(defaultAnswer), $"tool returned an error: {defaultAnswer}");

            var defaultBytes = Encoding.UTF8.GetByteCount(defaultAnswer);
            Assert.True(
                defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_query_store_top at default arguments answered {defaultBytes} bytes, ranked from "
                + $"{RowCount} seeded rows spanning query_text lengths {string.Join(",", QueryTextLengths)} "
                + $"-- over the {McpResponseBudget.DefaultBytes}-byte budget (#4198 measured 48 KB on a "
                + "production store's single server).");

            using var defaultDoc = JsonDocument.Parse(defaultAnswer);
            var defaultRoot = defaultDoc.RootElement;
            var queries = defaultRoot.GetProperty("queries").EnumerateArray();

            var sawTruncatedPreview = false;
            var sawUntouchedPreview = false;
            var rowCount = 0;
            foreach (var row in queries)
            {
                rowCount++;
                var preview = row.GetProperty("query_text").GetString();
                Assert.NotNull(preview);
                var wasTruncated = row.GetProperty("query_text_truncated").GetBoolean();

                if (wasTruncated)
                {
                    sawTruncatedPreview = true;
                    Assert.True(preview!.Length < 2000, "a row marked query_text_truncated still carried the old 2,000-character preview -- the wide field was not cut.");
                }
                else
                {
                    sawUntouchedPreview = true;
                }
            }

            Assert.Equal(20, rowCount);
            Assert.True(sawTruncatedPreview, "no row in the default page reported query_text_truncated=true, so the 1,400/2,000/2,600-character seeded rows were not previewed.");
            Assert.True(sawUntouchedPreview, "no row in the default page reported query_text_truncated=false, so a short seeded row (120-char) was previewed when it should not have been.");

            /* ── the explicit ask still gets the whole thing (#4198's "keep the envelope honest") ── */
            var fullTextAnswer = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName, top: 1, full_text: true);
            Assert.False(McpHelpers.IsErrorEnvelope(fullTextAnswer), $"tool returned an error: {fullTextAnswer}");

            using var fullTextDoc = JsonDocument.Parse(fullTextAnswer);
            var fullRow = Assert.Single(fullTextDoc.RootElement.GetProperty("queries").EnumerateArray());
            Assert.Equal(longestQueryText, fullRow.GetProperty("query_text").GetString());
            Assert.False(fullRow.GetProperty("query_text_truncated").GetBoolean());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Twenty-five distinct (database, query_id, plan_id) groups, twenty minutes apart so all fall inside the
    /// default 24-hour window; execution_count grows with <c>i</c> so the ranking (SUM(execution_count) *
    /// AVG(avg_duration_us)) is deterministic and the most expensive row is the last one seeded -- the one
    /// whose full, untruncated text the full_text assertion checks.
    /// </summary>
    private static async Task<string> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var newest = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-2));
        string? mostExpensiveQueryText = null;
        var highestCost = -1L;

        for (var i = 0; i < RowCount; i++)
        {
            var collectionTime = newest.AddMinutes(-20 * (RowCount - i));
            var databaseName = $"query_store_top_budget_db_{i % 4}";
            var queryId = 5_000_000_000L + i;
            var planId = 6_000_000_000L + i;
            var queryText = BuildQueryText(i, QueryTextLengths[i % QueryTextLengths.Length]);
            var executionCount = 100L + (i * 37);
            var avgDurationUs = 5_000L + (i * 211);
            var cost = executionCount * avgDurationUs;

            if (cost > highestCost)
            {
                highestCost = cost;
                mostExpensiveQueryText = queryText;
            }

            using var command = new NpgsqlCommand(
                """
                INSERT INTO query_store_stats
                    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                     execution_type_desc, last_execution_time, module_name, query_text, query_hash, query_plan_hash,
                     execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
                     avg_physical_io_reads, avg_rowcount)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7,
                     'Regular', $2, $8, $9, $10, $11,
                     $12, $13, $14, $15, $16,
                     $17, $18)
                """, connection);

            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(collectionTime);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(databaseName);
            command.Parameters.AddWithValue(queryId);
            command.Parameters.AddWithValue(planId);
            command.Parameters.AddWithValue((object?)(i % 3 == 0 ? null : $"dbo.usp_QueryStoreBudgetProbe_{i}") ?? DBNull.Value);
            command.Parameters.AddWithValue(queryText);
            command.Parameters.AddWithValue("0xQ" + queryId.ToString(System.Globalization.CultureInfo.InvariantCulture));
            command.Parameters.AddWithValue("0xP" + planId.ToString(System.Globalization.CultureInfo.InvariantCulture));
            command.Parameters.AddWithValue(executionCount);
            command.Parameters.AddWithValue(avgDurationUs);
            command.Parameters.AddWithValue(avgDurationUs / 2);
            command.Parameters.AddWithValue(120.5 + i);
            command.Parameters.AddWithValue(3.25 + (i % 5));
            command.Parameters.AddWithValue(45.0 + i);
            command.Parameters.AddWithValue(1_000.0 + (i * 10));
            await command.ExecuteNonQueryAsync(ct);
        }

        return mostExpensiveQueryText!;
    }

    /// <summary>
    /// A synthetic but Query-Store-shaped statement, an IN-list padded out to <paramref name="length"/>
    /// characters -- the shape a heavy analytical statement with a wide filter list actually has, rather than
    /// repeated filler that would not exercise anything about how a real query reads.
    /// </summary>
    private static string BuildQueryText(int index, int length)
    {
        var sb = new StringBuilder(length + 64);
        sb.Append("SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount FROM Sales.Orders AS o ")
          .Append("WHERE o.RegionId = ").Append(index % 12).Append(" AND o.StatusCode IN (");

        var n = 0;
        while (sb.Length < length)
        {
            sb.Append(n).Append(',');
            n++;
        }

        sb.Append(") ORDER BY o.OrderDate DESC;");
        return sb.ToString()[..length];
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; "
            + $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}

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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198: <c>get_plan_corrections</c> at default arguments measured 95,428 bytes on a busy production
/// store's single server — the third-largest of twelve read tools #4198 found over the shared 32 KB response
/// budget (<see cref="McpResponseBudget.DefaultBytes"/>). The default page (51 rows fetched to detect
/// truncation past a 50-row <c>limit</c>) carried each row's <c>query_text</c> truncated only at 2,000
/// characters, and Query Store regularly hands back regressed statements that long or longer — the wide field
/// the issue's fix shape names, not the row count.
///
/// <para>Fifty-one rows across five query-text lengths (150 to 4,200 characters, so some land under any
/// sensible preview and some well over it) reproduce that order of magnitude here without a production store:
/// the pre-fix page ran to six figures. Four of the rows share the single newest <c>collection_time</c>, one
/// per database, the way one capture cycle recording several databases' recommendations together would —
/// that is what <c>automatic_tuning</c> reads off.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PlanCorrectionBudgetLiveTests
{
    private const string ServerName = "plan-correction-budget-4198";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>Five lengths cycled over the seeded rows, skewed the way a real regressed-query population
    /// is: one lands under any reasonable preview, four exceed even the old 2,000-character cap.</summary>
    private static readonly int[] QueryTextLengths = { 100, 2200, 2800, 3600, 4600 };

    private const int RowCount = 51;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DefaultCall_StaysUnderTheResponseBudget_AndFullTextOptInStillGetsTheWholeStatement()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-correction budget census.");

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

            var defaultAnswer = await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, ServerName);
            Assert.False(McpHelpers.IsErrorEnvelope(defaultAnswer), $"tool returned an error: {defaultAnswer}");

            var defaultBytes = Encoding.UTF8.GetByteCount(defaultAnswer);
            Assert.True(
                defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_plan_corrections at default arguments answered {defaultBytes} bytes, paged from "
                + $"{RowCount} seeded rows spanning query_text lengths {string.Join(",", QueryTextLengths)} "
                + $"— over the {McpResponseBudget.DefaultBytes}-byte budget (#4198 measured 95,428 on a "
                + "production store's single busiest server).");

            using var defaultDoc = JsonDocument.Parse(defaultAnswer);
            var defaultRoot = defaultDoc.RootElement;
            Assert.True(defaultRoot.GetProperty("truncated").GetBoolean());
            Assert.Equal(25, defaultRoot.GetProperty("recommendations_returned").GetInt32());
            Assert.Equal(4, defaultRoot.GetProperty("automatic_tuning").GetArrayLength());

            var sawTruncatedPreview = false;
            var sawUntouchedPreview = false;
            foreach (var row in defaultRoot.GetProperty("recommendations").EnumerateArray())
            {
                var preview = row.GetProperty("query_text").GetString();
                Assert.NotNull(preview);
                var wasTruncated = row.GetProperty("query_text_truncated").GetBoolean();

                if (wasTruncated)
                {
                    sawTruncatedPreview = true;
                    Assert.True(preview!.Length < 2000, "a row marked query_text_truncated still carried the old 2,000-character preview — the wide field was not cut.");
                }
                else
                {
                    sawUntouchedPreview = true;
                }
            }

            Assert.True(sawTruncatedPreview, "no row in the default page reported query_text_truncated=true, so the 2,600/4,200-character seeded rows were not previewed.");
            Assert.True(sawUntouchedPreview, "no row in the default page reported query_text_truncated=false, so a short seeded row (150/600 chars) was previewed when it should not have been.");

            /* ── the explicit ask still gets the whole thing (#4198's "keep the envelope honest") ── */
            var fullTextAnswer = await DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, ServerName, limit: 1, full_text: true);
            Assert.False(McpHelpers.IsErrorEnvelope(fullTextAnswer), $"tool returned an error: {fullTextAnswer}");

            using var fullTextDoc = JsonDocument.Parse(fullTextAnswer);
            var fullRow = Assert.Single(fullTextDoc.RootElement.GetProperty("recommendations").EnumerateArray());
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
    /// Fifty-one recommendation rows, twenty minutes apart so all fall inside the default 24-hour window, the
    /// newest 51 of which is also all of them. The last four collapse onto one shared newest
    /// <c>collection_time</c> — one per database — so <c>automatic_tuning</c> sees four rows instead of one.
    /// Returns the newest row's full (untruncated) query text, for the <c>full_text</c> opt-in assertion.
    /// </summary>
    private static async Task<string> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var newest = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-2));
        string? longestQueryText = null;

        for (var i = 0; i < RowCount; i++)
        {
            /* The last four rows (i = RowCount-4 .. RowCount-1) share `newest` so one capture cycle's four
               databases land together; every earlier row steps back twenty minutes per index. */
            var collectionTime = i >= RowCount - 4
                ? newest
                : newest.AddMinutes(-20 * (RowCount - i));

            var databaseIndex = i % 4;
            var databaseName = $"plan_correction_budget_db_{databaseIndex}";
            var queryText = BuildQueryText(i, QueryTextLengths[i % QueryTextLengths.Length]);

            if (collectionTime == newest && (longestQueryText == null || queryText.Length > longestQueryText.Length))
            {
                longestQueryText = queryText;
            }

            using var command = new NpgsqlCommand(
                """
                INSERT INTO plan_correction
                    (collection_id, collection_time, server_id, server_name, database_name,
                     force_last_good_plan_desired_state, force_last_good_plan_actual_state, force_last_good_plan_reason,
                     recommendation_name, recommendation_state, recommendation_state_reason, recommendation_reason,
                     valid_since, last_refresh, score, query_id, query_text, regressed_plan_id, last_good_plan_id,
                     last_good_plan_forcing_type, last_good_plan_is_forced, last_good_plan_force_failure_reason,
                     regressed_plan_execution_count, regressed_plan_cpu_time_average_ms,
                     last_good_plan_execution_count, last_good_plan_cpu_time_average_ms,
                     estimated_gain_seconds, execute_action_initiated_by, execute_action_initiated_time,
                     revert_action_initiated_by, revert_action_initiated_time)
                VALUES
                    ($1, $2, $3, $4, $5,
                     $6, $6, $7,
                     $8, $9, $10, $11,
                     $2, $2, $12, $13, $14, $15, $16,
                     $17, $18, $19,
                     $20, $21,
                     $22, $23,
                     $24, $25, $26,
                     $25, $26)
                """, connection);

            /* Realistic sparsity, not every field populated on every row: most captures of an open
               recommendation are still Active, so the action fields are NULL until the engine (or an
               operator) actually acts. One row in five simulates an acted-on recommendation instead. A
               synthetic row with every column filled in would overstate real per-row bytes and understate
               how much of #4198's measured size was the query_text field specifically. */
            var wasActioned = i % 5 == 4;

            command.Parameters.AddWithValue(i + 1L);
            command.Parameters.AddWithValue(collectionTime);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(databaseName);
            command.Parameters.AddWithValue("Enabled");
            command.Parameters.AddWithValue("Automatic tuning option is enabled at the database level.");
            command.Parameters.AddWithValue($"PlanRegression_budget_probe_{i}");
            command.Parameters.AddWithValue("Active");
            command.Parameters.AddWithValue("A new plan was recommended after a regression was detected.");
            command.Parameters.AddWithValue(i % 3 == 0 ? "Query Duration Increased" : "Query CPU Time Increased");
            command.Parameters.AddWithValue(50 + (i % 50));
            command.Parameters.AddWithValue(1_000_000_000L + i);
            command.Parameters.AddWithValue(queryText);
            command.Parameters.AddWithValue(2_000_000_000L + i);
            command.Parameters.AddWithValue(3_000_000_000L + i);
            command.Parameters.AddWithValue("Auto");
            command.Parameters.AddWithValue(i % 2 == 0);
            command.Parameters.AddWithValue((object?)null ?? DBNull.Value);
            command.Parameters.AddWithValue(500L + i);
            command.Parameters.AddWithValue(120.5 + i);
            command.Parameters.AddWithValue(500L + i);
            command.Parameters.AddWithValue(45.25 + i);
            command.Parameters.AddWithValue(3.5 + (i % 10));
            command.Parameters.AddWithValue((object?)(wasActioned ? "AUTOMATIC" : null) ?? DBNull.Value);
            command.Parameters.AddWithValue((object?)(wasActioned ? collectionTime : (DateTime?)null) ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(ct);
        }

        return longestQueryText!;
    }

    /// <summary>
    /// A synthetic but Query-Store-shaped statement, an IN-list padded out to <paramref name="length"/>
    /// characters — the shape a regressed statement with a wide filter list actually has, rather than
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
            $"DELETE FROM plan_correction WHERE server_id = {ServerId}; "
            + $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}

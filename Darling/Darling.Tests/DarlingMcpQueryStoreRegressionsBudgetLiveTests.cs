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
/// #4198: get_query_store_regressions' own response-budget pin - the worst #4198 offender, a busy production
/// store's default call (limit 50 then) measured 211 KB, almost all of it fifty rows' worth of unbounded
/// query_text plus ten un-rounded numeric fields each. Plants forty regressed queries with ~4,000-character
/// text (a realistic wide generated statement) and asserts the default call stays under
/// <see cref="McpResponseBudget.DefaultBytes"/>, that <c>full_text: true</c> opts back into the whole text,
/// and that the default row limit (now 30) truncates with <c>truncated</c> observed rather than inferred. New
/// file (not the shared seeding in <see cref="DarlingQueryStoreRegressionsLiveTests"/>) because #4198 ran a
/// dozen lanes against this store tonight.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpQueryStoreRegressionsBudgetLiveTests
{
    private const string ServerName = "darling-mcp-qs-regressions-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "AppDb";
    private const int RegressedQueryCount = 40;
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private readonly ITestOutputHelper _output;

    public DarlingMcpQueryStoreRegressionsBudgetLiveTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task GetQueryStoreRegressions_Default_StaysUnderResponseBudget_WithFortyWideRows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-store-regressions budget test.");

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
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var queryText = BuildLongQueryText(approxLength: 4_000);

            for (var i = 0; i < RegressedQueryCount; i++)
            {
                var queryId = 1000 + i;
                await SeedAsync(connection, ct, baseNow.AddHours(-40), queryId, executions: 50, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2 * i + 1, text: "SELECT 1");
                await SeedAsync(connection, ct, baseNow.AddMinutes(-30), queryId, executions: 200, avgDurationUs: 4000, avgCpuUs: 4000, intervalId: 2 * i + 2, text: queryText);
            }

            var defaultJson = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName);
            using var defaultParsed = JsonDocument.Parse(defaultJson);
            Assert.Equal(30, defaultParsed.RootElement.GetProperty("regression_count").GetInt32());
            Assert.True(defaultParsed.RootElement.GetProperty("truncated").GetBoolean());

            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
            _output.WriteLine($"get_query_store_regressions default call: {defaultBytes:N0} bytes (budget {McpResponseBudget.DefaultBytes:N0}), {RegressedQueryCount} planted {queryText.Length:N0}-char rows.");
            Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_query_store_regressions' default call is {defaultBytes:N0} bytes over {RegressedQueryCount} planted {queryText.Length:N0}-char rows, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            var firstRow = defaultParsed.RootElement.GetProperty("regressions")[0];
            Assert.True(firstRow.GetProperty("query_text_truncated").GetBoolean());
            var previewText = firstRow.GetProperty("query_text").GetString();
            Assert.NotNull(previewText);
            Assert.True(previewText!.Length < queryText.Length, "the default call's query_text should be a preview shorter than the planted text.");

            /* full_text opts back into the whole text. */
            var fullJson = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, full_text: true);
            Assert.DoesNotContain("\"query_text_truncated\": true", fullJson, StringComparison.Ordinal);
            using var fullParsed = JsonDocument.Parse(fullJson);
            Assert.Equal(queryText, fullParsed.RootElement.GetProperty("regressions")[0].GetProperty("query_text").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Builds SQL text near <paramref name="approxLength"/> characters, ASCII only so its length in
    /// .NET UTF-16 chars and its size in UTF-8 bytes stay close (production application-generated SQL is
    /// almost entirely ASCII: identifiers, literals, punctuation).</summary>
    private static string BuildLongQueryText(int approxLength)
    {
        var sb = new StringBuilder();
        sb.Append("INSERT INTO dbo.Widgets (Id, Name, Description, CreatedAt, OwnerId) VALUES ");
        var i = 0;
        while (sb.Length < approxLength)
        {
            if (i > 0) sb.Append(", ");
            sb.Append($"({i}, N'widget-{i}', N'a generated row from a bulk insert batch', '2026-09-25T00:00:00', {i % 97})");
            i++;
        }

        return sb.ToString();
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, long queryId, long executions,
        long avgDurationUs, long avgCpuUs, long intervalId, string text) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            Db, queryId, 9L, "Regular", executions, avgDurationUs, avgCpuUs, 100L, intervalId,
            text, DarlingMcpTestData.Naive(collectionTime));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}; DELETE FROM config_monitored_servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

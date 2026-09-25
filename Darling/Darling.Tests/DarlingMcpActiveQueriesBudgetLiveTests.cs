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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198: get_active_queries' own response-budget pin. A busy production store's default call (limit 50,
/// hours_back 1) measured 56,586 bytes. Unlike get_deadlock_detail's one wide field per row, this tool's
/// overrun is TWENTY-THREE fields repeated across up to 50 rows: query_text is the widest single field, but
/// the fixed columns (collection_time, every wait/blocking/memory field, login_name, host_name,
/// program_name) add up across the page too. Plants 50 rows with realistic field widths — most query_text
/// around 700 characters (a parameterized statement with a modest literal list), ten near 2,800 characters
/// (a big IN-list, the realistic cause of an outsized capture) — and asserts the default call stays under
/// <see cref="McpResponseBudget.DefaultBytes"/>, that <c>full_text: true</c> (renamed from
/// <c>full_query_text</c> to match <c>get_store_query_stats</c>) opts back into the whole
/// text. New file (not the shared seeding in <c>DarlingMcpSessionToolsTests.cs</c>) because #4198 ran a
/// dozen lanes against this store tonight.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpActiveQueriesBudgetLiveTests
{
    private const string ServerName = "darling-mcp-active-queries-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private readonly ITestOutputHelper _output;

    public DarlingMcpActiveQueriesBudgetLiveTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task GetActiveQueries_Default_StaysUnderResponseBudget_WithFiftyRealisticRows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries budget test.");

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
            var baseTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-49);
            var wideText = BuildQueryText(seed: 999, approxLength: 2_800);

            for (var i = 0; i < 50; i++)
            {
                var t = DarlingMcpTestData.Naive(baseTime.AddMinutes(i));
                var db = i % 3 == 0 ? "StackOverflow" : i % 3 == 1 ? "AdventureWorks" : "ReportingDW";
                var isWide = i >= 40;
                var text = isWide ? wideText : BuildQueryText(seed: i, approxLength: 700);
                var hasWait = i % 4 == 0;

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, 100 + i, db,
                    "00 00:00:05.125", text, i % 5 == 0 ? "suspended" : "running", 0,
                    hasWait ? "PAGEIOLATCH_SH" : null, hasWait ? 250L + i : 0L, 1000L + (i * 37), 1500L + (i * 37),
                    10_000L + (i * 123), 50L + i, i % 7, i % 6 == 0 ? 0.75m : 0m,
                    i % 2 == 0 ? "Read Committed" : "Repeatable Read", i % 6 == 0 ? 8 : 1, i % 6 == 0 ? 4 : 0,
                    i % 2 == 0 ? "app_svc_prod" : @"CONTOSO\svc_reporting", $"APPSRV{i % 5:D2}",
                    i % 2 == 0 ? ".Net SqlClient Data Provider" : "MyOrderService.Worker", i % 3 == 0 ? 1 : 0, 0);
            }

            /* #4198's own default (25, down from 50) truncates the 50-row population — total_snapshots still
               says 50 held, so a caller can tell there was more. */
            var defaultJson = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(defaultJson, ServerName, "queries");
            JsonAssert.Contains("\"total_snapshots\": 50", defaultJson);
            JsonAssert.Contains("\"snapshots_returned\": 25", defaultJson);
            JsonAssert.Contains("\"truncated\": true", defaultJson);

            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
            _output.WriteLine($"get_active_queries default call: {defaultBytes:N0} bytes (budget {McpResponseBudget.DefaultBytes:N0}), 50 planted rows (page 25), 10 with a {wideText.Length:N0}-char query_text.");
            Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_active_queries' default call is {defaultBytes:N0} bytes over 50 planted rows, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            using var defaultParsed = JsonDocument.Parse(defaultJson);
            var wideRow = defaultParsed.RootElement.GetProperty("queries")[0];
            Assert.True(wideRow.GetProperty("query_text").GetString()!.Length < wideText.Length,
                "the default call's widest row should preview shorter than the planted text.");
            Assert.True(wideRow.GetProperty("query_text_truncated").GetBoolean());

            /* full_text opts back into the whole text for every row, including the newest (widest) one. */
            var fullJson = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, full_text: true);
            Assert.DoesNotContain("\"query_text_truncated\": true", fullJson, StringComparison.Ordinal);
            using var fullParsed = JsonDocument.Parse(fullJson);
            Assert.Equal(wideText, fullParsed.RootElement.GetProperty("queries")[0].GetProperty("query_text").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Builds ASCII SQL text (a big literal IN-list, a realistic cause of an outsized capture) near
    /// <paramref name="approxLength"/> characters, so its length in .NET UTF-16 chars and its size in UTF-8
    /// bytes stay close.</summary>
    private static string BuildQueryText(int seed, int approxLength)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount, c.CustomerName, c.Region FROM dbo.Orders o JOIN dbo.Customers c ON c.CustomerId = o.CustomerId WHERE o.OrderDate >= '2026-01-01' AND o.Status IN (");
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append(seed * 100_000 + i);
            sb.Append(',');
            i++;
        }

        sb.Append(") ORDER BY o.OrderDate DESC;");
        return sb.ToString();
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

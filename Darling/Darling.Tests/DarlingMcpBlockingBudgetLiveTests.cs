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
/// #4198: get_blocking's own response-budget pin. Every row on this tool carries ~37 fields (isolation
/// levels, client app/host/login for both sides, six last-tran/last-batch stamps, a dedup_key) before either
/// SQL text column is counted, so the default page's own width times the default row LIMIT is most of the
/// bytes here — not one wide field the way get_deadlock_detail's graph is. Plants 30 rows (the default
/// limit) with realistic-width blocked/blocking SQL text and asserts the default call stays under
/// <see cref="McpResponseBudget.DefaultBytes"/>, that <c>full_text</c> opts back into the whole text on both
/// columns, and that a <c>dedup_key</c> call (naming one incident) returns the whole text even without
/// <c>full_text</c>. New file (not the shared seeding in <see cref="DarlingMcpBlockingToolsLivePostgresTests"/>)
/// because #4198 ran a dozen lanes against this store tonight.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpBlockingBudgetLiveTests
{
    private const string ServerName = "darling-mcp-blocking-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private readonly ITestOutputHelper _output;

    public DarlingMcpBlockingBudgetLiveTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task GetBlocking_Default_StaysUnderResponseBudget_WithThirtyRealisticRows()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live get_blocking budget test.");

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
            var baseTime = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-60);
            var blockedSql = BuildQueryText("UPDATE dbo.Posts SET Score = Score + 1, LastActivityDate = GETUTCDATE()", approxLength: 650);
            var blockingSql = BuildQueryText("SELECT p.Id, p.Title, p.Body, u.DisplayName FROM dbo.Posts p JOIN dbo.Users u ON u.Id = p.OwnerUserId", approxLength: 820);

            for (var i = 0; i < 30; i++)
            {
                var t = baseTime.AddMinutes(i);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, wait_resource, lock_mode,
     blocked_status, blocked_isolation_level, blocked_log_used, blocked_transaction_count,
     blocked_client_app, blocked_host_name, blocked_login_name, blocked_sql_text,
     blocking_status, blocking_isolation_level, blocking_client_app, blocking_host_name, blocking_login_name,
     blocking_sql_text, blocked_transaction_name, blocking_transaction_name,
     blocked_last_tran_started, blocking_last_tran_started, blocked_last_batch_started, blocking_last_batch_started,
     blocked_last_batch_completed, blocking_last_batch_completed, blocked_priority, blocking_priority,
     blocked_process_report_xml, contentious_object)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, "StackOverflow",
                    100 + i, 0, 50 + (i % 5), 0, 5000L + i * 137, "KEY: 6:72057594057849856 (3a1c2b4e5f6a)", "X",
                    "suspended", "READ COMMITTED", 4096L, 1,
                    ".Net SqlClient Data Provider", "APPSERVER01", "CONTOSO\\svc_app", blockedSql + $" /* row {i} */",
                    "running", "READ COMMITTED", "Microsoft SQL Server Management Studio", "DBASERVER02", "CONTOSO\\dba_erik",
                    blockingSql + $" /* row {i} */", "user_transaction", "user_transaction",
                    t, t, t, t,
                    t, t, 0, 0,
                    "<blocked-process-report/>", "dbo.Posts");
            }

            var defaultJson = await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(defaultJson, ServerName, "events");
            JsonAssert.Contains($"\"events_returned\": {DarlingMcpBlockingTools.DefaultLimit}", defaultJson);
            JsonAssert.Contains("\"truncated\": true", defaultJson);

            /* #4198: BEFORE this lane's fix, 30 planted rows at this realistic-but-modest text width (well
               under the old 2000-char cap, so neither text column was even truncated) measured 89,096 bytes
               — 2.7x McpResponseBudget.DefaultBytes — because the row's other ~37 fields (isolation levels,
               client app/host/login for both sides, six last-tran/last-batch stamps) are most of the weight
               at the default 30-row page, not the two text columns alone. */
            JsonAssert.Contains("\"blocked_sql_text_truncated\": true", defaultJson);
            JsonAssert.Contains("\"blocking_sql_text_truncated\": true", defaultJson);

            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
            _output.WriteLine($"get_blocking default call: {defaultBytes:N0} bytes (budget {McpResponseBudget.DefaultBytes:N0}), 30 planted rows, blocked/blocking SQL text {blockedSql.Length:N0}/{blockingSql.Length:N0} chars.");
            Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_blocking's default call is {defaultBytes:N0} bytes, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            using var defaultParsed = JsonDocument.Parse(defaultJson);
            var defaultFirstBlocked = defaultParsed.RootElement.GetProperty("events")[0].GetProperty("blocked_sql_text").GetString();
            Assert.NotNull(defaultFirstBlocked);
            Assert.True(defaultFirstBlocked!.Length < blockedSql.Length,
                "the default call's blocked_sql_text should be a preview shorter than the planted text.");

            /* full_text opts back into the whole text on both columns. */
            var fullJson = await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName, limit: 30, full_text: true);
            Assert.DoesNotContain("_truncated\": true", fullJson, StringComparison.Ordinal);
            using var fullParsed = JsonDocument.Parse(fullJson);
            var fullFirstBlocked = fullParsed.RootElement.GetProperty("events")[0].GetProperty("blocked_sql_text").GetString();
            /* Newest first: events[0] is row 29 (latest event_time), not row 0 — just check the text is
               whole (starts with the planted clause, un-truncated) rather than hardcode which row sorts first. */
            Assert.NotNull(fullFirstBlocked);
            Assert.StartsWith(blockedSql, fullFirstBlocked!, StringComparison.Ordinal);
            Assert.True(fullFirstBlocked!.Length > blockedSql.Length, "the full_text call should include the ' /* row N */' suffix past the planted clause.");

            /* Naming one incident (dedup_key) returns the whole text even without full_text. */
            var firstKey = defaultParsed.RootElement.GetProperty("events")[0].GetProperty("dedup_key").GetString();
            Assert.False(string.IsNullOrEmpty(firstKey));

            var byKeyJson = await DarlingMcpBlockingTools.GetBlocking(postgres, ServerName, dedup_key: firstKey, full_text: false);
            Assert.DoesNotContain("_truncated\": true", byKeyJson, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Builds a T-SQL string near <paramref name="approxLength"/> characters by repeating a
    /// realistic clause, ASCII only so its .NET UTF-16 char length and its UTF-8 byte size stay close.</summary>
    private static string BuildQueryText(string clause, int approxLength)
    {
        var sb = new StringBuilder(clause);
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append($" AND p.Id NOT IN (SELECT TOP (1) Id FROM dbo.Posts WHERE ParentId = {i})");
            i++;
        }

        return sb.ToString();
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

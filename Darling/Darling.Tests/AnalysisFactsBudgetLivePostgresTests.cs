/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9195 on the real store (gated, DARLING_TEST_PG): Darling's get_analysis_facts and compare_analysis answer within the 32,768-byte
/// response budget and say so when they cut, the same as Lite's twin (<c>AnalysisFactsBudgetTests</c>). The store holds one fully
/// collected CXPACKET series and a single delta row for each of many other wait types, so the scored fact list is far past the budget.
/// The tools run end to end through the real service and store, because the cut is applied to the reply the tool builds.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalysisFactsBudgetLivePostgresTests
{
    private const string ServerName = "darling-analysis-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task GetAnalysisFacts_OverTheBudget_IsCutWorstFirst_AndCarriesTheBudgetKeys()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live budget test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var now = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            await PlantWaitAsync(connection, now.AddHours(-40), "OLD_WAIT", 1_000L, ct);
            await PlantWindowAsync(connection, now, 160, ct);

            var service = new DarlingAnalysisService(postgres);
            var result = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4);

            Assert.True(Encoding.UTF8.GetByteCount(result) <= McpResponseBudget.DefaultBytes, "the reply stays inside the response budget");
            using var doc = JsonDocument.Parse(result);
            var root = doc.RootElement;
            var returned = root.GetProperty("facts").GetArrayLength();
            var scored = root.GetProperty("total_facts").GetInt32();
            Assert.True(returned < scored, "the budget cut something");
            Assert.True(root.GetProperty("budget_truncated").GetBoolean());
            Assert.Contains("min_severity", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
            Assert.Equal(scored, root.GetProperty("shown").GetInt32());

            /* What was kept is the worst end of the list: severity never rises down the page. */
            var severities = root.GetProperty("facts").EnumerateArray().Select(f => f.GetProperty("severity").GetDouble()).ToList();
            Assert.Equal(severities.OrderByDescending(s => s), severities);

            /* Under the budget (one source only) the same keys say nothing was cut. */
            var small = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, "config");
            using var smallDoc = JsonDocument.Parse(small);
            Assert.False(smallDoc.RootElement.GetProperty("budget_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, smallDoc.RootElement.GetProperty("budget_note").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task CompareAnalysis_OverTheBudget_IsCut_AndCarriesTheBudgetKeys()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live budget test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var now = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            await PlantWaitAsync(connection, now.AddHours(-70), "OLD_WAIT", 1_000L, ct);
            await PlantWindowAsync(connection, now.AddHours(-24), 70, ct);
            await PlantWindowAsync(connection, now, 70, ct);

            var service = new DarlingAnalysisService(postgres);
            var result = await DarlingMcpTools.CompareAnalysis(service, postgres, ServerName, 4, 28);

            Assert.True(Encoding.UTF8.GetByteCount(result) <= McpResponseBudget.DefaultBytes, "the reply stays inside the response budget");
            using var doc = JsonDocument.Parse(result);
            var root = doc.RootElement;
            Assert.True(root.GetProperty("facts").GetArrayLength() < root.GetProperty("facts_total").GetInt32(), "the budget cut something");
            Assert.True(root.GetProperty("budget_truncated").GetBoolean());
            Assert.Contains("hours_back", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
            Assert.True(root.GetProperty("truncated").GetBoolean(), "the older key still says it");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>One fully collected series for CXPACKET, and one delta row inside the window for each of the other wait types.</summary>
    private static async Task PlantWindowAsync(NpgsqlConnection connection, DateTime windowEnd, int waitTypeCount, CancellationToken ct)
    {
        var start = windowEnd.AddHours(-4);
        await PlantWaitAsync(connection, start, "CXPACKET", 0L, ct);
        for (var i = 1; i <= 16; i++)
            await PlantWaitAsync(connection, start.AddMinutes(15 * i), "CXPACKET", 56_250L, ct);
        for (var i = 0; i < waitTypeCount - 1; i++)
            await PlantWaitAsync(connection, windowEnd.AddMinutes(-60), $"B9195_WAIT_{i:000}", 20_000 + (i * 1000L), ct);
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 16, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 16;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(
        NpgsqlConnection connection, DateTime at, string waitType, long deltaWaitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, 0)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(deltaWaitMs > 0 ? 10L : 0L);
        command.Parameters.AddWithValue(deltaWaitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

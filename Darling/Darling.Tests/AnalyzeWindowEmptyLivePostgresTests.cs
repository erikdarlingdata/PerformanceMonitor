/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// Gated (DARLING_TEST_PG) proof of #3524: <c>analyze_server</c> must not answer "all metrics are
/// within normal ranges" when the analysis window collected NOTHING. The pipeline's data-span gate
/// measures LIFETIME history, so a server whose collection died (broken credential, unreachable
/// target) still passes it — and the zero-facts pass used to return a bare <c>[]</c> that the tool
/// rendered as a true-negative all-clear. Both sides of the distinction are asserted, on the REAL
/// 24h gate rather than a zeroed one, because the bug lives precisely in the gap between "enough
/// history" and "an empty window".
/// </summary>
[Collection("live-postgres")]
public sealed class AnalyzeWindowEmptyLivePostgresTests
{
    private const string ServerName = "darling-analyze-window-empty-e2e";
    private const string StaleWait = "WE3524_STALE_WAIT";
    private const string BenignWait = "WE3524_BENIGN_WAIT";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ADeadCollectorWindow_IsUnavailable_AndAWindowWithFactsKeepsTheAllClear()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live window-empty analysis test.");

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
            var service = new DarlingAnalysisService(postgres);

            /* ── the dead-collector shape: 25 hours of history (the real 24h gate PASSES) whose
               newest row is five hours old, so the tool's default 4h window holds nothing. */
            await PlantWaitAsync(connection, Naive(DateTime.UtcNow.AddHours(-30)), StaleWait, 60_000L, ct);
            await PlantWaitAsync(connection, Naive(DateTime.UtcNow.AddHours(-5)), StaleWait, 60_000L, ct);

            var deadWindow = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(deadWindow))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());

                /* The #2506 persistence disclosure survives the new envelope — an anchored
                   empty-window run still owes the caller that context, and this unanchored one
                   reports the ordinary answer. */
                Assert.True(doc.RootElement.GetProperty("hints").GetProperty("persisted").GetBoolean());
            }
            Assert.Contains("get_collection_health", deadWindow, StringComparison.Ordinal);
            Assert.Contains("NOT an all-clear", deadWindow, StringComparison.Ordinal);
            Assert.DoesNotContain("within normal ranges", deadWindow, StringComparison.Ordinal);

            /* The service says WHICH kind of nothing this was: the window, not the lifetime span. */
            Assert.NotNull(service.WindowEmptyMessage);
            Assert.Null(service.InsufficientDataMessage);

            /* ── the control, and the reason the fix is a distinction rather than a rewording: the
               SAME server with one benign wait INSIDE the window has facts to score, finds nothing
               wrong, and keeps the genuine true-negative all-clear. */
            await PlantWaitAsync(connection, Naive(DateTime.UtcNow.AddMinutes(-30)), BenignWait, 100L, ct);

            var healthyWindow = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(healthyWindow))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }
            Assert.Contains("All metrics are within normal ranges", healthyWindow, StringComparison.Ordinal);
            Assert.Null(service.WindowEmptyMessage);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Naive-UTC, the Kind every timestamp column in this store is bound with.</summary>
    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task RegisterServerAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantWaitAsync(
        NpgsqlConnection connection, DateTime at, string waitType, long waitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(50L);
        command.Parameters.AddWithValue(waitMs);
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

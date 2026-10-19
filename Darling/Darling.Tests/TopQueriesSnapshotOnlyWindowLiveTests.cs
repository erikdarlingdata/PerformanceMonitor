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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6127 against live PostgreSQL: <c>get_top_queries_by_cpu</c> answered <c>unavailable</c> ("No query stats available for the
/// specified time range.") for a window whose raw <c>query_stats</c> held snapshots, because the ranking reads only rows that carry
/// a measured delta and every snapshot in the window was a first sighting or an unmoved repeat (zero deltas). The read must say
/// what it found: snapshots stored, none carrying a delta, and which window does.
/// </summary>
[Collection("live-postgres")]
public sealed class TopQueriesSnapshotOnlyWindowLiveTests
{
    private const string ServerName = "darling-top-queries-snapshot-only-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task SnapshotsWithoutADelta_AreNotCalledNoData_AndNameTheWindowThatAnswers()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live snapshot-only-window test.");

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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            /* Sparse raw rows inside a four-hour window: first sightings (interval 0, zero deltas) and unmoved repeats (a real
               interval, zero deltas). Nothing here can produce a CPU figure. */
            await PlantAsync(connection, ct, now.AddHours(-3), "0xFIRST1", interval: 0, executions: 0, cpu: 0, elapsed: 0);
            await PlantAsync(connection, ct, now.AddHours(-2), "0xFIRST2", interval: 0, executions: 0, cpu: 0, elapsed: 0);
            await PlantAsync(connection, ct, now.AddMinutes(-70), "0xFIRST2", interval: 60, executions: 0, cpu: 0, elapsed: 0);
            await PlantAsync(connection, ct, now.AddMinutes(-20), "0xFIRST3", interval: 0, executions: 0, cpu: 0, elapsed: 0);

            /* The last time the plan cache moved a counter: ten hours back, outside the four-hour window. */
            var measuredAt = now.AddHours(-10);
            await PlantAsync(connection, ct, measuredAt, "0xBUSY", interval: 60, executions: 40, cpu: 8_000_000, elapsed: 9_000_000);

            var asOf = now.ToString("o");
            var answer = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 4, as_of: asOf)).RootElement;

            Assert.Equal("empty", answer.GetProperty("status").GetString());
            var message = answer.GetProperty("message").GetString();
            Assert.DoesNotContain("No query stats available", message, StringComparison.Ordinal);
            Assert.Contains("none carries a measured delta", message, StringComparison.Ordinal);
            var hints = answer.GetProperty("hints");
            Assert.Equal(4, hints.GetProperty("snapshots_in_window").GetInt64());
            Assert.Equal(0, hints.GetProperty("snapshots_with_measured_delta").GetInt64());
            Assert.Equal(
                DateTime.SpecifyKind(measuredAt, DateTimeKind.Utc).ToString("o"),
                hints.GetProperty("newest_measured_delta_at").GetString());
            Assert.Equal(10, hints.GetProperty("suggested_hours_back").GetInt32());

            /* The window the answer names does return the busy plan, so the suggestion is one that works. */
            var wider = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(
                postgres, ServerName, hints.GetProperty("suggested_hours_back").GetInt32() + 1, as_of: asOf)).RootElement;
            Assert.False(wider.TryGetProperty("status", out var widerStatus) && widerStatus.GetString() == "empty");
            Assert.Equal("0xBUSY", wider.GetProperty("queries")[0].GetProperty("query_hash").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AWindowWithNoSnapshotsAtAll_StaysUnavailable()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live snapshot-only-window test.");

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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);
            await PlantAsync(connection, ct, now.AddHours(-10), "0xBUSY", interval: 60, executions: 40, cpu: 8_000_000, elapsed: 9_000_000);

            var answer = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 4, as_of: now.ToString("o"))).RootElement;

            Assert.Equal("unavailable", answer.GetProperty("status").GetString());
            Assert.Equal("No query stats available for the specified time range.", answer.GetProperty("message").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task PlantAsync(NpgsqlConnection connection, CancellationToken ct, DateTime at, string queryHash, int interval, long executions, long cpu, long elapsed) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, "StackOverflow", queryHash, "0xPLANHASH", "0xSQLH" + queryHash, "0xPLANH" + queryHash, "SELECT 1", executions, cpu, elapsed, 0L, 1, 1, interval);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

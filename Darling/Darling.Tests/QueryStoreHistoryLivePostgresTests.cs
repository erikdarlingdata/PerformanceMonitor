/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
/// #5234: get_query_store_query_history folds a query's snapshots plan by plan. An interval collected three times with
/// a growing execution_count counts once, at its latest snapshot (#1841); two plans come back, the points are ordered by
/// collection time, and a wrong query_id answers <c>empty</c>. Fixed anchors only.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreHistoryLivePostgresTests
{
    private const string ServerName = "darling-qs-history-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string Db = "QsHistoryDb";

    /// <summary>Fixed anchor, naive UTC as the store keeps it.</summary>
    private static readonly DateTime Anchor = new DateTime(2026, 3, 4, 5, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    /* as_of is fixed, so the window is [End - 24h, End] and no clock decides what is in it. */
    private static readonly DateTime End = Anchor.AddHours(2);

    [Fact]
    public async Task TheHistory_CountsAnIntervalOnce_FoldsPerPlan_AndAWrongQueryIdIsEmpty()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live history test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(connection, ct);
            var interval = Anchor.AddMinutes(-30);
            /* Plan 7, one interval collected three times: 10, 25, 40 executions. Only the 40 counts. */
            await InsertAsync(connection, Anchor.AddMinutes(1), 4242, 7, 1, interval, 10, 100_000, 40_000, ct);
            await InsertAsync(connection, Anchor.AddMinutes(2), 4242, 7, 1, interval, 25, 100_000, 40_000, ct);
            await InsertAsync(connection, Anchor.AddMinutes(3), 4242, 7, 1, interval, 40, 100_000, 40_000, ct);
            /* Plan 9, a second interval of its own. */
            await InsertAsync(connection, Anchor.AddMinutes(4), 4242, 9, 2, Anchor, 5, 20_000, 10_000, ct);

            using var doc = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(
                postgres, Db, 4242, ServerName, hours_back: 24, as_of: End.ToString("o", CultureInfo.InvariantCulture), cancellationToken: ct));
            var root = doc.RootElement;
            var plans = root.GetProperty("plans");
            Assert.Equal(2, plans.GetArrayLength());
            Assert.Equal(7, plans[0].GetProperty("plan_id").GetInt64());
            Assert.Equal(40, plans[0].GetProperty("execution_count").GetInt64());
            Assert.Equal(100.0, plans[0].GetProperty("avg_duration_ms").GetDouble(), 6);
            Assert.Equal(4000.0, plans[0].GetProperty("total_duration_ms").GetDouble(), 6);
            Assert.Equal(1600.0, plans[0].GetProperty("total_cpu_ms").GetDouble(), 6);
            Assert.Equal(9, plans[1].GetProperty("plan_id").GetInt64());
            Assert.Equal(5, plans[1].GetProperty("execution_count").GetInt64());

            var points = root.GetProperty("points");
            Assert.Equal(2, points.GetArrayLength());
            Assert.Equal(40, points[0].GetProperty("execution_count").GetInt64());
            Assert.True(string.CompareOrdinal(points[0].GetProperty("collection_time").GetString(), points[1].GetProperty("collection_time").GetString()) < 0);
            Assert.False(root.GetProperty("points_truncated").GetBoolean());

            Assert.Equal("empty", StatusOf(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(
                postgres, Db, 9999, ServerName, hours_back: 24, as_of: End.ToString("o", CultureInfo.InvariantCulture), cancellationToken: ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET server_name = EXCLUDED.server_name, display_name = EXCLUDED.display_name,
    is_enabled = TRUE, modified_date = EXCLUDED.modified_date;", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime collectionTime, long queryId, long planId, long intervalId, DateTime firstExecution,
        long executions, double avgDurationUs, double avgCpuUs, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, runtime_stats_interval_id,
     first_execution_time, last_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, is_forced_plan)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, $11, $12, FALSE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(intervalId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(firstExecution, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(executions);
        command.Parameters.AddWithValue(avgDurationUs);
        command.Parameters.AddWithValue(avgCpuUs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

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
/// collection time, and a wrong query_id answers <c>empty</c>. Anchored a few hours back from the run, with as_of fixed against it.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreHistoryLivePostgresTests
{
    private const string ServerName = "darling-qs-history-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string Db = "QsHistoryDb";

    /// <summary>
    /// Naive UTC as the store keeps it, a few hours back from the run so a retention or compression job (which works on
    /// chunks older than the raw window) cannot touch the seed; as_of is fixed against it, so no clock decides what the window holds.
    /// </summary>
    private static readonly DateTime Anchor = DateTime.SpecifyKind(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-3), DateTimeKind.Unspecified);

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
            /* The oldest row is 1 minute past the anchor, hours after the window's start (End - 24 h): the window is cut. */
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(Anchor.AddMinutes(1).ToString("yyyy-MM-dd'T'HH:mm", CultureInfo.InvariantCulture), root.GetProperty("effective_start").GetString()![..16]);

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

    /// <summary>#4966: an empty answer over a window the store does not reach back to carries the window notice in its hints.</summary>
    [Fact]
    public async Task AnEmptyAnswer_OverACutWindow_CarriesTheWindowNotice()
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
            await InsertAsync(connection, Anchor.AddMinutes(1), 4242, 7, 1, Anchor, 5, 20_000, 10_000, ct);

            using var doc = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(
                postgres, Db, 9999, ServerName, hours_back: 24, as_of: End.ToString("o", CultureInfo.InvariantCulture), cancellationToken: ct));
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            var hints = doc.RootElement.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.False(string.IsNullOrEmpty(hints.GetProperty("effective_start").GetString()));
            Assert.False(string.IsNullOrEmpty(hints.GetProperty("truncation_note").GetString()));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5234: many plans times many intervals stays under <see cref="McpResponseBudget.DefaultBytes"/>. The newest points are kept
    /// (so the last interval is present and the first is not), and the plans list is capped by total duration.
    /// </summary>
    [Fact]
    public async Task ManyPlansAndIntervals_StayUnderTheResponseBudget_AndKeepTheNewestPoints()
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
            const int planCount = 30;
            const int intervalCount = 40;
            for (var plan = 1; plan <= planCount; plan++)
            {
                for (var i = 0; i < intervalCount; i++)
                {
                    var intervalStart = Anchor.AddMinutes(-15 * i);
                    /* Plan N's average is N * 1.234567 ms, so the plans differ and the cut keeps the longest. */
                    await InsertAsync(connection, intervalStart.AddMinutes(14), 4242, plan, i + 1, intervalStart, 10 + i, plan * 1234.567891, plan * 987.654321, ct);
                }
            }

            var json = await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(
                postgres, Db, 4242, ServerName, hours_back: 24, as_of: End.ToString("o", CultureInfo.InvariantCulture), cancellationToken: ct);
            var bytes = System.Text.Encoding.UTF8.GetByteCount(json);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var points = root.GetProperty("points");
            Assert.True(bytes < McpResponseBudget.DefaultBytes, $"the answer is {bytes} bytes for {points.GetArrayLength()} points; the budget is {McpResponseBudget.DefaultBytes}.");
            Assert.True(root.GetProperty("points_truncated").GetBoolean());
            Assert.True(points.GetArrayLength() > 0 && points.GetArrayLength() <= DarlingMcpQueryStoreHistoryTools.MaxPoints);
            Assert.True(root.GetProperty("plans_truncated").GetBoolean());
            Assert.Equal(DarlingMcpQueryStoreHistoryTools.MaxPlans, root.GetProperty("plans").GetArrayLength());
            /* The newest interval is kept, ordered oldest first; the oldest interval is the one that was cut. */
            var times = Enumerable.Range(0, points.GetArrayLength()).Select(n => points[n].GetProperty("collection_time").GetString()!).ToList();
            Assert.Equal(times.OrderBy(t => t, StringComparer.Ordinal), times);
            var newest = Anchor.AddMinutes(14).ToString("yyyy-MM-dd'T'HH:mm", CultureInfo.InvariantCulture);
            Assert.StartsWith(newest, times[^1]);
            Assert.DoesNotContain(times, t => t.StartsWith(Anchor.AddMinutes(-15 * (intervalCount - 1) + 14).ToString("yyyy-MM-dd'T'HH:mm", CultureInfo.InvariantCulture), StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #5300: Query Store keeps a plan's Regular and Aborted runs, and an availability group's replicas, in separate rows
    /// that share a collection time. The series draws ONE value per plan and instant, execution-weighted, and the plan table
    /// folds the same rows the same way. A window under 24 hours never consults the interval table, so this reads raw.
    /// </summary>
    [Fact]
    public async Task OutcomesAndReplicas_AtOneInstant_GiveOnePointPerPlan_ExecutionWeighted()
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
            var at = Anchor.AddMinutes(10);
            /* Plan 7: 1000 Regular runs at 5 ms and 2 Aborted (timeout) runs at 30 s, one interval, one instant. */
            await InsertAsync(connection, at, 4242, 7, 1, Anchor, 1000, 5_000, 2_000, ct, outcome: "Regular");
            await InsertAsync(connection, at, 4242, 7, 1, Anchor, 2, 30_000_000, 30_000_000, ct, outcome: "Aborted");
            /* Plan 9: two replicas, one instant. */
            await InsertAsync(connection, at, 4242, 9, 2, Anchor, 10, 100_000, 10_000, ct, replicaRole: "primary");
            await InsertAsync(connection, at, 4242, 9, 2, Anchor, 30, 200_000, 30_000, ct, replicaRole: "secondary");
            /* A later instant of plan 7, a lone row. */
            await InsertAsync(connection, Anchor.AddMinutes(25), 4242, 7, 3, Anchor.AddMinutes(15), 40, 6_000, 2_000, ct);

            using var doc = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(
                postgres, Db, 4242, ServerName, hours_back: 12, as_of: End.ToString("o", CultureInfo.InvariantCulture), cancellationToken: ct));
            var points = doc.RootElement.GetProperty("points");
            Assert.Equal(3, points.GetArrayLength());
            Assert.Equal(new long[] { 7, 9, 7 }, Enumerable.Range(0, 3).Select(n => points[n].GetProperty("plan_id").GetInt64()));
            Assert.Equal(1002, points[0].GetProperty("execution_count").GetInt64());
            Assert.Equal((1000 * 5.0 + 2 * 30_000.0) / 1002, points[0].GetProperty("avg_duration_ms").GetDouble(), 6);
            Assert.Equal(40, points[1].GetProperty("execution_count").GetInt64());
            Assert.Equal((10 * 100.0 + 30 * 200.0) / 40, points[1].GetProperty("avg_duration_ms").GetDouble(), 6);
            Assert.Equal(6.0, points[2].GetProperty("avg_duration_ms").GetDouble(), 6);
            /* The plan table folds the same rows, so a point and its plan's row agree about what was combined. */
            var plan7 = doc.RootElement.GetProperty("plans")[0];
            Assert.Equal(1042, plan7.GetProperty("execution_count").GetInt64());

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
        long executions, double avgDurationUs, double avgCpuUs, System.Threading.CancellationToken ct,
        string outcome = "Regular", string? replicaRole = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, runtime_stats_interval_id,
     first_execution_time, last_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, is_forced_plan,
     execution_type_desc, replica_role)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10, $11, $12, FALSE, $13, $14)", connection);
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
        command.Parameters.AddWithValue(outcome);
        command.Parameters.AddWithValue((object?)replicaRole ?? DBNull.Value);
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

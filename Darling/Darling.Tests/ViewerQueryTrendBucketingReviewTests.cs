/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4234 (lane T2d): the viewer's raw-tier query-duration, procedure-duration and execution-count trends are
/// now BUCKETED, the PR #4304 pattern <c>ViewerTrendBucketingReviewTests.ViewerTrendBucketWidthSqlTests</c>
/// established for Wait/Perfmon. Ruling item 6's source-check half: each statement carries a bucket width and
/// the singleton-detection columns. No live store needed.
/// </summary>
public sealed class ViewerQueryTrendBucketWidthSqlTests
{
    [Fact]
    public void QueryDurationTrendSql_CarriesABucketWidth()
    {
        /* server/start/end/filter = $1-$4, so the width is $5 — proven once by hand against the pre-#4234
           text: DurationTrendRouting.BuildRawTrendSql's per-collection output (what this field built before)
           has no date_bin anywhere. */
        Assert.Contains("date_bin(CAST($5 AS integer) * INTERVAL '1 minute'", ViewerDataService.QueryDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, ViewerDataService.QueryDurationTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedureDurationTrendSql_CarriesABucketWidth()
    {
        Assert.Contains("date_bin(CAST($5 AS integer) * INTERVAL '1 minute'", ViewerDataService.ProcedureDurationTrendSql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, ViewerDataService.ProcedureDurationTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ExecutionCountTrendSql_CarriesABucketWidth()
    {
        Assert.Contains("date_bin(CAST($5 AS integer) * INTERVAL '1 minute'", ViewerDataService.ExecutionCountTrendSql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, ViewerDataService.ExecutionCountTrendSql, StringComparison.Ordinal);
    }

    [Fact]
    public void AllThree_ProjectFirstCollectionTimeAndCollectionCount()
    {
        /* The singleton-detection columns ReadBucketedDurationTrendAsync reads: MIN(collection_time) and
           COUNT(*) per bucket, mirroring DurationTrendRouting.BuildBucketedRawTrendSql's own columns. */
        foreach (var sql in new[] { ViewerDataService.QueryDurationTrendSql, ViewerDataService.ProcedureDurationTrendSql, ViewerDataService.ExecutionCountTrendSql })
        {
            Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
            Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// #4234 (lane T2d) ruling-item-6 live pins, gated on DARLING_TEST_PG: the 7-day row-budget cap and singleton
/// point-equality, for the three raw-tier trends this lane bucketed. Shares the serialized "live-postgres"
/// collection, own server_id so it cannot collide with a concurrent lane's rows.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerQueryTrendBucketingLiveTests
{
    private const int ServerId = -424243;
    private const string ServerName = "viewer-query-trend-bucketing-e2e";

    [Fact]
    public async Task QueryDurationTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            await BulkSeedQueryStatsAsync(connection, ct, CollectionIdGenerator.Next() * 1_000_000L, start, end);

            var series = await viewer.GetQueryDurationTrendAsync(ServerId, start, end, null, end, ct);

            Assert.True(series.Points.Count > 0 && series.Points.Count <= TrendBudget.Chart.AutoPoints,
                $"{series.Points.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
        });
    }

    [Fact]
    public async Task ProcedureDurationTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            await BulkSeedProcedureStatsAsync(connection, ct, CollectionIdGenerator.Next() * 1_000_000L, start, end);

            var series = await viewer.GetProcedureDurationTrendAsync(ServerId, start, end, null, end, ct);

            Assert.True(series.Points.Count > 0 && series.Points.Count <= TrendBudget.Chart.AutoPoints,
                $"{series.Points.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
        });
    }

    [Fact]
    public async Task ExecutionCountTrend_SevenDayWindow_ReturnsAtMostBudgetRows()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            await BulkSeedQueryStatsAsync(connection, ct, CollectionIdGenerator.Next() * 1_000_000L, start, end);

            var series = await viewer.GetExecutionCountTrendAsync(ServerId, start, end, null, end, ct);

            Assert.True(series.Points.Count > 0 && series.Points.Count <= TrendBudget.Chart.AutoPoints,
                $"{series.Points.Count} rows over a budget of {TrendBudget.Chart.AutoPoints}");
        });
    }

    [Fact]
    public async Task QueryDurationTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            /* Off the minute grid (:37 seconds) — proves a singleton bucket is stamped at its own collection
               time, not the date_bin grid line, and that the bucketed read's rate equals the pre-#4234
               per-collection read's rate when nothing merges. */
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertQueryStatsRowAsync(connection, ct, t1, executions: 10, elapsedMs: 3000, sampleIntervalSeconds: 300);
            await InsertQueryStatsRowAsync(connection, ct, t2, executions: 20, elapsedMs: 6000, sampleIntervalSeconds: 300);
            await InsertQueryStatsRowAsync(connection, ct, t3, executions: 30, elapsedMs: 9000, sampleIntervalSeconds: 300);

            var series = await viewer.GetQueryDurationTrendAsync(ServerId, t1.AddMinutes(-1), t3.AddMinutes(1), null, t3.AddMinutes(1), ct);

            Assert.Equal(new[] { t1, t2, t3 }, series.Points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(10.0, series.Points[0].Value, precision: 6);
            Assert.Equal(20.0, series.Points[1].Value, precision: 6);
            Assert.Equal(30.0, series.Points[2].Value, precision: 6);
        });
    }

    [Fact]
    public async Task ProcedureDurationTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertProcedureStatsRowAsync(connection, ct, t1, executions: 10, elapsedMs: 3000, sampleIntervalSeconds: 300);
            await InsertProcedureStatsRowAsync(connection, ct, t2, executions: 20, elapsedMs: 6000, sampleIntervalSeconds: 300);
            await InsertProcedureStatsRowAsync(connection, ct, t3, executions: 30, elapsedMs: 9000, sampleIntervalSeconds: 300);

            var series = await viewer.GetProcedureDurationTrendAsync(ServerId, t1.AddMinutes(-1), t3.AddMinutes(1), null, t3.AddMinutes(1), ct);

            Assert.Equal(new[] { t1, t2, t3 }, series.Points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(10.0, series.Points[0].Value, precision: 6);
            Assert.Equal(20.0, series.Points[1].Value, precision: 6);
            Assert.Equal(30.0, series.Points[2].Value, precision: 6);
        });
    }

    [Fact]
    public async Task ExecutionCountTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndValuesUnchanged()
    {
        await RunAsync(async (connection, viewer, ct) =>
        {
            var t1 = new DateTime(2026, 3, 10, 9, 0, 37);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertQueryStatsRowAsync(connection, ct, t1, executions: 10, elapsedMs: 3000, sampleIntervalSeconds: 300);
            await InsertQueryStatsRowAsync(connection, ct, t2, executions: 20, elapsedMs: 6000, sampleIntervalSeconds: 300);
            await InsertQueryStatsRowAsync(connection, ct, t3, executions: 30, elapsedMs: 9000, sampleIntervalSeconds: 300);

            var series = await viewer.GetExecutionCountTrendAsync(ServerId, t1.AddMinutes(-1), t3.AddMinutes(1), null, t3.AddMinutes(1), ct);

            Assert.Equal(new[] { t1, t2, t3 }, series.Points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(10.0 / 300.0, series.Points[0].Value, precision: 6);
            Assert.Equal(20.0 / 300.0, series.Points[1].Value, precision: 6);
            Assert.Equal(30.0 / 300.0, series.Points[2].Value, precision: 6);
        });
    }

    /* ───────────────────────────── plumbing ───────────────────────────── */

    private static async Task RunAsync(Func<NpgsqlConnection, ViewerDataService, System.Threading.CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-trend-bucketing tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var viewer = new ViewerDataService(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await body(connection, viewer, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task InsertQueryStatsRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime t, long executions, long elapsedMs, int sampleIntervalSeconds) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     delta_execution_count, delta_elapsed_time, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'AppDb', '0xQTB', $5, $6, $7)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, executions, elapsedMs * 1000, sampleIntervalSeconds);

    private static async Task InsertProcedureStatsRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime t, long executions, long elapsedMs, int sampleIntervalSeconds) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
     delta_execution_count, delta_elapsed_time, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'AppDb', 'dbo', 'usp_Trend', $5, $6, $7)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, executions, elapsedMs * 1000, sampleIntervalSeconds);

    /// <summary>One row per minute, generated server-side (the wait/perfmon <c>BulkSeedWaitAsync</c> idiom):
    /// enough collections over 7 days to force the auto-bucketer off 1-minute width, so the budget-cap tests
    /// exercise real merging rather than the singleton path the point-equality tests cover.</summary>
    private static async Task BulkSeedQueryStatsAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long baseId, DateTime start, DateTime end) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     delta_execution_count, delta_elapsed_time, sample_interval_seconds)
SELECT $1 + row_number() OVER (), g, $2, $3, 'AppDb', '0xQTB', 10, 100000, 60
FROM generate_series($4::timestamp, $5::timestamp, interval '1 minute') AS g",
            baseId, ServerId, ServerName, start, end);

    private static async Task BulkSeedProcedureStatsAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long baseId, DateTime start, DateTime end) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
     delta_execution_count, delta_elapsed_time, sample_interval_seconds)
SELECT $1 + row_number() OVER (), g, $2, $3, 'AppDb', 'dbo', 'usp_Trend', 10, 100000, 60
FROM generate_series($4::timestamp, $5::timestamp, interval '1 minute') AS g",
            baseId, ServerId, ServerName, start, end);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "query_stats", "procedure_stats", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}

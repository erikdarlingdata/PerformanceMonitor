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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8073: the three duration trends publish <c>execution_count</c> as the executions the point's rate was built from,
/// summed over the bucket, beside <c>executions_per_second</c>. A caller summing <c>execution_count</c> over a window
/// gets the executions that ran, as the field name and the shared <c>aggregate_note</c> ("Counts are summed across the
/// bucket") say, and the rate keeps its own key.
/// </summary>
[Collection("live-postgres")]
public sealed class DurationTrendExecutionCountLiveTests
{
    private const int ServerId = -949573;
    private const string ServerName = "duration-trend-execution-count";
    private const string Db = "AppDb";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheThreeTrends_PublishTheBucketsSummedExecutions_AsExecutionCount()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live duration-trend execution-count test.");

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

            /*
                ── query_stats: two collections five minutes apart, each 600 executions over a stored 300 s interval ──
                The store's first collection keeps its stored interval and the second is rated over its gap, so both are
                rated at 2/sec. One 60-minute bucket holds both: its rate is 1,200 over 600 s (2/sec) and its count is the
                1,200 executions that ran, not the rate truncated to an integer (2).

                Buckets are aligned to the clock, not to the window, so the window is anchored with as_of at the end of
                the previous clock hour and the collections sit at :10 and :15 of that hour: one aligned bucket holds both
                whatever the wall clock reads. Anchored at now, a run a few minutes past the hour would put the two
                collections either side of the hour boundary and read two points.
            */
            var hour = DateTime.UtcNow.AddHours(-1);
            hour = new DateTime(hour.Year, hour.Month, hour.Day, hour.Hour, 0, 0, DateTimeKind.Utc);
            var asOf = hour.AddMinutes(59).ToString("o");
            await SeedQueryAsync(connection, ct, hour.AddMinutes(10), executions: 600, elapsedUs: 300_000, intervalSeconds: 300);
            await SeedQueryAsync(connection, ct, hour.AddMinutes(15), executions: 600, elapsedUs: 300_000, intervalSeconds: 300);

            var queries = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, hours_back: 1, as_of: asOf, bucket_minutes: 60)).RootElement;
            Assert.Equal("raw", queries.GetProperty("source").GetString());
            var queryTrend = queries.GetProperty("trend");
            Assert.Equal(1, queryTrend.GetArrayLength());
            Assert.Equal(0, queries.GetProperty("unrated_collections").GetInt32());
            Assert.Equal(2.0, queryTrend[0].GetProperty("executions_per_second").GetDouble(), 9);
            Assert.Equal(1200, queryTrend[0].GetProperty("execution_count").GetInt64());

            /* ── procedure_stats: the same two collections through the procedure sibling ── */
            await SeedProcedureAsync(connection, ct, hour.AddMinutes(10), executions: 600, elapsedUs: 300_000, intervalSeconds: 300);
            await SeedProcedureAsync(connection, ct, hour.AddMinutes(15), executions: 600, elapsedUs: 300_000, intervalSeconds: 300);

            var procedures = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, ServerName, hours_back: 1, as_of: asOf, bucket_minutes: 60)).RootElement;
            Assert.Equal("raw", procedures.GetProperty("source").GetString());
            var procedureTrend = procedures.GetProperty("trend");
            Assert.Equal(1, procedureTrend.GetArrayLength());
            Assert.Equal(0, procedures.GetProperty("unrated_collections").GetInt32());
            Assert.Equal(2.0, procedureTrend[0].GetProperty("executions_per_second").GetDouble(), 9);
            Assert.Equal(1200, procedureTrend[0].GetProperty("execution_count").GetInt64());

            /*
                ── Query Store: two runtime intervals an hour apart, each with its stored end, so each is rated over its
                own 3,600 s ── 10 executions in the first and 25 in the second. The point's count is the interval's
                executions; its rate, below one per second on both, truncates to 0 and would read as an idle server.
            */
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var intervalA = baseNow.AddHours(-3);
            var intervalB = baseNow.AddHours(-2);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(110), intervalId: 41, intervalStart: intervalA, executions: 10, avgDurationUs: 2000);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(50), intervalId: 42, intervalStart: intervalB, executions: 25, avgDurationUs: 3000);

            var store = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, ServerName, hours_back: 6)).RootElement;
            var storeTrend = store.GetProperty("trend");
            Assert.Equal(2, storeTrend.GetArrayLength());
            Assert.Equal(0, store.GetProperty("unrated_points").GetInt32());
            Assert.Equal(10d / 3600d, storeTrend[0].GetProperty("executions_per_second").GetDouble(), 9);
            Assert.Equal(10, storeTrend[0].GetProperty("execution_count").GetInt64());
            Assert.Equal(25d / 3600d, storeTrend[1].GetProperty("executions_per_second").GetDouble(), 9);
            Assert.Equal(25, storeTrend[1].GetProperty("execution_count").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task SeedQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs, int intervalSeconds) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     delta_execution_count, delta_elapsed_time, delta_worker_time, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            Db, "0xEXECCOUNT", executions, elapsedUs, elapsedUs / 2, intervalSeconds);

    private static async Task SeedProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs, int intervalSeconds) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
     delta_execution_count, delta_elapsed_time, delta_worker_time, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            Db, "dbo", "usp_ExecCount", executions, elapsedUs, elapsedUs / 2, intervalSeconds);

    private static async Task SeedQueryStoreAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, long intervalId,
        DateTime intervalStart, long executions, long avgDurationUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            Db, 7L, 9L, "Regular", executions, avgDurationUs, intervalId,
            DarlingMcpTestData.Naive(intervalStart), DarlingMcpTestData.Naive(intervalStart.AddHours(1)));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM procedure_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_store_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Viewer's Query Store slicer keeps each interval's newest snapshot without sorting every raw row in the window. The
/// old statement numbered the rows with <c>ROW_NUMBER() OVER (PARTITION BY the interval key ORDER BY collection_time DESC,
/// execution_count DESC)</c>, an external merge of the whole slice (1.72 s of 2.05 s on a 30-day seed). The new one is a hash
/// aggregate. Two facts are asserted, neither a timing: the plan has no Sort over the raw rows (the old statement, run here as
/// the oracle, does), and the new statement returns the same buckets as the old one on rows that tie on collection_time, on
/// both collection_time and execution_count, on a NULL execution_count, and on microseconds. Skips without <c>DARLING_TEST_PG</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreSlicerNoSortLiveTests
{
    private const string ServerName = "darling-qs-slicer-nosort-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store slicer plan tests.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const int BulkQueries = 18;
    private const int BulkIntervals = 720;
    private const int BulkSnapshots = 12;
    private const int BulkRows = BulkQueries * BulkIntervals * BulkSnapshots;

    /// <summary>The statement before #8549, kept as the oracle: the same bounds and bucket, the ROW_NUMBER dedupe.</summary>
    private const string OldSlicerSql = $$"""
        WITH deduped AS (
            SELECT
                collection_time, interval_start_time_utc, query_id, execution_count, avg_cpu_time_us, avg_duration_us,
                avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   COALESCE(interval_start_time_utc, collection_time) <= $3
            AND   collection_time >= $2 - {{QueryStoreIntervalWide.CollectionFloorSlackSql}}
            AND   collection_time <= $3 + {{QueryStoreIntervalWide.CollectionCeilingSql}}
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        )
        SELECT
            date_bin(CAST($5 AS integer) * INTERVAL '1 minute', COALESCE(interval_start_time_utc, collection_time), {{TrendBucketSql.OriginSql}}) AS bucket,
            COUNT(DISTINCT query_id) AS query_count,
            COALESCE(SUM(CAST(avg_cpu_time_us AS double precision) * execution_count), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(CAST(avg_duration_us AS double precision) * execution_count), 0) / 1000.0 AS total_duration_ms,
            COALESCE(SUM(CAST(avg_logical_io_reads AS double precision) * execution_count), 0) AS total_reads,
            COALESCE(SUM(CAST(avg_logical_io_writes AS double precision) * execution_count), 0) AS total_writes,
            COALESCE(SUM(CAST(avg_physical_io_reads AS double precision) * execution_count), 0) AS total_physical_reads
        FROM deduped
        WHERE rn = 1
        GROUP BY 1
        ORDER BY bucket
        """;

    private static async Task<NpgsqlConnection> OpenAndResetAsync(string cs, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        return connection;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.query_store_stats WHERE server_id = {ServerId}; DELETE FROM collect.servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The bulk shape: 18 queries over 720 hourly intervals, each interval re-collected 12 times (every 5 minutes) with a count that grows.</summary>
    private static async Task SeedBulkAsync(NpgsqlConnection connection, DateTime firstHour, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
                  first_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
                  replica_role, runtime_stats_interval_id, interval_start_time_utc)
              SELECT $4::bigint + row_number() OVER (), $3::timestamp + i * INTERVAL '1 hour' + (c + 1) * INTERVAL '5 minutes' + INTERVAL '20 seconds',
                     $1::integer, $2::text, 'AppDb', q, q, 'Regular', $3::timestamp + i * INTERVAL '1 hour', c + 1, q * 10 + c, q * 20 + c, q, c, 1,
                     'PRIMARY', i, $3::timestamp + i * INTERVAL '1 hour'
              FROM generate_series(1, 18) AS qs(q), generate_series(0, 719) AS iv(i), generate_series(0, 11) AS cs(c)", connection);
        command.CommandTimeout = 120;
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(firstHour));
        command.Parameters.AddWithValue(7_700_000_000_000_000L);
        Assert.Equal(BulkRows, await command.ExecuteNonQueryAsync(ct));
    }

    private static async Task InsertTieRowAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, long queryId, long? executionCount, long avgCpu, DateTime? intervalStart, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time, " +
            "execution_count, avg_cpu_time_us, avg_duration_us, replica_role, runtime_stats_interval_id, interval_start_time_utc) " +
            "VALUES ($1, $2, $3, $4, 'TieDb', $5, $5, 'Regular', $2, $6, $7, $7, 'PRIMARY', $8, $9)",
            collectionId, DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName, queryId, (object?)executionCount ?? DBNull.Value, avgCpu,
            intervalStart is null ? DBNull.Value : (object)queryId, intervalStart is null ? DBNull.Value : (object)DarlingMcpTestData.Naive(intervalStart.Value));

    /// <summary>Six queries in 'TieDb', each a case the old ORDER BY decides. Every case but the legacy one sits in the interval that starts at <paramref name="hour"/>.</summary>
    private static async Task SeedTiesAsync(NpgsqlConnection connection, DateTime hour, CancellationToken ct)
    {
        /* A time with microseconds, and its neighbour one microsecond later (ticks are 100 ns, so ten ticks). */
        var t = hour.AddMinutes(10).AddTicks(1_234_560);
        var tPlusMicro = t.AddTicks(10);
        long id = 7_800_000_000_000_000L;

        /* 1001: tied collection_time, counts 5 and 9: the higher count wins (cpu 200 x 9). */
        await InsertTieRowAsync(connection, id++, t, 1001, 9, 200, hour, ct);
        await InsertTieRowAsync(connection, id++, t, 1001, 5, 100, hour, ct);
        /* 1002: tied on both, same averages: one row counts, once (300 x 7). */
        await InsertTieRowAsync(connection, id++, t, 1002, 7, 300, hour, ct);
        await InsertTieRowAsync(connection, id++, t, 1002, 7, 300, hour, ct);
        /* 1003: a NULL count and a count of 3 at one time: a descending sort puts the NULL first, so the NULL row wins and adds nothing. */
        await InsertTieRowAsync(connection, id++, t, 1003, 3, 500, hour, ct);
        await InsertTieRowAsync(connection, id++, t, 1003, null, 400, hour, ct);
        /* 1004: a microsecond apart, the later row has the lower count: the later time wins (700 x 4). */
        await InsertTieRowAsync(connection, id++, t, 1004, 20, 600, hour, ct);
        await InsertTieRowAsync(connection, id++, tPlusMicro, 1004, 4, 700, hour, ct);
        /* 1005: five minutes apart, the later row has the lower count: the time decides before the count (900 x 2). */
        await InsertTieRowAsync(connection, id++, t, 1005, 50, 800, hour, ct);
        await InsertTieRowAsync(connection, id++, t.AddMinutes(5), 1005, 2, 900, hour, ct);
        /* 1006: a legacy row, no interval id or start: it buckets on its collection_time, and the later row wins (1100 x 6). */
        await InsertTieRowAsync(connection, id++, hour.AddMinutes(30), 1006, 2, 1000, null, ct);
        await InsertTieRowAsync(connection, id++, hour.AddMinutes(31), 1006, 6, 1100, null, ct);
    }

    private static NpgsqlCommand ExplainCommand(NpgsqlConnection connection, string sql, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(start));
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(end));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.AddWithValue(SlicerBuckets.MinutesFor(start, end));
        return command;
    }

    /// <summary>The most rows any Sort node of the plan produced, summed over its loops (a parallel worker's sort counts too).</summary>
    private static long LargestSortRows(JsonElement node)
    {
        long largest = 0;
        if (node.TryGetProperty("Node Type", out var type) && (type.GetString() ?? "").Contains("Sort", StringComparison.Ordinal))
        {
            var rows = node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble();
            largest = (long)rows;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                largest = Math.Max(largest, LargestSortRows(child));
            }
        }

        return largest;
    }

    private static async Task<long> LargestSortRowsAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        using var command = ExplainCommand(connection, sql, start, end);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        return LargestSortRows(document.RootElement[0].GetProperty("Plan"));
    }

    private static void AssertSameBuckets(List<TimeSliceBucket> expected, List<TimeSliceBucket> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].BucketTime, actual[i].BucketTime);
            Assert.Equal(expected[i].SessionCount, actual[i].SessionCount);
            Assert.Equal(expected[i].TotalCpu, actual[i].TotalCpu, 6);
            Assert.Equal(expected[i].TotalElapsed, actual[i].TotalElapsed, 6);
            Assert.Equal(expected[i].TotalReads, actual[i].TotalReads, 6);
            Assert.Equal(expected[i].TotalWrites, actual[i].TotalWrites, 6);
            Assert.Equal(expected[i].TotalPhysicalReads, actual[i].TotalPhysicalReads, 6);
        }
    }

    [Fact]
    public async Task TheSlicerKeepsTheNewestSnapshotPerInterval_WithoutSortingTheRawRows_AndReturnsTheOldBuckets()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var start = end.AddDays(-30);
            var endHour = new DateTime(end.Year, end.Month, end.Day, end.Hour, 0, 0, DateTimeKind.Utc);
            await SeedBulkAsync(connection, endHour.AddHours(-(BulkIntervals - 1)), ct);
            var tieHour = endHour.AddDays(-2);
            await SeedTiesAsync(connection, tieHour, ct);
            using (var analyze = new NpgsqlCommand("ANALYZE query_store_stats", connection) { CommandTimeout = 120 })
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            /* The shape: the old statement sorts at least a quarter of the raw rows (the seed is big enough to force it), the new one sorts nothing
               near that, whatever the plan's other sorts (the bucket grouping sorts one row per interval at most). */
            var oldSort = await LargestSortRowsAsync(connection, OldSlicerSql, start, end, ct);
            var newSort = await LargestSortRowsAsync(connection, ViewerDataService.QueryStoreSlicerSql, start, end, ct);
            Assert.True(oldSort >= BulkRows / 4, $"the old statement's largest sort held {oldSort} rows; the seed no longer forces a sort over the raw rows.");
            Assert.True(newSort < BulkRows / 4, $"the new statement's largest sort held {newSort} rows; it sorts the raw rows again.");

            /* The rows: the new statement's buckets equal the old statement's over the whole window. */
            await using var viewer = new ViewerDataService(cs!);
            var oldBuckets = await viewer.ReadQueryStatsSlicerAsync(OldSlicerSql, ServerId, start, end, null, ct);
            var newBuckets = await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, cancellationToken: ct);
            Assert.NotEmpty(newBuckets);
            AssertSameBuckets(oldBuckets, newBuckets);

            /* And the winners, stated outright on the tie rows alone (a database filter): 200 x 9 + 300 x 7 + 0 + 700 x 4 + 900 x 2 + 1100 x 6
               microseconds of cpu, six queries, each in one bar. The old statement agrees (checked above on the whole window). */
            var ties = await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, new[] { "TieDb" }, ct);
            Assert.Equal(6, ties.Sum(b => b.SessionCount));
            Assert.Equal((1800 + 2100 + 0 + 2800 + 1800 + 6600) / 1000.0, ties.Sum(b => b.TotalCpu), 6);
            var oldTies = await viewer.ReadQueryStatsSlicerAsync(OldSlicerSql, ServerId, start, end, new[] { "TieDb" }, ct);
            AssertSameBuckets(oldTies, ties);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}

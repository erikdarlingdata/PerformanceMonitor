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

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres, then works entirely inside it, so it cannot race live collection.

   #8549: the Viewer's Query Store by Duration slicer, read on a store shaped like a field store: query_store_stats on 6 hour
   chunks, the older chunks compressed (segmented by server_id), the newest raw, each interval collected twice. The old statement
   numbered the rows with ROW_NUMBER, filtered rn = 1 (a default 0.5 percent guess for the planner) and then sorted EVERY deduped
   row by bucket for COUNT(DISTINCT query_id): on the field store that was a 1.77 million row external sort of 128 MB. The new
   statement keeps the same winners with DISTINCT ON and counts the distinct queries per bucket with a window over rows that
   already arrive sorted by query_id, so the bucket step is a hash aggregate. Two facts are asserted, neither a timing: the
   bucket step above the dedupe writes no temp files and sorts at most one row per bucket, and the buckets equal the old
   statement's on rows that tie. Skips without DARLING_TEST_PG or without TimescaleDB. */
public sealed class QueryStoreSlicerCompressedLiveTests
{
    private const int ServerId = -85491;
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store slicer plan test.";

    private const int Queries = 40;
    private const int Intervals = 96;

    /// <summary>The first seeded hour, on a six hour chunk boundary: the seed fills 17 chunks of six hours, the oldest 12 compressed, as on the field store.</summary>
    private static readonly DateTime FirstHour = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The statement before #8549, kept as the oracle: the same bounds and bucket, the ROW_NUMBER dedupe and COUNT(DISTINCT).</summary>
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

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The bulk shape: 40 queries over 96 hourly intervals, each collected twice (open at 20 minutes, closed at 62 minutes, the closing fetch in the next hour).</summary>
    private static async Task SeedBulkAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
                  first_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
                  replica_role, runtime_stats_interval_id, interval_start_time_utc)
              SELECT 8_000_000 + row_number() OVER (),
                     $2::timestamp + i * INTERVAL '1 hour' + CASE WHEN c = 0 THEN INTERVAL '20 minutes' ELSE INTERVAL '62 minutes' END + (q % 50) * INTERVAL '1 second',
                     $1::integer, 'qs-slicer-compressed', 'Db' || (q % 4), q, q, 'Regular',
                     $2::timestamp + i * INTERVAL '1 hour' + (q % 40) * INTERVAL '1 minute',
                     CASE WHEN c = 0 THEN 1 + (q * 7 + i) % 500 ELSE 2 + (q * 7 + i) % 500 + q % 5 END,
                     100 + (q * 37 + i * 11) % 100000, 1000 + (q * 53 + i * 17) % 900000, (q * 91 + i) % 50000, (q * 13 + i) % 300, (q * 7 + i) % 800,
                     'PRIMARY', i, $2::timestamp + i * INTERVAL '1 hour'
              FROM generate_series(1, 40) AS qs(q), generate_series(0, 95) AS iv(i), generate_series(0, 1) AS cs(c)", connection);
        command.CommandTimeout = 120;
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(FirstHour);
        Assert.Equal(Queries * Intervals * 2, await command.ExecuteNonQueryAsync(ct));
    }

    /// <summary>
    /// Seven queries in 'TieDb' for the interval that starts at <paramref name="hour"/>, each a case the dedupe's ORDER BY decides
    /// (the query id is <paramref name="set"/> * 1000 + the case number, so every set is its own seven queries).
    /// </summary>
    private static async Task SeedTiesAsync(NpgsqlConnection connection, DateTime hour, int set, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
                  first_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
                  replica_role, runtime_stats_interval_id, interval_start_time_utc)
              SELECT $3::bigint + n, ct, $1::integer, 'qs-slicer-compressed', 'TieDb', $4::bigint * 1000 + q, 1, 'Regular', $2::timestamp, ec, cpu, cpu + 10, rd, rd, rd,
                     'PRIMARY', CASE WHEN q = 6 THEN NULL ELSE 9000 + $4::integer END, CASE WHEN q = 6 THEN NULL ELSE $2::timestamp END
              FROM (VALUES
                /* 1: tied collection_time, counts 9 and 5: the higher count wins (cpu 200 x 9). */
                (1, 1, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', 9::bigint, 200::bigint, 5::bigint),
                (2, 1, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', 5, 100, 1),
                /* 2: tied on both, same averages: one row counts, once (300 x 7). */
                (3, 2, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', 7, 300, 4),
                (4, 2, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', 7, 300, 4),
                /* 3: a NULL count and a count of 3 at one time: a descending sort puts the NULL first, so the NULL row wins and adds nothing. */
                (5, 3, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', 3, 500, 8),
                (6, 3, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', NULL, 400, 9),
                /* 4: a microsecond apart, the later row has the lower count: the later time wins (700 x 4). */
                (7, 4, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', 20, 600, 1),
                (8, 4, $2::timestamp + INTERVAL '10 minutes 1.234561 seconds', 4, 700, 2),
                /* 5: five minutes apart, the later row has the lower count: the time decides before the count (900 x 2). */
                (9, 5, $2::timestamp + INTERVAL '10 minutes 1.234560 seconds', 50, 800, 3),
                (10, 5, $2::timestamp + INTERVAL '15 minutes 1.234560 seconds', 2, 900, 4),
                /* 6: a legacy query, no interval id or start, collected in two different hour buckets: the later copy wins and the bar is its hour (1100 x 6). */
                (11, 6, $2::timestamp + INTERVAL '50 minutes', 2, 1000, 5),
                (12, 6, $2::timestamp + INTERVAL '70 minutes', 6, 1100, 6),
                /* 7: a copy at 58 minutes and one at 63 minutes, on the two sides of the hour (a chunk boundary in two of the three sets): the later one wins (1300 x 13). */
                (13, 7, $2::timestamp + INTERVAL '58 minutes', 11, 1200, 7),
                (14, 7, $2::timestamp + INTERVAL '63 minutes', 13, 1300, 8)
              ) AS t(n, q, ct, ec, cpu, rd)", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(hour);
        command.Parameters.AddWithValue(8_100_000L + set * 100L);
        command.Parameters.AddWithValue(set);
        Assert.Equal(14, await command.ExecuteNonQueryAsync(ct));
    }

    private static NpgsqlCommand ExplainCommand(NpgsqlConnection connection, string sql, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(end);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.AddWithValue(SlicerBuckets.MinutesFor(start, end));
        return command;
    }

    private static string NodeType(JsonElement node) => node.GetProperty("Node Type").GetString() ?? "";

    private static IEnumerable<JsonElement> Children(JsonElement node)
        => node.TryGetProperty("Plans", out var children) ? children.EnumerateArray() : Enumerable.Empty<JsonElement>();

    private static long TempWrittenBlocks(JsonElement node)
        => node.TryGetProperty("Temp Written Blocks", out var blocks) ? blocks.GetInt64() : 0;

    /// <summary>The deepest Unique or WindowAgg node: the dedupe, whichever way the statement writes it (the new one has a second WindowAgg above it).</summary>
    private static JsonElement DeepestDedupe(JsonElement node, int depth, ref int bestDepth, ref JsonElement best)
    {
        if ((NodeType(node) == "Unique" || NodeType(node) == "WindowAgg") && depth > bestDepth)
        {
            bestDepth = depth;
            best = node;
        }

        foreach (var child in Children(node))
        {
            DeepestDedupe(child, depth + 1, ref bestDepth, ref best);
        }

        return best;
    }

    /// <summary>The nodes above the dedupe: everything outside its subtree.</summary>
    private static void WalkAbove(JsonElement node, JsonElement dedupe, List<JsonElement> above)
    {
        if (node.Equals(dedupe))
        {
            return;
        }

        above.Add(node);
        foreach (var child in Children(node))
        {
            WalkAbove(child, dedupe, above);
        }
    }

    private sealed record BucketStep(long TempWrittenBlocks, long LargestSortRows, bool HashedAggregate);

    /// <summary>What the bucket step above the dedupe did: temp blocks it wrote, the most rows one Sort node there handled, and whether an Aggregate there ran hashed.</summary>
    private static async Task<BucketStep> BucketStepAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        using var command = ExplainCommand(connection, sql, start, end);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement[0].GetProperty("Plan");

        var bestDepth = -1;
        var dedupe = default(JsonElement);
        DeepestDedupe(root, 0, ref bestDepth, ref dedupe);
        Assert.True(bestDepth >= 0, "the plan has no Unique or WindowAgg node, so the dedupe is not where this test looks for it: " + json);

        var above = new List<JsonElement>();
        WalkAbove(root, dedupe, above);
        var largestSort = above
            .Where(n => NodeType(n) == "Sort")
            .Select(n => (long)(n.GetProperty("Actual Rows").GetDouble() * n.GetProperty("Actual Loops").GetDouble()))
            .DefaultIfEmpty(0)
            .Max();
        var hashed = above.Any(n => NodeType(n) == "Aggregate" && n.TryGetProperty("Strategy", out var s) && s.GetString() == "Hashed");
        return new BucketStep(TempWrittenBlocks(root) - TempWrittenBlocks(dedupe), largestSort, hashed);
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
    public async Task TheSlicerBucketStep_OnACompressedStore_WritesNoTempFilesAndHashes_AndReturnsTheOldBuckets()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipWhen(!scratch.TimescaleEnabled, "The store has no TimescaleDB, so there are no compressed chunks to read.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The field store's shape: 6 hour chunks, segmented by server_id like every collector table. */
        await ExecAsync(connection, "SELECT create_hypertable('collect.query_store_stats', by_range('collection_time', INTERVAL '6 hours'), if_not_exists => true, migrate_data => true)", ct);
        await ExecAsync(connection, "SELECT set_chunk_time_interval('collect.query_store_stats', INTERVAL '6 hours')", ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql("collect.query_store_stats"), ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        await SeedBulkAsync(connection, ct);
        await SeedTiesAsync(connection, FirstHour.AddHours(83), 1, ct);   // raw chunks, the copies on the two sides of a chunk boundary
        await SeedTiesAsync(connection, FirstHour.AddHours(10), 2, ct);   // a compressed chunk
        await SeedTiesAsync(connection, FirstHour.AddHours(5), 3, ct);    // compressed chunks, the copies on the two sides of a chunk boundary
        await ExecAsync(connection, $"SELECT compress_chunk(c) FROM show_chunks('collect.query_store_stats', older_than => TIMESTAMP '{FirstHour.AddHours(72):yyyy-MM-dd HH:mm:ss}') AS c", ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_stats", ct);

        using (var census = new NpgsqlCommand(
            "SELECT count(*) FILTER (WHERE is_compressed), count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'query_store_stats'", connection))
        using (var reader = await census.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(12L, reader.GetInt64(0));
            Assert.Equal(17L, reader.GetInt64(1));
        }

        /* The statement's own plan, as the field planned it: serial, and a work_mem small enough that the old statement's
           sort of the deduped rows (3,840 rows) cannot stay in memory, however small the seed. */
        await ExecAsync(connection, "SET work_mem = '64kB'", ct);
        await ExecAsync(connection, "SET max_parallel_workers_per_gather = 0", ct);
        var start = FirstHour;
        var end = FirstHour.AddHours(Intervals);

        var oldStep = await BucketStepAsync(connection, OldSlicerSql, start, end, ct);
        var newStep = await BucketStepAsync(connection, ViewerDataService.QueryStoreSlicerSql, start, end, ct);
        Assert.True(oldStep.TempWrittenBlocks > 0 && oldStep.LargestSortRows >= Queries * Intervals,
            $"the old statement's bucket step wrote {oldStep.TempWrittenBlocks} temp blocks and sorted {oldStep.LargestSortRows} rows; the seed no longer forces the sort of the deduped rows.");

        /* One row per bucket at most: the final ORDER BY of the buckets, nothing that sorts the deduped rows. */
        Assert.True(newStep.TempWrittenBlocks == 0, $"the new statement's bucket step wrote {newStep.TempWrittenBlocks} temp blocks above the dedupe.");
        Assert.True(newStep.LargestSortRows <= Intervals + 2, $"the new statement sorts {newStep.LargestSortRows} rows above the dedupe; it sorts the deduped rows again.");
        Assert.True(newStep.HashedAggregate, "the new statement's bucket aggregate is not hashed.");

        /* The rows: the new statement's buckets equal the old statement's over the whole window. */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var oldBuckets = await viewer.ReadQueryStatsSlicerAsync(OldSlicerSql, ServerId, start, end, null, ct);
        var newBuckets = await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, cancellationToken: ct);
        Assert.Equal(Intervals, newBuckets.Count);
        AssertSameBuckets(oldBuckets, newBuckets);

        /* The winners, stated outright on the tie rows alone. Per set, in the interval's own hour: six queries (1 to 5 and 7) and
           (1800 + 2100 + 0 + 2800 + 1800 + 16900) = 25,400 microseconds of cpu, the NULL count adding nothing; and in the next hour
           the legacy query's later copy: one query and 1100 x 6 = 6,600 microseconds. The old statement agrees. */
        var ties = await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, new[] { "TieDb" }, ct);
        var oldTies = await viewer.ReadQueryStatsSlicerAsync(OldSlicerSql, ServerId, start, end, new[] { "TieDb" }, ct);
        AssertSameBuckets(oldTies, ties);
        Assert.Equal(6, ties.Count);
        foreach (var offset in new[] { 5, 10, 83 })
        {
            var inHour = ties.Single(b => b.BucketTime == FirstHour.AddHours(offset));
            Assert.Equal(6, inHour.SessionCount);
            Assert.Equal(25.4, inHour.TotalCpu, 6);
            var nextHour = ties.Single(b => b.BucketTime == FirstHour.AddHours(offset + 1));
            Assert.Equal(1, nextHour.SessionCount);
            Assert.Equal(6.6, nextHour.TotalCpu, 6);
        }
    }
}

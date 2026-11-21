/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */

/// <summary>
/// #9147: <see cref="QueryStatsCollectionSummary.FloorMoveSql"/> runs inside every query_stats batch's transaction. In the normal case (no raw
/// collection lacks a summary row) it must cost one probe of the newest chunk, not a read of the server's whole raw history, because the batch's
/// command timeout cancels it and the batch's raw rows roll back with it. The plan test runs the generic plan the service's prepared command gets.
/// </summary>
public sealed class QueryStatsFloorMoveLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the #9147 floor move live tests.";
    private const int ServerId = 91_470_001;

    private const string PlantRaw =
        @"INSERT INTO collect.query_stats
              (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
               query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
               delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
               min_dop, max_dop, sample_interval_seconds)
          VALUES ($1, $2, $3, 'floorserver', 'Db0', 'q0', '0xP', '0xS', '0xL', 'SELECT 1', decode('00', 'hex'), 10, 5, 5, 100,
                  0, 0, 1, 5, 1, 5, 1, 1, 1800)";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter is DateTime at ? Naive(at) : new NpgsqlParameter { Value = parameter });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static NpgsqlParameter Naive(DateTime at) =>
        new() { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(at, DateTimeKind.Unspecified) };

    private static async Task<long> CountSummaryAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM collect.query_stats_collection WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(ServerId);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> RunFloorMoveAsync(NpgsqlConnection connection, DateTime batch, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(QueryStatsCollectionSummary.FloorMoveSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(Naive(batch));
        return await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "floorserver", ct);
        return connection;
    }

    /// <summary>
    /// Twelve days of raw collections every 30 minutes, 20 rows each, with a summary row per collection and database, so the server has about a
    /// dozen one-day chunks. The chunks older than three days are compressed, as the store's policy leaves them. The newest collection is
    /// <paramref name="lastCollection"/>. A second server's rows share the same chunks, so the probe has to skip them.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime lastCollection, CancellationToken ct)
    {
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        /* Migration leaves the tables plain (#7152): the service's conversion step makes query_stats a hypertable, and the store compresses its old chunks. */
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(QueryStatsCollector.Instance), ct);
        await ExecAsync(connection,
            @"INSERT INTO collect.query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              SELECT 91_470_000_000_000 + s.k * 1000 + r.n + sv.id, $1::timestamp - s.k * interval '30 minutes', sv.id, 'floorserver',
                     'Db' || (r.n % 3), 'q' || (r.n % 7), '0xP', '0xS', '0xL', 'SELECT 1', decode('00', 'hex'), 10, 5, 5, 100, 0, 0, 1, 5, 1, 5, 1, 1, 1800
              FROM generate_series(0, 575) AS s (k)
              CROSS JOIN generate_series(1, 20) AS r (n)
              CROSS JOIN (VALUES (91470001), (91470002)) AS sv (id)", ct, lastCollection);
        await ExecAsync(connection,
            @"INSERT INTO collect.query_stats_collection (server_id, collection_time, database_name, n, sum_elapsed, sum_exec, max_interval)
              SELECT server_id, collection_time, database_name, count(*), sum(delta_elapsed_time)::bigint, sum(delta_execution_count)::bigint,
                     max(sample_interval_seconds)
              FROM collect.query_stats
              WHERE server_id = $1
              GROUP BY server_id, collection_time, database_name", ct, ServerId);
        await ExecAsync(connection,
            @"SELECT compress_chunk(c, if_not_compressed => true)
              FROM show_chunks('collect.query_stats', older_than => $1::timestamp) AS c", ct, lastCollection.AddDays(-3));
        await ExecAsync(connection, "ANALYZE collect.query_stats; ANALYZE collect.query_stats_collection", ct);
    }

    /// <summary>The chunk relations (and their compressed twins) a plan actually read: a branch that never ran has zero loops.</summary>
    private static void CollectChunks(JsonElement node, HashSet<string> read)
    {
        if (node.TryGetProperty("Relation Name", out var relation)
            && node.TryGetProperty("Actual Loops", out var loops) && loops.GetDouble() > 0
            && relation.GetString() is { } name
            && (name.StartsWith("_hyper_", StringComparison.Ordinal) || name.StartsWith("compress_hyper_", StringComparison.Ordinal)))
        {
            read.Add(name);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                CollectChunks(child, read);
            }
        }
    }

    [Fact]
    public async Task TheNormalCase_ReadsOnlyTheNewestChunk_InTheGenericPlan_OfAServerWithManyChunks()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(scratch, ct);
            var last = LiveClock.AnchoredEnd().AddMinutes(-30);
            await SeedAsync(connection, last, ct);
            var batch = last.AddMinutes(30);

            await using var transaction = await connection.BeginTransactionAsync(ct);
            await ExecAsync(connection, "SET LOCAL plan_cache_mode = force_generic_plan", ct);
            await ExecAsync(connection, "PREPARE floor_move (integer, timestamp) AS " + QueryStatsCollectionSummary.FloorMoveSql, ct);
            await using var explain = new NpgsqlCommand(
                string.Create(CultureInfo.InvariantCulture, $"EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE floor_move ({ServerId}, '{batch:yyyy-MM-dd HH:mm:ss}')"), connection);
            using var document = JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
            await transaction.RollbackAsync(ct);

            var read = new HashSet<string>(StringComparer.Ordinal);
            CollectChunks(document.RootElement[0].GetProperty("Plan"), read);
            Assert.InRange(read.Count, 1, 2);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ARawCollectionWithNoSummaryRow_MovesTheFloor_AndTheNormalCaseDeletesNothing()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(scratch, ct);
            var last = LiveClock.AnchoredEnd().AddMinutes(-30);
            await SeedAsync(connection, last, ct);
            var summaryRows = await CountSummaryAsync(connection, ct);
            Assert.True(summaryRows > 0);

            /* Normal case: the newest raw row is the newest summary row, so a batch one step later finds nothing to move. */
            Assert.Equal(0, await RunFloorMoveAsync(connection, last.AddMinutes(30), ct));
            Assert.Equal(summaryRows, await CountSummaryAsync(connection, ct));

            /* A batch at the newest collection's own time takes that row as the anchor, and the raw rows before it all have summary rows. */
            Assert.Equal(0, await RunFloorMoveAsync(connection, last, ct));
            Assert.Equal(summaryRows, await CountSummaryAsync(connection, ct));

            /* Hole: a raw collection an older build wrote with no summary row, between the anchor and the next batch. */
            await ExecAsync(connection, PlantRaw, ct, 91470999000001L, last.AddMinutes(10), ServerId);
            Assert.Equal(summaryRows, await RunFloorMoveAsync(connection, last.AddMinutes(30), ct));
            Assert.Equal(0, await CountSummaryAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AServerWithNoSummaryRow_AndASummaryRowWithNoRawRowBeforeTheBatch_DeleteNothing()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(scratch, ct);
            var at = LiveClock.AnchoredEnd().AddHours(-1);

            /* No summary row and no raw row: a NULL anchor and an empty prior. */
            Assert.Equal(0, await RunFloorMoveAsync(connection, at, ct));

            /* A raw row before the batch but no summary row at all (a NULL anchor): the first batch of a server never moves a floor. */
            await ExecAsync(connection, PlantRaw, ct, 91470999000002L, at.AddMinutes(-20), ServerId);
            Assert.Equal(0, await RunFloorMoveAsync(connection, at, ct));

            /* A summary row, and no raw row of the server before the batch (an empty prior): nothing to move the floor over. */
            await ExecAsync(connection,
                @"INSERT INTO collect.query_stats_collection (server_id, collection_time, database_name, n, sum_elapsed, sum_exec, max_interval)
                  VALUES ($1, $2, 'Db0', 1, 5, 10, 1800)", ct, ServerId, at.AddMinutes(-40));
            Assert.Equal(0, await RunFloorMoveAsync(connection, at.AddMinutes(-30), ct));
            Assert.Equal(1, await CountSummaryAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}

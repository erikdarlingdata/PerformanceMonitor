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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none is [Collection("live-postgres")]. */

/// <summary>
/// #9284, V294: the corrected daily Query Store rollup compresses ordered by <c>query_hash</c> then <c>bucket</c>, as the hourly rollup has since V279.
/// A chunk compressed in that order carries min/max metadata on <c>query_hash</c>, so a read of one hash can skip every batch whose range excludes it.
/// A chunk compressed in the bucket order carries none, so the same read has to open every batch. A fresh store gets the order from the builder, and a
/// store compressed before it existed is moved to it by the rung, with its compressed chunks kept as they were.
/// </summary>
public sealed class QueryStoreDailyCompressionOrderRungTests
{
    private const string View = TimescaleSupport.QueryStoreStatsCorrectedDailyView;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == 294);

    private const string OrderSql = @"
SELECT string_agg(cs.attname || CASE WHEN cs.orderby_asc THEN ' ASC' ELSE ' DESC' END, ', ' ORDER BY cs.orderby_column_index)
FROM timescaledb_information.continuous_aggregates AS ca
JOIN timescaledb_information.compression_settings AS cs
    ON cs.hypertable_schema = ca.materialization_hypertable_schema
   AND cs.hypertable_name = ca.materialization_hypertable_name
WHERE ca.view_schema = 'collect'
  AND ca.view_name = '" + View + @"'
  AND cs.orderby_column_index IS NOT NULL";

    private const string HistorySql =
        "SELECT count(*) FROM collect.aggregate_compression_order_history WHERE view_name = '" + View + "' AND compress_orderby = 'query_hash, bucket DESC' AND outcome = 'applied'";

    /* The hash a read looks for. It is on both servers and sorts in the middle of the hashes the seed writes. */
    private const string Hash = "7e6f6c4b2f0e4d6e8f1a2b3c4d5e6f70";

    [Fact]
    public async Task AStoreCompressedInTheBucketOrder_IsMovedToTheHashOrder_AndOnlyChunksCompressedAfterwardsLetAOneHashReadSkipBatches()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V294 live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* The ladder runs before the aggregate exists, so the rung finds nothing to move and records nothing. */
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "V294 moves a continuous aggregate's order, which needs TimescaleDB.");
            Assert.Equal(0L, await ScalarAsync(connection, $"SELECT count(*) FROM collect.aggregate_compression_order_history WHERE view_name = '{View}'", ct));

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

            /* A fresh store gets the hash order from the builder. The store under test is then put back to what a store from before V294 has. */
            var fresh = (string?)await ScalarAsync(connection, OrderSql, ct);
            Assert.Equal("query_hash ASC, bucket DESC", fresh);
            await ExecAsync(connection,
                $"ALTER MATERIALIZED VIEW collect.{View} SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id', timescaledb.compress_orderby = 'bucket DESC')", ct);
            Assert.Equal("bucket DESC", (string?)await ScalarAsync(connection, OrderSql, ct));

            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, View, ct);
            Assert.NotNull(materialization);
            var relation = $"\"{materialization.Value.Schema}\".\"{materialization.Value.Name}\"";

            await SeedAsync(connection, relation, LiveClock.Now().Date.AddDays(-20), ct);
            await CompressAllAsync(connection, relation, ct);
            var oldChunks = await CompressedChunksAsync(connection, ct);
            Assert.NotEmpty(oldChunks);

            await RunRungAsync(connection, ct);

            Assert.Equal(fresh, (string?)await ScalarAsync(connection, OrderSql, ct));
            Assert.Equal(1L, await ScalarAsync(connection, HistorySql, ct));

            /* A chunk compressed before the rung keeps its bucket layout, so a one-hash read still has to open every batch of it. */
            foreach (var chunk in oldChunks)
            {
                var (total, candidates) = await BatchesForHashAsync(connection, chunk, firstOrderIsHash: false, ct);
                Assert.True(total > 1, "the seed has to fill more than one batch per segment");
                Assert.Equal(total, candidates);
            }

            /* A chunk compressed after the rung is laid out by hash: the same read can rule most of its batches out. */
            await SeedAsync(connection, relation, LiveClock.Now().Date.AddDays(-120), ct);
            await CompressAllAsync(connection, relation, ct);
            var newChunks = (await CompressedChunksAsync(connection, ct)).Where(c => !oldChunks.Contains(c)).ToList();
            Assert.NotEmpty(newChunks);
            foreach (var chunk in newChunks)
            {
                var (total, candidates) = await BatchesForHashAsync(connection, chunk, firstOrderIsHash: true, ct);
                Assert.True(total > 1, "the seed has to fill more than one batch per segment");
                Assert.InRange(candidates, 1, total / 2);
            }

            /* Re-run (a rewound journal): the same order, a second applied row, and nothing refused. */
            await RunRungAsync(connection, ct);
            Assert.Equal(fresh, (string?)await ScalarAsync(connection, OrderSql, ct));
            Assert.Equal(2L, await ScalarAsync(connection, HistorySql, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.aggregate_compression_order_history WHERE outcome <> 'applied'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task OnPlainPostgres_TheRungMakesTheHistoryIndex_AndRecordsNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V294 live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'", ct));
            const string indexSql = "SELECT count(*) FROM pg_class WHERE relkind = 'i' AND relname = 'ix_aggregate_compression_order_history_view'";
            Assert.Equal(1L, await ScalarAsync(connection, indexSql, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.aggregate_compression_order_history", ct));

            /* A store from before the rung has no index, and the rung gives it one without recording anything. */
            await ExecAsync(connection, "DROP INDEX collect.ix_aggregate_compression_order_history_view", ct);
            Assert.Equal(0L, await ScalarAsync(connection, indexSql, ct));
            await RunRungAsync(connection, ct);
            Assert.Equal(1L, await ScalarAsync(connection, indexSql, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.aggregate_compression_order_history", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* 30,000 rows over three days, 100 hashes, two servers: 5,000 rows per segment per day, so a chunk that holds the days has several batches per segment. */
    private static async Task SeedAsync(NpgsqlConnection connection, string relation, DateTime firstDay, CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand($@"
INSERT INTO {relation} (server_id, server_name, database_name, module_name, query_hash, bucket, execution_count_sum, sample_count)
SELECT 1 + g % 2, 'orderby-9284', 'db', NULL,
       CASE WHEN g % 100 = 0 THEN $2 ELSE md5((g % 100)::text) END,
       $1::timestamp + make_interval(days => g / 10000), 1, 1
FROM generate_series(0, 29999) AS g", connection);
        seed.Parameters.AddWithValue(firstDay);
        seed.Parameters.AddWithValue(Hash);
        seed.CommandTimeout = 120;
        await seed.ExecuteNonQueryAsync(ct);
    }

    private static async Task CompressAllAsync(NpgsqlConnection connection, string relation, CancellationToken ct)
        => Assert.True((long)(await ScalarAsync(connection,
            $"SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('{relation}'::regclass) AS c", ct))! > 0);

    /* The compressed chunk relations in the store, found by the batch row count column every compressed chunk table carries. The catalog's own
       chunk to compressed-chunk link changed shape in TimescaleDB 2.29, and the only chunks this scratch store compresses are the ones the test seeds. */
    private static async Task<List<string>> CompressedChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var chunks = new List<string>();
        await using var command = new NpgsqlCommand(@"
SELECT format('%I.%I', n.nspname, c.relname)
FROM pg_attribute AS a
JOIN pg_class AS c ON c.oid = a.attrelid
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE a.attname = '_ts_meta_count' AND NOT a.attisdropped AND c.relkind = 'r'
ORDER BY c.oid", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(reader.GetString(0));
        }

        return chunks;
    }

    /* How many batches the chunk holds, and how many of them a read of one hash cannot rule out from batch metadata. A chunk with no min/max
       metadata on query_hash rules none out. The metadata column is named by the order's position on older TimescaleDB releases and by the
       column on newer ones, so both are looked up. */
    private static async Task<(long Total, long Candidates)> BatchesForHashAsync(NpgsqlConnection connection, string chunk, bool firstOrderIsHash, CancellationToken ct)
    {
        var total = (long)(await ScalarAsync(connection, $"SELECT count(*) FROM {chunk}", ct))!;
        string? min = null;
        string? max = null;
        var names = new List<string>();
        await using (var attributes = new NpgsqlCommand(
            "SELECT attname FROM pg_attribute WHERE attrelid = $1::regclass AND NOT attisdropped AND attname IN ('_ts_meta_v2_min_query_hash', '_ts_meta_v2_max_query_hash', '_ts_meta_min_1', '_ts_meta_max_1')", connection))
        {
            attributes.Parameters.AddWithValue(chunk);
            await using var reader = await attributes.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                names.Add(reader.GetString(0));
            }
        }

        if (names.Contains("_ts_meta_v2_min_query_hash") && names.Contains("_ts_meta_v2_max_query_hash"))
        {
            min = "_ts_meta_v2_min_query_hash";
            max = "_ts_meta_v2_max_query_hash";
        }
        else if (firstOrderIsHash && names.Contains("_ts_meta_min_1") && names.Contains("_ts_meta_max_1"))
        {
            min = "_ts_meta_min_1";
            max = "_ts_meta_max_1";
        }

        if (min is null || max is null)
        {
            return (total, total);
        }

        await using var candidates = new NpgsqlCommand($"SELECT count(*) FROM {chunk} WHERE {min} <= $1 AND {max} >= $1", connection);
        candidates.Parameters.AddWithValue(Hash);
        return (total, (long)(await candidates.ExecuteScalarAsync(ct))!);
    }

    /* SET LOCAL needs a transaction, as the migration runner gives each rung. */
    private static async Task RunRungAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using var command = new NpgsqlCommand(Rung.Sql, connection, transaction) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}

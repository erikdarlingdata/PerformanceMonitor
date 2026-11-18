/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none is [Collection("live-postgres")]. */

/// <summary>
/// #8570, V279: the corrected Query Store hourly aggregate compresses ordered by <c>query_hash</c> then <c>bucket</c>, so one
/// query's hours sit together in a compressed batch. A fresh store gets the order from the builder; a store compressed before
/// it existed is moved to it by the rung, with compressed chunks already present, and the rung records what it did.
/// </summary>
public sealed class AggregateCompressionOrderRungTests
{
    private const string View = TimescaleSupport.QueryStoreStatsCorrectedHourlyView;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == 279);

    /* An aggregate's order as the long-stable per-column view reports it, read off its materialization hypertable. */
    private static string OrderSqlFor(string view) => @"
SELECT string_agg(cs.attname || CASE WHEN cs.orderby_asc THEN ' ASC' ELSE ' DESC' END, ', ' ORDER BY cs.orderby_column_index)
FROM timescaledb_information.continuous_aggregates AS ca
JOIN timescaledb_information.compression_settings AS cs
    ON cs.hypertable_schema = ca.materialization_hypertable_schema
   AND cs.hypertable_name = ca.materialization_hypertable_name
WHERE ca.view_schema = 'collect'
  AND ca.view_name = '" + view + @"'
  AND cs.orderby_column_index IS NOT NULL";

    private static readonly string OrderSql = OrderSqlFor(View);

    private const string HistorySql =
        "SELECT count(*) FROM collect.aggregate_compression_order_history WHERE view_name = '" + View + "' AND compress_orderby = 'query_hash, bucket DESC' AND outcome = 'applied'";

    [Fact]
    public async Task AStoreCompressedInTheBucketOrder_IsMovedToTheFreshStoresOrder_WithCompressedChunksKept_AndTheRungReRuns()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V279 live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* The ladder runs before the aggregate exists, so the rung finds nothing to move and records nothing. */
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "V279 moves a continuous aggregate's order, which needs TimescaleDB.");
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.aggregate_compression_order_history", ct));

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

            var fresh = (string?)await ScalarAsync(connection, OrderSql, ct);
            Assert.Equal("query_hash ASC, bucket DESC", fresh);
            /* The plain Query Store hourly aggregate keeps the bucket order every other aggregate has. */
            Assert.Equal("bucket DESC", (string?)await ScalarAsync(connection, OrderSqlFor(TimescaleSupport.QueryStoreStatsHourlyView), ct));

            /* A store from before the order: the aggregate compressed by bucket alone, with a chunk compressed that way. */
            await ExecAsync(connection,
                $"ALTER MATERIALIZED VIEW collect.{View} SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id', timescaledb.compress_orderby = 'bucket DESC')", ct);
            Assert.Equal("bucket DESC", (string?)await ScalarAsync(connection, OrderSql, ct));

            var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, View, ct);
            Assert.NotNull(materialization);
            var relation = $"\"{materialization.Value.Schema}\".\"{materialization.Value.Name}\"";
            await using (var seed = new NpgsqlCommand($@"
INSERT INTO {relation} (server_id, server_name, database_name, module_name, query_hash, bucket, execution_count_sum, sample_count)
SELECT 1 + g % 2, 'orderby-8570', 'db', NULL, md5((g % 40)::text), $1::timestamp + make_interval(hours => g / 40), 1, 1
FROM generate_series(0, 1999) AS g", connection))
            {
                seed.Parameters.AddWithValue(LiveClock.Now().Date.AddDays(-20));
                await seed.ExecuteNonQueryAsync(ct);
            }

            var compressed = await ScalarAsync(connection,
                $"SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('{relation}'::regclass) AS c", ct);
            Assert.True((long)compressed! > 0);
            var compressedChunksSql =
                $"SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = '{materialization.Value.Schema}' AND hypertable_name = '{materialization.Value.Name}' AND is_compressed";
            var compressedBefore = (long)(await ScalarAsync(connection, compressedChunksSql, ct))!;

            await RunRungAsync(connection, ct);

            Assert.Equal(fresh, (string?)await ScalarAsync(connection, OrderSql, ct));
            Assert.Equal(1L, await ScalarAsync(connection, HistorySql, ct));
            Assert.Equal(compressedBefore, await ScalarAsync(connection, compressedChunksSql, ct));
            Assert.Equal(2000L, await ScalarAsync(connection, $"SELECT count(*) FROM {relation}", ct));

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
    public async Task OnPlainPostgres_TheRungMakesTheHistoryTable_AndRecordsNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V279 live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.aggregate_compression_order_history", ct));

            await RunRungAsync(connection, ct);
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.aggregate_compression_order_history", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
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

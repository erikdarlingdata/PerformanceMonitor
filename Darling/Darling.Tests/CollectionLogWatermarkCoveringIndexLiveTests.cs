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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */

/// <summary>
/// #9151 (V288): <c>idx_collection_log_watermark</c> gains <c>INCLUDE (rows_collected)</c>. A migrated store has the covering index. A store with the
/// narrow V150 index converges to it, applying the rung twice changes nothing, and the collector row estimate (<c>RawCollectorRowsSql</c>) answers the
/// same with either index. Whether the planner uses the index for the newest-row lookups is covered by <c>LatestCollectorOutcomePlanTests</c> and
/// <c>DarlingWatermarkFloorPlanShapeLiveTests</c>, which run on a migrated (covering) store.
/// </summary>
public sealed class CollectionLogWatermarkCoveringIndexLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the #9151 collection log index live tests.";
    private const string IndexName = "idx_collection_log_watermark";
    private const string Collector = "query_store_stats";
    private const int ServerCount = 6;

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    /// <summary>The indexes on collection_log as "name#oid#included columns", so a rebuild (new oid) or a changed list shows.</summary>
    private static async Task<List<string>> IndexesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var list = new List<string>();
        await using var command = new NpgsqlCommand(
            @"SELECT c.relname || '#' || c.oid || '#' || i.indnkeyatts || '/' || i.indnatts || '#'
                     || COALESCE((SELECT string_agg(a.attname, ',' ORDER BY k.ord) FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum WHERE k.ord > i.indnkeyatts), '')
              FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
              WHERE i.indrelid = 'collect.collection_log'::regclass ORDER BY c.relname", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(reader.GetString(0));
        }

        return list;
    }

    private static string? Watermark(List<string> indexes) => indexes.SingleOrDefault(i => i.StartsWith(IndexName + "#", StringComparison.Ordinal));

    private static async Task DowngradeToNarrowAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "DROP INDEX IF EXISTS collect." + IndexName, ct);
        await ExecAsync(connection, "CREATE INDEX " + IndexName + " ON collect.collection_log (server_id, collector_name, collection_time DESC)", ct);
        var watermark = Watermark(await IndexesAsync(connection, ct));
        Assert.NotNull(watermark);
        Assert.EndsWith("#", watermark!);
    }

    private static async Task RunV288Async(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = PgMigrations.Scripts.Single(m => m.Version == 288).Sql;
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await ExecAsync(connection, sql, ct);
        await transaction.CommitAsync(ct);
    }

    /// <summary>Several servers, each with one row per five minutes for three days for the collector and a second collector, written time-major so a
    /// server's rows spread over many pages.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        for (var id = 1; id <= ServerCount; id++)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, "srv" + id, ct);
        }

        await using var insert = new NpgsqlCommand(
            @"INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
              SELECT row_number() OVER (ORDER BY t.ct, s.n, k.n), s.n, 'srv' || s.n, CASE WHEN k.n = 1 THEN '" + Collector + @"' ELSE 'other_collector' END,
                     t.ct, 20, 'SUCCESS', CASE WHEN s.n = 3 THEN NULL ELSE 40 + (s.n * 3) + (k.n % 5) END
              FROM generate_series($1::timestamp - interval '3 days', $1::timestamp, interval '5 minutes') AS t(ct)
              CROSS JOIN generate_series(1, " + ServerCount + @") AS s(n) CROSS JOIN generate_series(1, 2) AS k(n)", connection) { CommandTimeout = 300 };
        insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, now);
        await insert.ExecuteNonQueryAsync(ct);
        await ExecAsync(connection, "VACUUM (ANALYZE) collect.collection_log", ct);
    }

    private static async Task<(long Rows, long DailyRows, int Servers)> EstimateAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(QueryStoreWideReadGuard.RawCollectorRowsSql, connection) { CommandTimeout = 300 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Collector);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, now.AddDays(-2));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, now);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = DBNull.Value });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1), reader.GetInt32(2));
    }

    [Fact]
    public async Task AMigratedStore_HasTheCoveringIndex_AndNoLeftoverBuildIndex()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(scratch, ct);
            var indexes = await IndexesAsync(connection, ct);
            var watermark = Watermark(indexes);
            Assert.NotNull(watermark);
            Assert.EndsWith("#rows_collected", watermark!);
            Assert.DoesNotContain(indexes, i => i.StartsWith(IndexName + "_new#", StringComparison.Ordinal));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ANarrowIndexStore_ConvergesToTheCoveringIndex_AndApplyingTwiceChangesNothing()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(scratch, ct);
            await DowngradeToNarrowAsync(connection, ct);
            var narrowStore = await IndexesAsync(connection, ct);

            await RunV288Async(connection, ct);
            var afterFirst = await IndexesAsync(connection, ct);
            Assert.EndsWith("#rows_collected", Watermark(afterFirst));
            /* the narrow index is gone (one index of the name, now covering), the build name is gone, and no other index moved */
            Assert.Equal(narrowStore.Count, afterFirst.Count);
            Assert.DoesNotContain(afterFirst, i => i.StartsWith(IndexName + "_new#", StringComparison.Ordinal));
            Assert.Equal(
                narrowStore.Where(i => !i.StartsWith(IndexName + "#", StringComparison.Ordinal)),
                afterFirst.Where(i => !i.StartsWith(IndexName + "#", StringComparison.Ordinal)));

            await RunV288Async(connection, ct);
            Assert.Equal(afterFirst, await IndexesAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheCollectorRowEstimate_AnswersTheSame_WithTheNarrowAndTheCoveringIndex()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenAsync(scratch, ct);
            var now = LiveClock.Now();
            await SeedAsync(connection, now, ct);
            await DowngradeToNarrowAsync(connection, ct);
            var narrow = await EstimateAsync(connection, now, ct);
            /* every server counts, except the one whose rows are all NULL (its daily sum is NULL, so it adds nothing) */
            Assert.Equal(ServerCount - 1, narrow.Servers);
            Assert.True(narrow.Rows > 0 && narrow.DailyRows > 0);

            await RunV288Async(connection, ct);
            await ExecAsync(connection, "VACUUM (ANALYZE) collect.collection_log", ct);
            Assert.Equal(narrow, await EstimateAsync(connection, now, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}

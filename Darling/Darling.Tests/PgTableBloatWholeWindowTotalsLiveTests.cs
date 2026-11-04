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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: a scratch database made through ScratchPostgres. */

/// <summary>
/// What get_pg_table_bloat says about the estimates it published, over the whole window and not the page (#7551). A page of
/// suppressed rows once counted itself as the suppressed total and summed to 0 bytes, which reads as no bloat.
/// </summary>
public sealed class PgTableBloatWholeWindowTotalsLiveTests
{
    private const int ServerId = -755101;
    private const string ServerName = "bloat-totals-e2e";

    private static async Task<JsonDocument> ReadAsync(string connectionString, int limit, Func<NpgsqlConnection, DateTime, Task> seed, CancellationToken ct)
    {
        await using var store = new NpgsqlConnection(connectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await DarlingMcpTestData.RegisterServerAsync(store, ServerId, ServerName, ct);
        await seed(store, DateTime.UtcNow.AddHours(-1));
        return JsonDocument.Parse(await DarlingMcpPgTableBloatTools.GetPgTableBloat(dataSource, ServerName, 24, limit, null, null, ct));
    }

    [Fact]
    public async Task APageOfSuppressedRows_CountsTheWholeWindow_AndSaysNoEstimateWasPublished()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            using var json = await ReadAsync(scratch.ConnectionString, 2, async (store, at) =>
            {
                for (var i = 0; i < 5; i++)
                    await SeedAsync(store, at, "never_analyzed_" + i, lastAnalyzed: null, bloatBytes: 5_000_000L, ct);
            }, ct);
            var root = json.RootElement;

            Assert.Equal(2, root.GetProperty("tables_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(5, root.GetProperty("suppressed_estimate_count").GetInt64());
            Assert.Equal(0, root.GetProperty("trusted_estimate_count").GetInt64());
            Assert.Equal(5, root.GetProperty("tables_in_window").GetInt64());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("estimated_bloat_bytes_over_trusted_rows").ValueKind);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("estimated_bloat_mb_over_trusted_rows").ValueKind);
            Assert.Equal("No estimate published", root.GetProperty("estimated_bloat_state").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task PublishedEstimatesPastThePage_StillCountAndSum()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            using var json = await ReadAsync(scratch.ConnectionString, 1, async (store, at) =>
            {
                await SeedAsync(store, at, "good_a", lastAnalyzed: at.AddHours(-2), bloatBytes: 3_145_728L, ct);
                await SeedAsync(store, at, "good_b", lastAnalyzed: at.AddHours(-2), bloatBytes: 2_097_152L, ct);
                await SeedAsync(store, at, "good_c", lastAnalyzed: at.AddHours(-2), bloatBytes: 1_048_576L, ct);
                await SeedAsync(store, at, "lost_record", lastAnalyzed: null, bloatBytes: 900_000_000_000L, ct);
            }, ct);
            var root = json.RootElement;

            Assert.Equal(1, root.GetProperty("tables_returned").GetInt32());
            Assert.Equal(3, root.GetProperty("trusted_estimate_count").GetInt64());
            Assert.Equal(1, root.GetProperty("suppressed_estimate_count").GetInt64());
            Assert.Equal(4, root.GetProperty("tables_in_window").GetInt64());
            Assert.Equal(6_291_456L, root.GetProperty("estimated_bloat_bytes_over_trusted_rows").GetInt64());
            Assert.Equal(6.0, root.GetProperty("estimated_bloat_mb_over_trusted_rows").GetDouble());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("estimated_bloat_state").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ACutIndexUsagePage_StatesHowManyIndexesTheWindowHeld()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingMcpTestData.RegisterServerAsync(store, ServerId, ServerName, ct);
            var at = DateTime.UtcNow.AddHours(-1);
            foreach (var index in new[] { "ix_a", "ix_b", "ix_c" })
            {
                /* Two samples each: the count is of indexes, not of stored rows. */
                foreach (var sample in new[] { at.AddHours(-1), at })
                {
                    await DarlingMcpTestData.ExecAsync(store, ct,
                        @"INSERT INTO pg_index_usage_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, index_name, index_scans,
     tuples_read, tuples_fetched, blocks_read, blocks_hit, index_bytes, table_bytes, is_unique, is_primary_key, is_valid, is_ready,
     is_replica_identity, is_partial, is_expression, supports_constraint, index_method, column_count, index_definition, stats_reset)
VALUES ($1,$2,$3,$4,'postgres','public','widget',$5, 0, 0,0,0,0, 1000000, 2000000, false,false,true,true, false,false,false,false, 'btree', 1,
        'CREATE INDEX x ON public.widget USING btree (grp)', NULL)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(sample), ServerId, ServerName, index);
                }
            }

            using var cut = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, 2, null, null, ct));
            Assert.True(cut.RootElement.GetProperty("truncated").GetBoolean());
            Assert.Equal(2, cut.RootElement.GetProperty("indexes_returned").GetInt32());
            Assert.Equal(3, cut.RootElement.GetProperty("indexes_in_window").GetInt64());

            using var whole = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, 10, null, null, ct));
            Assert.Equal(3, whole.RootElement.GetProperty("indexes_in_window").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task UnavailableRowsPastThePage_StillGiveTheGrantHint_AndTheWindowsCount()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            /* Suppressed rows sort by estimated bytes, so the never-analyzed rows (bigger) fill the page of 3 and the
               three estimate_unavailable rows sit past it, one of them with pgstattuple installed. */
            using var json = await ReadAsync(scratch.ConnectionString, 3, async (store, at) =>
            {
                for (var i = 0; i < 5; i++)
                    await SeedAsync(store, at, "never_analyzed_" + i, lastAnalyzed: null, bloatBytes: 5_000_000L, ct);
                await SeedAsync(store, at, "no_stats_a", lastAnalyzed: at.AddHours(-2), bloatBytes: 1_000L, ct, estimateUnavailable: true, pgstattuple: true);
                await SeedAsync(store, at, "no_stats_b", lastAnalyzed: at.AddHours(-2), bloatBytes: 1_000L, ct, estimateUnavailable: true);
                await SeedAsync(store, at, "no_stats_c", lastAnalyzed: at.AddHours(-2), bloatBytes: 1_000L, ct, estimateUnavailable: true);
            }, ct);
            var root = json.RootElement;

            Assert.Equal(3, root.GetProperty("tables_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            foreach (var table in root.GetProperty("tables").EnumerateArray())
                Assert.StartsWith("never_analyzed_", table.GetProperty("table").GetString(), StringComparison.Ordinal);
            Assert.Equal(8, root.GetProperty("suppressed_estimate_count").GetInt64());
            Assert.True(root.GetProperty("pgstattuple_available_anywhere").GetBoolean());
            Assert.Contains("3 table(s) have NO usable column statistics", root.GetProperty("note").GetString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ANullModsSinceAnalyze_IsPublished_InTheTallyAndTheGrid()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            using var json = await ReadAsync(scratch.ConnectionString, 25, async (store, at) =>
            {
                await SeedAsync(store, at, "null_mods", lastAnalyzed: at.AddHours(-2), bloatBytes: 3_145_728L, ct, modsSinceAnalyze: null);
            }, ct);
            var root = json.RootElement;

            /* The page reader maps a NULL mods_since_analyze to 0, so the grid publishes the row; the tally must say the same. */
            var row = root.GetProperty("tables")[0];
            Assert.False(row.GetProperty("estimate_suppressed").GetBoolean());
            Assert.Equal(3_145_728L, row.GetProperty("bloat_bytes_estimate").GetInt64());
            Assert.Equal(1, root.GetProperty("trusted_estimate_count").GetInt64());
            Assert.Equal(0, root.GetProperty("suppressed_estimate_count").GetInt64());
            Assert.Equal(3_145_728L, root.GetProperty("estimated_bloat_bytes_over_trusted_rows").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static Task SeedAsync(
        NpgsqlConnection store, DateTime at, string table, DateTime? lastAnalyzed, long bloatBytes, CancellationToken ct,
        bool estimateUnavailable = false, long? modsSinceAnalyze = 0, bool pgstattuple = false) =>
        DarlingMcpTestData.ExecAsync(store, ct,
            @"INSERT INTO pg_table_bloat_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, heap_bytes, heap_pages, toast_bytes,
     index_bytes, live_tuples, dead_tuples, mods_since_analyze, last_analyzed, estimated_tuple_bytes, estimated_heap_pages, fillfactor,
     bloat_bytes_estimate, bloat_pct_estimate, estimate_unavailable, alignment_bytes, pgstattuple_available)
VALUES ($1,$2,$3,$4,'postgres','public',$5, 1000000000,122070,0, 0, 1000,0,$8, $6, 100,100, 100, $7, 50.00, $9, 8, $10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, table,
            lastAnalyzed is { } analyzed ? DarlingMcpTestData.Naive(analyzed) : null, bloatBytes,
            modsSinceAnalyze, estimateUnavailable, pgstattuple);
}

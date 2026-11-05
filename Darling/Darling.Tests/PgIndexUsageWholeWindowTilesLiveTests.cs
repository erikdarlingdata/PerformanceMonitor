/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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
/// What get_pg_index_usage's tiles count: every index the window held, not the listed page (#7575). The unscanned count, the bytes
/// those indexes hold and the invalid count once read the page's rows, so a server with more indexes than the limit undercounted them.
/// </summary>
public sealed class PgIndexUsageWholeWindowTilesLiveTests
{
    private const int ServerId = -757501;
    private const string ServerName = "index-tiles-e2e";
    private const int Limit = 25;

    private static async Task SeedSampleAsync(
        NpgsqlConnection store, DateTime at, string index, long scans, long bytes, bool valid, bool primaryKey, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(store, ct,
            @"INSERT INTO pg_index_usage_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, index_name, index_scans,
     tuples_read, tuples_fetched, blocks_read, blocks_hit, index_bytes, table_bytes, is_unique, is_primary_key, is_valid, is_ready,
     is_replica_identity, is_partial, is_expression, supports_constraint, index_method, column_count, index_definition, stats_reset)
VALUES ($1,$2,$3,$4,'postgres','public','widget',$5, $6, 0,0,0,0, $7, 20000000, $9,$9,$8,true, false,false,false,$9, 'btree', 1,
        'CREATE INDEX x ON public.widget USING btree (grp)', NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, index, scans, bytes, valid, primaryKey);
    }

    [Fact]
    public async Task TheTilesCountEveryIndexInTheWindow_NotTheListedPage()
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

            var later = DateTime.UtcNow.AddHours(-1);
            var earlier = later.AddHours(-1);
            long expectedBytes = 0;

            /* 28 unscanned indexes nothing blocks. The page keeps the 20 biggest after the invalid ones, so 8 of these sit past it. */
            for (var i = 0; i < 28; i++)
            {
                var bytes = 1_000_000L + (i * 10_000L);
                expectedBytes += bytes;
                await SeedSampleAsync(store, earlier, "unscanned_" + i, 0, bytes, true, false, ct);
                await SeedSampleAsync(store, later, "unscanned_" + i, 0, bytes, true, false, ct);
            }

            /* 5 invalid: first on the page, and counted as invalid, not as unscanned. */
            for (var i = 0; i < 5; i++)
            {
                await SeedSampleAsync(store, earlier, "invalid_" + i, 0, 900_000, false, false, ct);
                await SeedSampleAsync(store, later, "invalid_" + i, 0, 900_000, false, false, ct);
            }

            /* Past the page and not counted: unscanned primary keys, a scanned index, one whose counter fell (a reset), and one seen once. */
            for (var i = 0; i < 4; i++)
            {
                await SeedSampleAsync(store, earlier, "pk_" + i, 0, 500_000, true, true, ct);
                await SeedSampleAsync(store, later, "pk_" + i, 0, 500_000, true, true, ct);
            }

            await SeedSampleAsync(store, earlier, "scanned", 5, 400_000, true, false, ct);
            await SeedSampleAsync(store, later, "scanned", 50, 400_000, true, false, ct);
            await SeedSampleAsync(store, earlier, "reset", 100, 300_000, true, false, ct);
            await SeedSampleAsync(store, later, "reset", 0, 300_000, true, false, ct);
            await SeedSampleAsync(store, later, "seen_once", 0, 200_000, true, false, ct);

            const int indexCount = 28 + 5 + 4 + 1 + 1 + 1;

            using var cut = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, Limit, null, null, ct));
            var root = cut.RootElement;
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(Limit, root.GetProperty("indexes_returned").GetInt32());
            Assert.Equal(indexCount, root.GetProperty("indexes_in_window").GetInt64());

            Assert.Equal(28, root.GetProperty("unscanned_without_a_structural_blocker").GetInt32());
            Assert.Equal(expectedBytes, root.GetProperty("bytes_held_by_those_indexes").GetInt64());
            Assert.Equal(5, root.GetProperty("invalid_index_count").GetInt32());
            Assert.True(root.GetProperty("statistics_were_reset_in_window").GetBoolean());

            /* The same tiles when the page holds everything: the page's SQL-classed flag and the tally agree. */
            using var whole = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, 100, null, null, ct));
            var all = whole.RootElement;
            Assert.False(all.GetProperty("truncated").GetBoolean());
            Assert.Equal(28, all.GetProperty("unscanned_without_a_structural_blocker").GetInt32());
            Assert.Equal(expectedBytes, all.GetProperty("bytes_held_by_those_indexes").GetInt64());
            Assert.Equal(5, all.GetProperty("invalid_index_count").GetInt32());

            var end = DateTime.UtcNow;
            var rows = await DarlingPgIndexUsageReader.GetPgIndexUsageAsync(dataSource, ServerId, end.AddHours(-24), end, 100, ct);
            var totals = await DarlingPgIndexUsageReader.GetPgIndexUsageTotalsAsync(dataSource, ServerId, end.AddHours(-24), end, ct);
            Assert.Equal(rows.Count(r => r.IsUnscannedCandidate), totals.UnscannedCount);
            Assert.Equal(rows.Where(r => r.IsUnscannedCandidate).Sum(r => r.IndexBytes), totals.UnscannedBytes);
            Assert.Equal(rows.Count(r => !r.IsValid), totals.InvalidCount);
            Assert.Equal(rows.Count, totals.IndexCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* is_unique, is_valid and supports_constraint are spliced as the literals true, false or NULL: they are test constants, and an
       untyped NULL parameter has no type to bind. */
    private static async Task SeedFlagsAsync(
        NpgsqlConnection store, DateTime at, string index, long bytes, string isUnique, string isValid, string supportsConstraint, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(store, ct,
            $@"INSERT INTO pg_index_usage_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, index_name, index_scans,
     tuples_read, tuples_fetched, blocks_read, blocks_hit, index_bytes, table_bytes, is_unique, is_primary_key, is_valid, is_ready,
     is_replica_identity, is_partial, is_expression, supports_constraint, index_method, column_count, index_definition, stats_reset)
VALUES ($1,$2,$3,$4,'postgres','public','widget',$5, 0, 0,0,0,0, $6, 20000000, {isUnique},false,{isValid},true, false,false,false,{supportsConstraint}, 'btree', 1,
        'CREATE INDEX x ON public.widget USING btree (grp)', NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, index, bytes);
    }

    /// <summary>
    /// A NULL flag is read the way the row reader reads it, in the tiles and in each row (#7575): is_valid NULL is valid, is_unique NULL is
    /// not a blocker, and supports_constraint NULL IS a blocker, so that index is not an unscanned drop candidate.
    /// </summary>
    [Fact]
    public async Task ANullFlagIsReadTheWayTheRowReaderReadsIt_InTheTilesAndInEachRow()
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

            var later = DateTime.UtcNow.AddHours(-1);
            var earlier = later.AddHours(-1);

            /* Two samples each, no scans. plain, null_is_valid and null_is_unique are candidates; null_supports_constraint is a blocker. */
            var indexes = new (string Name, long Bytes, string Unique, string Valid, string Supports)[]
            {
                ("plain", 4_000_000, "false", "true", "false"),
                ("null_is_valid", 3_000_000, "false", "NULL", "false"),
                ("null_is_unique", 2_000_000, "NULL", "true", "false"),
                ("null_supports_constraint", 1_000_000, "false", "true", "NULL"),
            };
            foreach (var (name, bytes, unique, valid, supports) in indexes)
            {
                await SeedFlagsAsync(store, earlier, name, bytes, unique, valid, supports, ct);
                await SeedFlagsAsync(store, later, name, bytes, unique, valid, supports, ct);
            }

            const long candidateBytes = 4_000_000 + 3_000_000 + 2_000_000;

            /* Cut at 2 so the tiles come from the whole-window read, then uncut so they come from the rows: both must say the same. */
            using var cut = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, 2, null, null, ct));
            Assert.True(cut.RootElement.GetProperty("truncated").GetBoolean());
            Assert.Equal(3, cut.RootElement.GetProperty("unscanned_without_a_structural_blocker").GetInt32());
            Assert.Equal(candidateBytes, cut.RootElement.GetProperty("bytes_held_by_those_indexes").GetInt64());
            Assert.Equal(0, cut.RootElement.GetProperty("invalid_index_count").GetInt32());
            Assert.Equal(4, cut.RootElement.GetProperty("indexes_in_window").GetInt64());

            using var whole = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, 100, null, null, ct));
            var all = whole.RootElement;
            Assert.False(all.GetProperty("truncated").GetBoolean());
            Assert.Equal(3, all.GetProperty("unscanned_without_a_structural_blocker").GetInt32());
            Assert.Equal(candidateBytes, all.GetProperty("bytes_held_by_those_indexes").GetInt64());
            Assert.Equal(0, all.GetProperty("invalid_index_count").GetInt32());

            var byName = all.GetProperty("indexes").EnumerateArray().ToDictionary(e => e.GetProperty("index").GetString()!);
            Assert.Equal("Warning", byName["plain"].GetProperty("severity").GetString());
            Assert.Equal("Warning", byName["null_is_valid"].GetProperty("severity").GetString());
            Assert.True(byName["null_is_valid"].GetProperty("is_valid").GetBoolean());
            Assert.Equal("Warning", byName["null_is_unique"].GetProperty("severity").GetString());
            Assert.False(byName["null_is_unique"].GetProperty("is_unique").GetBoolean());
            var blocked = byName["null_supports_constraint"];
            Assert.Equal("Healthy", blocked.GetProperty("severity").GetString());
            Assert.True(blocked.GetProperty("supports_constraint").GetBoolean());
            Assert.Contains("Backs a UNIQUE or EXCLUSION constraint", blocked.GetProperty("droppability_finding").GetString(), StringComparison.Ordinal);

            /* The SQL class of each row agrees with the findings above. */
            var end = DateTime.UtcNow;
            var rows = await DarlingPgIndexUsageReader.GetPgIndexUsageAsync(dataSource, ServerId, end.AddHours(-24), end, 100, ct);
            Assert.Equal(
                new[] { "null_is_unique", "null_is_valid", "plain" },
                rows.Where(r => r.IsUnscannedCandidate).Select(r => r.IndexName!).Order(StringComparer.Ordinal).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}

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
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;

namespace Darling.Tests;

/// <summary>
/// #5807: <c>get_query_store_query_history</c>'s table read (<see cref="DarlingMcpQueryStoreHistoryTools.HistoryTableSql"/>)
/// asks for one query, but no index led with <c>query_id</c>, so it read every row of the query's database in the
/// window and filtered. This plants many queries in one database across the day partitions of a promoted wide table,
/// builds the indexes a field store has, then the new <see cref="QueryStoreBackgroundIndexes.WideDatabaseQueryFirstExec"/>
/// through the background path, and runs the product's own statement under <c>EXPLAIN (ANALYZE, BUFFERS)</c> with a custom
/// and a generic plan: every index the read touches is a leaf of the new index, no leaf is read sequentially, the read
/// touches a fraction of the blocks it did before, and it returns the same rows.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreHistoryQueryIndexLiveTests
{
    private const string Parameters = "integer, text, bigint, timestamp, timestamp";

    /* One query of the busy database, its window the 24 hours before the fixed Now (the tool's default hours_back). */
    private const string Arguments = "1, 'db', 42, '2026-10-07 14:00:00', '2026-10-08 14:00:00'";

    private const string NewIndexLeafPrefix = "ix_query_store_interval_wide_db_query_first_exec_";

    private static readonly string[] Modes = { "force_custom_plan", "force_generic_plan" };

    /* One scan of a leaf in the plan: the leaf, the node type, and every index the node and its children read (a bitmap
       heap scan's indexes sit in its Bitmap Index Scan children, possibly under a BitmapAnd). */
    private sealed record LeafScan(string Leaf, string NodeType, IReadOnlyList<string> Indexes);

    /* What one EXPLAIN (ANALYZE, BUFFERS) reported: each leaf scan, and the execution's shared blocks (hit plus read) at
       the plan's root, which counts every node below it. */
    private sealed record Run(IReadOnlyList<LeafScan> Scans, long SharedBlocks, string Json);

    private static void IndexesUnder(JsonElement node, List<string> indexes)
    {
        if (node.TryGetProperty("Index Name", out var name) && name.GetString() is { } index)
        {
            indexes.Add(index);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                IndexesUnder(child, indexes);
            }
        }
    }

    private static void Walk(JsonElement node, List<LeafScan> scans)
    {
        if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() is { } leaf)
        {
            var indexes = new List<string>();
            IndexesUnder(node, indexes);
            scans.Add(new LeafScan(leaf, node.GetProperty("Node Type").GetString()!, indexes));
            return;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, scans);
            }
        }
    }

    private static async Task<Run> ExplainAsync(NpgsqlConnection connection, string planCacheMode, CancellationToken ct)
    {
        await ExecAsync(connection, $"SET plan_cache_mode = {planCacheMode}", ct);
        await ExecAsync(connection, $"PREPARE history_probe({Parameters}) AS {DarlingMcpQueryStoreHistoryTools.HistoryTableSql}", ct);
        try
        {
            await using var command = new NpgsqlCommand($"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE history_probe({Arguments})", connection) { CommandTimeout = 120 };
            var json = (string)(await command.ExecuteScalarAsync(ct))!;
            using var document = JsonDocument.Parse(json);
            var plan = document.RootElement[0].GetProperty("Plan");
            var scans = new List<LeafScan>();
            Walk(plan, scans);
            var blocks = plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();
            return new Run(scans, blocks, json);
        }
        finally
        {
            await ExecAsync(connection, "DEALLOCATE history_probe", ct);
        }
    }

    /* The read's rows, each stringified, in the statement's own order (a total order, #5300). */
    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "SET plan_cache_mode = auto", ct);
        await ExecAsync(connection, $"PREPARE history_rows({Parameters}) AS {DarlingMcpQueryStoreHistoryTools.HistoryTableSql}", ct);
        try
        {
            await using var command = new NpgsqlCommand($"EXECUTE history_rows({Arguments})", connection) { CommandTimeout = 120 };
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = new List<string>();
            while (await reader.ReadAsync(ct))
            {
                var values = new object[reader.FieldCount];
                reader.GetValues(values);
                rows.Add(string.Join("|", values.Select(v => v is DateTime stamp
                    ? stamp.ToString("O", CultureInfo.InvariantCulture)
                    : Convert.ToString(v, CultureInfo.InvariantCulture))));
            }

            return rows;
        }
        finally
        {
            await ExecAsync(connection, "DEALLOCATE history_rows", ct);
        }
    }

    /* Promotes the wide table as of Now - 10 d and creates the days ahead, then plants one row per hourly interval for
       three days (2026-10-05 15:00 to 2026-10-08 14:00) for 1,500 queries in 'db' and 300 in 'other' on server 1, and
       120 queries in 'db' on server 2: about 136,000 rows over four day partitions, the shape of a busy database
       beside quieter ones. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PromoteAsync(connection, Wide, Now.AddDays(-10), NullLogger.Instance, ct);
        await QueryStoreIntervalPartitions.CreateAheadAsync(connection, Wide, Now, NullLogger.Instance, ct);
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, avg_cpu_time_us, runtime_stats_interval_id, interval_start_time_utc)
SELECT h + interval '1 hour', d.server_id, d.database_name, q, q * 10, 'Regular', h, h + interval '50 minutes',
       'select 1', 10 + q, 1000 + q, 500 + q, extract(epoch FROM h)::bigint / 3600, h
FROM generate_series(timestamp '2026-10-05 15:00:00', timestamp '2026-10-08 14:00:00', interval '1 hour') AS h
CROSS JOIN (VALUES (1, 'db', 1500), (1, 'other', 300), (2, 'db', 120)) AS d (server_id, database_name, queries)
CROSS JOIN LATERAL generate_series(1, d.queries) AS q;", ct);
        await ExecAsync(connection, $"ANALYZE {Wide.Parent}", ct);
    }

    [Fact]
    public async Task TheOneQueryHistoryRead_ScansOnlyTheNewIndex_InEveryPartitionItReaches_AndReturnsTheSameRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);
        await ExecAsync(connection, "SET search_path = collect, config, public", ct);

        /* Before: the indexes a field store already has (the unique key, V154's, the background BRIN and server btree). */
        foreach (var spec in new[] { QueryStoreIntervalWideBrinIndex.Spec, QueryStoreBackgroundIndexes.WideServerFirstExec })
        {
            await QueryStoreBackgroundIndexes.EnsureAsync(connection, spec, NullLogger.Instance, ct);
        }

        await ExecAsync(connection, $"ANALYZE {Wide.Parent}", ct);
        var before = await ExplainAsync(connection, "force_custom_plan", ct);
        var rowsBefore = await RowsAsync(connection, ct);
        Assert.Equal(25, rowsBefore.Count); /* one row per hourly interval of query 42 collected in the window, both ends inclusive */

        /* After: the new index, built the way the service builds it (the partitioned path, leaf by leaf). */
        await QueryStoreBackgroundIndexes.EnsureAsync(connection, QueryStoreBackgroundIndexes.WideDatabaseQueryFirstExec, NullLogger.Instance, ct);
        Assert.True((bool)(await ScalarAsync(connection,
            $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryStoreBackgroundIndexes.WideDatabaseQueryFirstExecIndexName}'::regclass", ct))!);
        await ExecAsync(connection, $"ANALYZE {Wide.Parent}", ct);

        /* The leaves that hold rows. An empty leaf (a day ahead, the DEFAULT partition, the emptied legacy table) costs
           nothing to read whichever way the planner reads it, so only these are held to the new index. */
        var seeded = new List<string>();
        await using (var leaves = new NpgsqlCommand($"SELECT DISTINCT tableoid::regclass::text FROM {Wide.Parent}", connection))
        await using (var reader = await leaves.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                seeded.Add(reader.GetString(0));
            }
        }

        Assert.Equal(4, seeded.Count);
        Assert.Contains(before.Scans, scan => seeded.Contains(scan.Leaf));
        Assert.DoesNotContain(before.Scans, scan => scan.Indexes.Any(name => name.StartsWith(NewIndexLeafPrefix, StringComparison.Ordinal)));

        foreach (var mode in Modes)
        {
            var after = await ExplainAsync(connection, mode, ct);
            var busy = after.Scans.Where(scan => seeded.Contains(scan.Leaf)).ToList();

            /* The bound still prunes: the window (24 h, less the 26 h floor margin, plus the 12 h upper slack) reaches the
               three seeded days from 10-06 on, never 10-05. */
            Assert.Equal(new[] { "query_store_interval_wide_p20261006", "query_store_interval_wide_p20261007", "query_store_interval_wide_p20261008" },
                busy.Select(scan => scan.Leaf).OrderBy(leaf => leaf, StringComparer.Ordinal).ToArray());

            /* Every seeded leaf it reaches is read through its leaf of the new index (a bitmap may AND the BRIN beside it). */
            Assert.All(busy, scan =>
            {
                Assert.NotEqual("Seq Scan", scan.NodeType);
                Assert.Contains(scan.Indexes, name => name.StartsWith(NewIndexLeafPrefix, StringComparison.Ordinal));
            });
            Assert.True(after.SharedBlocks * 10 < before.SharedBlocks, $"{mode}: {after.SharedBlocks} blocks after, {before.SharedBlocks} before. {after.Json}");
        }

        Assert.Equal(rowsBefore, await RowsAsync(connection, ct));
    }

    /* #5807 review L2: leaves build oldest first and retention drops the oldest next. A drop that lands between the leaf list
       and a leaf's build used to throw 42P01 out of the whole spec, leaving the newer leaves unindexed and the parent INVALID. */
    [Fact]
    public async Task ALeafRetentionDropsBetweenTheLeafListAndItsBuild_IsSkipped_AndTheNewerLeavesAttachAndTheParentEndsValid()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);

        var dropped = new List<string>();
        var logger = new CapturingLogger();
        await QueryStoreBackgroundIndexes.EnsurePartitionedAsync(
            connection, QueryStoreBackgroundIndexes.WideDatabaseQueryFirstExec, logger, ct,
            async (leaf, token) =>
            {
                /* The oldest day leaf (the DEFAULT partition sorts before the day leaves and is built first). */
                if (dropped.Count == 0 && leaf.Contains("_p2026", StringComparison.Ordinal))
                {
                    dropped.Add(leaf);
                    await ExecAsync(connection, $"DROP TABLE {leaf}", token);
                }
            });

        Assert.Single(dropped);
        Assert.Single(logger.Lines, line => line.Contains(dropped[0], StringComparison.Ordinal));
        Assert.True((bool)(await ScalarAsync(connection,
            $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{QueryStoreBackgroundIndexes.WideDatabaseQueryFirstExecIndexName}'::regclass", ct))!);

        /* Every leaf that is left has its child attached. */
        Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, $@"
SELECT count(*) FROM pg_partition_tree('{Wide.Parent}'::regclass) AS t
WHERE t.isleaf AND NOT EXISTS (SELECT 1 FROM pg_inherits AS ih JOIN pg_index AS ci ON ci.indexrelid = ih.inhrelid
                               WHERE ih.inhparent = '{QueryStoreBackgroundIndexes.WideDatabaseQueryFirstExecIndexName}'::regclass AND ci.indrelid = t.relid)", ct), CultureInfo.InvariantCulture));
    }

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Add(formatter(state, exception));
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}

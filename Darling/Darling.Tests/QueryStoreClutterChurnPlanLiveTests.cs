/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #6808: <c>PlanChurnSql</c> (the plan-churn arm of <c>get_query_store_clutter</c>) sorted a materialised copy of every raw
/// <c>query_store_stats</c> row in the window twice, once per <c>COUNT(DISTINCT collection_time)</c>, and the sorts spilled to disk
/// at 2.4 million rows. These tests plant a store whose answer is known from a plain C# model of the seed, run the shipped
/// statement under a small <c>work_mem</c> (<c>SET LOCAL</c>), and assert two things: the rows are exactly the model's, for
/// two windows and with and without a database choice, and the plan the statement actually ran has no spill.
/// </summary>
public sealed class QueryStoreClutterChurnPlanLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live plan-churn test (it mints its own scratch database).";

    private const string Idx = "_hyper_1_1_chunk_idx_query_store_stats_server_db_query_plan_ti";

    /* #6816: the plan-text judges themselves, on plan text, so they run without a database. */

    [Fact]
    public void WalksCoveringIndexOnly_RequiresEveryStatsScanToBeIndexOnly_AndAtLeastTwoOfThem()
    {
        var indexOnly = "  ->  Index Only Scan using " + Idx + " on _hyper_1_1_chunk (actual rows=10 loops=1)\n";
        var indexOnlyAlias = "  ->  Index Only Scan using " + Idx + " on _hyper_1_1_chunk _hyper_1_1_chunk_1 (actual rows=10 loops=1)\n";
        var heap = "  ->  Index Scan using _hyper_1_1_chunk_idx_query_store_stats_time on _hyper_1_1_chunk _hyper_1_1_chunk_1 (actual rows=10 loops=1)\n";
        var seq = "  ->  Seq Scan on _hyper_1_1_chunk _hyper_1_1_chunk_1 (actual rows=10 loops=1)\n";
        var bitmap = "  ->  Bitmap Heap Scan on _hyper_1_1_chunk _hyper_1_1_chunk_1 (actual rows=10 loops=1)\n        ->  Bitmap Index Scan on " + Idx + " (actual rows=10 loops=1)\n";

        Assert.True(QueryStoreClutterChurnProbe.WalksCoveringIndexOnly(indexOnly + indexOnlyAlias));
        Assert.False(QueryStoreClutterChurnProbe.WalksCoveringIndexOnly(indexOnly + heap), "one scan went back to the heap");
        Assert.False(QueryStoreClutterChurnProbe.WalksCoveringIndexOnly(indexOnly + seq), "one scan is sequential");
        Assert.False(QueryStoreClutterChurnProbe.WalksCoveringIndexOnly(indexOnly + bitmap), "one scan is a bitmap heap scan");
        Assert.False(QueryStoreClutterChurnProbe.WalksCoveringIndexOnly(indexOnly), "the CTE is read twice, so one scan is not enough");
        Assert.False(QueryStoreClutterChurnProbe.WalksCoveringIndexOnly("Aggregate\n  ->  Result\n"), "no scan at all");
    }

    [Fact]
    public void SpillIn_FlagsAMultiBatchHashJoin_AndNotASingleBatchOne()
    {
        Assert.Contains("Batches: 4", QueryStoreClutterChurnProbe.SpillIn("Hash Join\n  ->  Hash\n        Buckets: 1024  Batches: 4  Memory Usage: 9kB\n"));
        Assert.Null(QueryStoreClutterChurnProbe.SpillIn("Hash Join\n  ->  Hash\n        Buckets: 1024  Batches: 1  Memory Usage: 9kB\n"));
    }

    [Fact]
    public async Task PlanChurn_ReturnsTheSeededAnswerExactly_ForTwoWindows_WithAndWithoutADatabaseChoice_AndNeverSpills()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var anchor = LiveClock.AnchoredEnd();

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        /* The service's own order, so the plan under EXPLAIN is production's (#6808 review): the hypertable conversion where
           TimescaleDB is present, then the tuning list that creates idx_query_store_stats_server_db_query_plan_time, the index
           PlanChurnSql's projection is cut to. MigrateAsync alone leaves a plain table with no such index, which can show
           neither an index-only walk nor the per-chunk plan production runs. */
        if (scratch.TimescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        await PgTableTuning.ApplyAsync(connection, null, ct);
        await QueryStoreClutterChurnProbe.SeedAsync(connection, anchor, ct);

        /* the whole seed, then a window that starts in the middle of it (the 24 h shape: older collections fall out and
           the arrivals, the first collection and the seen-once count all move) */
        var whole = anchor.AddDays(-7);
        var cut = QueryStoreClutterChurnProbe.CollectionTime(anchor, 4);
        var end = anchor;

        foreach (var start in new[] { whole, cut })
        {
            foreach (var databases in new string[]?[] { null, [QueryStoreClutterChurnProbe.Small], [QueryStoreClutterChurnProbe.Big, QueryStoreClutterChurnProbe.Small] })
            {
                var label = $"window start {start:o}, databases {(databases is null ? "all" : string.Join("+", databases))}";
                var run = await QueryStoreClutterChurnProbe.RunAsync(connection, start, end, databases, ct);
                var expected = QueryStoreClutterChurnProbe.ExpectedFor(anchor, start, end, databases);
                Assert.True(expected.Count > 0, "the model must give a row for " + label);
                Assert.True(QueryStoreClutterChurnProbe.FirstDifference(run.Rows, expected) is null,
                    label + "\n" + QueryStoreClutterChurnProbe.FirstDifference(run.Rows, expected));
                Assert.True(QueryStoreClutterChurnProbe.SpillIn(run.PlanText) is null,
                    label + ": the statement spilled at work_mem " + QueryStoreClutterChurnProbe.WorkMem + ": " + QueryStoreClutterChurnProbe.SpillIn(run.PlanText) + "\n" + run.PlanText);

                /* At 76,000 rows the planner prefers a sequential scan of the one chunk, where production's 2.4 million rows
                   take the index. With the sequential and bitmap scans off the planner has to pick an index path, and whether that path is
                   index-only is decided by the statement's own projection: a column the covering index does not carry
                   turns it into an Index Scan, which is the heap walk the projection is cut to avoid. */
                var viaIndex = await QueryStoreClutterChurnProbe.RunAsync(connection, start, end, databases, ct, "enable_seqscan = off", "enable_bitmapscan = off");
                Assert.True(QueryStoreClutterChurnProbe.FirstDifference(viaIndex.Rows, expected) is null,
                    label + " (index path)\n" + QueryStoreClutterChurnProbe.FirstDifference(viaIndex.Rows, expected));
                Assert.True(QueryStoreClutterChurnProbe.WalksCoveringIndexOnly(viaIndex.PlanText),
                    label + ": with the sequential and bitmap scans off, the statement did not read " + QueryStoreClutterChurnProbe.CoveringIndex + " with an index-only scan\n" + viaIndex.PlanText);
                Assert.True(QueryStoreClutterChurnProbe.SpillIn(viaIndex.PlanText) is null,
                    label + ": the index path spilled at work_mem " + QueryStoreClutterChurnProbe.WorkMem + ": " + QueryStoreClutterChurnProbe.SpillIn(viaIndex.PlanText) + "\n" + viaIndex.PlanText);
            }
        }
    }
}

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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5988 part A: the viewer's Queries grids carry the query tag and filter by it, through the shared <see cref="QueryTagReadJoin"/>.
/// The seed is three hashes: a tagged-nowhere hog that tops the unfiltered page, a child-tagged query and a batch-tagged one.
/// With a page of ONE row the hog fills the unfiltered page, so a tag's rows can only appear if the filter acts before the cut.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerQueryTagColumnLiveTests
{
    private const string ServerName = "darling-viewer-query-tag-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "ViewerTagDb";
    private const string Hog = "0xVTAGHOG";
    private const string Ord = "0xVTAGORD";
    private const string Bat = "0xVTAGBAT";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task RunAsync(Func<ViewerDataService, DateTime, DateTime, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer query-tag tests.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await CleanupAsync(connection, ct);
                await SeedAsync(connection, ct);
            }

            await using var viewer = new ViewerDataService(cs!);
            var end = DateTime.UtcNow.AddMinutes(5);
            await body(viewer, end.AddHours(-2), end, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection c, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(c, ct,
            $@"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM procedure_stats WHERE server_id = {ServerId};
               DELETE FROM query_store_stats WHERE server_id = {ServerId}; DELETE FROM query_snapshots WHERE server_id = {ServerId};
               DELETE FROM long_query_completions WHERE server_id = {ServerId}; DELETE FROM config.query_tag_map WHERE server_id = {ServerId};
               DELETE FROM config.query_tags WHERE name IN ('ViewerTagProd', 'ViewerTagOrders', 'ViewerTagBatch'); DELETE FROM servers WHERE server_id = {ServerId}");

    /// <summary>Tags: ViewerTagProd with child ViewerTagOrders, and ViewerTagBatch. Map: Ord to Orders, Bat to Batch, Hog unmapped.</summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO config.query_tags (name) VALUES ('ViewerTagProd')");
        await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO config.query_tags (name, parent_id) SELECT 'ViewerTagOrders', id FROM config.query_tags WHERE name = 'ViewerTagProd'");
        await DarlingMcpTestData.ExecAsync(c, ct, "INSERT INTO config.query_tags (name) VALUES ('ViewerTagBatch')");
        foreach (var (kind, key, tag) in new[]
        {
            ("hash", Ord, "ViewerTagOrders"), ("hash", Bat, "ViewerTagBatch"), ("object", $"{Db}.dbo.usp_VTagOrders", "ViewerTagOrders"),
        })
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source)
                  SELECT $1, $2, $3, id, 'rule' FROM config.query_tags WHERE name = $4",
                ServerId, kind, key, tag);
        }

        var at = DarlingMcpTestData.Naive(LiveClock.Now().AddMinutes(-10));
        foreach (var (hash, cpu) in new[] { (Hog, 9_000_000L), (Ord, 5_000_000L), (Bat, 1_000_000L) })
        {
            var text = "SELECT " + hash;
            var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text));
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                                           query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
                  VALUES ($1,$2,$3,$4,$5,$6,'0xPLAN','0xSQLH','0xPLANH',$7,$8,10,$9,$9,100,1,1)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, hash, text, digest, cpu);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, query_hash, query_plan_hash,
                                                 query_text, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, min_dop, max_dop, last_execution_time)
                  VALUES ($1,$2,$3,$4,$5,$6,$6,'Regular',$7,'0xPLAN',$8,10,$9,4000,800,0,40,250,1,4,$2)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, (long)Math.Abs(hash.GetHashCode() % 100000), hash, text, cpu / 10);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, query_hash, cpu_time_ms, total_elapsed_time_ms)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,'running',$8,$9,$9)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, 50 + Math.Abs(hash.GetHashCode() % 40), Db, text, hash, cpu / 1000);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text, query_hash)
                  VALUES ($1,$2,$3,$4,$2,'rpc_completed',$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, cpu, text, hash);
        }

        foreach (var (name, cpu) in new[] { ("usp_VTagPlain", 8_000_000L), ("usp_VTagOrders", 2_000_000L) })
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
                                               delta_execution_count, delta_worker_time, delta_elapsed_time)
                  VALUES ($1,$2,$3,$4,$5,'dbo',$6,'SQL_STORED_PROCEDURE',10,$7,$7)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, name, cpu);
        }
    }

    [Fact]
    public Task TopQueries_ASeededTaggedHash_ShowsItsTagPath_AndAnUntaggedOneShowsNone() => RunAsync(async (viewer, start, end, ct) =>
    {
        var rows = (await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, start, end, 10, null, null, ct)).Rows;
        Assert.Equal("ViewerTagProd / ViewerTagOrders", rows.Single(r => r.QueryHash == Ord).QueryTag);
        Assert.Equal("ViewerTagBatch", rows.Single(r => r.QueryHash == Bat).QueryTag);
        Assert.Null(rows.Single(r => r.QueryHash == Hog).QueryTag);
    });

    [Fact]
    public Task TopQueries_TheTagFilter_ReturnsThatTagsRows_IncludingOneOutsideTheUnfilteredTopN_AndAParentMatchesItsChildren() => RunAsync(async (viewer, start, end, ct) =>
    {
        Assert.Equal([Hog], (await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, start, end, 1, null, null, ct)).Rows.Select(r => r.QueryHash));

        var child = await viewer.ResolveQueryTagFilterAsync("ViewerTagProd / ViewerTagOrders", ct);
        Assert.Equal([Ord], (await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, start, end, 1, null, child, ct)).Rows.Select(r => r.QueryHash));

        var parent = await viewer.ResolveQueryTagFilterAsync("ViewerTagProd", ct);
        Assert.Equal([Ord], (await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, start, end, 10, null, parent, ct)).Rows.Select(r => r.QueryHash));
    });

    [Fact]
    public Task TopQueries_TheUntaggedChoice_ReturnsOnlyRowsWithNoTag() => RunAsync(async (viewer, start, end, ct) =>
    {
        var untagged = await viewer.ResolveQueryTagFilterAsync(ViewerDataService.UntaggedChoice, ct);
        var rows = (await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, start, end, 10, null, untagged, ct)).Rows;
        Assert.Equal([Hog], rows.Select(r => r.QueryHash));
        Assert.All(rows, r => Assert.Null(r.QueryTag));
    });

    [Fact]
    public Task TheOtherGrids_ShowTheTag_AndFilterBeforeTheirCut() => RunAsync(async (viewer, start, end, ct) =>
    {
        var batch = await viewer.ResolveQueryTagFilterAsync("ViewerTagBatch", ct);
        var untagged = await viewer.ResolveQueryTagFilterAsync(ViewerDataService.UntaggedChoice, ct);

        var store = await viewer.GetQueryStoreTopQueriesAsync(ServerId, start, end, 10, null, null, null, ct);
        Assert.Equal("ViewerTagBatch", store.Single(r => r.QueryHash == Bat).QueryTag);
        Assert.Equal([Bat], (await viewer.GetQueryStoreTopQueriesAsync(ServerId, start, end, 1, null, null, batch, ct)).Select(r => r.QueryHash));
        Assert.Equal([Hog], (await viewer.GetQueryStoreTopQueriesAsync(ServerId, start, end, 10, null, null, untagged, ct)).Select(r => r.QueryHash));

        var procs = await viewer.GetTopProceduresByCpuRoutedAsync(ServerId, start, end, 10, null, null, ct);
        Assert.Equal("ViewerTagProd / ViewerTagOrders", procs.Rows.Single(r => r.ObjectName == "usp_VTagOrders").QueryTag);
        var orders = await viewer.ResolveQueryTagFilterAsync("ViewerTagOrders", ct);
        Assert.Equal(["usp_VTagOrders"], (await viewer.GetTopProceduresByCpuRoutedAsync(ServerId, start, end, 1, null, orders, ct)).Rows.Select(r => r.ObjectName));
        Assert.Equal(["usp_VTagPlain"], (await viewer.GetTopProceduresByCpuRoutedAsync(ServerId, start, end, 10, null, untagged, ct)).Rows.Select(r => r.ObjectName));

        var longs = await viewer.GetRecentLongQueryCompletionsAsync(ServerId, start, end, null, null, ct);
        Assert.Equal("ViewerTagBatch", longs.Single(r => r.QueryHash == Bat).QueryTag);
        Assert.Equal([Hog], (await viewer.GetRecentLongQueryCompletionsAsync(ServerId, start, end, null, untagged, ct)).Select(r => r.QueryHash));
    });
}

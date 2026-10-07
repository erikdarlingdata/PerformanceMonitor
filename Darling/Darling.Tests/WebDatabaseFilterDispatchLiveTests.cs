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
using Darling.Tests;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5245 (part of #5244), PR1, the last lane: the six reads the server page's database filter drives, called the way the
/// page calls them, through the web read dispatch (<c>DarlingWebEndpoints.BuildReadDispatch</c>) with REPEATED
/// <c>database_name</c> keys. Each of get_active_queries, get_top_queries_by_cpu, get_top_procedures_by_cpu,
/// get_query_store_top, get_query_store_regressions and get_query_heatmap must answer with the chosen databases' rows
/// and no others. The seed is one row per read in every one of six awkward database names and a plain decoy, so a
/// dispatch that passes one name, none, or splits a name on its comma shows up as a wrong set of databases.
///
/// <para>The names are data, not code: a comma, a closing bracket, a leading space, a quote, a percent sign and a plus,
/// and markup. They bind as one <c>text[]</c> and are never concatenated or put in a LIKE.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class WebDatabaseFilterDispatchLiveTests
{
    private const string ServerName = "w1-5245-web-dispatch";
    private const string Decoy = "WebDispatchDecoy";

    /// <summary>The six awkward names of the plan's L6, in the order they are seeded.</summary>
    private static readonly string[] AwkwardNames =
    [
        "A,B", "x]", " SalesDb", "O'Brien", "50%+off", "<img src=x onerror=alert(1)>",
    ];

    private static readonly string[] AllNames = [.. AwkwardNames, Decoy];

    private static readonly string[] Reads =
    [
        "get_active_queries", "get_top_queries_by_cpu", "get_top_procedures_by_cpu",
        "get_query_store_top", "get_query_store_regressions", "get_query_heatmap",
    ];

    private const string Cleanup =
        "DELETE FROM query_snapshots WHERE server_id = {0}; DELETE FROM query_stats WHERE server_id = {0}; " +
        "DELETE FROM procedure_stats WHERE server_id = {0}; DELETE FROM query_store_stats WHERE server_id = {0}; " +
        "DELETE FROM query_store_health WHERE server_id = {0}";

    private static List<string> Sorted(IEnumerable<string> names) => names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();

    /// <summary>A read through the web dispatch, one <c>database_name</c> key per chosen database.</summary>
    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string read, params string[] databases)
    {
        var query = new List<KeyValuePair<string, string?>>
        {
            new("server_name", ServerName),
            new("hours_back", read is "get_query_store_top" ? "3" : read is "get_active_queries" ? "1" : "24"),
        };
        query.AddRange(databases.Select(d => new KeyValuePair<string, string?>("database_name", d)));
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    /// <summary>The databases a read's answer covers, by the property that carries the rows. The heatmap's cells name a
    /// hash, and each hash was seeded in one database.</summary>
    private static List<string> DatabasesOf(string read, string json, Dictionary<string, string> databaseByHash)
    {
        var root = JsonDocument.Parse(json).RootElement;
        Assert.True(root.TryGetProperty(read switch
        {
            "get_top_procedures_by_cpu" => "procedures",
            "get_query_store_regressions" => "regressions",
            "get_query_heatmap" => "cells",
            _ => "queries",
        }, out var rows), $"{read}: not the success shape: {json}");
        return Sorted(rows.EnumerateArray().Select(r => read == "get_query_heatmap"
            ? databaseByHash[r.GetProperty("top_query_hash").GetString()!]
            : r.GetProperty("database_name").GetString()!));
    }

    [Fact]
    public async Task EachOfTheSixReads_WithTwoChosenDatabases_ReturnsOnlyThoseDatabasesRows_AwkwardNamesIncluded()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live web database filter dispatch tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(ServerName);
        var cleanup = string.Format(System.Globalization.CultureInfo.InvariantCulture, Cleanup, serverId)
                      + string.Format(System.Globalization.CultureInfo.InvariantCulture, "; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var heatmapBin = DarlingMcpTestData.Naive(new DateTime(now.AddHours(-3).Ticks - (now.AddHours(-3).Ticks % TimeSpan.TicksPerHour), DateTimeKind.Utc));
            var databaseByHash = new Dictionary<string, string>();

            for (var i = 0; i < AllNames.Length; i++)
            {
                var db = AllNames[i];
                var tag = "W1" + i;
                var weight = 1_000L * (i + 1);
                /* The heatmap bins every query_stats row, so the top-queries row's hash names its database too. */
                databaseByHash["0xQ" + tag] = db;

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats
                          (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                           query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                           delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                           min_dop, max_dop, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 10, $11, $11, 100, 0, 0, 1, $11, 1, $11, 1, 1, 300)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddHours(-2)), serverId, ServerName, db,
                    "0xQ" + tag, "0xP" + tag, "0xS" + tag, "0xL" + tag, "SELECT " + tag, weight);

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO procedure_stats
                          (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                           delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, 10, $8, $8, 100, 300)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddHours(-2)), serverId, ServerName, db, "usp_" + tag, "0xSQLH" + tag, weight);

                /* Query Store: a baseline capture 40 hours back and a recent capture 30 minutes back that ran 4x slower, so the
                   same two rows feed get_query_store_top (the recent one, in the 3-hour window) and get_query_store_regressions. */
                foreach (var (at, executions, avgUs, intervalId) in new[] { (now.AddHours(-40), 100L, 1000L, 2L), (now.AddMinutes(-30), 200L, 4000L, 1L) })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO query_store_stats
                              (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash,
                               query_text, execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
                               runtime_stats_interval_id, last_execution_time)
                          VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9, 'Regular', $10, $11, $11, 100, $12, $2)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, ServerName, db, (long)(100 + i),
                        "0xQS" + tag, "0xQSP" + tag, "SELECT qs " + tag, executions, avgUs, intervalId);
                }

                /* The heatmap: one hot query per database, each in a 5-minute bin of its own so no two share a cell. */
                var heatHash = "0xHOT" + tag;
                databaseByHash[heatHash] = db;
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats
                          (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                           sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time,
                           delta_logical_reads, delta_logical_writes, query_text)
                      VALUES ($1, $2, $3, $4, $5, $6, 60, 5, 0, 250000, 0, 0, 'SELECT * FROM dbo.Widgets')",
                    CollectionIdGenerator.Next(), heatmapBin.AddMinutes(10 * i), serverId, ServerName, db, heatHash);

                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
                      VALUES ($1,$2,$3,$4,$5,$6,'00 00:00:05.000',$7,'running',0,NULL,0,$8,$8,0,0,2000,0.5,'Read Committed',1,0,'sa','APP01','SSMS',1,0)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-10)), serverId, ServerName, 100 + i, db, "SELECT " + tag + " FROM dbo.Posts", weight);
            }

            foreach (var read in Reads)
            {
                /* Control: with no database_name every read covers all seven, so the seed is visible to it and a filtered
                   answer cannot be empty for lack of data. */
                Assert.Equal(Sorted(AllNames), DatabasesOf(read, await WebReadAsync(postgres, read), databaseByHash));

                /* Three disjoint pairs, so the six awkward names are each chosen once. */
                for (var pair = 0; pair < AwkwardNames.Length; pair += 2)
                {
                    var chosen = new[] { AwkwardNames[pair], AwkwardNames[pair + 1] };
                    var answer = await WebReadAsync(postgres, read, chosen);
                    var got = DatabasesOf(read, answer, databaseByHash);
                    Assert.True(Sorted(chosen).SequenceEqual(got),
                        $"{read} over [{string.Join(" | ", chosen)}] covered [{string.Join(" | ", got)}]");
                }

                /* A request that names a database twice is one database. */
                Assert.Equal([Decoy], DatabasesOf(read, await WebReadAsync(postgres, read, Decoy, Decoy), databaseByHash));

                /* A blank-only list is refused, never widened to "all databases". */
                var refused = JsonDocument.Parse(await WebReadAsync(postgres, read, "  ", "")).RootElement;
                Assert.True(refused.GetProperty("status").GetString() == "invalid", $"{read}: a blank-only database_name must be refused");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}

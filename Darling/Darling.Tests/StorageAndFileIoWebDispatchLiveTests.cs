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
using System.Threading.Tasks;
using Darling.Tests;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5244 PR5 (lane W5): the five storage and file I/O reads the web pages drive (get_database_sizes, get_table_index_sizes,
/// get_pvs_stats, get_file_io_stats and get_file_io_trend), called the way the page calls them, through the web read dispatch
/// with two REPEATED <c>database_name</c> keys over a seed of databases A, B and C. Each read must answer with A's and B's rows
/// and none of C's; on the base the dispatch ignored the key (or, for the trend, read only the first), so each answered with all
/// three databases. The same class pins the <c>database_name</c> echo on the answers each tool gained it on (blank is null) and
/// the three public MCP tools' new parameter.
/// </summary>
[Collection("live-postgres")]
public sealed class StorageAndFileIoWebDispatchLiveTests
{
    private const string A = StorageDatabaseFilterLiveTests.A;
    private const string B = StorageDatabaseFilterLiveTests.B;
    private const string C = StorageDatabaseFilterLiveTests.C;
    private const string Cleanup = StorageDatabaseFilterLiveTests.Cleanup + "; DELETE FROM file_io_stats WHERE server_id = {0}";

    private static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string serverName, string read, params string[] databases)
    {
        var query = new List<KeyValuePair<string, string?>> { new("server_name", serverName), new("hours_back", "24") };
        query.AddRange(databases.Select(d => new KeyValuePair<string, string?>("database_name", d)));
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    private static string? Echo(string json) =>
        JsonDocument.Parse(json).RootElement.TryGetProperty("database_name", out var echo) && echo.ValueKind == JsonValueKind.String ? echo.GetString() : null;

    private static bool HasEchoKey(string json) => JsonDocument.Parse(json).RootElement.TryGetProperty("database_name", out _);

    private static string[] DatabasesIn(string json, string array) =>
        JsonDocument.Parse(json).RootElement.GetProperty(array).EnumerateArray()
            .Select(e => e.GetProperty("database_name").GetString()!).Where(n => n != "(other)").Distinct().Order().ToArray();

    private static Task InsertFileIoAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, int serverId, string serverName, DateTime t, string db) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
     physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
     delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), serverId, serverName, db, db + ".mdf", "ROWS",
            "D:\\data\\" + db + ".mdf", 1000m, 100L, 50L, 819_200L, 409_600L, 500L, 100L, 60);

    [Fact]
    public Task TheFiveReads_TwoRepeatedKeys_ReadOnlyAAndBs() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5w-web-dispatch", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            foreach (var (db, mb) in new[] { (A, 100m), (B, 200m), (C, 300m) })
            {
                await StorageDatabaseFilterLiveTests.InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), db, "T" + db, mb);
                await StorageDatabaseFilterLiveTests.InsertFileAsync(connection, ct, serverId, serverName, now.AddHours(-1), db, mb);
                await StorageDatabaseFilterLiveTests.InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), db, mb);
                await InsertFileIoAsync(connection, ct, serverId, serverName, now.AddMinutes(-30), db);
                await InsertFileIoAsync(connection, ct, serverId, serverName, now.AddMinutes(-5), db);
            }

            foreach (var (read, array) in new[]
                     {
                         ("get_database_sizes", "databases"), ("get_table_index_sizes", "tables"), ("get_pvs_stats", "databases"),
                         ("get_file_io_stats", "files"), ("get_file_io_trend", "trend"),
                     })
            {
                var all = await WebReadAsync(postgres, serverName, read);
                Assert.Equal(new[] { A, B, C }, DatabasesIn(all, array));
                Assert.True(HasEchoKey(all), $"{read} left the database_name echo off its unfiltered answer");
                Assert.Null(Echo(all));

                var both = await WebReadAsync(postgres, serverName, read, A, B);
                Assert.True(new[] { A, B }.SequenceEqual(DatabasesIn(both, array)), $"{read} over A and B read {string.Join(",", DatabasesIn(both, array))}: {both}");
                Assert.Equal("the chosen databases", Echo(both));

                var one = await WebReadAsync(postgres, serverName, read, B);
                Assert.Equal(new[] { B }, DatabasesIn(one, array));
                Assert.Equal(B, Echo(one));
            }

            /* The four public tools that gained database_name take it last, and a blank is every database. */
            var sizes = await DarlingMcpObjectStatsTools.GetDatabaseSizes(postgres, serverName, "  ", ct);
            Assert.Equal(new[] { A, B, C }, DatabasesIn(sizes, "databases"));
            Assert.True(HasEchoKey(sizes));
            Assert.Null(Echo(sizes));
            Assert.Equal(new[] { A },DatabasesIn(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, serverName, A, ct), "tables"));
            Assert.Equal(new[] { C }, DatabasesIn(await DarlingMcpPvsTools.GetPvsStats(postgres, serverName, 0, C, ct), "databases"));
            Assert.Equal(new[] { B }, DatabasesIn(await DarlingMcpDataTools.GetFileIoStats(postgres, serverName, B, ct), "files"));
        }, Cleanup);

    /// <summary>The empty answers of the three tools that gained <c>database_name</c> say which databases they were limited to.</summary>
    [Fact]
    public Task TheEmptyAnswers_CarryTheEcho() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5w-echo", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await StorageDatabaseFilterLiveTests.InsertTableAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, "TA", 100m);
            await StorageDatabaseFilterLiveTests.InsertFileAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, 100m);
            await StorageDatabaseFilterLiveTests.InsertPvsAsync(connection, ct, serverId, serverName, now.AddHours(-1), A, 100m);

            /* A snapshot exists, the chosen database is not in it: empty, naming the database. */
            foreach (var read in new[] { "get_database_sizes", "get_table_index_sizes", "get_pvs_stats" })
            {
                var one = await WebReadAsync(postgres, serverName, read, "NoSuchDb");
                Assert.Equal("empty", DarlingMcpTestData.StatusOf(one));
                Assert.Equal("NoSuchDb", Echo(one));
                var many = await WebReadAsync(postgres, serverName, read, "NoX", "NoY");
                Assert.Equal("empty", DarlingMcpTestData.StatusOf(many));
                Assert.Equal("the chosen databases", Echo(many));
            }
        }, Cleanup);

    /// <summary>A server with no data at all: the no-data answers carry the echo too, null for every database.</summary>
    [Fact]
    public Task TheNoDataAnswers_CarryTheEcho() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("a5244p5w-echo-none", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await Task.CompletedTask;
            foreach (var read in new[] { "get_database_sizes", "get_table_index_sizes", "get_pvs_stats" })
            {
                var none = await WebReadAsync(postgres, serverName, read, A, B);
                Assert.Equal("the chosen databases", Echo(none));
                var noneAll = await WebReadAsync(postgres, serverName, read);
                Assert.True(HasEchoKey(noneAll), $"{read}'s no-data answer for every database has no database_name key: {noneAll}");
                Assert.Null(Echo(noneAll));
            }
        }, Cleanup);
}

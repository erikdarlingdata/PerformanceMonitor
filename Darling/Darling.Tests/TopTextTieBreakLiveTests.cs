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
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5299 round 2 (N3 and N6): a newest-text pick and a top statement are deterministic when two rows tie. Two rows of one key at the
/// same <c>collection_time</c> with different texts (one shape hash, two plans, one collection) used to give an arbitrary text, which
/// also decides the WAITFOR trim; two groups with an exactly equal ranking value used to come out in an arbitrary order, so a refill
/// round could pick different members. The text now breaks the tie on <c>collection_id DESC</c> (the row collected last, unique per
/// row) and the ranking on the group key. Each case seeds the tie so the arbitrary answer is the WRONG one (the lower id is inserted
/// first, and a heap scan returns it first), reads the statement three times, and asserts the same answer each time.
/// </summary>
[Collection("live-postgres")]
public sealed class TopTextTieBreakLiveTests
{
    private const string TextServer = "a5299-tie-text";
    private const string StoreServer = "a5299-tie-store";

    private static NpgsqlParameter P(NpgsqlDbType type, object? value) => new() { NpgsqlDbType = type, Value = value ?? DBNull.Value };

    private const string StatsColumns = @"(collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                           query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                           delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                           min_dop, max_dop, sample_interval_seconds)";

    private static async Task PlantStatAsync(NpgsqlConnection connection, int serverId, string serverName, string hash, DateTime at, string? text, long cpu, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO query_stats " + StatsColumns + " VALUES ($1, $2, $3, $4, 'D1', $5, '0xP', '0xS', '0xL', $6, 10, $7, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600)",
            CollectionIdGenerator.Next(), at, serverId, serverName, hash, text, cpu);

    private static async Task PlantStoreAsync(NpgsqlConnection connection, int serverId, string serverName, long queryId, DateTime at, string text, CancellationToken ct, long? collectionId = null) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, min_dop, max_dop, last_execution_time)
              VALUES ($1,$2,$3,$4,'D1',$5,7,'Regular','0xQH','0xPLAN',$6,10,5000,4000,800,0,40,250,1,4,$2)",
            collectionId ?? CollectionIdGenerator.Next(), at, serverId, serverName, queryId, text);

    /// <summary>Runs the statement and returns, per page row, the group key text and the representative text.</summary>
    private static async Task<List<(string? Key, string? Text)>> ReadPageAsync(NpgsqlConnection connection, string sql, string keyColumn, CancellationToken ct,
        int serverId, DateTime start, DateTime end, params NpgsqlParameter[] rest)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.Add(P(NpgsqlDbType.Integer, serverId));
        command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified)));
        command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified)));
        foreach (var p in rest)
        {
            command.Parameters.Add(p);
        }

        var rows = new List<(string?, string?)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        var page = reader.GetOrdinal("page_ord");
        var key = reader.GetOrdinal(keyColumn);
        var text = reader.GetOrdinal("query_text");
        while (await reader.ReadAsync(ct))
        {
            if (!reader.IsDBNull(page))
            {
                rows.Add((reader.IsDBNull(key) ? null : Convert.ToString(reader.GetValue(key), CultureInfo.InvariantCulture),
                          reader.IsDBNull(text) ? null : reader.GetString(text)));
            }
        }

        return rows;
    }

    private static async Task WithServerAsync(string serverName, Func<NpgsqlConnection, int, DateTime, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tie-break tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var cleanup = string.Format(CultureInfo.InvariantCulture,
            "DELETE FROM query_stats WHERE server_id = {0}; DELETE FROM query_store_stats WHERE server_id = {0}; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
            await body(connection, serverId, DarlingMcpTestData.Naive(DateTime.UtcNow), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }

    [Fact]
    public Task TheRawAndViewerStatements_PickTheTextOfTheRowCollectedLast_WhenTwoRowsShareATime() =>
        WithServerAsync(TextServer, async (connection, serverId, now, ct) =>
        {
            var tie = now.AddHours(-2);
            await PlantStatAsync(connection, serverId, TextServer, "0xT1", tie, "tie text A (lower collection id)", 5000, ct);
            await PlantStatAsync(connection, serverId, TextServer, "0xT1", tie, "tie text B (higher collection id)", 5000, ct);

            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);
            for (var run = 0; run < 3; run++)
            {
                foreach (var sql in new[] { DarlingDataReader.TopQueriesSql, DarlingDataReader.TopQueriesByHostObjectSql })
                {
                    var page = await ReadPageAsync(connection, TopRankings.Apply(sql, TopRanking.Cpu, hourly: false), "query_hash", ct, serverId, start, end,
                        P(NpgsqlDbType.Integer, 5), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 0), P(NpgsqlDbType.Integer, 10));
                    Assert.Equal("tie text B (higher collection id)", Assert.Single(page).Text);
                }

                var viewer = await ReadPageAsync(connection, ViewerDataService.TopQueriesSql, "query_hash", ct, serverId, start, end,
                    P(NpgsqlDbType.Integer, 5), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 10));
                Assert.Equal("tie text B (higher collection id)", Assert.Single(viewer).Text);
            }
        });

    [Fact]
    public Task TheHourlyStatement_PicksTheTextOfTheRowCollectedLast_InTheWindowAndInTheFallback() =>
        WithServerAsync(TextServer, async (connection, serverId, now, ct) =>
        {
            /* In the window: two rows at one time. In the fallback: the key's window row has no text, and two rows ten days back share a time. */
            await PlantStatAsync(connection, serverId, TextServer, "0xT1", now.AddHours(-2), "window text A", 5000, ct);
            await PlantStatAsync(connection, serverId, TextServer, "0xT1", now.AddHours(-2), "window text B", 5000, ct);
            await PlantStatAsync(connection, serverId, TextServer, "0xT2", now.AddHours(-3), null, 4000, ct);
            await PlantStatAsync(connection, serverId, TextServer, "0xT2", now.AddDays(-10), "old text A", 4000, ct);
            await PlantStatAsync(connection, serverId, TextServer, "0xT2", now.AddDays(-10), "old text B", 4000, ct);

            var standIn = "(SELECT server_id, collection_time AS bucket, database_name, query_hash, sql_handle, " +
                "delta_execution_count AS execution_count_sum, delta_worker_time AS worker_time_sum, " +
                "delta_elapsed_time AS elapsed_time_sum FROM collect.query_stats WHERE collection_time >= '" +
                now.AddDays(-2).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "') AS f";
            var sql = TopRankings.Apply(DarlingDataReader.TopQueriesHourlySql, TopRanking.Cpu, hourly: true)
                .Replace("$FROM$", standIn, StringComparison.Ordinal)
                .Replace("$CEIL$", "", StringComparison.Ordinal);
            for (var run = 0; run < 3; run++)
            {
                var page = await ReadPageAsync(connection, sql, "query_hash", ct, serverId, now.AddHours(-24), now.AddMinutes(5),
                    P(NpgsqlDbType.Integer, 5), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 10));
                Assert.Equal(2, page.Count);
                Assert.Equal("window text B", page.Find(r => r.Key == "0xT1").Text);
                Assert.Equal("old text B", page.Find(r => r.Key == "0xT2").Text);
            }
        });

    [Fact]
    public Task TheQueryStoreStatements_PickTheTextOfTheRowCollectedLast_AndTheLowestKeyOnATie() =>
        WithServerAsync(StoreServer, async (connection, serverId, now, ct) =>
        {
            var tie = now.AddHours(-2);
            /* Query 20 is inserted before query 10, and both rank equally: the page of one must be query 10 on every read. */
            await PlantStoreAsync(connection, serverId, StoreServer, 20, tie, "store text 20", ct);
            await PlantStoreAsync(connection, serverId, StoreServer, 10, tie, "store text 10 A (lower collection id)", ct);
            await PlantStoreAsync(connection, serverId, StoreServer, 10, tie, "store text 10 B (higher collection id)", ct);
            /* Query 11's rows go in the other way round (the higher collection id first), so neither insertion order can be the answer. */
            var baseId = CollectionIdGenerator.Next() + 1_000_000;
            await PlantStoreAsync(connection, serverId, StoreServer, 11, tie, "store text 11 B (higher collection id)", ct, baseId + 2);
            await PlantStoreAsync(connection, serverId, StoreServer, 11, tie, "store text 11 A (lower collection id)", ct, baseId + 1);

            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);
            for (var run = 0; run < 3; run++)
            {
                var page = await ReadPageAsync(connection, DarlingDataReader.QueryStoreTopSql, "query_id", ct, serverId, start, end,
                    P(NpgsqlDbType.Integer, 1), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Text, null), P(NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 1));
                Assert.Equal("10", Assert.Single(page).Key);
                var viewerOne = await ReadPageAsync(connection, ViewerDataService.QueryStoreTopSql, "query_id", ct, serverId, start, end,
                    P(NpgsqlDbType.Integer, 1), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 1));
                Assert.Equal("10", Assert.Single(viewerOne).Key);

                var all = await ReadPageAsync(connection, DarlingDataReader.QueryStoreTopSql, "query_id", ct, serverId, start, end,
                    P(NpgsqlDbType.Integer, 3), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Text, null), P(NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 3));
                var viewerAll = await ReadPageAsync(connection, ViewerDataService.QueryStoreTopSql, "query_id", ct, serverId, start, end,
                    P(NpgsqlDbType.Integer, 3), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 3));
                foreach (var rows in new[] { all, viewerAll })
                {
                    Assert.Equal(new string?[] { "10", "11", "20" }, rows.ConvertAll(r => r.Key).ToArray());
                    Assert.Equal("store text 10 B (higher collection id)", rows[0].Text);
                    Assert.Equal("store text 11 B (higher collection id)", rows[1].Text);
                }
            }
        });

    [Fact]
    public Task TheViewerTopQueries_BreaksAnExactTieOnTheGroupKey() =>
        WithServerAsync(TextServer, async (connection, serverId, now, ct) =>
        {
            /* Two groups with equal elapsed time; the higher hash is inserted first. One candidate: the lowest key must win every time. */
            await PlantStatAsync(connection, serverId, TextServer, "0xT9", now.AddHours(-2), "viewer text 9", 5000, ct);
            await PlantStatAsync(connection, serverId, TextServer, "0xT3", now.AddHours(-2), "viewer text 3", 5000, ct);

            for (var run = 0; run < 3; run++)
            {
                var page = await ReadPageAsync(connection, ViewerDataService.TopQueriesSql, "query_hash", ct, serverId, now.AddHours(-24), now.AddMinutes(5),
                    P(NpgsqlDbType.Integer, 1), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 1));
                Assert.Equal(("0xT3", "viewer text 3"), Assert.Single(page));
            }
        });
}

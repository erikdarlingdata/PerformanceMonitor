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
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #9147 part E: the query-stats duration trend reads the per-collection summary (V286) for a server whose summary covers the whole
/// window, and reads raw otherwise. The summary route must give the raw route's answer for every database filter, with a NULL database,
/// ties and a collection that holds rows of several databases. Raw rows are planted directly, the summary is written by the collector's
/// own writer, so a summary row is exactly what the collector would have stored.
/// </summary>
[Collection("live-postgres")]
public sealed class DurationTrendCollectionSummaryLiveTests
{
    private const string A = "SummaryA";
    private const string B = "SummaryB";

    private const string Cleanup =
        "DELETE FROM query_stats WHERE server_id = {0}; DELETE FROM collect.query_stats_collection WHERE server_id = {0}; DELETE FROM collect.query_stats_collection_floor WHERE server_id = {0}";

    private static Task PlantAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string? db, string hash, long weight) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, '0xP', '0xS', '0xL', 'SELECT 1', decode('00', 'hex'), 10, $7, $7, 100, 0, 0, 1, $7, 1, $7, 1, 1, 1200)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, (object?)db ?? DBNull.Value, hash, weight);

    private static async Task WriteSummaryAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime at)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await QueryStatsCollectionSummary.UpsertBatchAsync(connection, transaction, serverId, DarlingMcpTestData.TruncateToSeconds(at), 1, 30, null, ct);
        await transaction.CommitAsync(ct);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static string Stamp(DateTime at) => DarlingMcpTestData.TruncateToSeconds(at).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static async Task<string> ReadAsync(NpgsqlDataSource postgres, int serverId, DateTime now, DatabaseFilter filter, CancellationToken ct, DateTime? windowStart = null)
    {
        var start = windowStart ?? now.AddHours(-24);
        var end = now.AddMinutes(5);
        var rollups = await ComposeStoreAvailability.GetRollupsAsync(postgres, ct);
        var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(start, rollups.Item1, rollups.Item2, windowEndUtc: end);
        Assert.Equal(RetentionTier.Raw, route.Tier);
        var result = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, serverId, start, end, route, 1, filter, ct);
        return JsonSerializer.Serialize(result.Points);
    }

    /// <summary>30 collections across the window: A twice (a tie), the NULL database once, and every third collection also B.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime now)
    {
        for (var k = 0; k < 30; k++)
        {
            var at = now.AddHours(-26).AddMinutes(k * 50);
            await PlantAsync(connection, ct, serverId, serverName, at, A, "0xQ1", 3_000 + k);
            await PlantAsync(connection, ct, serverId, serverName, at, A, "0xQ2", 3_000 + k);
            await PlantAsync(connection, ct, serverId, serverName, at, null, "0xQ3", 500 + k);
            if (k % 3 == 0)
            {
                await PlantAsync(connection, ct, serverId, serverName, at, B, "0xQ4", 7_000 + k);
            }

            await WriteSummaryAsync(connection, ct, serverId, at);
        }
    }

    [Fact]
    public Task TheSummaryRoute_GivesTheRawRoutesAnswer_ForEveryFilter_AndNeverReadsRawRows() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-route-equals-raw", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);
            var filters = new[] { DatabaseFilter.All, DatabaseFilter.One(A), DatabaseFilter.One(B), DatabaseFilter.Of(new[] { A, B }) };

            var fromSummary = new List<string>();
            foreach (var filter in filters)
            {
                fromSummary.Add(await ReadAsync(postgres, serverId, now, filter, ct));
            }

            /* Tamper proof: the route really reads the summary, because a changed summary row changes the answer. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.query_stats_collection SET sum_elapsed = sum_elapsed * 100 WHERE server_id = $1", serverId);
            Assert.NotEqual(fromSummary[0], await ReadAsync(postgres, serverId, now, DatabaseFilter.All, ct));
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection WHERE server_id = $1", serverId);

            var fromRaw = new List<string>();
            foreach (var filter in filters)
            {
                fromRaw.Add(await ReadAsync(postgres, serverId, now, filter, ct));
            }

            for (var i = 0; i < filters.Length; i++)
            {
                Assert.Equal(fromRaw[i], fromSummary[i]);
            }

            /* The summary alone answers: rebuild it from the rows, remove the rows, read again. */
            for (var k = 0; k < 30; k++)
            {
                await WriteSummaryAsync(connection, ct, serverId, now.AddHours(-26).AddMinutes(k * 50));
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", serverId);
            for (var i = 0; i < filters.Length; i++)
            {
                Assert.Equal(fromRaw[i], await ReadAsync(postgres, serverId, now, filters[i], ct));
            }
        }, Cleanup);

    [Fact]
    public Task AServerWhoseSummaryStartsAfterTheWindowStart_ReadsRaw_AndALateBatchUpdatesItsOwnRow() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-route-floor", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);
            var raw = await ReadAsync(postgres, serverId, now, DatabaseFilter.All, ct);

            /* A late batch: one more row lands in an old collection, and the writer's second run sets that collection's row. */
            var old = now.AddHours(-26).AddMinutes(10 * 50);
            await PlantAsync(connection, ct, serverId, serverName, old, A, "0xQ9", 1);
            await WriteSummaryAsync(connection, ct, serverId, old);
            Assert.Equal(3, await ScalarAsync(connection, ct, $"SELECT n FROM collect.query_stats_collection WHERE server_id = {serverId} AND database_name = '{A}' AND collection_time = '{Stamp(old)}'"));
            Assert.Equal(2, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.query_stats_collection WHERE server_id = {serverId} AND collection_time = '{Stamp(old)}'"));

            /* The summary starts after the window start, so it does not cover the window and the read ignores it: a tampered summary changes nothing. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.query_stats_collection SET sum_elapsed = sum_elapsed * 100 WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection WHERE server_id = $1 AND collection_time < $2", serverId, now.AddHours(-23));
            var withHole = await ReadAsync(postgres, serverId, now, DatabaseFilter.All, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection WHERE server_id = $1", serverId);
            Assert.Equal(await ReadAsync(postgres, serverId, now, DatabaseFilter.All, ct), withHole);
            Assert.Equal(raw.Length > 2, withHole.Length > 2);
        }, Cleanup);

    [Fact]
    public Task ACollectionAnOlderBuildLeftWithoutASummaryRow_MovesTheServersFloorToTheNextBatch_AndDeletesNoSummaryRow() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-route-hole", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var t1 = now.AddHours(-3);
            var t2 = now.AddHours(-2);
            var t3 = now.AddHours(-1);
            await PlantAsync(connection, ct, serverId, serverName, t1, A, "0xQ1", 10);
            await WriteSummaryAsync(connection, ct, serverId, t1);
            await PlantAsync(connection, ct, serverId, serverName, t2, A, "0xQ1", 10);
            await PlantAsync(connection, ct, serverId, serverName, t3, A, "0xQ1", 10);
            await WriteSummaryAsync(connection, ct, serverId, t3);

            /* #9193: the batch that finds the hole writes the server's floor at its own time and deletes nothing, so the older row stays. */
            Assert.Equal(2, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.query_stats_collection WHERE server_id = {serverId}"));
            Assert.Equal(1, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.query_stats_collection WHERE server_id = {serverId} AND collection_time = '{Stamp(t3)}'"));
            Assert.Equal(1, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.query_stats_collection_floor WHERE server_id = {serverId} AND valid_from = '{Stamp(t3)}'"));
        }, Cleanup);

    [Fact]
    public Task AfterAHole_TheTrendReadsRawForAWindowStartingBeforeTheFloor_AndTheSummaryForOneStartingAfterIt_AndBothEqualRaw() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-route-floor-read", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            /* The summary is whole through the seed's last collection (about 1.8 hours before now). Then an older build wrote two raw collections
               with no summary row, and the first batch of this build, at -60 min, found the hole and wrote the floor. */
            await SeedAsync(connection, ct, serverId, serverName, now);
            await PlantAsync(connection, ct, serverId, serverName, now.AddMinutes(-100), A, "0xQ1", 4_000);
            await PlantAsync(connection, ct, serverId, serverName, now.AddMinutes(-80), A, "0xQ1", 5_000);
            await PlantAsync(connection, ct, serverId, serverName, now.AddMinutes(-80), B, "0xQ4", 6_000);
            var floor = now.AddMinutes(-60);
            foreach (var at in new[] { floor, now.AddMinutes(-40), now.AddMinutes(-20) })
            {
                await PlantAsync(connection, ct, serverId, serverName, at, A, "0xQ1", 7_000);
                await PlantAsync(connection, ct, serverId, serverName, at, B, "0xQ4", 8_000);
                await WriteSummaryAsync(connection, ct, serverId, at);
            }

            Assert.Equal(1, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.query_stats_collection_floor WHERE server_id = {serverId} AND valid_from = '{Stamp(floor)}'"));
            var beforeFloor = now.AddHours(-24);
            var afterFloor = now.AddMinutes(-50);
            var filters = new[] { DatabaseFilter.All, DatabaseFilter.One(A), DatabaseFilter.One(B) };
            var viaFloor = new List<(string Before, string After)>();
            foreach (var filter in filters)
            {
                viaFloor.Add((await ReadAsync(postgres, serverId, now, filter, ct, beforeFloor), await ReadAsync(postgres, serverId, now, filter, ct, afterFloor)));
            }

            /* Which table each window read: a tampered summary changes only the window that starts after the floor. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.query_stats_collection SET sum_elapsed = sum_elapsed * 100 WHERE server_id = $1", serverId);
            Assert.Equal(viaFloor[0].Before, await ReadAsync(postgres, serverId, now, DatabaseFilter.All, ct, beforeFloor));
            Assert.NotEqual(viaFloor[0].After, await ReadAsync(postgres, serverId, now, DatabaseFilter.All, ct, afterFloor));

            /* Both windows give a pure raw read's answer, with no summary and no floor. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection WHERE server_id = $1", serverId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection_floor WHERE server_id = $1", serverId);
            for (var i = 0; i < filters.Length; i++)
            {
                Assert.Equal(await ReadAsync(postgres, serverId, now, filters[i], ct, beforeFloor), viaFloor[i].Before);
                Assert.Equal(await ReadAsync(postgres, serverId, now, filters[i], ct, afterFloor), viaFloor[i].After);
            }

            Assert.True(viaFloor[0].Before.Length > 2 && viaFloor[0].After.Length > 2);
        }, Cleanup);

    [Fact]
    public Task ARolledBackServiceThatLeftRawCollectionsNewerThanTheSummary_ReadsRaw_AndKeepsThoseCollections() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-route-newer-raw", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            /* The summary is whole up to T (the seed's last collection). Then an older build wrote raw collections after T with no summary
               row, and no batch of this build has run since, so the floor has not moved. A window that ends after T must still show them. */
            await SeedAsync(connection, ct, serverId, serverName, now);
            await PlantAsync(connection, ct, serverId, serverName, now.AddMinutes(-90), A, "0xQ1", 4_000);
            await PlantAsync(connection, ct, serverId, serverName, now.AddMinutes(-30), A, "0xQ1", 5_000);
            await PlantAsync(connection, ct, serverId, serverName, now.AddMinutes(-30), B, "0xQ4", 6_000);

            var filters = new[] { DatabaseFilter.All, DatabaseFilter.One(A), DatabaseFilter.One(B) };
            var withSummary = new List<string>();
            foreach (var filter in filters)
            {
                withSummary.Add(await ReadAsync(postgres, serverId, now, filter, ct));
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection WHERE server_id = $1", serverId);
            for (var i = 0; i < filters.Length; i++)
            {
                Assert.Equal(await ReadAsync(postgres, serverId, now, filters[i], ct), withSummary[i]);
            }
        }, Cleanup);

    [Fact]
    public Task TheWritersDatabaseLimit_SetsOnlyTheBatchsDatabases_ThroughTheRunnersOwnNameList() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-writer-database-limit", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var at = DarlingMcpTestData.TruncateToSeconds(now.AddHours(-2));

            static List<QueryStatsCollector.Row> RowsOf(params string?[] databases) =>
                databases.Select(static database => new QueryStatsCollector.Row { DatabaseName = database }).ToList();

            async Task WriteBatchAsync(List<QueryStatsCollector.Row> rows)
            {
                await using var transaction = await connection.BeginTransactionAsync(ct);
                await QueryStatsCollectionSummary.UpsertBatchAsync(
                    connection, transaction, serverId, at, rows.Count, 30, QueryStatsCollectionSummary.DatabaseNamesOf(rows), ct);
                await transaction.CommitAsync(ct);
            }

            /* The rows the first batch stored: two named databases and the NULL database, a tie inside A. */
            await PlantAsync(connection, ct, serverId, serverName, at, A, "0xQ1", 100);
            await PlantAsync(connection, ct, serverId, serverName, at, A, "0xQ2", 200);
            await PlantAsync(connection, ct, serverId, serverName, at, B, "0xQ4", 300);
            await PlantAsync(connection, ct, serverId, serverName, at, null, "0xQ3", 400);
            await WriteBatchAsync(RowsOf(A, A, B, null));

            /* Every summary row equals GROUP BY over the raw rows of the collection (NULL-safe on the database). */
            var mismatch = $@"SELECT count(*) FROM
                (
                    (SELECT database_name, count(*), sum(delta_elapsed_time)::bigint, sum(delta_execution_count)::bigint, max(sample_interval_seconds)
                     FROM query_stats WHERE server_id = {serverId} AND collection_time = '{Stamp(at)}' GROUP BY database_name
                     EXCEPT
                     SELECT database_name, n, sum_elapsed, sum_exec, max_interval
                     FROM collect.query_stats_collection WHERE server_id = {serverId} AND collection_time = '{Stamp(at)}')
                    UNION ALL
                    (SELECT database_name, n, sum_elapsed, sum_exec, max_interval
                     FROM collect.query_stats_collection WHERE server_id = {serverId} AND collection_time = '{Stamp(at)}'
                     EXCEPT
                     SELECT database_name, count(*), sum(delta_elapsed_time)::bigint, sum(delta_execution_count)::bigint, max(sample_interval_seconds)
                     FROM query_stats WHERE server_id = {serverId} AND collection_time = '{Stamp(at)}' GROUP BY database_name)
                ) AS d";
            Assert.Equal(0, await ScalarAsync(connection, ct, mismatch));
            Assert.Equal(3, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.query_stats_collection WHERE server_id = {serverId}"));

            /* A second batch at the same collection time holds one database only. B's and the NULL database's summary rows are set aside
               to a marker value: only a batch that reads them back would move them. A's row follows its new raw row. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE collect.query_stats_collection SET sum_elapsed = -1 WHERE server_id = $1 AND database_name IS DISTINCT FROM $2", serverId, A);
            await PlantAsync(connection, ct, serverId, serverName, at, A, "0xQ5", 50);
            await WriteBatchAsync(RowsOf(A));

            Assert.Equal(3, await ScalarAsync(connection, ct, $"SELECT n FROM collect.query_stats_collection WHERE server_id = {serverId} AND database_name = '{A}'"));
            Assert.Equal(350, await ScalarAsync(connection, ct, $"SELECT sum_elapsed FROM collect.query_stats_collection WHERE server_id = {serverId} AND database_name = '{A}'"));
            Assert.Equal(2, await ScalarAsync(connection, ct, $"SELECT count(*) FROM collect.query_stats_collection WHERE server_id = {serverId} AND sum_elapsed = -1"));
        }, Cleanup);

    private static Task PlantRateRowAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string hash, long executions, long elapsedUs, int? interval) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, '0xP', '0xS', '0xL', 'SELECT 1', decode('00', 'hex'), $7, $8, $8, 100, 0, 0, 1, $8, 1, $8, 1, 1, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, A, hash, executions, elapsedUs, (object?)interval ?? DBNull.Value);

    /// <summary>
    /// #9147, and the behaviour the two text tests on the duration and execution-count statements stood for: both trends rate a collection over
    /// its gap to the previous collection (capped at the collector's 3600 s, never the stored interval of any one query), keep the stored
    /// interval only for the first collection read, leave a restart (every row 0) and a gap past the cap unrated, sum the elapsed time for the
    /// duration series, and count only executions for the execution series. Proved with fixed numbers on both routes: the summary with the raw
    /// rows still stored, the raw rows alone (the summary rows removed), and the summary alone. The viewer reads a short window (one point per
    /// collection) and the MCP reader reads a window that also holds a collection after a two hour gap.
    /// </summary>
    [Fact]
    public Task BothTrends_RateOverTheGapToThePreviousCollection_OnTheRawRoute_AndTheSummaryRoute() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-route-rates", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var t1 = DarlingMcpTestData.TruncateToSeconds(now.AddHours(-5));
            var t0 = t1.AddHours(-2);       /* before the hour the LAG reads, so the first collection read has no previous one; it holds the summary floor */
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);
            var t4 = t3.AddMinutes(5);
            var t5 = t4.AddHours(2);        /* past the 3600 s cap */
            var all = new[] { t0, t1, t2, t3, t4, t5 };

            async Task PlantCollectionAsync(DateTime at)
            {
                if (at == t0) { await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xQ0", 3, 30_000, 600); }
                if (at == t1)
                {
                    await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xQ1", 40, 800_000, 900);
                    await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xQ2", 5, 100_000, 300);
                }

                if (at == t2) { await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xQ1", 30, 600_000, null); }
                if (at == t3) { await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xQ1", 0, 0, 0); }
                if (at == t4)
                {
                    await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xQ1", 24, 1_200_000, 120);
                    await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xNEW", 0, 0, 0);
                }

                if (at == t5) { await PlantRateRowAsync(connection, ct, serverId, serverName, at, "0xQ1", 12, 600_000, 60); }
            }

            async Task CheckAsync(string route)
            {
                /* The viewer: a point per collection with a rate; t3 (restart) is dropped. Seconds: t1 the stored 900 (no previous collection
                   read), t2 and t4 the 300 s gaps (t4's stored 120 s is not used). */
                await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!);
                var viewerEnd = t4.AddMinutes(1);
                var duration = await viewer.GetQueryDurationTrendAsync(serverId, t1.AddMinutes(-1), viewerEnd, nowUtc: viewerEnd, cancellationToken: ct);
                Assert.Equal(RetentionTier.Raw, duration.Tier);
                Assert.True(new[] { t1, t2, t4 }.SequenceEqual(duration.Points.Select(p => p.CollectionTime)), route);
                Assert.True(new[] { 1.0, 2.0, 4.0 }.SequenceEqual(duration.Points.Select(p => Math.Round(p.Value, 6))), route);      /* elapsed ms / seconds: 900/900, 600/300, 1200/300 */
                var executions = await viewer.GetExecutionCountTrendAsync(serverId, t1.AddMinutes(-1), viewerEnd, nowUtc: viewerEnd, cancellationToken: ct);
                Assert.True(new[] { t1, t2, t4 }.SequenceEqual(executions.Points.Select(p => p.CollectionTime)), route);
                Assert.True(new[] { 0.05, 0.1, 0.08 }.SequenceEqual(executions.Points.Select(p => Math.Round(p.Value, 6))), route); /* executions / seconds: 45/900, 30/300, 24/300 */

                /* The MCP reader, one-minute buckets: every collection is a point; the restart (t3) and the collection after the two hour gap (t5)
                   are unrated points, not zeros. */
                var start = t1.AddMinutes(-1);
                var end = t5.AddMinutes(1);
                var rollups = await ComposeStoreAvailability.GetRollupsAsync(postgres, ct);
                var tierRoute = DarlingTrendReader.ResolveQueryDurationTrendRoute(start, rollups.Item1, rollups.Item2, windowEndUtc: end);
                Assert.Equal(RetentionTier.Raw, tierRoute.Tier);
                var points = (await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, serverId, start, end, tierRoute, 1, DatabaseFilter.All, ct)).Points;
                Assert.Equal(5, points.Count);
                Assert.True(new double?[] { 1.0, 2.0, null, 4.0, null }.SequenceEqual(points.Select(p => p.Value.HasValue ? Math.Round(p.Value.Value, 6) : (double?)null)), route);
                Assert.True(new double?[] { 0.05, 0.1, null, 0.08, null }.SequenceEqual(points.Select(p => p.ExecutionsPerSecond.HasValue ? Math.Round(p.ExecutionsPerSecond.Value, 6) : (double?)null)), route);
            }

            foreach (var at in all)
            {
                await PlantCollectionAsync(at);
                await WriteSummaryAsync(connection, ct, serverId, at);
            }

            await CheckAsync("summary, raw rows still stored");

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection WHERE server_id = $1", serverId);
            await CheckAsync("raw, no summary");

            foreach (var at in all)
            {
                await WriteSummaryAsync(connection, ct, serverId, at);
            }

            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", serverId);
            await CheckAsync("summary alone");
        }, Cleanup);

    [Fact]
    public Task TheSummaryRoute_ReadsFewerBuffersThanTheRawRoute() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("summary-route-buffers", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            for (var k = 0; k < 40; k++)
            {
                var at = DarlingMcpTestData.TruncateToSeconds(now.AddHours(-26).AddMinutes(k * 40));
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats
                          (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                           query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                           delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                           min_dop, max_dop, sample_interval_seconds)
                      SELECT $1 * 1000 + g, $2, $3, $4, 'Bulk' || (g % 5), '0xQ' || g, '0xP', '0xS', '0xL', repeat('x', 300), decode('00', 'hex'), 10, 100, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 1200
                      FROM generate_series(1, 300) AS g",
                    (long)k, at, serverId, serverName);
                await WriteSummaryAsync(connection, ct, serverId, at);
            }

            var summaryBuffers = await BuffersAsync(connection, ct, serverId, now);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats_collection WHERE server_id = $1", serverId);
            var rawBuffers = await BuffersAsync(connection, ct, serverId, now);
            Assert.True(summaryBuffers * 3 < rawBuffers, $"summary {summaryBuffers} buffers, raw {rawBuffers}");
        }, Cleanup);

    private static async Task<long> BuffersAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime now)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingTrendReader.QueryDurationTrendFilteredSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddHours(-24) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now.AddMinutes(5) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 60 });
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        var plan = JsonDocument.Parse(json).RootElement[0].GetProperty("Plan");
        return plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();
    }
}

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
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5244, PR4 lane R1: the database filter on the three duration trends (get_query_duration_trend,
/// get_procedure_duration_trend, get_query_store_duration_trend). Each trend is one series summed over the databases it
/// reads, so the databases a call covered show in the VALUE: A, B and C each plant a different weight, and a call with
/// [A, B] must read exactly A's plus B's work, never C's. The internal overloads take a <see cref="DatabaseFilter"/>; the
/// public methods still pass <see cref="DatabaseFilter.All"/>, so there is no MCP schema, dispatch or tools/list change.
///
/// <para>This class is the shared-store half (the raw tier and Query Store's raw-only route);
/// <see cref="DurationTrendDatabaseFilterHourlyLiveTests"/> is the half that mints its own database for the hourly rollup
/// tier.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DurationTrendDatabaseFilterLiveTests
{
    internal const string A = "TrendFilterA";
    internal const string B = "TrendFilterB";
    internal const string C = "TrendFilterC";

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    /// <summary>The rate of the series' last point (the elapsed rate, ms per second), or null when it has none.</summary>
    internal static double? LastRate(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("trend").EnumerateArray().Last().GetProperty("elapsed_ms_per_second") is { ValueKind: JsonValueKind.Number } n
            ? n.GetDouble()
            : null;

    internal static Task PlantQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, string text, long weight) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 10, $12, $12, 100, 0, 0, 1, $12, 1, $12, 1, 1, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db,
            "0xQ" + db, "0xP" + db, "0xS" + db, "0xL" + db, text, SHA256.HashData(Encoding.UTF8.GetBytes(text)), weight);

    internal static Task PlantProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, long weight) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats
                  (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                   delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, 10, $8, $8, 100, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db, "usp_" + db, "0xSQLH" + db, weight);

    internal static Task PlantQueryStoreAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, long durationUs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
              VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9, 10, $10, 800, $2)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db, (long)(10 + db[^1]),
            "0xQ" + db, "0xP" + db, "SELECT qs " + db, durationUs);

    internal const string Cleanup =
        "DELETE FROM query_stats WHERE server_id = {0}; DELETE FROM procedure_stats WHERE server_id = {0}; " +
        "DELETE FROM query_store_stats WHERE server_id = {0}; DELETE FROM query_store_health WHERE server_id = {0}";

    [Fact]
    public Task TheRawTier_AListOfDatabases_ReadsOnlyThoseDatabasesWork_ForQueriesAndProcedures() =>
        WithSharedStoreAsync("trend-filter-raw-tier", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            foreach (var (db, weight) in new[] { (A, 3_000L), (B, 2_000L), (C, 9_000L) })
            {
                await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, "SELECT " + db, weight);
                await PlantProcedureAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, weight);
            }

            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            const double perSecond = 1.0 / 1000 / 3600;

            foreach (var procedures in new[] { false, true })
            {
                async Task<string> Read(DatabaseFilter databases) => procedures
                    ? await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 24, null, null, databases, budget, ct)
                    : await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, serverName, 24, null, null, databases, budget, ct);

                var all = await Read(DatabaseFilter.All);
                Assert.Equal("raw", JsonDocument.Parse(all).RootElement.GetProperty("source").GetString());
                Assert.Equal(14_000 * perSecond, LastRate(all)!.Value, 9);
                Assert.Equal(5_000 * perSecond, LastRate(await Read(Of(A, B)))!.Value, 9);
                Assert.Equal(5_000 * perSecond, LastRate(await Read(Of(B, A)))!.Value, 9);
                Assert.Equal(2_000 * perSecond, LastRate(await Read(DatabaseFilter.One(B)))!.Value, 9);
                /* A repeated name and a blank one are one name, and the public method's answer is the All one. */
                Assert.Equal(2_000 * perSecond, LastRate(await Read(Of(B, B, " ")))!.Value, 9);
                Assert.Equal(LastRate(all), LastRate(procedures
                    ? await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 24, null, null, null, ct)
                    : await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, serverName, 24, null, null, null, ct)));
            }

            /* The reader overloads: the same set, a bucket width at $5. */
            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);
            var coverageRollups = await ComposeStoreAvailability.GetRollupsAsync(postgres, ct);
            var route = DarlingTrendReader.ResolveQueryDurationTrendRoute(start, coverageRollups.Item1, coverageRollups.Item2, windowEndUtc: end);
            Assert.Equal(RetentionTier.Raw, route.Tier);
            var twoDatabases = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, serverId, start, end, route, 60, Of(A, B), ct);
            var everyDatabase = await DarlingTrendReader.GetQueryDurationTrendAsync(postgres, serverId, start, end, route, 60, DatabaseFilter.All, ct);
            Assert.Equal(5_000 * perSecond, twoDatabases.Points.Last().Value!.Value, 9);
            Assert.Equal(14_000 * perSecond, everyDatabase.Points.Last().Value!.Value, 9);
        }, Cleanup);

    [Fact]
    public Task TheQueryStoreRawRoute_AListOfDatabases_ReadsOnlyThoseDatabasesWork() =>
        WithSharedStoreAsync("trend-filter-query-store", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            /* Two collections per database, so the second is rated over the spacing to the first (3600 s). */
            foreach (var (db, durationUs) in new[] { (A, 3_000L), (B, 2_000L), (C, 9_000L) })
            {
                await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-3), db, durationUs);
                await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, durationUs);
            }

            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);
            /* The raw-only route reads the $4 statement; every execution is 10 per row, so a point is 10 * Σ duration µs / 1000. */
            const double perSecond = 10.0 / 1000 / 3600;
            var rawOnly = QueryStoreTrendRouting.QueryStoreTrendRoute.RawOnly;

            var all = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(postgres, serverId, start, end, rawOnly, DatabaseFilter.All, ct);
            Assert.Equal(14_000 * perSecond, all.Last().Value!.Value, 9);
            Assert.Equal(5_000 * perSecond, (await DarlingTrendReader.GetQueryStoreDurationTrendAsync(postgres, serverId, start, end, rawOnly, Of(A, B), ct)).Last().Value!.Value, 9);
            Assert.Equal(2_000 * perSecond, (await DarlingTrendReader.GetQueryStoreDurationTrendAsync(postgres, serverId, start, end, rawOnly, DatabaseFilter.One(B), ct)).Last().Value!.Value, 9);
            Assert.Equal(all.Select(p => p.Value), (await DarlingTrendReader.GetQueryStoreDurationTrendAsync(postgres, serverId, start, end, rawOnly, ct)).Select(p => p.Value));

            /* The tool, end to end (whichever route the store resolves, the filter is on every arm of it). */
            Assert.Equal(5_000 * perSecond, LastRate(await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, serverName, 24, null, Of(A, B), ct))!.Value, 9);
            Assert.Equal(14_000 * perSecond, LastRate(await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, serverName, 24, null, DatabaseFilter.All, ct))!.Value, 9);
        }, Cleanup);

    /// <summary>
    /// [M4] The one-name consumers on the empty path: with a filter, "quiet" is a statement about the chosen databases,
    /// not about the server, and it names them (one name by its name, two or more as "the chosen databases"). The status
    /// word stays the contract: the server has data, so it is "empty", and a never-sampled server is still "unavailable"
    /// on the bare server name because its probe is server-wide.
    /// </summary>
    [Fact]
    public Task TheEmptyAnswer_UnderAFilter_NamesTheChosenDatabases_AndKeepsItsStatusWord() =>
        WithSharedStoreAsync("trend-filter-empty", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2), A, "SELECT " + A, 3_000);
            await PlantProcedureAsync(connection, ct, serverId, serverName, now.AddHours(-2), A, 3_000);
            await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2), A, 3_000);

            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            var none = Of("NoSuchDatabase");
            var several = Of("NoSuchDatabase", "NoSuchEither");

            foreach (var json in new[]
            {
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, serverName, 24, null, null, none, budget, ct),
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 24, null, null, none, budget, ct),
                await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, serverName, 24, null, none, ct),
            })
            {
                Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
                Assert.Contains(" for the database NoSuchDatabase", JsonDocument.Parse(json).RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            foreach (var json in new[]
            {
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, serverName, 24, null, null, several, budget, ct),
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 24, null, null, several, budget, ct),
                await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, serverName, 24, null, several, ct),
            })
            {
                Assert.Equal("empty", DarlingMcpTestData.StatusOf(json));
                var message = JsonDocument.Parse(json).RootElement.GetProperty("message").GetString()!;
                Assert.Contains(DatabaseFilter.ManyDatabasesDescription, message, StringComparison.Ordinal);
                Assert.DoesNotContain("NoSuchDatabase", message, StringComparison.Ordinal);
            }

            /* Every database: the sentence is the one the tools always said (no scope text), over a window with no rows. */
            var quiet = await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, serverName, 1, DateTime.UtcNow.AddDays(-2).ToString("o"), null, DatabaseFilter.All, budget, ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(quiet));
            Assert.DoesNotContain("database_name", JsonDocument.Parse(quiet).RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }, Cleanup);

    [Fact]
    public void TheFilteredStatements_CarryTheListPredicate_OnEveryArm()
    {
        const string predicate4 = "($4::text[] IS NULL OR database_name = ANY($4))";
        const string predicate5 = "($5::text[] IS NULL OR database_name = ANY($5))";

        static int Count(string sql, string text) => System.Text.RegularExpressions.Regex.Matches(sql, System.Text.RegularExpressions.Regex.Escape(text)).Count;

        /* Raw: the per-collection CTE, the filter at $4 and the bucket width at $5. */
        Assert.Equal(1, Count(DarlingTrendReader.QueryDurationTrendFilteredSql, predicate4));
        Assert.Equal(1, Count(DarlingTrendReader.ProcedureDurationTrendFilteredSql, predicate4));
        Assert.Contains("CAST($5 AS integer)", DarlingTrendReader.QueryDurationTrendFilteredSql, StringComparison.Ordinal);
        /* Hourly: the rollup CTE, same numbering. */
        var hourly = DurationTrendRouting.BuildBucketedHourlyTrendSql("collect.query_stats_interval_hourly AS h", withDatabaseFilter: true);
        Assert.Equal(1, Count(hourly, predicate4));
        Assert.Contains("CAST($5 AS integer)", hourly, StringComparison.Ordinal);
        /* Unfiltered text is untouched: no predicate, the width at $4, so the pinned constants keep their text. */
        var unfiltered = DurationTrendRouting.BuildBucketedHourlyTrendSql("collect.query_stats_interval_hourly AS h");
        Assert.DoesNotContain("database_name", unfiltered, StringComparison.Ordinal);
        Assert.Contains("CAST($4 AS integer)", unfiltered, StringComparison.Ordinal);
        Assert.Equal(unfiltered, DurationTrendRouting.BuildBucketedHourlyTrendSql("collect.query_stats_interval_hourly AS h", withDatabaseFilter: false));
        /* Query Store: both arms of the raw-only statement; the rollup arm and both raw arms of the routed one. */
        Assert.Equal(2, Count(DarlingTrendReader.QueryStoreDurationTrendFilteredSql, predicate4));
        Assert.DoesNotContain("database_name = ANY", DarlingTrendReader.QueryStoreDurationTrendSql, StringComparison.Ordinal);
        Assert.Equal(3, Count(DarlingTrendReader.QueryStoreDurationTrendRollupFilteredSql, predicate5));
        Assert.Equal(QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true), DarlingTrendReader.QueryStoreDurationTrendRollupFilteredSql);
    }

    internal static async Task WithSharedStoreAsync(
        string serverName, Func<NpgsqlConnection, NpgsqlDataSource, int, string, DateTime, CancellationToken, Task> body, string cleanupSql)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live duration trend database filter tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var cleanup = string.Format(System.Globalization.CultureInfo.InvariantCulture, cleanupSql, serverId)
                      + string.Format(System.Globalization.CultureInfo.InvariantCulture, "; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
            await body(connection, postgres, serverId, serverName, DarlingMcpTestData.Naive(DateTime.UtcNow), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")] (it also stops the scratch database's TimescaleDB
   background workers, which the shared store must never inherit). */

/// <summary>
/// #5244, PR4 lane R1: the hourly rollup tier of get_query_duration_trend and get_procedure_duration_trend (a TimescaleDB
/// scratch store; the raw rows of the window are purged after the rollup refreshes, so the route reads the interval
/// successor views, which carry <c>database_name</c>). The hourly statement carries the list predicate at $4 and the bucket
/// width at $5.
/// </summary>
public sealed class DurationTrendDatabaseFilterHourlyLiveTests
{
    private const int ServerId = -943853;
    private const string ServerName = "a5244-trend-filter-hourly";
    private const string A = "TrendFilterA";
    private const string B = "TrendFilterB";
    private const string C = "TrendFilterC";

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime SurvivorAt = WindowStart.AddHours(5);

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    /// <summary>The series as time to elapsed rate.</summary>
    private static Dictionary<DateTime, double> Series(string json) =>
        JsonDocument.Parse(json).RootElement.GetProperty("trend").EnumerateArray().ToDictionary(
            p => DateTime.Parse(p.GetProperty("time").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind),
            p => p.GetProperty("elapsed_ms_per_second").GetDouble());

    [Fact]
    public async Task TheHourlyTier_AListOfDatabases_ReadsOnlyThoseDatabasesWork_ForQueriesAndProcedures()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly duration trend filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly duration trend filter test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* C is the hottest, so an unfiltered hourly read would show it. Hour 1 holds all three; hour 5 holds A alone. */
            var windowEnd = WindowStart.AddDays(1);
            var at = WindowStart.AddHours(1);
            foreach (var (db, weight) in new[] { (A, 3_000L), (B, 2_000L), (C, 9_000L) })
            {
                at = at.AddMinutes(10);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, $5, $6, $7, 10, $8, $8, 100, 1000, 3600)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, db, "0xQ" + db, "0xSQLH" + db, weight);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, 10, $8, $8, 100, 1000, 3600)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, db, "usp_" + db, "0xSQLH" + db, weight);
            }

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                  VALUES ($1, $2, $3, $4, $5, '0xQSURV', '0xSQLHSURV', 1, 1000, 1000, 7, 1000, 3600)",
                CollectionIdGenerator.Next(), SurvivorAt, ServerId, ServerName, A);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                  VALUES ($1, $2, $3, $4, $5, 'dbo', 'usp_SURV', '0xSQLHSURV', 1, 1000, 1000, 7, 1000, 3600)",
                CollectionIdGenerator.Next(), SurvivorAt, ServerId, ServerName, A);

            foreach (var view in new[] { TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(WindowStart);
                refresh.Parameters.AddWithValue(windowEnd.AddHours(1));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            foreach (var table in new[] { "collect.query_stats", "collect.procedure_stats" })
            {
                await using var purge = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(WindowStart.AddHours(3));
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var hourly = NpgsqlDataSource.Create(scratch.ConnectionString);
            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            var asOf = DateTime.SpecifyKind(windowEnd, DateTimeKind.Utc).ToString("o");
            const double perSecond = 1.0 / 1000 / 3600;
            var hourOne = WindowStart.AddHours(1);
            var hourFive = SurvivorAt;

            foreach (var procedures in new[] { false, true })
            {
                async Task<string> Read(DatabaseFilter databases) => procedures
                    ? await DarlingMcpTrendTools.GetProcedureDurationTrend(hourly, ServerName, 24, asOf, 60, databases, budget, ct)
                    : await DarlingMcpTrendTools.GetQueryDurationTrend(hourly, ServerName, 24, asOf, 60, databases, budget, ct);

                var label = procedures ? "procedures" : "queries";
                var all = await Read(DatabaseFilter.All);
                Assert.True(JsonDocument.Parse(all).RootElement.GetProperty("source").GetString() == "hourly", label + ": expected the hourly tier");
                var allSeries = Series(all);
                Assert.Equal(14_000 * perSecond, allSeries[hourOne], 9);
                Assert.Equal(1_000 * perSecond, allSeries[hourFive], 9);

                var two = Series(await Read(Of(A, B)));
                Assert.Equal(5_000 * perSecond, two[hourOne], 9);
                Assert.Equal(1_000 * perSecond, two[hourFive], 9);

                /* C alone: it has no row in hour 5, so the series is one point, and A's survivor is not in it. */
                var onlyC = Series(await Read(DatabaseFilter.One(C)));
                Assert.Equal(9_000 * perSecond, onlyC[hourOne], 9);
                Assert.DoesNotContain(hourFive, onlyC.Keys);

                var oneByName = Series(await Read(DatabaseFilter.One(B)));
                Assert.Equal(2_000 * perSecond, oneByName[hourOne], 9);
                Assert.DoesNotContain(hourFive, oneByName.Keys);

                /* [M4] A list that matches nothing in the rollup is "empty", and the message names the chosen databases. */
                var emptyJson = await Read(Of("NoSuchDatabase", "NoSuchEither"));
                Assert.Equal("empty", DarlingMcpTestData.StatusOf(emptyJson));
                Assert.Contains(DatabaseFilter.ManyDatabasesDescription, JsonDocument.Parse(emptyJson).RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt)));
            });
        }
    }
}

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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5245 (part of #5244), PR1: the database filter on the three top reads (get_top_queries_by_cpu,
/// get_top_procedures_by_cpu, get_query_store_top). Each read has an internal overload that takes a
/// <see cref="DatabaseFilter"/>, with the list predicate (<c>$5::text[] IS NULL OR database_name = ANY($5)</c>) in every
/// statement of every tier, so an overload call with [A, B] over a seed of A, B and C returns only A's and B's rows, one
/// name reads as the public method does, and a <see cref="TopFill"/> refill round can never pull a row from C.
/// The public methods still pass one name, so there is no MCP schema, dispatch or tools/list byte change.
///
/// <para>This class is the shared-store half (the raw tiers and Query Store's raw route, which the store's own
/// precondition health table feeds); <see cref="TopQueriesDatabaseFilterScratchLiveTests"/> is the half that mints its own
/// database for the hourly rollup tier and Query Store's interval-table and daily routes.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TopQueriesDatabaseFilterLiveTests
{
    private const string A = "TopFilterA";
    private const string B = "TopFilterB";
    private const string C = "TopFilterC";
    private const string WaitforText = "WAITFOR DELAY '00:00:30'";

    /// <summary>The names of the databases a list of rows covers, in a stable order.</summary>
    private static List<string> Databases(IEnumerable<string> names) => names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();

    private static List<string> JsonDatabases(string json, string property) =>
        Databases(JsonDocument.Parse(json).RootElement.GetProperty(property).EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!));

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    /// <summary>One query_stats row with a text of its own; <paramref name="weight"/> is its CPU and elapsed time.</summary>
    private static Task PlantQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, string tag, string text, long weight) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 10, $12, $12, 100, 0, 0, 1, $12, 1, $12, 1, 1, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db,
            "0xQ" + tag, "0xP" + tag, "0xS" + tag, "0xL" + tag, text, SHA256.HashData(Encoding.UTF8.GetBytes(text)), weight);

    private static Task PlantProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, string name, long weight) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats
                  (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                   delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, 10, $8, $8, 100, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db, name, "0xSQLH" + name, weight);

    private static Task PlantQueryStoreAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, long queryId, string text, long durationUs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
              VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $8, $9, 10, $10, 800, $2)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db, queryId,
            "0xQ" + queryId, "0xP" + queryId, text, durationUs);

    private const string Cleanup =
        "DELETE FROM query_stats WHERE server_id = {0}; DELETE FROM procedure_stats WHERE server_id = {0}; " +
        "DELETE FROM query_store_stats WHERE server_id = {0}; DELETE FROM query_store_health WHERE server_id = {0}";

    [Fact]
    public Task TheRawTiers_AListOfDatabases_ReturnsOnlyThoseDatabases_AndOneNameReadsAsThePublicMethodDoes() =>
        WithSharedStoreAsync("top-filter-raw-tiers", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            foreach (var (db, weight) in new[] { (A, 3_000L), (B, 2_000L), (C, 9_000L) })
            {
                await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, "Q" + db, "SELECT " + db, weight);
                await PlantProcedureAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, "usp_" + db, weight);
                await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2), db, (long)(10 + db[^1]), "SELECT qs " + db, weight * 1000);
            }

            var (start, end) = (now.AddHours(-24), now.AddMinutes(5));
            foreach (var rollUp in new[] { false, true })
            {
                var queries = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(postgres, serverId, start, end, 10, Of(A, B), rollUp, 0, TopRanking.Cpu, ct);
                Assert.Equal(RetentionTier.Raw, queries.Tier);
                Assert.Equal([A, B], Databases(queries.Rows.Select(r => r.DatabaseName)));
                Assert.Equal([A, B], Databases((await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, 10, Of(B, A), rollUp, 0, TopRanking.Cpu, ct)).Select(r => r.DatabaseName)));
                Assert.Equal([A, B, C], Databases((await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, 10, DatabaseFilter.All, rollUp, 0, TopRanking.Cpu, ct)).Select(r => r.DatabaseName)));
                /* One name: the public one-name method and the overload give the same page. */
                var one = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, 10, DatabaseFilter.One(B), rollUp, 0, TopRanking.Cpu, ct);
                var oneByName = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, 10, B, rollUp, 0, TopRanking.Cpu, ct);
                Assert.Equal(["SELECT " + B], one.Select(r => r.QueryText));
                Assert.Equal(one.Select(r => r.QueryHash), oneByName.Select(r => r.QueryHash));
            }

            var procedures = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(postgres, serverId, start, end, 10, Of(A, B), TopRanking.Cpu, ct);
            Assert.Equal(RetentionTier.Raw, procedures.Tier);
            Assert.Equal([A, B], Databases(procedures.Rows.Select(r => r.DatabaseName)));
            Assert.Equal([A, B, C], Databases((await DarlingDataReader.GetTopProceduresByCpuAsync(postgres, serverId, start, end, 10, DatabaseFilter.All, TopRanking.Cpu, ct)).Select(r => r.DatabaseName)));
            Assert.Equal([B], Databases((await DarlingDataReader.GetTopProceduresByCpuAsync(postgres, serverId, start, end, 10, B, TopRanking.Cpu, ct)).Select(r => r.DatabaseName)));

            var store = await DarlingDataReader.GetQueryStoreTopWithReachAsync(postgres, serverId, now.AddHours(-3), end, 10, Of(A, B), null, null, ct);
            Assert.Null(store.Table);
            Assert.Equal([A, B], Databases(store.Rows.Select(r => r.DatabaseName)));
            Assert.Equal([A, B, C], Databases((await DarlingDataReader.GetQueryStoreTopAsync(postgres, serverId, now.AddHours(-3), end, 10, DatabaseFilter.All, null, null, ct)).Select(r => r.DatabaseName)));
            Assert.Equal([B], Databases((await DarlingDataReader.GetQueryStoreTopAsync(postgres, serverId, now.AddHours(-3), end, 10, B, ct)).Select(r => r.DatabaseName)));

            /* The tools, end to end: the page is the top N of the chosen databases. */
            Assert.Equal([A, B], JsonDatabases(await DarlingMcpDataTools.GetTopQueriesRanked(postgres, serverName, 24, 10, Of(A, B), false, 0, "query_hash", null, "summary", null, ct), "queries"));
            Assert.Equal([A, B], JsonDatabases(await DarlingMcpDataTools.GetTopProceduresRanked(postgres, serverName, 24, 10, Of(A, B), null, "summary", null, ct), "procedures"));
            Assert.Equal([A, B], JsonDatabases(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, Of(A, B), null, null, null, false, 400, ct), "queries"));
        }, Cleanup);

    /// <summary>
    /// Twelve WAITFOR shells (six in A, six in B) outrank everything, so the first round (top + 5 = 8 candidates) sees only
    /// shells and a refill must run. C's real groups outrank the real groups of A and B, so a refill that dropped the filter
    /// would fill the page from C. Under the filter the page is A's and B's real groups.
    /// </summary>
    [Fact]
    public Task ARefillRound_UnderTheFilter_NeverReturnsAnUnchosenDatabase() =>
        WithSharedStoreAsync("top-filter-refill", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            var tick = 0;
            foreach (var db in new[] { A, B })
            {
                for (var i = 1; i <= 6; i++)
                {
                    tick++;
                    await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2).AddSeconds(tick), db, "Shell" + db + i, WaitforText, 1_000_000 - tick);
                    await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2).AddSeconds(tick), db, tick, WaitforText, 50_000_000 - tick);
                }

                for (var i = 1; i <= 2; i++)
                {
                    tick++;
                    await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2).AddSeconds(tick), db, "Real" + db + i, "SELECT real " + db + i, 10_000 - tick * 100);
                    await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2).AddSeconds(tick), db, tick, "SELECT real " + db + i, 10_000 - tick * 10);
                }
            }

            for (var i = 1; i <= 6; i++)
            {
                tick++;
                await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2).AddSeconds(tick), C, "Real" + C + i, "SELECT real " + C + i, 500_000 - tick);
                await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2).AddSeconds(tick), C, tick, "SELECT real " + C + i, 20_000_000 - tick);
            }

            var (start, end) = (now.AddHours(-24), now.AddMinutes(5));
            foreach (var rollUp in new[] { false, true })
            {
                var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, 3, Of(A, B), rollUp, 0, TopRanking.Cpu, ct);
                Assert.True(rows.Count == 3, $"roll-up {rollUp}: {rows.Count} rows, expected 3");
                Assert.DoesNotContain(rows, r => (r.QueryText ?? "").StartsWith("WAITFOR", StringComparison.Ordinal));
                Assert.Equal([A, B], Databases(rows.Select(r => r.DatabaseName)));
                Assert.DoesNotContain(rows, r => r.DatabaseName == C);

                /* Control: with no filter the same refill fills the page from C, which is what the filter has to stop. */
                var unfiltered = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, serverId, start, end, 3, DatabaseFilter.All, rollUp, 0, TopRanking.Cpu, ct);
                Assert.All(unfiltered, r => Assert.Equal(C, r.DatabaseName));
            }

            var store = await DarlingDataReader.GetQueryStoreTopAsync(postgres, serverId, now.AddHours(-3), end, 3, Of(A, B), null, null, ct);
            Assert.True(store.Count == 3, $"query store: {store.Count} rows, expected 3");
            Assert.DoesNotContain(store, r => (r.QueryText ?? "").StartsWith("WAITFOR", StringComparison.Ordinal));
            Assert.Equal([A, B], Databases(store.Select(r => r.DatabaseName)));
            var storeUnfiltered = await DarlingDataReader.GetQueryStoreTopAsync(postgres, serverId, now.AddHours(-3), end, 3, DatabaseFilter.All, null, null, ct);
            Assert.All(storeUnfiltered, r => Assert.Equal(C, r.DatabaseName));
        }, Cleanup);

    private static Task PlantHealthAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, string actualState) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
              VALUES ($1,$2,$3,$4,$5,$6,'READ_WRITE',0,50,1000,'AUTO',30,200,60)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db, actualState);

    /// <summary>
    /// Every one-name consumer on get_query_store_top's empty paths is list-aware: Query Store off in one of two chosen
    /// databases hands back the other's rows with no precondition envelope; off in both, an empty window answers with the
    /// precondition envelope; off in one and recording in the other stays silent (the rule is "any database in scope
    /// recording", not a per-database verdict). The execution_type and module_name empty answers name the chosen databases
    /// for two or more names and keep today's text for one.
    /// </summary>
    [Fact]
    public Task QueryStoreTop_TheEmptyPaths_AreListAware() =>
        WithSharedStoreAsync("top-filter-qs-precondition", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await PlantHealthAsync(connection, ct, serverId, serverName, now.AddMinutes(-10), A, "OFF");
            await PlantHealthAsync(connection, ct, serverId, serverName, now.AddMinutes(-10), B, "READ_WRITE");
            await PlantHealthAsync(connection, ct, serverId, serverName, now.AddMinutes(-10), C, "OFF");
            await PlantQueryStoreAsync(connection, ct, serverId, serverName, now.AddHours(-2), B, 7, "SELECT qs " + B, 5_000);

            /* Off in A, on in B: B's rows come back, with no precondition envelope. */
            var rows = await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, Of(A, B), null, null, null, false, 400, ct);
            Assert.Equal([B], JsonDatabases(rows, "queries"));
            Assert.False(JsonDocument.Parse(rows).RootElement.TryGetProperty("status", out var rowsStatus) && rowsStatus.GetString() == "precondition");

            /* Off in both A and C, nothing recorded for them in the window: the precondition envelope. */
            var offBoth = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, Of(A, C), null, null, null, false, 400, ct)).RootElement;
            Assert.Equal("precondition", offBoth.GetProperty("status").GetString());
            Assert.Contains("Query Store is not enabled", offBoth.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* Only A is in the snapshot, and A is off: the envelope. Once a database in scope records, the envelope is silent. */
            var onlyOff = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 1, 10, Of(A, "TopFilterNoRows"), null, null, null, false, 400, ct)).RootElement;
            Assert.Equal("precondition", onlyOff.GetProperty("status").GetString());
            await PlantHealthAsync(connection, ct, serverId, serverName, now.AddMinutes(-10), "TopFilterNoRows", "READ_WRITE");
            var anyRecording = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 1, 10, Of(A, "TopFilterNoRows"), null, null, null, false, 400, ct)).RootElement;
            Assert.NotEqual("precondition", anyRecording.GetProperty("status").GetString());

            /* The execution_type empty answer: B has Regular rows only. */
            var two = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, Of(A, B), null, "Aborted", null, false, 400, ct)).RootElement;
            Assert.Equal("empty", two.GetProperty("status").GetString());
            Assert.Contains("No Aborted executions for the chosen databases in the 3-hour window", two.GetProperty("message").GetString(), StringComparison.Ordinal);
            var one = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, DatabaseFilter.One(B), null, "Aborted", null, false, 400, ct)).RootElement;
            Assert.Contains("No Aborted executions in database '" + B + "' in the 3-hour window", one.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal(one.GetProperty("message").GetString(),
                JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, B, null, "Aborted", null, false, 400, ct)).RootElement.GetProperty("message").GetString());

            /* The module_name empty answer, the same two phrasings. */
            var moduleTwo = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, Of(A, B), null, null, "dbo.usp_Missing", false, 400, ct)).RootElement;
            Assert.Contains("matched module_name 'dbo.usp_Missing' for the chosen databases in the 3-hour window", moduleTwo.GetProperty("message").GetString(), StringComparison.Ordinal);
            var moduleOne = JsonDocument.Parse(await DarlingMcpDataTools.GetQueryStoreTop(postgres, serverName, 3, 10, DatabaseFilter.One(B), null, null, "dbo.usp_Missing", false, 400, ct)).RootElement;
            Assert.Contains("matched module_name 'dbo.usp_Missing' in database '" + B + "' in the 3-hour window", moduleOne.GetProperty("message").GetString(), StringComparison.Ordinal);
        }, Cleanup);

    /// <summary>The statement text pins: every pass of every tier carries the list predicate, and none keeps the one-name shape.</summary>
    [Theory]
    [InlineData(nameof(DarlingDataReader.TopQueriesSql), 2)]
    [InlineData(nameof(DarlingDataReader.TopQueriesByHostObjectSql), 2)]
    [InlineData(nameof(DarlingDataReader.TopQueriesHourlySql), 1)]
    [InlineData(nameof(DarlingDataReader.TopProceduresSql), 2)]
    [InlineData(nameof(DarlingDataReader.TopProceduresHourlySql), 1)]
    public void EveryStatement_CarriesTheListPredicate_InEveryPass(string statement, int passes)
    {
        var sql = (string)typeof(DarlingDataReader).GetField(statement, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.GetValue(null)!;
        Assert.DoesNotContain("$5::text IS NULL", sql, StringComparison.Ordinal);
        Assert.Equal(passes, System.Text.RegularExpressions.Regex.Matches(sql, System.Text.RegularExpressions.Regex.Escape("($5::text[] IS NULL OR database_name = ANY($5))")).Count);
    }

    [Fact]
    public void TheQueryStoreStatements_CarryTheListPredicate_OnEveryRoute()
    {
        foreach (var (name, sql) in new[]
        {
            ("QueryStoreTopSql", DarlingDataReader.QueryStoreTopSql),
            ("QueryStoreTopTableSql", DarlingDataReader.QueryStoreTopTableSql),
            ("QueryStoreTopDailyTableSql", DarlingDataReader.QueryStoreTopDailyTableSql),
        })
        {
            Assert.DoesNotContain("$5::text IS NULL", sql, StringComparison.Ordinal);
            Assert.True(
                System.Text.RegularExpressions.Regex.Matches(sql, System.Text.RegularExpressions.Regex.Escape("($5::text[] IS NULL OR database_name = ANY($5))")).Count >= 1,
                name + " must carry the list predicate");
        }

        Assert.Contains("($2::text[] IS NULL OR database_name = ANY($2))", DarlingRuntimePrecondition.LatestQueryStoreStatesSql, StringComparison.Ordinal);
    }

    private static async Task WithSharedStoreAsync(
        string serverName, Func<NpgsqlConnection, NpgsqlDataSource, int, string, DateTime, CancellationToken, Task> body, string cleanupSql)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live top-reads database filter tests.");
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

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres for every test and never touches
   another test's rows, so it is deliberately NOT [Collection("live-postgres")] (it also stops the scratch database's
   TimescaleDB background workers, which the shared store must never inherit). */

/// <summary>
/// #5245: the other half of <see cref="TopQueriesDatabaseFilterLiveTests"/>: the hourly rollup tier of the queries and
/// procedures reads (a TimescaleDB scratch store; the raw rows are purged after the rollup refreshes, so the router reads
/// the rollup), and get_query_store_top's interval-table route and daily-summary route.
/// </summary>
public sealed class TopQueriesDatabaseFilterScratchLiveTests
{
    private const int ServerId = -943852;
    private const string ServerName = "a5245-top-filter-hourly";
    private const string A = "TopFilterA";
    private const string B = "TopFilterB";
    private const string C = "TopFilterC";

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime SurvivorAt = WindowStart.AddHours(5);

    private static List<string> Databases(IEnumerable<string> names) => names.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList();

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    [Fact]
    public async Task TheHourlyTier_AListOfDatabases_ReturnsOnlyThoseDatabases_ForQueriesAndProcedures()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly database filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly database filter test needs TimescaleDB.");
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
            /* C is the hottest, so an unfiltered hourly read would put it first. */
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

            /* A survivor in raw keeps the raw floor later than the window start, so the router sends the read to the rollup. */
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

            var all = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(hourly, ServerId, WindowStart, windowEnd, 10, DatabaseFilter.All, ranking: TopRanking.Cpu, cancellationToken: ct);
            Assert.True(all.Tier == RetentionTier.Hourly, $"queries: expected the hourly tier but read {all.Tier}");
            Assert.Equal([A, B, C], Databases(all.Rows.Select(r => r.DatabaseName)));

            var two = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(hourly, ServerId, WindowStart, windowEnd, 10, Of(A, B), ranking: TopRanking.Cpu, cancellationToken: ct);
            Assert.Equal(RetentionTier.Hourly, two.Tier);
            Assert.Equal([A, B], Databases(two.Rows.Select(r => r.DatabaseName)));
            Assert.DoesNotContain(two.Rows, r => r.QueryHash == "0xQ" + C);

            var one = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(hourly, ServerId, WindowStart, windowEnd, 10, B, ranking: TopRanking.Cpu, cancellationToken: ct);
            Assert.Equal([B], Databases(one.Rows.Select(r => r.DatabaseName)));
            Assert.Equal(one.Rows.Select(r => r.QueryHash),
                (await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(hourly, ServerId, WindowStart, windowEnd, 10, DatabaseFilter.One(B), ranking: TopRanking.Cpu, cancellationToken: ct)).Rows.Select(r => r.QueryHash));

            var procedures = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(hourly, ServerId, WindowStart, windowEnd, 10, Of(A, B), TopRanking.Cpu, ct);
            Assert.True(procedures.Tier == RetentionTier.Hourly, $"procedures: expected the hourly tier but read {procedures.Tier}");
            Assert.Equal([A, B], Databases(procedures.Rows.Select(r => r.DatabaseName)));
            Assert.Equal([A, B, C], Databases((await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(hourly, ServerId, WindowStart, windowEnd, 10, DatabaseFilter.All, TopRanking.Cpu, ct)).Rows.Select(r => r.DatabaseName)));
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

    private static readonly DateTime QsEnd = new(2026, 1, 14, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime QsBuildNow = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Nine queries over db0, db1 and db2 (query q lives in db(q % 3)), one row per query per hour from 2026-01-05 to 2026-01-14 12:00.</summary>
    private const string QsSeedSql = @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
 avg_physical_io_reads, avg_rowcount, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT
    ct, 1, 'db' || (q % 3), q, q, 'Regular', ct - interval '10 minutes', ct, NULL,
    'h' || q, 5, 1000 + q * 100, 500 + q, 100, 10, 5, 3, 'ph', NULL, q * 100000 + h, ct - interval '10 minutes'
FROM
(
    SELECT q, h, TIMESTAMP '2026-01-05 00:00:00' + make_interval(hours => h) AS ct
    FROM generate_series(1, 9) AS q
    CROSS JOIN generate_series(0, 24 * 9 + 12) AS h
) AS s;";

    [Fact]
    public async Task QueryStoreTop_TheIntervalTableAndDailyRoutes_AListOfDatabases_ReturnsOnlyThoseDatabases()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store routes database filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, QsSeedSql);
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (1, 'srv1', TRUE)");
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES (1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')");

            var start = QsEnd.AddHours(-72);

            /* No built day yet: the interval-table statement. */
            var table = await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, 1, start, QsEnd, 30, Of("db0", "db1"), null, null, ct);
            Assert.NotNull(table.Table);
            Assert.Equal(0, table.DailyDaysUsed);
            Assert.Equal(["db0", "db1"], Databases(table.Rows.Select(r => r.DatabaseName)));
            Assert.Equal(6, table.Rows.Count);
            Assert.Equal(["db2"], Databases((await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, 1, start, QsEnd, 30, "db2", null, null, ct)).Rows.Select(r => r.DatabaseName)));
            Assert.Equal(["db0", "db1", "db2"], Databases((await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, 1, start, QsEnd, 30, DatabaseFilter.All, null, null, ct)).Rows.Select(r => r.DatabaseName)));

            /* Built days: the daily-summary statement. */
            var result = await QueryStoreTopDaily.RunTickAsync(source, QsBuildNow, 9, NullLogger.Instance, ct);
            Assert.Equal(0, result.Failed);
            var daily = await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, 1, start, QsEnd, 30, Of("db0", "db1"), null, null, ct);
            Assert.True(daily.DailyDaysUsed > 0, "the read must use built days");
            Assert.Equal(["db0", "db1"], Databases(daily.Rows.Select(r => r.DatabaseName)));
            Assert.Equal(6, daily.Rows.Count);
            Assert.Equal(["db0", "db2"], Databases((await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, 1, start, QsEnd, 30, Of("db2", "db0"), null, null, ct)).Rows.Select(r => r.DatabaseName)));
            var oneDaily = await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, 1, start, QsEnd, 30, DatabaseFilter.One("db1"), null, null, ct);
            Assert.Equal(["db1"], Databases(oneDaily.Rows.Select(r => r.DatabaseName)));
            Assert.Equal(oneDaily.Rows.Select(r => r.QueryId),
                (await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, 1, start, QsEnd, 30, "db1", null, null, ct)).Rows.Select(r => r.QueryId));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}

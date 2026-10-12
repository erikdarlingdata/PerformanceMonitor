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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5244 (PR4, lane R2): get_long_query_completions, get_plan_corrections and get_query_store_clutter read a SET of
/// databases. Each reader and tool gains an overload that takes a <see cref="DatabaseFilter"/>; the public tool methods keep
/// passing every database until the wiring lane. This class pins each overload over a seed of three databases (A, B and C):
/// [A, B] returns only A and B rows on every statement the read uses, the cap applies after the filter, and the one-name
/// consumers on the empty path and the server-level replica flag say something true for a list.
/// Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryEventsDatabaseFilterLiveTests
{
    internal const string ServerName = "darling-query-events-dbfilter-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    internal const string DbA = "FilterDbA";
    internal const string DbB = "FilterDbB";
    internal const string DbC = "FilterDbC";
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live query-events database-filter test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void Sql_IsTheListForm_OnEveryStatementTheReadsUse_WithNoNameSplicedIn()
    {
        Assert.Contains("($5::text[] IS NULL OR database_name = ANY($5))", DarlingLongQueryReader.LongQueryCompletionsSql, StringComparison.Ordinal);
        Assert.Contains("($5::text[] IS NULL OR database_name = ANY($5))", DarlingPlanCorrectionReader.PlanCorrectionsSql, StringComparison.Ordinal);
        Assert.Contains("($2::text[] IS NULL OR database_name = ANY($2))", DarlingPlanCorrectionReader.AutomaticTuningSql, StringComparison.Ordinal);
        Assert.Contains("($5::text[] IS NULL OR d.database_name = ANY($5))", DarlingQueryStoreClutterReader.ReadCostSql, StringComparison.Ordinal);
        Assert.Contains("($4::text[] IS NULL OR database_name = ANY($4))", DarlingQueryStoreClutterReader.PlanChurnSql, StringComparison.Ordinal);
        Assert.Contains("($4::text[] IS NULL OR database_name = ANY($4))", DarlingQueryStoreClutterReader.ConfigSql, StringComparison.Ordinal);
        /* The per-server arms stay whole: no database predicate. */
        Assert.DoesNotContain("database_name", DarlingQueryStoreClutterReader.QdsWaitsSql, StringComparison.Ordinal);
        Assert.DoesNotContain("database_name", DarlingQueryStoreClutterReader.QueryStoreClerkSql, StringComparison.Ordinal);
        /* The snapshot's anchor subquery is NOT filtered, so a filtered and an unfiltered call show the same capture. */
        var anchor = DarlingPlanCorrectionReader.AutomaticTuningSql;
        var anchorSubquery = anchor[anchor.IndexOf("(SELECT MAX", StringComparison.Ordinal)..anchor.IndexOf("($2::text[]", StringComparison.Ordinal)];
        Assert.DoesNotContain("$2", anchorSubquery, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LongQueries_TwoNames_ReturnOnlyThoseDatabases_TheCapAppliesAfterTheFilter_AndAnEmptyAnswerSaysTheChosenDatabases()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var start = end.AddHours(-1);

            var ab = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { DbA, DbA, DbB }, ab.Select(r => r.DatabaseName).Order().ToArray());
            var all = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.All, ct);
            Assert.Equal(6, all.Count);
            var viaDefault = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, start, end, 100, cancellationToken: ct);
            Assert.Equal(6, viaDefault.Count);

            /* The cap is applied AFTER the filter: C holds the 2nd and 3rd slowest rows, but the top 2 of [A, B] are A's 9 s and B's 6 s. */
            var capped = await DarlingLongQueryReader.GetRecentLongQueryCompletionsAsync(postgres, ServerId, start, end, 2, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new long?[] { 9_000_000, 6_000_000 }, capped.Select(r => r.DurationMicroseconds).ToArray());

            /* The tool overload: the page holds only the chosen databases, and a miss names them. */
            var asOf = end.ToString("o");
            var page = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, ServerName, 1, 100, asOf, DatabaseFilter.Of([DbA, DbB]), null, ct)).RootElement;
            Assert.Equal(3, page.GetProperty("completions_returned").GetInt32());
            Assert.All(page.GetProperty("completions").EnumerateArray(), r => Assert.Contains(r.GetProperty("database_name").GetString(), new[] { DbA, DbB }));

            var miss = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, ServerName, 1, 100, asOf, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), null, ct)).RootElement;
            Assert.Equal("empty", miss.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases", miss.GetProperty("message").GetString(), StringComparison.Ordinal);
            var publicMiss = JsonDocument.Parse(await DarlingMcpLongQueryTools.GetLongQueryCompletions(
                postgres, ServerName, 1, 100, DateTime.UtcNow.AddDays(-30).ToString("o"), null, null, ct)).RootElement;
            Assert.DoesNotContain("for the chosen databases", publicMiss.GetProperty("message").GetString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task PlanCorrections_TwoNames_FilterTheRecommendationsAndTheTuningSnapshot_WithoutMovingTheSnapshotAnchor()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var start = end.AddHours(-1);

            var ab = await DarlingPlanCorrectionReader.GetPlanCorrectionsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { DbA, DbA, DbA, DbB }, ab.Select(r => r.DatabaseName).Order().ToArray());
            var all = await DarlingPlanCorrectionReader.GetPlanCorrectionsAsync(postgres, ServerId, start, end, 100, DatabaseFilter.All, ct);
            Assert.Equal(6, all.Count);
            /* Newest capture first: unfiltered, the top 2 are C and A (the newest capture); the cap applies after the filter, so [A, B]'s top 2 is A, then B (C's older row is out). */
            var capped = await DarlingPlanCorrectionReader.GetPlanCorrectionsAsync(postgres, ServerId, start, end, 2, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { DbA, DbB }, capped.Select(r => r.DatabaseName).ToArray());

            /* The newest capture holds A and C only. Filtering to [A, B] keeps A and does NOT fall back to B's older capture. */
            var tuningAb = await DarlingPlanCorrectionReader.GetLatestAutomaticTuningAsync(postgres, ServerId, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { DbA }, tuningAb.Select(t => t.DatabaseName).ToArray());
            var tuningAll = await DarlingPlanCorrectionReader.GetLatestAutomaticTuningAsync(postgres, ServerId, DatabaseFilter.All, ct);
            Assert.Equal(new[] { DbA, DbC }, tuningAll.Select(t => t.DatabaseName).Order().ToArray());
            Assert.Equal(tuningAll.Single(t => t.DatabaseName == DbA).CollectionTime, tuningAb.Single().CollectionTime);

            var asOf = end.ToString("o");
            var page = JsonDocument.Parse(await DarlingMcpPlanCorrectionTools.GetPlanCorrections(
                postgres, ServerName, 1, 100, asOf, false, DatabaseFilter.Of([DbA, DbB]), null, ct)).RootElement;
            Assert.Equal(4, page.GetProperty("recommendations_returned").GetInt32());
            Assert.Equal(new[] { DbA }, page.GetProperty("automatic_tuning").EnumerateArray().Select(t => t.GetProperty("database_name").GetString()).ToArray());

            var miss = JsonDocument.Parse(await DarlingMcpPlanCorrectionTools.GetPlanCorrections(
                postgres, ServerName, 1, 100, asOf, false, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), null, ct)).RootElement;
            Assert.Equal("empty", miss.GetProperty("status").GetString());
            var message = miss.GetProperty("message").GetString()!;
            Assert.Contains("for the chosen databases", message, StringComparison.Ordinal);
            Assert.DoesNotContain("for this server", message, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task QueryStoreClutter_TwoNames_NarrowOnlyThePerDatabaseArms_AndTheServerLevelFlagsStayWhole()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var start = end.AddHours(-1);
            int[] servers = [ServerId];
            var ab = DatabaseFilter.Of([DbA, DbB]);

            /* Each per-database arm: only the chosen databases. */
            var readCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, servers, start, end, ab, ct);
            Assert.Equal(new[] { DbA, DbB }, readCost.Select(r => r.DatabaseName).Order().ToArray());
            var readCostAll = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, servers, start, end, cancellationToken: ct);
            Assert.Equal(new[] { DbA, DbB, DbC }, readCostAll.Select(r => r.DatabaseName).Order().ToArray());
            /* The denominator and the comparison pool read every run: a database's figures do not move with the selection. */
            foreach (var row in readCost)
            {
                var whole = readCostAll.Single(r => r.DatabaseName == row.DatabaseName);
                Assert.Equal(whole.RunsObserved, row.RunsObserved);
                Assert.Equal(whole.OthersSlowestItemMsP50, row.OthersSlowestItemMsP50);
            }

            var churn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(postgres, servers, start, end, ab, ct);
            Assert.Equal(new[] { DbA, DbB }, churn.Select(r => r.DatabaseName).Order().ToArray());
            Assert.Equal(3, (await DarlingQueryStoreClutterReader.GetPlanChurnAsync(postgres, servers, start, end, cancellationToken: ct)).Count);

            var config = await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, servers, start, end, ab, ct);
            Assert.Equal(new[] { DbA, DbB }, config.Select(r => r.DatabaseName).Order().ToArray());
            Assert.Equal(3, (await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, servers, start, end, cancellationToken: ct)).Count);

            /* The tool: rows for the chosen databases, the per-server overhead still there. */
            var asOf = end.ToString("o");
            var page = JsonDocument.Parse(await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(
                postgres, ServerName, 1, 50, false, asOf, ab, ct)).RootElement;
            Assert.Equal(2, page.GetProperty("database_count").GetInt32());
            Assert.Equal(new[] { DbA, DbB }, page.GetProperty("databases").EnumerateArray().Select(d => d.GetProperty("database_name").GetString()).Order().ToArray());
            Assert.Equal(JsonValueKind.Object, page.GetProperty("qs_overhead").ValueKind);
            Assert.False(page.GetProperty("server_is_replica").GetBoolean());

            /* [M4] server_is_replica is a SERVER claim: B alone is a readable secondary (the only config row the filter keeps), but the server
               is not one, because A and C record. Read off the filtered rows it would say true. */
            var onlyReplica = JsonDocument.Parse(await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(
                postgres, ServerName, 1, 50, false, asOf, DatabaseFilter.One(DbB), ct)).RootElement;
            Assert.Equal(1, onlyReplica.GetProperty("database_count").GetInt32());
            Assert.False(onlyReplica.GetProperty("server_is_replica").GetBoolean());
            Assert.Equal(JsonValueKind.Null, onlyReplica.GetProperty("server_note").ValueKind);

            /* [M4] The empty path: a filter naming databases nothing was collected for says "empty ... for the chosen databases", never an
               unavailable verdict about the server, and the Query Store precondition takes the filter. */
            var miss = JsonDocument.Parse(await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(
                postgres, ServerName, 1, 50, false, asOf, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), ct)).RootElement;
            Assert.Equal("empty", miss.GetProperty("status").GetString());
            var message = miss.GetProperty("message").GetString()!;
            Assert.Contains("for the chosen databases", message, StringComparison.Ordinal);
            Assert.DoesNotContain("for this server", message, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Seeds one server and returns the window end (now). Long-query completions: A 9 s and 3 s, B 6 s, C 8 s, 7 s and 1 s.
    /// Plan corrections: an older capture with A (two rows), B (one row) and C (one row), and a newest capture with A and C
    /// (one row each), so B has recommendations but no row in the newest capture. Query Store: the query_store collector's
    /// fan-out runs were slowest on A (3 runs), B (1) and C (2); one query_store_stats plan row per database; one health row per
    /// database, B being a readable secondary (readonly_reason bit 8) and A and C recording.
    /// </summary>
    internal static async Task<DateTime> SeedAsync(string cs, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

        var completions = new (string Db, long Seconds)[] { (DbA, 9), (DbA, 3), (DbB, 6), (DbC, 8), (DbC, 7), (DbC, 1) };
        for (var i = 0; i < completions.Length; i++)
        {
            var at = DarlingMcpTestData.Naive(end.AddMinutes(-10 - i));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, duration_microseconds, statement_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, at, "rpc_completed", completions[i].Db, completions[i].Seconds * 1_000_000, "EXEC p" + i);
        }

        var older = DarlingMcpTestData.Naive(end.AddMinutes(-40));
        var newest = DarlingMcpTestData.Naive(end.AddMinutes(-20));
        var corrections = new (string Db, DateTime At, int Score)[]
        {
            (DbA, older, 10), (DbA, older, 11), (DbB, older, 12), (DbC, older, 13),
            (DbA, newest, 20), (DbC, newest, 21),
        };
        foreach (var (db, at, score) in corrections)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_desired_state, force_last_good_plan_actual_state, recommendation_name, recommendation_state, query_id, score, query_text)
VALUES ($1,$2,$3,$4,$5,'Enabled','Enabled',$6,'Active',$7,$8,'SELECT 1')",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, db, "PR_" + score, 1000L + score, score);
        }

        var runs = new (string Slowest, int Ms)[] { (DbA, 900), (DbA, 800), (DbA, 700), (DbB, 600), (DbC, 500), (DbC, 400) };
        for (var i = 0; i < runs.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms, fanout_item_count, slowest_item, slowest_item_ms)
VALUES ($1, $2, $3, $4, $5, 1000, 'SUCCESS', NULL, 100, 990, 10, 5, $6, $7)",
                CollectionIdGenerator.Next(), ServerId, ServerName, DarlingQueryStoreClutterReader.CollectorName, DarlingMcpTestData.Naive(end.AddMinutes(-30 + i)), runs[i].Slowest, runs[i].Ms);
        }

        var dbs = new[] { DbA, DbB, DbC };
        for (var i = 0; i < dbs.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'SELECT 1', 10, 1000, 800, $2)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddMinutes(-15)), ServerId, ServerName, dbs[i], 100L + i, 200L + i, "0xQ" + i, "0xP" + i);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode)
VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', $7, 100, 8192, 'AUTO', 21, 200, 60, 'AUTO')",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddMinutes(-12)), ServerId, ServerName, dbs[i],
                dbs[i] == DbB ? "READ_ONLY" : "READ_WRITE", dbs[i] == DbB ? 8 : 0);
        }

        return end;
    }

    internal static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM long_query_completions WHERE server_id = {ServerId}; DELETE FROM plan_correction WHERE server_id = {ServerId}; "
            + $"DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM query_store_stats WHERE server_id = {ServerId}; "
            + $"DELETE FROM query_store_health WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch store and its own SQL Server database, so the class is deliberately NOT
   [Collection("live-postgres")]. It shares "remediation-live-sql" with the other classes that use the one test SQL Server. */

/// <summary>
/// #6211 end to end: the real <c>DarlingCollectorRunner</c> runs the real <c>query_store</c> collector against a scratch SQL Server database whose
/// Query Store holds nine plans in one interval, with the fetch top N at 3, and a scratch store. Needs <c>DARLING_TEST_PG</c> and
/// <c>DARLING_TEST_SQL</c> (with user and password); skips without them.
///
/// <para>The nine plans: six "cold" queries run once each, created FIRST so they hold the lowest plan ids, and three "hot" queries run 40, 50 and
/// 60 times, created last. Every query reads one table with no sort, hash or write, so the memory grant, tempdb and writes metrics are zero for
/// all nine. The lowest-id-first tie break therefore picks the cold plans exactly when a zero value is ranked, which is the defect the "zero is
/// not ranked" fact guards. The tests read plan and query ids out of Query Store with one view per statement (no
/// <c>sys.query_store_*</c> join, #6221).</para>
/// </summary>
[Collection("remediation-live-sql")]
public sealed class QueryStoreTopNFetchLiveTests
{
    private const int SmallN = 3;
    private static readonly string[] Cold = { "cold1", "cold2", "cold3", "cold4", "cold5", "cold6" };
    private static readonly (string Tag, int Runs)[] Hot = { ("hot1", 40), ("hot2", 50), ("hot3", 60) };

    private static string? StoreConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static SqlConnectionStringBuilder Admin(string database) => new()
    {
        DataSource = Environment.GetEnvironmentVariable("DARLING_TEST_SQL"),
        InitialCatalog = database,
        UserID = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"),
        Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
        TrustServerCertificate = true,
        Encrypt = SqlConnectionEncryptOption.Optional,
        Pooling = false,
    };

    private static async Task SqlAsync(string database, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long?> ScalarAsync(string database, string sql, long? key, CancellationToken ct, string? tag = null)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue("@k", (object?)key ?? DBNull.Value);
        if (tag is not null) command.Parameters.AddWithValue("@tag", tag);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    /// <summary>The query text of one tagged query: a plain scan of one table. The tag is the column alias, because simple parameterization drops a comment and
    /// would fold every query into one text; an alias stays in the parameterized text and makes each tag its own query.</summary>
    private static string QueryText(string tag, bool hot) => hot
        ? $"SELECT COUNT_BIG(*) AS {tag} FROM dbo.t WHERE pad = 'y' OPTION (MAXDOP 1)"
        : $"SELECT TOP (1) id AS {tag} FROM dbo.t";

    private static async Task RunQueryAsync(string database, string tag, bool hot, int times, CancellationToken ct)
    {
        await using var connection = new SqlConnection(Admin(database).ConnectionString);
        await connection.OpenAsync(ct);
        for (var i = 0; i < times; i++)
        {
            await using var command = new SqlCommand(QueryText(tag, hot), connection) { CommandTimeout = 120 };
            await command.ExecuteScalarAsync(ct);
        }
    }

    /// <summary>query_id then plan_id of a tagged query, one Query Store view per statement.</summary>
    private static async Task<(long QueryId, long PlanId)> IdsAsync(string database, string tag, CancellationToken ct)
    {
        var textId = await ScalarAsync(database,
            "SELECT TOP (1) query_text_id FROM sys.query_store_query_text WHERE query_sql_text LIKE N'%AS ' + @tag + N' FROM dbo%' OR query_sql_text LIKE N'%AS ' + @tag + N' FROM [[]dbo]%';", null, ct, tag);
        var queryId = await ScalarAsync(database, "SELECT TOP (1) query_id FROM sys.query_store_query WHERE query_text_id = @k;", textId, ct);
        var planId = await ScalarAsync(database, "SELECT TOP (1) plan_id FROM sys.query_store_plan WHERE query_id = @k ORDER BY plan_id;", queryId, ct);
        if (queryId is null || planId is null)
        {
            var texts = await ScalarAsync(database, "SELECT COUNT_BIG(*) FROM sys.query_store_query_text WHERE query_sql_text LIKE N'%AS cold%' OR query_sql_text LIKE N'%AS hot%';", null, ct);
            var state = await ScalarAsync(database, "SELECT actual_state FROM sys.database_query_store_options;", null, ct);
            Assert.Fail($"Query Store holds no plan for {tag}: text id {textId}, query id {queryId}, plan id {planId}; {texts} tagged texts, actual_state {state}");
        }

        return (queryId!.Value, planId!.Value);
    }

    private sealed class Rig
    {
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required ServerRuntime Server { get; init; }
        public required DarlingCollectorRunner Runner { get; set; }
        public required DarlingConfig Config { get; init; }
        public required StoreConfigProvider Provider { get; init; }
        public required string Database { get; init; }
        public Dictionary<string, (long QueryId, long PlanId)> Ids { get; } = new();
    }

    private static async Task Exec(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        foreach (var value in values) command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A runner wired the way the worker wires it. A second one has empty probe caches, as after a service restart.</summary>
    private static DarlingCollectorRunner NewRunner(NpgsqlDataSource postgres, DarlingConfig config) =>
        new(postgres, new CollectorDeltaCalculator(), null, capturePlans: () => true,
            queryStoreFetchTopN: () => StoreConfigProvider.ClampQueryStoreFetchTopN(config.QueryStoreFetchTopN));

    /// <summary>The Viewer's path: a store UPDATE of the setting, then the worker's per-cycle reload into the live config the runner's provider reads.</summary>
    private static async Task SetTopNInStoreAsync(Rig rig, int topN, CancellationToken ct)
    {
        await Exec(rig.Connection, ct, "UPDATE config.config_service SET query_store_fetch_top_n = $1 WHERE id = 1", topN);
        var view = await rig.Provider.LoadViewAsync(rig.Config, ct);
        Assert.NotNull(view);
        StoreConfigProvider.ApplyToConfig(rig.Config, view);
    }

    private static async Task<(HashSet<long> Plans, HashSet<long> Queries)> StoredAsync(Rig rig, CancellationToken ct)
    {
        var plans = new HashSet<long>();
        var queries = new HashSet<long>();
        await using (var command = new NpgsqlCommand("SELECT plan_id FROM collect.query_store_plan_map WHERE server_id = $1 AND database_name = $2", rig.Connection))
        {
            command.Parameters.AddWithValue(rig.Server.ServerId);
            command.Parameters.AddWithValue(rig.Database);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct)) plans.Add(reader.GetInt64(0));
        }

        await using (var command = new NpgsqlCommand("SELECT query_id FROM collect.query_store_text WHERE server_id = $1 AND database_name = $2 AND query_sql_text IS NOT NULL", rig.Connection))
        {
            command.Parameters.AddWithValue(rig.Server.ServerId);
            command.Parameters.AddWithValue(rig.Database);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct)) queries.Add(reader.GetInt64(0));
        }

        return (plans, queries);
    }

    private static async Task CollectAsync(Rig rig, CancellationToken ct)
    {
        await SqlAsync(rig.Database, "EXEC sys.sp_query_store_flush_db;", ct);

        /* The collector skips the still-open interval for 30 minutes after it last read it (QueryStoreOpenIntervalState). This database's one
           interval is open for the whole test, so a second run in the same interval needs that stamp gone, which is what 30 minutes would do. */
        await Exec(rig.Connection, ct, "DELETE FROM collect.collector_state WHERE server_id = $1 AND state_key LIKE $2", rig.Server.ServerId, QueryStoreOpenIntervalState.WatermarkKeyPrefix + "%");
        var result = await rig.Runner.RunAsync(QueryStoreCollector.Instance, rig.Server, ct);
        Assert.True(result.Rows > 0, "the run read the Query Store rows");
    }

    /// <summary>The scratch database with nine queries in one interval, the setting stored as <paramref name="topN"/>, and a runner wired the way the worker wires it.</summary>
    private static async Task RunAsync(int topN, Func<Rig, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(StoreConnectionString), "Set DARLING_TEST_PG to run the Query Store top-N fetch live tests.");
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL")), "Set DARLING_TEST_SQL (with DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD) to run the Query Store top-N fetch live tests.");
        var ct = TestContext.Current.CancellationToken;
        var database = "pm_6211_" + Guid.NewGuid().ToString("N")[..12];
        await using var scratch = await ScratchPostgres.CreateAsync(StoreConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            /* The table first and Query Store second, so the setup statements are not plans in the store. One interval of a day, so the later run is in the same interval. */
            await SqlAsync("master", $"CREATE DATABASE [{database}];", ct);
            await SqlAsync(database, @"
CREATE TABLE dbo.t (id int IDENTITY PRIMARY KEY, pad char(100) NOT NULL DEFAULT 'x');
INSERT dbo.t (pad) SELECT TOP (20000) 'x' FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;", ct);
            await SqlAsync("master", $"ALTER DATABASE [{database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL, INTERVAL_LENGTH_MINUTES = 1440, DATA_FLUSH_INTERVAL_SECONDS = 60);", ct);

            foreach (var tag in Cold) await RunQueryAsync(database, tag, hot: false, 1, ct);
            foreach (var (tag, runs) in Hot) await RunQueryAsync(database, tag, hot: true, runs, ct);
            await SqlAsync(database, "EXEC sys.sp_query_store_flush_db;", ct);

            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var others = new List<string>();
            await using (var list = new SqlConnection(Admin("master").ConnectionString))
            {
                await list.OpenAsync(ct);
                await using var command = new SqlCommand("SELECT name FROM sys.databases WHERE name <> @d", list);
                command.Parameters.AddWithValue("@d", database);
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct)) others.Add(reader.GetString(0));
            }

            var monitored = new MonitoredServer
            {
                Name = "pm6211-" + Guid.NewGuid().ToString("N")[..6],
                Host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL")!,
                Auth = string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER")) ? "integrated" : "sql",
                Username = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"),
                Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
                TrustServerCertificate = true,
                ExcludedDatabases = others,
            };
            var server = await DarlingServerConnector.ConnectAsync(monitored, null, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, server.ServerId, server.StorageName, ct);

            var provider = new StoreConfigProvider(postgres);
            var config = new DarlingConfig();
            await provider.SeedIfEmptyAsync(config, ct);
            var rig = new Rig
            {
                Connection = connection, Postgres = postgres, Server = server, Config = config, Provider = provider, Database = database,
                Runner = NewRunner(postgres, config),
            };
            await SetTopNInStoreAsync(rig, topN, ct);
            foreach (var tag in Cold.Concat(Hot.Select(h => h.Tag))) rig.Ids[tag] = await IdsAsync(database, tag, ct);

            await body(rig, ct);
            bodySucceeded = true;
        }
        finally
        {
            try
            {
                await SqlAsync("master", $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END;", CancellationToken.None);
            }
            catch (SqlException)
            {
                /* The assertions report a real failure; a cleanup that cannot reach the server leaves a uniquely named scratch database. */
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static void AssertFetched(Rig rig, HashSet<long> plans, HashSet<long> queries, IEnumerable<string> tags, bool expected)
    {
        foreach (var tag in tags)
        {
            var (queryId, planId) = rig.Ids[tag];
            Assert.True(plans.Contains(planId) == expected, $"{tag} (plan {planId}) plan XML fetched should be {expected}");
            Assert.True(queries.Contains(queryId) == expected, $"{tag} (query {queryId}) text fetched should be {expected}");
        }
    }

    [Fact]
    public async Task OnlyTheTopNPlans_AreFetched_AndAZeroMetricIsNotRanked()
    {
        await RunAsync(SmallN, async (rig, ct) =>
        {
            await CollectAsync(rig, ct);
            var (plans, queries) = await StoredAsync(rig, ct);

            AssertFetched(rig, plans, queries, Hot.Select(h => h.Tag), expected: true);
            /* The six cold plans hold the lowest plan ids and rank on nothing: writes, memory grant and tempdb are zero for every plan, and a
               ranked zero would pick the lowest three ids. */
            AssertFetched(rig, plans, queries, Cold, expected: false);
        });
    }

    [Fact]
    public async Task AForcedPlan_OutsideTheTopN_IsFetched()
    {
        await RunAsync(SmallN, async (rig, ct) =>
        {
            var (queryId, planId) = rig.Ids["cold2"];
            await SqlAsync(rig.Database, $"EXEC sys.sp_query_store_force_plan {queryId}, {planId};", ct);

            await CollectAsync(rig, ct);
            var (plans, queries) = await StoredAsync(rig, ct);

            AssertFetched(rig, plans, queries, new[] { "cold2" }, expected: true);
            AssertFetched(rig, plans, queries, Hot.Select(h => h.Tag), expected: true);
            AssertFetched(rig, plans, queries, Cold.Where(t => t != "cold2"), expected: false);
        });
    }

    [Fact]
    public async Task APlanUnrankedInOneRun_IsFetchedInALaterRunOfTheSameHour_WhenItRanks()
    {
        await RunAsync(SmallN, async (rig, ct) =>
        {
            await CollectAsync(rig, ct);
            var (first, firstQueries) = await StoredAsync(rig, ct);
            AssertFetched(rig, first, firstQueries, new[] { "cold4" }, expected: false);

            /* cold4 now runs 200 times in the same interval, more than any hot plan: it is the top plan on CPU, duration and executions. */
            await RunQueryAsync(rig.Database, "cold4", hot: false, 200, ct);
            await CollectAsync(rig, ct);
            var (second, secondQueries) = await StoredAsync(rig, ct);

            AssertFetched(rig, second, secondQueries, new[] { "cold4" }, expected: true);
            AssertFetched(rig, second, secondQueries, new[] { "cold1", "cold2", "cold3", "cold5", "cold6" }, expected: false);
        });
    }

    [Fact]
    public async Task ChangingTheSetting_TakesEffectOnTheNextRun_WithNoRestart()
    {
        await RunAsync(SmallN, async (rig, ct) =>
        {
            await CollectAsync(rig, ct);
            var (first, firstQueries) = await StoredAsync(rig, ct);
            AssertFetched(rig, first, firstQueries, Cold, expected: false);

            /* The same runner object, a store UPDATE, the worker's reload: N of 0 now fetches every plan still not stored. A cold plan has to run
               again to be read again (the collector reads what changed since its last run). */
            await SetTopNInStoreAsync(rig, 0, ct);
            foreach (var tag in Cold) await RunQueryAsync(rig.Database, tag, hot: false, 1, ct);
            await CollectAsync(rig, ct);
            var (second, secondQueries) = await StoredAsync(rig, ct);

            AssertFetched(rig, second, secondQueries, Cold.Concat(Hot.Select(h => h.Tag)), expected: true);
        });
    }

    /// <summary>The newest last_seen of the plan's map row, its plan-dim row (by the map's digest) and its query's text row.</summary>
    private static async Task<(DateTime Map, DateTime Dim, DateTime Text)> LastSeenAsync(Rig rig, string tag, CancellationToken ct)
    {
        var (queryId, planId) = rig.Ids[tag];
        await using var command = new NpgsqlCommand(@"
SELECT m.last_seen, d.last_seen, t.last_seen
FROM collect.query_store_plan_map AS m
JOIN collect.query_plan_dim AS d ON d.digest = m.digest
JOIN collect.query_store_text AS t ON t.server_id = m.server_id AND t.database_name = m.database_name AND t.query_id = $4
WHERE m.server_id = $1 AND m.database_name = $2 AND m.plan_id = $3", rig.Connection);
        command.Parameters.AddWithValue(rig.Server.ServerId);
        command.Parameters.AddWithValue(rig.Database);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(queryId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{tag} has a stored map row, plan XML and text");
        return (reader.GetDateTime(0), reader.GetDateTime(1), reader.GetDateTime(2));
    }

    [Fact]
    public async Task AFetchedPlan_ThatNoLongerRanks_StillHasItsMapPlanAndTextRowsKeptAlive()
    {
        await RunAsync(0, async (rig, ct) =>
        {
            /* Run 1 at N = 0 fetches every plan, cold1 included. Then its three rows are aged two days (inside the prune window, past the touch guard). */
            await CollectAsync(rig, ct);
            var (queryId, planId) = rig.Ids["cold1"];
            await Exec(rig.Connection, ct, "UPDATE collect.query_store_plan_map SET last_seen = last_seen - interval '2 days' WHERE server_id = $1 AND database_name = $2 AND plan_id = $3", rig.Server.ServerId, rig.Database, planId);
            await Exec(rig.Connection, ct, "UPDATE collect.query_plan_dim SET last_seen = last_seen - interval '2 days' WHERE digest IN (SELECT digest FROM collect.query_store_plan_map WHERE server_id = $1 AND database_name = $2 AND plan_id = $3)", rig.Server.ServerId, rig.Database, planId);
            await Exec(rig.Connection, ct, "UPDATE collect.query_store_text SET last_seen = last_seen - interval '2 days' WHERE server_id = $1 AND database_name = $2 AND query_id = $3", rig.Server.ServerId, rig.Database, queryId);
            var aged = await LastSeenAsync(rig, "cold1", ct);

            /* N drops to 1 and cold1 runs again: it is in the batch, it is far outside the top 1 on every metric, and it is already stored. The fetch
               skips it, but it is still referenced, so its rows must be refreshed: that touch is the only thing that keeps them from the prune. */
            await SetTopNInStoreAsync(rig, 1, ct);
            rig.Runner = NewRunner(rig.Postgres, rig.Config);
            await RunQueryAsync(rig.Database, "cold1", hot: false, 1, ct);
            await CollectAsync(rig, ct);

            var after = await LastSeenAsync(rig, "cold1", ct);
            Assert.True(after.Map > aged.Map.AddDays(1), $"plan map last_seen advanced: {aged.Map:O} -> {after.Map:O}");
            Assert.True(after.Dim > aged.Dim.AddDays(1), $"plan dim last_seen advanced: {aged.Dim:O} -> {after.Dim:O}");
            Assert.True(after.Text > aged.Text.AddDays(1), $"text last_seen advanced: {aged.Text:O} -> {after.Text:O}");
        });
    }

    [Fact]
    public async Task AQueryInTheNewestForcedPlansCapture_HasItsPlanFetched_EvenOutsideTheTopN()
    {
        await RunAsync(SmallN, async (rig, ct) =>
        {
            /* The capture names cold3's query (with some other plan id as the forced one), so cold3's plan is a variant of a forced query that does not rank. */
            var (queryId, planId) = rig.Ids["cold3"];
            await Exec(rig.Connection, ct, @"
INSERT INTO collect.query_store_forced_plans (collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id)
VALUES (1, (now() AT TIME ZONE 'utc'), $1, 'forced-test', $2, $3, $4)", rig.Server.ServerId, rig.Database, planId + 100_000, queryId);

            await CollectAsync(rig, ct);
            var (plans, queries) = await StoredAsync(rig, ct);

            AssertFetched(rig, plans, queries, new[] { "cold3" }, expected: true);
            AssertFetched(rig, plans, queries, Cold.Where(t => t != "cold3"), expected: false);
        });
    }

    [Fact]
    public async Task ThePlansAPlanCorrectionRowNames_AreFetched_EvenOutsideTheTopN()
    {
        await RunAsync(SmallN, async (rig, ct) =>
        {
            var (regressed, lastGood) = (rig.Ids["cold5"], rig.Ids["cold6"]);
            await Exec(rig.Connection, ct, @"
INSERT INTO collect.plan_correction (collection_id, collection_time, server_id, server_name, database_name, query_id, regressed_plan_id, last_good_plan_id)
VALUES (1, (now() AT TIME ZONE 'utc'), $1, 'correction-test', $2, $3, $4, $5)", rig.Server.ServerId, rig.Database, regressed.QueryId, regressed.PlanId, lastGood.PlanId);

            await CollectAsync(rig, ct);
            var (plans, queries) = await StoredAsync(rig, ct);

            AssertFetched(rig, plans, queries, new[] { "cold5", "cold6" }, expected: true);
            AssertFetched(rig, plans, queries, new[] { "cold1", "cold2", "cold3", "cold4" }, expected: false);
        });
    }

    [Fact]
    public async Task ANOfZero_FetchesEveryNewPlan()
    {
        await RunAsync(0, async (rig, ct) =>
        {
            await CollectAsync(rig, ct);
            var (plans, queries) = await StoredAsync(rig, ct);

            AssertFetched(rig, plans, queries, Cold.Concat(Hot.Select(h => h.Tag)), expected: true);
        });
    }

    [Fact]
    public async Task TheStoredPlanXmlRead_ForAnUnfetchedPlan_AnswersNotCollected_WithTheReason_AndTheFetchedPlanReturnsItsXml()
    {
        await RunAsync(SmallN, async (rig, ct) =>
        {
            await CollectAsync(rig, ct);
            var (cold, hot) = (rig.Ids["cold1"], rig.Ids["hot3"]);

            var unfetched = await DarlingMcpPlanTools.GetQueryStorePlanXml(rig.Postgres, rig.Database, cold.QueryId, rig.Server.StorageName, cold.PlanId, ct);
            Assert.Contains("not_collected", unfetched, StringComparison.Ordinal);
            Assert.Contains("top 3", unfetched, StringComparison.Ordinal);
            Assert.Contains("query_store_fetch_top_n", unfetched, StringComparison.Ordinal);

            var fetched = await DarlingMcpPlanTools.GetQueryStorePlanXml(rig.Postgres, rig.Database, hot.QueryId, rig.Server.StorageName, hot.PlanId, ct);
            Assert.Contains("ShowPlanXML", fetched, StringComparison.Ordinal);
            Assert.DoesNotContain("not_collected", fetched, StringComparison.Ordinal);
        });
    }
}

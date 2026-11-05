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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7567: the service's own statements are marked with a trailing comment and told apart from the application's in
/// <c>pg_stat_statements</c>, and Analysis leaves them out of the totals and the candidate lists.
/// <para>The first test reads the stored text back from a real <c>pg_stat_statements</c>: a LEADING comment is cut from
/// the stored text, so the marker trails, and every statement of a batch carries its own. The others seed the store with
/// a service statement that dominates a quiet server beside an ordinary application statement and read it the way the
/// pages, the charts and the Analysis pass do.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgOwnStatementLiveTests
{
    internal const int ServerId = -756701;
    internal const string ServerName = "pg-own-statement-e2e";
    private const long ServiceQueryId = -7567000000000000001L;
    private const long AppQueryId = -7567000000000000002L;
    private const long SharedQueryId = -7567000000000000003L;
    private const long ServiceUserId = 10;
    private const long AppUserId = 20;
    private const long DatabaseId = 16384;

    internal static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheMarker_SurvivesInPgStatStatements_OnTheProviderCommand_AndOnEveryStatementOfABatch()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: CREATE EXTENSION lands in the first schema of the store's search_path, which is collect, so on the
           shared store it left collect.pg_stat_statements and collect.pg_stat_statements_info behind (LiveStoreResidueException).
           A scratch database of its own takes the extension and goes with it. */
        var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await RunMarkerChecksAsync(scratch.ConnectionString, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// The connect-time read of the extension schemas and the per-pass read are ONE <c>pg_stat_statements</c> entry (same
    /// text, same login and database, the parameter value is not part of the key), and the entry keeps the text of
    /// whichever call created it. After a service start or a <c>pg_stat_statements_reset()</c> the connector runs first, so
    /// an unmarked connector copy would leave the standing per-pass read counted as the application's (#7567 round 6).
    /// </summary>
    [Fact]
    public async Task TheConnectorsSchemaRead_RunningBeforeThePerPassRead_StillLeavesTheSharedEntryMarked_AndTheFetchLearnsItAsTheServices()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: CREATE EXTENSION would leave residue in the shared store; a scratch database of its own goes with it. */
        var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            try
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
                await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT pg_stat_statements_reset()");
            }
            catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
            {
                Assert.Skip("pg_stat_statements is not available on this server: " + ex.Message);
            }

            /* The connector first, the way a service start runs it, then the per-pass read of a collector that reads Aurora functions. */
            await DarlingServerConnector.ResolveAuroraFunctionSchemaAsync(connection, "aurora_version", ct);
            var context = new CollectorContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                CollectionTime = DateTime.UtcNow,
                Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo
                {
                    Engine = CollectorTargetEngine.PostgreSql,
                    PostgresMajorVersion = connection.PostgreSqlVersion.Major,
                    IsAurora = true,
                },
            };
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
                context, PgWaitStatsCollector.Instance, connection, null, ct));

            var texts = new List<string>();
            await using (var read = new NpgsqlCommand(
                "SELECT query FROM pg_stat_statements WHERE query LIKE '%pg\\_catalog.pg\\_extension AS e%'", connection))
            await using (var reader = await read.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    texts.Add(reader.GetString(0));
                }
            }

            var stored = Assert.Single(texts);
            Assert.True(PgOwnStatement.IsOwn(stored), stored);

            /* And the hourly fetch learns it as the service's: a non-NULL own_entries for the shared queryid. */
            context.PgExtensionSchemas = new Dictionary<string, string>(StringComparer.Ordinal) { ["pg_stat_statements"] = "public" };
            await using var fetch = new NpgsqlCommand(
                PgOwnStatement.Mark(PgStatementText.FetchSqlFor(false, connection.PostgreSqlVersion.Major, context)), connection);
            fetch.Parameters.AddWithValue(10_000);
            await using var fetched = await fetch.ExecuteReaderAsync(ct);
            string? ownEntries = null;
            var found = false;
            while (await fetched.ReadAsync(ct))
            {
                if (!fetched.IsDBNull(1) && fetched.GetString(1).Contains("pg_catalog.pg_extension AS e", StringComparison.Ordinal))
                {
                    found = true;
                    ownEntries = fetched.IsDBNull(2) ? null : fetched.GetString(2);
                    break;
                }
            }

            Assert.True(found);
            Assert.NotNull(ownEntries);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static async Task RunMarkerChecksAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            await using var probe = new NpgsqlCommand("SELECT count(*) FROM pg_stat_statements", connection);
            await probe.ExecuteScalarAsync(ct);
        }
        catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000")
        {
            Assert.Skip("pg_stat_statements is not preloaded on this server: " + ex.Message);
        }

        /* First, so the check that a new target connection runs is the call that creates its pg_stat_statements entry: the entry keeps the text of the call that made it. */
        var pinned = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Options = MonitoredServerConnection.PinnedSearchPathOption,
            ApplicationName = "pm7567-r7-pinned",
        };
        await using (var pinnedConnection = PostgresTargetProvider.CreatePinnedConnection(pinned.ConnectionString))
        {
            await pinnedConnection.OpenAsync(ct);
        }

        /* The provider's own command: a single statement, with the trailing semicolon a collector may carry. */
        var query = new CollectorQuery("SELECT 7567 + 1 AS pm7567_single;");
        await using (var command = PostgresTargetProvider.Instance.CreateCommand(query, connection, 30))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        /* A batch the way the wait sampler sends one: every statement is stored on its own row. */
        var batch = PgOwnStatement.MarkBatch(new[]
        {
            "SELECT 7567 + 2 AS pm7567_first, 1 AS a",
            "SELECT pg_sleep(0.01)",
            "SELECT pg_stat_clear_snapshot()",
            "SELECT 7567 + 3 AS pm7567_last, 1 AS a, 2 AS b",
        });
        await using (var batchCommand = new NpgsqlCommand(batch, connection))
        {
            await batchCommand.ExecuteNonQueryAsync(ct);
        }

        /* A named parameter, the way ResolvePgExtensionSchemasAsync binds @functions: Npgsql rewrites it to $1 before the server sees it. */
        await using (var named = new NpgsqlCommand(
            PgOwnStatement.Mark("SELECT @functions::text AS pm7567_named"), connection))
        {
            named.Parameters.AddWithValue("functions", "x");
            await named.ExecuteNonQueryAsync(ct);
        }

        /* The connect-time probes (round 4): ProbeAuroraAsync resolves the extension schemas through the SAME statement the per-pass
           read sends, and runs first after a service start or a pg_stat_statements_reset(), so an unmarked copy there would
           create the shared entry and keep the unmarked text. Both statements of the Aurora probe are marked too. */
        await DarlingServerConnector.ProbeAuroraAsync(connection, ct);

        /* The control: the same shape with no marker, which must read as the application's. */
        await using (var plain = new NpgsqlCommand("SELECT 7567 + 4 AS pm7567_plain, 1 AS a, 2 AS b, 3 AS c", connection))
        {
            await plain.ExecuteNonQueryAsync(ct);
        }

        var stored = new Dictionary<string, string>(StringComparer.Ordinal);
        await using (var read = new NpgsqlCommand(
            "SELECT query FROM pg_stat_statements WHERE query LIKE '%pm7567\\_%' OR query LIKE '%pg\\_stat\\_clear\\_snapshot%' OR query LIKE '%pg\\_sleep(%' "
            + "OR query LIKE '%pg\\_catalog.pg\\_extension AS e%' OR query LIKE '%aurora\\_probe%' OR query LIKE 'SELECT pg\\_catalog.current\\_setting(%'",
            connection))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var text = reader.GetString(0);
                stored[text] = text;
            }
        }

        string Stored(string needle) => Assert.Single(stored.Values, t => t.Contains(needle, StringComparison.Ordinal));

        Assert.True(PgOwnStatement.IsOwn(Stored("pm7567_single")), Stored("pm7567_single"));
        Assert.True(PgOwnStatement.IsOwn(Stored("pm7567_first")), Stored("pm7567_first"));
        Assert.True(PgOwnStatement.IsOwn(Stored("pm7567_last")), Stored("pm7567_last"));
        Assert.True(PgOwnStatement.IsOwn(Stored("pm7567_named")), Stored("pm7567_named"));
        Assert.True(PgOwnStatement.IsOwn(Stored("pg_sleep(")), Stored("pg_sleep("));
        Assert.True(PgOwnStatement.IsOwn(Stored("pg_stat_clear_snapshot")), Stored("pg_stat_clear_snapshot"));
        Assert.True(PgOwnStatement.IsOwn(Stored("pg_catalog.pg_extension AS e")), Stored("pg_catalog.pg_extension AS e"));
        Assert.True(PgOwnStatement.IsOwn(Stored("aurora_probe")), Stored("aurora_probe"));
        Assert.True(PgOwnStatement.IsOwn(Stored("SELECT pg_catalog.current_setting($1, $2)")), Stored("SELECT pg_catalog.current_setting($1, $2)"));
        Assert.False(PgOwnStatement.IsOwn(Stored("pm7567_plain")), Stored("pm7567_plain"));

        /* The SQL predicate the readers use agrees with the C# one on the stored text. */
        await using var predicate = new NpgsqlCommand(
            "SELECT " + PgOwnStatement.IsOwnSql("query") + " FROM pg_stat_statements WHERE query LIKE '%pm7567\\_single%'", connection);
        Assert.True((bool)(await predicate.ExecuteScalarAsync(ct))!);

        /* Round 7: the commands that build their own SQL and skip the provider. The zone read is a two-statement batch, and
           pg_stat_statements stores each statement on its own, so BOTH must end with the marker; the capability probes and the
           per-connection search_path check run on a target too. Each goes through its real code path, not a copy of its SQL. */
        PgLogNamedZone.ForgetZoneNames();
        var zonePairs = new[] { new PgLogZonePair("2026-03-01 12:00:00", "UTC") };
        Assert.NotNull(await PgLogNamedZone.ResolveAsync(connection, "UTC", zonePairs, 30, ct));
        /* The second read of a setting already found to be a named zone sends the other form of the batch. */
        Assert.NotNull(await PgLogNamedZone.ResolveAsync(connection, "UTC", zonePairs, 30, ct));
        PgFileSettingsCapability.Reset();
        PgLogFormatCapability.Reset();
        PgReadBinaryFileCapability.Reset();
        await PgFileSettingsCapability.IsReadableAsync(connection, "pm7567-r7", ct);
        await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "pm7567-r7", ct);
        await PgReadBinaryFileCapability.IsGrantedAsync(connection, "pm7567-r7", ct);

        var texts = new List<string>();
        await using (var readAll = new NpgsqlCommand("SELECT query FROM pg_stat_statements", connection))
        await using (var readerAll = await readAll.ExecuteReaderAsync(ct))
        {
            while (await readerAll.ReadAsync(ct))
            {
                texts.Add(readerAll.GetString(0));
            }
        }

        void AssertMarked(string what, string needle)
        {
            var found = texts.Where(t => t.Contains(needle, StringComparison.Ordinal)).ToList();
            Assert.True(found.Count > 0, what + " was not stored: " + needle);
            foreach (var text in found)
            {
                Assert.True(PgOwnStatement.IsOwn(text), what + " is stored without the marker: " + text);
            }
        }

        AssertMarked("the zone lookup (statement one)", "FROM pg_catalog.pg_timezone_names");
        AssertMarked("the zone match (statement two)", "GROUP BY m.stamp, m.zone");
        AssertMarked("the known-zone set (statement one, later reads)", "set_config($2, $1, $3)");
        AssertMarked("the pg_file_settings probe", "has_table_privilege(");
        AssertMarked("the log format probe", "string_to_array(");
        AssertMarked("the pg_read_binary_file probe", "has_function_privilege(current_user, $3, $4)");
        AssertMarked("the search_path check", "SELECT pg_catalog.current_setting($1)");
    }

    [Fact]
    public async Task ASeededServiceStatement_IsMarkedInTopQueries_AndNeverChosenForTheTrendCharts()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, busy: false);
            var from = DateTime.UtcNow.AddHours(-1);
            var to = DateTime.UtcNow.AddMinutes(1);

            var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, ServerId, from, to, 10, ct);

            /* Kept in Top Query Shapes, and marked. */
            var service = Assert.Single(page.Rows, r => r.QueryId == ServiceQueryId);
            var app = Assert.Single(page.Rows, r => r.QueryId == AppQueryId);
            Assert.True(service.IsServiceStatement);
            Assert.False(app.IsServiceStatement);

            /* The service's statement is the heaviest, and still never the automatic pick for either chart. */
            Assert.Equal(AppQueryId, await DarlingPgTrendReader.GetTopQueryIdAsync(dataSource, ServerId, from, to, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ServiceStatements_AreLeftOutOfTheCandidates_TheStatementShares_AndTheServerWideMean_AndARealStatementStillRanks()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            /* A busy seed: the application's statement runs every minute for the whole hour, the service's far heavier one beside it. */
            await SeedAsync(connection, ct, busy: true);
            var from = DateTime.UtcNow.AddHours(-1);
            var to = DateTime.UtcNow.AddMinutes(1);
            var naiveFrom = DateTime.SpecifyKind(from, DateTimeKind.Unspecified);
            var naiveTo = DateTime.SpecifyKind(to, DateTimeKind.Unspecified);

            /* The fact collector's top statements: only the application's, and the window total is the application's alone. */
            await using (var top = new NpgsqlCommand(PgTargetFactCollector.PgTargetTopStatementsSql, connection))
            {
                top.Parameters.AddWithValue(ServerId);
                top.Parameters.AddWithValue(naiveFrom);
                top.Parameters.AddWithValue(naiveTo);
                top.Parameters.AddWithValue(5);
                await using var reader = await top.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(AppQueryId, reader.GetInt64(reader.GetOrdinal("queryid")));
                Assert.Equal(reader.GetInt64(reader.GetOrdinal("total_exec_ms")), reader.GetInt64(reader.GetOrdinal("window_total_exec_ms")));
                Assert.False(await reader.ReadAsync(ct));
            }

            /* The anomaly pass's candidates: the same. A quiet-server swing between 0% and 100% needs a service statement in the denominator. */
            await using (var share = new NpgsqlCommand(PgTargetAnomalyDetector.StatementShareWindowSql, connection))
            {
                share.Parameters.AddWithValue(ServerId);
                share.Parameters.AddWithValue(naiveFrom);
                share.Parameters.AddWithValue(naiveTo);
                share.Parameters.AddWithValue(5);
                await using var reader = await share.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(AppQueryId, reader.GetInt64(0));
                Assert.Equal(1.0, reader.GetDouble(1), 6);
                Assert.Equal(1.0, reader.GetDouble(2), 6);
                Assert.False(await reader.ReadAsync(ct));
            }

            /* The server-wide mean per call: the application's 20 ms, not the service's 1000 ms sleeps. */
            await using (var mean = new NpgsqlCommand(PgTargetAnomalyDetector.StatementMeanWindowSql, connection))
            {
                mean.Parameters.AddWithValue(ServerId);
                mean.Parameters.AddWithValue(naiveFrom);
                mean.Parameters.AddWithValue(naiveTo);
                await using var reader = await mean.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(20.0, reader.GetDouble(0), 6);
                Assert.Equal(20.0, reader.GetDouble(1), 6);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The shared-queryid case (#7567 round 2): the service's marked entry and an application entry under ANOTHER login share
    /// one queryid, as <c>pg_stat_statements</c> keeps them (one entry per userid). Own-ness is per entry, so the application's
    /// rows stay in the bad-actor candidates and the shares, and the service's do not.
    /// </summary>
    [Fact]
    public async Task AnApplicationEntryUnderAnotherLogin_SharingTheServiceQueryId_StaysInAnalysis_AndTheServicesEntryDoesNot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* One text for the queryid (the first call's, the service's, marked), and the list says only user 10's entry is the service's. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen, own_entries)
VALUES ($1, $2, $3, $4, $4, $5::jsonb)",
                ServerId, SharedQueryId, PgOwnStatement.Mark("SELECT pg_sleep($1)"), DarlingMcpTestData.Naive(DateTime.UtcNow), OwnEntries(ServiceUserId));
            for (var i = 6; i >= 1; i--)
            {
                var at = DateTime.UtcNow.AddMinutes(-i);
                await SeedStatsAsync(connection, ct, at, SharedQueryId, ServiceUserId, deltaCalls: 30, deltaTimeMs: 30_000);
                await SeedStatsAsync(connection, ct, at, SharedQueryId, AppUserId, deltaCalls: 10, deltaTimeMs: 200);
                /* The service's pair reversed: login = the service's database id, database = the service's login id. An application
                   entry, so it stays in; matching on the two numbers in any order would drop it (round 6). */
                await SeedStatsAsync(connection, ct, at, SharedQueryId, DatabaseId, deltaCalls: 5, deltaTimeMs: 100, databaseId: ServiceUserId);
            }

            var from = DateTime.UtcNow.AddHours(-1);
            var to = DateTime.UtcNow.AddMinutes(1);
            var naiveFrom = DateTime.SpecifyKind(from, DateTimeKind.Unspecified);
            var naiveTo = DateTime.SpecifyKind(to, DateTimeKind.Unspecified);

            /* The bad-actor candidates: the shared queryid is there, with the application's time only (6 x 200 ms and the reversed pair's 6 x 100 ms), never the service's. */
            await using (var top = new NpgsqlCommand(PgTargetFactCollector.PgTargetTopStatementsSql, connection))
            {
                top.Parameters.AddWithValue(ServerId);
                top.Parameters.AddWithValue(naiveFrom);
                top.Parameters.AddWithValue(naiveTo);
                top.Parameters.AddWithValue(5);
                await using var reader = await top.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(SharedQueryId, reader.GetInt64(reader.GetOrdinal("queryid")));
                Assert.Equal(1800, reader.GetInt64(reader.GetOrdinal("total_exec_ms")));
                Assert.Equal(1800, reader.GetInt64(reader.GetOrdinal("window_total_exec_ms")));
                Assert.False(await reader.ReadAsync(ct));
            }

            /* The shares: the application's entry holds the whole of the (application) window. */
            await using (var share = new NpgsqlCommand(PgTargetAnomalyDetector.StatementShareWindowSql, connection))
            {
                share.Parameters.AddWithValue(ServerId);
                share.Parameters.AddWithValue(naiveFrom);
                share.Parameters.AddWithValue(naiveTo);
                share.Parameters.AddWithValue(5);
                await using var reader = await share.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(SharedQueryId, reader.GetInt64(0));
                Assert.Equal(1.0, reader.GetDouble(1), 6);
                Assert.False(await reader.ReadAsync(ct));
            }

            /* The top-queries read groups the two logins into one row, so it is NOT flagged as the service's (not every entry is). */
            var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, ServerId, from, to, 10, ct);
            /* One row per (queryid, database): the main database's group holds the service's entry and the application's, and the reversed pair is a group of its own. */
            var sharedRows = page.Rows.Where(r => r.QueryId == SharedQueryId).ToArray();
            Assert.Equal(2, sharedRows.Length);
            Assert.All(sharedRows, r => Assert.False(r.IsServiceStatement));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Change Watch reads the top queries without the service's statements (#7567): the service's sleep is the heaviest
    /// statement, and with a cap of one the application's statement is still the row returned.
    /// </summary>
    [Fact]
    public async Task TopQueries_WithServiceStatementsExcluded_DoNotLetTheSamplerTakeTheSlot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct, busy: false);
            var from = DateTime.UtcNow.AddHours(-1);
            var to = DateTime.UtcNow.AddMinutes(1);

            var withService = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, ServerId, from, to, 1, ct);
            Assert.Equal(ServiceQueryId, Assert.Single(withService.Rows).QueryId);

            var withoutService = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(
                dataSource, ServerId, from, to, 1, includeServiceStatements: false, ct);
            Assert.Equal(AppQueryId, Assert.Single(withoutService.Rows).QueryId);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The application statement (20 ms a call) every minute for the last 30 minutes, and the service's sampler sleep
    /// (1000 ms a call) on a few of them. Text for both, the service's carrying the marker the way the collector's
    /// statements are stored.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, bool busy)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen, own_entries)
VALUES ($1, $2, $3, $5, $5, $7::jsonb), ($1, $4, $6, $5, $5, NULL)
ON CONFLICT (server_id, queryid) DO UPDATE SET query_text = EXCLUDED.query_text, own_entries = EXCLUDED.own_entries",
            ServerId, ServiceQueryId, PgOwnStatement.Mark("SELECT pg_sleep($1)"), AppQueryId,
            DarlingMcpTestData.Naive(DateTime.UtcNow), "SELECT id FROM orders WHERE customer_id = $1",
            OwnEntries(ServiceUserId));

        var minutes = busy ? 30 : 6;
        for (var i = minutes; i >= 1; i--)
        {
            var at = DateTime.UtcNow.AddMinutes(-i);
            await SeedStatsAsync(connection, ct, at, AppQueryId, AppUserId, deltaCalls: 10, deltaTimeMs: 200);
            if (!busy || i % 5 == 0)
            {
                await SeedStatsAsync(connection, ct, at, ServiceQueryId, ServiceUserId, deltaCalls: 30, deltaTimeMs: 30_000);
            }
        }
    }

    internal static async Task SeedStatsAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        long queryId, long userId, long deltaCalls, long deltaTimeMs, long databaseId = DatabaseId) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned,
     shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written,
     delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, $5, $9, $8, TRUE,
        100, 5000, 91.5, 250,
        10, 5, 0, 0,
        $6, $7, 0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            queryId, deltaCalls, deltaTimeMs, userId, databaseId);

    /// <summary>The JSON the text fetch stores for the service's own entries: [{"u": user_id, "d": database_id}].</summary>
    private static string OwnEntries(long userId) =>
        string.Create(CultureInfo.InvariantCulture, $"[{{\"u\":{userId},\"d\":{DatabaseId}}}]");

    internal static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_text WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

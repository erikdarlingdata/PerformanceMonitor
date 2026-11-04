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
    private const int ServerId = -756701;
    private const string ServerName = "pg-own-statement-e2e";
    private const long ServiceQueryId = -7567000000000000001L;
    private const long AppQueryId = -7567000000000000002L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheMarker_SurvivesInPgStatStatements_OnTheProviderCommand_AndOnEveryStatementOfABatch()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
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

        /* The provider's own command: a single statement, with the trailing semicolon a collector may carry. */
        var query = new CollectorQuery("SELECT 7567 + 1 AS pm7567_single;");
        await using (var command = PostgresTargetProvider.Instance.CreateCommand(query, connection, 30))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        /* A batch the way the wait sampler sends one: every statement is stored on its own row. */
        var batch = PgOwnStatement.MarkBatch(new[]
        {
            "SELECT 7567 + 2 AS pm7567_first",
            "SELECT pg_sleep(0.01)",
            "SELECT pg_stat_clear_snapshot()",
            "SELECT 7567 + 3 AS pm7567_last",
        });
        await using (var batchCommand = new NpgsqlCommand(batch, connection))
        {
            await batchCommand.ExecuteNonQueryAsync(ct);
        }

        /* The control: the same shape with no marker, which must read as the application's. */
        await using (var plain = new NpgsqlCommand("SELECT 7567 + 4 AS pm7567_plain", connection))
        {
            await plain.ExecuteNonQueryAsync(ct);
        }

        var stored = new Dictionary<string, string>(StringComparer.Ordinal);
        await using (var read = new NpgsqlCommand(
            "SELECT query FROM pg_stat_statements WHERE query LIKE '%pm7567\\_%' OR query LIKE '%pg\\_stat\\_clear\\_snapshot%' OR query LIKE '%pg\\_sleep(%'",
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
        Assert.True(PgOwnStatement.IsOwn(Stored("pg_sleep(")), Stored("pg_sleep("));
        Assert.True(PgOwnStatement.IsOwn(Stored("pg_stat_clear_snapshot")), Stored("pg_stat_clear_snapshot"));
        Assert.False(PgOwnStatement.IsOwn(Stored("pm7567_plain")), Stored("pm7567_plain"));

        /* The SQL predicate the readers use agrees with the C# one on the stored text. */
        await using var predicate = new NpgsqlCommand(
            "SELECT " + PgOwnStatement.IsOwnSql("query") + " FROM pg_stat_statements WHERE query LIKE '%pm7567\\_single%'", connection);
        Assert.True((bool)(await predicate.ExecuteScalarAsync(ct))!);
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
    /// The application statement (20 ms a call) every minute for the last 30 minutes, and the service's sampler sleep
    /// (1000 ms a call) on a few of them. Text for both, the service's carrying the marker the way the collector's
    /// statements are stored.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, bool busy)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_text (server_id, queryid, query_text, first_seen, last_seen)
VALUES ($1, $2, $3, $5, $5), ($1, $4, $6, $5, $5)
ON CONFLICT (server_id, queryid) DO UPDATE SET query_text = EXCLUDED.query_text",
            ServerId, ServiceQueryId, PgOwnStatement.Mark("SELECT pg_sleep($1)"), AppQueryId,
            DarlingMcpTestData.Naive(DateTime.UtcNow), "SELECT id FROM orders WHERE customer_id = $1");

        var minutes = busy ? 30 : 6;
        for (var i = minutes; i >= 1; i--)
        {
            var at = DateTime.UtcNow.AddMinutes(-i);
            await SeedStatsAsync(connection, ct, at, AppQueryId, deltaCalls: 10, deltaTimeMs: 200);
            if (!busy || i % 5 == 0)
            {
                await SeedStatsAsync(connection, ct, at, ServiceQueryId, deltaCalls: 30, deltaTimeMs: 30_000);
            }
        }
    }

    private static async Task SeedStatsAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc,
        long queryId, long deltaCalls, long deltaTimeMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned,
     shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written,
     delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, $5, 16384, 10, TRUE,
        100, 5000, 91.5, 250,
        10, 5, 0, 0,
        $6, $7, 0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            queryId, deltaCalls, deltaTimeMs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_text WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

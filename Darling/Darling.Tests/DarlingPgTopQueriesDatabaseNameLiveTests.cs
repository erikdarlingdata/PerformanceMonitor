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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7731: <c>get_pg_top_queries</c> names the database. The read's grain is (queryid, database_id), so one statement run in two
/// databases is two rows, and with only a number on each the page showed one Query ID twice with different figures. The collector now
/// stores <c>database_name</c> beside <c>dbid</c> and the reply carries it beside <c>database_id</c>; a row stored before the column
/// existed has no name and says <c>database &lt;id&gt;</c>. The name is a carried attribute: it never splits or merges a group.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPgTopQueriesDatabaseNameLiveTests
{
    private const int ServerId = -853978;
    private const string ServerName = "pg-top-queries-dbname";
    private const long QueryId = 7731_000_000_001L;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OneQueryIdInThreeDatabases_ComesBackAsThreeRows_EachNamingItsDatabase_AndAnUnnamedRowSaysDatabaseId()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database name test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, MonitoredEngineKind.Postgres);

            var t0 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-30));
            var t1 = t0.AddSeconds(60);

            /* Database 16384 is named on both snapshots. Database 16385 is the row an older build stored: no name on either. Database
               16386 has an old unnamed snapshot and a newer named one, so the NEWEST non-null name is the one carried. */
            foreach (var (time, calls) in new[] { (t0, 0L), (t1, 1820L) })
            {
                await SeedAsync(connection, ct, time, 16384, "appdb", calls, 900);
                await SeedAsync(connection, ct, time, 16385, null, calls / 10, 90);
            }

            await SeedAsync(connection, ct, t0, 16386, null, 0, 0);
            await SeedAsync(connection, ct, t1, 16386, "reportsdb", 7, 70);

            var root = JsonDocument.Parse(await DarlingMcpPgStatementTools.GetPgTopQueries(dataSource, ServerName, 4)).RootElement;
            var rows = root.GetProperty("queries").EnumerateArray().ToArray();
            Assert.NotEmpty(rows);

            JsonElement RowOf(long databaseId) => rows.Single(r => r.GetProperty("database_id").GetInt64() == databaseId);

            Assert.Equal(3, rows.Length);
            Assert.Equal("appdb", RowOf(16384).GetProperty("database_name").GetString());
            Assert.Equal("database 16385", RowOf(16385).GetProperty("database_name").GetString());
            Assert.Equal("reportsdb", RowOf(16386).GetProperty("database_name").GetString());
            Assert.Equal(1820, RowOf(16384).GetProperty("calls").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheMigrationLadder_AddsANullableDatabaseNameColumnToTheStatementStats_AndASecondRunChangesNothing()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database name test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var command = new NpgsqlCommand(
            "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
            + "WHERE table_schema = 'collect' AND table_name = 'pg_statement_stats' AND column_name = 'database_name'", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "collect.pg_statement_stats has no database_name column");
        Assert.Equal("text", reader.GetString(0));
        Assert.Equal("YES", reader.GetString(1));
        Assert.True(reader.IsDBNull(2), "the column has no default");
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long databaseId, string? databaseName, long deltaCalls, long deltaTimeMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, database_name, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, $5, $6, $7, 10, TRUE, 100, 5000, 91.5, 250, $8, $9, 0)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            QueryId, databaseId, (object?)databaseName ?? DBNull.Value, deltaCalls, deltaTimeMs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM pg_statement_text WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

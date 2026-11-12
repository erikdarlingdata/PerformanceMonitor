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

            var t0 = DarlingMcpTestData.TruncateToSeconds(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc).AddMinutes(-30));
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

    /// <summary>
    /// V249 on a COMPRESSED hypertable, the shape field stores have. The store is built at V248 (the column dropped again), a row is
    /// planted, the table is made a hypertable and its chunk compressed the product's way, then the V249 stamp is rolled back and the
    /// ladder re-run. The ALTER must succeed, the column must exist and be nullable, the planted row must read NULL, and a second run
    /// must change nothing.
    /// </summary>
    [Fact]
    public async Task AStoreAtV248WithCompressedChunks_MigratesToV249_AndOldRowsReadDatabaseNameNull()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V249 compressed-hypertable round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await PgMigrations.MigrateAsync(connection, ct);

        /* A V248 store has no database_name column. */
        await DarlingMcpTestData.ExecAsync(connection, ct, "ALTER TABLE collect.pg_statement_stats DROP COLUMN database_name");
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, delta_calls, delta_total_exec_time_ms, delta_rows)
VALUES ($1, $2, $3, $4, $5, 16384, 10, TRUE, 100, 5000, 91.5, 250, 7, 70, 0)",
            1L, DarlingMcpTestData.Naive(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)), ServerId, ServerName, QueryId);

        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB must be available on CI for the compressed-hypertable round trip");
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "SELECT create_hypertable('collect.pg_statement_stats', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true, migrate_data => true)");
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "ALTER TABLE collect.pg_statement_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')");

        var chunks = new System.Collections.Generic.List<string>();
        await using (var list = new NpgsqlCommand("SELECT show_chunks('collect.pg_statement_stats')::text", connection))
        await using (var reader = await list.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        foreach (var chunk in chunks)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"SELECT compress_chunk('{chunk}', if_not_compressed => true)");
        }

        Assert.Equal(true, await ScalarAsync(connection, ct,
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.chunks WHERE hypertable_name = 'pg_statement_stats' AND is_compressed)"));
        Assert.Equal(false, await ScalarAsync(connection, ct,
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'pg_statement_stats' AND column_name = 'database_name')"));

        /* Roll the version stamp back to V248, then upgrade: V249 and every rung above it apply. */
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 249");
        Assert.Equal(StorageVersion.SchemaVersion - 248, await PgMigrations.MigrateAsync(connection, ct));

        await using (var column = new NpgsqlCommand(
            "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
            + "WHERE table_schema = 'collect' AND table_name = 'pg_statement_stats' AND column_name = 'database_name'", connection))
        await using (var reader = await column.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct), "V249 did not add database_name to the compressed hypertable");
            Assert.Equal("text", reader.GetString(0));
            Assert.Equal("YES", reader.GetString(1));
            Assert.True(reader.IsDBNull(2), "the column has no default");
        }

        Assert.Equal(true, await ScalarAsync(connection, ct,
            "SELECT database_name IS NULL FROM collect.pg_statement_stats WHERE collection_id = 1"));
        Assert.Equal(true, await ScalarAsync(connection, ct,
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.chunks WHERE hypertable_name = 'pg_statement_stats' AND is_compressed)"));

        Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
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

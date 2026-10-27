/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6836: the READ path of the collectors that send an old-version query (#6206), run end to end on a real server:
/// the collector's own query text, its <c>ReadAsync</c>, then its <c>WritePayload</c> through the real
/// <see cref="PgCollectorRowWriter"/> into the migrated store. #6206 ran the old-version texts on PostgreSQL 10 and 12
/// and never read their rows, so a NULL projection that the reader called <c>GetInt64</c> on went out in 3.11
/// (<c>Column 'wal_records' is null</c> on every PostgreSQL 10 and 12 target).
/// <para>The server here is newer than the version the test claims. The collector is handed the old major (and the old
/// extension version), so it projects the NULLs an old server would send. Where the old text names a column the new
/// server no longer has, the source object is a stand-in: for <c>pg_stat_statements</c> a table in a scratch schema with
/// the pre-1.8 column names, reached through the same extension-schema lookup the real view goes through. Write stats
/// below 14 read <c>pg_stat_bgwriter</c> columns that PostgreSQL 17 moved out, so they cannot run on a new server; the
/// Aurora flavor of the query sends the same NULL WAL projections, and runs here.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgOldVersionReadPathLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the old-version read path tests.";
    private const string Scratch = "pm6836_old";
    private const string Slot = "pm6836_slot";
    private static readonly DateTime Anchor = new(2031, 6, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private static (int ServerId, string ServerName) NewServer()
    {
        var suffix = Guid.NewGuid().ToString("N")[..12];
        return (-Random.Shared.Next(1_000_000, int.MaxValue), "pm6836-" + suffix);
    }

    private static CollectorContext Context(int serverId, string serverName, DateTime at, int major, bool aurora = false, string? statementsVersion = null)
        => new()
        {
            ServerId = serverId,
            ServerName = serverName,
            CollectionTime = at,
            Deltas = DeltaCalculator,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major, PostgresVersionNum = major * 10000, IsAurora = aurora,
            },
            PgExtensionSchemas = new Dictionary<string, string> { ["pg_stat_statements"] = Scratch },
            PgExtensionVersions = statementsVersion is null ? null : new Dictionary<string, string> { ["pg_stat_statements"] = statementsVersion },
        };

    private static readonly CollectorDeltaCalculator DeltaCalculator = new();

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, bool migrate, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = migrate ? PgSchemaGenerator.SearchPath : "pg_catalog", Pooling = false };
        var connection = new NpgsqlConnection(builder.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            if (migrate)
            {
                await PgMigrations.MigrateAsync(connection, ct);
            }
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        return connection;
    }

    /// <summary>The collector's query on the source, its <c>ReadAsync</c>, then every row through the store's COPY.</summary>
    private static async Task<List<TRow>> RunAsync<TRow>(
        CollectorDefinitionBase<TRow> definition, CollectorContext context, NpgsqlConnection source, NpgsqlConnection store, CancellationToken ct)
    {
        List<TRow> rows;
        await using (var command = new NpgsqlCommand(definition.BuildQuery(context).Text, source))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            rows = await definition.ReadAsync(reader, context, ct);
        }

        var writer = new PgCollectorRowWriter();
        using var importer = await store.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct);
        writer.Importer = importer;
        foreach (var row in rows)
        {
            await importer.StartRowAsync(ct);
            if (definition.IncludesCollectionId)
            {
                writer.Value(CollectionIdGenerator.Next());
            }

            writer.Value(context.CollectionTime).Value(context.ServerId).Value(context.ServerName);
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
        }

        await importer.CompleteAsync(ct);
        return rows;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(serverId);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task CleanupAsync(NpgsqlConnection cleanup, int serverId, CancellationToken ct)
    {
        foreach (var table in new[] { "pg_statement_stats", "pg_replication_slot_stats", "pg_session_states", "pg_write_stats" })
        {
            await ExecAsync(cleanup, $"DELETE FROM collect.{table} WHERE server_id = {serverId}", ct);
        }

        await ExecAsync(cleanup, $"DROP SCHEMA IF EXISTS {Scratch} CASCADE", ct);
        await ExecAsync(cleanup, $"SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = '{Slot}'", ct);
    }

    /// <summary>
    /// The 3.11 failure. Three server versions send the NULL WAL projection: PostgreSQL 10 with extension 1.6,
    /// PostgreSQL 12 with 1.7, and a current server whose extension was never updated past 1.7. Rows must come back, the
    /// WAL figures must stay NULL (not zero) in the store, and the top-queries read must report them as not reported.
    /// </summary>
    [Theory]
    [InlineData(10, "1.6")]
    [InlineData(12, "1.7")]
    [InlineData(15, "1.7")]
    public async Task StatementStatsWithoutWalColumns_ReadsAndStoresNullWal_AndTheTopQueriesReadReportsItNotReported(int major, string extensionVersion)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        await using var store = await OpenAsync(connectionString!, migrate: true, ct);
        await using var source = await OpenAsync(connectionString!, migrate: false, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(source, $"""
                DROP SCHEMA IF EXISTS {Scratch} CASCADE;
                CREATE SCHEMA {Scratch};
                CREATE TABLE {Scratch}.pg_stat_statements (
                    queryid bigint, dbid oid, userid oid, calls bigint,
                    total_time double precision, min_time double precision, max_time double precision, mean_time double precision,
                    rows bigint, shared_blks_hit bigint, shared_blks_read bigint, shared_blks_dirtied bigint, shared_blks_written bigint,
                    temp_blks_read bigint, temp_blks_written bigint, blk_read_time double precision, blk_write_time double precision);
                INSERT INTO {Scratch}.pg_stat_statements VALUES
                    (101, 1, 10, 5, 50, 1, 20, 10, 5, 100, 10, 1, 1, 0, 0, 0, 0),
                    (102, 1, 10, 9, 90, 2, 30, 10, 9, 200, 20, 2, 2, 0, 0, 0, 0);
                """, ct);

            var first = Context(serverId, serverName, Anchor, major, statementsVersion: extensionVersion);
            var rows = await RunAsync(PgStatementStatsCollector.Instance, first, source, store, ct);
            Assert.Equal(2, rows.Count);
            Assert.All(rows, row =>
            {
                Assert.Null(row.WalRecords);
                Assert.Null(row.WalFpi);
                Assert.Null(row.WalBytes);
            });

            await ExecAsync(source, $"UPDATE {Scratch}.pg_stat_statements SET calls = calls + 3, total_time = total_time + 30, rows = rows + 3", ct);
            var second = Context(serverId, serverName, Anchor.AddMinutes(1), major, statementsVersion: extensionVersion);
            Assert.Equal(2, (await RunAsync(PgStatementStatsCollector.Instance, second, source, store, ct)).Count);

            Assert.Equal(4L, await CountAsync(store, "SELECT count(*) FROM collect.pg_statement_stats WHERE server_id = $1", serverId, ct));
            Assert.Equal(4L, await CountAsync(store,
                "SELECT count(*) FROM collect.pg_statement_stats WHERE server_id = $1 AND wal_records IS NULL AND wal_fpi IS NULL AND wal_bytes IS NULL AND calls > 0", serverId, ct));

            var builder = new NpgsqlConnectionStringBuilder(connectionString!) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
            var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, serverId, Anchor.AddHours(-1), Anchor.AddHours(1), 10, ct);
            Assert.Equal(2, page.Rows.Count);
            Assert.All(page.Rows, row => Assert.Null(row.WalBytes));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) => CleanupAsync(cleanup, serverId, cleanupCt));
        }
    }

    [Theory]
    [InlineData(10)]
    [InlineData(12)]
    [InlineData(13)]
    public async Task ReplicationSlotsOnAnOldMajor_ReadAndStoreTheNullProjections(int major)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        await using var store = await OpenAsync(connectionString!, migrate: true, ct);
        await using var source = await OpenAsync(connectionString!, migrate: false, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(source, $"SELECT pg_catalog.pg_create_physical_replication_slot('{Slot}')", ct);

            var rows = await RunAsync(PgReplicationSlotsCollector.Instance, Context(serverId, serverName, Anchor, major), source, store, ct);

            var slot = Assert.Single(rows, r => r.SlotName == Slot);
            Assert.Null(slot.WalStatus);
            Assert.Equal(-1, slot.SafeWalSizeBytes);
            Assert.False(slot.TwoPhase);
            Assert.Equal(1L, await CountAsync(store,
                $"SELECT count(*) FROM collect.pg_replication_slot_stats WHERE server_id = $1 AND slot_name = '{Slot}' AND wal_status IS NULL", serverId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) => CleanupAsync(cleanup, serverId, cleanupCt));
        }
    }

    [Theory]
    [InlineData(10)]
    [InlineData(12)]
    [InlineData(13)]
    public async Task SessionStatesOnAnOldMajor_ReadAndStoreTheNullProjections(int major)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        await using var store = await OpenAsync(connectionString!, migrate: true, ct);
        await using var source = await OpenAsync(connectionString!, migrate: false, ct);
        var bodySucceeded = false;
        try
        {
            /* The query leaves out its own backend and stores only the sessions that hold the xmin horizon or sit idle in a
               transaction, so a second session holds a transaction with an xid open. */
            await using var other = await OpenAsync(connectionString!, migrate: false, ct);
            await ExecAsync(other, "BEGIN; SELECT pg_catalog.txid_current()", ct);
            var rows = await RunAsync(PgSessionStatesCollector.Instance, Context(serverId, serverName, Anchor, major), source, store, ct);

            Assert.NotEmpty(rows);
            Assert.Equal(rows.Count, await CountAsync(store, "SELECT count(*) FROM collect.pg_session_states WHERE server_id = $1", serverId, ct));
            Assert.All(rows, row => Assert.Null(row.QueryId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) => CleanupAsync(cleanup, serverId, cleanupCt));
        }
    }

    [Fact]
    public async Task WriteStatsWithoutTheWalView_ReadAndStoreNullWalColumns()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        var (serverId, serverName) = NewServer();
        await using var store = await OpenAsync(connectionString!, migrate: true, ct);
        await using var source = await OpenAsync(connectionString!, migrate: false, ct);
        var bodySucceeded = false;
        try
        {
            /* The Aurora flavor sends the NULL WAL projections a pre-14 server does, on a major this server has. */
            var rows = await RunAsync(PgWriteStatsCollector.Instance, Context(serverId, serverName, Anchor, 18, aurora: true), source, store, ct);

            var row = Assert.Single(rows);
            Assert.Null(row.WalRecords);
            Assert.Null(row.WalFpi);
            Assert.Null(row.WalBytes);
            Assert.Equal(1L, await CountAsync(store,
                "SELECT count(*) FROM collect.pg_write_stats WHERE server_id = $1 AND wal_records IS NULL AND wal_fpi IS NULL AND wal_bytes IS NULL", serverId, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) => CleanupAsync(cleanup, serverId, cleanupCt));
        }
    }
}

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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9164: the hourly host rollup's migration and the tick that fills it. The migration applied twice leaves the same schema. One tick on a
/// seeded store builds the rows of a closed hour and builds nothing for an hour that closed less than the grace ago.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStatsHostHourlyLiveTests
{
    private const int ServerId = -9164287;
    private const int RetentionDays = 30;

    /// <summary>The tick's clock. The 11:00 hour closed at 12:00, five minutes before it, so it is inside the ten-minute grace.</summary>
    private static readonly DateTime TickNow = new(2026, 9, 15, 12, 5, 0, DateTimeKind.Unspecified);

    private static readonly DateTime ClosedHour = new(2026, 9, 15, 9, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime GraceHour = new(2026, 9, 15, 11, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The migration, run twice over a store that has no trace of it, leaves the same tables, columns and indexes, and no rows.</summary>
    [Fact]
    public async Task TheMigration_AppliedTwice_LeavesTheSameSchema()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        var rung = PgMigrations.Scripts[^1];
        Assert.Equal(QueryStatsHostHourly.Version, rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, rung.Version);

        await ExecuteAsync(connection, "DROP TABLE IF EXISTS collect.query_stats_host_hourly; DROP TABLE IF EXISTS collect.query_stats_host_hourly_built;", ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        var once = await SchemaAsync(connection, ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        var twice = await SchemaAsync(connection, ct);

        Assert.NotEmpty(once);
        Assert.Equal(once, twice);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly", ct));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built", ct));

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));
    }

    /// <summary>
    /// A tick on a seeded store builds the closed hour (two keys, one of them from two raw rows), records it as built, and leaves the hour
    /// that closed five minutes ago alone: no rollup row and no built row for it.
    /// </summary>
    [Fact]
    public async Task OneTick_BuildsAClosedHour_AndNothingForAnHourInsideTheGrace()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9164 host hourly live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecuteAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (@id, 'hourly-9164', TRUE) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct,
            ("id", ServerId));
        await InsertRawAsync(connection, 1, ClosedHour.AddMinutes(10), "DbA", "0xH1", ct);
        await InsertRawAsync(connection, 2, ClosedHour.AddMinutes(40), "DbA", "0xH1", ct);
        await InsertRawAsync(connection, 3, ClosedHour.AddMinutes(20), "DbA", "0xH2", ct);
        await InsertRawAsync(connection, 4, GraceHour.AddMinutes(30), "DbA", "0xH1", ct);
        await QueryStatsLedgerSeed.SeedAsync(connection, ClosedHour.AddHours(-1), GraceHour.AddHours(2), ct);

        var result = await QueryStatsHostHourly.RunTickAsync(postgres, TickNow, RetentionDays, NullLogger.Instance, ct);

        Assert.Equal(0, result.Failed);
        Assert.True(result.HoursBuilt > 0);

        var closed = new Dictionary<string, long>();
        await using (var read = new NpgsqlCommand("SELECT query_hash, raw_rows FROM collect.query_stats_host_hourly WHERE server_id = @id AND bucket = @bucket", connection))
        {
            read.Parameters.AddWithValue("id", ServerId);
            read.Parameters.AddWithValue("bucket", ClosedHour);
            await using var reader = await read.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                closed[reader.GetString(0)] = reader.GetInt64(1);
            }
        }

        Assert.Equal(new Dictionary<string, long> { ["0xH1"] = 2, ["0xH2"] = 1 }, closed);
        Assert.Equal(3L, await ScalarAsync<long>(connection, "SELECT raw_rows FROM collect.query_stats_host_hourly_built WHERE server_id = @id AND bucket = @bucket", ct,
            ("id", ServerId), ("bucket", ClosedHour)));

        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly WHERE server_id = @id AND bucket = @bucket", ct,
            ("id", ServerId), ("bucket", GraceHour)));
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.query_stats_host_hourly_built WHERE server_id = @id AND bucket = @bucket", ct,
            ("id", ServerId), ("bucket", GraceHour)));
    }

    private static async Task InsertRawAsync(NpgsqlConnection connection, long collectionId, DateTime at, string database, string hash, CancellationToken ct) =>
        await ExecuteAsync(connection, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (@collection_id, @at, @server_id, 'hourly-9164', @database, @hash, @hash, 100, 100, 1, 300)", ct,
            ("collection_id", collectionId), ("at", at), ("server_id", ServerId), ("database", database), ("hash", hash));

    /// <summary>The two tables' columns and indexes as sorted text, so a second run that changed either shows as a different list.</summary>
    private static async Task<List<string>> SchemaAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var lines = new List<string>();
        await using var command = new NpgsqlCommand(@"
SELECT table_name || '.' || column_name || ' ' || data_type || ' ' || is_nullable
FROM information_schema.columns
WHERE table_schema = 'collect' AND table_name IN ('query_stats_host_hourly', 'query_stats_host_hourly_built')
UNION ALL
SELECT tablename || ' ' || indexdef
FROM pg_indexes
WHERE schemaname = 'collect' AND tablename IN ('query_stats_host_hourly', 'query_stats_host_hourly_built')
ORDER BY 1", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(reader.GetString(0));
        }

        return lines;
    }

    private static async Task<int> ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}

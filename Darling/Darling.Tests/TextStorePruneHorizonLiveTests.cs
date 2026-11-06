/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The two statement-text stores are pruned on the horizon of the collectors whose fact rows point at them, not only on the
/// horizon of the two dimension-feeding SQL Server collectors. A per-collector retention raised above the base window on a
/// PostgreSQL statement collector, or on a Query Store collector, keeps the text its facts resolve to for as long as those
/// facts live. Gated on DARLING_TEST_PG: runs the real purge against a scratch store, through the same resolver seam the
/// worker hands it.
/// </summary>
[Collection("live-postgres")]
public sealed class TextStorePruneHorizonLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Distinctive fake id: a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -737373;

    /// <summary>Older than the base window plus either text store's margin, younger than the raised retention below.</summary>
    private const int RowAgeDays = 45;

    private const int RaisedRetentionDays = 90;

    [Fact]
    public async Task PgStatementText_SurvivesWhileAPgStatementCollectorsRaisedRetentionKeepsItsFacts()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the text-store prune horizon live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var lastSeen = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddDays(-RowAgeDays);
        await using (var seed = new NpgsqlCommand(
            "INSERT INTO collect.pg_statement_text (server_id, queryid, query_text, first_seen, last_seen) VALUES ($1, 1, 'SELECT 1', $2, $2)",
            connection))
        {
            seed.Parameters.AddWithValue(TestServerId);
            seed.Parameters.AddWithValue(lastSeen);
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, new CapturingTestLogger(), ct,
            retentionDaysFor: name => RetentionFor(name, raised: "pg_wait_sampling"));

        Assert.Equal(1L, await CountAsync(connection, "collect.pg_statement_text", ct));
    }

    [Fact]
    public async Task QueryStoreText_SurvivesWhileAQueryStoreCollectorsRaisedRetentionKeepsItsFacts()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the text-store prune horizon live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var lastSeen = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddDays(-RowAgeDays);
        await using (var seed = new NpgsqlCommand(
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, 'db', 1, 'SELECT 1', $2)",
            connection))
        {
            seed.Parameters.AddWithValue(TestServerId);
            seed.Parameters.AddWithValue(lastSeen);
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, new CapturingTestLogger(), ct,
            retentionDaysFor: name => RetentionFor(name, raised: "query_store_hints"));

        Assert.Equal(1L, await CountAsync(connection, "collect.query_store_text", ct));
    }

    /// <summary>The schedule's own retention for every collector but one, which carries the raised override.</summary>
    private static int RetentionFor(string collectorName, string raised) =>
        string.Equals(collectorName, raised, StringComparison.Ordinal)
            ? RaisedRetentionDays
            : CollectorScheduleDefaults.All.TryGetValue(collectorName, out var schedule)
                ? schedule.RetentionDays
                : DarlingRetentionHorizons.DataRetentionBaseDays;

    private static async Task<long> CountAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT COUNT(*) FROM " + table + " WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}

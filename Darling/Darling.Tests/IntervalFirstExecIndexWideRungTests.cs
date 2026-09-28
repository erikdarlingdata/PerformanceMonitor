/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V154 (#4608, split #4615): a plain btree on <c>first_execution_time</c> for
/// <c>query_store_interval_wide</c> (V145) \u2014 V153's (<c>IntervalFirstExecIndexRungTests</c>) twin for the
/// per-interval Query Store table this rung's SQL leaves alone. Split into its own rung so each index
/// build gets its own <c>MigrationCommandTimeoutSeconds</c> window rather than sharing one between the
/// two tables. This file is the RUNG (ladder, viewer probe) and the live schema-after-migrate proof: the
/// index exists on a fresh migrate, and the read gate's floor query uses it.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class IntervalFirstExecIndexWideRungTests
{
    private const int RungVersion = 154;
    private const int PreviousVersion = 153;

    /// <summary>This rung's sentinel ordinal in the viewer probe \u2014 the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 129;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The rung is registered and is the new top of the ladder \u2014 the claim this class takes over from
    /// <c>IntervalFirstExecIndexRungTests</c> (V153) now that V154 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("interval-tables-wide-first-exec-index", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as the TOP arm: a missing top arm
    /// maps a fully-migrated store one rung short, permanently, because
    /// <see cref="ViewerDataService.RequiredStoreSchemaVersion"/> is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("idx_query_store_interval_wide_first_exec", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasIntervalWideFirstExecIndex", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasIntervalWideFirstExecIndex)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasIntervalFirstExecIndexes)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V154 sentinel arm \u2014 a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "the V154 arm sits below V153's, so a current store maps one rung short");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>
    /// The LIVE schema after migrate: the _wide index exists. Run against a pre-V154 build this is RED
    /// \u2014 the index does not exist.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_TheWideFirstExecIndexExists()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V154 schema pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            Assert.True(await IndexExistsAsync(connection, "idx_query_store_interval_wide_first_exec", ct),
                "V154 adds idx_query_store_interval_wide_first_exec");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The read gate's per-server floor (<see cref="QueryStoreIntervalWide.PlainTableFloorSql"/>) gets an
    /// efficient plan off this rung's index \u2014 no Seq Scan.
    /// </summary>
    [Fact]
    public async Task TheReadGatesFloorQuery_UsesAnIndex_NoSeqScan()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V154 read-gate-plan pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await SeedIntervalWideAsync(connection, ct, rowCount: 2000);
            await ExecAsync(connection, ct, "VACUUM ANALYZE collect.query_store_interval_wide");

            var plan = await ExplainParamAsync(connection, ct, QueryStoreIntervalWide.PlainTableFloorSql, 1);

            Assert.DoesNotContain("Seq Scan", plan, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// Rerunning the migration ladder (as startup does on an already-migrated store) is idempotent: this
    /// rung's <c>CREATE INDEX IF NOT EXISTS</c> statement does not error and the index still exists.
    /// </summary>
    [Fact]
    public async Task MigrateAsync_RunTwice_IsIdempotent()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V154 idempotency pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await PgMigrations.MigrateAsync(connection, ct);

            Assert.True(await IndexExistsAsync(connection, "idx_query_store_interval_wide_first_exec", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task SeedIntervalWideAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, int rowCount)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count, replica_role, runtime_stats_interval_id)
SELECT
    TIMESTAMP '2026-01-01 00:00:00' + (g % 5) * INTERVAL '1 days' + (g % 1440) * INTERVAL '1 minutes' + INTERVAL '1 minutes',
    (g % 10) + 1, 'db' || (g % 3), g, g % 1000, 'exec',
    TIMESTAMP '2026-01-01 00:00:00' + (g % 5) * INTERVAL '1 days' + (g % 1440) * INTERVAL '1 minutes',
    TIMESTAMP '2026-01-01 00:00:00' + (g % 5) * INTERVAL '1 days' + (g % 1440) * INTERVAL '1 minutes' + INTERVAL '1 minutes',
    'mod', 'select 1', 'qh', 1, NULL, g
FROM generate_series(1, $1) AS g;", connection);
        command.Parameters.AddWithValue(rowCount);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainParamAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql, int serverId)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + sql, connection);
        command.Parameters.AddWithValue(serverId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var lines = new System.Text.StringBuilder();
        while (await reader.ReadAsync(ct))
        {
            lines.AppendLine(reader.GetString(0));
        }

        return lines.ToString();
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)", connection);
        command.Parameters.AddWithValue(indexName);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}

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
/// Pins Darling rung V153 (#4608): a plain btree on <c>first_execution_time</c> for each per-interval
/// Query Store table (<c>query_store_interval_latest</c>, V143, and <c>query_store_interval_wide</c>,
/// V145) — the column both the daily retention sweep's
/// <see cref="DarlingRetention.TimeSlicedDeleteSql"/> filters on and the read gate's per-server floor
/// reads. This file is the RUNG (ladder, viewer probe) and the live schema-after-migrate proof: both
/// indexes exist on a fresh migrate, the purge's plan uses one of them (no full scan), and the read
/// gate's floor query uses one too.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class IntervalFirstExecIndexRungTests
{
    private const int RungVersion = 153;
    private const int PreviousVersion = 152;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 128;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The rung is registered and is the new top of the ladder — the claim this class takes over from
    /// <c>CaggGroupIndexDropRungTests</c> (V152) now that V153 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("interval-tables-first-exec-index", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
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
        Assert.Contains("idx_query_store_interval_latest_first_exec", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasIntervalFirstExecIndexes", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasIntervalFirstExecIndexes)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasCaggGroupIndexDrop)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V153 sentinel arm — a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "the V153 arm sits below V152's, so a current store maps one rung short");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>
    /// The LIVE schema after migrate: both indexes exist on the two per-interval tables. Run against a
    /// pre-V153 build this is RED — neither index exists.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_BothFirstExecIndexesExist()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V153 schema pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            Assert.True(await IndexExistsAsync(connection, "idx_query_store_interval_latest_first_exec", ct),
                "V153 adds idx_query_store_interval_latest_first_exec");
            Assert.True(await IndexExistsAsync(connection, "idx_query_store_interval_wide_first_exec", ct),
                "V153 adds idx_query_store_interval_wide_first_exec");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The purge's plan (<see cref="DarlingRetention.TimeSlicedDeleteSql"/>) against the migrated schema
    /// uses the new index for its outer scan and both <c>min()</c> subqueries — no Seq Scan (or the
    /// equivalent full-index walk a leading-column-less plan would take) on the table. Seeded with rows
    /// spanning two days so both the "nothing to delete" and the min()-subquery shapes are exercised.
    /// </summary>
    [Fact]
    public async Task ThePurgesPlan_UsesTheIndex_NoSeqScan()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V153 purge-plan pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await SeedIntervalLatestAsync(connection, ct, rowCount: 2000);
            await ExecAsync(connection, ct, "VACUUM ANALYZE collect.query_store_interval_latest");

            var sql = DarlingRetention.TimeSlicedDeleteSql("collect.query_store_interval_latest", "first_execution_time");
            var plan = await ExplainAsync(connection, ct, sql, new DateTime(2026, 1, 3, 0, 0, 0, DateTimeKind.Unspecified));

            Assert.DoesNotContain("Seq Scan", plan, StringComparison.Ordinal);
            Assert.Contains("idx_query_store_interval_latest_first_exec", plan, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The read gate's per-server floor (<see cref="QueryStoreIntervalLatest.PlainTableFloorSql"/>) also
    /// gets an efficient plan off this rung's index — no Seq Scan.
    /// </summary>
    [Fact]
    public async Task TheReadGatesFloorQuery_UsesAnIndex_NoSeqScan()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V153 read-gate-plan pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await SeedIntervalLatestAsync(connection, ct, rowCount: 2000);
            await ExecAsync(connection, ct, "VACUUM ANALYZE collect.query_store_interval_latest");

            var plan = await ExplainParamAsync(connection, ct, QueryStoreIntervalLatest.PlainTableFloorSql, 1);

            Assert.DoesNotContain("Seq Scan", plan, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// Rerunning the migration ladder (as startup does on an already-migrated store) is idempotent: the
    /// two <c>CREATE INDEX IF NOT EXISTS</c> statements do not error and both indexes still exist.
    /// </summary>
    [Fact]
    public async Task MigrateAsync_RunTwice_IsIdempotent()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V153 idempotency pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await PgMigrations.MigrateAsync(connection, ct);

            Assert.True(await IndexExistsAsync(connection, "idx_query_store_interval_latest_first_exec", ct));
            Assert.True(await IndexExistsAsync(connection, "idx_query_store_interval_wide_first_exec", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task SeedIntervalLatestAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, int rowCount)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_latest
(server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count, query_text)
SELECT
    (g % 10) + 1, 'db' || (g % 3), g, g % 1000, NULL, g,
    TIMESTAMP '2026-01-01 00:00:00' + (g % 5) * INTERVAL '1 days' + (g % 1440) * INTERVAL '1 minutes',
    TIMESTAMP '2026-01-01 00:00:00' + (g % 5) * INTERVAL '1 days' + (g % 1440) * INTERVAL '1 minutes' + INTERVAL '1 minutes',
    'ph', 'qh', 1, 1, 1,
    TIMESTAMP '2026-01-01 00:00:00' + (g % 5) * INTERVAL '1 days' + (g % 1440) * INTERVAL '1 minutes' + INTERVAL '1 minutes',
    false, 0, 'select 1'
FROM generate_series(1, $1) AS g;", connection);
        command.Parameters.AddWithValue(rowCount);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql, DateTime cutoff)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + sql, connection);
        command.Parameters.AddWithValue(cutoff);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var lines = new System.Text.StringBuilder();
        while (await reader.ReadAsync(ct))
        {
            lines.AppendLine(reader.GetString(0));
        }

        return lines.ToString();
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

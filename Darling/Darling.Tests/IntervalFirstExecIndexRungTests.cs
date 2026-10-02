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
/// Pins Darling rung V153 (#4608, split #4615): a plain btree on <c>first_execution_time</c> for
/// <c>query_store_interval_latest</c> (V143) only — the column both the daily retention sweep's
/// <see cref="DarlingRetention.TimeSlicedDeleteSql"/> filters on and the read gate's per-server floor
/// reads. V154 (<c>IntervalFirstExecIndexWideRungTests</c>) is the twin rung for
/// <c>query_store_interval_wide</c>'s index, split into its own rung so each index build gets its own
/// migration-command-timeout window. This file's "I am the top rung" claim moved to that class now that
/// V154 has landed; this file's own rung/probe facts below keep asserting what stays true forever
/// (present, in-order, gated behind the arm above it) rather than "is exactly the top". This file is the
/// RUNG (ladder, viewer probe) and the live schema-after-migrate proof for the <c>_latest</c> index: it
/// exists on a fresh migrate, the purge's plan uses it (no full scan), and the read gate's floor query
/// uses it too.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class IntervalFirstExecIndexRungTests
{
    private const int RungVersion = 153;
    private const int PreviousVersion = 152;

    /// <summary>This rung's sentinel ordinal in the viewer probe — no longer the newest, since V154
    /// landed above it.</summary>
    private const int ProbeOrdinal = 128;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The rung is registered, and the ladder stays dense above it — the claim this class took over from
    /// <c>CaggGroupIndexDropRungTests</c> (V152) moved on again to <c>IntervalFirstExecIndexWideRungTests</c>
    /// (V154) now that V154 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegistered_AndTheLadderIsDenseAboveIt()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("interval-tables-first-exec-index", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as an arm gated below the
    /// current top's arm — a missing arm maps a fully-migrated store one rung short, permanently, because
    /// <see cref="ViewerDataService.RequiredStoreSchemaVersion"/> is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndTheArmSitsBelowTheCurrentTop()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("idx_query_store_interval_latest_first_exec", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal("hasIntervalFirstExecIndexes", method.GetParameters()[ProbeOrdinal].Name);

        /* Every rung above this one (V154's hasIntervalWideFirstExecIndex) must also be false, or the map
           finds the newer arm first and this assertion is checking the wrong rung's fallthrough. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        var behind = (object[])all.Clone();
        for (var i = ProbeOrdinal; i < arity; i++)
        {
            behind[i] = false;
        }
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* V154 (#4608, split #4615) is now the top rung, so this arm no longer needs to be the LAST one —
           it only has to sit below the current top's arm, which is what the ladder-dense invariant above
           already guarantees is registered ahead of it. */
        var thisArm = viewer.IndexOf("if (hasIntervalFirstExecIndexes)", StringComparison.Ordinal);
        var topArm = viewer.IndexOf("if (hasIntervalWideFirstExecIndex)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasCaggGroupIndexDrop)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V153 sentinel arm — a fully-migrated store would map one rung short");
        Assert.True(topArm >= 0 && topArm < thisArm, "the current top rung's arm must sit above the V153 arm");
        Assert.True(thisArm < previousArm, "the V153 arm sits below V152's, so a current store maps one rung short");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>
    /// The LIVE schema after migrate: the _latest index exists. V154's twin
    /// (<c>IntervalFirstExecIndexWideRungTests</c>) covers <c>_wide</c>'s index in its own rung's file.
    /// Run against a pre-V153 build this is RED — the index does not exist.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_TheLatestFirstExecIndexExists()
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
    /// Rerunning the migration ladder (as startup does on an already-migrated store) is idempotent: this
    /// rung's <c>CREATE INDEX IF NOT EXISTS</c> statement does not error and the index still exists.
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

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V149 (#4250): the Query Store liveness touch becomes a HOT update on
/// <c>collect.query_store_plan_map</c> and <c>collect.query_store_text</c> — the two <c>last_seen</c> btree
/// indexes are dropped and both tables get <c>fillfactor = 90</c>. This file is the RUNG (ladder, viewer
/// probe) and the HOT-eligibility proof: the schema after migrate carries neither index and the fillfactor
/// reloption, startup's <c>CREATE TABLE IF NOT EXISTS</c> convergence does not recreate the index, and two
/// touches of the same row report a HOT update via <c>pg_stat_user_tables.n_tup_hot_upd</c>.
///
/// <para>This file's "I am the top rung" claim takes over from <c>ReadLatencyFlushLiveTests</c> (V148) now
/// that V149 has landed.</para>
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class QueryStoreLivenessHotTouchLiveTests
{
    private const int RungVersion = 149;
    private const int PreviousVersion = 148;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 124;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The rung is registered and is the new top of the ladder — the claim this class takes over from
    /// <c>ReadLatencyFlushLiveTests</c> (V148) now that V149 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("query-store-liveness-hot-touch", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
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
        Assert.Contains("idx_query_store_plan_map_last_seen", probe, StringComparison.Ordinal);
        Assert.Contains("fillfactor=90", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasHotLivenessTouch", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasHotLivenessTouch)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasReadLatency)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V149 sentinel arm — a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "the V149 arm sits below V148's, so a current store maps one rung short");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>
    /// The LIVE schema after migrate: neither <c>last_seen</c> index exists on either table, and both carry
    /// the <c>fillfactor=90</c> reloption. Run against <c>origin/dev</c> (pre-V149) this is RED — the indexes
    /// exist and the reloption is absent — proving the pin actually checks the rung rather than a tautology.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_NeitherLastSeenIndexExists_AndBothTablesCarryFillfactor90()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V149 schema pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.False(await IndexExistsAsync(connection, "idx_query_store_plan_map_last_seen", ct),
            "V149 drops idx_query_store_plan_map_last_seen");
        Assert.False(await IndexExistsAsync(connection, "idx_query_store_text_last_seen", ct),
            "V149 drops idx_query_store_text_last_seen");

        Assert.True(await HasFillfactor90Async(connection, "collect.query_store_plan_map", ct),
            "V149 sets fillfactor = 90 on query_store_plan_map");
        Assert.True(await HasFillfactor90Async(connection, "collect.query_store_text", ct),
            "V149 sets fillfactor = 90 on query_store_text");
    }

    /// <summary>
    /// Startup convergence (<see cref="QueryStorePlanMap.CreateTableSql"/>, <see cref="QueryStoreTextStore.CreateTableSql"/>)
    /// no longer recreates either index: run the table-ensure path a second time after migrate, on top of an
    /// already-migrated store, and the index must still be absent. A real mutation (re-adding the
    /// <c>CREATE INDEX IF NOT EXISTS ... last_seen</c> line back to either helper's DDL) turns this RED —
    /// see the PR body for the exact revert-and-rerun.
    /// </summary>
    [Fact]
    public async Task StartupConvergence_RunTwice_DoesNotRecreateEitherIndex()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V149 convergence pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        for (var i = 0; i < 2; i++)
        {
            await using var mapCommand = new NpgsqlCommand(QueryStorePlanMap.CreateTableSql, connection);
            await mapCommand.ExecuteNonQueryAsync(ct);

            await using var textCommand = new NpgsqlCommand(QueryStoreTextStore.CreateTableSql, connection);
            await textCommand.ExecuteNonQueryAsync(ct);
        }

        Assert.False(await IndexExistsAsync(connection, "idx_query_store_plan_map_last_seen", ct),
            "convergence must not recreate idx_query_store_plan_map_last_seen");
        Assert.False(await IndexExistsAsync(connection, "idx_query_store_text_last_seen", ct),
            "convergence must not recreate idx_query_store_text_last_seen");
    }

    /// <summary>
    /// The payoff: two touches of the same map row after V149 report a HOT update. The first touch (row
    /// starts fresh, no prior version) may or may not be HOT depending on page layout; the SECOND touch of
    /// the same row — after the guard interval has re-elapsed — is the one this asserts, because it is the
    /// steady-state case the field actually runs (every guard cycle touches rows that were touched last
    /// cycle too). Uses <c>pg_stat_force_next_flush()</c> plus a short settle (as
    /// <c>QueryStoreIntervalWideGridLiveTests</c> and <c>StoreToastAndCheckpointerTests</c> do) because
    /// <c>pg_stat_user_tables</c> counters are backend-pending and throttled to once a second.
    /// </summary>
    [Fact]
    public async Task TwoTouchesOfTheSameRow_TheSecondTouchReportsAHotUpdate()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V149 HOT pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var seed = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) " +
            "VALUES (1, 'db', 1, '\\x01', 'h1', now() AT TIME ZONE 'UTC' - interval '1 day')", connection);
        await seed.ExecuteNonQueryAsync(ct);

        var touch = "UPDATE collect.query_store_plan_map SET last_seen = now() AT TIME ZONE 'UTC' " +
                    "WHERE server_id = 1 AND database_name = 'db' AND plan_id = 1";

        // First touch: may build a fresh heap version, not the steady-state case.
        await using (var first = new NpgsqlCommand(touch, connection))
        {
            await first.ExecuteNonQueryAsync(ct);
        }

        await using (var flush1 = new NpgsqlCommand("SELECT pg_stat_force_next_flush()", connection))
        {
            await flush1.ExecuteScalarAsync(ct);
        }

        var before = await ReadHotUpdatesAsync(connection, ct);

        // Second touch: the steady-state case — the row already has a settled heap version with room.
        await using (var second = new NpgsqlCommand(touch, connection))
        {
            await second.ExecuteNonQueryAsync(ct);
        }

        await using (var flush2 = new NpgsqlCommand("SELECT pg_stat_force_next_flush()", connection))
        {
            await flush2.ExecuteScalarAsync(ct);
        }

        var after = await ReadHotUpdatesAsync(connection, ct);

        Assert.True(after > before,
            $"the second touch of an already-settled row must be HOT (n_tup_hot_upd {before} -> {after})");
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1)", connection);
        command.Parameters.AddWithValue(indexName);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<bool> HasFillfactor90Async(NpgsqlConnection connection, string tableName, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT coalesce((SELECT c.reloptions FROM pg_class c WHERE c.oid = $1::regclass) @> ARRAY['fillfactor=90'], false)", connection);
        command.Parameters.AddWithValue(tableName);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> ReadHotUpdatesAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT n_tup_hot_upd FROM pg_stat_user_tables WHERE schemaname = 'collect' AND relname = 'query_store_plan_map'", connection);
        var scalar = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(scalar, System.Globalization.CultureInfo.InvariantCulture);
    }
}

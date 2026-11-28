/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// V322 (#9863) on a real store: the hourly host rollup keeps its unique index and loses <c>idx_query_stats_host_hourly_hash</c>, on a store
/// the ladder builds from empty and on a store that holds the index and rollup rows at V321, and the Viewer's probe does not read a store below V321 whose index was dropped by hand as V322.
/// A store already stamped V322 that holds the index again loses it at the next start, and a drop that cannot take its lock leaves the start whole. Each test is a behaviour of the migrated store.
/// </summary>
public sealed class HostHourlyHashIndexDropLiveTests
{
    private const string HashIndex = "collect.idx_query_stats_host_hourly_hash";
    private const string UniqueIndex = "collect.ux_query_stats_host_hourly";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AStoreTheLadderBuildsFromEmpty_HasNoHashIndex_KeepsTheUniqueIndex_AndTheProbeNamesTheNewestRung()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V322 live tests (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);

            Assert.False(await IndexExistsAsync(connection, HashIndex, ct), "the hash index is created at V287 and dropped at V322");
            Assert.True(await IndexExistsAsync(connection, UniqueIndex, ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT max(version) FROM darling_schema_version", ct), System.Globalization.CultureInfo.InvariantCulture));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreAtV321_HoldingTheIndexAndRollupRows_LosesTheIndexAtV322_AndEveryRowStays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V322 live tests (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);

            /* A store one rung behind: the index is back, the version rows stop at V321, and the rollup holds rows with and without a hash. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, HostHourlyHashIndexDrop.Version - 1, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + HostHourlyHashIndexDrop.Version, ct);
            await ExecAsync(connection,
                "INSERT INTO collect.query_stats_host_hourly (server_id, bucket, database_name, query_hash, host_object_name, raw_rows, total_executions) VALUES "
                + "(1, '2026-11-01 10:00', 'Sales', '0xAA', 'dbo.Orders', 4, 40), "
                + "(1, '2026-11-01 10:00', 'Sales', '0xBB', NULL, 2, 7), "
                + "(1, '2026-11-01 11:00', 'Sales', '0xAA', 'dbo.Orders', 3, 30), "
                + "(2, '2026-11-01 10:00', NULL, NULL, NULL, 1, 1)", ct);

            Assert.True(await IndexExistsAsync(connection, HashIndex, ct), "the store at V321 holds the index");
            Assert.Equal(HostHourlyHashIndexDrop.Version - 1, await ProbedVersionAsync(connection, ct));
            var before = await ScalarAsync(connection, RollupFingerprintSql, ct);

            var applied = await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(1, applied);
            Assert.False(await IndexExistsAsync(connection, HashIndex, ct));
            Assert.True(await IndexExistsAsync(connection, UniqueIndex, ct));
            Assert.Equal(before, await ScalarAsync(connection, RollupFingerprintSql, ct));
            Assert.Equal(4L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_host_hourly", ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            /* The rung runs again on a store that already lacks the index. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + HostHourlyHashIndexDrop.Version, ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.False(await IndexExistsAsync(connection, HashIndex, ct));
            Assert.Equal(before, await ScalarAsync(connection, RollupFingerprintSql, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreBelowV321_WhoseHashIndexWasDroppedByHand_DoesNotReadAsV322()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V322 live tests (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);

            /* A store at V320, which has the rollup and its hash index, then the index dropped by hand: the index's absence alone does not make it a V322 store. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, HostHourlyHashIndexDrop.Version - 2, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + (HostHourlyHashIndexDrop.Version - 1), ct);
            Assert.True(await IndexExistsAsync(connection, HashIndex, ct), "the store at V320 holds the index");
            await ExecAsync(connection, "DROP INDEX " + HashIndex, ct);

            Assert.Equal(HostHourlyHashIndexDrop.Version - 2, await ProbedVersionAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreAtTheTopRung_WhoseHashIndexWasCreatedByHand_LosesItAtTheNextStart_AndTheProbeNamesTheNewestRung()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V322 live tests (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection,
                "INSERT INTO collect.query_stats_host_hourly (server_id, bucket, database_name, query_hash, host_object_name, raw_rows, total_executions) VALUES "
                + "(1, '2026-11-01 10:00', 'Sales', '0xAA', 'dbo.Orders', 4, 40), "
                + "(1, '2026-11-01 10:00', 'Sales', '0xBB', NULL, 2, 7)", ct);
            var before = await ScalarAsync(connection, RollupFingerprintSql, ct);

            /* The store is stamped at the top rung and an operator puts the index back (the admin.md restore, or by hand), so the rung does not run again and the probe reads the store one version behind. */
            await ExecAsync(connection, "CREATE INDEX idx_query_stats_host_hourly_hash ON collect.query_stats_host_hourly (server_id, query_hash, bucket)", ct);
            Assert.True(await IndexExistsAsync(connection, HashIndex, ct));
            Assert.Equal(HostHourlyHashIndexDrop.Version - 1, await ProbedVersionAsync(connection, ct));

            var log = new CapturingTestLogger();
            var applied = await PgMigrations.MigrateAsync(connection, log, ct);

            Assert.Equal(0, applied);
            Assert.False(await IndexExistsAsync(connection, HashIndex, ct), log.Joined);
            Assert.True(await IndexExistsAsync(connection, UniqueIndex, ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(before, await ScalarAsync(connection, RollupFingerprintSql, ct));
            var named = LinesNaming(log);
            Assert.Single(named);
            Assert.StartsWith("Information:", named[0], StringComparison.Ordinal);

            /* A start that finds the index absent changes nothing and logs nothing. */
            var quiet = new CapturingTestLogger();
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, quiet, ct));
            Assert.False(await IndexExistsAsync(connection, HashIndex, ct));
            Assert.Empty(LinesNaming(quiet));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ADropThatCannotTakeItsLock_WarnsAndLeavesTheStartWhole_AndTheNextStartDropsTheIndex()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V322 live tests (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "CREATE INDEX idx_query_stats_host_hourly_hash ON collect.query_stats_host_hourly (server_id, query_hash, bucket)", ct);

            /* A session holds ACCESS SHARE on the rollup (a long read), so the drop's ACCESS EXCLUSIVE queues behind it until the drop's own lock_timeout gives up. */
            await using (var hold = await holder.BeginTransactionAsync(ct))
            {
                await using (var lockRollup = new NpgsqlCommand("LOCK TABLE collect.query_stats_host_hourly IN ACCESS SHARE MODE", holder, hold))
                {
                    await lockRollup.ExecuteNonQueryAsync(ct);
                }

                var log = new CapturingTestLogger();
                var applied = await PgMigrations.MigrateAsync(connection, log, ct);

                Assert.Equal(0, applied);
                Assert.True(await IndexExistsAsync(connection, HashIndex, ct), "the drop could not take its lock, so the index stays");
                var named = LinesNaming(log);
                Assert.Single(named);
                Assert.StartsWith("Warning:", named[0], StringComparison.Ordinal);
                Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, "SELECT 1", ct), System.Globalization.CultureInfo.InvariantCulture));
                await hold.RollbackAsync(ct);
            }

            await PgMigrations.MigrateAsync(connection, ct);
            Assert.False(await IndexExistsAsync(connection, HashIndex, ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* Every row's key and counts, in a fixed order, as one string. */
    private const string RollupFingerprintSql =
        "SELECT COALESCE(string_agg(server_id || '|' || bucket || '|' || COALESCE(database_name, '-') || '|' || COALESCE(query_hash, '-') || '|' || "
        + "COALESCE(host_object_name, '-') || '|' || raw_rows || '|' || total_executions, ';' ORDER BY server_id, bucket, query_hash NULLS LAST), '') "
        + "FROM collect.query_stats_host_hourly";

    /* The log lines that name the index: the re-assert's own, apart from whatever else a start logs. */
    private static System.Collections.Generic.IReadOnlyList<string> LinesNaming(CapturingTestLogger log) =>
        log.Lines.Where(line => line.Contains(HostHourlyHashIndexDrop.IndexName, StringComparison.Ordinal)).ToList();

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string qualifiedName, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, $"SELECT to_regclass('{qualifiedName}') IS NOT NULL", ct))!;

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}

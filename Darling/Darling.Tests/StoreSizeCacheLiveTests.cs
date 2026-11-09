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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4477: the status bar's <c>pg_database_size(current_database())</c> walks every file in the database
/// directory, so its cost scales with the store rather than with the one number it returns — a production
/// session measured 9 calls of it in 4.5 minutes, once per refresh tick. <see cref="ViewerDataService.GetStoreSizeBytesAsync"/>
/// now caches the reading for <see cref="ViewerDataService.StoreSizeCacheLifetime"/>, so two refreshes
/// inside that window cost one store round trip.
///
/// <para>Its own scratch database (<c>#1776 own-store</c>).</para>
/// </summary>
public sealed class StoreSizeCacheLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The statement-count pin: two calls to <see cref="ViewerDataService.GetStoreSizeBytesAsync"/> inside
    /// the cache window produce exactly ONE <c>pg_database_size</c> call, counted through
    /// <c>pg_stat_statements</c> against the actual product call path.
    /// </summary>
    [Fact]
    public Task TwoRefreshesInsideTheWindow_ReadPgDatabaseSizeOnce()
        => TwoRefreshesAsync(forcePass: false);

    internal async Task TwoRefreshesAsync(bool forcePass)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live store-size cache test.");

        var ct = TestContext.Current.CancellationToken;

        await using (var probe = new NpgsqlConnection(baseConnectionString))
        {
            await probe.OpenAsync(ct);
            await using var preloadCmd = new NpgsqlCommand("SELECT current_setting('shared_preload_libraries')", probe);
            var preload = (string)(await preloadCmd.ExecuteScalarAsync(ct))!;
            var loaded = preload.Split(',').Select(s => s.Trim()).Contains("pg_stat_statements");
            Assert.SkipWhen(!loaded,
                $"pg_stat_statements is not in shared_preload_libraries ('{preload}') - this rig's postgresql.conf must carry it in shared_preload_libraries to run this pin.");
        }

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);

            using var extCmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", setup);
            await extCmd.ExecuteNonQueryAsync(ct);
        }

        long dbId;
        await using (var oidConn = new NpgsqlConnection(scratch.ConnectionString))
        {
            await oidConn.OpenAsync(ct);
            using var oidCmd = new NpgsqlCommand("SELECT oid FROM pg_database WHERE datname = current_database()", oidConn);
            dbId = (uint)(await oidCmd.ExecuteScalarAsync(ct))!;
        }

        await using var table = new NpgsqlConnection(scratch.ConnectionString);
        await table.OpenAsync(ct);

        var bodySucceeded = false;
        ViewerDataService? service = null;
        try
        {
            /* The whole window, from the counter's reset to the count, runs under PgStatementTable: the view is cluster-wide,
               and an eviction pass from another session between the refreshes and the count takes the one pg_database_size
               entry (run once, the least-used kind there is), which a run the pass landed in is voided and run again for. Each
               run starts a service of its own, since a reading is cached on the instance for StoreSizeCacheLifetime. */
            var run = await PgStatementTable.RunAsync(table, ct, async pass =>
            {
                if (service is not null)
                {
                    await service.DisposeAsync();
                }

                await ResetStatementCountAsync(table, dbId, ct);
                service = new ViewerDataService(scratch.ConnectionString);

                var first = await service.GetStoreSizeBytesAsync(ct);
                var second = await service.GetStoreSizeBytesAsync(ct);
                Assert.Equal(first, second);

                await pass.BeforeReadAsync(ct);

                long calls;
                await using (var countConn = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await countConn.OpenAsync(ct);
                    using var countCmd = new NpgsqlCommand(
                        "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = $1 AND query LIKE '%pg_database_size%current_database%'",
                        countConn);
                    countCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
                    calls = (long)(await countCmd.ExecuteScalarAsync(ct))!;
                }

                Assert.Equal(1, calls);
            }, forcePass);
            PgStatementTable.AssertNoPassInside(run);

            bodySucceeded = true;
        }
        finally
        {
            if (service is not null)
            {
                await service.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Clears the module's counters for this database, so the counts that follow are of this run's statements alone.</summary>
    private static async Task ResetStatementCountAsync(NpgsqlConnection connection, long dbId, System.Threading.CancellationToken ct)
    {
        await using var reset = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", connection);
        reset.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
        await reset.ExecuteNonQueryAsync(ct);
    }
}

/* pg-cluster-roles, not the parallel own-store shape: the fact fills pg_stat_statements until eviction passes have taken the
   body's own entries, on purpose, and in the parallel collections those passes take the other sessions' entries run once
   as well. The collection runs alone, after the parallel collections, so the fill takes nobody else's entries. The body
   is the one StoreSizeCacheLiveTests runs without the forced pass, and it mints its own scratch database. */

/// <summary>The forcing run of <see cref="StoreSizeCacheLiveTests"/>'s counting body: an eviction pass between the refreshes and the count, and the count is still of the call the refreshes made.</summary>
[Collection("pg-cluster-roles")]
public sealed class StoreSizeCacheForcedPassLiveTests
{
    [Fact]
    public Task TwoRefreshesInsideTheWindow_ReadPgDatabaseSizeOnce_WhenAnEvictionPassLandsBetweenItsStatementsAndItsReads_StillReadsTheEntriesItsStatementsMade()
        => new StoreSizeCacheLiveTests().TwoRefreshesAsync(forcePass: true);
}

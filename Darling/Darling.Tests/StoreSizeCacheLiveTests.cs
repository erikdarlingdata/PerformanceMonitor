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
    public async Task TwoRefreshesInsideTheWindow_ReadPgDatabaseSizeOnce()
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

            using var resetCmd = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", oidConn);
            resetCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
            await resetCmd.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        ViewerDataService? service = null;
        try
        {
            service = new ViewerDataService(scratch.ConnectionString);

            var first = await service.GetStoreSizeBytesAsync(ct);
            var second = await service.GetStoreSizeBytesAsync(ct);
            Assert.Equal(first, second);

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
}

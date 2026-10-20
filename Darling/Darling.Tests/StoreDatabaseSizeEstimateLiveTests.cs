/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6245: the hourly self-metrics sweep's whole-store row came from <c>pg_database_size(current_database())</c>, which opens
/// every file of the database (a TimescaleDB store has a file per chunk and index): 7.2 s on average and 77 s at worst on a dev
/// store. The hourly sweep now writes a catalog estimate (<c>relpages</c> times the block size) marked
/// <c>size_estimated = true</c>, and one sweep a day (the first without an exact row in the last
/// <see cref="StoreSelfMetrics.ExactSizeIntervalHours"/> hours, the same decision #5857 made for hypertables) writes the exact
/// figure. Each test mints its own scratch store (#1776 own-store).
/// </summary>
public sealed class StoreDatabaseSizeEstimateLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record StoreRow(long? TotalBytes, bool? SizeEstimated);

    private static async Task<StoreRow> ReadStoreRowAsync(NpgsqlConnection connection, DateTime metricTime, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT total_bytes, size_estimated FROM collect.store_metrics WHERE object_kind = $1 AND metric_time = $2",
            connection);
        command.Parameters.AddWithValue(StoreSelfMetrics.StoreObjectKind);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(metricTime, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"no whole-store row at {metricTime:o}");
        return new StoreRow(
            reader.IsDBNull(0) ? null : reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetBoolean(1));
    }

    private static async Task<long> CountPgDatabaseSizeCallsAsync(string connectionString, long dbId, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = $1 AND query LIKE '%pg_database_size%current_database%'",
            connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// Sweeps three times on a scratch store: the first (no exact row yet) is exact, a sweep an hour later is the estimate and
    /// makes no <c>pg_database_size</c> call (counted through <c>pg_stat_statements</c>), a sweep 24 hours after the first is
    /// exact again. The estimate is below the exact figure by a sane margin on a vacuumed store.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Sweep_HourlyWritesTheCatalogEstimateWithoutPgDatabaseSize_AndTheDailySweepIsExact(bool timescale)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live store database-size estimate test.");

        var ct = TestContext.Current.CancellationToken;

        await using (var probe = new NpgsqlConnection(baseConnectionString))
        {
            await probe.OpenAsync(ct);
            await using var preloadCmd = new NpgsqlCommand("SELECT current_setting('shared_preload_libraries')", probe);
            var preload = (string)(await preloadCmd.ExecuteScalarAsync(ct))!;
            Assert.SkipWhen(!preload.Split(',').Select(s => s.Trim()).Contains("pg_stat_statements"),
                $"pg_stat_statements is not in shared_preload_libraries ('{preload}') - this rig must preload it to count the statements.");
        }

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        long dbId;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            if (timescale)
            {
                Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
                    "the dev fixture is expected to have TimescaleDB installed");
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            }

            await using (var extCmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", connection))
            {
                await extCmd.ExecuteNonQueryAsync(ct);
            }

            /* A store the autovacuum daemon has visited: relpages is what the last vacuum or analyze recorded. */
            await using (var vacuum = new NpgsqlCommand("VACUUM (ANALYZE)", connection))
            {
                await vacuum.ExecuteNonQueryAsync(ct);
            }

            await using (var oidCmd = new NpgsqlCommand("SELECT oid FROM pg_database WHERE datname = current_database()", connection))
            {
                dbId = (uint)(await oidCmd.ExecuteScalarAsync(ct))!;
            }

            await using (var resetCmd = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", connection))
            {
                resetCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
                await resetCmd.ExecuteNonQueryAsync(ct);
            }

            var first = DateTime.UtcNow;
            await StoreSelfMetrics.SweepAsync(connection, timescale, first, null, null, null, ct);
            var firstRow = await ReadStoreRowAsync(connection, first, ct);
            Assert.False(firstRow.SizeEstimated, "the first sweep of a store has no exact row in the last 23 hours, so it is exact");
            Assert.True(firstRow.TotalBytes > 0);
            var callsAfterFirst = await CountPgDatabaseSizeCallsAsync(scratch.ConnectionString, dbId, ct);
            Assert.True(callsAfterFirst >= 1, "the exact sweep calls pg_database_size");

            /* An hour later: the estimate, and no pg_database_size call. */
            var hourly = first.AddHours(1);
            await StoreSelfMetrics.SweepAsync(connection, timescale, hourly, null, null, null, ct);
            var hourlyRow = await ReadStoreRowAsync(connection, hourly, ct);
            Assert.True(hourlyRow.SizeEstimated, "a sweep an hour after an exact one writes the catalog estimate");
            Assert.True(hourlyRow.TotalBytes > 0);
            Assert.Equal(callsAfterFirst, await CountPgDatabaseSizeCallsAsync(scratch.ConnectionString, dbId, ct));

            /* Sane range: the estimate lags the files a little (relpages is the last vacuum's count, and the free-space
               and visibility forks are not counted) but is the same order as the exact size. */
            var ratio = (double)hourlyRow.TotalBytes!.Value / firstRow.TotalBytes!.Value;
            TestContext.Current.SendDiagnosticMessage($"#6245 estimate {hourlyRow.TotalBytes} bytes vs exact {firstRow.TotalBytes} bytes (ratio {ratio:F3}, timescale {timescale})");
            Assert.InRange(ratio, 0.5, 1.1);

            /* The newest recorded figure says it is an estimate to the readers that show it. */
            await using (var stored = new NpgsqlCommand(StoreSelfMetrics.LatestStoreSizeWithTimeSql, connection))
            await using (var storedReader = await stored.ExecuteReaderAsync(ct))
            {
                Assert.True(await storedReader.ReadAsync(ct));
                Assert.Equal(hourlyRow.TotalBytes, storedReader.GetInt64(0));
                Assert.True(storedReader.GetBoolean(2));
            }

            /* 24 hours after the first: no exact row in the last 23 hours, so exact again, with one more call. */
            var daily = first.AddHours(24);
            await StoreSelfMetrics.SweepAsync(connection, timescale, daily, null, null, null, ct);
            var dailyRow = await ReadStoreRowAsync(connection, daily, ct);
            Assert.False(dailyRow.SizeEstimated, "the daily sweep writes the exact figure");
            Assert.True(dailyRow.TotalBytes > 0);
            Assert.True(await CountPgDatabaseSizeCallsAsync(scratch.ConnectionString, dbId, ct) > callsAfterFirst,
                "the daily sweep calls pg_database_size again");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}

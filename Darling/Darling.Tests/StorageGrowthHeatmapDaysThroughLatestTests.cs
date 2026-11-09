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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Object Growth heatmap's series keeps a sample stamped past the caller's clock. The caller builds its days through its own
/// clock, and the store's host can write a newer sample when that clock trails it; the reader runs the days through the window's
/// newest sample, so a sample at or after the last day's end still lands in a column, the one its day names, while the ranked
/// objects count it as the newest size.
/// </summary>
/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class StorageGrowthHeatmapDaysThroughLatestTests
{
    private const string ServerName = "darling-sg-heatmap-days-through-latest";
    private const string DatabaseName = "HeatDb";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task ASampleOneMinutePastTheCallersClock_KeepsItsColumn()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live heatmap days test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Fixed instants, never the wall clock: the caller's clock reads half a minute before a UTC midnight, and the store's newest
           sample is a minute later, past that midnight and so past the end of the last day the caller built. */
        var callerNowUtc = new DateTime(2026, 11, 9, 23, 59, 30, DateTimeKind.Unspecified);
        var windowStart = callerNowUtc.AddDays(-30);
        var earlier = new DateTime(2026, 11, 9, 12, 0, 0, DateTimeKind.Unspecified);
        var pastNow = callerNowUtc.AddMinutes(1);
        await SeedAsync(connection, earlier, 100m, ct);
        await SeedAsync(connection, pastNow, 110m, ct);

        var days = DarlingFinOpsUtilizationReader.UtcDays(windowStart, callerNowUtc);
        Assert.Equal(pastNow.Date, days[^1].EndUtc);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var (objects, samples) = await DarlingFinOpsStorageGrowthReader.GetObjectGrowthHeatmapDataAsync(
            dataSource, ServerId, DatabaseName, windowStart, 30, days, 20, 30, ct);

        var orders = Assert.Single(objects);
        Assert.Equal(110m, orders.CurrentReservedMb);
        Assert.Equal([earlier.Date, pastNow.Date], samples.Select(s => s.Day).ToArray());
        Assert.Equal(110d, samples.Single(s => s.Day == pastNow.Date).ReservedMb);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime at, decimal reservedMb, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, object_id, schema_name, table_name, index_id, index_name, reserved_mb, used_mb, total_rows, row_lock_wait_in_ms, index_lock_promotion_count) VALUES ($1, $2, $3, $4, $5, 1001, 'dbo', 'Orders', 1, 'PK_Orders', $6, $6, 1000, 0, 0)",
            connection);
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = at.Ticks });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = at });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = ServerName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = DatabaseName });
        command.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = reservedMb });
        await command.ExecuteNonQueryAsync(ct);
    }
}

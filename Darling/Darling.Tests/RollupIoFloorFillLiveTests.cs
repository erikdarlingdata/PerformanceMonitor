/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Tests;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database, because it materializes continuous aggregates and stops the
   scheduler, which no other class sharing the store may see. */
/// <summary>
/// #5329: the rollup floor cache sees an io rollup filled in place. A new io hourly rollup is filled recent hours first
/// by the refresh policy and the earlier hours later, usually into the materialization chunk the floor cache already
/// keys on, so neither the chunk name nor the hypertable moves. The cache used to keep the later floor until the hour
/// was up, so <see cref="ViewerDataService.IoHourlyCoversWindow"/> stayed false and Top Queries stayed on the partial
/// route. After the fix the next coverage probe reports the earlier floor.
/// </summary>
public sealed class RollupIoFloorFillLiveTests
{
    private const int ServerId = -953291;
    private const string ServerName = "a5329-io-floor-fill";
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task AnIoRollupFilledEarlierInsideItsOldestChunk_IsSeenByTheNextProbe_AndTheRouteBecomesCovering()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #5329 io floor fill test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #5329 io floor fill test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var io = TimescaleSupport.QueryStatsIoHourlyView;
            var late = WindowStart.AddHours(8);

            /* Raw rows for an early window and a late one, eight hours apart (the seed is four hourly buckets each). */
            await IoRollupOracleSeed.PlantAsync(connection, ct, ServerId, ServerName, WindowStart);
            await IoRollupOracleSeed.PlantAsync(connection, ct, ServerId, ServerName, late);

            /* The refresh policy's first fill: the late hours only. The floor is inside the oldest chunk. */
            await RefreshAsync(connection, io, late, late.AddHours(5), ct);
            var chunkBefore = await OldestChunkAsync(connection, io, ct);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            Assert.True(availability.Has(io));

            var partial = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);
            Assert.Equal(late, partial.FloorOf(io));
            Assert.False(ViewerDataService.IoHourlyCoversWindow(availability, partial, io, WindowStart),
                "while only the late hours are materialized the io route must stay on the partial route");

            /* The later fill: the early hours, into the same chunk. */
            await RefreshAsync(connection, io, WindowStart, WindowStart.AddHours(5), ct);
            Assert.Equal(chunkBefore, await OldestChunkAsync(connection, io, ct));

            var covering = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);
            Assert.Equal(WindowStart, covering.FloorOf(io));
            Assert.True(ViewerDataService.IoHourlyCoversWindow(availability, covering, io, WindowStart),
                "after the early hours are materialized the next probe must route to the io rollup");

            /* A probe with nothing new keeps the floor. */
            var again = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);
            Assert.Equal(WindowStart, again.FloorOf(io));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, System.Threading.CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> OldestChunkAsync(NpgsqlConnection connection, string view, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT c.chunk_name
FROM timescaledb_information.continuous_aggregates AS ca
JOIN timescaledb_information.chunks AS c
  ON c.hypertable_schema = ca.materialization_hypertable_schema AND c.hypertable_name = ca.materialization_hypertable_name
WHERE ca.view_schema = 'collect' AND ca.view_name = $1
ORDER BY c.range_start
LIMIT 1", connection);
        command.Parameters.AddWithValue(view);
        return (string?)await command.ExecuteScalarAsync(ct);
    }
}

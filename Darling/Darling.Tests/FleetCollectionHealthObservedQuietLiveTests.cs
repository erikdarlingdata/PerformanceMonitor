// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7242: the activity collectors' 72 hour quiet bar is measured in time the service observed. A server whose service was
/// stopped for a weekend has a productive run 75 hours back but only 19 hours of zero-row runs since, and must read HEALTHY on
/// every surface: the per-server read, the service's fleet card and the Viewer's fleet read. A service that ran the whole time
/// and saw 73 hours of zero-row runs is still flagged on all three.
/// </summary>
// #1776 own-store: the live tests mint their own scratch database through ScratchPostgres.
[Collection("live-postgres")]
public sealed class FleetCollectionHealthObservedQuietLiveTests
{
    private const int ServerId = -7242001;
    private const string ServerName = "fleet-health-observed-quiet";
    private const string StoppedService = "procedure_stats";
    private const string RunningService = "query_snapshots";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AStoppedService_IsNotFlaggedOnEverySurface_ARunningOneStillIs_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live observed-quiet test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            var now = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc);

            /* The field case: the last productive run 75 hours back, then 19 hours of zero-row runs the service saw, at the
               collector's own interval. The 56 hours between are the hours the service was stopped. */
            var stoppedInterval = IntervalOf(StoppedService);
            await SeedAsync(connection, ct, StoppedService, 1_000_000, now.AddHours(-75), runs: 1, stoppedInterval, rowsCollected: 5);
            var stoppedRuns = 19 * 60 / stoppedInterval;
            await SeedAsync(connection, ct, StoppedService, 2_000_000, now.AddMinutes(-(stoppedRuns - 1) * stoppedInterval), stoppedRuns, stoppedInterval, rowsCollected: 0);

            /* A real stop: the service ran the whole time and saw 73 hours of zero-row runs after a productive run. */
            var runningInterval = IntervalOf(RunningService);
            await SeedAsync(connection, ct, RunningService, 3_000_000, now.AddHours(-80), runs: 1, runningInterval, rowsCollected: 5);
            var runningRuns = 73 * 60 / runningInterval;
            await SeedAsync(connection, ct, RunningService, 4_000_000, now.AddMinutes(-(runningRuns - 1) * runningInterval), runningRuns, runningInterval, rowsCollected: 0);

            var windowStart = DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7));
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* The per-server read counts the runs exactly. */
            var perServer = (await DarlingDataReader.GetCollectionHealthAsync(postgres, ServerId, windowStart, ct))
                .ToDictionary(r => r.CollectorName, r => r.ProducedThenStopped);
            Assert.False(perServer[StoppedService]);
            Assert.True(perServer[RunningService]);

            /* The service's fleet card: one regressed collector, the one whose service ran the whole time. */
            var read = typeof(DarlingFleetReader).GetMethod("ReadFailingCollectorCountsAsync", BindingFlags.NonPublic | BindingFlags.Static)
                ?? throw new InvalidOperationException("ReadFailingCollectorCountsAsync not found");
            var counts = await (Task<Dictionary<int, DarlingFleetReader.CollectorCounts>>)read.Invoke(null, [postgres, DateTime.UtcNow, ct])!;
            Assert.Equal(1, counts[ServerId].Regressed);

            /* The Viewer's fleet read (the Overview cards and the status bar) agrees with both. */
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var fleet = (await viewer.GetFleetCollectionHealthByServerAsync(ct))[ServerId]
                .ToDictionary(r => r.CollectorName, r => r.ProducedThenStopped);
            Assert.Equal(perServer, fleet);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static int IntervalOf(string collector) =>
        CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(CollectorScheduleDefaults.All[collector].FrequencyMinutes);

    /// <summary><paramref name="runs"/> SUCCESS runs, <paramref name="stepMinutes"/> apart, the first at <paramref name="firstUtc"/>.</summary>
    private static Task SeedAsync(
        NpgsqlConnection c, CancellationToken ct, string collector, long firstLogId, DateTime firstUtc, int runs, int stepMinutes, int rowsCollected) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, sql_duration_ms, duckdb_duration_ms)
SELECT $1::bigint + g, $2, $3, $4, $5::timestamp + (g * $6) * INTERVAL '1 minute', 100, 'SUCCESS', NULL, $7, 80, 20
FROM generate_series(0, $8 - 1) AS g",
            firstLogId, ServerId, ServerName, collector, DarlingMcpTestData.Naive(firstUtc), stepMinutes, rowsCollected, runs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}

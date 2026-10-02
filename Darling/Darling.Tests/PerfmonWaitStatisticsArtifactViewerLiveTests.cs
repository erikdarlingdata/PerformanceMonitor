/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live half of #4476, through the Viewer's actual batched trend read
/// (<see cref="ViewerDataService.GetPerfmonTrendsByCountersAsync"/>), gated on <c>DARLING_TEST_PG</c>: plants
/// the field-shaped isolated single-sample artifact directly on <c>perfmon_stats</c> (the collector's own
/// write path is not under test here — <c>PerfmonCounterTypeLivePostgresTests</c> and
/// <c>WaitStatisticsArtifactSqlEquivalenceLiveTests</c> already cover that boundary) across 4 instances of one
/// <c>SQLServer:Wait Statistics</c> counter family, 5 collections one minute apart, and reads it back through
/// the real query <see cref="ViewerDataService.PerfmonTrendsSql"/> builds.
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonWaitStatisticsArtifactViewerLiveTests
{
    private const int ServerId = -447644760;
    private const string ServerName = "perfmon-waitstats-artifact-viewer-e2e";
    private const string ObjectName = "SQLServer:Wait Statistics";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ArtifactPointIsSetAside_AndTheSurroundingPointsPlotNormally_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the Wait Statistics artifact viewer round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var t0 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            var times = Enumerable.Range(0, 5).Select(i => t0.AddMinutes(i)).ToArray();

            /* Waits started per second: the spiking instance. Collection 3 (index 2) reads the field-shaped
               cumulative artifact; its neighbours are normal. */
            var startedPerSec = new long[] { 318, 400, 214_396_451, 1_021, 500 };
            /* Waits in progress: a normal, non-spiking instance under the same counter name. */
            var inProgress = new long[] { 2, 3, 2, 1, 2 };
            /* Average wait time (ms) and Cumulative wait time (ms) per second: two more instances, flat. */
            var avgWaitMs = new long[] { 10, 10, 10, 10, 10 };
            var cumulativeWaitMs = new long[] { 50, 50, 50, 50, 50 };

            for (var i = 0; i < 5; i++)
            {
                await PlantAsync(connection, ct, times[i], "Waits started per second", startedPerSec[i]);
                await PlantAsync(connection, ct, times[i], "Waits in progress", inProgress[i]);
                await PlantAsync(connection, ct, times[i], "Average wait time (ms)", avgWaitMs[i]);
                await PlantAsync(connection, ct, times[i], "Cumulative wait time (ms) per second", cumulativeWaitMs[i]);
            }

            var startUtc = times[0].AddMinutes(-1);
            var endUtc = times[^1].AddMinutes(1);

            /* Fact A + B: all four instances planted. */
            await using (var viewer = new ViewerDataService(cs!))
            {
                var trends = await viewer.GetPerfmonTrendsByCountersAsync(ServerId, new List<string> { CounterName }, startUtc, endUtc, ct);

                Assert.True(trends.TryGetValue(CounterName, out var points), "the counter did not plot at all");

                /* Window is 12 minutes: TrendBuckets.AutoMinutes(12, 1, 1500) picks the ladder's narrowest
                   width (1 minute), so each collection is its own point — the pin this test depends on. */
                Assert.Equal(5, points!.Count);

                var point3 = points.Single(p => p.CollectionTime == times[2]);
                /* Fact A: the artifact-excluded sum for collection 3 is 2 (Waits in progress) + 10 (Average
                   wait time) + 50 (Cumulative wait time) = 62 — the 214,396,451 spike is set aside entirely. */
                Assert.Equal(62, point3.Value);

                /* Fact B: only collection 3 set an artifact aside; the other four collections plot with
                   ArtifactsSetAside == 0. */
                Assert.Equal(1, point3.ArtifactsSetAside);
                foreach (var p in points!.Where(p => p.CollectionTime != times[2]))
                {
                    Assert.Equal(0, p.ArtifactsSetAside);
                }
            }

            /* Fact C: plant ONLY the spiking instance under a SEPARATE counter_name (disjoint from the
               CounterName group Facts A/B used, so the two queries never share a row) and confirm collection 3
               drops out of the trend entirely (no fabricated 0) while the other four collections still plot. */
            const string SoloCounterName = "Waits started per second";
            for (var i = 0; i < 5; i++)
            {
                await PlantSoloAsync(connection, ct, times[i], SoloCounterName, startedPerSec[i]);
            }

            await using (var viewer = new ViewerDataService(cs!))
            {
                var trends = await viewer.GetPerfmonTrendsByCountersAsync(ServerId, new List<string> { SoloCounterName }, startUtc, endUtc, ct);
                Assert.True(trends.TryGetValue(SoloCounterName, out var points), "the solo counter did not plot at all");
                Assert.Equal(4, points!.Count);
                Assert.DoesNotContain(points!, p => p.CollectionTime == times[2]);
                Assert.Equal(new[] { times[0], times[1], times[3], times[4] }, points!.Select(p => p.CollectionTime).OrderBy(t => t).ToArray());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteRowsAsync(cleanup, cleanupCt);
                using var servers = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                servers.Parameters.AddWithValue(ServerId);
                await servers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private const string CounterName = "Page latch waits";

    private static Task PlantAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at, string instanceName, long value) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, ObjectName, CounterName, instanceName, value,
            PerfmonCounterTypes.PerfCounterLargeRawCount);

    private static Task PlantSoloAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at, string counterName, long value) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, ObjectName, counterName, "", value,
            PerfmonCounterTypes.PerfCounterLargeRawCount);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM perfmon_stats WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

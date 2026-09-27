/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4476, through <c>get_perfmon_trend</c> itself (<see cref="DarlingMcpTrendTools.GetPerfmonTrend"/>), not the
/// SQL text or the reader alone: a <c>SQLServer:Wait Statistics</c> gauge instance (<c>Waits started per
/// second</c>) plants an isolated single-sample artifact among four otherwise-normal instances, and the tool's
/// JSON must both keep the artifact out of the published peak (Fact A) and say how many it set aside (Fact B).
/// Serialized against every other live class because it shares the store.
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonWaitStatisticsArtifactMcpLiveTests
{
    private const int ServerId = -447647;
    private const string ServerName = "perfmon-wait-artifact-e2e";
    private const string ObjectName = "SQLServer:Wait Statistics";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task GetPerfmonTrend_SetsAsideTheIsolatedSpike_AndReportsHowMany_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live artifact round trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var t0 = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);

            /* Four instances under the Wait Statistics object, five collections one minute apart. Waits started
               per second is a gauge (PerfCounterLargeRawCount, cntr_type 65792) whose middle sample reads its
               lifetime cumulative count — the field-measured shape #4476 exists for. The other three instances
               stay normal at every collection, so the read has real neighbours to sum alongside the artifact. */
            var startedPerSecond = new long[] { 318, 400, 214_396_451, 1_021, 500 };
            var inProgress = new long[] { 2, 3, 2, 1, 2 };
            var avgWaitMs = new long[] { 10, 10, 10, 10, 10 };
            var cumulativeWaitMs = new long[] { 50, 50, 50, 50, 50 };

            for (var i = 0; i < 5; i++)
            {
                var at = t0.AddMinutes(i);
                await PlantAsync(connection, ct, at, "Waits started per second", startedPerSecond[i], PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "Waits in progress", inProgress[i], PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "Average wait time (ms)", avgWaitMs[i], PerfmonCounterTypes.PerfCounterLargeRawCount);
                await PlantAsync(connection, ct, at, "Cumulative wait time (ms) per second", cumulativeWaitMs[i], PerfmonCounterTypes.PerfCounterLargeRawCount);
            }

            /* Smallest window/bucket: one point per collection, so the artifact's collection is its own bucket
               rather than folded into a wider one. */
            using var trend = JsonDocument.Parse(await DarlingMcpTrendTools.GetPerfmonTrend(
                postgres, "Waits started per second", ServerName, hours_back: 1, as_of: null, bucket_minutes: 1));

            var root = trend.RootElement;
            var points = root.GetProperty("trend").EnumerateArray().ToList();
            /* Only 4 points, not 5: this counter has a single instance, so the artifact's own collection has
               nothing else to sum — every row in that bucket was set aside, and GetPerfmonBucketsAsync drops
               a bucket like that rather than publish a fabricated 0. */
            Assert.Equal(4, points.Count);

            /* Fact A: the artifact never reaches the published peak. */
            var maxValue = points.Max(p => p.GetProperty("peak_value").ValueKind == JsonValueKind.Number
                ? p.GetProperty("peak_value").GetInt64()
                : p.GetProperty("value").GetInt64());
            Assert.True(maxValue < 10_000, $"expected the artifact excluded from the published max, got {maxValue}");

            /* Fact B: the envelope says exactly one was set aside, with the caption. */
            Assert.Equal(1, root.GetProperty("artifacts_set_aside").GetInt64());
            Assert.True(root.TryGetProperty("notes", out var notes), "expected a notes line when artifacts were set aside");
            Assert.Contains("1 one-sample Wait Statistics spike set aside", notes.GetString(), StringComparison.Ordinal);

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

    private static Task PlantAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at,
        string counterName, long value, int type) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, ObjectName, counterName, "", value,
            (long?)null, (int?)null, type);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand("DELETE FROM perfmon_stats WHERE server_id = $1", connection);
        cleanup.Parameters.AddWithValue(ServerId);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

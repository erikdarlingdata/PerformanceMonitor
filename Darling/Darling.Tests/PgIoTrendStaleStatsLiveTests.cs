/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5425: <c>get_pg_io_trend</c> answered "Exception while reading from stream" - its 30 s read deadline - whenever
/// the planner's row estimate for <c>pg_io_stats</c> was stale.
///
/// <para>The trend joined its per-snapshot sums back to a "spans" CTE (the interval to the previous collection) on
/// <c>collection_time</c>. When the table's statistics describe other rows - an autoanalyze that ran while the table
/// held another server's data, or none, which is what a store shared with other tests looks like - the planner
/// estimates one row for the window, picks a nested loop for that join, and re-runs the whole windowed
/// "sampled" pass once per snapshot. Measured on a local PostgreSQL over 72 hours of one-minute collections:
/// 64.7 s with the join (7.7 s over 24 hours), 60 ms without it, on the same stale estimate. This fixture builds
/// that state on purpose - statistics taken over another server's older rows, which are then deleted - so the
/// read is run against the estimate that makes the old shape quadratic, instead of waiting for a parallel test run
/// to do it by luck.</para>
///
/// <para>Only the timing of the read is asserted through its own deadline (<see cref="StorageCommandDeadlines.McpReadSeconds"/>):
/// the old shape throws at that deadline, the new one returns in well under a second. The values are pinned by
/// <c>TrendPayloadBudgetLiveTests</c> and the other get_pg_io_trend tests.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgIoTrendStaleStatsLiveTests
{
    private const string ServerName = "pg-io-trend-stale-stats";
    private const string OtherServerName = "pg-io-trend-stale-stats-other";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int OtherServerId = ServerIdHelper.GetDeterministicHashCode(OtherServerName);

    private const int WindowHours = 72;
    private const int BucketMinutes = 30;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheIoTrend_StaysLinear_WhenThePlannersRowEstimateForTheTableIsStale()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live pg_io_stats stale-statistics read.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0).AddMinutes(-1);
            var windowMinutes = WindowHours * 60;

            /* Statistics over ANOTHER server's rows, from well before this window, then those rows gone: the
               planner's picture of pg_io_stats is now "nothing near this server_id or these times". */
            await PlantAsync(connection, OtherServerId, OtherServerName, end.AddDays(-40), windowMinutes, 5_000_000L, ct);
            using (var analyze = new NpgsqlCommand("ANALYZE pg_io_stats", connection) { CommandTimeout = 300 })
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            await DeleteRowsAsync(connection, ct);
            await PlantAsync(connection, ServerId, ServerName, end.AddMinutes(-windowMinutes), windowMinutes, 6_000_000L, ct);

            var points = await DarlingPgTrendReader.GetIoTrendAsync(
                postgres, ServerId, "client backend", "normal",
                end.AddHours(-WindowHours), end, BucketMinutes, ct);

            /* 72 hours at 30-minute buckets is 144 buckets; the first snapshot has nothing to difference against
               and the last bucket may be partial. */
            Assert.InRange(points.Count, 140, 146);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>Two object types, one-minute cadence, cumulative counters - the shape
    /// <c>TrendPayloadBudgetLiveTests</c> and <c>PgIoTrendDefaultBudgetLiveTests</c> seed, in this file's own rows.</summary>
    private static async Task PlantAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime start, int minutes, long idOffset, CancellationToken ct)
    {
        using var plant = new NpgsqlCommand(@"
WITH s AS (
    SELECT n, o.i, o.object_type, 1100 + (n % 13) * 57 AS r
    FROM generate_series(0, $5) AS n
    CROSS JOIN (VALUES (0, 'relation'), (1, 'temp relation')) AS o(i, object_type)
)
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, extends, op_bytes, hits, evictions, stats_reset)
SELECT $1 + n * 2 + i, $2 + n * interval '1 minute', $3, $4, 'client backend', object_type, 'normal',
       SUM(r) OVER w, SUM(r * 1.37) OVER w, SUM(r / 5) OVER w, SUM(r / 5 * 0.81) OVER w, SUM(r / 30) OVER w,
       8192, SUM(r * 83) OVER w, SUM(r / 50) OVER w, NULL::timestamp
FROM s
WINDOW w AS (PARTITION BY i ORDER BY n)", connection) { CommandTimeout = 300 };
        plant.Parameters.AddWithValue(CollectionIdGenerator.Next() + idOffset * 100);
        plant.Parameters.AddWithValue(DarlingMcpTestData.Naive(start));
        plant.Parameters.AddWithValue(serverId);
        plant.Parameters.AddWithValue(serverName);
        plant.Parameters.AddWithValue(minutes);
        await plant.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { ServerId, OtherServerId })
        {
            using var delete = new NpgsqlCommand("DELETE FROM pg_io_stats WHERE server_id = $1", connection) { CommandTimeout = 300 };
            delete.Parameters.AddWithValue(id);
            await delete.ExecuteNonQueryAsync(ct);
        }
    }
}

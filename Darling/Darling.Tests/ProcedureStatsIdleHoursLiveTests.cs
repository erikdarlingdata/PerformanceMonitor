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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")] (it also stops the scratch database's TimescaleDB
   background workers, which the shared store must never inherit). */

/// <summary>
/// #5449: the procedure_stats collector stores no row for a procedure that did no work in a cycle, so the hourly rollup holds
/// no row for an hour in which no procedure worked. An older store kept the idle rows, so its rollup has a (zero) row for every
/// hour. The hourly-tier trend must answer the same on both: a quiet hour between two busy ones is a measured 0 (not a gap), and
/// a wider bucket divides by every hour in it, not only the busy ones. Each test seeds two servers with the same work:
/// <see cref="OldServer"/> keeps the idle rows, <see cref="NewServer"/> leaves them out. A TimescaleDB scratch store; the raw rows
/// are purged after the rollup refreshes, so the routed read serves from the interval successor views.
/// </summary>
public sealed class ProcedureStatsIdleHoursLiveTests
{
    private const int OldServer = -544911;
    private const int NewServer = -544912;
    private const string OldName = "idle-hours-old";
    private const string NewName = "idle-hours-new";

    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Work in hours 1, 2 and 6 (hours 3, 4 and 5 are quiet): the same on both servers. The older store keeps a zero row
    /// for the quiet hours between them, and none for the hours before the first work or after the last.</summary>
    private static readonly int[] BusyHours = { 1, 2, 6 };

    [Fact]
    public async Task HourlyTier_AQuietHourIsAZeroPoint_AndAWiderBucketCountsIt_OnBothStores()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live procedure_stats idle-hour test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live procedure_stats idle-hour test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, OldServer, OldName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, NewServer, NewName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            var windowEnd = WindowStart.AddDays(1);
            for (var hour = 1; hour <= 6; hour++)
            {
                var busy = BusyHours.Contains(hour);
                var at = WindowStart.AddHours(hour).AddMinutes(10);
                await InsertAsync(connection, NewServer, NewName, at, busy, ct, skipWhenIdle: true);
                await InsertAsync(connection, OldServer, OldName, at, busy, ct, skipWhenIdle: false);
            }

            await using (var refresh = new NpgsqlCommand(
                $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection))
            {
                refresh.Parameters.AddWithValue(WindowStart);
                refresh.Parameters.AddWithValue(windowEnd.AddHours(1));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            await using (var purge = new NpgsqlCommand("DELETE FROM collect.procedure_stats WHERE collection_time >= $1 AND collection_time < $2", connection))
            {
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            var asOf = DateTime.SpecifyKind(windowEnd, DateTimeKind.Utc).ToString("o");

            /* The MCP tool, one-hour buckets: a point per hour from the first busy hour to the last, the quiet ones at 0. */
            var oldHours = await McpSeriesAsync(data, OldName, asOf, 60, budget, ct);
            var newHours = await McpSeriesAsync(data, NewName, asOf, 60, budget, ct);
            Assert.Equal(oldHours.OrderBy(p => p.Key).ToArray(), newHours.OrderBy(p => p.Key).ToArray());
            Assert.Equal(6, newHours.Count);
            Assert.Equal(0.0, newHours[WindowStart.AddHours(4)], 9);
            Assert.True(newHours[WindowStart.AddHours(1)] > 0);

            /* Four-hour buckets: [00:00, 04:00) holds hours 1, 2 and 3 (3 is quiet) and [04:00, 08:00) holds hours 4, 5 and 6 (only 6
               worked). Without the quiet hours the rollup holds hours 1 and 2 in the first and only hour 6 in the second, so
               the new store's rates read 1.5 and 3 times the old store's. */
            var oldWide = await McpSeriesAsync(data, OldName, asOf, 240, budget, ct);
            var newWide = await McpSeriesAsync(data, NewName, asOf, 240, budget, ct);
            Assert.Equal(oldWide.OrderBy(p => p.Key).ToArray(), newWide.OrderBy(p => p.Key).ToArray());
            Assert.Equal((newHours[WindowStart.AddHours(1)] + newHours[WindowStart.AddHours(2)]) / 3, newWide[WindowStart], 12);
            Assert.Equal(newHours[WindowStart.AddHours(6)] / 3, newWide[WindowStart.AddHours(4)], 12);

            /* The desktop viewer's read, the same two shapes. */
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var now = DateTime.UtcNow;
            var oldViewer = await ViewerSeriesAsync(viewer, OldServer, windowEnd, now, ct);
            var newViewer = await ViewerSeriesAsync(viewer, NewServer, windowEnd, now, ct);
            Assert.Equal(oldViewer.OrderBy(p => p.Key).ToArray(), newViewer.OrderBy(p => p.Key).ToArray());
            Assert.Equal(6, newViewer.Count);
            Assert.Equal(0.0, newViewer[WindowStart.AddHours(4)], 9);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt)));
            });
        }
    }

    private static async Task<Dictionary<DateTime, double>> McpSeriesAsync(
        NpgsqlDataSource data, string serverName, string asOf, int bucketMinutes, TrendBudget budget, CancellationToken ct)
    {
        var json = await DarlingMcpTrendTools.GetProcedureDurationTrend(data, serverName, 24, asOf, bucketMinutes, DatabaseFilter.All, budget, ct);
        var root = JsonDocument.Parse(json).RootElement;
        Assert.Equal("hourly", root.GetProperty("source").GetString());
        return root.GetProperty("trend").EnumerateArray().ToDictionary(
            p => DateTime.Parse(p.GetProperty("time").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind),
            p => p.GetProperty("elapsed_ms_per_second").GetDouble());
    }

    private static async Task<Dictionary<DateTime, double>> ViewerSeriesAsync(
        ViewerDataService viewer, int serverId, DateTime windowEnd, DateTime now, CancellationToken ct)
    {
        var series = await viewer.GetProcedureDurationTrendAsync(serverId, WindowStart, windowEnd, null, now, ct);
        Assert.Equal(RetentionTier.Hourly, series.Tier);
        return series.Points.ToDictionary(p => p.CollectionTime, p => p.Value);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at, bool busy, CancellationToken ct, bool skipWhenIdle)
    {
        if (!busy && skipWhenIdle)
        {
            return;
        }

        var weight = busy ? 3_600_000L : 0L;
        var executions = busy ? 10L : 0L;
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, 'AppDb', 'dbo', 'usp_Work', '0x01', $5, $6, $6, 0, 0, 60)",
            CollectionIdGenerator.Next(), at, serverId, serverName, executions, weight);
    }
}

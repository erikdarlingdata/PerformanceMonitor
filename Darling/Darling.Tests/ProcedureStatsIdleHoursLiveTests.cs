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
    /// for the quiet hours between them, and none for the hours before the first work or after the last. The new store logs a
    /// collector run in each of hours 1 to 6.</summary>
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
                /* Production order: the new store's busy run stores its row at its start and is logged just after it; a quiet run is
                   logged and stores nothing. The older store has no run log, so its hours are the rollup's own. */
                await InsertAsync(connection, NewServer, NewName, at, busy, ct, skipWhenIdle: true);
                await LogRunAsync(connection, NewServer, NewName, at.AddSeconds(3), busy ? 1 : 0, ct);
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

    /// <summary>
    /// A seven-hour bucket on a store with an outage in it. The rollup holds hours 1, 2 and 4; the collector ran in hours 0, 1, 2,
    /// 4 and 5 (hours 0 and 5 stored nothing, so they are measured zeros); the collector was down in hour 3 (no row, no run); and hour
    /// 6 has runs and raw rows the rollup has not materialized yet. The hours in the series are the five with a rollup row or a run
    /// and no raw row: 0, 1, 2, 4 and 5. The outage hour and the not-yet-materialized hour stay gaps, so the bucket divides by 5
    /// hours (0.6), where the earlier fill between the first and last rollup hour counted 4 (0.75), counted the outage as a quiet hour,
    /// and missed the idle edge hours.
    /// </summary>
    [Fact]
    public async Task HourlyTier_AnOutageAndAnUnmaterializedHourStayGaps_AndTheIdleEdgeHoursFill()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live procedure_stats idle-hour test.");
        var ct = TestContext.Current.CancellationToken;
        const int server = -544913;
        const string name = "idle-hours-outage";

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenHypertableStoreAsync(scratch, ct, (server, name));
        var bodySucceeded = false;
        try
        {
            /* The 420-minute buckets start on the origin's multiples of seven hours: start on one, so hours 0 to 6 are one bucket. */
            var origin = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
            var anchor = WindowStart.AddDays(30);
            var h0 = origin.AddMinutes(420 * Math.Floor((anchor - origin).TotalMinutes / 420));
            var end = h0.AddHours(7);

            foreach (var hour in new[] { 1, 2, 4 })
            {
                await InsertAsync(connection, server, name, h0.AddHours(hour).AddMinutes(10), busy: true, ct, skipWhenIdle: true);
            }

            foreach (var hour in new[] { 0, 1, 2, 4, 5 })
            {
                var at = h0.AddHours(hour).AddMinutes(10);
                await LogRunAsync(connection, server, name, at.AddSeconds(3), rowsCollected: hour is 1 or 2 or 4 ? 1 : 0, ct);
            }

            await RefreshAndPurgeAsync(connection, h0, h0.AddHours(6), ct);

            /* Hour 6 is the newest hour: raw rows and runs, no rollup row yet (and nothing purged). */
            await InsertAsync(connection, server, name, h0.AddHours(6).AddMinutes(10), busy: true, ct, skipWhenIdle: true);
            await LogRunAsync(connection, server, name, h0.AddHours(6).AddMinutes(10).AddSeconds(3), rowsCollected: 1, ct);

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* The chart's hourly read over the interval rollup, unfiltered and filtered: zeros at hours 0 and 5, no point at 3 or 6. */
            var rollup = $"collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}";
            foreach (var withFilter in new[] { false, true })
            {
                var hourlyPoints = await ReadAsync(
                    data, DurationTrendRouting.BuildHourlyTrendSql(rollup, withFilter, coverIdleHours: true), withFilter, server, h0, end, null, ct);
                Assert.Equal(new[] { 0, 1, 2, 4, 5 }.Select(h => h0.AddHours(h)), hourlyPoints.Select(p => p.Time));
                Assert.Equal(new[] { 0.0, 1.0, 1.0, 1.0, 0.0 }, hourlyPoints.Select(p => Math.Round(p.Value, 9)));
            }

            /* The 7-hour bucket, both statements over the rollup: 3 busy hours of 3,600 ms over five hours. */
            foreach (var withFilter in new[] { false, true })
            {
                var bucketed = await ReadAsync(
                    data, DurationTrendRouting.BuildBucketedHourlyTrendSql(rollup, withFilter, coverIdleHours: true), withFilter, server, h0, end, 420, ct);
                var point = Assert.Single(bucketed);
                Assert.Equal(h0, point.Time);
                Assert.Equal(3.0 / 5.0, point.Value, 9);
            }

            /* And the tool's own statement: the same bucket. */
            var json = await DarlingMcpTrendTools.GetProcedureDurationTrend(
                data, name, 7, DateTime.SpecifyKind(end, DateTimeKind.Utc).ToString("o"), 420, DatabaseFilter.All,
                TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), ct);
            var trend = JsonDocument.Parse(json).RootElement.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(3.0 / 5.0, Assert.Single(trend).GetProperty("elapsed_ms_per_second").GetDouble(), 9);

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// L1: a window that opens on idle hours the collector ran in. The first two hours stored nothing (two quiet runs), the work
    /// starts in hour 2. The series starts at the window's start, so the tool does not report the window as cut short; before the
    /// idle hours were read from the runs, the first point was hour 2, more than the 90-minute slack after the start.
    /// </summary>
    [Fact]
    public async Task HourlyTier_AWindowThatOpensOnIdleHoursWithRuns_IsNotTruncated()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live procedure_stats idle-hour test.");
        var ct = TestContext.Current.CancellationToken;
        const int server = -544914;
        const string name = "idle-hours-edge";

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenHypertableStoreAsync(scratch, ct, (server, name));
        var bodySucceeded = false;
        try
        {
            var windowEnd = WindowStart.AddDays(1);
            for (var hour = 0; hour <= 3; hour++)
            {
                var at = WindowStart.AddHours(hour).AddMinutes(10);
                var busy = hour >= 2;
                await InsertAsync(connection, server, name, at, busy, ct, skipWhenIdle: true);
                await LogRunAsync(connection, server, name, at.AddSeconds(3), busy ? 1 : 0, ct);
            }

            await RefreshAndPurgeAsync(connection, WindowStart, windowEnd, ct);

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpTrendTools.GetProcedureDurationTrend(
                data, name, 24, DateTime.SpecifyKind(windowEnd, DateTimeKind.Utc).ToString("o"), 60, DatabaseFilter.All,
                TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), ct);
            var root = JsonDocument.Parse(json).RootElement;

            Assert.Equal("hourly", root.GetProperty("source").GetString());
            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            var trend = root.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(4, trend.Length);
            Assert.Equal(0.0, trend[0].GetProperty("elapsed_ms_per_second").GetDouble(), 9);
            Assert.Equal(0.0, trend[1].GetProperty("elapsed_ms_per_second").GetDouble(), 9);
            Assert.True(trend[2].GetProperty("elapsed_ms_per_second").GetDouble() > 0);

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>
    /// #5449 round 2: a busy run (it stored rows) whose point lands in the next hour must not make that hour. The run starts at
    /// 1:59:58 and stores its row in hour 1, but its log row (2:00:02, duration 1 s) puts the plotted point in hour 2. Hour 2 has no
    /// rollup row and no other run: the collector was down for it, so it stays a gap. Counting a busy run in <c>run_hours</c> filled it
    /// with a 0 over 3,600 s. Hours 0 and 3 hold idle runs and fill with 0.
    /// </summary>
    [Fact]
    public async Task HourlyTier_ABusyRunCrossingAnHourEdgeDoesNotFillTheOutageHourAfterIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live procedure_stats idle-hour test.");
        var ct = TestContext.Current.CancellationToken;
        const int server = -544915;
        const string name = "idle-hours-edge-run";

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenHypertableStoreAsync(scratch, ct, (server, name));
        var bodySucceeded = false;
        try
        {
            var h0 = WindowStart.AddDays(30);
            var end = h0.AddHours(6);

            await LogRunAsync(connection, server, name, h0.AddMinutes(10).AddSeconds(3), rowsCollected: 0, ct);
            await InsertAsync(connection, server, name, h0.AddHours(1).AddMinutes(10), busy: true, ct, skipWhenIdle: true);
            await LogRunAsync(connection, server, name, h0.AddHours(1).AddMinutes(10).AddSeconds(3), rowsCollected: 1, ct);
            await InsertAsync(connection, server, name, h0.AddHours(1).AddMinutes(59).AddSeconds(58), busy: true, ct, skipWhenIdle: true);
            await LogRunAsync(connection, server, name, h0.AddHours(2).AddSeconds(2), rowsCollected: 1, ct, durationMs: 1000);
            await LogRunAsync(connection, server, name, h0.AddHours(3).AddMinutes(10).AddSeconds(3), rowsCollected: 0, ct);

            await RefreshAndPurgeAsync(connection, h0, end, ct);

            await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollup = $"collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}";
            var points = await ReadAsync(
                data, DurationTrendRouting.BuildHourlyTrendSql(rollup, false, coverIdleHours: true), false, server, h0, end, null, ct);

            Assert.Equal(new[] { 0, 1, 3 }.Select(h => h0.AddHours(h)), points.Select(p => p.Time));
            Assert.Equal(0.0, points[0].Value, 9);
            Assert.Equal(0.0, points[2].Value, 9);

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    private static async Task<NpgsqlConnection> OpenHypertableStoreAsync(
        ScratchPostgres scratch, CancellationToken ct, params (int Id, string Name)[] servers)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
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

        foreach (var (id, name) in servers)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return connection;
    }

    /// <summary>Materializes the interval rollup over [from, to) and then deletes the raw rows in it, so the routed read serves from the rollup.</summary>
    private static async Task RefreshAndPurgeAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using (var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection))
        {
            refresh.Parameters.AddWithValue(from);
            refresh.Parameters.AddWithValue(to);
            await refresh.ExecuteNonQueryAsync(ct);
        }

        await using var purge = new NpgsqlCommand("DELETE FROM collect.procedure_stats WHERE collection_time >= $1 AND collection_time < $2", connection);
        purge.Parameters.AddWithValue(from);
        purge.Parameters.AddWithValue(to);
        await purge.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupAsync(ScratchPostgres scratch, bool bodySucceeded)
    {
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
        {
            await using var probe = new NpgsqlCommand(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
            Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt)));
        });
    }

    /// <summary>Runs one hourly statement. Parameters: $1 server, $2 start, $3 end, [$4 the database filter, null = all], then the bucket width when given.</summary>
    private static async Task<List<(DateTime Time, double Value)>> ReadAsync(
        NpgsqlDataSource data, string sql, bool withFilter, int serverId, DateTime start, DateTime end, int? widthMinutes, CancellationToken ct)
    {
        await using var command = data.CreateCommand(sql);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, start);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, end);
        if (withFilter)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = DBNull.Value });
        }

        if (widthMinutes is int width)
        {
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, width);
        }

        var rows = new List<(DateTime, double)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.GetDouble(1)));
        }

        return rows;
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

    private static async Task LogRunAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime loggedAt, int rowsCollected, CancellationToken ct, int durationMs = 3000)
        => await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
              VALUES ($1, $2, $3, 'procedure_stats', $4, $6, 'SUCCESS', $5)",
            CollectionIdGenerator.Next(), serverId, serverName, loggedAt, rowsCollected, durationMs);

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

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
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
/// #9147 LIVE: <c>get_query_duration_trend</c> over a day read every raw row of the window. A raw-tier window of several hours now takes its whole
/// hours from the hourly rollup and reads only its two edges raw. The store holds one collection every five minutes (one query, 10 executions each)
/// and the rollup is built up to two hours below the newest whole hour H, so the hours H-23 to H-3 come from the rollup, the first partial hour
/// from raw, and H-2 to the window's end from raw. The executions the points carry must equal the raw rows' executions for the window.
/// </summary>
/* #1776 own-store: the class mints a scratch database (it builds continuous aggregates), so it stays out of [Collection("live-postgres")]. */
public sealed class DurationTrendHourlyStitchLiveTests
{
    private const int ServerId = -914701;
    private const string ServerName = "a9147-trend-stitch";

    [Fact]
    public async Task ADayWindow_TakesItsWholeHoursFromTheRollup_AndTheExecutionsStillAddUpToRaw()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live duration trend stitch test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "The hourly tier needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = LiveClock.Now();
            var h = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-1);
            var firstCollection = h.AddHours(-30);

            /* One collection every five minutes from H-30 to H+30 minutes. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, min_dop, max_dop,
                      query_plan_hash, sample_interval_seconds)
                  SELECT $1 + g, $2 + g * INTERVAL '5 minutes', $3, $4, 'StitchDb', '0xQ9147', '0xH9147', 10, 10000, 20000, 100, 100, 1, 1, '0xP9147', 300
                  FROM generate_series(0, $5) AS g",
                CollectionIdGenerator.Next(), firstCollection, ServerId, ServerName, 30 * 12 + 6);

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView })
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(firstCollection.AddHours(-1));
                refresh.Parameters.AddWithValue(h.AddHours(-2));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            var end = h.AddMinutes(30);
            var start = end.AddHours(-24);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var reply = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryDurationTrend(
                postgres, ServerName, 24, end.ToString("o", CultureInfo.InvariantCulture), null, DatabaseFilter.All, TrendBudget.Chart, ct)).RootElement;
            var points = reply.GetProperty("trend").EnumerateArray().ToArray();

            /* The window starts at H-23:30, so the first partial hour holds 5 collections, then the 21 hours H-23 to H-3 (one point each), then H-2 to the end (31 collections). */
            Assert.Equal(5 + 21 + 31, points.Length);
            var hourly = points.Skip(5).Take(21).ToArray();
            Assert.All(hourly, p => Assert.Equal(120, p.GetProperty("execution_count").GetInt64()));
            Assert.Equal(h.AddHours(-23).ToString("o", CultureInfo.InvariantCulture)[..19], hourly[0].GetProperty("time").GetString()![..19]);
            Assert.Contains("hourly rollup", reply.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);

            await using var raw = new NpgsqlCommand(
                "SELECT COALESCE(SUM(delta_execution_count), 0)::bigint FROM collect.query_stats WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3", connection);
            raw.Parameters.AddWithValue(ServerId);
            raw.Parameters.AddWithValue(start);
            raw.Parameters.AddWithValue(end);
            Assert.Equal(Convert.ToInt64(await raw.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture), points.Sum(p => p.GetProperty("execution_count").GetInt64()));

            /* A window of three hours stays on the raw rows: every collection is its own point and the note says nothing of the rollup. */
            var short3 = JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryDurationTrend(
                postgres, ServerName, 3, end.ToString("o", CultureInfo.InvariantCulture), null, DatabaseFilter.All, TrendBudget.Chart, ct)).RootElement;
            Assert.Equal(36, short3.GetProperty("trend").GetArrayLength());
            Assert.DoesNotContain("hourly rollup", short3.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt), CultureInfo.InvariantCulture));
            });
        }
    }
}

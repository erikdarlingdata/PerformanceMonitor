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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4716: an hourly hole repair invalidates the day of every daily that already holds a bucket for it, but no
/// daily refresh covered that day again: the ordinary repair loop repaired only the hourly, and the daily chase
/// ran only from the seam loop. Once the day is older than the daily policy's 3-day window it stays partial, and
/// it becomes the only copy when the hourly ages out. Two dailies are proved end to end here (the interval
/// query_stats and query_stats_db pairs, both raw-sourced from collect.query_stats): a day whose daily row was
/// built while two hours were missing must equal the hourly and the raw after the repair, and a day the daily
/// holds no row for must be left to the daily's own scan.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables, so it cannot race the live
   collection, and serializing it would be pure slowdown. Leave it out; this comment is here so the next sweep
   does not "fix" it. */
public sealed class DailyAfterHoleRepairLiveTests
{
    private const int ServerId = -471600;
    private const string ServerName = "daily-after-hole-repair";

    /// <summary>Fixed anchor day (never wall clock): a Monday. The repair's clock is D + 6 days, so both seeded days are complete.</summary>
    private static readonly DateTime D = new(2026, 2, 2, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task AHourlyHoleRepair_RefreshesTheDaysItsDailiesAlreadyHold_AndLeavesADayWithNoDailyRowAlone()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live daily-after-hole-repair test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live daily-after-hole-repair test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var pairs = new[]
        {
            (Hourly: TimescaleSupport.QueryStatsIntervalHourlyView, Daily: TimescaleSupport.QueryStatsIntervalDailyView),
            (Hourly: TimescaleSupport.QueryStatsDbIntervalHourlyView, Daily: TimescaleSupport.QueryStatsDbIntervalDailyView),
        };

        var bodySucceeded = false;
        try
        {
            var d2 = D.AddDays(1);
            var missing = new[] { 9, 10 };

            /* Two days of raw, two mid-day hours missing from each. */
            await InsertDayAsync(connection, D, 0, missing, ct);
            await InsertDayAsync(connection, d2, 1, missing, ct);

            /* Both hourlies over both days, then each daily over D ONLY: D2 has an hourly bucket but no daily row. */
            foreach (var pair in pairs)
            {
                await RefreshAsync(connection, pair.Hourly, D, D.AddDays(2), ct);
                await RefreshAsync(connection, pair.Daily, D, d2, ct);
            }

            /* The defect, measured: the daily holds a row for D built from the incomplete hourly. */
            foreach (var pair in pairs)
            {
                var daily = await ResolveAsync(connection, pair.Daily, ct);
                var partial = await ReadTotalsAsync(connection, daily, D, d2, ct);
                Assert.Equal(22 * 3 * 10, partial.Executions);
                Assert.True(partial.Rows > 0);
            }

            /* The missing hours land below both hourlies' watermark: raw now holds them, the hourlies do not. */
            await InsertDayAsync(connection, D, 0, Array.Empty<int>(), ct, only: missing);
            await InsertDayAsync(connection, d2, 1, Array.Empty<int>(), ct, only: missing);

            var log = new CapturingTestLogger();
            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, log, D.AddDays(6), ct);

            var rawD = await ReadRawAsync(connection, D, d2, ct);
            var rawD2 = await ReadRawAsync(connection, d2, d2.AddDays(1), ct);
            Assert.Equal(24 * 3 * 10, rawD);
            Assert.Equal(24 * 3 * 10, rawD2);

            foreach (var pair in pairs)
            {
                var hourlyD = await ReadTotalsAsync(connection, "collect." + pair.Hourly, D, d2, ct);
                var hourlyD2 = await ReadTotalsAsync(connection, "collect." + pair.Hourly, d2, d2.AddDays(1), ct);
                var daily = await ResolveAsync(connection, pair.Daily, ct);
                var dailyD = await ReadTotalsAsync(connection, daily, D, d2, ct);
                var dailyD2 = await ReadTotalsAsync(connection, daily, d2, d2.AddDays(1), ct);

                /* The hourly repair closed both days' holes... */
                Assert.Equal(rawD, hourlyD.Executions);
                Assert.Equal(rawD2, hourlyD2.Executions);

                /* ...and the day the daily already held is whole again: every additive total agrees. */
                Assert.Equal(hourlyD.Executions, dailyD.Executions);
                Assert.Equal(hourlyD.WorkerTime, dailyD.WorkerTime);
                Assert.Equal(hourlyD.Samples, dailyD.Samples);

                /* A day with no daily row is not refreshed by this: the daily's own scan repairs it later. */
                Assert.Equal(0, dailyD2.Rows);
            }

            Assert.True(summary.DailyBucketsChained >= pairs.Length, $"refreshed days recorded: {summary.DailyBucketsChained}");
            var chained = log.Joined.Split(" | ").Where(l => l.Contains("Materialization-hole repair (#4716)", StringComparison.Ordinal)).ToArray();
            Assert.NotEmpty(chained);
            Assert.DoesNotContain(chained, l => l.Contains("2026-02-03", StringComparison.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>Hours 0-23 of <paramref name="day"/> except <paramref name="skip"/>, or only the hours in <paramref name="only"/> when given. Three rows an hour.</summary>
    private static async Task InsertDayAsync(
        NpgsqlConnection connection, DateTime day, int dayIndex, int[] skip, CancellationToken ct, int[]? only = null)
    {
        for (var hour = 0; hour < 24; hour++)
        {
            if (only is null ? skip.Contains(hour) : !only.Contains(hour))
            {
                continue;
            }

            for (var minute = 0; minute < 60; minute += 20)
            {
                await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'DailyDb', '0xDAILYHASH', '0xDAILYHANDLE', 1000, 1000, 10, 1200)", connection);
                insert.Parameters.AddWithValue((long)(dayIndex * 10000 + hour * 100 + minute));
                insert.Parameters.AddWithValue(day.AddHours(hour).AddMinutes(minute));
                insert.Parameters.AddWithValue(ServerId);
                insert.Parameters.AddWithValue(ServerName);
                await insert.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The daily's MATERIALIZATION hypertable, not the view: a real-time view shows rows that are not materialized.</summary>
    private static async Task<string> ResolveAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);
        return $"\"{materialization.Value.Schema}\".\"{materialization.Value.Name}\"";
    }

    private static async Task<(decimal Executions, decimal WorkerTime, decimal Samples, long Rows)> ReadTotalsAsync(
        NpgsqlConnection connection, string relation, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            $"SELECT coalesce(sum(execution_count_sum), 0), coalesce(sum(worker_time_sum), 0), coalesce(sum(sample_count), 0), count(*) " +
            $"FROM {relation} WHERE server_id = $1 AND bucket >= $2 AND bucket < $3", connection);
        read.Parameters.AddWithValue(ServerId);
        read.Parameters.AddWithValue(from);
        read.Parameters.AddWithValue(to);
        await using var reader = await read.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return (Convert.ToDecimal(reader.GetValue(0)), Convert.ToDecimal(reader.GetValue(1)), Convert.ToDecimal(reader.GetValue(2)), reader.GetInt64(3));
    }

    private static async Task<decimal> ReadRawAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT coalesce(sum(delta_execution_count), 0) FROM collect.query_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
        read.Parameters.AddWithValue(ServerId);
        read.Parameters.AddWithValue(from);
        read.Parameters.AddWithValue(to);
        return Convert.ToDecimal(await read.ExecuteScalarAsync(ct));
    }
}

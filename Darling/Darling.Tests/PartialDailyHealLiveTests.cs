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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4716: end to end, the one-time heal of daily days an earlier hourly repair left short. The pair is the raw
/// <c>collect.query_store_stats</c> to <c>query_store_stats_hourly</c> to <c>query_store_stats_daily</c> (the Query
/// Store pair, whose raw rows other live tests in this suite already seed). Five days are seeded and each takes a different path through the heal: a day the daily already matches
/// (left alone), a day the daily holds fewer samples for (rebuilt), a day the hourly holds and the daily holds
/// no row for (left to the hole scan), a day the daily holds MORE samples for than the hourly (never shrunk),
/// and the oldest day (never compared: the day the source begins in). The second run must find every marker
/// set and touch nothing.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables, so it cannot race the live
   collection, and serializing it would be pure slowdown. Leave it out; this comment is here so the next sweep
   does not "fix" it. */
public sealed class PartialDailyHealLiveTests
{
    private const int ServerId = -471601;
    private const string ServerName = "partial-daily-heal";
    private const long SentinelExecutions = 999_999;

    /// <summary>The sentinel is written to each of a day's two query identities, so a day that was not rebuilt sums to twice it.</summary>
    private const long SentinelDayTotal = 2 * SentinelExecutions;

    /// <summary>Fixed anchor day (never wall clock): a Monday. The heal's clock is D0 + 12 days, so every seeded day is complete and older than the daily policy's window.</summary>
    private static readonly DateTime D0 = new(2026, 2, 2, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task TheHeal_RebuildsOnlyTheDayTheDailyHoldsFewerSamplesFor_WritesItsMarker_AndTheSecondRunTouchesNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live partial-daily heal test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live partial-daily heal test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var hourly = TimescaleSupport.QueryStoreStatsHourlyView;
        var daily = TimescaleSupport.QueryStoreStatsDailyView;

        var bodySucceeded = false;
        try
        {
            var d1 = D0.AddDays(1);
            var d2 = D0.AddDays(2);
            var d3 = D0.AddDays(3);
            var d4 = D0.AddDays(4);
            var d5 = D0.AddDays(5);

            /* Five whole days of raw: 24 hours a day, two queries an hour, so a day holds 48 hourly buckets. */
            var collectionId = 1L;
            foreach (var day in new[] { D0, d1, d2, d3, d4 })
            {
                await InsertAsync(connection, day, 0, 23, 0, collectionId, ct);
                collectionId += 100_000;
            }

            /* The hourly over all five days; the daily over D0 to D2 and over D4, but NOT over D3. */
            await RefreshAsync(connection, hourly, D0, d5, ct);
            await RefreshAsync(connection, daily, D0, d3, ct);
            await RefreshAsync(connection, daily, d4, d5, ct);

            var hourlyMaterialization = await ResolveAsync(connection, hourly, ct);
            var dailyMaterialization = await ResolveAsync(connection, daily, ct);

            /* The defect: rows land in one hour of D2 below the hourly's watermark, and ONLY the hourly is refreshed. */
            await InsertAsync(connection, d2, 5, 5, 30, collectionId, ct);
            await RefreshAsync(connection, hourly, d2, d3, ct);

            /* Set up the two days the heal must NOT touch: D1 carries a sentinel a rebuild would erase, and D4's daily
               holds one MORE sample than its hourly (the daily outliving rows its source lost). */
            await ExecuteAsync(connection, $"UPDATE {dailyMaterialization} SET execution_count_sum = {SentinelExecutions} WHERE bucket = $1", d1, ct);
            await ExecuteAsync(connection, $"UPDATE {dailyMaterialization} SET sample_count = sample_count + 1 WHERE bucket = $1 AND query_hash = md5('heal0')", d4, ct);

            Assert.Equal((48L, 48L), (await SamplesAsync(connection, hourlyMaterialization, d1, ct), await SamplesAsync(connection, dailyMaterialization, d1, ct)));
            Assert.Equal((50L, 48L), (await SamplesAsync(connection, hourlyMaterialization, d2, ct), await SamplesAsync(connection, dailyMaterialization, d2, ct)));
            Assert.Equal((48L, 0L), (await SamplesAsync(connection, hourlyMaterialization, d3, ct), await SamplesAsync(connection, dailyMaterialization, d3, ct)));
            Assert.Equal((48L, 49L), (await SamplesAsync(connection, hourlyMaterialization, d4, ct), await SamplesAsync(connection, dailyMaterialization, d4, ct)));

            var now = D0.AddDays(12);

            /* Cancellation writes no marker. */
            using (var cancelled = new CancellationTokenSource())
            {
                await cancelled.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => PartialDailyHeal.RunAsync(connection, null, now, cancelled.Token));
            }

            Assert.Equal(0L, await MarkerCountAsync(connection, ct));

            /* First run. */
            var log = new CapturingTestLogger();
            var first = await PartialDailyHeal.RunAsync(connection, log, now, ct);

            var dailies = TimescaleSupport.PartialDailyHealOrder().Count;
            Assert.Equal(dailies, first.DailiesChecked);
            Assert.Equal(0, first.DailiesAlreadyDone);
            Assert.Equal(3, first.DaysCompared);   // D1, D2 and D4: D3 has no daily row to compare, D0 is the source's first day
            Assert.Equal(1, first.DaysPartial);
            Assert.Equal(1, first.DaysRefreshed);
            Assert.Equal(0, first.Failures);

            /* The partial day matches its hourly again... */
            Assert.Equal(50L, await SamplesAsync(connection, dailyMaterialization, d2, ct));
            Assert.Equal(await SamplesAsync(connection, hourlyMaterialization, d2, ct), await SamplesAsync(connection, dailyMaterialization, d2, ct));

            /* ...and nothing else was rebuilt: D1's sentinel and D4's extra sample stand, and D3 still has no daily row. */
            Assert.Equal(SentinelDayTotal, await SumAsync(connection, dailyMaterialization, "execution_count_sum", d1, ct));
            Assert.Equal(49L, await SamplesAsync(connection, dailyMaterialization, d4, ct));
            Assert.Equal(0L, await SamplesAsync(connection, dailyMaterialization, d3, ct));
            Assert.Equal(0L, await RowCountAsync(connection, dailyMaterialization, d3, ct));

            /* One Information line per refresh, naming the daily and the day, with a duration and both totals. */
            var refreshes = log.Lines.Where(l => l.Contains("Partial-daily heal (#4716): refreshed", StringComparison.Ordinal)).ToArray();
            var refreshLine = Assert.Single(refreshes);
            Assert.Contains(daily, refreshLine, StringComparison.Ordinal);
            Assert.Contains("2026-02-04", refreshLine, StringComparison.Ordinal);
            Assert.Contains("48 sample(s) against 50", refreshLine, StringComparison.Ordinal);
            Assert.Contains(" s ", refreshLine, StringComparison.Ordinal);
            Assert.Equal(0, log.CountAtLevel(Microsoft.Extensions.Logging.LogLevel.Warning));

            /* Every daily finished its walk (an empty source is a finished walk), so every marker is set. */
            Assert.Equal((long)dailies, await MarkerCountAsync(connection, ct));
            Assert.Equal(PartialDailyHeal.DoneStateValue, await MarkerValueAsync(connection, daily, ct));

            /* Second run: every daily is already done, nothing is compared, nothing is refreshed. */
            var secondLog = new CapturingTestLogger();
            var second = await PartialDailyHeal.RunAsync(connection, secondLog, now, ct);
            Assert.Equal(dailies, second.DailiesChecked);
            Assert.Equal(dailies, second.DailiesAlreadyDone);
            Assert.Equal(0, second.DaysCompared);
            Assert.Equal(0, second.DaysRefreshed);
            Assert.Equal(0, second.Failures);
            Assert.DoesNotContain("refreshed", secondLog.Joined, StringComparison.Ordinal);
            Assert.Equal(SentinelDayTotal, await SumAsync(connection, dailyMaterialization, "execution_count_sum", d1, ct));

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

    /// <summary>Raw rows for hours <paramref name="fromHour"/> to <paramref name="toHour"/> of <paramref name="day"/>: two queries an hour, at <paramref name="minute"/> past the hour.</summary>
    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime day, int fromHour, int toHour, int minute, long collectionIdBase, CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT $4::bigint + h * 10 + q, $1::timestamp + (h * interval '1 hour') + ($7::int * interval '1 minute'), $2::int, $3::text, 'HealDb', 100 + q, 1000 + q,
    'Regular', $1::timestamp, 'mod', md5('heal' || q), 10, 500, 300, 900, 700, 'PRIMARY', 9000 + h, $1::timestamp + (h * interval '1 hour')
FROM generate_series($5::int, $6::int) AS h CROSS JOIN generate_series(0, 1) AS q", connection);
        seed.Parameters.AddWithValue(day);
        seed.Parameters.AddWithValue(ServerId);
        seed.Parameters.AddWithValue(ServerName);
        seed.Parameters.AddWithValue(collectionIdBase);
        seed.Parameters.AddWithValue(fromHour);
        seed.Parameters.AddWithValue(toHour);
        seed.Parameters.AddWithValue(minute);
        await seed.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The MATERIALIZATION hypertable, not the view: a real-time view shows rows that are not materialized.</summary>
    private static async Task<string> ResolveAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var materialization = await TimescaleSupport.ResolveMaterializationAsync(connection, view, ct);
        Assert.NotNull(materialization);
        return $"\"{materialization.Value.Schema}\".\"{materialization.Value.Name}\"";
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, DateTime day, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(day);
        Assert.True(await command.ExecuteNonQueryAsync(ct) > 0);
    }

    private static async Task<long> SamplesAsync(NpgsqlConnection connection, string relation, DateTime day, CancellationToken ct) =>
        await SumAsync(connection, relation, "sample_count", day, ct);

    private static async Task<long> SumAsync(NpgsqlConnection connection, string relation, string column, DateTime day, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            $"SELECT coalesce(sum({column}), 0)::bigint FROM {relation} WHERE server_id = $1 AND bucket >= $2 AND bucket < $3", connection);
        read.Parameters.AddWithValue(ServerId);
        read.Parameters.AddWithValue(day);
        read.Parameters.AddWithValue(day.AddDays(1));
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct));
    }

    private static async Task<long> RowCountAsync(NpgsqlConnection connection, string relation, DateTime day, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            $"SELECT count(*) FROM {relation} WHERE server_id = $1 AND bucket >= $2 AND bucket < $3", connection);
        read.Parameters.AddWithValue(ServerId);
        read.Parameters.AddWithValue(day);
        read.Parameters.AddWithValue(day.AddDays(1));
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct));
    }

    private static async Task<long> MarkerCountAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT count(*) FROM collect.collector_state WHERE server_id = $1 AND collector_name = $2", connection);
        read.Parameters.AddWithValue(DarlingObservability.FleetServerId);
        read.Parameters.AddWithValue(PartialDailyHeal.StateCollectorName);
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct));
    }

    private static async Task<string?> MarkerValueAsync(NpgsqlConnection connection, string daily, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand(
            "SELECT state_value FROM collect.collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3", connection);
        read.Parameters.AddWithValue(DarlingObservability.FleetServerId);
        read.Parameters.AddWithValue(PartialDailyHeal.StateCollectorName);
        read.Parameters.AddWithValue(daily);
        return await read.ExecuteScalarAsync(ct) as string;
    }
}

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
/// #8801: on a window past the raw tier's four days <c>get_query_trend</c> answers every hour of the window. The hourly
/// rollup is materialized only up to its last refresh, so the hours from that ceiling to the window's end are summed
/// from the raw rows into the same buckets, and the series agrees with <c>get_query_fleet_summary</c> for the same
/// window and with the raw rows themselves.
///
/// <para>The store here holds one query under one handle with one collection five minutes into every hour (10
/// executions, 10 ms of CPU) and one more a minute into the newest hour (7 executions, 7 ms), so a total names its
/// hours. The hourly rollups are refreshed over a bounded range that stops two hours below the newest whole hour H:
/// the hours H-2 and H-1 are complete in raw and absent from the rollup, and the hour H is still filling. Every read
/// passes an explicit <c>as_of</c>, and each bound is the window's end, never the clock.</para>
///
/// <para><b>#1776 own-store</b> — each test mints a scratch database (it materializes continuous aggregates the
/// shared fixture must never inherit), so the class is deliberately NOT in the <c>live-postgres</c>
/// collection.</para>
/// </summary>
public sealed class QueryTrendRollupRawTailLiveTests
{
    private const int ServerId = -880101;
    private const string ServerName = "a8801-rollup-tail";
    private const string Db = "TailDb";
    private const string QueryHash = "0xQ8801";
    private const long HourExecutions = 10;
    private const long NewestHourExecutions = 7;
    private const long WorkerUsPerExecution = 1_000;
    private const string Stamp = "yyyy-MM-ddTHH:mm:ss";

    /// <summary>The hours seeded before the newest whole hour: past the widest window read here (169 hours and a few minutes).</summary>
    private const int SeededHours = 172;

    [Fact]
    public async Task AWindowPastTheRawTier_AnswersTheHoursTheRollupHasNotBuilt_AndAgreesWithTheFleetSummaryAndRaw()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        if (store is null)
        {
            return;
        }

        var bodySucceeded = false;
        try
        {
            var h = store.NewestWholeHour;
            await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);

            /* ── the window ends three minutes into the hour that is still filling ── */
            var end = h.AddMinutes(3);
            var trend = await ReadTrendAsync(postgres, end, 60, ct);
            Assert.Equal("hourly", trend.GetProperty("source").GetString());
            var points = trend.GetProperty("trend").EnumerateArray().ToArray();

            /* 165 hours from the rollup (H-167 to H-3: the window's first hour is cut, so its bucket starts before the
               window), then H-2 and H-1 and the filling hour H from raw. */
            Assert.Equal(168, points.Length);
            Assert.Equal(168, trend.GetProperty("data_points").GetInt32());
            Assert.Equal(h.AddHours(-167).ToString(Stamp, CultureInfo.InvariantCulture), StampOf(points[0]));
            AssertPoint(points[^3], h.AddHours(-2), HourExecutions);
            AssertPoint(points[^2], h.AddHours(-1), HourExecutions);
            AssertPoint(points[^1], h, NewestHourExecutions);
            AssertPoint(points[^4], h.AddHours(-3), HourExecutions);

            var expected = (165 * HourExecutions) + HourExecutions + HourExecutions + NewestHourExecutions;
            Assert.Equal(expected, points.Sum(p => p.GetProperty("execution_count").GetInt64()));
            Assert.Equal(expected, await RawExecutionsAsync(store.Connection, h.AddHours(-167), end, inclusiveEnd: true, ct));
            Assert.Equal(expected * WorkerUsPerExecution / 1000.0, points.Sum(p => p.GetProperty("cpu_ms").GetDouble()));

            var (fleetExecutions, fleetCpuMs) = await ReadFleetAsync(postgres, end, ct);
            Assert.Equal(expected, fleetExecutions);
            Assert.Equal(expected * WorkerUsPerExecution / 1000.0, fleetCpuMs);

            /* The hours read from raw keep the hourly tier's columns: the ones the rollup does not carry stay null in
               every point, and the note says where the newest hours come from. */
            foreach (var point in points[^3..])
            {
                Assert.Equal(JsonValueKind.Null, point.GetProperty("logical_reads").ValueKind);
                Assert.Equal(JsonValueKind.Null, point.GetProperty("collection_count").ValueKind);
                Assert.Equal(JsonValueKind.Null, point.GetProperty("query_plan_hash").ValueKind);
            }

            Assert.Contains("raw rows", trend.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);
            Assert.False(trend.GetProperty("window_truncated").GetBoolean());

            /* ── the window ends exactly on the hour, in daily buckets: the newest day holds H-2 and H-1 ── */
            var daily = await ReadTrendAsync(postgres, h, 1440, ct);
            var days = daily.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(168 * HourExecutions, days.Sum(p => p.GetProperty("execution_count").GetInt64()));
            for (var i = 0; i < days.Length; i++)
            {
                var from = DateTime.ParseExact(StampOf(days[i]), Stamp, CultureInfo.InvariantCulture);
                var raw = i + 1 < days.Length
                    ? await RawExecutionsAsync(
                        store.Connection, from, DateTime.ParseExact(StampOf(days[i + 1]), Stamp, CultureInfo.InvariantCulture), inclusiveEnd: false, ct)
                    : await RawExecutionsAsync(store.Connection, from, h, inclusiveEnd: true, ct);
                Assert.True(raw == days[i].GetProperty("execution_count").GetInt64(),
                    $"the day starting {StampOf(days[i])} reads {days[i].GetProperty("execution_count").GetInt64()} executions, raw holds {raw}");
            }

            var (dailyFleetExecutions, _) = await ReadFleetAsync(postgres, h, ct);
            Assert.Equal(168 * HourExecutions, dailyFleetExecutions);

            /* ── the window ends three minutes into H-1, an hour behind the rows the store holds: the raw rows are read to
               the window's end and no further, so H-2 is the last point and H-1's own collection (five past) is not in it ── */
            var earlierEnd = h.AddHours(-1).AddMinutes(3);
            var earlier = await ReadTrendAsync(postgres, earlierEnd, 60, ct);
            var earlierPoints = earlier.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(167, earlierPoints.Length);
            AssertPoint(earlierPoints[^1], h.AddHours(-2), HourExecutions);
            Assert.Equal(167 * HourExecutions, earlierPoints.Sum(p => p.GetProperty("execution_count").GetInt64()));
            var (earlierFleetExecutions, _) = await ReadFleetAsync(postgres, earlierEnd, ct);
            Assert.Equal(167 * HourExecutions, earlierFleetExecutions);

            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    /// <summary>
    /// The duration trend reads the rollup alone, so its note owes the reader the bound that holds: a refresh runs once
    /// an hour at the rollup's own minute and builds only the hours that ended an hour or more before it, so the series
    /// can end up to three hours behind the clock.
    /// </summary>
    [Fact]
    public async Task TheDurationTrendsHourlyNote_StatesTheBoundTheRollupHolds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        if (store is null)
        {
            return;
        }

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
            var json = await DarlingMcpTrendTools.GetQueryDurationTrend(
                postgres, ServerName, 168, AsOf(store.NewestWholeHour.AddMinutes(3)), cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.False(root.TryGetProperty("error", out _), json);
            Assert.Equal("hourly", root.GetProperty("source").GetString());

            var note = root.GetProperty("aggregate_note").GetString()!;
            Assert.DoesNotContain("up to two hours", note, StringComparison.Ordinal);
            Assert.Contains("up to three hours", note, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    private static string AsOf(DateTime end) => end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private static string StampOf(JsonElement point) => point.GetProperty("collection_time").GetString()![..19];

    private static void AssertPoint(JsonElement point, DateTime hour, long executions)
    {
        Assert.Equal(hour.ToString(Stamp, CultureInfo.InvariantCulture), StampOf(point));
        Assert.Equal(executions, point.GetProperty("execution_count").GetInt64());
        Assert.Equal(executions * WorkerUsPerExecution / 1000.0, point.GetProperty("cpu_ms").GetDouble());
    }

    /// <summary><c>get_query_trend</c> over the 168 hours ending at <paramref name="end"/>, at the named width.</summary>
    private static async Task<JsonElement> ReadTrendAsync(NpgsqlDataSource postgres, DateTime end, int bucketMinutes, CancellationToken ct)
    {
        /* #8812: the web viewer's overload, with no response byte budget: the tool's own 169 hourly points are cut to the budget, and this
           test reads every one of them to check the sums either side of the rollup's end. */
        var json = await DarlingMcpTrendTools.GetQueryTrend(
            postgres, QueryHash, Db, ServerName, 168, AsOf(end), bucketMinutes, TrendBudget.Mcp(TrendBuckets.QueryTrendMaxPoints), null, ct);
        var root = JsonDocument.Parse(json).RootElement;
        Assert.False(root.TryGetProperty("error", out _), json);
        Assert.True(root.TryGetProperty("trend", out _), json);
        return root;
    }

    /// <summary>The one server and database row <c>get_query_fleet_summary</c> gives for the 168 hours ending at <paramref name="end"/>.</summary>
    private static async Task<(long Executions, double CpuMs)> ReadFleetAsync(NpgsqlDataSource postgres, DateTime end, CancellationToken ct)
    {
        var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, QueryHash, 168, AsOf(end), cancellationToken: ct);
        var root = JsonDocument.Parse(json).RootElement;
        Assert.True(root.TryGetProperty("servers", out var servers), json);
        var row = Assert.Single(servers.EnumerateArray());
        Assert.Equal(ServerName, row.GetProperty("server_name").GetString());
        return (row.GetProperty("execution_count").GetInt64(), row.GetProperty("cpu_ms").GetDouble());
    }

    /// <summary>The executions the raw rows hold from <paramref name="from"/> to <paramref name="to"/>.</summary>
    private static async Task<long> RawExecutionsAsync(NpgsqlConnection connection, DateTime from, DateTime to, bool inclusiveEnd, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT COALESCE(SUM(delta_execution_count), 0)::bigint FROM collect.query_stats " +
            "WHERE server_id = $1 AND query_hash = $2 AND collection_time >= $3 AND collection_time " + (inclusiveEnd ? "<=" : "<") + " $4", connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(QueryHash);
        command.Parameters.AddWithValue(from);
        command.Parameters.AddWithValue(to);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// A scratch store with the hourly collections seeded and both hourly rollups refreshed up to two hours below
    /// <see cref="NewestWholeHour"/>. The background workers are stopped, so no policy run moves the ceiling.
    /// </summary>
    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(ScratchPostgres scratch, NpgsqlConnection connection, DateTime newestWholeHour)
        {
            _scratch = scratch;
            Connection = connection;
            NewestWholeHour = newestWholeHour;
        }

        public NpgsqlConnection Connection { get; }

        public string ConnectionString => _scratch.ConnectionString;

        /// <summary>H: the newest hour that is whole on the test's clock when the store is seeded, less one more hour so every row is in the past.</summary>
        public DateTime NewestWholeHour { get; }

        public static async Task<SeededStore?> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live query trend raw tail test.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            var created = false;
            try
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
                    "The hourly tier needs TimescaleDB: it is the rollup's continuous aggregate.");
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

                await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
                {
                    await stop.ExecuteNonQueryAsync(ct);
                }

                await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

                /* H is two whole days behind the test clock's hour: the reads' as_of stays behind the product's own clock when
                   DARLING_TEST_CLOCK_UTC runs ahead of it, and H keeps the hour of day the override is proving. */
                var now = LiveClock.Now();
                var h = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified).AddDays(-2);
                var firstHour = h.AddHours(-SeededHours);

                /* One collection five minutes into every hour from H-172 to H-1, and one a minute into H. */
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, min_dop, max_dop,
                          query_plan_hash, sample_interval_seconds)
                      SELECT $1 + g, $2 + g * INTERVAL '1 hour', $3, $4, $5, $6, '0xH8801', $7, $8, $9, 100, 100, 1, 1, '0xPLAN8801', 3600
                      FROM generate_series(0, $10) AS g",
                    CollectionIdGenerator.Next(), firstHour.AddMinutes(5), ServerId, ServerName, Db, QueryHash,
                    HourExecutions, HourExecutions * WorkerUsPerExecution, HourExecutions * 2 * WorkerUsPerExecution, SeededHours - 1);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, min_dop, max_dop,
                          query_plan_hash, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, $5, $6, '0xH8801', $7, $8, $9, 100, 100, 1, 1, '0xPLAN8801', 60)",
                    CollectionIdGenerator.Next(), h.AddMinutes(1), ServerId, ServerName, Db, QueryHash,
                    NewestHourExecutions, NewestHourExecutions * WorkerUsPerExecution, NewestHourExecutions * 2 * WorkerUsPerExecution);

                /* The rollups are built by the product's own refresh, over a range that stops at H-2: the ceiling. */
                await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
                foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView })
                {
                    await using var refresh = new NpgsqlCommand(
                        $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
                    refresh.Parameters.AddWithValue(firstHour);
                    refresh.Parameters.AddWithValue(h.AddHours(-2));
                    await refresh.ExecuteNonQueryAsync(ct);
                }

                created = true;
                return new SeededStore(scratch, connection, h);
            }
            finally
            {
                if (!created)
                {
                    await connection.DisposeAsync();
                    await scratch.DisposeAsync();
                }
            }
        }

        public Task CleanupAsync(bool bodySucceeded) =>
            LiveStoreCleanup.RunAsync(ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                Assert.Equal(0L, Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt), CultureInfo.InvariantCulture));
            });

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

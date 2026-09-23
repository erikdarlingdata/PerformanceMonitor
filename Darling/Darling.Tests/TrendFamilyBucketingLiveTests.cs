/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// #3960, proven against a live store: the rest of the trend family (wait, CPU, tempdb, memory's grant join is
/// covered by <see cref="DarlingMemoryTrendGrantJoinTests"/>, perfmon, the PostgreSQL query-duration pair) rolls
/// its collections up the way its notes say, on planted rows whose expected figures are worked out by hand in the
/// comments — <see cref="TrendBucketingLiveTests"/>'s twin for the five #3897 reads.
///
/// <para>What each fact holds:</para>
/// <list type="bullet">
/// <item><b>Wait</b> is time-weighted (summed wait over summed seconds, never an average of per-collection rates),
/// keeps the busiest collection as the peak, and a bucket with no RATED collection is left out entirely rather
/// than answered as a false zero.</item>
/// <item><b>CPU</b> buckets on the de-skewed sample's own clock, unclamped to the window's start — a sample whose
/// true time sits before the window (the collector's own de-skew can place it there) still lands in its own,
/// earlier bucket rather than being pulled forward.</item>
/// <item><b>tempdb</b> averages every space figure, but <c>sessions_using_tempdb</c> and the top consumer are the
/// bucket's busiest COLLECTION, never an average of two different sessions.</item>
/// <item><b>perfmon</b> shapes by the counter's kind: a rate sums its deltas per interval-completeness class (rated,
/// unknowable-zero, unrecorded-null) and keeps the busiest rated collection as <c>peak_per_second</c>; a gauge
/// averages the bucket and keeps the highest sample as <c>peak_value</c>, with no delta at all.</item>
/// <item><b>get_pg_query_duration_trend</b> recomputes its mean from the bucket's summed calls and time, never
/// averages the per-interval means, and keeps the costliest interval as <c>peak_mean_exec_ms</c>.</item>
/// </list>
/// </summary>
[Collection("live-postgres")]
public sealed class TrendFamilyBucketingLiveTests
{
    private const string ServerName = "trend-family-bucketing-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Two RATED collections in the 10:00 ten-minute bucket — 600 ms over 60 s (10 ms/s) and 2,400 ms over 30 s
    /// (80 ms/s, the peak) — plus one UNKNOWABLE collection alone in the 10:20 bucket (interval 0, a restart
    /// marker). The family bucket's rate is (600+2400)/(60+30) = 33.333 ms/s: time-weighted, not the average of
    /// 10 and 80 (45). The 10:20 bucket has no rated collection, so it does not appear at all — answered as
    /// nothing, never a fabricated zero.
    /// </summary>
    [Fact]
    public async Task WaitTrend_IsTimeWeighted_KeepsThePeak_AndDropsABucketWithNoRatedCollection()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var t1 = new DateTime(2026, 4, 6, 10, 1, 0);
            var t2 = new DateTime(2026, 4, 6, 10, 3, 0);
            var t3 = new DateTime(2026, 4, 6, 10, 21, 0);

            await SeedWaitAsync(connection, ct, t1, "PAGEIOLATCH_SH", deltaWaitMs: 600, deltaSignalMs: 60, interval: 60);
            await SeedWaitAsync(connection, ct, t2, "PAGEIOLATCH_SH", deltaWaitMs: 2400, deltaSignalMs: 240, interval: 30);
            await SeedWaitAsync(connection, ct, t3, "PAGEIOLATCH_SH", deltaWaitMs: 999, deltaSignalMs: 99, interval: 0);

            var root = Root(await DarlingMcpDataTools.GetWaitTrend(postgres, "PAGEIOLATCH_SH", ServerName, 1, "2026-04-06T10:30:00Z", bucket_minutes: 10));
            var trend = root.GetProperty("trend");
            Assert.Equal(1, trend.GetArrayLength());

            var bucket = trend[0];
            Assert.Equal(Stamp(new DateTime(2026, 4, 6, 10, 0, 0)), bucket.GetProperty("time").GetString());
            Assert.Equal(3000.0 / 90.0, bucket.GetProperty("wait_time_ms_per_second").GetDouble(), precision: 6);
            Assert.Equal(300.0 / 90.0, bucket.GetProperty("signal_wait_time_ms_per_second").GetDouble(), precision: 6);
            Assert.Equal(80.0, bucket.GetProperty("peak_wait_time_ms_per_second").GetDouble(), precision: 6);
        });
    }

    /// <summary>
    /// A sample at 09:22 and one at 09:26 — both inside the 09:20 ten-minute bucket — with a window that starts at
    /// 09:30 (the collector's de-skew can genuinely place a sample's own clock before the window collection_time
    /// falls in: the read's WHERE is on collection_time, the GROUP BY on the de-skewed sample_time). The bucket
    /// answers at 09:20, ten minutes before the window it was asked for — proof the read does not clamp a CPU
    /// bucket forward to the window's start the way the other bucketed reads do. sql_server_cpu averages 40 and 60
    /// to 50; peak_sql_server_cpu keeps 60.
    /// </summary>
    [Fact]
    public async Task CpuUtilization_BucketsOnItsOwnClock_Unclamped_AndAveragesWithThePeak()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var collectionTime = new DateTime(2026, 4, 6, 9, 31, 0);
            await SeedCpuAsync(connection, ct, collectionTime, sampleTime: new DateTime(2026, 4, 6, 9, 22, 0), sqlCpu: 40, otherCpu: 5);
            await SeedCpuAsync(connection, ct, collectionTime.AddMinutes(1), sampleTime: new DateTime(2026, 4, 6, 9, 26, 0), sqlCpu: 60, otherCpu: 5);

            var root = Root(await DarlingMcpDataTools.GetCpuUtilization(postgres, ServerName, 1, "2026-04-06T10:30:00Z", bucket_minutes: 10));
            var samples = root.GetProperty("samples");
            Assert.Equal(1, samples.GetArrayLength());

            var bucket = samples[0];
            Assert.Equal(Stamp(new DateTime(2026, 4, 6, 9, 20, 0)), bucket.GetProperty("sample_time").GetString());
            Assert.Equal(50, bucket.GetProperty("sql_server_cpu").GetInt32());
            Assert.Equal(60, bucket.GetProperty("peak_sql_server_cpu").GetInt32());
            Assert.Equal(2, bucket.GetProperty("samples_in_bucket").GetInt64());
        });
    }

    /// <summary>
    /// Two collections in one bucket: 100 MB reserved / 5 sessions / session 55 at 8 MB, then 300 MB / 9 sessions /
    /// session 77 at 20 MB. total_reserved_mb averages to 200 with 300 as the peak; sessions_using_tempdb is 9 (the
    /// busiest COLLECTION, never an average of two different session counts); the top consumer is session 77's 20
    /// MB, the single largest any collection in the bucket saw, not a sum or an average of the two sessions.
    /// </summary>
    [Fact]
    public async Task TempDbTrend_AveragesSpace_ButKeepsSessionsAndTopConsumerAsTheBusiestCollection()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var t1 = new DateTime(2026, 4, 6, 10, 1, 0);
            var t2 = new DateTime(2026, 4, 6, 10, 4, 0);

            await SeedTempDbAsync(connection, ct, t1, totalReserved: 100, versionStore: 10, sessions: 5, topSessionId: 55, topSessionMb: 8);
            await SeedTempDbAsync(connection, ct, t2, totalReserved: 300, versionStore: 50, sessions: 9, topSessionId: 77, topSessionMb: 20);

            var root = Root(await DarlingMcpDataTools.GetTempDbTrend(postgres, ServerName, 1, "2026-04-06T10:30:00Z", bucket_minutes: 10));
            var bucket = Assert.Single(root.GetProperty("trend").EnumerateArray());

            Assert.Equal(200.0, bucket.GetProperty("total_reserved_mb").GetDouble(), precision: 6);
            Assert.Equal(300.0, bucket.GetProperty("peak_total_reserved_mb").GetDouble(), precision: 6);
            Assert.Equal(50.0, bucket.GetProperty("peak_version_store_mb").GetDouble(), precision: 6);
            Assert.Equal(9, bucket.GetProperty("sessions_using_tempdb").GetInt64());
            Assert.Equal(77, bucket.GetProperty("top_consumer_session_id").GetInt32());
            Assert.Equal(20.0, bucket.GetProperty("top_consumer_mb").GetDouble(), precision: 6);
        });
    }

    /// <summary>
    /// A rate counter ("Batch Requests/sec"): one RATED collection (900 ms delta over 300 s, 3/s) and one twice as
    /// busy (1,200 over 200 s, 6/s, the peak) share the 10:00 bucket; a THIRD collection at 10:11 — the only one in
    /// its bucket — is UNKNOWABLE (interval 0, a counter reset). The rated bucket's delta is the summed 2,100 over
    /// the summed 500 s (4.2/s if divided, though the tool leaves that division to the caller), with 6 as
    /// peak_per_second; the unknowable bucket reports its own delta with sample_interval_seconds 0 — the marker a
    /// caller must not divide by — rather than folding into, or being hidden by, the rated bucket beside it.
    /// </summary>
    [Fact]
    public async Task PerfmonTrend_Rate_SumsDeltasPerIntervalClass_AndKeepsThePeakPerSecond()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const string counter = "Batch Requests/sec";
            var t1 = new DateTime(2026, 4, 6, 10, 1, 0);
            var t2 = new DateTime(2026, 4, 6, 10, 4, 0);
            var t3 = new DateTime(2026, 4, 6, 10, 11, 0);

            await SeedPerfmonAsync(connection, ct, t1, counter, cntrValue: 100_000, deltaCntrValue: 900, interval: 300, cntrType: PerfmonCounterTypes.PerfCounterBulkCount);
            await SeedPerfmonAsync(connection, ct, t2, counter, cntrValue: 101_200, deltaCntrValue: 1200, interval: 200, cntrType: PerfmonCounterTypes.PerfCounterBulkCount);
            await SeedPerfmonAsync(connection, ct, t3, counter, cntrValue: 101_300, deltaCntrValue: 100, interval: 0, cntrType: PerfmonCounterTypes.PerfCounterBulkCount);

            var root = Root(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, counter, ServerName, 1, "2026-04-06T10:30:00Z", bucket_minutes: 10));
            var points = root.GetProperty("trend");
            Assert.Equal(2, points.GetArrayLength());

            var rated = points[0];
            Assert.Equal(Stamp(new DateTime(2026, 4, 6, 10, 0, 0)), rated.GetProperty("time").GetString());
            Assert.Equal(2100, rated.GetProperty("delta_value").GetInt64());
            Assert.Equal(500, rated.GetProperty("sample_interval_seconds").GetInt64());
            Assert.Equal(6.0, rated.GetProperty("peak_per_second").GetDouble(), precision: 6);
            Assert.Equal(101_200, rated.GetProperty("value").GetInt64());

            var unknowable = points[1];
            Assert.Equal(Stamp(new DateTime(2026, 4, 6, 10, 10, 0)), unknowable.GetProperty("time").GetString());
            Assert.Equal(100, unknowable.GetProperty("delta_value").GetInt64());
            Assert.Equal(0, unknowable.GetProperty("sample_interval_seconds").GetInt64());
        });
    }

    /// <summary>
    /// A GAUGE counter classified by its STORED type (<c>PerfCounterRawCount</c>, the same shape Lite.Tests'
    /// <c>DeltaSeriesShapingTests</c> uses for "Memory Grants Pending" — a name is never evidence of a gauge on
    /// its own, per <c>DeltaSeriesShaping.BasisFor</c>'s own remarks): two collections at 4 and 8 in the same
    /// bucket average to 6, with 8 kept as
    /// <c>peak_value</c>, and <c>delta_value</c> / <c>sample_interval_seconds</c> stay null — a level has no delta
    /// to divide, bucketed or not.
    /// </summary>
    [Fact]
    public async Task PerfmonTrend_Gauge_AveragesTheBucket_KeepsThePeakValue_AndPublishesNoDelta()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const string counter = "Memory Grants Pending";
            var t1 = new DateTime(2026, 4, 6, 10, 1, 0);
            var t2 = new DateTime(2026, 4, 6, 10, 4, 0);

            await SeedPerfmonAsync(connection, ct, t1, counter, cntrValue: 4, deltaCntrValue: null, interval: null, cntrType: PerfmonCounterTypes.PerfCounterRawCount);
            await SeedPerfmonAsync(connection, ct, t2, counter, cntrValue: 8, deltaCntrValue: null, interval: null, cntrType: PerfmonCounterTypes.PerfCounterRawCount);

            var root = Root(await DarlingMcpTrendTools.GetPerfmonTrend(postgres, counter, ServerName, 1, "2026-04-06T10:30:00Z", bucket_minutes: 10));
            Assert.Equal("gauge", root.GetProperty("counter_kind").GetString());
            var bucket = Assert.Single(root.GetProperty("trend").EnumerateArray());

            Assert.Equal(6.0, bucket.GetProperty("value").GetDouble(), precision: 6);
            Assert.Equal(8, bucket.GetProperty("peak_value").GetInt64());
            Assert.Equal(JsonValueKind.Null, bucket.GetProperty("delta_value").ValueKind);
            Assert.Equal(JsonValueKind.Null, bucket.GetProperty("sample_interval_seconds").ValueKind);
        });
    }

    /// <summary>
    /// Two rated intervals in one bucket: 1,000 calls / 2,000 ms (2.0 ms mean) and 500 calls / 3,000 ms (6.0 ms
    /// mean, the peak). The bucket's mean is the summed 5,000 ms over the summed 1,500 calls = 3.333 ms — between
    /// the two, never their 4.0 average, which would weight the quieter interval as heavily as the busier one.
    /// first_mean_exec_ms and last_mean_exec_ms are this single bucket's own first and last ran interval, 2.0 and
    /// 6.0, because the tool reads its window ends from the served buckets' own carried fields, not recomputed.
    /// </summary>
    [Fact]
    public async Task PgQueryDurationTrend_Bucketed_RecomputesTheMean_AndKeepsThePeakFirstAndLast()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            const long queryId = 555_666_777L;
            var t1 = new DateTime(2026, 4, 6, 10, 1, 0);
            var t2 = new DateTime(2026, 4, 6, 10, 4, 0);

            await SeedPgStatementAsync(connection, ct, t1, queryId, deltaCalls: 1000, deltaTotalExecMs: 2000, interval: 60);
            await SeedPgStatementAsync(connection, ct, t2, queryId, deltaCalls: 500, deltaTotalExecMs: 3000, interval: 60);

            var root = Root(await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(postgres, ServerName, queryId.ToString(), 1, "2026-04-06T10:30:00Z", bucket_minutes: 10));
            var bucket = Assert.Single(root.GetProperty("points").EnumerateArray());

            Assert.Equal(1500, bucket.GetProperty("calls").GetInt64());
            /* The tool rounds mean_exec_ms to 3 places (Math.Round), so the assertion matches THAT figure,
               not the unrounded 5000.0 / 1500.0 = 3.3333... */
            Assert.Equal(Math.Round(5000.0 / 1500.0, 3), bucket.GetProperty("mean_exec_ms").GetDouble(), precision: 6);
            Assert.Equal(6.0, bucket.GetProperty("peak_mean_exec_ms").GetDouble(), precision: 6);
            Assert.Equal(2.0, root.GetProperty("first_mean_exec_ms").GetDouble(), precision: 6);
            Assert.Equal(6.0, root.GetProperty("last_mean_exec_ms").GetDouble(), precision: 6);
        });
    }

    /* ───────────────────────────── plumbing ───────────────────────────── */

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend-family-bucketing tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task SeedWaitAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string waitType, long deltaWaitMs, long deltaSignalMs, int? interval) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, waitType, deltaWaitMs, deltaSignalMs, 1L, interval);

    /// <summary>Sets <c>sample_time_utc</c> directly (V134, #3653 item 13) rather than the raw <c>sample_time</c>
    /// the collector writes: <see cref="DarlingDataReader.CpuUtilizationSql"/> COALESCEs to it first, ahead of a
    /// quantised de-skew this fixture has no reason to fight, so the seeded value is exactly the bucketing key.</summary>
    private static async Task SeedCpuAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, DateTime sampleTime, int sqlCpu, int otherCpu) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $5, $6, $7)",
            CollectionIdGenerator.Next(), collectionTime, ServerId, ServerName, sampleTime, sqlCpu, otherCpu);

    private static async Task SeedTempDbAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, decimal totalReserved, decimal versionStore, long sessions, int topSessionId, decimal topSessionMb) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO tempdb_stats
    (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb,
     version_store_reserved_mb, total_reserved_mb, unallocated_mb, total_sessions_using_tempdb, top_session_id, top_session_tempdb_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, 10m, 5m, versionStore, totalReserved, 1000m - totalReserved, sessions, topSessionId, topSessionMb);

    private static async Task SeedPerfmonAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string counterName, long cntrValue, long? deltaCntrValue, int? interval, int? cntrType = null) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1, $2, $3, $4, 'SQLServer:SQL Statistics', $5, '', $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, counterName, cntrValue, deltaCntrValue, interval, cntrType);

    private static async Task SeedPgStatementAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, long queryId, long deltaCalls, double deltaTotalExecMs, int? interval) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, delta_calls, delta_total_exec_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, queryId, deltaCalls, deltaTotalExecMs, interval);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "wait_stats", "cpu_utilization_stats", "tempdb_stats", "perfmon_stats", "pg_statement_stats", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }

    /// <summary>The store's own stamp format (<c>TrendPayloads.Stamp</c>): round-trip ("o"), which always carries
    /// seven fractional digits even on a whole second.</summary>
    private static string Stamp(DateTime value) => value.ToString("o", System.Globalization.CultureInfo.InvariantCulture);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;
}

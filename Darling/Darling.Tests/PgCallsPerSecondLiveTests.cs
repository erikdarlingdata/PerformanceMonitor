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
/// #7566, against a live store: a PostgreSQL statement's calls per second divides by the seconds the collector's
/// runs covered in the bucket, not by the seconds of only the runs where the statement had a row. The collector
/// skips idle rows at the write, so a statement that runs a batch every 5 minutes has a row in one run of five.
/// The old read gave that statement 29 / 60 per second in the run it moved and a pair of buckets at 58 / 120; the
/// rate over the time is 58 / 600 per 10-minute bucket.
///
/// <para>The same read feeds the web chart, <c>get_pg_query_duration_trend</c> and the Viewer, so each fact reads it
/// through the reader and through the tool's parsed JSON.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgCallsPerSecondLiveTests
{
    private const string ServerName = "pg-calls-per-second-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const long QueryId = 7566;
    private static readonly DateTime T0 = new(2026, 3, 6, 10, 0, 0);
    private const string AsOfText = "2026-03-06T12:00:00Z";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Runs every minute from 09:50 to 10:29, then none until 11:35 (an outage past the collector's gap policy),
    /// then every minute from 11:35 to 11:44. The statement has 29 calls in the runs at :00 and :05 of 10:00 to
    /// 10:25 and nothing else. Expected, per 10-minute bucket:
    /// 09:50 runs but no calls (zero, 9 rated runs: the window's first run has no previous one), 10:00 / 10:10 /
    /// 10:20 two batches over 600 s, 10:30 to 11:20 no run so no bucket, 11:30 and 11:40 runs but no calls (zero).
    /// </summary>
    [Fact]
    public async Task CallsPerSecond_DividesByTheTimeTheRunsCovered_ReadsZeroWhenItDidNotRun_AndLeavesAGapAbsent()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0.AddMinutes(-10); t < T0.AddMinutes(30); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            for (var t = T0.AddMinutes(95); t < T0.AddMinutes(105); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            for (var t = T0; t < T0.AddMinutes(30); t = t.AddMinutes(5))
            {
                await SeedStatementAsync(connection, ct, t, deltaCalls: 29, deltaMs: 290.0);
            }

            var start = new DateTime(2026, 3, 6, 9, 0, 0);
            var end = new DateTime(2026, 3, 6, 12, 0, 0);

            var points = await DarlingPgTrendReader.GetQueryDurationBucketsAsync(postgres, ServerId, QueryId, start, end, 10, ct);
            var root = JsonDocument.Parse(await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(
                postgres, ServerName, QueryId.ToString(CultureInfo.InvariantCulture), 3, AsOfText, 10, TrendBudget.Chart, cancellationToken: ct)).RootElement;
            var tool = root.GetProperty("points").EnumerateArray().ToArray();

            var expected = new (string Start, long Calls, double Rate)[]
            {
                ("09:50", 0, 0.0),
                ("10:00", 58, 58.0 / 600.0),
                ("10:10", 58, 58.0 / 600.0),
                ("10:20", 58, 58.0 / 600.0),
                ("11:30", 0, 0.0),
                ("11:40", 0, 0.0),
            };

            /* The rate itself, first: 58 calls over a 600 s bucket is 0.0967 per second. The old read divided by only the
               runs the statement had a row in (120 s) and drew 0.4833. */
            Assert.Equal(58.0 / 600.0, points.First(p => p.Calls > 0).CallsPerSecond, 9);
            Assert.Equal(expected.Length, points.Count);
            Assert.Equal(expected.Length, tool.Length);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i].Start, points[i].CollectionTimeUtc.ToString("HH:mm", CultureInfo.InvariantCulture));
                Assert.Equal(expected[i].Calls, points[i].Calls);
                Assert.Equal(expected[i].Rate, points[i].CallsPerSecond, 9);

                Assert.Equal(expected[i].Start, DateTime.Parse(tool[i].GetProperty("collection_time").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal).ToString("HH:mm", CultureInfo.InvariantCulture));
                Assert.Equal(expected[i].Calls, tool[i].GetProperty("calls").GetInt64());
                Assert.Equal(Math.Round(expected[i].Rate, 4), tool[i].GetProperty("calls_per_second").GetDouble(), 9);
            }

            /* A bucket where the statement did not run has no mean: absent, not 0 ms. */
            Assert.Equal(JsonValueKind.Null, tool[0].GetProperty("mean_exec_ms").ValueKind);
            Assert.Equal(10.0, tool[1].GetProperty("mean_exec_ms").GetDouble(), 6);
        });
    }

    /// <summary>A statement that never ran in the window is an empty answer, not a row of zeros over every run.</summary>
    [Fact]
    public async Task AStatementWithNoSnapshotInTheWindow_ReadsNothing()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddMinutes(20); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            var points = await DarlingPgTrendReader.GetQueryDurationBucketsAsync(
                postgres, ServerId, QueryId, new DateTime(2026, 3, 6, 9, 0, 0), new DateTime(2026, 3, 6, 12, 0, 0), 10, ct);
            Assert.Empty(points);
        });
    }

    /// <summary>
    /// The first run after an outage past the collector's gap policy has no trustworthy length: the time since the
    /// run before it is the outage, not the interval the statement's calls were counted over. Its calls and its
    /// seconds are both left out of the bucket, so the bucket's rate is what the steady runs around it give.
    /// Runs 10:00 to 10:09, an outage, then runs 11:31 to 11:39. The 11:31 run carries 29 calls and the 11:35 run 7.
    /// </summary>
    [Fact]
    public async Task TheFirstRunAfterAnOutage_ThatCarriesCalls_IsLeftOutOfItsBucketsCallsAndSeconds()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddMinutes(10); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            for (var t = T0.AddMinutes(91); t < T0.AddMinutes(100); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            await SeedStatementAsync(connection, ct, T0.AddMinutes(91), deltaCalls: 29, deltaMs: 290.0);
            await SeedStatementAsync(connection, ct, T0.AddMinutes(95), deltaCalls: 7, deltaMs: 70.0);

            var bucket = await ReadBucketAsync(postgres, ct, "11:30");

            /* Eight rated runs (11:32 to 11:39) cover 480 s. The 29 calls of the run after the outage are not in the bucket
               and neither are the 82 minutes since the run before it: with the gap check gone this reads 36 calls over
               roughly 5,400 s. */
            Assert.Equal(7, bucket.Calls);
            Assert.Equal(7.0 / 480.0, bucket.CallsPerSecond, 9);
            Assert.Equal(8, bucket.Intervals);
        });
    }

    /// <summary>
    /// A restart snapshot (<c>sample_interval_seconds = 0</c>) has no interval to divide by. Its calls and its
    /// seconds are both left out of the bucket, so the rate is what the steady runs around it give.
    /// Runs 10:00 to 10:09 each with 10 calls, except the 10:05 restart snapshot, which carries 500.
    /// </summary>
    [Fact]
    public async Task ARestartSnapshotInASteadyRun_ThatCarriesCalls_IsLeftOutOfItsBucketsCallsAndSeconds()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddMinutes(10); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
                var restart = t == T0.AddMinutes(5);
                await SeedStatementAsync(connection, ct, t, deltaCalls: restart ? 500 : 10, deltaMs: 100.0, intervalSeconds: restart ? 0 : 60);
            }

            var bucket = await ReadBucketAsync(postgres, ct, "10:00");

            /* Nine steady runs cover 540 s and carry 90 calls. With the restart arm gone the 10:05 run is rated at the
               60 s gap and the bucket reads 590 calls over 600 s. */
            Assert.Equal(90, bucket.Calls);
            Assert.Equal(90.0 / 540.0, bucket.CallsPerSecond, 9);
            Assert.Equal(9, bucket.Intervals);
        });
    }

    /// <summary>
    /// #7661: the web chart asks for about one point per 2 px of its plot. A day asked for 300 points comes back as 5-minute buckets
    /// (289 points) instead of the 1-minute ones the full chart budget gives (1,440); a bucket's rate is still its calls over its seconds,
    /// and one slow minute still shows in the bucket's peak while its mean is far lower.
    /// </summary>
    [Fact]
    public async Task APlotSizedAsk_ForADay_GivesWiderBuckets_WithATrueRate_AndAPeakThatKeepsAOneMinuteSpike()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            for (var t = T0; t < T0.AddHours(24); t = t.AddMinutes(1))
            {
                await SeedRunAsync(connection, ct, t);
            }

            for (var t = T0; t < T0.AddHours(24); t = t.AddMinutes(5))
            {
                await SeedStatementAsync(connection, ct, t, deltaCalls: 29, deltaMs: 290.0);
            }

            // The spike: one minute, one call, 5 s, between two of the batches.
            var spikeAt = T0.AddHours(12).AddMinutes(1);
            await SeedStatementAsync(connection, ct, spikeAt, deltaCalls: 1, deltaMs: 5000.0);

            async Task<(JsonElement[] Points, int BucketMinutes)> ReadAsync(TrendBudget budget)
            {
                var root = JsonDocument.Parse(await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(
                    postgres, ServerName, QueryId.ToString(CultureInfo.InvariantCulture), 24, "2026-03-07T10:00:00Z", null, budget, cancellationToken: ct)).RootElement;
                return (root.GetProperty("points").EnumerateArray().ToArray(), root.GetProperty("bucket_minutes").GetInt32());
            }

            var (full, fullMinutes) = await ReadAsync(TrendBudget.Chart);
            var (sized, sizedMinutes) = await ReadAsync(TrendBudget.ChartFor(300));

            Assert.Equal(1, fullMinutes);
            Assert.Equal(5, sizedMinutes);
            Assert.InRange(sized.Length, 250, 300);
            Assert.True(full.Length > 1000, $"the full chart budget gave {full.Length} points");

            // A bucket with one batch: 29 calls over the 300 s the runs covered.
            var midday = sized.First(p => p.GetProperty("collection_time").GetDateTime().TimeOfDay == TimeSpan.FromHours(14));
            Assert.Equal(29, midday.GetProperty("calls").GetInt64());
            Assert.Equal(Math.Round(29.0 / 300.0, 4), midday.GetProperty("calls_per_second").GetDouble(), 9);

            // The bucket with the spike: peak is the 5 s call, mean is the bucket's total over its calls.
            var spike = sized.First(p => p.GetProperty("collection_time").GetDateTime().TimeOfDay == TimeSpan.FromHours(22));
            Assert.Equal(5000.0, spike.GetProperty("peak_mean_exec_ms").GetDouble(), 3);
            Assert.True(spike.GetProperty("mean_exec_ms").GetDouble() < 300.0, "the spike is not the mean");
        });
    }

    private static async Task<(long Calls, double CallsPerSecond, long Intervals)> ReadBucketAsync(
        NpgsqlDataSource postgres, CancellationToken ct, string start)
    {
        var from = new DateTime(2026, 3, 6, 9, 0, 0);
        var to = new DateTime(2026, 3, 6, 12, 0, 0);
        var points = await DarlingPgTrendReader.GetQueryDurationBucketsAsync(postgres, ServerId, QueryId, from, to, 10, ct);
        var point = Assert.Single(points, p => p.CollectionTimeUtc.ToString("HH:mm", CultureInfo.InvariantCulture) == start);

        var root = JsonDocument.Parse(await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(
            postgres, ServerName, QueryId.ToString(CultureInfo.InvariantCulture), 3, AsOfText, 10, TrendBudget.Chart, cancellationToken: ct)).RootElement;
        var tool = Assert.Single(root.GetProperty("points").EnumerateArray().ToArray(),
            p => DateTime.Parse(p.GetProperty("collection_time").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal)
                     .ToString("HH:mm", CultureInfo.InvariantCulture) == start);
        Assert.Equal(point.Calls, tool.GetProperty("calls").GetInt64());
        Assert.Equal(Math.Round(point.CallsPerSecond, 4), tool.GetProperty("calls_per_second").GetDouble(), 9);

        return (point.Calls, point.CallsPerSecond, point.Intervals);
    }

    private static async Task SeedRunAsync(NpgsqlConnection connection, CancellationToken ct, DateTime start) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) VALUES ($1, $2, $3, 'pg_statement_stats', $4, 1500, 'SUCCESS', 0)",
            CollectionIdGenerator.Next(), ServerId, ServerName, start.AddSeconds(2));

    private static async Task SeedStatementAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, long deltaCalls, double deltaMs, int intervalSeconds = 60) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, calls, total_exec_time_ms, min_exec_time_ms, max_exec_time_ms, mean_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, temp_blks_read, temp_blks_written, blk_read_time_ms, blk_write_time_ms, wal_records, wal_fpi, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, 1, 1, TRUE, 10, 100.0, 1.0, 20.0, 10.0, 100, 100, 10, 0, 0, 0, 0, 1.0, 0.0, 0, 0, 0, $6, $7, 50, $8)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, QueryId, deltaCalls, deltaMs, intervalSeconds);

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL calls-per-second tests.");

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

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "collect.collection_log", "pg_statement_stats", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}

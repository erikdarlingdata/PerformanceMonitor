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
/// #3897, proven against a live store: the bucketed trends roll their collections up the way their notes say, on
/// planted rows whose every figure is worked out by hand in the comments. The SQL pins say what the statements
/// look like; these say what they answer — which is where a ratio of averages or a summed interval would show.
///
/// <para>What each fact holds:</para>
/// <list type="bullet">
/// <item><b>File I/O</b> ranks its series on the window's I/O stall, folds everything past the fifth into one
/// "(other)" line POOLED COLLECTION BY COLLECTION before bucketing, recomputes latency as the bucket's summed stall
/// over its summed operations (null over none), keeps the worst single collection as the peak, and charts one
/// database per file when asked.</item>
/// <item><b>The duration trend</b> weights its collections by the seconds they covered, leaves an unknowable
/// collection out of both sums while counting it, and stamps the first point at the window's start while
/// <c>effective_start</c> names the first collection.</item>
/// <item><b>The PostgreSQL pair</b> sums each bucket's intervals and recomputes its rates and ratios from the sums,
/// keeps the worst interval, and answers the same window figures at any width.</item>
/// </list>
/// </summary>
[Collection("live-postgres")]
public sealed class TrendBucketingLiveTests
{
    private const string ServerName = "trend-bucketing-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Seven (database, file type) series over two collections one minute apart, all in the 10:00 ten-minute
    /// bucket, plus one collection at 10:12. Stall ranks them A-ROWS, A-LOG, B, C, D, then E and F folded.
    /// </summary>
    [Fact]
    public async Task FileIoTrend_RanksByStall_FoldsPerCollection_AndRecomputesLatencyFromSums()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var t1 = new DateTime(2026, 3, 4, 10, 1, 0);
            var t2 = t1.AddMinutes(1);
            var t3 = new DateTime(2026, 3, 4, 10, 12, 0);

            /* dbA's data files are TWO files, pooled into one (dbA, ROWS) line: at t1 a1 read 100 with 500 ms of
               stall and a2 100 with 100 ms, so the line read 200 at 3 ms each — never the average of 5 and 1
               weighted as equals by accident, and never a max of 5. */
            await SeedFileAsync(connection, ct, t1, "dbA", "a1.mdf", "ROWS", reads: 100, writes: 0, stallRead: 500, stallWrite: 0);
            await SeedFileAsync(connection, ct, t1, "dbA", "a2.ndf", "ROWS", reads: 100, writes: 0, stallRead: 100, stallWrite: 0);
            await SeedFileAsync(connection, ct, t2, "dbA", "a1.mdf", "ROWS", reads: 200, writes: 0, stallRead: 200, stallWrite: 0);
            await SeedFileAsync(connection, ct, t1, "dbA", "a.ldf", "LOG", reads: 0, writes: 100, stallRead: 0, stallWrite: 500);
            await SeedFileAsync(connection, ct, t1, "dbB", "b.mdf", "ROWS", reads: 100, writes: 0, stallRead: 400, stallWrite: 0);
            await SeedFileAsync(connection, ct, t1, "dbC", "c.mdf", "ROWS", reads: 100, writes: 0, stallRead: 300, stallWrite: 0);
            await SeedFileAsync(connection, ct, t1, "dbD", "d.mdf", "ROWS", reads: 100, writes: 0, stallRead: 250, stallWrite: 0);

            /* The two folded series. At t1, E read 10 with 100 ms (10 ms each) and F 90 with 90 ms (1 ms each):
               pooled per collection that is 190 ms over 100 reads, 1.9 ms — the (other) line's latency at t1, and
               its peak is the worse of t1's 1.9 and t2's pooled 1.0, NOT the 10 ms one member hit alone. */
            await SeedFileAsync(connection, ct, t1, "dbE", "e.mdf", "ROWS", reads: 10, writes: 0, stallRead: 100, stallWrite: 0);
            await SeedFileAsync(connection, ct, t1, "dbF", "f.mdf", "ROWS", reads: 90, writes: 0, stallRead: 90, stallWrite: 0);
            await SeedFileAsync(connection, ct, t2, "dbE", "e.mdf", "ROWS", reads: 50, writes: 0, stallRead: 50, stallWrite: 0);

            /* dbB at 10:12 only wrote: its 10:10 bucket has a write latency and NO read latency — null, never the
               0.00 ms an idle bucket used to be printed as. */
            await SeedFileAsync(connection, ct, t3, "dbB", "b.mdf", "ROWS", reads: 0, writes: 10, stallRead: 0, stallWrite: 50);

            /* A restart row (a stored interval of 0, #3540) carrying a garbage delta ranks nothing and charts
               nothing: without the filter it would put dbZ first. */
            await SeedFileAsync(connection, ct, t2, "dbZ", "z.mdf", "ROWS", reads: 1_000_000, writes: 0, stallRead: 99_000_000, stallWrite: 0, interval: 0);

            var root = Root(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, "2026-03-04T11:00:00Z", bucket_minutes: 10));

            Assert.Equal("database_file_type", root.GetProperty("series_grain").GetString());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("database_name").ValueKind);
            Assert.Equal("10 minutes", root.GetProperty("bucket").GetString());
            Assert.Equal(10, root.GetProperty("bucket_minutes").GetInt32());
            Assert.Equal(7, root.GetProperty("series_active").GetInt32());
            Assert.Equal(5, root.GetProperty("series_charted").GetInt32());
            Assert.Equal(2, root.GetProperty("series_folded").GetInt32());

            /* The legend, in rank order: stall 800, 500, 400 + 50, 300, 250, then the fold. */
            var legend = root.GetProperty("series").EnumerateArray().ToArray();
            Assert.Equal(
                new[] { "dbA|ROWS", "dbA|LOG", "dbB|ROWS", "dbC|ROWS", "dbD|ROWS", "(other)|(other)" },
                legend.Select(s => s.GetProperty("database_name").GetString() + "|" + s.GetProperty("file_type").GetString()).ToArray());
            Assert.Equal(2, legend[0].GetProperty("files").GetInt32());
            Assert.Equal(400, legend[0].GetProperty("reads").GetInt64());
            Assert.Equal(2.0, legend[0].GetProperty("avg_read_latency_ms").GetDouble(), 6);   // 800 ms / 400 reads
            Assert.Equal(3.0, legend[0].GetProperty("peak_read_latency_ms").GetDouble(), 6);  // t1's pooled 600 / 200
            Assert.Equal(2, legend[5].GetProperty("files").GetInt32());
            Assert.Equal(150, legend[5].GetProperty("reads").GetInt64());
            Assert.Equal(1.6, legend[5].GetProperty("avg_read_latency_ms").GetDouble(), 6);   // 240 ms / 150 reads
            Assert.Equal(1.9, legend[5].GetProperty("peak_read_latency_ms").GetDouble(), 6);

            var trend = root.GetProperty("trend").EnumerateArray().ToArray();
            JsonElement Point(string database, string fileType, string stamp) =>
                Assert.Single(trend, p => p.GetProperty("database_name").GetString() == database
                                          && p.GetProperty("file_type").GetString() == fileType
                                          && p.GetProperty("time").GetString()!.StartsWith(stamp, StringComparison.Ordinal));

            var aRows = Point("dbA", "ROWS", "2026-03-04T10:00:00");
            Assert.Equal(400, aRows.GetProperty("reads").GetInt64());
            Assert.Equal(2.0, aRows.GetProperty("avg_read_latency_ms").GetDouble(), 6);
            Assert.Equal(3.0, aRows.GetProperty("peak_read_latency_ms").GetDouble(), 6);
            Assert.Equal(JsonValueKind.Null, aRows.GetProperty("avg_write_latency_ms").ValueKind);
            Assert.False(aRows.TryGetProperty("file_name", out _), "the per-(database, file type) grain names no file");

            var other = Point("(other)", "(other)", "2026-03-04T10:00:00");
            Assert.Equal(150, other.GetProperty("reads").GetInt64());
            Assert.Equal(1.6, other.GetProperty("avg_read_latency_ms").GetDouble(), 6);
            Assert.Equal(1.9, other.GetProperty("peak_read_latency_ms").GetDouble(), 6);

            var bWrites = Point("dbB", "ROWS", "2026-03-04T10:10:00");
            Assert.Equal(0, bWrites.GetProperty("reads").GetInt64());
            Assert.Equal(JsonValueKind.Null, bWrites.GetProperty("avg_read_latency_ms").ValueKind);
            Assert.Equal(5.0, bWrites.GetProperty("avg_write_latency_ms").GetDouble(), 6);

            Assert.DoesNotContain(trend, p => p.GetProperty("database_name").GetString() == "dbZ");
            Assert.Equal(7, trend.Length);   // six lines at 10:00, dbB alone at 10:10

            /* Left to size itself, one hour over six lines is two-minute buckets: 31 x 6 = 186 points, inside 200. */
            var auto = Root(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, "2026-03-04T11:00:00Z"));
            Assert.Equal(2, auto.GetProperty("bucket_minutes").GetInt32());
            Assert.Contains("near 200 points", auto.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);

            /* ── scoped to one database: its files, one line each ── */
            var scoped = Root(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, "2026-03-04T11:00:00Z", bucket_minutes: 10, database_name: "dbA"));
            Assert.Equal("file", scoped.GetProperty("series_grain").GetString());
            Assert.Equal("dbA", scoped.GetProperty("database_name").GetString());
            Assert.Equal(0, scoped.GetProperty("series_folded").GetInt32());
            Assert.Equal(
                new[] { "a1.mdf", "a.ldf", "a2.ndf" },
                scoped.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("file_name").GetString()).ToArray());
            var a1 = Assert.Single(scoped.GetProperty("trend").EnumerateArray(), p => p.GetProperty("file_name").GetString() == "a1.mdf");
            Assert.Equal(300, a1.GetProperty("reads").GetInt64());
            Assert.Equal(700.0 / 300.0, a1.GetProperty("avg_read_latency_ms").GetDouble(), 2);
            Assert.Equal(5.0, a1.GetProperty("peak_read_latency_ms").GetDouble(), 6);

            /* A name the store never collected is answered about the NAME. */
            var missing = Root(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, "2026-03-04T11:00:00Z", database_name: "dbNope"));
            Assert.Equal("empty", missing.GetProperty("status").GetString());
            Assert.Contains("'dbNope'", missing.GetProperty("message").GetString()!, StringComparison.Ordinal);
            Assert.Contains("get_file_io_stats lists them", missing.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* A width the read cannot serve is refused before anything is read — on a window with no rows too. */
            var refusedEmpty = await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, "2026-02-01T00:00:00Z", bucket_minutes: 0);
            Assert.True(McpHelpers.IsRefusalEnvelope(refusedEmpty));
            var overCap = await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 24, "2026-03-04T11:00:00Z", bucket_minutes: 1);
            Assert.True(McpHelpers.IsRefusalEnvelope(overCap));
            Assert.Contains("across 6 series", McpHelpers.ErrorMessageOf(overCap), StringComparison.Ordinal);
        });
    }

    /// <summary>Six active series draw six lines: folding would pool a single series under a worse name.</summary>
    [Fact]
    public async Task FileIoTrend_SixSeries_AreAllCharted()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var t = new DateTime(2026, 3, 4, 12, 30, 0);
            foreach (var (db, stall) in new[] { ("d1", 600), ("d2", 500), ("d3", 400), ("d4", 300), ("d5", 200), ("d6", 100) })
            {
                await SeedFileAsync(connection, ct, t, db, db + ".mdf", "ROWS", reads: 100, writes: 0, stallRead: stall, stallWrite: 0);
            }

            var root = Root(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1, "2026-03-04T13:00:00Z"));
            Assert.Equal(6, root.GetProperty("series_active").GetInt32());
            Assert.Equal(6, root.GetProperty("series_charted").GetInt32());
            Assert.Equal(0, root.GetProperty("series_folded").GetInt32());
            Assert.DoesNotContain(root.GetProperty("trend").EnumerateArray(), p => p.GetProperty("database_name").GetString() == TrendPayloads.OtherLabel);
            Assert.Contains("Every line that did I/O in the window is shown", root.GetProperty("series_note").GetString()!, StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// The raw duration trend, time-weighted. Recent instants (the raw tier holds four days), aligned to ten
    /// minutes. In the first bucket: a 60-second collection at 100 ms/s, a 120-second one at 10 ms/s, and a
    /// restart (stored interval 0) whose fabricated delta must not count. Time-weighted that bucket is
    /// (6,000 + 1,200) ms over 180 s = 40 ms/s; averaging the two rates would say 55, and letting the restart in
    /// would say anything. The second bucket holds only a restart; the third a pre-V128 collection rated over
    /// the gap since the one before.
    /// </summary>
    [Fact]
    public async Task DurationTrend_IsTimeWeighted_LeavesTheUnknowableOut_AndStampsTheFirstPointAtTheWindowStart()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var now = DateTime.UtcNow;
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute / 10 * 10, 0).AddMinutes(-20);

            await SeedQueryAsync(connection, ct, end.AddMinutes(-63), executions: 1, elapsedMs: 60, interval: 60);
            await SeedQueryAsync(connection, ct, end.AddMinutes(-58), executions: 60, elapsedMs: 6_000, interval: 60);
            await SeedQueryAsync(connection, ct, end.AddMinutes(-56), executions: 12, elapsedMs: 1_200, interval: 120);
            await SeedQueryAsync(connection, ct, end.AddMinutes(-55), executions: 99_999, elapsedMs: 999_000, interval: 0);
            await SeedQueryAsync(connection, ct, end.AddMinutes(-45), executions: 5, elapsedMs: 500, interval: 0);
            await SeedQueryAsync(connection, ct, end.AddMinutes(-35), executions: 6, elapsedMs: 600, interval: null);

            var asOf = end.ToString("yyyy-MM-ddTHH:mm:ss") + "Z";
            var root = Root(await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, hours_back: 1, as_of: asOf, bucket_minutes: 10));
            Assert.Equal("raw", root.GetProperty("source").GetString());
            Assert.Equal("10 minutes", root.GetProperty("bucket").GetString());

            var trend = root.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(3, trend.Length);

            Assert.StartsWith(Stamp(end.AddMinutes(-60)), trend[0].GetProperty("time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(40.0, trend[0].GetProperty("elapsed_ms_per_second").GetDouble(), 9);
            Assert.Equal(0.4, trend[0].GetProperty("executions_per_second").GetDouble(), 9);
            Assert.Equal(100.0, trend[0].GetProperty("peak_elapsed_ms_per_second").GetDouble(), 9);

            /* Nothing rated in the second bucket: an unrated point, never 0. */
            Assert.StartsWith(Stamp(end.AddMinutes(-50)), trend[1].GetProperty("time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(JsonValueKind.Null, trend[1].GetProperty("elapsed_ms_per_second").ValueKind);
            Assert.Equal(JsonValueKind.Null, trend[1].GetProperty("peak_elapsed_ms_per_second").ValueKind);

            /* The pre-V128 collection, rated over the ten minutes since the restart before it: 600 ms / 600 s. */
            Assert.Equal(1.0, trend[2].GetProperty("elapsed_ms_per_second").GetDouble(), 9);

            Assert.Equal(1, root.GetProperty("unrated_points").GetInt32());
            Assert.Equal(2, root.GetProperty("unrated_collections").GetInt32());
            Assert.Equal(Stamp(end.AddMinutes(-58)), root.GetProperty("effective_start").GetString()![..19]);

            /* A window whose start is not a bucket boundary: the 10:57-style collection's bucket began before the
               window did, so its point is stamped at the window's start — and effective_start is still the
               collection itself. */
            var shifted = Root(await DarlingMcpTrendTools.GetQueryDurationTrend(
                postgres, ServerName, hours_back: 1, as_of: end.AddMinutes(-5).ToString("yyyy-MM-ddTHH:mm:ss") + "Z", bucket_minutes: 10));
            var first = shifted.GetProperty("trend")[0];
            Assert.StartsWith(Stamp(end.AddMinutes(-65)), first.GetProperty("time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(1.0, first.GetProperty("elapsed_ms_per_second").GetDouble(), 9);
            Assert.Equal(Stamp(end.AddMinutes(-63)), shifted.GetProperty("effective_start").GetString()![..19]);
        });
    }

    /// <summary>
    /// get_pg_io_trend bucketed: four intervals after a first snapshot (60, 90, 90 and 960 seconds), the first
    /// three in the 10:00 bucket. Hand-worked: 1,650 reads over 240 s = 6.875 reads/s, the busiest interval 11/s;
    /// 2,550 ms over 1,650 reads = 1.545 ms, the slowest interval 2 ms; 600 hits over 2,250 accesses = 26.67%.
    /// And the window figures do not move with the width.
    /// </summary>
    [Fact]
    public async Task PgIoTrend_SumsTheIntervals_RecomputesFromTheSums_AndTheWindowFiguresIgnoreTheWidth()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var t0 = new DateTime(2026, 3, 4, 10, 0, 0);
            var times = new[] { t0, t0.AddSeconds(60), t0.AddSeconds(150), t0.AddSeconds(240), t0.AddSeconds(1_200) };

            /* Cumulative counters, as the collector stores them. relation: +600/+90/+900/+10 reads at 1, 1, 2 and
               2 ms each; temp relation: +60 reads in the first interval, at 1 ms. */
            long[] relReads = { 1_000, 1_600, 1_690, 2_590, 2_600 };
            double[] relReadMs = { 100, 700, 790, 2_590, 2_610 };
            long[] relHits = { 10_000, 10_400, 10_500, 10_600, 20_600 };
            long[] tmpReads = { 0, 60, 60, 60, 60 };
            double[] tmpReadMs = { 0, 60, 60, 60, 60 };

            for (var i = 0; i < times.Length; i++)
            {
                await SeedPgIoAsync(connection, ct, times[i], "relation", relReads[i], relReadMs[i], relHits[i], writes: 50 + 10 * i);
                await SeedPgIoAsync(connection, ct, times[i], "temp relation", tmpReads[i], tmpReadMs[i], hits: 0, writes: 0);
            }

            var asOf = "2026-03-04T11:00:00Z";
            var tenMinutes = Root(await DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName, "client backend", "normal", 2, asOf, bucket_minutes: 10));
            Assert.Equal("10 minutes", tenMinutes.GetProperty("bucket").GetString());
            Assert.Equal(2, tenMinutes.GetProperty("point_count").GetInt32());
            Assert.Equal(4, tenMinutes.GetProperty("interval_count").GetInt32());

            var points = tenMinutes.GetProperty("points").EnumerateArray().ToArray();
            Assert.StartsWith("2026-03-04T10:00:00", points[0].GetProperty("collection_time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(240.0, points[0].GetProperty("interval_seconds").GetDouble(), 6);
            Assert.Equal(6.875, points[0].GetProperty("reads_per_second").GetDouble(), 3);
            Assert.Equal(11.0, points[0].GetProperty("peak_reads_per_second").GetDouble(), 3);
            Assert.Equal(2_550.0 / 1_650.0, points[0].GetProperty("avg_read_ms").GetDouble(), 3);
            Assert.Equal(2.0, points[0].GetProperty("peak_read_ms").GetDouble(), 3);
            Assert.Equal(600.0 / 2_250.0 * 100, points[0].GetProperty("cache_hit_pct").GetDouble(), 2);
            Assert.StartsWith("2026-03-04T10:20:00", points[1].GetProperty("collection_time").GetString()!, StringComparison.Ordinal);
            Assert.Equal(10.0 / 960.0, points[1].GetProperty("reads_per_second").GetDouble(), 3);

            /* The window figures are about INTERVALS, so a one-minute width — every interval its own point —
               answers them identically. */
            var oneMinute = Root(await DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName, "client backend", "normal", 2, asOf, bucket_minutes: 1));
            Assert.Equal(4, oneMinute.GetProperty("point_count").GetInt32());
            foreach (var key in new[]
            {
                "interval_count", "counter_reset_count", "total_reads", "total_writes", "total_extends", "total_hits",
                "total_read_bytes", "total_write_bytes", "peak_reads_per_second", "write_counters_tracked",
                "io_timing_tracked", "bytes_source",
            })
            {
                Assert.Equal(oneMinute.GetProperty(key).GetRawText(), tenMinutes.GetProperty(key).GetRawText());
            }

            Assert.Equal(1_660, tenMinutes.GetProperty("total_reads").GetInt64());
            Assert.Equal(11.0, tenMinutes.GetProperty("peak_reads_per_second").GetDouble(), 3);
        });
    }

    /// <summary>
    /// get_pg_database_trend bucketed: the 10:02 interval falls off a cliff (10% cache hits) and spills 6 MB in
    /// 60 s, inside a 10:00 bucket whose pooled ratio is 63.3%. The bucket keeps the cliff as its worst interval and
    /// the spill as its peak, and the window figures — the worst interval and when, the spilling intervals, the
    /// peak spill — are the same at any width.
    /// </summary>
    [Fact]
    public async Task PgDatabaseTrend_KeepsTheWorstInterval_AndTheWindowFiguresIgnoreTheWidth()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            var t0 = new DateTime(2026, 3, 4, 10, 0, 0);

            /* Cumulative (commit, rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks). */
            var rows = new (DateTime T, long Commit, long Rollback, long Read, long Hit, long TempFiles, long TempBytes, long Deadlocks)[]
            {
                (t0,                1_000, 10,   100,   900, 0,         0, 0),
                (t0.AddMinutes(1),  1_600, 10,   200, 1_800, 0,         0, 0),   // 90% hit
                (t0.AddMinutes(2),  1_660, 70, 1_100, 1_900, 2, 6_000_000, 0),   // 10% hit, 100,000 B/s spill
                (t0.AddMinutes(3),  2_260, 70, 1_200, 2_800, 2, 6_000_000, 0),   // 90% hit
                (t0.AddMinutes(25), 2_270, 70, 1_210, 2_890, 3, 7_320_000, 1),   // 1,320 s: 1,000 B/s, one deadlock
            };
            foreach (var r in rows)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', $5, $6, $7, $8, $9, $10, $11, NULL)",
                    CollectionIdGenerator.Next(), r.T, ServerId, ServerName, r.Commit, r.Rollback, r.Read, r.Hit, r.TempFiles, r.TempBytes, r.Deadlocks);
            }

            var asOf = "2026-03-04T11:00:00Z";
            var tenMinutes = Root(await DarlingMcpPgTrendTools.GetPgDatabaseTrend(postgres, ServerName, "appdb", 2, asOf, bucket_minutes: 10));
            var points = tenMinutes.GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(2, points.Length);

            /* 10:00 bucket: hits 1,900 over 3,000 accesses; 1,320 transactions over 180 s; 60 of them rolled back. */
            Assert.Equal(1_900.0 / 3_000.0 * 100, points[0].GetProperty("cache_hit_pct").GetDouble(), 2);
            Assert.Equal(10.0, points[0].GetProperty("worst_cache_hit_pct").GetDouble(), 2);
            Assert.Equal(1_320.0 / 180.0, points[0].GetProperty("transactions_per_second").GetDouble(), 3);
            Assert.Equal(60.0 / 1_320.0 * 100, points[0].GetProperty("rollback_pct").GetDouble(), 2);
            Assert.Equal(100_000.0, points[0].GetProperty("peak_temp_bytes_per_second").GetDouble(), 1);
            Assert.Equal(6_000_000.0 / 180.0, points[0].GetProperty("temp_bytes_per_second").GetDouble(), 1);
            Assert.Equal(1, points[1].GetProperty("deadlocks").GetInt64());

            Assert.Equal(10.0, tenMinutes.GetProperty("worst_interval_cache_hit_pct").GetDouble(), 2);
            Assert.StartsWith("2026-03-04T10:02:00", tenMinutes.GetProperty("worst_interval_at").GetString()!, StringComparison.Ordinal);
            Assert.Equal(2, tenMinutes.GetProperty("intervals_with_temp_files").GetInt32());
            Assert.Equal(4, tenMinutes.GetProperty("interval_count").GetInt32());

            var oneMinute = Root(await DarlingMcpPgTrendTools.GetPgDatabaseTrend(postgres, ServerName, "appdb", 2, asOf, bucket_minutes: 1));
            Assert.Equal(4, oneMinute.GetProperty("point_count").GetInt32());
            foreach (var key in new[]
            {
                "interval_count", "counter_reset_count", "intervals_with_temp_files", "total_temp_files", "total_temp_bytes",
                "total_deadlocks", "cache_hit_pct_window", "worst_interval_cache_hit_pct", "worst_interval_at",
                "peak_temp_bytes_per_second", "xact_commit", "xact_rollback",
            })
            {
                Assert.Equal(oneMinute.GetProperty(key).GetRawText(), tenMinutes.GetProperty(key).GetRawText());
            }
        });
    }

    /* ───────────────────────────── plumbing ───────────────────────────── */

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend-bucketing tests.");

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

    private static async Task SeedFileAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string database, string file, string fileType,
        long reads, long writes, long stallRead, long stallWrite, int? interval = 60) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
     physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
     delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, database, file, fileType,
            "D:\\data\\" + file, 1000m, reads, writes, reads * 8192, writes * 8192, stallRead, stallWrite, interval);

    private static async Task SeedQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, long executions, long elapsedMs, int? interval) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, execution_count, total_worker_time, total_elapsed_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'Sales', '0xBUCKETHASH', '0xPLANHASH', '0xSQLH', '0xPLANH', 'SELECT 1', 0, 0, 0, $5, 0, $6, $7)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, executions, elapsedMs * 1000, interval);

    private static async Task SeedPgIoAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string objectType, long reads, double readTimeMs, long hits, long writes) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, extends, op_bytes, hits, evictions, stats_reset)
VALUES ($1, $2, $3, $4, 'client backend', $5, 'normal', $6, $7, $8, 0, 0, 8192, $9, 0, NULL)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, objectType, reads, readTimeMs, writes, hits);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "file_io_stats", "query_stats", "pg_io_stats", "pg_database_stats", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-ddTHH:mm:ss");

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;
}

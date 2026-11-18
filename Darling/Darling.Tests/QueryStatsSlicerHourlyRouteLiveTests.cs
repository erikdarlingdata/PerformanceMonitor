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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Top Queries slicer reads the tier the grid below it reads. A window the grid routes to the hourly rollup reads the io
/// hourly rollup, with the partial hours at the window's edges (and any hour past the rollup's ceiling) read from raw, and every
/// column equals what the raw read gave, bucket by bucket. Against real rollups, seeded for three days over two databases.
///
/// <para>The oracle is a copy of the statement as it was before #8549 (raw <c>query_stats</c> only), run over the same rows while
/// raw is still there. Then raw is purged for the window, so the hourly rollup is all that is left: the route is proved by the
/// strip still showing the week, which the old statement (raw only) cannot. A window the io rollup does not cover, and a store
/// without the io rollup, keep the raw read, so with raw gone they show nothing, never a strip of zeros.</para>
///
/// <para><b>#1776 own-store</b> — the test mints a scratch database (it materializes continuous aggregates the shared fixture
/// must never inherit), so the class is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStatsSlicerHourlyRouteLiveTests
{
    private const int ServerId = -85492;
    private const string ServerName = "slicer-hourly-route";
    private const int Hashes = 6;

    /// <summary>A fixed anchor, never wall-clock relative: old enough that the grid's router sends every window below to the hourly rollup.</summary>
    private static readonly DateTime D = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The statement before #8549, kept as the oracle: raw <c>query_stats</c> only, same bounds and bucket.</summary>
    private const string OldSlicerSql = $$"""
        SELECT
            date_bin(CAST($5 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}) AS bucket,
            COUNT(DISTINCT query_hash) AS query_count,
            COALESCE(SUM(delta_worker_time), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(delta_elapsed_time), 0) / 1000.0 AS total_elapsed_ms,
            COALESCE(SUM(delta_logical_reads), 0) AS total_reads,
            COALESCE(SUM(delta_logical_writes), 0) AS total_writes,
            COALESCE(SUM(delta_physical_reads), 0) AS total_physical_reads
        FROM query_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        GROUP BY 1
        ORDER BY bucket
        """;

    [Theory]
    [InlineData("2026-01-06T02:00", "2026-01-07T20:00", "", "2026-01-06T02:00|2026-01-07T20:00")]
    [InlineData("2026-01-06T02:20", "2026-01-07T20:40", "", "2026-01-06T03:00|2026-01-07T20:00")]
    [InlineData("2026-01-06T02:20", "2026-01-07T20:40", "2026-01-07T12:00", "2026-01-06T03:00|2026-01-07T12:00")]
    [InlineData("2026-01-06T02:20", "2026-01-07T20:40", "2026-01-08T00:00", "2026-01-06T03:00|2026-01-07T20:00")]
    [InlineData("2026-01-06T05:10", "2026-01-06T05:50", "", "none")]
    [InlineData("2026-01-06T05:10", "2026-01-06T06:20", "", "none")]
    [InlineData("2026-01-06T05:30", "2026-01-06T07:20", "", "2026-01-06T06:00|2026-01-06T07:00")]
    [InlineData("2026-01-06T05:30", "2026-01-06T07:20", "2026-01-06T06:00", "none")]
    public void WholeHours_AreTheHoursTheRollupCanAnswerExactly(string start, string end, string ceiling, string expected)
    {
        DateTime? ceil = ceiling.Length == 0 ? null : DateTime.Parse(ceiling, System.Globalization.CultureInfo.InvariantCulture);
        var whole = ViewerDataService.SlicerWholeHours(
            DateTime.Parse(start, System.Globalization.CultureInfo.InvariantCulture), DateTime.Parse(end, System.Globalization.CultureInfo.InvariantCulture), ceil);

        if (expected == "none")
        {
            Assert.Null(whole);
            return;
        }

        var parts = expected.Split('|');
        Assert.Equal(DateTime.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture), whole!.Value.Start);
        Assert.Equal(DateTime.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture), whole.Value.End);
    }

    [Fact]
    public async Task Slicer_ReadsTheHourlyRollupTheGridReads_AndEveryBucketEqualsTheRawRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live slicer hourly-route test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live slicer hourly-route test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct);

            /* Windows the grid routes hourly: whole hours; edges inside an hour; a window of two hours or less that holds a whole hour. */
            var aligned = (Start: D.AddDays(1).AddHours(2), End: D.AddDays(2).AddHours(20));
            var unaligned = (Start: D.AddDays(1).AddHours(2).AddMinutes(20), End: D.AddDays(2).AddHours(20).AddMinutes(40));
            var shortWithHour = (Start: D.AddDays(1).AddHours(5).AddMinutes(30), End: D.AddDays(1).AddHours(7).AddMinutes(20));
            var noWholeHour = (Start: D.AddDays(1).AddHours(5).AddMinutes(10), End: D.AddDays(1).AddHours(5).AddMinutes(50));
            var beforeFloor = (Start: D, End: D.AddDays(2));

            /* ── phase 1: the rollups are materialized only up to day 3 at 12:00 (a ceiling inside both windows' ends): the hours after
               it must come from raw. ── */
            await RefreshAllAsync(connection, D, D.AddDays(2).AddHours(12), ct);
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                await AssertEqualsRawAsync(viewer, connection, aligned.Start, aligned.End, null, 60, ct);
                await AssertEqualsRawAsync(viewer, connection, unaligned.Start, unaligned.End, null, 60, ct);
                await AssertEqualsRawAsync(viewer, connection, unaligned.Start, unaligned.End, ["DbA"], 60, ct);
                await AssertEqualsRawAsync(viewer, connection, beforeFloor.Start, beforeFloor.End, null, 60, ct);
            }

            /* ── phase 2: materialized to the end of the data. ── */
            await RefreshAllAsync(connection, D, D.AddDays(4), ct);
            await using var viewer2 = new ViewerDataService(scratch.ConnectionString);
            await AssertEqualsRawAsync(viewer2, connection, aligned.Start, aligned.End, null, 60, ct);
            await AssertEqualsRawAsync(viewer2, connection, unaligned.Start, unaligned.End, null, 60, ct);
            await AssertEqualsRawAsync(viewer2, connection, unaligned.Start, unaligned.End, ["DbA"], 60, ct);
            await AssertEqualsRawAsync(viewer2, connection, unaligned.Start, unaligned.End, ["DbB"], 60, ct);
            /* A window of two hours or less takes a minute bucket on the raw read, and the hour on the hourly route (a rollup has no finer
               grain): the same totals, by the hour. */
            await AssertEqualsRawAsync(viewer2, connection, shortWithHour.Start, shortWithHour.End, null, 60, ct);
            /* No whole hour in the window: the read stays on raw, at the minute width the strip draws. */
            await AssertEqualsRawAsync(viewer2, connection, noWholeHour.Start, noWholeHour.End, null, 1, ct);
            /* The io rollup starts at the first hour that holds data, an hour after this window's start: it does not cover it, so the read is raw. */
            await AssertEqualsRawAsync(viewer2, connection, beforeFloor.Start, beforeFloor.End, null, 60, ct);

            /* ── phase 3: purge raw for the window, so the hourly rollup is all that is left. ── */
            var expectedAligned = Lines(await RawOracleAsync(connection, aligned.Start, aligned.End, null, 60, ct));
            Assert.NotEmpty(expectedAligned);
            var expectedUnaligned = Lines(await RawOracleAsync(connection, unaligned.Start, unaligned.End, null, 60, ct));
            await using (var purge = new NpgsqlCommand(
                "DELETE FROM collect.query_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection))
            {
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(D);
                purge.Parameters.AddWithValue(D.AddDays(4));
                await purge.ExecuteNonQueryAsync(ct);
            }

            await using var viewer3 = new ViewerDataService(scratch.ConnectionString);
            /* The old statement reads nothing now; the strip still shows the window, bucket for bucket, from the rollup. */
            Assert.Empty(await RawOracleAsync(connection, aligned.Start, aligned.End, null, 60, ct));
            Assert.Equal(expectedAligned, Lines(await viewer3.GetQueryStatsSlicerDataAsync(ServerId, aligned.Start, aligned.End, cancellationToken: ct)));

            /* Edges inside an hour: raw is gone, so the rollup's whole hours are what the strip can show (the partial hours at the edges are not). */
            var wholeStart = SlicerBuckets.Ceiling(unaligned.Start, 60);
            var wholeEnd = SlicerBuckets.Floor(unaligned.End, 60);
            var onlyWhole = expectedUnaligned.Where(l => l.Bucket >= wholeStart && l.Bucket < wholeEnd).ToList();
            Assert.Equal(onlyWhole, Lines(await viewer3.GetQueryStatsSlicerDataAsync(ServerId, unaligned.Start, unaligned.End, cancellationToken: ct)));

            /* A window the io rollup does not cover keeps the raw read, which is empty now: never a strip of zeros for the missing columns. */
            Assert.Empty(await viewer3.GetQueryStatsSlicerDataAsync(ServerId, beforeFloor.Start, beforeFloor.End, cancellationToken: ct));

            /* A store without the io rollup: the covering window keeps the raw read as well. */
            await using (var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW collect.{TimescaleSupport.QueryStatsIoHourlyView}", connection))
            {
                await drop.ExecuteNonQueryAsync(ct);
            }

            await using var viewer4 = new ViewerDataService(scratch.ConnectionString);
            Assert.Empty(await viewer4.GetQueryStatsSlicerDataAsync(ServerId, aligned.Start, aligned.End, cancellationToken: ct));

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

    private static async Task AssertEqualsRawAsync(
        ViewerDataService viewer, NpgsqlConnection connection, DateTime start, DateTime end, IReadOnlyList<string>? databases, int minutes, CancellationToken ct)
    {
        var expected = Lines(await RawOracleAsync(connection, start, end, databases, minutes, ct));
        Assert.NotEmpty(expected);
        var actual = Lines(await viewer.GetQueryStatsSlicerDataAsync(ServerId, start, end, databases, ct));
        Assert.Equal(expected, actual);
    }

    /// <summary>The statement before #8549, over raw rows, at the bucket width given.</summary>
    private static async Task<List<TimeSliceBucket>> RawOracleAsync(
        NpgsqlConnection connection, DateTime start, DateTime end, IReadOnlyList<string>? databases, int minutes, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(OldSlicerSql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(end);
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(databases));
        command.Parameters.AddWithValue(minutes);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await ViewerDataService.ReadSlicerBucketsAsync(reader, ct);
    }

    private static List<(DateTime Bucket, long Count, double Cpu, double Elapsed, double Reads, double Writes, double Physical)> Lines(List<TimeSliceBucket> buckets) =>
        [.. buckets.Select(b => (b.BucketTime, b.SessionCount, b.TotalCpu, b.TotalElapsed, b.TotalReads, b.TotalWrites, b.TotalPhysicalReads))];

    /// <summary>
    /// Three days of samples from hour 1 (so the io rollup's first bucket is an hour after D): six query hashes over two databases, most
    /// hours holding most hashes, a second sample for two of them on the hour (a row stamped exactly on an hour boundary), and a row with no
    /// physical reads. Every row has a real sample interval, the rows the rollup keeps.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        for (var hour = 1; hour < 72; hour++)
        {
            for (var k = 0; k < Hashes; k++)
            {
                if ((hour + k) % 3 == 0)
                {
                    continue;
                }

                await PlantAsync(connection, D.AddHours(hour).AddMinutes((7 * k + hour) % 60).AddSeconds(k), hour, k, ct);
                if (k < 2)
                {
                    await PlantAsync(connection, D.AddHours(hour).AddMinutes(k == 0 ? 59 : 0), hour + 100, k, ct);
                }
            }
        }
    }

    private static async Task PlantAsync(NpgsqlConnection connection, DateTime at, int salt, int k, CancellationToken ct)
    {
        var cpu = (salt * 31L + k * 7 + 1) * 100;
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
     min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(at);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(k < 3 ? "DbA" : "DbB");
        insert.Parameters.AddWithValue("0xSLQ" + k);
        insert.Parameters.AddWithValue("0xSLH" + k);
        insert.Parameters.AddWithValue(cpu);
        insert.Parameters.AddWithValue(cpu * 9 / 10);
        insert.Parameters.AddWithValue(3L + k);
        insert.Parameters.AddWithValue(3600);
        insert.Parameters.AddWithValue(salt * 1_000L + k * 17 + 5);
        insert.Parameters.AddWithValue(salt % 13L + k);
        insert.Parameters.Add(new NpgsqlParameter { Value = (salt + k) % 11 == 0 ? DBNull.Value : salt % 29L + k });
        insert.Parameters.AddWithValue(5L);
        insert.Parameters.AddWithValue(0L);
        insert.Parameters.AddWithValue(10L);
        insert.Parameters.AddWithValue(20L);
        insert.Parameters.AddWithValue(10L);
        insert.Parameters.AddWithValue(20L);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAllAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        foreach (var view in new[] { TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIoHourlyView })
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
            refresh.Parameters.AddWithValue(from);
            refresh.Parameters.AddWithValue(to);
            await refresh.ExecuteNonQueryAsync(ct);
        }
    }
}

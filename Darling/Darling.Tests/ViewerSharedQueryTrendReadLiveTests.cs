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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549 on a live store: the Performance Trends tab draws the Query Duration and Executions charts from one
/// query-stats read, <see cref="ViewerDataService.GetQueryDurationAndExecutionCountTrendsAsync"/>. Each test plants a
/// seed, asks for the pair and for the two stand-alone reads over the same window, and requires the pair's execution
/// series to be the one <see cref="ViewerDataService.GetExecutionCountTrendAsync"/> returns (on the raw tier that read
/// runs <see cref="ViewerDataService.ExecutionCountTrendSql"/>) and its duration series to be the one
/// <see cref="ViewerDataService.GetQueryDurationTrendAsync"/> returns: the same tier and coverage, the same instants,
/// the same values. The raw tier is covered with one collection per bucket and with merged buckets, the hourly rollup
/// with a week of hours; each with no database filter, a filter that matches and a filter that matches nothing.
/// Every test mints its own scratch database (the <c>ViewerTrendRoutingLivePostgresTests</c> pattern), so the class
/// is not in the <c>live-postgres</c> collection.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE and drop its own scratch database through ScratchPostgres. */
public sealed class ViewerSharedQueryTrendReadLiveTests
{
    private const int ServerId = -854901;

    private static readonly string[] OneDatabase = { "DbB" };
    private static readonly string[] NoSuchDatabase = { "NoSuchDb" };

    /// <summary>
    /// Seven collections, five minutes apart and off the minute grid, in a 92-minute window: every bucket holds one
    /// collection, so each point is stamped at its collection's own time. The seed carries each state of the interval
    /// rule: the store's first collection (rated over its stored 300 s), a restart collection (every row stores 0,
    /// unrated), a collection 3,900 s after its previous one (outside the collector's 3,600 s gap policy, unrated), and
    /// collections the second database has no row in.
    /// </summary>
    [Fact]
    public async Task RawTier_OneCollectionPerBucket_TheSharedReadServesTheStandAloneExecutionSeries()
    {
        var baseConnectionString = StoreOrSkip();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var t0 = new DateTime(2026, 3, 10, 9, 0, 37);
        await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    $1 + row_number() OVER (),
    $2::timestamp + v.offset_seconds * INTERVAL '1 second',
    $3,
    'shared-trend-read',
    v.database_name,
    decode(md5('q' || v.database_name), 'hex'),
    decode(md5('h' || v.database_name), 'hex'),
    1000,
    v.elapsed_us,
    v.executions,
    v.interval_seconds
FROM (VALUES
    (   0, 'DbA',  30,  3000000, 300),
    (   0, 'DbB',  60,  9000000, 300),
    ( 300, 'DbA',  45,  1500000, 300),
    ( 600, 'DbA',  10,  1000000,   0),
    ( 600, 'DbB',  10,  1000000,   0),
    ( 900, 'DbA', 600, 60000000, 300),
    ( 900, 'DbB', 300, 30000000, 300),
    (4800, 'DbA',  50,  5000000, 300),
    (5100, 'DbA',   1,   100000, 300),
    (5400, 'DbB', 150, 12000000, 300)
) AS v(offset_seconds, database_name, executions, elapsed_us, interval_seconds)", connection))
        {
            insert.Parameters.AddWithValue(CollectionIdGenerator.Next() * 1_000_000L);
            insert.Parameters.AddWithValue(t0);
            insert.Parameters.AddWithValue(ServerId);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var start = t0.AddMinutes(-1);
        var end = t0.AddSeconds(5400).AddMinutes(1);
        var served = new[] { t0, t0.AddSeconds(300), t0.AddSeconds(900), t0.AddSeconds(5100), t0.AddSeconds(5400) };

        var all = await ReadPairAsync(viewer, start, end, null, end, ct);
        Assert.Equal(RetentionTier.Raw, all.Executions.Tier);
        Assert.Equal(0, all.Executions.BucketMinutes);
        Assert.Equal(t0, all.Executions.EffectiveStartUtc);
        Assert.False(all.Executions.Truncated);
        Assert.Equal(served, all.Executions.Points.Select(p => p.CollectionTime).ToArray());
        AssertValues(new[] { 90.0 / 300.0, 45.0 / 300.0, 900.0 / 300.0, 1.0 / 300.0, 150.0 / 300.0 }, all.Executions.Points);
        AssertValues(new[] { 12000.0 / 300.0, 1500.0 / 300.0, 90000.0 / 300.0, 100.0 / 300.0, 12000.0 / 300.0 }, all.Duration.Points);
        /* The duration points carry the rate as a whole number; the execution series carries it as the chart plots it. */
        Assert.Equal(new long[] { 0, 0, 3, 0, 0 }, all.Duration.Points.Select(p => p.ExecutionCount).ToArray());

        /* A collection the chosen database has no row in stays in the series as zero work over its real seconds. */
        var filtered = await ReadPairAsync(viewer, start, end, OneDatabase, end, ct);
        Assert.Equal(RetentionTier.Raw, filtered.Executions.Tier);
        Assert.Equal(served, filtered.Executions.Points.Select(p => p.CollectionTime).ToArray());
        AssertValues(new[] { 60.0 / 300.0, 0.0, 300.0 / 300.0, 0.0, 150.0 / 300.0 }, filtered.Executions.Points);

        var unmatched = await ReadPairAsync(viewer, start, end, NoSuchDatabase, end, ct);
        Assert.Equal(RetentionTier.Raw, unmatched.Executions.Tier);
        Assert.Empty(unmatched.Executions.Points);
        Assert.Empty(unmatched.Duration.Points);
        Assert.Equal(start, unmatched.Executions.EffectiveStartUtc);
    }

    /// <summary>
    /// A collection a minute over a three-day window: the chart budget gathers them into 5-minute buckets, so every
    /// point is a merged bucket stamped at its bucket's start. The window is (start, end], so the first bucket holds the four
    /// collections after the one stamped on the start. The second database has a row in every third collection
    /// only, one 10-minute stretch of restart collections empties two whole buckets, and a 2-minute stretch leaves a
    /// bucket rated over three of its five collections.
    /// </summary>
    [Fact]
    public async Task RawTier_MergedBuckets_TheSharedReadServesTheStandAloneExecutionSeries()
    {
        var baseConnectionString = StoreOrSkip();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var end = new DateTime(2026, 3, 10, 0, 0, 0);
        var start = end.AddDays(-3);
        var restartFrom = start.AddDays(1);
        var restartTo = restartFrom.AddMinutes(10);
        var partialFrom = start.AddDays(2).AddMinutes(1);
        var partialTo = partialFrom.AddMinutes(2);

        /* Executions follow the collection's minute number, so neighbouring buckets differ and a shifted series cannot pass. */
        await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    $1 + row_number() OVER (),
    g,
    $2,
    'shared-trend-read',
    d.name,
    decode(md5('q' || d.name), 'hex'),
    decode(md5('h' || d.name), 'hex'),
    1000,
    (d.base + m.minute_number % d.cycle) * 250000,
    d.base + m.minute_number % d.cycle,
    CASE WHEN (g >= $5 AND g < $6) OR (g >= $7 AND g < $8) THEN 0 ELSE 60 END
FROM generate_series($3::timestamp, $4::timestamp, INTERVAL '1 minute') AS g
CROSS JOIN LATERAL (SELECT extract(epoch FROM g)::bigint / 60 AS minute_number) AS m
CROSS JOIN (VALUES ('DbA', 1, 7), ('DbB', 5, 11)) AS d(name, base, cycle)
WHERE d.name = 'DbA' OR m.minute_number % 3 = 0", connection))
        {
            insert.Parameters.AddWithValue(CollectionIdGenerator.Next() * 1_000_000L);
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(start.AddMinutes(-30));
            insert.Parameters.AddWithValue(end);
            insert.Parameters.AddWithValue(restartFrom);
            insert.Parameters.AddWithValue(restartTo);
            insert.Parameters.AddWithValue(partialFrom);
            insert.Parameters.AddWithValue(partialTo);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var all = await ReadPairAsync(viewer, start, end, null, end, ct);
        Assert.Equal(RetentionTier.Raw, all.Executions.Tier);
        Assert.Equal(5, all.Executions.BucketMinutes);
        /* 865 buckets start inside the window; the two the restart stretch fills hold nothing rated and are not served. */
        Assert.Equal(863, all.Executions.Points.Count);
        Assert.DoesNotContain(all.Executions.Points, p => p.CollectionTime == restartFrom || p.CollectionTime == restartFrom.AddMinutes(5));
        Assert.Equal(start, all.Executions.Points[0].CollectionTime);
        Assert.Equal(ExpectedFirstBucketRate(start, 5, includeSecondDatabase: true, firstDatabase: true), all.Executions.Points[0].Value, 9);

        /* The partly rated bucket is rated over the three collections outside the 2-minute stretch (minutes 0, 3 and 4). */
        var partialBucket = partialFrom.AddMinutes(-1);
        var partial = Assert.Single(all.Executions.Points, p => p.CollectionTime == partialBucket);
        var ratedMinutes = new[] { partialBucket, partialBucket.AddMinutes(3), partialBucket.AddMinutes(4) };
        Assert.Equal(ratedMinutes.Sum(t => Executions(t, firstDatabase: true, secondDatabase: true)) / 180.0, partial.Value, 9);

        var filtered = await ReadPairAsync(viewer, start, end, OneDatabase, end, ct);
        Assert.Equal(RetentionTier.Raw, filtered.Executions.Tier);
        Assert.Equal(863, filtered.Executions.Points.Count);
        Assert.Equal(ExpectedFirstBucketRate(start, 5, includeSecondDatabase: true, firstDatabase: false), filtered.Executions.Points[0].Value, 9);

        var unmatched = await ReadPairAsync(viewer, start, end, NoSuchDatabase, end, ct);
        Assert.Empty(unmatched.Executions.Points);
        Assert.Empty(unmatched.Duration.Points);
    }

    /// <summary>
    /// Eight days of hourly collections for two databases, rolled up: a seven-day window reads the hourly rollup, where
    /// both series are columns of one statement. Executions follow the hour of the day, so neighbouring points differ.
    /// </summary>
    [Fact]
    public async Task HourlyTier_TheSharedReadServesTheStandAloneExecutionSeries()
    {
        var baseConnectionString = StoreOrSkip();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Naive UTC throughout. Hour-aligned so a bucket and a collection coincide exactly. */
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var nowHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
        var oldest = nowHour.AddDays(-8);

        /* One row per database per hour: DbA runs 10 + h executions in hour-of-day h, DbB 30 + h. */
        await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count)
SELECT
    (extract(epoch FROM g)::bigint * 100 + d.n),
    g,
    $3,
    'shared-trend-read',
    d.name,
    decode(md5('q' || d.n::text), 'hex'),
    decode(md5('h' || d.n::text), 'hex'),
    1000,
    d.n * 2000000,
    d.n * 10 + extract(hour FROM g)::int
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g
CROSS JOIN (VALUES (1, 'DbA'), (3, 'DbB')) AS d(n, name)", connection))
        {
            insert.Parameters.AddWithValue(oldest);
            insert.Parameters.AddWithValue(nowHour);
            insert.Parameters.AddWithValue(ServerId);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await RollupBackfill.RunSliceAsync(
            connection, TimescaleSupport.QueryStatsHourlyView, oldest.Date.AddDays(-1), nowHour.Date.AddDays(1),
            new RefreshDisclosure(message => Assert.Fail($"the backfill refresh degraded unexpectedly: {message}")), ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var start = nowHour.AddDays(-7);
        var end = nowHour;

        var all = await ReadPairAsync(viewer, start, end, null, now, ct);
        Assert.Equal(RetentionTier.Hourly, all.Executions.Tier);
        Assert.Equal(7 * 24, all.Executions.Points.Count);
        Assert.Equal(start, all.Executions.EffectiveStartUtc);
        Assert.All(all.Executions.Points, p => Assert.Equal((40.0 + 2 * p.CollectionTime.Hour) / 3600.0, p.Value, 9));
        Assert.All(all.Duration.Points, p => Assert.Equal(8000.0 / 3600.0, p.Value, 6));

        var filtered = await ReadPairAsync(viewer, start, end, OneDatabase, now, ct);
        Assert.Equal(RetentionTier.Hourly, filtered.Executions.Tier);
        Assert.Equal(7 * 24, filtered.Executions.Points.Count);
        Assert.All(filtered.Executions.Points, p => Assert.Equal((30.0 + p.CollectionTime.Hour) / 3600.0, p.Value, 9));

        var unmatched = await ReadPairAsync(viewer, start, end, NoSuchDatabase, now, ct);
        Assert.Equal(RetentionTier.Hourly, unmatched.Executions.Tier);
        Assert.Empty(unmatched.Executions.Points);
        Assert.Empty(unmatched.Duration.Points);
    }

    /* ------------------------------ plumbing ------------------------------ */

    private static string StoreOrSkip()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8549 shared trend read tests (each mints its own scratch database).");
        return baseConnectionString!;
    }

    /// <summary>
    /// The pair from the shared read, checked against the two stand-alone reads of the same window, filter and clock.
    /// </summary>
    private static async Task<(QueryTrendSeries Duration, QueryTrendSeries Executions)> ReadPairAsync(
        ViewerDataService viewer, DateTime start, DateTime end, IReadOnlyList<string>? databases, DateTime now, CancellationToken ct)
    {
        var shared = await viewer.GetQueryDurationAndExecutionCountTrendsAsync(ServerId, start, end, databases, now, ct);
        var executions = await viewer.GetExecutionCountTrendAsync(ServerId, start, end, databases, now, ct);
        var duration = await viewer.GetQueryDurationTrendAsync(ServerId, start, end, databases, now, ct);

        AssertSameSeries(executions, shared.Executions);
        AssertSameSeries(duration, shared.Duration);
        return shared;
    }

    /// <summary>Every field the chart reads, and every point exactly: instant, value and execution count.</summary>
    private static void AssertSameSeries(QueryTrendSeries expected, QueryTrendSeries actual)
    {
        Assert.Equal(expected.Tier, actual.Tier);
        Assert.Equal(expected.Source, actual.Source);
        Assert.Equal(expected.EffectiveStartUtc, actual.EffectiveStartUtc);
        Assert.Equal(expected.Truncated, actual.Truncated);
        Assert.Equal(expected.BucketMinutes, actual.BucketMinutes);
        Assert.Equal(expected.StartOlderThanTier, actual.StartOlderThanTier);
        Assert.Equal(expected.TierKeepsDays, actual.TierKeepsDays);
        Assert.Equal(
            expected.Points.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToArray(),
            actual.Points.Select(p => (p.CollectionTime, p.Value, p.ExecutionCount)).ToArray());
    }

    private static void AssertValues(double[] expected, List<QueryTrendPoint> points)
    {
        Assert.Equal(expected.Length, points.Count);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], points[i].Value, 9);
        }
    }

    /// <summary>The merged-bucket seed's executions in the collection at <paramref name="collectionTime"/>, by database.</summary>
    private static long Executions(DateTime collectionTime, bool firstDatabase, bool secondDatabase)
    {
        var minuteNumber = (collectionTime - new DateTime(1970, 1, 1)).Ticks / TimeSpan.TicksPerMinute;
        var executions = 0L;
        if (firstDatabase)
        {
            executions += 1 + minuteNumber % 7;
        }

        if (secondDatabase && minuteNumber % 3 == 0)
        {
            executions += 5 + minuteNumber % 11;
        }

        return executions;
    }

    /// <summary>The executions per second of the window's first bucket of the merged-bucket seed: a collection a minute, 60 s each. The
    /// collection stamped on the window's start belongs to the window before, so the bucket is rated over the collections after it.</summary>
    private static double ExpectedFirstBucketRate(DateTime windowStart, int bucketMinutes, bool includeSecondDatabase, bool firstDatabase)
    {
        var executions = 0L;
        for (var minute = 1; minute < bucketMinutes; minute++)
        {
            executions += Executions(windowStart.AddMinutes(minute), firstDatabase, includeSecondDatabase);
        }

        return executions / ((bucketMinutes - 1) * 60.0);
    }
}

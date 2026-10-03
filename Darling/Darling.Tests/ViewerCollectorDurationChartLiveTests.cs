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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres, seeds it
   once, and every fact then reads inside it and never writes. */
/// <summary>
/// The store behind <see cref="ViewerCollectorDurationChartLiveTests"/>: two servers, each holding the collection log runs the
/// Duration Trends chart draws. Every time below is measured back from <see cref="End"/>, the minute the store was seeded at, in
/// naive UTC.
/// </summary>
public sealed class CollectorDurationChartStore : IAsyncLifetime
{
    public const string WaitStats = "wait_stats";
    public const string CpuUtilization = "cpu_utilization";
    public const string LatchStats = "latch_stats";

    /// <summary>Monitored for months, logging fast: <see cref="WaitStats"/> every minute and <see cref="CpuUtilization"/> every
    /// two minutes for the 24 hours ending at <see cref="End"/>, 2,162 runs, more than four pages of the grid.</summary>
    public const int DenseServerId = -496901;

    /// <summary>Monitored for months; <see cref="WaitStats"/> every minute for three hours from <see cref="T"/>, every run 100 ms
    /// but one 5,000 ms, plus the runs the chart must leave out and one run of another collector.</summary>
    public const int ExactServerId = -496902;

    /// <summary>The dense server's duration, in milliseconds, for every run.</summary>
    public const int DenseDurationMs = 20;

    /// <summary>The exact server's ordinary run, in milliseconds.</summary>
    public const int RunMs = 100;

    /// <summary>The exact server's one slow run, in milliseconds, at <see cref="T"/> plus 25 minutes.</summary>
    public const int SlowRunMs = 5000;

    private ScratchPostgres? _scratch;
    private long _nextLogId = 1;

    public ViewerDataService? Viewer { get; private set; }

    /// <summary>The minute the history ends at, naive UTC.</summary>
    public DateTime End { get; private set; } = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    /// <summary>The exact server's first run: an hour boundary six hours before <see cref="End"/>, which every ladder width up to an hour divides.</summary>
    public DateTime T => new DateTime(End.Year, End.Month, End.Day, End.Hour, 0, 0, DateTimeKind.Unspecified).AddHours(-6);

    public static string ServerName(int serverId) => $"cdc-{-serverId}";

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var now = DateTime.UtcNow;
        End = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Unspecified);
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, CancellationToken.None);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, CancellationToken.None);
            await SeedAsync(connection);
        }

        Viewer = new ViewerDataService(_scratch.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        if (Viewer is not null)
        {
            await Viewer.DisposeAsync();
        }

        if (_scratch is not null)
        {
            await _scratch.DisposeAsync();
        }
    }

    private async Task SeedAsync(NpgsqlConnection connection)
    {
        /* Dense: 1,441 wait_stats runs and 721 cpu_utilization runs across the 24 hours, the first and the last run on the range's own ends. */
        await AddServerAsync(connection, DenseServerId, End.AddDays(-120));
        await RunsAsync(connection, DenseServerId, WaitStats, End.AddHours(-24), End, 1, DenseDurationMs);
        await RunsAsync(connection, DenseServerId, CpuUtilization, End.AddHours(-24), End, 2, DenseDurationMs);

        /* Exact: 180 one-minute runs of 100 ms from T, so every 10-minute bucket from T holds ten. The run at T plus 25 minutes is
           the slow one (its bucket's maximum is 5,000, its average 590). Two rows in the bucket from T plus 10 minutes that the chart
           has never drawn and must not count: an ERROR run of 99,999 ms, and a SUCCESS run with no duration. One run of another collector. */
        await AddServerAsync(connection, ExactServerId, End.AddDays(-120));
        await RunsAsync(connection, ExactServerId, WaitStats, T, T.AddMinutes(179), 1, RunMs);
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            "UPDATE collect.collection_log SET duration_ms = $4 WHERE server_id = $1 AND collector_name = $2 AND collection_time = $3",
            ExactServerId, WaitStats, DarlingMcpTestData.Naive(T.AddMinutes(25)), SlowRunMs);
        await RunAsync(connection, ExactServerId, WaitStats, T.AddMinutes(12).AddSeconds(30), "ERROR", 99_999);
        await RunAsync(connection, ExactServerId, WaitStats, T.AddMinutes(14).AddSeconds(30), "SUCCESS", null);
        await RunAsync(connection, ExactServerId, LatchStats, T.AddMinutes(30), "SUCCESS", 7);
    }

    private static async Task AddServerAsync(NpgsqlConnection connection, int serverId, DateTime createdUtc)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, ServerName(serverId), CancellationToken.None);
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None, "UPDATE collect.servers SET created_date = $2 WHERE server_id = $1",
            serverId, DarlingMcpTestData.Naive(createdUtc));
    }

    /* The collector's successful runs from first to last, every stepMinutes, each of durationMs. */
    private async Task RunsAsync(
        NpgsqlConnection connection, int serverId, string collector, DateTime first, DateTime last, int stepMinutes, int durationMs)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
            + "SELECT $5 + row_number() OVER (ORDER BY t), $1, $2, $6, t, $7, 'SUCCESS', 0 "
            + $"FROM generate_series($3::timestamp, $4::timestamp, interval '{stepMinutes} minutes') AS t",
            serverId, ServerName(serverId), DarlingMcpTestData.Naive(first), DarlingMcpTestData.Naive(last), _nextLogId, collector, durationMs);
        _nextLogId += 100_000;
    }

    private async Task RunAsync(NpgsqlConnection connection, int serverId, string collector, DateTime at, string status, int? durationMs)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
            + "VALUES ($1, $2, $3, $4, $5, $6, $7, 0)",
            _nextLogId, serverId, ServerName(serverId), collector, DarlingMcpTestData.Naive(at), durationMs, status);
        _nextLogId += 100_000;
    }
}

/// <summary>
/// The Collection Health tab's Duration Trends chart draws its own read over the whole range (#4966), over a store. Each fact runs
/// the read the tab starts beside the grid's (<see cref="ViewerDataService.GetCollectorDurationTrendAsync"/>) and builds the lines
/// from it with the step the chart draws them with (<see cref="CollectorDurationSeries.Build"/>), so what is asserted is what the chart
/// plots. The case that matters: a 24-hour range holding more than the grid's page of runs draws points from one end of the range to
/// the other, where the chart fed the page drew only the newest runs. The rest: the bucket width the shared helper picks for 7 days,
/// each bucket's maximum, average and count, the first bucket clamped to the range's start, the range's end inclusive, and the runs
/// the chart has never drawn (not successful, no duration, another server) left out.
/// </summary>
public sealed class ViewerCollectorDurationChartLiveTests : IClassFixture<CollectorDurationChartStore>
{
    private readonly CollectorDurationChartStore _store;

    public ViewerCollectorDurationChartLiveTests(CollectorDurationChartStore store) => _store = store;

    private DateTime End => _store.End;

    private void RequireStore() =>
        Assert.SkipWhen(_store.Viewer is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live Duration Trends chart tests.");

    private async Task<List<CollectorDurationBucket>> ChartReadAsync(int serverId, DateTime startUtc, DateTime endUtc)
    {
        RequireStore();
        return await _store.Viewer!.GetCollectorDurationTrendAsync(serverId, startUtc, endUtc, TestContext.Current.CancellationToken);
    }

    // ── The case the chart got wrong: a range that holds more than the grid's page ──

    [Fact]
    public async Task A24HourRange_HoldingMoreThanTheGridsPage_DrawsPointsAcrossTheWholeRange()
    {
        RequireStore();
        var start = End.AddHours(-24);

        /* The store really outgrows the page: 2,162 runs in the range, the grid keeps the newest 500, and those end hours into the range. */
        var page = await _store.Viewer!.GetRecentCollectionLogAsync(
            CollectorDurationChartStore.DenseServerId, start, End, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(ViewerDataService.CollectionLogRowCap, page.Count);
        Assert.True(page.Min(r => r.CollectionTime) >= End.AddHours(-6), "the page the grid keeps ends hours into the 24 hours");

        /* What the chart plots: each collector's line starts on the range's first instant and ends on its last, and holds a point
           for every bucket (a minute wide for 24 hours), well over the page. */
        var buckets = await ChartReadAsync(CollectorDurationChartStore.DenseServerId, start, End);
        var lines = CollectorDurationSeries.Build(buckets);

        Assert.Equal(new[] { CollectorDurationChartStore.CpuUtilization, CollectorDurationChartStore.WaitStats }, lines.Select(l => l.Collector).ToArray());
        foreach (var line in lines)
        {
            Assert.Equal(start.ToOADate(), line.Times[0]);
            Assert.Equal(End.ToOADate(), line.Times[^1]);
        }

        var wait = lines.Single(l => l.Collector == CollectorDurationChartStore.WaitStats);
        var cpu = lines.Single(l => l.Collector == CollectorDurationChartStore.CpuUtilization);
        Assert.Equal(1441, wait.Times.Length);
        Assert.Equal(721, cpu.Times.Length);
        Assert.True(wait.Times.Length > ViewerDataService.CollectionLogRowCap, "the read is not capped at the grid's page");
        Assert.All(wait.MaxMs, v => Assert.Equal(CollectorDurationChartStore.DenseDurationMs, v));

        /* The bucket is the shared helper's width: every gap between a collector's buckets is that many minutes. */
        var width = ViewerDataService.CollectorDurationBucketMinutes(start, End);
        Assert.Equal(1, width);
        var waitBuckets = buckets.Where(b => b.CollectorName == CollectorDurationChartStore.WaitStats).ToList();
        Assert.All(waitBuckets.Zip(waitBuckets.Skip(1)), p => Assert.Equal(TimeSpan.FromMinutes(width), p.Second.BucketStart - p.First.BucketStart));
    }

    [Fact]
    public async Task A7DayRange_BucketsAtTheSharedWidth_AndMergesTheRunsIntoEachBucket()
    {
        var start = End.AddDays(-7);

        /* A week sizes to ten minutes against the chart budget, per the shared helper. If the shared ladder or budget moves, this and the
           expectations below are the ones to move with it. */
        var width = ViewerDataService.CollectorDurationBucketMinutes(start, End);
        Assert.Equal(TrendBuckets.AutoMinutes(7 * 24 * 60, 1, TrendBudget.Chart.AutoPoints), width);
        Assert.Equal(10, width);

        /* The dense server's 24 hours of one-minute runs now fall ten to a bucket: 1,441 runs in 145 buckets or fewer, every one counted. */
        var buckets = await ChartReadAsync(CollectorDurationChartStore.DenseServerId, start, End);
        var wait = buckets.Where(b => b.CollectorName == CollectorDurationChartStore.WaitStats).ToList();

        Assert.InRange(wait.Count, 145, 146);
        Assert.Equal(1441, wait.Sum(b => b.RunCount));
        Assert.All(wait.Zip(wait.Skip(1)), p => Assert.Equal(TimeSpan.FromMinutes(width), p.Second.BucketStart - p.First.BucketStart));
        Assert.All(wait, b => Assert.Equal(0, b.BucketStart.Minute % width));
        Assert.All(wait.Skip(1).SkipLast(1), b => Assert.Equal(width, b.RunCount));
    }

    [Fact]
    public async Task EachBucket_CarriesItsMaximumAverageAndCount_SoASlowRunStillShows()
    {
        var t = _store.T;
        var buckets = await ChartReadAsync(CollectorDurationChartStore.ExactServerId, End.AddDays(-7), End);
        var wait = buckets.Where(b => b.CollectorName == CollectorDurationChartStore.WaitStats).ToList();

        /* 180 one-minute runs from T, ten minutes a bucket: 18 buckets of ten, the first on T itself (the range starts days earlier). */
        Assert.Equal(18, wait.Count);
        Assert.Equal(t, wait[0].BucketStart);
        Assert.All(wait, b => Assert.Equal(10, b.RunCount));

        /* The slow run is in the bucket from T plus 20 minutes: nine runs of 100 and one of 5,000. The bucket's maximum is the slow
           run, which is what the chart draws; its average is 590. Every other bucket's three figures are the ordinary run's. */
        var slow = wait[2];
        Assert.Equal(t.AddMinutes(20), slow.BucketStart);
        Assert.Equal(CollectorDurationChartStore.SlowRunMs, slow.MaxDurationMs);
        Assert.Equal(590d, slow.AvgDurationMs);
        Assert.All(wait.Where((_, i) => i != 2), b =>
        {
            Assert.Equal(CollectorDurationChartStore.RunMs, b.MaxDurationMs);
            Assert.Equal((double)CollectorDurationChartStore.RunMs, b.AvgDurationMs);
        });

        /* The line holds that maximum: the chart plots the slow bucket at 5,000 ms. */
        var line = CollectorDurationSeries.Build(buckets).Single(l => l.Collector == CollectorDurationChartStore.WaitStats);
        Assert.Equal((double)CollectorDurationChartStore.SlowRunMs, line.MaxMs.Max());
        Assert.Equal(590d, line.AvgMs[2]);
        Assert.Equal(10L, line.Runs[2]);
    }

    [Fact]
    public async Task ARunThatIsNotSuccessfulOrHasNoDuration_IsNotInTheChart()
    {
        /* The bucket from T plus 10 minutes holds an ERROR run of 99,999 ms and a SUCCESS run with no duration beside its ten ordinary
           runs: neither is counted nor raises the maximum. */
        var t = _store.T;
        var buckets = await ChartReadAsync(CollectorDurationChartStore.ExactServerId, End.AddDays(-7), End);
        var bucket = buckets.Single(b => b.CollectorName == CollectorDurationChartStore.WaitStats && b.BucketStart == t.AddMinutes(10));

        Assert.Equal(10, bucket.RunCount);
        Assert.Equal(CollectorDurationChartStore.RunMs, bucket.MaxDurationMs);
        Assert.Equal((double)CollectorDurationChartStore.RunMs, bucket.AvgDurationMs);
        Assert.DoesNotContain(buckets, b => b.MaxDurationMs == 99_999);
    }

    [Fact]
    public async Task TheFirstBucket_StartsAtTheRangesStart_WhenTheRangeStartsInsideABucket()
    {
        /* A range that starts five minutes into a ten-minute bucket: the first bucket is clamped to the range's start, not the grid line
           before it, and holds only the five runs from there. A range's end may lie past the data. */
        var start = _store.T.AddMinutes(5);
        var end = start.AddDays(7);
        Assert.Equal(10, ViewerDataService.CollectorDurationBucketMinutes(start, end));

        var buckets = await ChartReadAsync(CollectorDurationChartStore.ExactServerId, start, end);
        var wait = buckets.Where(b => b.CollectorName == CollectorDurationChartStore.WaitStats).ToList();

        Assert.Equal(start, wait[0].BucketStart);
        Assert.Equal(5, wait[0].RunCount);
        Assert.Equal(_store.T.AddMinutes(10), wait[1].BucketStart);
        Assert.Equal(10, wait[1].RunCount);
    }

    [Fact]
    public async Task TheRangesEnd_IsInclusive_AndNothingLaterIsRead()
    {
        /* One hour from T to T plus 60 minutes reads a minute wide: the run on the end instant is in, the one a minute after is not. */
        var t = _store.T;
        var buckets = await ChartReadAsync(CollectorDurationChartStore.ExactServerId, t, t.AddMinutes(60));
        var wait = buckets.Where(b => b.CollectorName == CollectorDurationChartStore.WaitStats).ToList();

        Assert.Equal(61, wait.Count);
        Assert.Equal(t, wait[0].BucketStart);
        Assert.Equal(t.AddMinutes(60), wait[^1].BucketStart);
    }

    [Fact]
    public async Task AnotherCollectorAndAnotherServer_StayOutOfEachOthersLines()
    {
        /* The exact server's one latch_stats run is a bucket of its own, so the chart draws no line for it; and the dense server's
           cpu_utilization collector never shows on the exact server. */
        var buckets = await ChartReadAsync(CollectorDurationChartStore.ExactServerId, End.AddDays(-7), End);

        Assert.Equal(new[] { CollectorDurationChartStore.LatchStats, CollectorDurationChartStore.WaitStats }, buckets.Select(b => b.CollectorName).Distinct().Order().ToArray());
        Assert.Single(buckets, b => b.CollectorName == CollectorDurationChartStore.LatchStats);
        Assert.Equal(new[] { CollectorDurationChartStore.WaitStats }, CollectorDurationSeries.Build(buckets).Select(l => l.Collector).ToArray());
    }

    [Fact]
    public async Task ARangeBeforeAnyRun_ReadsNothing()
    {
        var buckets = await ChartReadAsync(CollectorDurationChartStore.ExactServerId, End.AddDays(-30), End.AddDays(-20));

        Assert.Empty(buckets);
        Assert.Empty(CollectorDurationSeries.Build(buckets));
    }
}

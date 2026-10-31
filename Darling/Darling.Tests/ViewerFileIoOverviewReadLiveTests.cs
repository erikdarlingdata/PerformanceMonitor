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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6970: the Overview timeline's I/O Latency row reads one row per bucket (<c>GetFileIoOverviewLatencyTrendAsync</c>)
/// instead of one per file per bucket, and the series it returns must be the one the row drew from the per-file rows
/// (<c>GetFileIoLatencyTrendAsync</c> grouped by time and collapsed through <c>IoLatencyWeighting.Weighted</c>). The
/// seed holds the zero cases: log files that never read, a file with NULL counts, an unrated row (interval 0), and a time
/// at which only a log file reported.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerFileIoOverviewReadLiveTests
{
    private const int ServerId = -971012;
    private const int EmptyServerId = -971013;

    private static readonly DateTime Anchor = new(2026, 3, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private static List<(DateTime Time, double Value)> SeriesFromPerFilePoints(List<FileIoLatencyPoint> points) =>
        points.GroupBy(p => p.CollectionTime)
            .OrderBy(g => g.Key)
            .Select(g => (g.Key, IoLatencyWeighting.Weighted(g.Select(x => (x.AvgReadLatencyMs, x.Reads)))))
            .ToList();

    private static async Task SeedAsync(NpgsqlConnection connection, int minutesAgo, string db, string file,
        long? reads, long? writes, long? stallRead, long? stallWrite, int? intervalSeconds = 60)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
     delta_stall_queued_read_ms, delta_stall_queued_write_ms, sample_interval_seconds)
VALUES (1, $1, $2, 'viewer-6970', $3, $4, $5, $6, 0, 0, $7, $8, 0, 0, $9)", connection);
        command.Parameters.Add(new NpgsqlParameter { Value = Anchor.AddMinutes(-minutesAgo).AddSeconds(37) });
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(db);
        command.Parameters.AddWithValue(file);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)reads ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)writes ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)stallRead ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)stallWrite ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)intervalSeconds ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task SeedShapesAsync(NpgsqlConnection connection)
    {
        foreach (var minutes in new[] { 20, 19, 18, 10, 9 })
        {
            for (var i = 0; i < 3; i++)
            {
                await SeedAsync(connection, minutes, $"Db{i}", $"Db{i}_log", 0, 50_000, 0, 100_000);
            }

            await SeedAsync(connection, minutes, "Db0", "Db0_data", 500, 20, 4_000, 60);
            await SeedAsync(connection, minutes, "Db1", "Db1_data", 300 + minutes, 10, 1_500 + minutes * 3, 30);
        }

        /* Only a log file reports at 15 minutes ago: the row draws 0 there. */
        await SeedAsync(connection, 15, "Db0", "Db0_log", 0, 50_000, 0, 100_000);
        /* A file with NULL counts (its bucket drops out) and an unrated row (interval 0: its bucket has nothing rated and drops out). */
        await SeedAsync(connection, 10, "Db9", "nullcounts_log", null, 7, null, 7);
        await SeedAsync(connection, 8, "Db1", "Db1_data", 900, 5, 99_999, 5, intervalSeconds: 0);
    }

    private static async Task AssertSameSeriesAsync(ViewerDataService viewer, DateTime start, DateTime end, Func<int, bool> countOk)
    {
        var perFile = await viewer.GetFileIoLatencyTrendAsync(ServerId, start, end, cancellationToken: TestContext.Current.CancellationToken);
        var expected = SeriesFromPerFilePoints(perFile);
        var actual = await viewer.GetFileIoOverviewLatencyTrendAsync(ServerId, start, end, TestContext.Current.CancellationToken);

        Assert.True(countOk(expected.Count), $"unexpected point count {expected.Count}");
        Assert.Equal(expected.Count, actual.Count);
        Assert.Contains(expected, e => e.Value == 0);
        Assert.Contains(expected, e => e.Value > 0);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Time, actual[i].CollectionTime);
            Assert.Equal(expected[i].Value, actual[i].ReadLatencyMs, 9);
        }

        Assert.True(actual.Count < perFile.Count, "the overview read returns fewer rows than the per-file read");
    }

    [Fact]
    public async Task OverviewRead_SingletonAndMergedBuckets_MatchTheSeriesTheRowDrewFromPerFileRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live File I/O overview read test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAsync(connection, ServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            await SeedShapesAsync(connection);

            /* An hour is a one-minute bucket: every bucket holds one collection, so points sit at their own collection times (37 seconds past the minute). */
            await AssertSameSeriesAsync(viewer, Anchor.AddMinutes(-60), Anchor.AddMinutes(1), count => count == 6);

            /* A seven-day window buckets wide, so neighbouring collections merge and points sit at the bucket start. */
            await AssertSameSeriesAsync(viewer, Anchor.AddDays(-7), Anchor.AddMinutes(1), count => count < 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, ServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task OverviewRead_ServerWithNoRows_ReturnsNothing()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live File I/O overview read test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);
        Assert.Empty(await viewer.GetFileIoOverviewLatencyTrendAsync(EmptyServerId, Anchor.AddMinutes(-60), Anchor, TestContext.Current.CancellationToken));
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM file_io_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

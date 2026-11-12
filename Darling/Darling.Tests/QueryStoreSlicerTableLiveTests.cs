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
using System.Security.Cryptography;
using System.Text;
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
/// #8549: the Query Store slicer reads <c>query_store_interval_wide</c> for a window of seven days or more, where it used to read the raw
/// stats on every window. The test seeds a store through the collector's own writer (raw rows and the interval table together, every
/// outcome, an open interval re-fetched three times, a secondary replica, two databases, and a row 45 days before the windows), then
/// checks that the Viewer's slicer returns exactly the points the raw statement returns for several windows and a database filter,
/// that it really reads the table (a change made only to the table shows in the points), and that it falls back to raw when the
/// gate says no (a pending batch) or the window is under the minimum.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreSlicerTableLiveTests
{
    private const int ServerId = -3953930;

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>One line per bucket, every value to six places, so a wrong count, bucket or total shows as a different line.</summary>
    private static List<string> Lines(List<TimeSliceBucket> buckets) => buckets
        .Select(b => string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"{b.BucketTime:O}|{b.SessionCount}|{b.TotalCpu:F6}|{b.TotalElapsed:F6}|{b.TotalReads:F6}|{b.TotalWrites:F6}|{b.TotalPhysicalReads:F6}"))
        .ToList();

    private static string Md5(List<string> lines) => Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(string.Join('\n', lines))));

    [Fact]
    public async Task TheSlicer_AtSevenDaysAndUp_ReadsTheIntervalTable_ReturnsTheRawPoints_AndFallsBackWhenTheGateSaysNo()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #8549 slicer live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, WindowStart, ct);
        /* The writer's coverage bookkeeping stamps filled_since at the wall clock; put it back so the gate sees a long-covered store, as the grid's test does. */
        await using (var force = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection))
        {
            force.Parameters.AddWithValue("server_id", ServerId);
            force.Parameters.AddWithValue("value", WindowStart.AddDays(-60));
            await force.ExecuteNonQueryAsync(ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        async Task AssertSameAsRawAsync(DateTime start, DateTime end, string[]? databases, string label)
        {
            var raw = Lines(await viewer.ReadQueryStatsSlicerAsync(ViewerDataService.QueryStoreSlicerSql, ServerId, start, end, databases, ct));
            var actual = Lines(await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, databases, ct));
            Assert.True(raw.Count > 0, $"{label}: the raw statement returned no points, so the comparison would prove nothing");
            Assert.Equal(raw, actual);
            Assert.Equal(Md5(raw), Md5(actual));
        }

        await AssertSameAsRawAsync(WindowStart, WindowStart.AddDays(8), null, "8 days from the first row");
        await AssertSameAsRawAsync(WindowStart.AddDays(-22), WindowStart.AddDays(8), null, "30 days, the anchor row 45 days back outside it");
        await AssertSameAsRawAsync(WindowStart.AddDays(-22), WindowStart.AddDays(8), new[] { "qsB" }, "30 days, one database");
        await AssertSameAsRawAsync(WindowStart.AddDays(-22), WindowStart.AddDays(8), new[] { "qsA" }, "30 days, the other database");
        await AssertSameAsRawAsync(WindowStart.AddDays(1), WindowStart.AddDays(9), null, "8 days that start after the first row");

        /* The table is what answers: a change made to the table alone shows in the points of a 30 day window and not in a 6 day one. */
        var start = WindowStart.AddDays(-22);
        var end = WindowStart.AddDays(8);
        var before = Lines(await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, null, ct));
        await using (var bump = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide SET avg_cpu_time_us = avg_cpu_time_us + 1000000 WHERE server_id = @server_id AND query_id = 1", connection))
        {
            bump.Parameters.AddWithValue("server_id", ServerId);
            Assert.Equal(1, await bump.ExecuteNonQueryAsync(ct));
        }

        var viaTable = Lines(await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, null, ct));
        Assert.NotEqual(before, viaTable);
        var rawAfter = Lines(await viewer.ReadQueryStatsSlicerAsync(ViewerDataService.QueryStoreSlicerSql, ServerId, start, end, null, ct));
        Assert.Equal(before, rawAfter);

        /* The read names the source that answered: the table's plan over the 30 day window, and no plan under the slicer's seven days. */
        var tableRead = await viewer.GetQueryStoreSlicerWithReachAsync(ServerId, start, end, null, ct);
        Assert.Equal(viaTable, Lines(tableRead.Buckets));
        Assert.True(tableRead.Plan is { UseTable: true, EffectiveStart: not null }, "the 30 day slicer read reports the interval table's plan and the start it served from");
        Assert.Null((await viewer.GetQueryStoreSlicerWithReachAsync(ServerId, WindowStart, WindowStart.AddDays(6), null, ct)).Plan);

        var sixDays = Lines(await viewer.GetQueryStoreSlicerDataAsync(ServerId, WindowStart, WindowStart.AddDays(6), null, ct));
        Assert.Equal(Lines(await viewer.ReadQueryStatsSlicerAsync(ViewerDataService.QueryStoreSlicerSql, ServerId, WindowStart, WindowStart.AddDays(6), null, ct)), sixDays);

        /* A pending batch makes the gate say no: the points are raw's again, with the table still changed. */
        await using (var pending = new NpgsqlCommand(
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES (@server_id, @at, 'qsA', @at)", connection))
        {
            pending.Parameters.AddWithValue("server_id", ServerId);
            pending.Parameters.AddWithValue("at", WindowStart);
            await pending.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(before, Lines(await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, null, ct)));
        Assert.Null((await viewer.GetQueryStoreSlicerWithReachAsync(ServerId, start, end, null, ct)).Plan);
    }
}

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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6284: a server's Blocking/Deadlocks page listed one deadlock from yesterday, and the Overview's
/// Blocking &amp; Deadlocking lane over the same window drew a barely visible line and read 0 on hover. The lane's
/// reads agree with the page (same table, same event-time column, same window). The fault was the drawing: a bar a
/// fixed 30 seconds wide is under a pixel in a 24-hour window, and a hover reached one minute. These tests run the
/// lane's real reads against a seeded store and then ask the geometry the control uses what a viewer would see.
/// </summary>
[Collection("live-postgres")]
public sealed class OverviewEventLaneLiveTests
{
    private const string ServerName = "darling-overview-lane-6284-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Graph =
        "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list>" +
        "<process id=\"process1\" spid=\"55\" waittime=\"1000\"><inputbuf>x</inputbuf></process></process-list></deadlock>";

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM deadlocks WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task OneDeadlockAndOneBlockedReportYesterday_AreListed_Charted_AndReadOneOnHover()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live overview lane tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* "Yesterday": the whole previous calendar day, ending at today's midnight. */
            var end = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
            var start = end.AddDays(-1);
            var happened = start.AddHours(14).AddMinutes(7).AddSeconds(31);
            var collected = happened.AddMinutes(1);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,'process1','select 1',$6)",
                CollectionIdGenerator.Next(), collected, ServerId, ServerName, happened, Graph);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, database_name, blocked_process_report_xml)
VALUES ($1,$2,$3,$4,$5,12000,60,70,'AppDb','<blocked-process-report/>')",
                CollectionIdGenerator.Next(), collected, ServerId, ServerName, happened);

            var viewer = new ViewerDataService(cs!);

            /* The Blocking/Deadlocks page's read, and the lane's reads, over the same window. */
            var listed = await viewer.GetRecentDeadlocksAsync(ServerId, start, end, ct);
            var deadlockBars = await viewer.GetDeadlockTrendAsync(ServerId, start, end, ct);
            var blockingBars = await viewer.GetBlockingTrendAsync(ServerId, start, end, cancellationToken: ct);

            Assert.Single(listed);
            var deadlockBar = Assert.Single(deadlockBars);
            Assert.Equal(listed.Count, deadlockBars.Sum(b => b.Count));
            Assert.Equal(1, deadlockBar.Count);
            var blockingBar = Assert.Single(blockingBars);
            Assert.Equal(1, blockingBar.Count);

            /* What a viewer sees of that one bucket over a 24-hour window on a 1000-pixel plot. */
            var span = (end - start).TotalDays;
            var pixelsPerDay = 1000 / span;
            var x = deadlockBar.Time.ToOADate();
            Assert.True(EventLaneGeometry.BarWidthDays(span) * pixelsPerDay >= 5,
                "one deadlock must draw a bar a few pixels wide, not a sliver");

            var tolerance = EventLaneGeometry.HoverToleranceDays(span);
            double[] times = [x];
            double[] values = [1];
            foreach (var pixelsAway in new[] { 0, 2, -2, 4, -4 })
            {
                Assert.Equal(1, EventLaneGeometry.NearestValue(times, values, x + pixelsAway / pixelsPerDay, true, tolerance));
            }

            /* Far from the bucket the lane still says nothing happened. */
            Assert.Equal(0, EventLaneGeometry.NearestValue(times, values, x + 60 / pixelsPerDay, true, tolerance));

            /* The reach the lane had before: the same pointer, 4 pixels from the bar, read 0. */
            Assert.Equal(0, EventLaneGeometry.NearestValue(times, values, x + 4 / pixelsPerDay, true, EventLaneGeometry.MinHoverToleranceDays));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}

/// <summary>#6284: the lane's bar width and hover reach, with no store.</summary>
public sealed class EventLaneGeometryTests
{
    private const double PlotPixels = 1000;

    [Theory]
    [InlineData(1.0)]
    [InlineData(2.0)]
    [InlineData(7.0)]
    [InlineData(30.0)]
    public void ABucketIsDrawnAtLeastFivePixelsWide_AtEveryWindowLongerThanAnHour(double spanDays)
    {
        var pixels = EventLaneGeometry.BarWidthDays(spanDays) * PlotPixels / spanDays;
        Assert.InRange(pixels, 5, 8);
    }

    [Fact]
    public void AShortWindowKeepsTheThirtySecondBarAndOneMinuteReach()
    {
        var hour = 1.0 / 24;
        Assert.Equal(30.0 / 86400.0, EventLaneGeometry.BarWidthDays(hour));
        Assert.Equal(1.0 / 1440.0, EventLaneGeometry.HoverToleranceDays(hour));
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(0.0)]
    [InlineData(-1.0)]
    [InlineData(double.PositiveInfinity)]
    public void ASpanThatIsNotAWindowFallsBackToTheMinimumBar(double spanDays)
    {
        Assert.Equal(EventLaneGeometry.MinBarWidthDays, EventLaneGeometry.BarWidthDays(spanDays));
    }

    [Fact]
    public void AnEventSeriesReadsZeroBeyondTheReach_AndALevelSeriesAlwaysReadsItsNearestSample()
    {
        double[] times = [10.0, 10.5];
        double[] values = [3, 7];

        Assert.Equal(7, EventLaneGeometry.NearestValue(times, values, 10.49, true, 0.02));
        Assert.Equal(0, EventLaneGeometry.NearestValue(times, values, 10.25, true, 0.02));
        Assert.Equal(3, EventLaneGeometry.NearestValue(times, values, 10.2, false, 0.02));
        Assert.Null(EventLaneGeometry.NearestValue([], [], 10, true, 0.02));
        Assert.Null(EventLaneGeometry.NearestValue(times, [double.NaN, double.NaN], 10, true, 0.02));
    }
}

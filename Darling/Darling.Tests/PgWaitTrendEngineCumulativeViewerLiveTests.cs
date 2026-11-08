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
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8074: the Viewer's Wait Trend on an Aurora target reads the engine's cumulative counters: it follows the event with the most
/// wait time, serves that event's per-collection deltas as its points, and names the engine_cumulative instrument, the same
/// arm <c>get_pg_wait_trend</c> buckets for the web. Its own class because the Viewer is a WPF project; the tool-level facts are
/// in <see cref="PgWaitTrendEngineCumulativeLiveTests"/>.
/// </summary>
[Collection("live-postgres")]
public sealed class PgWaitTrendEngineCumulativeViewerLiveTests
{
    [Fact]
    public async Task TheViewer_ServesTheSameTrend_OnAnAuroraTarget()
    {
        await PgWaitTrendEngineCumulativeLiveTests.RunAsync(MonitoredEngineKind.AuroraPostgres, async (connection, postgres, ct) =>
        {
            await PgWaitTrendEngineCumulativeLiveTests.SeedCountersAsync(connection, ct);
            await using var viewer = new ViewerDataService(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!);

            var web = JsonDocument.Parse(await DarlingMcpPgTrendTools.GetPgWaitTrend(
                postgres, PgWaitTrendEngineCumulativeLiveTests.ServerName, null, PgWaitTrendEngineCumulativeLiveTests.Hours,
                PgWaitTrendEngineCumulativeLiveTests.AsOfText, cancellationToken: ct)).RootElement;
            var end = new DateTime(2026, 3, 5, 11, 0, 0, DateTimeKind.Utc);
            var read = await viewer.GetPgWaitTrendAsync(PgWaitTrendEngineCumulativeLiveTests.ServerId, end.AddHours(-PgWaitTrendEngineCumulativeLiveTests.Hours), end, ct);

            Assert.Equal("DataFileRead", read.Subject);
            Assert.Equal(web.GetProperty("wait_event").GetString(), read.Subject);
            var webPoints = web.GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(2, webPoints.Length);
            Assert.Equal(webPoints.Length, read.Points.Count);
            for (var i = 0; i < webPoints.Length; i++)
            {
                Assert.Equal(webPoints[i].GetProperty("sample_count").GetInt64(), read.Points[i].SampleCount);
                Assert.Equal(webPoints[i].GetProperty("estimated_wait_ms_per_second").GetDouble(), Math.Round(read.Points[i].EstimatedWaitMsPerSecond!.Value, 3), 3);
                Assert.Null(read.Points[i].BackendCount);
                Assert.False(read.Points[i].CounterReset);
            }
        });
    }
}

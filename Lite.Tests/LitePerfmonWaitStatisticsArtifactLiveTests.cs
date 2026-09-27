/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4476's Lite twin of Darling's <c>PerfmonWaitStatisticsArtifactViewerLiveTests</c> /
/// <c>PerfmonWaitStatisticsArtifactMcpLiveTests</c>: runs the REAL Lite reads
/// (<see cref="LocalDataService.GetPerfmonTrendsByCountersAsync"/> and
/// <see cref="LocalDataService.GetPerfmonBucketsAsync"/>) against an embedded DuckDB planted with the same
/// field-shaped isolated single-sample <c>SQLServer:Wait Statistics</c> artifact those two classes use — one
/// server, four instances of <c>Page latch waits</c>, five collections one minute apart, one spiking instance
/// (<c>Waits started per second</c> reading 318, 400, 214,396,451, 1,021, 500) among three flat neighbours.
/// </summary>
[Collection("server-time-helper")]
public sealed class LitePerfmonWaitStatisticsArtifactLiveTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -447644761;
    private const string ServerName = "lite-perfmon-waitstats-artifact-e2e";
    private const string ObjectName = "SQLServer:Wait Statistics";
    private const string CounterName = "Page latch waits";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _savedUtcOffsetMinutes;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public LitePerfmonWaitStatisticsArtifactLiveTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _savedUtcOffsetMinutes = ServerTimeHelper.UtcOffsetMinutes;
        ServerTimeHelper.UtcOffsetMinutes = 0;
    }

    public void Dispose()
    {
        ServerTimeHelper.UtcOffsetMinutes = _savedUtcOffsetMinutes;
        _seedConn?.Dispose();
    }

    /// <summary>Through <see cref="LocalDataService.GetPerfmonTrendsByCountersAsync"/> (the chart read):
    /// collection 3's per-collection sum with the spike set aside is 62 (2 + 10 + 50, the three normal
    /// instances' values at that collection), exactly one artifact is reported, and an all-artifact
    /// collection (a counter with ONLY the spiking instance planted) is skipped rather than published as a
    /// fabricated 0.</summary>
    [Fact]
    public async Task GetPerfmonTrendsByCountersAsync_SetsAsideTheIsolatedSpike_AndDropsAnAllArtifactCollection()
    {
        var t0 = new DateTime(2026, 3, 15, 9, 0, 0);
        var times = Enumerable.Range(0, 5).Select(i => t0.AddMinutes(i)).ToArray();

        /* Waits started per second: the spiking instance. Collection 3 (index 2) reads the field-shaped
           cumulative artifact; its neighbours are normal. */
        var startedPerSec = new long[] { 318, 400, 214_396_451, 1_021, 500 };
        /* Waits in progress: a normal, non-spiking instance under the same counter name. */
        var inProgress = new long[] { 2, 3, 2, 1, 2 };
        /* Average wait time (ms) and Cumulative wait time (ms) per second: two more instances, flat. */
        var avgWaitMs = new long[] { 10, 10, 10, 10, 10 };
        var cumulativeWaitMs = new long[] { 50, 50, 50, 50, 50 };

        for (var i = 0; i < 5; i++)
        {
            await PlantAsync(times[i], CounterName, "Waits started per second", startedPerSec[i]);
            await PlantAsync(times[i], CounterName, "Waits in progress", inProgress[i]);
            await PlantAsync(times[i], CounterName, "Average wait time (ms)", avgWaitMs[i]);
            await PlantAsync(times[i], CounterName, "Cumulative wait time (ms) per second", cumulativeWaitMs[i]);
        }

        var startUtc = times[0].AddMinutes(-1);
        var endUtc = times[^1].AddMinutes(1);

        var trends = await _dataService.GetPerfmonTrendsByCountersAsync(
            ServerId, new List<string> { CounterName }, fromDate: startUtc, toDate: endUtc);

        Assert.True(trends.TryGetValue(CounterName, out var points), "the counter did not plot at all");
        Assert.Equal(5, points!.Count);

        var point3 = points.Single(p => p.CollectionTime == times[2]);
        /* Fact: the artifact-excluded sum for collection 3 is 2 (Waits in progress) + 10 (Average wait time)
           + 50 (Cumulative wait time) = 62 — the 214,396,451 spike is set aside entirely. */
        Assert.Equal(62, point3.Value);
        Assert.Equal(1, point3.ArtifactsSetAside);
        foreach (var p in points.Where(p => p.CollectionTime != times[2]))
        {
            Assert.Equal(0, p.ArtifactsSetAside);
        }

        /* Only the spiking instance planted under a SEPARATE counter name (no other instance in that
           collection to survive the exclusion): collection 3 drops out entirely rather than publishing a
           fabricated 0. */
        const string SoloCounterName = "Waits started per second";
        for (var i = 0; i < 5; i++)
        {
            await PlantAsync(times[i], SoloCounterName, "", startedPerSec[i]);
        }

        var soloTrends = await _dataService.GetPerfmonTrendsByCountersAsync(
            ServerId, new List<string> { SoloCounterName }, fromDate: startUtc, toDate: endUtc);

        Assert.True(soloTrends.TryGetValue(SoloCounterName, out var soloPoints), "the solo counter did not plot at all");
        Assert.Equal(4, soloPoints!.Count);
        Assert.DoesNotContain(soloPoints, p => p.CollectionTime == times[2]);
    }

    /// <summary>Through <see cref="LocalDataService.GetPerfmonBucketsAsync"/> (the <c>get_perfmon_trend</c>
    /// MCP tool's read): the published peak stays under 10,000 (the spike never reaches it) and exactly one
    /// artifact is reported across the window.</summary>
    [Fact]
    public async Task GetPerfmonBucketsAsync_KeepsTheSpikeOutOfThePublishedPeak_AndReportsOneArtifact()
    {
        var t0 = new DateTime(2026, 3, 15, 9, 0, 0);
        var times = Enumerable.Range(0, 5).Select(i => t0.AddMinutes(i)).ToArray();

        var startedPerSec = new long[] { 318, 400, 214_396_451, 1_021, 500 };
        var inProgress = new long[] { 2, 3, 2, 1, 2 };
        var avgWaitMs = new long[] { 10, 10, 10, 10, 10 };
        var cumulativeWaitMs = new long[] { 50, 50, 50, 50, 50 };

        for (var i = 0; i < 5; i++)
        {
            await PlantAsync(times[i], CounterName, "Waits started per second", startedPerSec[i]);
            await PlantAsync(times[i], CounterName, "Waits in progress", inProgress[i]);
            await PlantAsync(times[i], CounterName, "Average wait time (ms)", avgWaitMs[i]);
            await PlantAsync(times[i], CounterName, "Cumulative wait time (ms) per second", cumulativeWaitMs[i]);
        }

        /* Smallest bucket width (1 minute), asOfUtc past the last plant, hoursBack wide enough to cover the
           whole 5-minute plant — one point per collection, so the artifact's own collection is its own
           bucket rather than folded into a wider one. */
        var asOfUtc = times[^1].AddMinutes(1);

        var result = await _dataService.GetPerfmonBucketsAsync(
            ServerId, "Waits started per second", hoursBack: 1, asOfUtc: asOfUtc, bucketMinutes: 1);

        /* Only 4 points, not 5: this counter has a single instance, so the artifact's own collection has
           nothing else to sum — every row in that bucket was set aside, and GetPerfmonBucketsAsync drops a
           bucket like that rather than publish a fabricated 0. */
        Assert.Equal(4, result.Points.Count);

        var maxValue = result.Points.Max(p => p.MaxValue);
        Assert.True(maxValue < 10_000, $"expected the artifact excluded from the published max, got {maxValue}");

        Assert.Equal(1, result.ArtifactsSetAside);
    }

    /* ---- seeding ---------------------------------------------------------------------------------------- */

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task PlantAsync(DateTime at, string counterName, string instanceName, long value)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO perfmon_stats
            (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
             cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, $9)";
        foreach (var v in new object[]
        {
            _nextId--, at, ServerId, ServerName, ObjectName, counterName, instanceName, value,
            PerfmonCounterTypes.PerfCounterLargeRawCount
        })
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A8 option B (PR #4176): behaviour tests for Lite's tiled anomaly gate on the four families this PR
/// carries — batch requests, sessions, query duration and memory. Each detector now scores every target-local
/// hour of the analysis window against ITS OWN hour-of-week baseline (<see cref="AnomalyGate.EvaluateTiles"/>)
/// instead of collapsing the whole window to one peak/mean pair, so a sustained shift confined to part of the
/// window fires where the old whole-window gate stayed quiet, and a lone spike inside an otherwise-quiet window
/// still does not fire. Every scenario runs through the REAL <see cref="AnomalyDetector.DetectAnomaliesAsync"/>,
/// never a hand-copied tile call, and reads the fired fact's own metadata (<c>tile_local_hour</c>,
/// <c>tiles_scored</c>, <c>tiles_fired</c>, <c>fire_threshold</c>).
///
/// <para><b>First family: batch requests.</b> Scenarios 1, 3 and 4 (the design's own reduced set for a
/// multi-family PR). <b>Sessions, query duration and memory:</b> scenario 1 only.</para>
///
/// <para>Seeding is set-based (DuckDB <c>range()</c>), never a row-per-roundtrip loop, and every seeded time is
/// truncated to whole seconds. No <c>server_properties</c> row is seeded, so keying is UTC and
/// <c>WindowTile.LocalHour.Hour</c>/<c>DayOfWeek</c> read directly off the UTC instants below.</para>
/// </summary>
public sealed class AnomalyDetectorTileBehaviourBatchSessionQueryMemoryTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _baselineProvider;
    private readonly AnomalyDetector _detector;
    private DuckDBConnection? _seedConn;

    // Wednesday 2026-01-07, 10:00 UTC (whole seconds; no server_properties row, so keying is UTC).
    private static readonly DateTime WindowStartT = new(2026, 1, 7, 10, 0, 0, DateTimeKind.Unspecified);

    public AnomalyDetectorTileBehaviourBatchSessionQueryMemoryTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _baselineProvider = new BaselineProvider(_duckDb);
        _detector = new AnomalyDetector(_duckDb, _baselineProvider);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ExecuteSeedAsync(string sql, params object[] parameters)
    {
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters)
            cmd.Parameters.Add(new DuckDBParameter { Value = p });
        await cmd.ExecuteNonQueryAsync();
    }

    private static AnalysisContext FourHourContext(int serverId, DateTime windowStart) => new()
    {
        ServerId = serverId,
        ServerName = "TileServer",
        TimeRangeStart = windowStart,
        TimeRangeEnd = windowStart.AddHours(4)
    };

    /* ───────────────────────── Batch requests: scenarios 1, 3 and 4 ───────────────────────── */

    private const double BatchMu = 5000.0;    // requests/sec-equivalent delta per 10s collection
    private const double BatchSigma = 200.0;
    private const double BatchShift = BatchMu + 6 * BatchSigma; // 6200, clears BatchRequestFloor (500)

    /// <summary>
    /// #21 days of history for hours 10-13 of this weekday, ~12 samples/hour (3-minute cadence),
    /// deterministic pseudo-noise (μ + σ·sin(i)) so the bucket is trustworthy with a known median/spread.
    /// </summary>
    private async Task SeedBatchHistoryAsync(int serverId, DateTime windowStart, int hours)
    {
        const string sql = @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name,
     object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT -9000000 - (d * 10000 + h * 100 + i), $1 - to_days(d) + to_hours(h) + to_minutes(i * 5), $2, 'TileServer',
       'SQLServer:SQL Statistics', 'Batch Requests/sec', '',
       GREATEST(0, $3 + $4 * sin(d * 4 + h * 12 + i)), GREATEST(0, $3 + $4 * sin(d * 4 + h * 12 + i)), 10
FROM range(1, 22) AS d(d)
CROSS JOIN range(0, $5) AS hh(h)
CROSS JOIN range(0, 12) AS ii(i)";
        await ExecuteSeedAsync(sql, windowStart, serverId, BatchMu, BatchSigma, hours);
    }

    /// <summary>Window: hours below <paramref name="shiftFromHour"/> at baseline shape; hours at/above it run
    /// the WHOLE hour at the shift. A <paramref name="shiftFromHour"/> past the last hour plants an all-baseline
    /// window (used by scenario 3, which plants its lone spike separately).</summary>
    private async Task SeedBatchWindowAsync(int serverId, DateTime windowStart, int hours, int shiftFromHour)
    {
        const string sql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= $5 THEN $4 ELSE $2 + $3 * sin(h * 12 + i) END AS value
    FROM range(0, $6) AS hh(h)
    CROSS JOIN range(0, 12) AS ii(i)
)
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name,
     object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT -9500000 - (h * 100 + i), $1 + to_hours(h) + to_minutes(i * 5), $7, 'TileServer',
       'SQLServer:SQL Statistics', 'Batch Requests/sec', '',
       GREATEST(0, value), GREATEST(0, value), 10
FROM s";
        await ExecuteSeedAsync(sql, windowStart, BatchMu, BatchSigma, BatchShift, shiftFromHour, hours, serverId);
    }

    private async Task SeedOneBatchSampleAsync(int serverId, DateTime at, double value)
    {
        const string sql = @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name,
     object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES (-9600000, $1, $2, 'TileServer', 'SQLServer:SQL Statistics', 'Batch Requests/sec', '', $3, $3, 10)";
        await ExecuteSeedAsync(sql, at, serverId, value);
    }

    private async Task SeedSparseBatchWindowAsync(int serverId, DateTime windowStart, double shift)
    {
        const string sql = @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name,
     object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT -9700000 - (h * 10 + i), $1 + to_hours(h) + to_minutes(i * 20), $2, 'TileServer',
       'SQLServer:SQL Statistics', 'Batch Requests/sec', '', $3, $3, 10
FROM range(0, 4) AS hh(h)
CROSS JOIN range(0, 2) AS ii(i)";
        await ExecuteSeedAsync(sql, windowStart, serverId, shift);
    }

    /// <summary>Canary so <c>HasBaselineDataAsync</c> passes for this server: a little CPU/wait coverage over
    /// the whole history+window span.</summary>
    private async Task SeedCanaryAsync(int serverId, DateTime windowStart, int historyDays = 21, int windowHours = 4)
    {
        const string cpuSql = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT -9800000 - d, $1 - to_days(d), $2, 'TileServer', $1 - to_days(d), 10, 2
FROM range(0, $3) AS dd(d)";
        await ExecuteSeedAsync(cpuSql, windowStart, serverId, historyDays + windowHours + 1);

        const string waitSql = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT -9900000 - h, $1 + to_hours(h), $2, 'TileServer', 'SOS_SCHEDULER_YIELD', 0, 0, 0, 0, 100, 0, 10
FROM range(0, $3) AS hh(h)";
        await ExecuteSeedAsync(waitSql, windowStart, serverId, windowHours + 1);
    }

    /// <summary>
    /// Scenario 1 (design's "the shift, 4 h"): a 4 h window [T, T+4h) whose FIRST two hours sit at the baseline
    /// and whose LAST two hours run the whole hour at μ + 6σ. The per-hour tile gate fires (hours 3/4 clear the
    /// cutoff against their own bucket); the SAME numbers, judged as one whole-window peak/mean pair against
    /// the start hour's bucket alone, do not clear the cutoff — the shift is diluted by the two quiet hours,
    /// exactly the case this lane exists to prove.
    /// </summary>
    [Fact]
    public async Task BatchRequests_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        const int serverId = -8001;
        await SeedBatchHistoryAsync(serverId, WindowStartT, 4);
        await SeedBatchWindowAsync(serverId, WindowStartT, 4, shiftFromHour: 2);
        await SeedCanaryAsync(serverId, WindowStartT);

        var startBucket = await _baselineProvider.GetBaselineAsync(serverId, MetricNames.BatchRequests, WindowStartT);
        Assert.True(startBucket.IsTrustworthy, "the start-hour batch-requests bucket must be trustworthy");

        var wholeWindowMean = (BatchMu + BatchShift) / 2.0; // two hours at baseline, two hours at the shift
        var wholeWindowMeanSigma = startBucket.EffectiveRobustSigma > 0
            ? (wholeWindowMean - startBucket.Median) / startBucket.EffectiveRobustSigma
            : (wholeWindowMean - startBucket.Mean) / startBucket.EffectiveStdDev;
        var referenceCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.BatchRequests);
        Assert.True(wholeWindowMeanSigma < referenceCutoff,
            $"the whole-window mean deviation ({wholeWindowMeanSigma}) unexpectedly cleared the cutoff on its own — the shift was not actually diluted by construction");

        var facts = await _detector.DetectAnomaliesAsync(FourHourContext(serverId, WindowStartT));
        var fact = Assert.Single(facts, f => f.Key == "ANOMALY_BATCH_REQUESTS");

        Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained two-hour shift should have scored through the tile path");
        Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
        Assert.Equal(4, fact.Metadata["tiles_scored"]);
        Assert.Equal(2, fact.Metadata["tiles_fired"]);
        Assert.Equal(referenceCutoff, fact.Metadata["fire_threshold"], precision: 6);
    }

    /// <summary>
    /// Scenario 3 (design's "the lone spike"): a 4 h window at baseline throughout, plus ONE sample at
    /// μ + 10σ in hour 2. A single sample cannot drag its hour's tile mean past the (peak AND mean) pair gate,
    /// so it does not fire.
    /// </summary>
    [Fact]
    public async Task BatchRequests_LoneSpikeInsideAnHour_DoesNotFire()
    {
        const int serverId = -8002;
        const double spike = BatchMu + 10 * BatchSigma;

        await SeedBatchHistoryAsync(serverId, WindowStartT, 4);
        await SeedBatchWindowAsync(serverId, WindowStartT, 4, shiftFromHour: 99); // all-baseline window
        await SeedOneBatchSampleAsync(serverId, WindowStartT.AddHours(2).AddMinutes(30), spike);
        await SeedCanaryAsync(serverId, WindowStartT);

        var facts = await _detector.DetectAnomaliesAsync(FourHourContext(serverId, WindowStartT));

        Assert.DoesNotContain(facts, f => f.Key == "ANOMALY_BATCH_REQUESTS");
    }

    /// <summary>
    /// Scenario 4 (design's "the fallback"): a 4 h window with only 2 samples per hour — under
    /// <see cref="AnomalyThresholds.MinTileSamples"/> (3) in every tile — all at μ + 6σ. No tile can be scored,
    /// so the detector falls through to today's whole-window path; the resulting fact still fires, but carries
    /// no <c>tile_local_hour</c> key.
    /// </summary>
    [Fact]
    public async Task BatchRequests_UnderMinTileSamplesEveryTile_FallsBackToTheWholeWindowPath_WithNoTileKeys()
    {
        const int serverId = -8003;
        await SeedBatchHistoryAsync(serverId, WindowStartT, 4);
        await SeedSparseBatchWindowAsync(serverId, WindowStartT, BatchShift);
        await SeedCanaryAsync(serverId, WindowStartT);

        var facts = await _detector.DetectAnomaliesAsync(FourHourContext(serverId, WindowStartT));
        var fact = Assert.Single(facts, f => f.Key == "ANOMALY_BATCH_REQUESTS");

        Assert.False(fact.Metadata.ContainsKey("tile_local_hour"), "under MinTileSamples in every tile, the fallback fact must not carry tile keys");
    }

    /* ───────────────────────── Sessions: scenario 1 only ───────────────────────── */

    /// <summary>Scenario 1 for sessions: <c>session_stats.connection_count</c>, summed per collection, shifted
    /// for the last two hours of a 4h window. The magnitude the tile reports must clear
    /// <see cref="AnomalyThresholds.SessionCountFloor"/> (50) — sized well above it here (μ 200, shift 6σ with
    /// σ 20 → 320), unlike the small demo numbers used for the batch-requests family above.</summary>
    [Fact]
    public async Task Sessions_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        const int serverId = -8004;
        const double mu = 200.0;
        const double sigma = 20.0;
        const double shift = mu + 6 * sigma; // 320, clears SessionCountFloor (50)

        const string historySql = @"
INSERT INTO session_stats
    (collection_id, collection_time, server_id, server_name, program_name,
     connection_count, running_count, sleeping_count, dormant_count)
SELECT -9100000 - (d * 10000 + h * 100 + i), $1 - to_days(d) + to_hours(h) + to_minutes(i * 5), $2, 'TileServer', 'App1',
       GREATEST(1, ROUND($3 + $4 * sin(d * 4 + h * 12 + i))), 0, 0, 0
FROM range(1, 22) AS d(d)
CROSS JOIN range(0, 4) AS hh(h)
CROSS JOIN range(0, 12) AS ii(i)";
        await ExecuteSeedAsync(historySql, WindowStartT, serverId, mu, sigma);

        const string windowSql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= 2 THEN $3 ELSE $1 + $2 * sin(h * 12 + i) END AS value
    FROM range(0, 4) AS hh(h)
    CROSS JOIN range(0, 12) AS ii(i)
)
INSERT INTO session_stats
    (collection_id, collection_time, server_id, server_name, program_name,
     connection_count, running_count, sleeping_count, dormant_count)
SELECT -9600000 - (h * 100 + i), $4 + to_hours(h) + to_minutes(i * 5), $5, 'TileServer', 'App1',
       GREATEST(1, ROUND(value)), 0, 0, 0
FROM s";
        await ExecuteSeedAsync(windowSql, mu, sigma, shift, WindowStartT, serverId);

        await SeedCanaryAsync(serverId, WindowStartT);

        var startBucket = await _baselineProvider.GetBaselineAsync(serverId, MetricNames.SessionCount, WindowStartT);
        Assert.True(startBucket.IsTrustworthy, "the start-hour session bucket must be trustworthy");
        var wholeWindowMean = (mu + shift) / 2.0;
        var wholeWindowMeanSigma = startBucket.EffectiveRobustSigma > 0
            ? (wholeWindowMean - startBucket.Median) / startBucket.EffectiveRobustSigma
            : (wholeWindowMean - startBucket.Mean) / startBucket.EffectiveStdDev;
        var referenceCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.SessionCount);
        Assert.True(wholeWindowMeanSigma < referenceCutoff,
            $"the whole-window session mean deviation ({wholeWindowMeanSigma}) unexpectedly cleared the cutoff on its own");

        var facts = await _detector.DetectAnomaliesAsync(FourHourContext(serverId, WindowStartT));
        var fact = Assert.Single(facts, f => f.Key == "ANOMALY_SESSION_SPIKE");

        Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained session shift should have scored through the tile path");
        Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
        Assert.Equal(4, fact.Metadata["tiles_scored"]);
        Assert.Equal(2, fact.Metadata["tiles_fired"]);
        Assert.Equal(referenceCutoff, fact.Metadata["fire_threshold"], precision: 6);
    }

    /* ───────────────────────── Query duration: scenario 1 only ───────────────────────── */

    /// <summary>Scenario 1 for query duration: <c>query_stats</c> total elapsed microseconds per collection,
    /// shifted for the last two hours of a 4h window. Sized above
    /// <see cref="AnomalyThresholds.QueryDurationFloorUs"/> (1,000,000 us = 1s).</summary>
    [Fact]
    public async Task QueryDuration_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        const int serverId = -8005;
        const double bigMu = 700_000.0;
        const double bigSigma = 40_000.0;
        const double bigShift = bigMu + 6 * bigSigma; // 1,140,000 us, clears QueryDurationFloorUs (1,000,000)

        const string historySql = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     execution_count, total_elapsed_time, total_worker_time,
     total_logical_reads, total_logical_writes, total_physical_reads,
     delta_execution_count, delta_elapsed_time, delta_worker_time,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills)
SELECT -9200000 - (d * 10000 + h * 100 + i), $1 - to_days(d) + to_hours(h) + to_minutes(i * 5), $2, 'TileServer',
       100, 0, 0, 0, 0, 0,
       100, GREATEST(0, ROUND($3 + $4 * sin(d * 4 + h * 12 + i))), 0, 0, 0, 0, 0, 0
FROM range(1, 22) AS d(d)
CROSS JOIN range(0, 4) AS hh(h)
CROSS JOIN range(0, 12) AS ii(i)";
        await ExecuteSeedAsync(historySql, WindowStartT, serverId, bigMu, bigSigma);

        const string windowSql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= 2 THEN $3 ELSE $1 + $2 * sin(h * 12 + i) END AS value
    FROM range(0, 4) AS hh(h)
    CROSS JOIN range(0, 12) AS ii(i)
)
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name,
     execution_count, total_elapsed_time, total_worker_time,
     total_logical_reads, total_logical_writes, total_physical_reads,
     delta_execution_count, delta_elapsed_time, delta_worker_time,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills)
SELECT -9700000 - (h * 100 + i), $4 + to_hours(h) + to_minutes(i * 5), $5, 'TileServer',
       100, 0, 0, 0, 0, 0,
       100, GREATEST(0, ROUND(value)), 0, 0, 0, 0, 0, 0
FROM s";
        await ExecuteSeedAsync(windowSql, bigMu, bigSigma, bigShift, WindowStartT, serverId);

        await SeedCanaryAsync(serverId, WindowStartT);

        var startBucket = await _baselineProvider.GetBaselineAsync(serverId, MetricNames.QueryDuration, WindowStartT);
        Assert.True(startBucket.IsTrustworthy, "the start-hour query-duration bucket must be trustworthy");
        var wholeWindowMean = (bigMu + bigShift) / 2.0;
        var wholeWindowMeanSigma = startBucket.EffectiveRobustSigma > 0
            ? (wholeWindowMean - startBucket.Median) / startBucket.EffectiveRobustSigma
            : (wholeWindowMean - startBucket.Mean) / startBucket.EffectiveStdDev;
        var referenceCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.QueryDuration);
        Assert.True(wholeWindowMeanSigma < referenceCutoff,
            $"the whole-window query-duration mean deviation ({wholeWindowMeanSigma}) unexpectedly cleared the cutoff on its own");

        var facts = await _detector.DetectAnomaliesAsync(FourHourContext(serverId, WindowStartT));
        var fact = Assert.Single(facts, f => f.Key == "ANOMALY_QUERY_DURATION");

        Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained query-duration shift should have scored through the tile path");
        Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
        Assert.Equal(4, fact.Metadata["tiles_scored"]);
        Assert.Equal(2, fact.Metadata["tiles_fired"]);
        Assert.Equal(referenceCutoff, fact.Metadata["fire_threshold"], precision: 6);
    }

    /* ───────────────────────── Memory: scenario 1 only ───────────────────────── */

    /// <summary>Scenario 1 for memory: <c>total_server_memory_mb / target_server_memory_mb * 100</c>, shifted
    /// for the last two hours of a 4h window. Sized above
    /// <see cref="AnomalyThresholds.MemoryPressureFloorPct"/> (90).</summary>
    [Fact]
    public async Task Memory_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        const int serverId = -8006;
        const double mu = 60.0;
        const double sigma = 2.0;
        const double shift = 96.0; // clears MemoryPressureFloorPct (90)
        const double targetMb = 100_000.0;

        const string historySql = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
SELECT -9300000 - (d * 10000 + h * 100 + i), $1 - to_days(d) + to_hours(h) + to_minutes(i * 5), $2, 'TileServer',
       $6 * 1.2, $6 * 0.2, $6, GREATEST(0, $6 * ($3 + $4 * sin(d * 4 + h * 12 + i)) / 100.0), GREATEST(0, $6 * ($3 + $4 * sin(d * 4 + h * 12 + i)) / 100.0)
FROM range(1, 22) AS d(d)
CROSS JOIN range(0, 4) AS hh(h)
CROSS JOIN range(0, 12) AS ii(i)";
        await ExecuteSeedAsync(historySql, WindowStartT, serverId, mu, sigma, 0, targetMb);

        const string windowSql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= 2 THEN $3 ELSE $1 + $2 * sin(h * 12 + i) END AS pct
    FROM range(0, 4) AS hh(h)
    CROSS JOIN range(0, 12) AS ii(i)
)
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
SELECT -9800001 - (h * 100 + i), $4 + to_hours(h) + to_minutes(i * 5), $5, 'TileServer',
       $6 * 1.2, $6 * 0.2, $6, GREATEST(0, $6 * pct / 100.0), GREATEST(0, $6 * pct / 100.0)
FROM s";
        await ExecuteSeedAsync(windowSql, mu, sigma, shift, WindowStartT, serverId, targetMb);

        await SeedCanaryAsync(serverId, WindowStartT);

        var startBucket = await _baselineProvider.GetBaselineAsync(serverId, MetricNames.Memory, WindowStartT);
        Assert.True(startBucket.IsTrustworthy, "the start-hour memory-pressure bucket must be trustworthy");
        var wholeWindowMean = (mu + shift) / 2.0;
        var wholeWindowMeanSigma = startBucket.EffectiveRobustSigma > 0
            ? (wholeWindowMean - startBucket.Median) / startBucket.EffectiveRobustSigma
            : (wholeWindowMean - startBucket.Mean) / startBucket.EffectiveStdDev;
        var referenceCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.Memory);
        Assert.True(wholeWindowMeanSigma < referenceCutoff,
            $"the whole-window memory-pressure mean deviation ({wholeWindowMeanSigma}) unexpectedly cleared the cutoff on its own");

        var facts = await _detector.DetectAnomaliesAsync(FourHourContext(serverId, WindowStartT));
        var fact = Assert.Single(facts, f => f.Key == "ANOMALY_MEMORY_PRESSURE");

        Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained memory-pressure shift should have scored through the tile path");
        Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
        Assert.Equal(4, fact.Metadata["tiles_scored"]);
        Assert.Equal(2, fact.Metadata["tiles_fired"]);
        Assert.Equal(referenceCutoff, fact.Metadata["fire_threshold"], precision: 6);
    }
}

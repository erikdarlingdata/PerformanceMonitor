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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A8 option B (lane L2b, PR #4177): behaviour tests proving the SQL Server-store batch-requests,
/// sessions, query-duration and memory detectors score each hour of the analysis window against its own
/// hour-of-week baseline, instead of the window as a whole. Sibling of
/// <see cref="SqlServerStoreTileBehaviourTests"/> (CPU, waits, I/O) — same rig, same seeding style: a
/// Wednesday-anchored window, 21 days of deterministic pseudo-noise history (about 12 samples/hour), a
/// set-based INSERT per hour, no row-per-roundtrip loop, every seeded time truncated to whole seconds
/// (#4155).
///
/// <para>First family (batch requests): all four scenarios. Query duration and memory get scenario 1 only,
/// per the lane brief. Sessions is skipped here: <c>AnomalyNullWindowLiveTests</c> already plants a
/// sessions shift.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class SqlServerStoreTileBehaviourBatchQueryMemoryTests
{
    /* Wednesday 2026-01-14, 10:00 — the fixed analysis-window start every scenario anchors to. No
       server_properties row is seeded, so keying is UTC. Matches the sibling file's own T. */
    private static readonly DateTime T = new(2026, 1, 14, 10, 0, 0, DateTimeKind.Unspecified);

    private const double BatchMu = 1000.0;
    private const double BatchSigmaAmp = 100.0; // deterministic pseudo-noise amplitude: mu + sigmaAmp * sin(i)

    // ───────────────────────── batch requests: all four scenarios ─────────────────────────

    /// <summary>
    /// Scenario 1 — the sustained 2 h shift a 4 h whole-window gate misses. Window [T, T+4h): hours 1-2
    /// (local 10, 11) sit at the batch-requests baseline; hours 3-4 (local 12, 13) run the WHOLE hour at
    /// mu + 6 sigma. The tiled gate fires on the worst tile (local hour 12 or 13); the same shift judged as
    /// a single 4 h average would not clear the cutoff — proven in the same test by computing the
    /// whole-window mean directly and showing its z against the baseline is under the batch-requests
    /// cutoff (the case this lane exists for).
    /// </summary>
    [Fact]
    public async Task BatchRequests_TwoHourSustainedShift_FiresOnWorstTile_WhereWholeWindowMeanWouldNotClearCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4177_01;
        const string serverName = "sqlstore-tile-batch-shift-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupBatchAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            // 21 days of history for local hours 10 and 11 of every Wednesday-of-week in range — 12 samples/hour.
            await SeedBatchBaselineAsync(connection, serverId, serverName, T, new[] { 10, 11 }, ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = T,
                TimeRangeEnd = T.AddHours(4),
                ServerUtcOffset = TimeSpan.Zero
            };

            // Hours 1-2 (local 10, 11) at baseline; hours 3-4 (local 12, 13) run the WHOLE hour at mu + 6 sigma.
            await SeedBatchHourAsync(connection, serverId, serverName, T, 0, seededHigh: false, ct);
            await SeedBatchHourAsync(connection, serverId, serverName, T, 1, seededHigh: false, ct);
            await SeedBatchHourAsync(connection, serverId, serverName, T, 2, seededHigh: true, ct);
            await SeedBatchHourAsync(connection, serverId, serverName, T, 3, seededHigh: true, ct);

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_BATCH_REQUESTS"));

            var tileLocalHour = fact.Metadata["tile_local_hour"];
            Assert.True(tileLocalHour == 12 || tileLocalHour == 13, $"expected tile_local_hour 12 or 13, got {tileLocalHour}");
            Assert.Equal(4.0, fact.Metadata["tiles_scored"]);
            Assert.Equal(2.0, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.DefaultDeviationThreshold, fact.Metadata["fire_threshold"], 0.001); // 4h window, no Sidak raise

            // What the whole-window gate would have seen: the window MEAN across all 4 hours, judged as one
            // z against the baseline mean/stddev — the case tiling exists for.
            var wholeMean = await ReadBatchWindowMeanAsync(connection, serverId, T, T.AddHours(4), ct);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.BatchRequests, T.AddHours(-24), ct);
            var wholeWindowZ = (wholeMean - baseline.Mean) / baseline.EffectiveStdDev;
            Assert.True(wholeWindowZ < AnomalyThresholds.DefaultDeviationThreshold,
                $"the whole-window mean's z ({wholeWindowZ:F2}) must stay under the batch-requests cutoff for this to be the case tiling exists for");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupBatchAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>
    /// Scenario 2 — the same 2 h shift, but placed at hours 22-23 of a 24 h <c>as_of</c> window
    /// [T-22h, T+2h). The baseline must cover those 24 local hours of week. Fires, and <c>fire_threshold</c>
    /// is the Sidak-raised <c>NAwarePeakCutoff</c> for a 24 h window (+-0.01).
    /// </summary>
    [Fact]
    public async Task BatchRequests_TwoHourShift_In24HourWindow_FiresWithRaisedCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4177_02;
        const string serverName = "sqlstore-tile-batch-24h-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupBatchAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var windowStart = T.AddHours(-22);
            var windowEnd = T.AddHours(2);
            // All 24 local hours the window's 24 tiles will fall on need history; simplest correct statement:
            // seed every hour 0..23 for the window's own weekday-span.
            await SeedBatchBaselineAsync(connection, serverId, serverName, windowStart, Enumerable.Range(0, 24).ToArray(), ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };

            // 24 hourly tiles across the window; every hour at baseline except hours 22-23 (local hour of T-2h,T-1h)
            // which run the whole hour at mu + 6 sigma.
            for (var h = 0; h < 24; h++)
            {
                var hourStart = windowStart.AddHours(h);
                var seededHigh = h == 22 || h == 23;
                await SeedBatchHourAsync(connection, serverId, serverName, hourStart, 0, seededHigh, ct);
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_BATCH_REQUESTS"));

            var expectedThreshold = AnomalyThresholds.NAwarePeakCutoff(AnomalyThresholds.DefaultDeviationThreshold, TimeSpan.FromHours(24));
            Assert.Equal(expectedThreshold, fact.Metadata["fire_threshold"], 0.01);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupBatchAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>Scenario 3 — a lone spike (ONE sample at mu + 10 sigma in hour 2) inside an otherwise-baseline
    /// 4 h window does not fire: a single hot sample is not a sustained shift, and the tile's MEAN clause
    /// keeps it quiet even though the tile's peak clears the magnitude floor.</summary>
    [Fact]
    public async Task BatchRequests_LoneSpike_DoesNotFire()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4177_03;
        const string serverName = "sqlstore-tile-batch-lonespike-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupBatchAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await SeedBatchBaselineAsync(connection, serverId, serverName, T, new[] { 10, 11, 12, 13 }, ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = T,
                TimeRangeEnd = T.AddHours(4),
                ServerUtcOffset = TimeSpan.Zero
            };

            for (var h = 0; h < 4; h++)
                await SeedBatchHourAsync(connection, serverId, serverName, T, h, seededHigh: false, ct);

            // ONE sample at mu + 10 sigma in hour 2 (local hour 12), extra row on top of the baseline-shaped hour.
            var stdDev = BatchBaselineStdDev();
            var spikeAt = TruncateToSeconds(T.AddHours(2).AddMinutes(37));
            await InsertBatchRowAsync(connection, serverId, serverName, spikeAt, BatchMu + 10 * stdDev, ct);

            var facts = await detector.DetectAnomaliesAsync(context);
            Assert.DoesNotContain(facts, f => f.Key == "ANOMALY_BATCH_REQUESTS");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupBatchAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>Scenario 4 — the fallback path. A 4 h window with only 2 samples per hour (under
    /// <see cref="AnomalyThresholds.MinTileSamples"/> = 3) at mu + 6 sigma still fires, through today's
    /// whole-window path — and the fact has NO <c>tile_local_hour</c> key, proving <c>EvaluateTiles</c>
    /// returned null and the caller fell back.</summary>
    [Fact]
    public async Task BatchRequests_UnderMinTileSamples_FallsBackToWholeWindow_NoTileKeys()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4177_04;
        const string serverName = "sqlstore-tile-batch-fallback-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupBatchAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await SeedBatchBaselineAsync(connection, serverId, serverName, T, new[] { 10, 11, 12, 13 }, ct);
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = T,
                TimeRangeEnd = T.AddHours(4),
                ServerUtcOffset = TimeSpan.Zero
            };

            var stdDev = BatchBaselineStdDev();
            var shiftedValue = BatchMu + 6 * stdDev;
            for (var h = 0; h < 4; h++)
            {
                var hourStart = T.AddHours(h);
                for (var i = 0; i < 2; i++) // under MinTileSamples (3) — every tile falls back
                    await InsertBatchRowAsync(connection, serverId, serverName, TruncateToSeconds(hourStart.AddMinutes(20 * i)), shiftedValue, ct);
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_BATCH_REQUESTS"));

            Assert.False(fact.Metadata.ContainsKey("tile_local_hour"), "the never-blind fallback must not carry tile keys");
            Assert.False(fact.Metadata.ContainsKey("tile_day_of_week"));
            Assert.False(fact.Metadata.ContainsKey("tiles_scored"));
            Assert.False(fact.Metadata.ContainsKey("tiles_fired"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupBatchAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    // ───────────────────────── query duration: scenario 1 only ─────────────────────────

    /// <summary>Query duration — scenario 1 only. Seeds <c>query_stats</c> so
    /// <c>QueryDurationTileWindowSql</c>'s per-collection SUM(delta_elapsed_time) sees one query per
    /// collection. Hours 1-2 (local 10, 11) at the baseline's own cycling elapsed-time shape; hours 3-4
    /// (local 12, 13) run the WHOLE hour at mu + 6 sigma (still clearing the 1-second magnitude floor).</summary>
    [Fact]
    public async Task QueryDuration_TwoHourSustainedShift_FiresOnWorstTile_WhereWholeWindowMeanWouldNotClearCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4177_05;
        const string serverName = "sqlstore-tile-qd-shift-e2e";
        const long baseElapsedUs = 2_000_000L; // 2s per collection — clears the 1s magnitude floor comfortably

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupQueryDurationAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var id = 900_000L;
            // 21 Wednesdays of history at local hours 10 and 11, 12 collections/hour (5-min spacing), one
            // query per collection at a fixed 2s elapsed — a near-constant baseline still clears the
            // absolute-floor rule's own AbsStdDevFloor via the robust scaffold's floored dispersion.
            for (var week = 0; week < 21; week++)
            {
                foreach (var hourOffset in new[] { 0, 1 })
                {
                    var hourStart = T.AddDays(-7 * week).AddHours(hourOffset);
                    for (var i = 0; i < 12; i++)
                        await InsertQueryStatsRowAsync(connection, id++, TruncateToSeconds(hourStart.AddMinutes(5 * i)), serverId, serverName, baseElapsedUs, ct);
                }
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.QueryDuration, T.AddHours(-24), ct);
            var sigmaEff = baseline.EffectiveRobustSigma > 0 ? baseline.EffectiveRobustSigma : baseline.EffectiveStdDev;
            var start = baseline.EffectiveRobustSigma > 0 ? baseline.Median : baseline.Mean;

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = T,
                TimeRangeEnd = T.AddHours(4),
                ServerUtcOffset = TimeSpan.Zero
            };

            // Hours 1-2 (local 10, 11) at baseline; hours 3-4 (local 12, 13) run the WHOLE hour at
            // start + 6 sigma_eff (sized from the bucket actually read, per WAVE2 lessons).
            var shiftedElapsedUs = (long)(start + 6 * sigmaEff);
            for (var h = 0; h < 4; h++)
            {
                var hourStart = T.AddHours(h);
                var seededHigh = h >= 2;
                for (var i = 0; i < 12; i++)
                {
                    var at = TruncateToSeconds(hourStart.AddMinutes(5 * i));
                    var elapsedUs = seededHigh ? shiftedElapsedUs : baseElapsedUs;
                    await InsertQueryStatsRowAsync(connection, id++, at, serverId, serverName, elapsedUs, ct);
                }
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_QUERY_DURATION"));

            var tileLocalHour = fact.Metadata["tile_local_hour"];
            Assert.True(tileLocalHour == 12 || tileLocalHour == 13, $"expected tile_local_hour 12 or 13, got {tileLocalHour}");
            Assert.Equal(4.0, fact.Metadata["tiles_scored"]);
            Assert.Equal(2.0, fact.Metadata["tiles_fired"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupQueryDurationAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    // ───────────────────────── memory: scenario 1 only ─────────────────────────

    /// <summary>Memory — scenario 1 only. Seeds <c>memory_stats</c> so <c>MemoryTileWindowSql</c> reads
    /// total/target as memory-pressure %. Hours 1-2 (local 10, 11) at the baseline's own cycling pressure;
    /// hours 3-4 (local 12, 13) run the WHOLE hour at mu + 6 sigma (still clearing the 90% magnitude
    /// floor).</summary>
    [Fact]
    public async Task Memory_TwoHourSustainedShift_FiresOnWorstTile_WhereWholeWindowMeanWouldNotClearCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4177_06;
        const string serverName = "sqlstore-tile-mem-shift-e2e";
        const decimal targetMb = 1000m;
        const decimal baseTotalMb = 850m; // 85% pressure — a near-constant baseline

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupMemoryAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var id = 900_000L;
            for (var week = 0; week < 21; week++)
            {
                foreach (var hourOffset in new[] { 0, 1 })
                {
                    var hourStart = T.AddDays(-7 * week).AddHours(hourOffset);
                    for (var i = 0; i < 12; i++)
                        await InsertMemoryRowAsync(connection, id++, TruncateToSeconds(hourStart.AddMinutes(5 * i)), serverId, serverName, targetMb, baseTotalMb, ct);
                }
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.Memory, T.AddHours(-24), ct);
            var sigmaEff = baseline.EffectiveRobustSigma > 0 ? baseline.EffectiveRobustSigma : baseline.EffectiveStdDev;
            var start = baseline.EffectiveRobustSigma > 0 ? baseline.Median : baseline.Mean;

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = T,
                TimeRangeEnd = T.AddHours(4),
                ServerUtcOffset = TimeSpan.Zero
            };

            // Shift sized off the bucket actually read; clamp to the 90% magnitude floor plus a margin so
            // the shift still clears it comfortably even when the near-constant baseline floors dispersion.
            var shiftedPct = Math.Max(start + 6 * sigmaEff, 96.0);
            var shiftedTotalMb = (decimal)(shiftedPct / 100.0) * targetMb;
            for (var h = 0; h < 4; h++)
            {
                var hourStart = T.AddHours(h);
                var seededHigh = h >= 2;
                for (var i = 0; i < 12; i++)
                {
                    var at = TruncateToSeconds(hourStart.AddMinutes(5 * i));
                    var totalMb = seededHigh ? shiftedTotalMb : baseTotalMb;
                    await InsertMemoryRowAsync(connection, id++, at, serverId, serverName, targetMb, totalMb, ct);
                }
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_MEMORY_PRESSURE"));

            var tileLocalHour = fact.Metadata["tile_local_hour"];
            Assert.True(tileLocalHour == 12 || tileLocalHour == 13, $"expected tile_local_hour 12 or 13, got {tileLocalHour}");
            Assert.Equal(4.0, fact.Metadata["tiles_scored"]);
            Assert.Equal(2.0, fact.Metadata["tiles_fired"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupMemoryAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    // ───────────────────────── mutation check helper (documented in the PR body, not a test) ─────────────────────────
    // See the PR body's "## Behaviour tests" section for the mutation-check procedure and result.

    // ───────────────────────── shared helpers ─────────────────────────

    /// <summary>The 21-Wednesday batch-requests baseline's population stddev for the deterministic
    /// pseudo-noise shape (mu + sigmaAmp * sin(i), i = 0..11, repeated 21 times) — computed once,
    /// closed-form, so every scenario's "6 sigma" shift is the same number the live baseline will
    /// actually report.</summary>
    private static double BatchBaselineStdDev()
    {
        var vals = Enumerable.Range(0, 12).Select(i => BatchMu + BatchSigmaAmp * Math.Sin(i)).ToArray();
        var mean = vals.Average();
        var sumSq = vals.Sum(v => (v - mean) * (v - mean));
        return Math.Sqrt(sumSq / (vals.Length - 1)); // sample stddev, matches STDDEV_SAMP
    }

    /// <summary>Seeds 21 days (weeks) of batch-requests history for the given local hours-of-day, anchored
    /// to the same weekday as <paramref name="anchor"/>, one set-based INSERT per hour. Each row carries a
    /// measured 60s interval so <c>delta_cntr_value / sample_interval_seconds</c> rates at mu + sigmaAmp *
    /// sin(i) requests/sec exactly.</summary>
    private static async Task SeedBatchBaselineAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime anchor, int[] hours, System.Threading.CancellationToken ct)
    {
        foreach (var hour in hours)
        {
            var hourAnchor = TruncateToSeconds(new DateTime(anchor.Year, anchor.Month, anchor.Day, hour, 0, 0, DateTimeKind.Unspecified));
            await using var plant = new NpgsqlCommand($@"
INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT {800_000_000L + hour}::bigint + w * 100 + i, ts, {serverId}, '{serverName}', 'SQLServer:SQL Statistics', 'Batch Requests/sec', '',
       (({BatchMu} + {BatchSigmaAmp} * sin(i)) * 60)::bigint, (({BatchMu} + {BatchSigmaAmp} * sin(i)) * 60)::bigint, 60
FROM generate_series(1, 21) w
CROSS JOIN generate_series(0, 11) i
CROSS JOIN LATERAL (SELECT $1::timestamp - (w * 7) * INTERVAL '1 day' + i * INTERVAL '5 minutes') AS _(ts)", connection);
            plant.Parameters.AddWithValue(hourAnchor);
            await plant.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>Seeds one hour's 12 batch-requests samples, set-based: at baseline shape, or the WHOLE hour
    /// shifted to mu + 6 sigma. Every row carries a measured 60s interval.</summary>
    private static async Task SeedBatchHourAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hourOffset, bool seededHigh, System.Threading.CancellationToken ct)
    {
        var hourStart = TruncateToSeconds(windowStart.AddHours(hourOffset));
        var shiftedValue = BatchMu + 6 * BatchBaselineStdDev();
        var shiftedExpr = shiftedValue.ToString(System.Globalization.CultureInfo.InvariantCulture);
        await using var plant = new NpgsqlCommand($@"
INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT {900_000_000L}::bigint + {hourOffset} * 100 + i, ts, {serverId}, '{serverName}', 'SQLServer:SQL Statistics', 'Batch Requests/sec', '',
       ({(seededHigh ? shiftedExpr : $"{BatchMu} + {BatchSigmaAmp} * sin(i)")}) * 60,
       (({(seededHigh ? shiftedExpr : $"{BatchMu} + {BatchSigmaAmp} * sin(i)")}) * 60)::bigint, 60
FROM generate_series(0, 11) i
CROSS JOIN LATERAL (SELECT $1::timestamp + i * INTERVAL '5 minutes') AS _(ts)", connection);
        plant.Parameters.AddWithValue(hourStart);
        await plant.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertBatchRowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, double ratePerSec, System.Threading.CancellationToken ct)
    {
        var delta = (long)(ratePerSec * 60);
        await InsertAsync(connection,
            "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), at, serverId, serverName, "SQLServer:SQL Statistics", "Batch Requests/sec", "", delta, delta, 60);
    }

    private static async Task<double> ReadBatchWindowMeanAsync(NpgsqlConnection connection, int serverId, DateTime start, DateTime end, System.Threading.CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            @"SELECT AVG(delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0)) FROM v_perfmon_stats
              WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
              AND counter_name = 'Batch Requests/sec' AND delta_cntr_value >= 0 AND sample_interval_seconds > 0", connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(start);
        cmd.Parameters.AddWithValue(end);
        return Convert.ToDouble(await cmd.ExecuteScalarAsync(ct));
    }

    private static async Task InsertQueryStatsRowAsync(NpgsqlConnection connection, long id, DateTime at, int serverId, string serverName, long elapsedUs, System.Threading.CancellationToken ct)
    {
        var queryHash = "0xTILEQD" + id.ToString(System.Globalization.CultureInfo.InvariantCulture);
        await InsertAsync(connection,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
            id, at, serverId, serverName, "tiledb", queryHash, "0xPLANHASH", "0xSQLH" + id, "0xPLANH" + id,
            "SELECT 1", 1L, elapsedUs, elapsedUs, 10L, 1, 1);
    }

    private static async Task InsertMemoryRowAsync(NpgsqlConnection connection, long id, DateTime at, int serverId, string serverName, decimal targetMb, decimal totalMb, System.Threading.CancellationToken ct)
    {
        await InsertAsync(connection,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, target_server_memory_mb, total_server_memory_mb) VALUES ($1, $2, $3, $4, $5, $6)",
            id, at, serverId, serverName, targetMb, totalMb);
    }

    private static DateTime TruncateToSeconds(DateTime dt) =>
        DateTime.SpecifyKind(new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Kind == DateTimeKind.Unspecified ? DateTimeKind.Unspecified : dt.Kind), DateTimeKind.Unspecified);

    private static async Task InsertAsync(NpgsqlConnection connection, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
            command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task CleanupBatchAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand($"DELETE FROM perfmon_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupQueryDurationAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand($"DELETE FROM query_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupMemoryAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand($"DELETE FROM memory_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DropBaselineFallbackViewsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var (_, view) in TimescaleSupport.BaselineAggregates)
        {
            using var drop = new NpgsqlCommand(TimescaleSupport.DropBaselineFallbackViewSql(view), connection);
            await drop.ExecuteNonQueryAsync(ct);
        }
    }
}

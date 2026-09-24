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
/// #3653 A8 option B (lane L2a, PR #4172): behaviour tests proving the SQL Server-store CPU, waits and I/O
/// detectors score each hour of the analysis window against its own hour-of-week baseline, instead of the
/// window as a whole. Every scenario below plants a Wednesday-anchored window, seeds a 21-day baseline (about
/// 12 samples per hour) with deterministic pseudo-noise so the bucket's median/mean and spread are known, and
/// runs the real <see cref="PgAnomalyDetector"/> against a live TimescaleDB. Set-based INSERTs
/// (<c>generate_series</c>) only — no row-per-roundtrip loop. Every seeded time is truncated to whole seconds
/// (a .NET tick is not stored by PostgreSQL, and Windows CI resolves ticks a macOS/Linux run does not — #4155).
///
/// <para>First family (CPU): all four scenarios. The other two families (waits, I/O) get scenario 1 only, per
/// the lane brief.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class SqlServerStoreTileBehaviourTests
{
    /* Wednesday 2026-01-14, 10:00 — the fixed analysis-window start every scenario anchors to. No
       server_properties row is seeded, so keying is UTC. */
    private static readonly DateTime T = new(2026, 1, 14, 10, 0, 0, DateTimeKind.Unspecified);
    private const int Wednesday = (int)DayOfWeek.Wednesday;

    // mu = 30, sigmaAmp = 5 gives a sample stddev of about 3.69 (CpuBaselineStdDev, below), so mu + 6 sigma is
    // about 52.1 — clears CpuFloorPct (50.0 %) with margin, which mu = 20 amp = 2 (stddev about 1.48, mu + 6
    // sigma about 28.9) never did (#4172 lane T4172-2 diagnosis: no fact fired because nothing cleared the floor).
    private const double CpuMu = 30.0;
    private const double CpuSigmaAmp = 5.0; // deterministic pseudo-noise amplitude: mu + sigmaAmp * sin(i)

    // ───────────────────────── CPU: all four scenarios ─────────────────────────

    /// <summary>
    /// Scenario 1 — the sustained 2 h shift a 4 h whole-window gate misses. Window [T, T+4h): hours 1-2 (local
    /// 10, 11) sit at the CPU baseline; hours 3-4 (local 12, 13) run the WHOLE hour at mu + 6 sigma. The tiled
    /// gate fires on the worst tile (local hour 12 or 13); the same shift judged as a single 4 h average would
    /// not clear the cutoff — proven in the same test by computing the whole-window mean directly and showing
    /// its z against the baseline is under the CPU cutoff (the case this lane exists for).
    /// </summary>
    [Fact]
    public async Task Cpu_TwoHourSustainedShift_FiresOnWorstTile_WhereWholeWindowMeanWouldNotClearCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4172_01;
        const string serverName = "sqlstore-tile-cpu-shift-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupCpuAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            // 21 days of history for local hours 10 and 11 of every Wednesday-of-week in range — 12 samples/hour.
            await SeedCpuBaselineAsync(connection, serverId, serverName, T, new[] { 10, 11 }, ct);
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
            await SeedCpuHourAsync(connection, serverId, serverName, T, 0, seededHigh: false, ct);
            await SeedCpuHourAsync(connection, serverId, serverName, T, 1, seededHigh: false, ct);
            await SeedCpuHourAsync(connection, serverId, serverName, T, 2, seededHigh: true, ct);
            await SeedCpuHourAsync(connection, serverId, serverName, T, 3, seededHigh: true, ct);

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_CPU_SPIKE"));

            var tileLocalHour = fact.Metadata["tile_local_hour"];
            Assert.True(tileLocalHour == 12 || tileLocalHour == 13, $"expected tile_local_hour 12 or 13, got {tileLocalHour}");
            Assert.Equal(4.0, fact.Metadata["tiles_scored"]);
            Assert.Equal(2.0, fact.Metadata["tiles_fired"]);
            // The CPU baseline carries a positive robust sigma (deterministic sin(i) noise), so the tiled gate's
            // trustworthy path runs the MODIFIED-z frame (AnomalyGate.DecideRobustFirst), not the classical one —
            // ModifiedZThreshold (3.5), not DefaultDeviationThreshold (2.0). 4h window, no Sidak raise.
            Assert.Equal(AnomalyThresholds.ModifiedZThreshold, fact.Metadata["fire_threshold"], 0.001);

            // What the whole-window gate would have seen: the window MEAN across all 4 hours, judged as one
            // z against the baseline the detector actually trusts — the robust (median/MAD) frame, since this
            // baseline carries a positive robust sigma and the tiled/whole-window gates both run the modified-z
            // path (ModifiedZThreshold, 3.5) on it, not the classical DefaultDeviationThreshold path.
            var wholeMean = await ReadWindowMeanAsync(connection, serverId, T, T.AddHours(4), ct);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.Cpu, T.AddHours(-24), ct);
            var wholeWindowZ = (wholeMean - baseline.Median) / baseline.EffectiveRobustSigma;
            Assert.True(wholeWindowZ < AnomalyThresholds.ModifiedZThreshold,
                $"the whole-window mean's z ({wholeWindowZ:F2}) must stay under the CPU cutoff for this to be the case tiling exists for");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupCpuAsync(cleanup, serverId, cleanupCt);
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
    public async Task Cpu_TwoHourShift_In24HourWindow_FiresWithRaisedCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4172_02;
        const string serverName = "sqlstore-tile-cpu-24h-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupCpuAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var windowStart = T.AddHours(-22);
            var windowEnd = T.AddHours(2);
            var hoursOfWeek = Enumerable.Range(0, 24).Select(h => ((windowStart.Hour + h) % 24)).Distinct().ToArray();
            // All 24 local hours the window's 24 tiles will fall on need history; simplest correct statement:
            // seed every hour 0..23 for the window's own weekday-span.
            await SeedCpuBaselineAsync(connection, serverId, serverName, windowStart, Enumerable.Range(0, 24).ToArray(), ct);
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
                await SeedCpuHourAsync(connection, serverId, serverName, hourStart, 0, seededHigh, ct);
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_CPU_SPIKE"));

            // The robust (modified-z) frame runs here too (see scenario 1's note), so the Sidak raise is over
            // ModifiedZThreshold (3.5), not the classical DefaultDeviationThreshold (2.0).
            var expectedThreshold = AnomalyThresholds.NAwarePeakCutoff(AnomalyThresholds.ModifiedZThreshold, TimeSpan.FromHours(24));
            Assert.Equal(expectedThreshold, fact.Metadata["fire_threshold"], 0.01);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupCpuAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>Scenario 3 — a lone spike (ONE sample at mu + 10 sigma in hour 2) inside an otherwise-baseline
    /// 4 h window does not fire: a single hot sample is not a sustained shift, and the tile's MEAN clause keeps
    /// it quiet even though the tile's peak clears the magnitude floor.</summary>
    [Fact]
    public async Task Cpu_LoneSpike_DoesNotFire()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4172_03;
        const string serverName = "sqlstore-tile-cpu-lonespike-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupCpuAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await SeedCpuBaselineAsync(connection, serverId, serverName, T, new[] { 10, 11, 12, 13 }, ct);
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
                await SeedCpuHourAsync(connection, serverId, serverName, T, h, seededHigh: false, ct);

            // ONE sample at mu + 10 sigma in hour 2 (local hour 12), extra row on top of the baseline-shaped hour.
            var stdDev = CpuBaselineStdDev();
            var spikeAt = TruncateToSeconds(T.AddHours(2).AddMinutes(30));
            await InsertCpuRowAsync(connection, serverId, serverName, spikeAt, CpuMu + 10 * stdDev, ct);

            var facts = await detector.DetectAnomaliesAsync(context);
            Assert.DoesNotContain(facts, f => f.Key == "ANOMALY_CPU_SPIKE");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupCpuAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>Scenario 4 — the fallback path. A 4 h window with only 2 samples per hour (under
    /// <see cref="AnomalyThresholds.MinTileSamples"/> = 3) at mu + 6 sigma still fires, through today's
    /// whole-window path — and the fact has NO <c>tile_local_hour</c> key, proving <c>EvaluateTiles</c> returned
    /// null and the caller fell back.</summary>
    [Fact]
    public async Task Cpu_UnderMinTileSamples_FallsBackToWholeWindow_NoTileKeys()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4172_04;
        const string serverName = "sqlstore-tile-cpu-fallback-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupCpuAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await SeedCpuBaselineAsync(connection, serverId, serverName, T, new[] { 10, 11, 12, 13 }, ct);
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

            var stdDev = CpuBaselineStdDev();
            var shiftedValue = CpuMu + 6 * stdDev;
            for (var h = 0; h < 4; h++)
            {
                var hourStart = T.AddHours(h);
                for (var i = 0; i < 2; i++) // under MinTileSamples (3) — every tile falls back
                    await InsertCpuRowAsync(connection, serverId, serverName, TruncateToSeconds(hourStart.AddMinutes(20 * i)), shiftedValue, ct);
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_CPU_SPIKE"));

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
                await CleanupCpuAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    // ───────────────────────── waits: scenario 1 only ─────────────────────────

    /// <summary>Waits — scenario 1 only. Copies <c>EndToEnd_WaitProfileDetector_PeakAndMeanPair_…</c>'s cumulative
    /// wait_stats seeding (per-collection totals rated by the LAG interval). The robust arm is the only tiled
    /// arm, so the baseline must carry a positive robust sigma. Hours 1-2 (local 10, 11) rate at the baseline
    /// median (200 ms/sec); hours 3-4 (local 12, 13) run the WHOLE hour at 6x the robust sigma above the
    /// median.</summary>
    [Fact]
    public async Task Waits_TwoHourSustainedShift_FiresOnWorstTile_WhereWholeWindowMeanWouldNotClearCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4172_05;
        const string serverName = "sqlstore-tile-waits-shift-e2e";
        const string waitType = "TILE_TEST_WAIT";
        const string insertWait =
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupWaitsAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            // 21 Wednesdays of history at local hours 10 and 11, 12 collections/hour (5-min spacing), rates
            // cycling 100/200/300 ms/sec exactly as the sibling live test does — median 200, MAD 100.
            var id = 900_000L;
            for (var week = 0; week < 21; week++)
            {
                foreach (var hourOffset in new[] { 0, 1 })
                {
                    var hourStart = T.AddDays(-7 * week).AddHours(hourOffset);
                    for (var i = 0; i < 12; i++)
                    {
                        var totalMs = (i % 3) switch { 0 => 30000L, 1 => 60000L, _ => 90000L };
                        await InsertAsync(connection, insertWait, id++, TruncateToSeconds(hourStart.AddMinutes(5 * i)), serverId, serverName, waitType, 10L, totalMs);
                    }
                }
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.WaitMsPerSec, T.AddHours(-24), ct);
            Assert.True(baseline.EffectiveRobustSigma > 0, "the tiled arm requires a positive robust sigma");
            var robustSigma = baseline.EffectiveRobustSigma;

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = T,
                TimeRangeEnd = T.AddHours(4),
                ServerUtcOffset = TimeSpan.Zero
            };

            // Hours 1-2 (local 10, 11) at the baseline's own cycling rates; hours 3-4 (local 12, 13) run the
            // WHOLE hour at the median + 6 robust sigma.
            var shiftedTotalMs = (long)((baseline.Median + 6 * robustSigma) * 300); // ms/sec * 300s interval
            for (var h = 0; h < 4; h++)
            {
                var hourStart = T.AddHours(h);
                var seededHigh = h >= 2;
                for (var i = 0; i < 12; i++)
                {
                    var at = TruncateToSeconds(hourStart.AddMinutes(5 * i));
                    var totalMs = seededHigh ? shiftedTotalMs : (i % 3) switch { 0 => 30000L, 1 => 60000L, _ => 90000L };
                    await InsertAsync(connection, insertWait, id++, at, serverId, serverName, waitType, 10L, totalMs);
                }
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_WAIT_PROFILE"));

            var tileLocalHour = fact.Metadata["tile_local_hour"];
            Assert.True(tileLocalHour == 12 || tileLocalHour == 13, $"expected tile_local_hour 12 or 13, got {tileLocalHour}");
            Assert.Equal(4.0, fact.Metadata["tiles_scored"]);
            Assert.Equal(2.0, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.HeavyTailModifiedZThreshold, fact.Metadata["fire_threshold"], 0.001); // 4h window, no Sidak raise

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupWaitsAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    // ───────────────────────── I/O: scenario 1 only ─────────────────────────

    /// <summary>I/O — scenario 1 only. Copies <c>EndToEnd_IoDetector_…</c>'s file_io_stats seeding
    /// (delta_reads/delta_writes/delta_stall_read_ms rows). Hours 1-2 (local 10, 11) at the baseline read
    /// latency; hours 3-4 (local 12, 13) run the WHOLE hour at 6x the baseline's effective sigma above its
    /// mean.</summary>
    [Fact]
    public async Task Io_TwoHourSustainedShift_FiresOnWorstTile_WhereWholeWindowMeanWouldNotClearCutoff()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live tile-behaviour test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -4172_06;
        const string serverName = "sqlstore-tile-io-shift-e2e";
        const string insertIo =
            "INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, delta_reads, delta_writes, delta_stall_read_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupIoAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var id = 900_000L;
            // 21 Wednesdays of history at local hours 10 and 11, 12 rows/hour at a fixed 20 ms read latency
            // (10 reads, 200 ms stall — matches the sibling live test's 10/0/20 shape, MAD 0 -> the I/O
            // absolute floor drives EffectiveRobustSigma, exactly as the sibling test measures it).
            for (var week = 0; week < 21; week++)
            {
                foreach (var hourOffset in new[] { 0, 1 })
                {
                    var hourStart = T.AddDays(-7 * week).AddHours(hourOffset);
                    for (var i = 0; i < 12; i++)
                        await InsertAsync(connection, insertIo, id++, TruncateToSeconds(hourStart.AddMinutes(5 * i)), serverId, serverName, 10L, 0L, 200L);
                }
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(serverId, MetricNames.IoLatency, T.AddHours(-24), ct);

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = T,
                TimeRangeEnd = T.AddHours(4),
                ServerUtcOffset = TimeSpan.Zero
            };

            var shiftedStallMs = (long)((baseline.Mean + 6 * baseline.EffectiveRobustSigma) * 10); // 10 reads/row
            for (var h = 0; h < 4; h++)
            {
                var hourStart = T.AddHours(h);
                var seededHigh = h >= 2;
                for (var i = 0; i < 12; i++)
                {
                    var at = TruncateToSeconds(hourStart.AddMinutes(5 * i));
                    var stallMs = seededHigh ? shiftedStallMs : 200L;
                    await InsertAsync(connection, insertIo, id++, at, serverId, serverName, 10L, 0L, stallMs);
                }
            }

            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts.Where(f => f.Key == "ANOMALY_READ_LATENCY"));

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
                await CleanupIoAsync(cleanup, serverId, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    // ───────────────────────── shared helpers ─────────────────────────

    /// <summary>The 21-Wednesday CPU baseline's population stddev for the deterministic pseudo-noise shape
    /// (mu + sigmaAmp * sin(i), i = 0..11, repeated 21 times) — computed once, closed-form, so every scenario's
    /// "6 sigma" shift is the same number the live baseline will actually report.</summary>
    private static double CpuBaselineStdDev()
    {
        var vals = Enumerable.Range(0, 12).Select(i => CpuMu + CpuSigmaAmp * Math.Sin(i)).ToArray();
        var mean = vals.Average();
        var sumSq = vals.Sum(v => (v - mean) * (v - mean));
        return Math.Sqrt(sumSq / (vals.Length - 1)); // sample stddev, matches STDDEV_SAMP
    }

    /// <summary>Seeds 21 days (weeks) of CPU history for the given local hours-of-day, anchored to the same
    /// weekday as <paramref name="anchor"/>, one set-based INSERT per hour.</summary>
    private static async Task SeedCpuBaselineAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime anchor, int[] hours, System.Threading.CancellationToken ct)
    {
        foreach (var hour in hours)
        {
            var hourAnchor = TruncateToSeconds(new DateTime(anchor.Year, anchor.Month, anchor.Day, hour, 0, 0, DateTimeKind.Unspecified));
            await using var plant = new NpgsqlCommand($@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT {800_000_000L + hour}::bigint + w * 100 + i, ts, {serverId}, '{serverName}', ts, {CpuMu} + {CpuSigmaAmp} * sin(i), 5
FROM generate_series(1, 21) w
CROSS JOIN generate_series(0, 11) i
CROSS JOIN LATERAL (SELECT $1::timestamp - (w * 7) * INTERVAL '1 day' + i * INTERVAL '5 minutes') AS _(ts)", connection);
            plant.Parameters.AddWithValue(hourAnchor);
            await plant.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>Seeds one hour's 12 CPU samples, set-based: at baseline shape, or the WHOLE hour shifted to
    /// mu + 6 sigma.</summary>
    private static async Task SeedCpuHourAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hourOffset, bool seededHigh, System.Threading.CancellationToken ct)
    {
        var hourStart = TruncateToSeconds(windowStart.AddHours(hourOffset));
        var shiftedValue = CpuMu + 6 * CpuBaselineStdDev();
        await using var plant = new NpgsqlCommand($@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT {900_000_000L}::bigint + {hourOffset} * 100 + i, ts, {serverId}, '{serverName}', ts,
       {(seededHigh ? shiftedValue.ToString(System.Globalization.CultureInfo.InvariantCulture) : $"{CpuMu} + {CpuSigmaAmp} * sin(i)")}, 5
FROM generate_series(0, 11) i
CROSS JOIN LATERAL (SELECT $1::timestamp + i * INTERVAL '5 minutes') AS _(ts)", connection);
        plant.Parameters.AddWithValue(hourStart);
        await plant.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCpuRowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, double cpuValue, System.Threading.CancellationToken ct)
    {
        await InsertAsync(connection,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), at, serverId, serverName, at, cpuValue, 5);
    }

    private static async Task<double> ReadWindowMeanAsync(NpgsqlConnection connection, int serverId, DateTime start, DateTime end, System.Threading.CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT AVG(sqlserver_cpu_utilization) FROM v_cpu_utilization_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(start);
        cmd.Parameters.AddWithValue(end);
        return Convert.ToDouble(await cmd.ExecuteScalarAsync(ct));
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

    private static async Task CleanupCpuAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand($"DELETE FROM cpu_utilization_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupWaitsAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand($"DELETE FROM wait_stats WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupIoAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand($"DELETE FROM file_io_stats WHERE server_id = {serverId};", connection);
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

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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A8 option B (PR #4178): behaviour tests for the PostgreSQL target's tiled anomaly gate on the three
/// families this PR carries — WAL volume, CPU burn (the kernel family) and sampled waits (the robust arm only).
/// Each moves onto <see cref="AnomalyGate.EvaluateTiles"/>, scoring every target-local hour of the analysis
/// window against ITS OWN hour-of-week baseline instead of collapsing the whole window to one peak/mean pair, so
/// a sustained shift confined to part of the window fires where the old whole-window gate stayed quiet, and a
/// lone spike inside an otherwise-quiet window still does not fire. Every scenario runs through the REAL
/// <see cref="PgTargetAnomalyDetector.DetectAnomaliesAsync"/>, never a hand-copied tile call.
///
/// <para><b>First family: WAL volume.</b> All four scenarios. <b>CPU burn and sampled waits:</b> scenario 1 only,
/// per the coordinator's brief for a multi-family PR. Seeding copies <c>PgTargetWriteTests</c>' (WAL),
/// <c>PgTargetKernelTests</c>' (CPU) and <c>PgTargetSampledWaitTests</c>'/<c>PgTargetSampledWaitLiveTests</c>'
/// (sampled waits) live row shapes, generalised to <c>generate_series</c> tile seeding with 21 days of history so
/// every window hour-of-week clears <c>BaselineBucket</c>'s three-distinct-day floor (<c>FullDayMin = 3</c>).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetTileBehaviourWalKernelSampledTests
{
    // Wednesday 2026-01-07, 10:00 UTC. No server_properties row is seeded for any server here, so keying is UTC
    // and LocalHour.Hour/DayOfWeek read directly off these UTC instants.
    private static readonly DateTime WindowStartT = new(2026, 1, 7, 10, 0, 0, DateTimeKind.Unspecified);

    private static AnalysisContext FourHourContext(int serverId, string serverName) => new()
    {
        ServerId = serverId, ServerName = serverName, TimeRangeStart = WindowStartT, TimeRangeEnd = WindowStartT.AddHours(4), ServerUtcOffset = TimeSpan.Zero,
        Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 48 },
    };

    /* ───────────────────────── WAL volume: all four scenarios ───────────────────────── */

    private const string WalShiftServerName = "darling-pg-tile-wal-shift-4h";
    private static readonly int WalShiftServerId = ServerIdHelper.GetDeterministicHashCode(WalShiftServerName);

    private const long WalBaseBytesPerMinute = 1L * 1024 * 1024 * 60;   // 1 MiB/s routine
    // 30% above the routine rate: the routine's 5% deterministic ripple gives the bucket sigma ≈ 5% of its mean,
    // so this shift sits at ≈ mean + 6·sigma on the robust scale — the SAME "6σ" shape the design's scenario 1
    // asks for (its own peak clears the modified-z cutoff of 3.5; the two-hour-diluted whole-window average sits
    // at ≈ mean + 3·sigma, comfortably under it).
    private const long WalShiftBytesPerMinute = (long)(WalBaseBytesPerMinute * 1.3); // clears PgWalBytesFloorPerSec (1 MiB/s)

    /// <summary>
    /// Scenario 1 (design's "the shift, 4 h"): a 4 h window [T, T+4h) whose first two hours run the routine WAL
    /// rate and whose last two hours run the WHOLE hour at the shifted rate. The per-hour tile gate fires (hours
    /// 3/4 clear the cutoff against their own bucket); the SAME numbers, judged as one whole-window peak/mean
    /// pair against the START hour's bucket alone, do not clear the cutoff — the shift is diluted by the two
    /// quiet hours, exactly the case this lane exists to prove.
    /// </summary>
    [Fact]
    public async Task WalVolume_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteWalRowsAsync(connection, WalShiftServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, WalShiftServerId, WalShiftServerName, MonitoredEngineKind.Postgres, 18, ct);

            // 21 days of history for hours 10-13 of this weekday, ~12 samples/hour, deterministic pseudo-noise.
            await PlantWalHistoryAsync(connection, WalShiftServerId, WalShiftServerName, WindowStartT, 4, WalBaseBytesPerMinute, ct);
            // The window: hours 0-1 at the routine rate; hours 2-3 (local hours 12-13) the WHOLE hour at the shift.
            await PlantWalWindowAsync(connection, WalShiftServerId, WalShiftServerName, WindowStartT, 4, shiftFromHour: 2, WalBaseBytesPerMinute, WalShiftBytesPerMinute, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var startBucket = await baselines.GetBaselineAsync(WalShiftServerId, MetricNames.PgWalBytesPerSec, WindowStartT, ct);
            Assert.True(startBucket.IsTrustworthy, "the start-hour WAL bucket must be trustworthy");

            // What dev did: the whole window's own mean rate (two quiet hours + two shifted hours), judged against
            // the START hour's bucket alone — below the cutoff, because the quiet half of the window drags the
            // window average toward the routine rate. This is the dilution the tile path exists to undo.
            var wholeWindowMean = (WalBaseBytesPerMinute / 60.0 + WalShiftBytesPerMinute / 60.0) / 2.0;
            var wholeWindowMeanZ = BaselineMath.ModifiedZScore(startBucket, wholeWindowMean);
            Assert.True(wholeWindowMeanZ < AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec),
                $"the whole-window mean z ({wholeWindowMeanZ}) unexpectedly cleared the cutoff on its own — the shift was not actually diluted by construction (median={startBucket.Median}, sigma={startBucket.EffectiveRobustSigma})");

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(WalShiftServerId, WalShiftServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalyWalVolume);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained two-hour WAL shift should have scored through the tile path");
            Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
            Assert.Equal(4, fact.Metadata["tiles_scored"]);
            Assert.Equal(2, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec), fact.Metadata["fire_threshold"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWalRowsAsync(cleanup, WalShiftServerId, cleanupCt));
        }
    }

    private const string Wal24hServerName = "darling-pg-tile-wal-shift-24h";
    private static readonly int Wal24hServerId = ServerIdHelper.GetDeterministicHashCode(Wal24hServerName);

    /// <summary>
    /// Scenario 2 (design's "the shift, 24 h, the <c>as_of</c> pass"): the same 2 h sustained shift, but at the
    /// LAST two hours of a 24 h window [T−22h, T+2h). Fires, and <c>fire_threshold</c> is the Šidák-raised cutoff
    /// for a 24 h window, not the bare cutoff.
    /// </summary>
    [Fact]
    public async Task WalVolume_TwoHourShiftInTwentyFourHourWindow_Fires_WithTheRaisedCutoff()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteWalRowsAsync(connection, Wal24hServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, Wal24hServerId, Wal24hServerName, MonitoredEngineKind.Postgres, 18, ct);

            var windowStart = WindowStartT.AddHours(-22);
            var windowEnd = WindowStartT.AddHours(2);

            // 21 days of history covering all 24 hours of week the window spans.
            await PlantWalHistoryAsync(connection, Wal24hServerId, Wal24hServerName, windowStart, 24, WalBaseBytesPerMinute, ct);
            // The window: routine rate everywhere except the last two hours (local hours 22-23), which run the
            // WHOLE hour at the shift.
            await PlantWalWindowAsync(connection, Wal24hServerId, Wal24hServerName, windowStart, 24, shiftFromHour: 22, WalBaseBytesPerMinute, WalShiftBytesPerMinute, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var window = windowEnd - windowStart;
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var context = new AnalysisContext
            {
                ServerId = Wal24hServerId, ServerName = Wal24hServerName, TimeRangeStart = windowStart, TimeRangeEnd = windowEnd, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = (long)window.TotalMilliseconds, ObservedMs = (long)window.TotalMilliseconds, SampleCount = 288 },
            };
            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalyWalVolume);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained WAL shift at the tail of a 24h window should have scored through the tile path");
            var referenceCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec);
            var expectedRaised = AnomalyThresholds.NAwarePeakCutoff(referenceCutoff, window);
            Assert.InRange(fact.Metadata["fire_threshold"], expectedRaised - 0.01, expectedRaised + 0.01);
            Assert.True(expectedRaised > referenceCutoff, "a 24h window must raise the cutoff above the 4h reference value");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWalRowsAsync(cleanup, Wal24hServerId, cleanupCt));
        }
    }

    private const string WalSpikeServerName = "darling-pg-tile-wal-lone-spike";
    private static readonly int WalSpikeServerId = ServerIdHelper.GetDeterministicHashCode(WalSpikeServerName);

    /// <summary>
    /// Scenario 3 (design's "the lone spike"): a 4 h window at the routine rate throughout, plus ONE minute's
    /// increment at the shifted rate in hour 2. A single sample cannot drag its hour's tile mean past the (peak
    /// AND mean) pair gate, so it does not fire.
    /// </summary>
    [Fact]
    public async Task WalVolume_LoneSpikeInsideAnHour_DoesNotFire()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteWalRowsAsync(connection, WalSpikeServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, WalSpikeServerId, WalSpikeServerName, MonitoredEngineKind.Postgres, 18, ct);

            await PlantWalHistoryAsync(connection, WalSpikeServerId, WalSpikeServerName, WindowStartT, 4, WalBaseBytesPerMinute, ct);
            // The whole window at the routine rate, with ONE lone-minute spike inside hour 2 (local hour 12).
            await PlantWalWindowWithLoneSpikeAsync(connection, WalSpikeServerId, WalSpikeServerName, WindowStartT, WalBaseBytesPerMinute, WalShiftBytesPerMinute, spikeAtMinute: 2 * 60 + 30, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(WalSpikeServerId, WalSpikeServerName));

            Assert.DoesNotContain(facts, f => f.Key == PgTargetFactKeys.AnomalyWalVolume);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWalRowsAsync(cleanup, WalSpikeServerId, cleanupCt));
        }
    }

    private const string WalFallbackServerName = "darling-pg-tile-wal-fallback";
    private static readonly int WalFallbackServerId = ServerIdHelper.GetDeterministicHashCode(WalFallbackServerName);

    /// <summary>
    /// Scenario 4 (design's "the fallback"): a 4 h window with only 2 rated collections per hour (so every tile
    /// is under <see cref="AnomalyThresholds.MinTileSamples"/>, 3), all at the shifted rate. No tile can be
    /// scored, so the detector falls through to today's whole-window path; the resulting fact still fires, but
    /// carries no <c>tile_local_hour</c> key.
    /// </summary>
    [Fact]
    public async Task WalVolume_UnderMinTileSamplesEveryTile_FallsBackToTheWholeWindowPath_WithNoTileKeys()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteWalRowsAsync(connection, WalFallbackServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, WalFallbackServerId, WalFallbackServerName, MonitoredEngineKind.Postgres, 18, ct);

            await PlantWalHistoryAsync(connection, WalFallbackServerId, WalFallbackServerName, WindowStartT, 4, WalBaseBytesPerMinute, ct);
            // Only 2 rated collections per hour, all at the shift: every tile is under MinTileSamples (3).
            await PlantSparseWalWindowAsync(connection, WalFallbackServerId, WalFallbackServerName, WindowStartT, WalShiftBytesPerMinute, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(WalFallbackServerId, WalFallbackServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalyWalVolume);

            Assert.False(fact.Metadata.ContainsKey("tile_local_hour"), "under MinTileSamples in every tile, the fallback fact must not carry tile keys");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWalRowsAsync(cleanup, WalFallbackServerId, cleanupCt));
        }
    }

    /* ───────────────────────── CPU burn: scenario 1 only ───────────────────────── */

    private const string CpuShiftServerName = "darling-pg-tile-cpu-burn-shift-4h";
    private static readonly int CpuShiftServerId = ServerIdHelper.GetDeterministicHashCode(CpuShiftServerName);

    private const double CpuBaseCores = 1.0;   // routine
    private const double CpuShiftCores = 5.0;  // shifted — clears PgCpuBurnCoresFloor (0.5)

    /// <summary>Scenario 1 for CPU burn: <c>pg_kernel_stats</c> user/system time per minute, rated to cores busy —
    /// same shift shape as the WAL family's. Copies <c>PgTargetKernelTests</c>' live seeding.</summary>
    [Fact]
    public async Task CpuBurn_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteCpuBurnRowsAsync(connection, CpuShiftServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, CpuShiftServerId, CpuShiftServerName, MonitoredEngineKind.Postgres, 18, ct);

            await PlantCpuBurnHistoryAsync(connection, CpuShiftServerId, CpuShiftServerName, WindowStartT, 4, CpuBaseCores, ct);
            await PlantCpuBurnWindowAsync(connection, CpuShiftServerId, CpuShiftServerName, WindowStartT, 4, shiftFromHour: 2, CpuBaseCores, CpuShiftCores, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var startBucket = await baselines.GetBaselineAsync(CpuShiftServerId, MetricNames.PgCpuBurnCores, WindowStartT, ct);
            Assert.True(startBucket.IsTrustworthy, "the start-hour CPU-burn bucket must be trustworthy");

            var wholeWindowMean = (CpuBaseCores + CpuShiftCores) / 2.0;
            var wholeWindowMeanZ = BaselineMath.ModifiedZScore(startBucket, wholeWindowMean);
            Assert.True(wholeWindowMeanZ < AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgCpuBurnCores),
                $"the whole-window mean z ({wholeWindowMeanZ}) unexpectedly cleared the cutoff on its own");

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(CpuShiftServerId, CpuShiftServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalyCpuBurn);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained CPU-burn shift should have scored through the tile path");
            Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
            Assert.Equal(4, fact.Metadata["tiles_scored"]);
            Assert.Equal(2, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgCpuBurnCores), fact.Metadata["fire_threshold"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteCpuBurnRowsAsync(cleanup, CpuShiftServerId, cleanupCt));
        }
    }

    /* ───────────────────────── sampled waits: scenario 1 only (robust arm) ───────────────────────── */

    private const string SampledWaitShiftServerName = "darling-pg-tile-sampled-wait-shift-4h";
    private static readonly int SampledWaitShiftServerId = ServerIdHelper.GetDeterministicHashCode(SampledWaitShiftServerName);

    private const int SampledCycleMinutes = 5, SampledMs = 30_000, SampledPeriodMs = 1_000;
    private const int SampledBaseRelation = 20;   // 20 samples * 1s period = 20 s / 30 s watched -> ~667 ms/s
    private const int SampledShiftRelation = 90;  // 90 s / 30 s watched -> 3,000 ms/s, clears the fallback (500 ms/s)

    /// <summary>
    /// Scenario 1 for sampled waits: only the ROBUST arm tiles (lesson 2), so the start bucket must be
    /// trustworthy with robust sigma &gt; 0 — copies <c>PgTargetSampledWaitTests</c>'/<c>PgTargetSampledWaitLiveTests</c>'
    /// live seeding (<c>pg_wait_sampling</c>, no <c>pg_wait_stats</c> rows, so the exact source never suppresses
    /// the sampled arm).
    /// </summary>
    [Fact]
    public async Task SampledWaits_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSampledWaitRowsAsync(connection, SampledWaitShiftServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, SampledWaitShiftServerId, SampledWaitShiftServerName, MonitoredEngineKind.Postgres, 18, ct);

            await PlantSampledWaitHistoryAsync(connection, SampledWaitShiftServerId, SampledWaitShiftServerName, WindowStartT, 4, SampledBaseRelation, ct);
            await PlantSampledWaitWindowAsync(connection, SampledWaitShiftServerId, SampledWaitShiftServerName, WindowStartT, 4, shiftFromHour: 2, SampledBaseRelation, SampledShiftRelation, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var startBucket = await baselines.GetBaselineAsync(SampledWaitShiftServerId, MetricNames.PgSampledWaitMsPerSec, WindowStartT, ct);
            Assert.True(startBucket.IsTrustworthy, "the start-hour sampled-wait bucket must be trustworthy");
            Assert.True(startBucket.EffectiveRobustSigma > 0, "the start-hour bucket must carry a robust sigma so the robust arm — the only one this lane tiles — runs");

            var baseRatePerSec = SampledBaseRelation * SampledPeriodMs / (SampledMs / 1000.0);
            var shiftRatePerSec = SampledShiftRelation * SampledPeriodMs / (SampledMs / 1000.0);
            var wholeWindowMean = (baseRatePerSec + shiftRatePerSec) / 2.0;
            var wholeWindowMeanZ = BaselineMath.ModifiedZScore(startBucket, wholeWindowMean);
            Assert.True(wholeWindowMeanZ < AnomalyThresholds.HeavyTailModifiedZThreshold,
                $"the whole-window mean z ({wholeWindowMeanZ}) unexpectedly cleared the cutoff on its own");

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(SampledWaitShiftServerId, SampledWaitShiftServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalySampledWaitProfile);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained sampled-wait shift should have scored through the robust tile path");
            Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
            Assert.Equal(4, fact.Metadata["tiles_scored"]);
            Assert.Equal(2, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.HeavyTailModifiedZThreshold, fact.Metadata["fire_threshold"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSampledWaitRowsAsync(cleanup, SampledWaitShiftServerId, cleanupCt));
        }
    }

    /* ───────────────────────── planting: WAL volume ───────────────────────── */

    /// <summary>A pg_database_stats canary row so the collector's own gates have something to read.</summary>
    private static async Task PlantCanaryDatabaseStatsRowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', 1000, 0, 100, 9000, 0, 0, 0, NULL)";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 20_000_000L);
        command.Parameters.AddWithValue(windowStart.AddDays(-1));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>21 days of one-minute cumulative <c>wal_bytes</c> history at <paramref name="basePerMinute"/> a
    /// minute (deterministic ±5% ripple), one day per history row group, covering <paramref name="hours"/> hours
    /// starting at <paramref name="windowStart"/>'s hour-of-day, so every history row lands on the SAME
    /// hour-of-week as its matching window hour. The counter resets to a fresh base each history day so no
    /// history day's rate is diluted by another day's cumulative total.</summary>
    private static async Task PlantWalHistoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, long basePerMinute, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT d, h, i,
           (h * 60 + i) AS n,
           $5::bigint + round($5::bigint * 0.05 * sin(h * 12 + i)) AS wal_inc
    FROM generate_series(1, 21) AS d
    CROSS JOIN generate_series(0, $6) AS h
    CROSS JOIN generate_series(0, 59) AS i
)
INSERT INTO pg_write_stats
    (collection_id, collection_time, server_id, server_name,
     num_timed, num_requested, num_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, checkpointer_stats_reset,
     buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
     wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset)
SELECT $1 + (d * 100000 + n), $2 - (d * interval '1 day') + (n * interval '1 minute'), $3, $4,
       0, 0, 0, 0, 0, 0, NULL,
       0, 0, 0, NULL, NULL, NULL,
       (SUM(wal_inc) OVER (PARTITION BY d ORDER BY n)) / 8192,
       0,
       1000000000000 + SUM(wal_inc) OVER (PARTITION BY d ORDER BY n),
       0, NULL, NULL, NULL, NULL, NULL
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 21_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(basePerMinute);
        command.Parameters.AddWithValue(hours - 1);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Window: minutes below the shift hour boundary increment at <paramref name="basePerMinute"/> (±5%
    /// ripple); minutes at/above it increment at <paramref name="shiftPerMinute"/> for the WHOLE hour. A
    /// <paramref name="shiftFromHour"/> past the last hour plants an all-routine window.</summary>
    private static async Task PlantWalWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, int shiftFromHour, long basePerMinute, long shiftPerMinute, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT n,
           CASE WHEN (n / 60) >= $5 THEN $4::bigint ELSE $3::bigint + round($3::bigint * 0.05 * sin(n)) END AS wal_inc
    FROM generate_series(0, $6) AS n
)
INSERT INTO pg_write_stats
    (collection_id, collection_time, server_id, server_name,
     num_timed, num_requested, num_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, checkpointer_stats_reset,
     buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
     wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $7, $8,
       0, 0, 0, 0, 0, 0, NULL,
       0, 0, 0, NULL, NULL, NULL,
       (SUM(wal_inc) OVER (ORDER BY n)) / 8192,
       0,
       1000000000000 + SUM(wal_inc) OVER (ORDER BY n),
       0, NULL, NULL, NULL, NULL, NULL
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 22_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(basePerMinute);
        command.Parameters.AddWithValue(shiftPerMinute);
        command.Parameters.AddWithValue(shiftFromHour);
        command.Parameters.AddWithValue(hours * 60 - 1);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The whole window at the routine rate, plus ONE minute's increment at the shifted rate, at
    /// <paramref name="spikeAtMinute"/>.</summary>
    private static async Task PlantWalWindowWithLoneSpikeAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, long basePerMinute, long spikePerMinute, int spikeAtMinute, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT n,
           CASE WHEN n = $5 THEN $4::bigint ELSE $3::bigint + round($3::bigint * 0.05 * sin(n)) END AS wal_inc
    FROM generate_series(0, $6) AS n
)
INSERT INTO pg_write_stats
    (collection_id, collection_time, server_id, server_name,
     num_timed, num_requested, num_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, checkpointer_stats_reset,
     buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
     wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $7, $8,
       0, 0, 0, 0, 0, 0, NULL,
       0, 0, 0, NULL, NULL, NULL,
       (SUM(wal_inc) OVER (ORDER BY n)) / 8192,
       0,
       1000000000000 + SUM(wal_inc) OVER (ORDER BY n),
       0, NULL, NULL, NULL, NULL, NULL
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 23_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(basePerMinute);
        command.Parameters.AddWithValue(spikePerMinute);
        command.Parameters.AddWithValue(spikeAtMinute);
        command.Parameters.AddWithValue(4 * 60 - 1);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Only 2 rated collections per hour (20-minute spacing, both ends of a 40-minute span, so both
    /// LAG deltas land inside the same hour), all at <paramref name="shiftPerMinute"/>: every tile is under
    /// <c>MinTileSamples</c> (3).</summary>
    private static async Task PlantSparseWalWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, long shiftPerMinute, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT h, i
    FROM generate_series(0, 3) AS h
    CROSS JOIN generate_series(0, 1) AS i
)
INSERT INTO pg_write_stats
    (collection_id, collection_time, server_id, server_name,
     num_timed, num_requested, num_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, checkpointer_stats_reset,
     buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
     wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset)
SELECT $1 + (h * 10 + i), $2 + (h * interval '1 hour') + (i * interval '20 minutes'), $4, $5,
       0, 0, 0, 0, 0, 0, NULL,
       0, 0, 0, NULL, NULL, NULL,
       (SUM($3::bigint * i) OVER (PARTITION BY h ORDER BY i)) / 8192,
       0,
       1000000000000 + $3::bigint * (h * 100 + i) * 20,
       0, NULL, NULL, NULL, NULL, NULL
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 24_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(shiftPerMinute);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteWalRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_write_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────── planting: CPU burn ───────────────────────── */

    /// <summary>21 days of one-minute <c>pg_kernel_stats</c> history for query 7001 at <paramref name="baseCores"/>
    /// cores busy (deterministic ±5% ripple: user_ms per minute = cores × 60,000), covering <paramref name="hours"/>
    /// hours starting at <paramref name="windowStart"/>'s hour-of-day, one day per history row group so every
    /// history row lands on the SAME hour-of-week as its matching window hour.</summary>
    private static async Task PlantCpuBurnHistoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, double baseCores, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT d, h, i,
           (h * 60 + i) AS n,
           $5::double precision * 60000 * (1.0 + 0.05 * sin(h * 12 + i)) AS user_inc
    FROM generate_series(1, 21) AS d
    CROSS JOIN generate_series(0, $6) AS h
    CROSS JOIN generate_series(0, 59) AS i
)
INSERT INTO pg_kernel_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id,
     exec_user_time_ms, exec_system_time_ms, plan_cpu_time_ms, exec_read_bytes, exec_write_bytes, minor_faults, major_faults, stats_since)
SELECT $1 + (d * 100000 + n), $2 - (d * interval '1 day') + (n * interval '1 minute'), $3, $4, 'app', 7001,
       SUM(user_inc) OVER (PARTITION BY d ORDER BY n), 0, 0, 0, 0, 0, 0, $2 - (d * interval '1 day') - interval '1 day'
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 25_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(baseCores);
        command.Parameters.AddWithValue(hours - 1);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Window: minutes below the shift hour boundary run at <paramref name="baseCores"/> (±5% ripple);
    /// minutes at/above it run at <paramref name="shiftCores"/> for the WHOLE hour.</summary>
    private static async Task PlantCpuBurnWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, int shiftFromHour, double baseCores, double shiftCores, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT n,
           (CASE WHEN (n / 60) >= $5 THEN $4 ELSE $3 * (1.0 + 0.05 * sin(n)) END) * 60000 AS user_inc
    FROM generate_series(0, $6) AS n
)
INSERT INTO pg_kernel_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id,
     exec_user_time_ms, exec_system_time_ms, plan_cpu_time_ms, exec_read_bytes, exec_write_bytes, minor_faults, major_faults, stats_since)
SELECT $1 + n, $2 + (n * interval '1 minute'), $7, $8, 'app', 7001,
       SUM(user_inc) OVER (ORDER BY n), 0, 0, 0, 0, 0, 0, $2 - interval '1 day'
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 26_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(baseCores);
        command.Parameters.AddWithValue(shiftCores);
        command.Parameters.AddWithValue(shiftFromHour);
        command.Parameters.AddWithValue(hours * 60 - 1);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteCpuBurnRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_kernel_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────── planting: sampled waits ───────────────────────── */

    /// <summary>21 days of five-minute <c>pg_wait_sampling</c> cycles for one server, Lock:relation sample counts
    /// incrementing by <paramref name="baseRelation"/> a cycle (deterministic ±1-2 ripple via a mod-3 offset),
    /// covering <paramref name="hours"/> hours starting at <paramref name="windowStart"/>'s hour-of-day, one day
    /// per history row group so every history row lands on the SAME hour-of-week as its matching window hour.
    /// No <c>pg_wait_stats</c> rows anywhere in this test, so the exact source never suppresses the sampled arm.</summary>
    private static async Task PlantSampledWaitHistoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, int baseRelation, CancellationToken ct)
    {
        var sql = @"
WITH cycles AS (
    SELECT d, n,
           $5 + ((n / " + SampledCycleMinutes + @") % 3) AS relation_inc
    FROM generate_series(1, 21) AS d
    CROSS JOIN generate_series(0, $6, " + SampledCycleMinutes + @") AS n
),
running AS (
    SELECT d, n, SUM(relation_inc) OVER (PARTITION BY d ORDER BY n) AS relation_total
    FROM cycles
)
INSERT INTO pg_wait_sampling
    (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms)
SELECT $1 + (d * 100000 + n), $2 - (d * interval '1 day') + (n * interval '1 minute'), $3, $4, 'Lock', 'relation', 1001,
       100000 + relation_total, " + SampledPeriodMs + @", 3, " + SampledMs + @"
FROM running";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 27_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(baseRelation);
        command.Parameters.AddWithValue(hours * 60 - 1);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Window: cycles below the shift hour boundary increment by <paramref name="baseRelation"/> (with
    /// the same mod-3 ripple); cycles at/above it increment by <paramref name="shiftRelation"/> for the WHOLE
    /// hour.</summary>
    private static async Task PlantSampledWaitWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, int shiftFromHour, int baseRelation, int shiftRelation, CancellationToken ct)
    {
        var sql = @"
WITH cycles AS (
    SELECT n,
           CASE WHEN (n / 60) >= $5 THEN $4 ELSE $3 + ((n / " + SampledCycleMinutes + @") % 3) END AS relation_inc
    FROM generate_series(0, $6, " + SampledCycleMinutes + @") AS n
),
running AS (
    SELECT n, SUM(relation_inc) OVER (ORDER BY n) AS relation_total
    FROM cycles
)
INSERT INTO pg_wait_sampling
    (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms)
SELECT $1 + n, $2 + (n * interval '1 minute'), $7, $8, 'Lock', 'relation', 1001,
       100000 + relation_total, " + SampledPeriodMs + @", 3, " + SampledMs + @"
FROM running";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 28_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(baseRelation);
        command.Parameters.AddWithValue(shiftRelation);
        command.Parameters.AddWithValue(shiftFromHour);
        command.Parameters.AddWithValue(hours * 60 - 1);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSampledWaitRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_wait_sampling WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_wait_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A8 option B (PR #4171): behaviour tests for the PostgreSQL target's tiled anomaly gate on the four
/// families this PR carries — TPS, sessions, CPU and the Aurora wait profile. Each detector now scores every
/// target-local hour of the analysis window against ITS OWN hour-of-week baseline
/// (<see cref="AnomalyGate.EvaluateTiles"/>) instead of collapsing the whole window to one peak/mean pair, so a
/// sustained shift confined to part of the window fires where the old whole-window gate stayed quiet, and a lone
/// spike inside an otherwise-quiet window still does not fire. Every scenario runs through the REAL
/// <see cref="PgTargetAnomalyDetector.DetectAnomaliesAsync"/>, never a hand-copied tile call, and reads the fired
/// fact's own metadata (<c>tile_local_hour</c>, <c>tiles_scored</c>, <c>tiles_fired</c>, <c>fire_threshold</c>).
///
/// <para><b>First family: sessions.</b> All four scenarios. This surface is also the regression test for
/// <c>BindTileClock</c> (commit f68a59f43): before it, the tiled reads' <c>$4..$6</c> window-clock parameters were
/// never bound, so every tile read silently ran against the wrong clock window and the tiled path never fired —
/// scenario 1 below would have failed the "fires" assertion before that fix.</para>
///
/// <para><b>TPS, CPU and the Aurora wait profile:</b> scenario 1 only, per the coordinator's brief for a
/// multi-family PR.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetTileBehaviourTests
{
    // Wednesday 2026-01-07, 10:00 UTC. No server_properties row is seeded for any server here, so keying is UTC
    // and LocalHour.Hour/DayOfWeek read directly off these UTC instants.
    private static readonly DateTime WindowStartT = new(2026, 1, 7, 10, 0, 0, DateTimeKind.Unspecified);

    private const double SessionMu = 50.0;
    private const double SessionSigma = 4.0;
    private const double SessionShift = SessionMu + 6 * SessionSigma; // 74, clears PgSessionCountFloor (20)

    private static AnalysisContext FourHourContext(int serverId, string serverName) => new()
    {
        ServerId = serverId, ServerName = serverName, TimeRangeStart = WindowStartT, TimeRangeEnd = WindowStartT.AddHours(4), ServerUtcOffset = TimeSpan.Zero,
        Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 48 },
    };

    /* ───────────────────────── sessions: all four scenarios ───────────────────────── */

    private const string SessionsShiftServerName = "darling-pg-tile-sessions-shift-4h";
    private static readonly int SessionsShiftServerId = ServerIdHelper.GetDeterministicHashCode(SessionsShiftServerName);

    /// <summary>
    /// Scenario 1 (design's "the shift, 4 h"): a 4 h window [T, T+4h) whose FIRST two hours sit at the baseline
    /// and whose LAST two hours run the whole hour at μ + 6σ. The per-hour tile gate fires (hours 3/4 clear the
    /// cutoff against their own bucket); the SAME numbers, judged as one whole-window peak/mean pair against the
    /// START hour's bucket alone, do not clear the cutoff — the shift is diluted by the two quiet hours, exactly
    /// the case this lane exists to prove. This is also the <c>BindTileClock</c> regression test (f68a59f43):
    /// before that fix the tiled reads never bound their window-clock parameters, so <c>tv</c> would have come
    /// back <c>null</c> and this fact would carry no tile keys at all.
    /// </summary>
    [Fact]
    public async Task Sessions_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSessionRowsAsync(connection, SessionsShiftServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, SessionsShiftServerId, SessionsShiftServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);
            await PlantCanaryDatabaseStatsRowAsync(connection, SessionsShiftServerId, SessionsShiftServerName, WindowStartT, ct);

            // 21 days of history for hours 10-13 of this weekday, ~12 samples/hour, deterministic pseudo-noise.
            await PlantSessionHistoryAsync(connection, SessionsShiftServerId, SessionsShiftServerName, WindowStartT, 4, SessionMu, SessionSigma, ct);
            // The window: hours 0-1 at baseline shape; hours 2-3 (local hours 12-13) the WHOLE hour at the shift.
            await PlantSessionWindowAsync(connection, SessionsShiftServerId, SessionsShiftServerName, WindowStartT, 4, shiftFromHour: 2, SessionMu, SessionSigma, SessionShift, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var context = FourHourContext(SessionsShiftServerId, SessionsShiftServerName);

            // What dev did: the whole window's own MEAN (two quiet hours + two shifted hours), judged against
            // the START hour's bucket alone — below the cutoff, because the quiet half of the window drags the
            // window average toward baseline. This is the dilution the tile path exists to undo.
            var startBucket = await baselines.GetBaselineAsync(SessionsShiftServerId, MetricNames.PgSessionCount, WindowStartT, ct);
            Assert.True(startBucket.IsTrustworthy, "the start-hour session bucket must be trustworthy");
            var wholeWindowMean = (SessionMu + SessionShift) / 2.0; // two hours at baseline, two hours at the shift
            var wholeWindowMeanZ = BaselineMath.ModifiedZScore(startBucket, wholeWindowMean);
            Assert.True(wholeWindowMeanZ < AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgSessionCount),
                $"the whole-window mean z ({wholeWindowMeanZ}) unexpectedly cleared the cutoff on its own — the shift was not actually diluted by construction (median={startBucket.Median}, sigma={startBucket.EffectiveRobustSigma})");

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalySessionSpike);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained two-hour shift should have scored through the tile path");
            Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
            Assert.Equal(4, fact.Metadata["tiles_scored"]);
            Assert.Equal(2, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgSessionCount), fact.Metadata["fire_threshold"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSessionRowsAsync(cleanup, SessionsShiftServerId, cleanupCt));
        }
    }

    private const string Sessions24hServerName = "darling-pg-tile-sessions-shift-24h";
    private static readonly int Sessions24hServerId = ServerIdHelper.GetDeterministicHashCode(Sessions24hServerName);

    /// <summary>
    /// Scenario 2 (design's "the shift, 24 h, the <c>as_of</c> pass"): the same 2 h sustained shift, but at the
    /// LAST two hours of a 24 h window [T−22h, T+2h) — the shape a long <c>as_of</c> lookback takes. Fires, and
    /// <c>fire_threshold</c> is the Šidák-raised cutoff for a 24 h window, not the bare 3.5.
    /// </summary>
    [Fact]
    public async Task Sessions_TwoHourShiftInTwentyFourHourWindow_Fires_WithTheRaisedCutoff()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSessionRowsAsync(connection, Sessions24hServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, Sessions24hServerId, Sessions24hServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);
            await PlantCanaryDatabaseStatsRowAsync(connection, Sessions24hServerId, Sessions24hServerName, WindowStartT, ct);

            var windowStart = WindowStartT.AddHours(-22);
            var windowEnd = WindowStartT.AddHours(2);

            // 21 days of history covering all 24 hours of week the window spans.
            await PlantSessionHistoryFromAsync(connection, Sessions24hServerId, Sessions24hServerName, windowStart, 24, SessionMu, SessionSigma, ct);
            // The window: baseline everywhere except the last two hours (local hours 22-23 relative to the window
            // start correspond to T-2h..T+2h) which run the whole hour at the shift.
            await PlantSessionWindowAsync(connection, Sessions24hServerId, Sessions24hServerName, windowStart, 24, shiftFromHour: 22, SessionMu, SessionSigma, SessionShift, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var window = windowEnd - windowStart;
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var context = new AnalysisContext
            {
                ServerId = Sessions24hServerId, ServerName = Sessions24hServerName, TimeRangeStart = windowStart, TimeRangeEnd = windowEnd, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = (long)window.TotalMilliseconds, ObservedMs = (long)window.TotalMilliseconds, SampleCount = 288 },
            };
            var facts = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalySessionSpike);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained shift at the tail of a 24h window should have scored through the tile path");
            var referenceCutoff = AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgSessionCount);
            var expectedRaised = AnomalyThresholds.NAwarePeakCutoff(referenceCutoff, window);
            Assert.InRange(fact.Metadata["fire_threshold"], expectedRaised - 0.01, expectedRaised + 0.01);
            Assert.True(expectedRaised > referenceCutoff, "a 24h window must raise the cutoff above the 4h reference value");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSessionRowsAsync(cleanup, Sessions24hServerId, cleanupCt));
        }
    }

    private const string SessionsSpikeServerName = "darling-pg-tile-sessions-lone-spike";
    private static readonly int SessionsSpikeServerId = ServerIdHelper.GetDeterministicHashCode(SessionsSpikeServerName);

    /// <summary>
    /// Scenario 3 (design's "the lone spike"): a 4 h window at baseline throughout, plus ONE sample at μ + 10σ in
    /// hour 2. A single sample cannot drag its hour's tile mean past the (peak AND mean) pair gate, so it does not
    /// fire — the tile gate's own noise floor, distinct from the whole-window peak-only gate this PR replaces.
    /// </summary>
    [Fact]
    public async Task Sessions_LoneSpikeInsideAnHour_DoesNotFire()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSessionRowsAsync(connection, SessionsSpikeServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, SessionsSpikeServerId, SessionsSpikeServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);
            await PlantCanaryDatabaseStatsRowAsync(connection, SessionsSpikeServerId, SessionsSpikeServerName, WindowStartT, ct);
            const double spike = SessionMu + 10 * SessionSigma;

            await PlantSessionHistoryAsync(connection, SessionsSpikeServerId, SessionsSpikeServerName, WindowStartT, 4, SessionMu, SessionSigma, ct);
            // The whole window at baseline, plus one lone spike sample at hour 2 (local hour 12).
            await PlantSessionWindowAsync(connection, SessionsSpikeServerId, SessionsSpikeServerName, WindowStartT, 4, shiftFromHour: 99, SessionMu, SessionSigma, SessionShift, ct);
            await PlantOneSessionSampleAsync(connection, SessionsSpikeServerId, SessionsSpikeServerName, WindowStartT.AddHours(2).AddMinutes(30), spike, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(SessionsSpikeServerId, SessionsSpikeServerName));

            Assert.DoesNotContain(facts, f => f.Key == PgTargetFactKeys.AnomalySessionSpike);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSessionRowsAsync(cleanup, SessionsSpikeServerId, cleanupCt));
        }
    }

    private const string SessionsFallbackServerName = "darling-pg-tile-sessions-fallback";
    private static readonly int SessionsFallbackServerId = ServerIdHelper.GetDeterministicHashCode(SessionsFallbackServerName);

    /// <summary>
    /// Scenario 4 (design's "the fallback"): a 4 h window with only 2 samples per hour — under
    /// <see cref="AnomalyThresholds.MinTileSamples"/> (3) in every tile — all at μ + 6σ. No tile can be scored, so
    /// the detector falls through to today's whole-window path; the resulting fact still fires, but carries no
    /// <c>tile_local_hour</c> key.
    /// </summary>
    [Fact]
    public async Task Sessions_UnderMinTileSamplesEveryTile_FallsBackToTheWholeWindowPath_WithNoTileKeys()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSessionRowsAsync(connection, SessionsFallbackServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, SessionsFallbackServerId, SessionsFallbackServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);
            await PlantCanaryDatabaseStatsRowAsync(connection, SessionsFallbackServerId, SessionsFallbackServerName, WindowStartT, ct);

            await PlantSessionHistoryAsync(connection, SessionsFallbackServerId, SessionsFallbackServerName, WindowStartT, 4, SessionMu, SessionSigma, ct);
            // Only 2 samples per hour, all at the shift: every tile is under MinTileSamples (3).
            await PlantSparseSessionWindowAsync(connection, SessionsFallbackServerId, SessionsFallbackServerName, WindowStartT, SessionShift, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(SessionsFallbackServerId, SessionsFallbackServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalySessionSpike);

            Assert.False(fact.Metadata.ContainsKey("tile_local_hour"), "under MinTileSamples in every tile, the fallback fact must not carry tile keys");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSessionRowsAsync(cleanup, SessionsFallbackServerId, cleanupCt));
        }
    }

    /* ───────────────────────── TPS: scenario 1 only ───────────────────────── */

    private const string TpsShiftServerName = "darling-pg-tile-tps-shift-4h";
    private static readonly int TpsShiftServerId = ServerIdHelper.GetDeterministicHashCode(TpsShiftServerName);

    /// <summary>
    /// Scenario 1 for TPS: a running xact-commit sum rated per collection by LAG, so a "shift" is a higher
    /// per-minute increment for the shifted hours. Values are scaled well above <c>PgTpsFloor</c> (50/sec) so the
    /// magnitude floor is comfortably clear at every hour.
    /// </summary>
    [Fact]
    public async Task Tps_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTpsRowsAsync(connection, TpsShiftServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, TpsShiftServerId, TpsShiftServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);

            const double baseMu = 60.0;
            const double baseSigma = 3.0;
            const double shiftedTps = baseMu + 16.0 / 3.0 * baseSigma; // 76 tps, clears PgTpsFloor (50), and the
            // whole-window mean of baseline+shift stays under the fire cutoff (measured against the live bucket).

            await PlantTpsHistoryAsync(connection, TpsShiftServerId, TpsShiftServerName, WindowStartT, baseMu, baseSigma, ct);
            await PlantTpsWindowAsync(connection, TpsShiftServerId, TpsShiftServerName, WindowStartT, baseMu, baseSigma, shiftedTps, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var startBucket = await baselines.GetBaselineAsync(TpsShiftServerId, MetricNames.PgTps, WindowStartT, ct);
            Assert.True(startBucket.IsTrustworthy, "the start-hour TPS bucket must be trustworthy");
            // The window's own per-collection mean (two quiet hours' seeded incr plus two shifted hours), NOT the
            // simple (mu+shift)/2 — the seeded increments are pseudo-noise, not flat, so the true mean must be
            // computed the same way the product's own read would collapse it.
            var wholeWindowMean = Enumerable.Range(0, 4).SelectMany(h => Enumerable.Range(0, 12)
                .Select(i => h >= 2 ? shiftedTps : baseMu + baseSigma * Math.Sin(h * 12 + i))).Average();
            var wholeWindowMeanZ = BaselineMath.ModifiedZScore(startBucket, wholeWindowMean);
            Assert.True(wholeWindowMeanZ < AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgTps),
                $"the whole-window TPS mean z ({wholeWindowMeanZ}) unexpectedly cleared the cutoff on its own");

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(TpsShiftServerId, TpsShiftServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalyTps);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained TPS shift should have scored through the tile path");
            Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
            Assert.Equal(4, fact.Metadata["tiles_scored"]);
            Assert.Equal(2, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgTps), fact.Metadata["fire_threshold"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTpsRowsAsync(cleanup, TpsShiftServerId, cleanupCt));
        }
    }

    /* ───────────────────────── CPU: scenario 1 only ───────────────────────── */

    private const string CpuShiftServerName = "darling-pg-tile-cpu-shift-4h";
    private static readonly int CpuShiftServerId = ServerIdHelper.GetDeterministicHashCode(CpuShiftServerName);

    /// <summary>Scenario 1 for CPU: <c>acu_utilization_percent</c> — the capacity-percent supply the CPU detector
    /// reads (<see cref="PgTargetAnomalyDetector.CpuTileWindowSql"/>) — same shift shape as sessions'.</summary>
    [Fact]
    public async Task Cpu_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteCpuRowsAsync(connection, CpuShiftServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, CpuShiftServerId, CpuShiftServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);
            await PlantCanaryDatabaseStatsRowAsync(connection, CpuShiftServerId, CpuShiftServerName, WindowStartT, ct);

            const double cpuMu = 30.0;
            const double cpuSigma = 2.0;
            const double cpuShift = 60.0; // clears PgCpuFloorPct (40); the whole-window mean of baseline+shift
            // stays under the fire cutoff (measured against the live bucket).

            await PlantCpuHistoryAsync(connection, CpuShiftServerId, CpuShiftServerName, WindowStartT, cpuMu, cpuSigma, ct);
            await PlantCpuWindowAsync(connection, CpuShiftServerId, CpuShiftServerName, WindowStartT, cpuMu, cpuSigma, cpuShift, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var startBucket = await baselines.GetBaselineAsync(CpuShiftServerId, MetricNames.PgCpu, WindowStartT, ct);
            Assert.True(startBucket.IsTrustworthy, "the start-hour CPU bucket must be trustworthy");
            // The window's own per-collection mean of the ROUNDED seeded values (the product rounds the CPU
            // reading on the way in), not the simple (mu+shift)/2.
            var wholeWindowMean = Enumerable.Range(0, 4).SelectMany(h => Enumerable.Range(0, 12)
                .Select(i => Math.Round(h >= 2 ? cpuShift : cpuMu + cpuSigma * Math.Sin(h * 12 + i)))).Average();
            var wholeWindowMeanZ = BaselineMath.ModifiedZScore(startBucket, wholeWindowMean);
            Assert.True(wholeWindowMeanZ < AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgCpu),
                $"the whole-window CPU mean z ({wholeWindowMeanZ}) unexpectedly cleared the cutoff on its own");

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(CpuShiftServerId, CpuShiftServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalyCpuSpike);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained CPU shift should have scored through the tile path");
            Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
            Assert.Equal(4, fact.Metadata["tiles_scored"]);
            Assert.Equal(2, fact.Metadata["tiles_fired"]);
            Assert.Equal(AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgCpu), fact.Metadata["fire_threshold"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteCpuRowsAsync(cleanup, CpuShiftServerId, cleanupCt));
        }
    }

    /* ───────────────────────── Aurora wait profile: scenario 1 only ───────────────────────── */

    private const string WaitShiftServerName = "darling-pg-tile-wait-shift-4h";
    private static readonly int WaitShiftServerId = ServerIdHelper.GetDeterministicHashCode(WaitShiftServerName);

    /// <summary>
    /// Scenario 1 for the Aurora wait profile: the ROBUST arm is the only one tiled (the PR's own ruling), so the
    /// baseline must resolve to a trustworthy bucket with robust sigma &gt; 0 — a Lock:relation series whose
    /// history varies (not a flat constant, which collapses MAD to 0), seeded the same way
    /// <c>PgTargetAnomalyTests.TheAuroraWaitProfile_…</c> seeds its wait-gate rows.
    /// </summary>
    [Fact]
    public async Task WaitProfile_TwoHourShiftInFourHourWindow_FiresPerTile_WhereTheWholeWindowGateWouldStayQuiet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the tile behaviour tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteWaitRowsAsync(connection, WaitShiftServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, WaitShiftServerId, WaitShiftServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);

            const long baseMsPerSec = 200L;    // matches PgTargetAnomalyTests' quiet-wait history shape
            const long shiftMsPerSec = 3_200L; // clears PgWaitProfileFallbackMsPerSec (500) and the heavy-tail z

            await PlantWaitHistoryAsync(connection, WaitShiftServerId, WaitShiftServerName, WindowStartT, ct);
            await PlantWaitWindowAsync(connection, WaitShiftServerId, WaitShiftServerName, WindowStartT, baseMsPerSec, shiftMsPerSec, ct);

            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(WaitShiftServerId, MetricNames.PgWaitMsPerSec, WindowStartT, ct);
            Assert.True(bucket.IsTrustworthy, "the 21-day wait bucket must be trustworthy");
            Assert.True(bucket.EffectiveRobustSigma > 0, "the wait bucket must carry robust sigma > 0 — the only arm this PR tiles");

            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var facts = await detector.DetectAnomaliesAsync(FourHourContext(WaitShiftServerId, WaitShiftServerName));
            var fact = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AnomalyWaitProfile);

            Assert.True(fact.Metadata.ContainsKey("tile_local_hour"), "the sustained shift should score through the tiled robust arm");
            Assert.InRange(fact.Metadata["tile_local_hour"], 12, 13);
            Assert.Equal(4, fact.Metadata["tiles_scored"]);
            Assert.Equal(2, fact.Metadata["tiles_fired"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWaitRowsAsync(cleanup, WaitShiftServerId, cleanupCt));
        }
    }

    /* ───────────────────────── planting: sessions ───────────────────────── */

    /// <summary><see cref="PgTargetAnomalyDetector.HasBaselineDataAsync"/>'s canary reads <c>pg_database_stats</c>
    /// alone; a scenario whose own family lives in a different table (sessions, CPU) must still plant one row
    /// there inside the 30-day lookback or <c>DetectAnomaliesAsync</c> returns empty before any detector runs.</summary>
    private static async Task PlantCanaryDatabaseStatsRowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', 1000, 0, 100, 9000, 0, 0, 0, NULL)";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 9_000_000L);
        command.Parameters.AddWithValue(windowStart.AddDays(-1));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>21 days of history at deterministic pseudo-noise, one row per (day, hour, i) covering
    /// <paramref name="hours"/> hours starting at <paramref name="windowStart"/>'s hour-of-day, going back one day
    /// at a time so every history row lands on the SAME hour-of-week as its matching window hour.</summary>
    private static async Task PlantSessionHistoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, double mu, double sigma, CancellationToken ct)
        => await PlantSessionHistoryFromAsync(connection, serverId, serverName, windowStart, hours, mu, sigma, ct);

    private static async Task PlantSessionHistoryFromAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, double mu, double sigma, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
SELECT $1 + (d * 10000 + h * 100 + i), $2 - (d * interval '1 day') + (h * interval '1 hour') + (i * interval '5 minutes'), $3, $4, FALSE,
       ROUND($5 + $6 * sin(d * 4 + h * 12 + i))::int, 4, 1, 1
FROM generate_series(1, 21) AS d
CROSS JOIN generate_series(0, $7) AS h
CROSS JOIN generate_series(0, 11) AS i";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 6_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(mu);
        command.Parameters.AddWithValue(sigma);
        command.Parameters.AddWithValue(hours - 1);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Window: hours below <paramref name="shiftFromHour"/> at baseline shape; hours at/above it run the
    /// WHOLE hour at <paramref name="shift"/>. A <paramref name="shiftFromHour"/> past the last hour (e.g. 99)
    /// plants an all-baseline window.</summary>
    private static async Task PlantSessionWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, int hours, int shiftFromHour, double mu, double sigma, double shift, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= $6 THEN $5 ELSE $3 + $4 * sin(h * 12 + i) END AS value
    FROM generate_series(0, $7) AS h
    CROSS JOIN generate_series(0, 11) AS i
)
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
SELECT $1 + (h * 100 + i), $2 + (h * interval '1 hour') + (i * interval '5 minutes'), $8, $9, FALSE,
       ROUND(value)::int, 4, 1, 1
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 7_000_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(mu);
        command.Parameters.AddWithValue(sigma);
        command.Parameters.AddWithValue(shift);
        command.Parameters.AddWithValue(shiftFromHour);
        command.Parameters.AddWithValue(hours - 1);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantOneSessionSampleAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, double value, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, FALSE, $5, 4, 1, 1)";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 8_000_000L);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue((int)Math.Round(value));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantSparseSessionWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, double shift, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
SELECT $1 + (h * 10 + i), $2 + (h * interval '1 hour') + (i * interval '20 minutes'), $3, $5, FALSE,
       ROUND($4)::int, 4, 1, 1
FROM generate_series(0, 3) AS h
CROSS JOIN generate_series(0, 1) AS i";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 8_500_000L);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(shift);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSessionRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_session_states WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────── planting: TPS ───────────────────────── */

    private static async Task PlantTpsHistoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, double mu, double sigma, CancellationToken ct)
    {
        // Cumulative xact_commit per minute-sample, incrementing by (mu + sigma*sin) each minute within each
        // (day, hour) block so LAG rates each collection to ~mu tps; 21 days back, hours 0-3.
        const string sql = @"
WITH s AS (
    SELECT d, h, i, ($3 + $4 * sin(d * 4 + h * 12 + i)) AS incr
    FROM generate_series(1, 21) AS d
    CROSS JOIN generate_series(0, 3) AS h
    CROSS JOIN generate_series(0, 11) AS i
),
running AS (
    SELECT d, h, i, SUM(incr) OVER (PARTITION BY d, h ORDER BY i) * 300 AS cum_xacts
    FROM s
)
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT 200000000 + (d * 10000 + h * 100 + i), $2 - (d * interval '1 day') + (h * interval '1 hour') + (i * interval '5 minutes'), $1, $5, 'appdb',
       ROUND(cum_xacts)::bigint, 0, 100, 9000, 0, 0, 0, NULL
FROM running";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(mu);
        command.Parameters.AddWithValue(sigma);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantTpsWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, double mu, double sigma, double shift, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= 2 THEN $5 ELSE $3 + $4 * sin(h * 12 + i) END AS incr
    FROM generate_series(0, 3) AS h
    CROSS JOIN generate_series(0, 11) AS i
),
running AS (
    SELECT h, i, SUM(incr) OVER (ORDER BY h, i) * 300 AS cum_xacts
    FROM s
)
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT 300000000 + (h * 100 + i), $2 + (h * interval '1 hour') + (i * interval '5 minutes'), $1, $6, 'appdb',
       ROUND(cum_xacts)::bigint, 0, 100, 9000, 0, 0, 0, NULL
FROM running";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(mu);
        command.Parameters.AddWithValue(sigma);
        command.Parameters.AddWithValue(shift);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTpsRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────── planting: CPU ───────────────────────── */

    private static async Task PlantCpuHistoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, double mu, double sigma, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO pg_cpu_utilization
    (collection_id, collection_time, server_id, server_name, sample_time,
     cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu)
SELECT 400000000 + (d * 10000 + h * 100 + i), $2 - (d * interval '1 day') + (h * interval '1 hour') + (i * interval '5 minutes'), $1, $5,
       $2 - (d * interval '1 day') + (h * interval '1 hour') + (i * interval '5 minutes'),
       ROUND($3 + $4 * sin(d * 4 + h * 12 + i)), ROUND($3 + $4 * sin(d * 4 + h * 12 + i)), 6.0, 12
FROM generate_series(1, 21) AS d
CROSS JOIN generate_series(0, 3) AS h
CROSS JOIN generate_series(0, 11) AS i";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 180 };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(mu);
        command.Parameters.AddWithValue(sigma);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantCpuWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, double mu, double sigma, double shift, CancellationToken ct)
    {
        const string sql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= 2 THEN $5 ELSE $3 + $4 * sin(h * 12 + i) END AS value
    FROM generate_series(0, 3) AS h
    CROSS JOIN generate_series(0, 11) AS i
)
INSERT INTO pg_cpu_utilization
    (collection_id, collection_time, server_id, server_name, sample_time,
     cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu)
SELECT 500000000 + (h * 100 + i), $2 + (h * interval '1 hour') + (i * interval '5 minutes'), $1, $6,
       $2 + (h * interval '1 hour') + (i * interval '5 minutes'),
       ROUND(value), ROUND(value), 10.8, 12
FROM s";
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(windowStart);
        command.Parameters.AddWithValue(mu);
        command.Parameters.AddWithValue(sigma);
        command.Parameters.AddWithValue(shift);
        command.Parameters.AddWithValue(serverName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteCpuRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_cpu_utilization WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────── planting: Aurora wait profile ───────────────────────── */

    private static async Task PlantWaitHistoryAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, CancellationToken ct)
    {
        // Baseline database-stats gate row per minute for 21 days over hours 0-3, plus a varying Lock:relation
        // series (100/200/300 ms/sec cycling by minute, so MAD > 0) with a CPU row beside it to prove exclusion.
        const string dbSql = @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT 600000000 + (d * 10000 + h * 100 + i), $2 - (d * interval '1 day') + (h * interval '1 hour') + (i * interval '1 minute'), $1, $3, 'appdb',
       1000, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(1, 21) AS d
CROSS JOIN generate_series(0, 3) AS h
CROSS JOIN generate_series(0, 59) AS i";
        using (var command = new NpgsqlCommand(dbSql, connection) { CommandTimeout = 240 })
        {
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(windowStart);
            command.Parameters.AddWithValue(serverName);
            await command.ExecuteNonQueryAsync(ct);
        }

        const string waitSql = @"
WITH s AS (
    SELECT d, h, i, (100 + 100 * (i % 3)) AS lock_ms
    FROM generate_series(1, 21) AS d
    CROSS JOIN generate_series(0, 3) AS h
    CROSS JOIN generate_series(0, 59) AS i
)
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event,
     waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
SELECT 700000000 + (d * 1000000 + h * 10000 + i * 10) + w.type_id, $2 - (d * interval '1 day') + (h * interval '1 hour') + (i * interval '1 minute'),
       $1, $3, w.type_id, w.event_id, w.wait_type, w.wait_event,
       1000000, 1000000000000, 10, CASE WHEN w.type_id = 0 THEN 40000000::bigint ELSE lock_ms * 60000::bigint END, 60
FROM s
CROSS JOIN (VALUES (3, 300001::bigint, 'Lock', 'relation'), (0, 1::bigint, 'CPU', 'CPU')) AS w(type_id, event_id, wait_type, wait_event)";
        using (var command = new NpgsqlCommand(waitSql, connection) { CommandTimeout = 240 })
        {
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(windowStart);
            command.Parameters.AddWithValue(serverName);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task PlantWaitWindowAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime windowStart, long baseMsPerSec, long shiftMsPerSec, CancellationToken ct)
    {
        const string dbSql = @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT 800000000 + (h * 1000 + i), $2 + (h * interval '1 hour') + (i * interval '1 minute'), $1, $3, 'appdb', 1000, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, 3) AS h
CROSS JOIN generate_series(0, 59) AS i";
        using (var command = new NpgsqlCommand(dbSql, connection) { CommandTimeout = 120 })
        {
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(windowStart);
            command.Parameters.AddWithValue(serverName);
            await command.ExecuteNonQueryAsync(ct);
        }

        const string waitSql = @"
WITH s AS (
    SELECT h, i, CASE WHEN h >= 2 THEN $4 ELSE $3 END AS lock_ms_per_sec
    FROM generate_series(0, 3) AS h
    CROSS JOIN generate_series(0, 59) AS i
)
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event,
     waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
SELECT 900000000 + (h * 100000 + i * 10) + w.type_id, $2 + (h * interval '1 hour') + (i * interval '1 minute'),
       $1, $5, w.type_id, w.event_id, w.wait_type, w.wait_event,
       1000000, 1000000000000, 10, CASE WHEN w.type_id = 0 THEN 40000000::bigint ELSE lock_ms_per_sec * 60000::bigint END, 60
FROM s
CROSS JOIN (VALUES (3, 300001::bigint, 'Lock', 'relation'), (0, 1::bigint, 'CPU', 'CPU')) AS w(type_id, event_id, wait_type, wait_event)";
        using (var command = new NpgsqlCommand(waitSql, connection) { CommandTimeout = 120 })
        {
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(windowStart);
            command.Parameters.AddWithValue(baseMsPerSec);
            command.Parameters.AddWithValue(shiftMsPerSec);
            command.Parameters.AddWithValue(serverName);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteWaitRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_wait_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A8 option B (lane L1a): <c>AnomalyGate.EvaluateTiles</c> and its math — design-3653-A8-B.md §1/§3.
/// A trustworthy classical bucket (mean 0, stddev 1) is used throughout unless a case is explicitly about
/// the untrustworthy or zero-history paths, so the peak/mean values below ARE sigmas.
/// </summary>
public class AnomalyGateTilesTests
{
    private const double ClassicalThreshold = AnomalyThresholds.DefaultDeviationThreshold; // 2.0
    private const double ModifiedZThreshold = AnomalyThresholds.ModifiedZThreshold;         // 3.5
    private const double MagnitudeFloor = 0.0;
    private const double AbsoluteFallbackBar = 1_000_000.0;
    private const double SigmaCap = AnomalyThresholds.SigmaDisplayCap;

    private static readonly DateTime WindowEnd = new(2026, 9, 24, 0, 0, 0, DateTimeKind.Unspecified);

    private static BaselineBucket TrustworthyBucket(int hour, int dow, double mean = 0.0, double stdDev = 1.0)
        => new()
        {
            HourOfDay = hour,
            DayOfWeek = dow,
            Tier = BaselineTier.Full,
            Mean = mean,
            StdDev = stdDev,
            SampleCount = 100,
            DistinctDays = 10,
            AbsStdDevFloor = 0,
        };

    private static BaselineBucketMap MapFrom(params BaselineBucket[] buckets)
    {
        var dict = new Dictionary<(int, int), BaselineBucket>();
        foreach (var b in buckets)
            dict[(b.HourOfDay, b.DayOfWeek)] = b;
        return new BaselineBucketMap(dict, LocalClockWindow.Utc(WindowEnd));
    }

    private static WindowTile Tile(int localHour, int dow, double peak, double mean, long samples = 100)
        => new(new DateTime(2026, 9, 24, localHour, 0, 0, DateTimeKind.Unspecified).AddDays(dow - (int)new DateTime(2026, 9, 24).DayOfWeek), peak, mean, samples);

    // ── the 4h == k identity, and the 24h ≈ 3.95 raise, on BOTH clauses ────────────────────────

    [Fact]
    public void AtFourHours_TileCutoff_EqualsK_Exactly_OnBothClauses()
    {
        var bucket = TrustworthyBucket(hour: 10, dow: 3);
        var map = MapFrom(bucket);

        // Peak exactly at k (2.0) fires; a shade under does not — the peak clause byte-identical to k.
        var tiles = new[] { Tile(10, 3, peak: 2.0, mean: 0.0) };
        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));
        Assert.NotNull(verdict);
        Assert.Equal(ClassicalThreshold, verdict!.Value.Decision.ThresholdUsed);

        // Mean also judged at k exactly: mean at k fires when peak also clears k (peak == mean here).
        var atK = new[] { Tile(10, 3, peak: 2.0, mean: 2.0) };
        var verdictAtK = AnomalyGate.EvaluateTiles(
            tiles: atK, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));
        Assert.True(verdictAtK!.Value.Decision.Fire);

        var justUnder = new[] { Tile(10, 3, peak: 2.0, mean: 1.999) };
        var verdictUnder = AnomalyGate.EvaluateTiles(
            justUnder, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));
        Assert.False(verdictUnder!.Value.Decision.Fire);
    }

    [Fact]
    public void AtTwentyFourHours_TileCutoff_IsApproximately3_95_OnBothClauses()
    {
        var bucket = TrustworthyBucket(hour: 10, dow: 3);
        var map = MapFrom(bucket);
        var window = TimeSpan.FromHours(24);
        var expected = AnomalyThresholds.NAwarePeakCutoff(ModifiedZThreshold, window);
        Assert.InRange(expected, 3.95 - 0.01, 3.95 + 0.01);

        // Peak clause: at the corrected cutoff (robust frame degrades to classical here since the bucket has
        // no robust stats — Mad == 0, Median == 0 — so both frames report EffectiveRobustSigma == 0 and Decide
        // falls back to the classical dispersion 1.0 with classicalDeviationThreshold's OWN NAwarePeakCutoff).
        var classicalExpected = AnomalyThresholds.NAwarePeakCutoff(ClassicalThreshold, window);
        var tiles = new[] { Tile(10, 3, peak: classicalExpected, mean: 0.0) };
        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap, window);
        Assert.NotNull(verdict);
        Assert.Equal(classicalExpected, verdict!.Value.Decision.ThresholdUsed, 6);

        // Mean clause raised the same way: mean just under the corrected cutoff, with peak clearing, does not fire.
        var meanJustUnder = new[] { Tile(10, 3, peak: classicalExpected + 5, mean: classicalExpected - 0.01) };
        var verdictMeanUnder = AnomalyGate.EvaluateTiles(
            meanJustUnder, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap, window);
        Assert.False(verdictMeanUnder!.Value.Decision.Fire);

        // Mean at (or just over) the corrected cutoff, with peak clearing, fires.
        var meanAt = new[] { Tile(10, 3, peak: classicalExpected + 5, mean: classicalExpected + 0.01) };
        var verdictMeanAt = AnomalyGate.EvaluateTiles(
            meanAt, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap, window);
        Assert.True(verdictMeanAt!.Value.Decision.Fire);
    }

    // ── a 2h +6sigma shift over two tiles fires at 4h and 24h, and the worst tile is right ────

    [Fact]
    public void TwoHourSixSigmaShift_FiresAtFourHours_AndPicksWorstTileByLaterHourTie()
    {
        var b9 = TrustworthyBucket(9, 3);
        var b10 = TrustworthyBucket(10, 3);
        var map = MapFrom(b9, b10);

        // Two tiles both at +6 sigma (tied Sigma) — the worst tile ties go to the later LocalHour (10).
        var tiles = new[]
        {
            Tile(9, 3, peak: 6.0, mean: 6.0),
            Tile(10, 3, peak: 6.0, mean: 6.0),
        };

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));

        Assert.NotNull(verdict);
        Assert.True(verdict!.Value.Decision.Fire);
        Assert.Equal(2, verdict.Value.TilesScored);
        Assert.Equal(2, verdict.Value.TilesFired);
        Assert.Equal(10, verdict.Value.Tile.LocalHour.Hour);
    }

    [Fact]
    public void TwoHourSixSigmaShift_FiresAtTwentyFourHours_DespiteRaisedCutoff()
    {
        var b9 = TrustworthyBucket(9, 3);
        var b10 = TrustworthyBucket(10, 3);
        var map = MapFrom(b9, b10);

        var tiles = new[]
        {
            Tile(9, 3, peak: 6.0, mean: 6.0),
            Tile(10, 3, peak: 6.0, mean: 6.0),
        };

        var window = TimeSpan.FromHours(24);
        var raised = AnomalyThresholds.NAwarePeakCutoff(ClassicalThreshold, window);
        Assert.True(raised < 6.0, $"the raised cutoff ({raised}) must still be under 6 sigma for this fixture to prove anything");

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap, window);

        Assert.NotNull(verdict);
        Assert.True(verdict!.Value.Decision.Fire);
        Assert.Equal(2, verdict.Value.TilesFired);
    }

    // ── one lone +8sigma sample in a tile with a flat mean → no fire (the mean clause guards it) ──

    [Fact]
    public void LoneEightSigmaSampleWithFlatMean_DoesNotFire()
    {
        var bucket = TrustworthyBucket(10, 3);
        var map = MapFrom(bucket);

        // The tile's peak is a lone spike (+8 sigma) but its MEAN stays at baseline (0.0) — the mean clause
        // guards a single hot sample exactly as the design intends ("one lone spike still never fires alone").
        var tiles = new[] { Tile(10, 3, peak: 8.0, mean: 0.0) };

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));

        Assert.NotNull(verdict);
        Assert.False(verdict!.Value.Decision.Fire);
        Assert.Equal(0, verdict.Value.TilesFired);
        Assert.Equal(1, verdict.Value.TilesScored);
    }

    // ── a tile under 3 samples is skipped; all skipped → null; a missing bucket is skipped ─────

    [Fact]
    public void TileUnderMinSamples_IsSkipped_ButOtherTilesStillScore()
    {
        var b9 = TrustworthyBucket(9, 3);
        var b10 = TrustworthyBucket(10, 3);
        var map = MapFrom(b9, b10);

        var tiles = new[]
        {
            Tile(9, 3, peak: 10.0, mean: 10.0, samples: 2), // under MinTileSamples (3) — skipped
            Tile(10, 3, peak: 1.0, mean: 1.0, samples: 100),
        };

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));

        Assert.NotNull(verdict);
        Assert.Equal(1, verdict!.Value.TilesScored);
        Assert.Equal(10, verdict.Value.Tile.LocalHour.Hour); // the surviving tile, not the skipped one
    }

    [Fact]
    public void EveryTileUnderMinSamples_ReturnsNull()
    {
        var bucket = TrustworthyBucket(10, 3);
        var map = MapFrom(bucket);

        var tiles = new[]
        {
            Tile(10, 3, peak: 10.0, mean: 10.0, samples: 1),
            Tile(11, 3, peak: 10.0, mean: 10.0, samples: 2),
        };

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));

        Assert.Null(verdict);
    }

    [Fact]
    public void MissingBucket_IsSkipped_AndAllMissing_ReturnsNull()
    {
        var map = MapFrom(); // no buckets at all — SelectBucket returns BaselineBucket.Empty (SampleCount 0)

        var tiles = new[] { Tile(10, 3, peak: 10.0, mean: 10.0) };

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));

        Assert.Null(verdict);
    }

    // ── the untrustworthy path per tile ──────────────────────────────────────────────────────

    [Fact]
    public void UntrustworthyTile_FiresOnAbsoluteBar_AndMagnitudeFloorOnMean()
    {
        // Untrustworthy: too few samples/days to clear the tier floors.
        var untrustworthy = new BaselineBucket
        {
            HourOfDay = 10, DayOfWeek = 3, Tier = BaselineTier.Flat,
            Mean = 0, StdDev = 0, SampleCount = 5, DistinctDays = 1, AbsStdDevFloor = 0,
        };
        Assert.False(untrustworthy.IsTrustworthy);
        var map = MapFrom(untrustworthy);

        const double floor = 50.0;
        const double fallbackBar = 500.0;

        // Peak clears the absolute bar, mean clears the magnitude floor → fires.
        var firing = new[] { Tile(10, 3, peak: 600.0, mean: 60.0) };
        var verdictFiring = AnomalyGate.EvaluateTiles(
            firing, map, ClassicalThreshold, ModifiedZThreshold, floor, fallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));
        Assert.NotNull(verdictFiring);
        Assert.True(verdictFiring!.Value.Decision.Fire);
        Assert.True(verdictFiring.Value.Decision.LowQualityBaseline);

        // Peak clears the bar but the mean sits under the magnitude floor → does not fire.
        var meanUnderFloor = new[] { Tile(10, 3, peak: 600.0, mean: 10.0) };
        var verdictMeanUnder = AnomalyGate.EvaluateTiles(
            meanUnderFloor, map, ClassicalThreshold, ModifiedZThreshold, floor, fallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));
        Assert.False(verdictMeanUnder!.Value.Decision.Fire);
    }

    // ── the zero-history path per tile ───────────────────────────────────────────────────────

    [Fact]
    public void ZeroHistoryTile_FiresOnMagnitudeFloor_AgainstThePeakAsAnExtremity()
    {
        var zeroHistory = new BaselineBucket
        {
            HourOfDay = 10, DayOfWeek = 3, Tier = BaselineTier.Full,
            Mean = 0, StdDev = 0, Median = 0, Mad = 0,
            SampleCount = 100, DistinctDays = 10, AbsStdDevFloor = 0,
        };
        Assert.True(zeroHistory.IsZeroHistory);
        var map = MapFrom(zeroHistory);

        const double floor = 3.0;

        var firing = new[] { Tile(10, 3, peak: 5.0, mean: 5.0) };
        var verdictFiring = AnomalyGate.EvaluateTiles(
            firing, map, ClassicalThreshold, ModifiedZThreshold, floor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));
        Assert.NotNull(verdictFiring);
        Assert.True(verdictFiring!.Value.Decision.Fire);
        Assert.True(verdictFiring.Value.Decision.ZeroHistory);
        Assert.False(verdictFiring.Value.Decision.LowQualityBaseline);

        var notFiring = new[] { Tile(10, 3, peak: 1.0, mean: 1.0) };
        var verdictNotFiring = AnomalyGate.EvaluateTiles(
            notFiring, map, ClassicalThreshold, ModifiedZThreshold, floor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));
        Assert.False(verdictNotFiring!.Value.Decision.Fire);
    }

    // ── nothing fires → a verdict with Fire = false, the highest-Sigma scored tile, TilesFired = 0 ──

    [Fact]
    public void NothingFires_ReturnsVerdictWithHighestSigmaTile_AndTilesFiredZero()
    {
        var b9 = TrustworthyBucket(9, 3);
        var b10 = TrustworthyBucket(10, 3);
        var map = MapFrom(b9, b10);

        // Neither tile fires (both under the 2.0 cutoff), but tile 10 has the higher (still sub-threshold) sigma.
        var tiles = new[]
        {
            Tile(9, 3, peak: 0.5, mean: 0.5),
            Tile(10, 3, peak: 1.5, mean: 1.5),
        };

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));

        Assert.NotNull(verdict);
        Assert.False(verdict!.Value.Decision.Fire);
        Assert.Equal(0, verdict.Value.TilesFired);
        Assert.Equal(2, verdict.Value.TilesScored);
        Assert.Equal(10, verdict.Value.Tile.LocalHour.Hour);
    }

    // ── a tie goes to the later hour (both firing, exactly equal Sigma) ─────────────────────────

    [Fact]
    public void TiedSigma_WorstTile_GoesToTheLaterHour()
    {
        var b9 = TrustworthyBucket(9, 3);
        var b23 = TrustworthyBucket(23, 3);
        var map = MapFrom(b9, b23);

        var tiles = new[]
        {
            Tile(23, 3, peak: 6.0, mean: 6.0),
            Tile(9, 3, peak: 6.0, mean: 6.0),
        };

        var verdict = AnomalyGate.EvaluateTiles(
            tiles, map, ClassicalThreshold, ModifiedZThreshold, MagnitudeFloor, AbsoluteFallbackBar, SigmaCap,
            window: TimeSpan.FromHours(4));

        Assert.NotNull(verdict);
        Assert.Equal(23, verdict!.Value.Tile.LocalHour.Hour);
    }
}

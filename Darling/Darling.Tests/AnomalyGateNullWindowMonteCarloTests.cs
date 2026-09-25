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
/// #3653 A8 validation, offline half (design-3653-A8.md §4): does the N-aware (Šidák) peak cutoff
/// #4152 added actually equalize the null-window peak-clause fire rate between the 4-hour scheduled
/// pass and a 24-hour <c>as_of</c> pass, for the two window-length families the fleet runs (5-minute:
/// N=48 vs N=288; 1-minute: N=240 vs N=1440)? This is the offline half of the validation plan; the live
/// half (<c>DarlingAnomalyBaselineTests</c>-style, gated <c>DARLING_TEST_PG</c>) runs the real detector
/// SQL over a planted 8-server, 14-day store and is the sibling class carrying the same #3653 A8
/// validation banner.
///
/// <para><b>Method.</b> Draw N iid standard-normal "peaks" per trial (a per-sample z already in the
/// baseline's own frame — the gate's peak clause is exactly this comparison), record whether
/// <c>AnomalyGate.EvaluateZScore</c>'s pair overload fires with <c>window: null</c> (today's verdict,
/// used here as the pre-A8, non-N-aware baseline for comparison) and with the family's actual
/// <c>window: 4h</c> / <c>window: 24h</c>, and count the fraction of trials that fire. Held fixed across
/// both windows for a family: the baseline (mean 0, stddev 1 — a trustworthy classical bucket), the
/// cutoff (<c>AnomalyThresholds.DefaultDeviationThreshold</c> = 2.0, the classical family's), the
/// magnitude floor (0, so it never gates) and the window MEAN clause (fixed comfortably under threshold,
/// so only the peak clause is under test — the design's own scope: "Applies to the PEAK clause only").
/// The window peak fed to the gate is the MAX of the N per-sample draws, mirroring what every detector
/// actually computes and feeds as <c>peak</c>.
///
/// <para><b>Byte-identical claim.</b> <c>window: null</c> and <c>window: 4h-or-shorter</c> must agree on
/// EVERY trial, not just in aggregate rate — <c>AnomalyThresholds.NAwarePeakCutoff</c> returns
/// <paramref name="k"/> itself (no Φ⁻¹(Φ(k)) round trip) at or under the 4-hour reference window, so the
/// two calls compute the identical <c>peakThreshold</c> and must produce the identical <c>Fire</c> for
/// the identical draw.</para>
/// </summary>
public class AnomalyGateNullWindowMonteCarloTests(ITestOutputHelper output)
{
    private const double Threshold = AnomalyThresholds.DefaultDeviationThreshold; // 2.0 — the classical family's cutoff at the 4h reference
    private const double MagnitudeFloor = 0.0;      // never gates in this test — peak clause only, per the design's scope
    private const double AbsoluteFallbackBar = 1_000_000.0; // never reached — baseline below is always trustworthy
    private const double SigmaCap = 25.0;
    // #3653 A8 slice 1's own remarks: the N-aware correction "[a]pplies to the PEAK clause only" — the mean
    // clause and the magnitude floor are unchanged. To isolate exactly the peak clause's null-window fire rate
    // (the one thing #4152 changed), the window mean is fixed comfortably ABOVE Threshold so the mean clause
    // always clears and `Fire` reduces to the peak clause alone. A natural per-window sample mean would instead
    // shrink toward 0 as N grows (CLT: variance sigma^2/N) and increasingly gate the AND on the MEAN side —
    // confounding the peak-only question this test asks with an unrelated, non-N-biased effect the design
    // explicitly says needs no correction.
    private const double SafeWindowMean = 1_000.0;

    [Theory]
    [InlineData(48, 4)]     // 5-minute family: 4h scheduled pass, N = 4h / 5min
    [InlineData(288, 24)]   // 5-minute family: 24h as_of pass, N = 24h / 5min
    [InlineData(240, 4)]    // 1-minute family: 4h scheduled pass, N = 4h / 1min
    [InlineData(1440, 24)]  // 1-minute family: 24h as_of pass, N = 24h / 1min
    public void NAwareCutoff_FireRate_AtEachWindow_IsMeasuredAndReported(int n, double hours)
    {
        const int trials = 100_000;
        var rng = new Random(4058);
        var window = TimeSpan.FromHours(hours);

        var fires = 0;
        for (var i = 0; i < trials; i++)
        {
            var peak = double.NegativeInfinity;
            for (var s = 0; s < n; s++)
            {
                var z = NextStandardNormal(rng);
                if (z > peak) peak = z;
            }

            var decision = AnomalyGate.EvaluateZScore(
                mean: 0.0, effectiveStdDev: 1.0, isTrustworthy: true, peak, windowMean: SafeWindowMean,
                deviationThreshold: Threshold, magnitudeFloor: MagnitudeFloor,
                absoluteFallbackBar: AbsoluteFallbackBar, sigmaCap: SigmaCap, window: window);
            if (decision.Fire) fires++;
        }

        var rate = fires / (double)trials;
        output.WriteLine($"N={n} window={hours}h fire_rate={rate:0.#####} ({fires}/{trials})");

        // Reported, not asserted here — the cross-window agreement assertion below is what the design's
        // "20% or both under 1e-3" rule actually binds. This assert exists so the measured rate is pinned
        // in the test output / a future maintainer's diff rather than silently drifting: a rate outside
        // [0, 1] would mean the harness itself is broken.
        Assert.InRange(rate, 0.0, 1.0);
    }

    [Theory]
    [InlineData(48, 288)]   // 5-minute family: 4h (N=48) vs 24h (N=288)
    [InlineData(240, 1440)] // 1-minute family: 4h (N=240) vs 24h (N=1440)
    public void NAwareCutoff_Equalizes_4h_vs_24h_NullWindowFireRate(int nShort, int nLong)
    {
        const int trials = 100_000;
        var rateShort = MeasureFireRate(nShort, TimeSpan.FromHours(4), seed: 4058);
        var rateLong = MeasureFireRate(nLong, TimeSpan.FromHours(24), seed: 4059);
        output.WriteLine($"N={nShort}@4h rate={rateShort:0.#####} vs N={nLong}@24h rate={rateLong:0.#####}");

        // Design's tolerance rule, named per the brief: agree within 20% OF EACH OTHER when both rates are
        // non-trivial, or both sit below 1e-3 (at a nominal ~5% per-sample tail and N in the hundreds, the
        // window-level null rate at k=2.0 is itself a few percent, not 1e-3 — the "both tiny" arm is the
        // escape hatch for a cutoff so high the trial count above can't resolve a nonzero rate at all, not
        // the branch this test expects to take).
        if (rateShort < 1e-3 && rateLong < 1e-3)
        {
            Assert.True(true, $"both rates below 1e-3: 4h={rateShort:0.#####}, 24h(as {nLong}={rateLong:0.#####}) — trivially equalized");
            return;
        }

        var larger = Math.Max(rateShort, rateLong);
        var smaller = Math.Min(rateShort, rateLong);
        var relativeGap = larger > 0 ? (larger - smaller) / larger : 0.0;

        Assert.True(relativeGap <= 0.20,
            $"N={nShort}@4h rate={rateShort:0.#####} vs N={nLong}@24h rate={rateLong:0.#####} — relative gap "
            + $"{relativeGap:0.###} exceeds the design's 20% tolerance ({trials} trials/window, seeds 4058/4059).");
    }

    [Theory]
    [InlineData(4)]   // at-or-under the 4h reference window: window and null must agree on every trial
    [InlineData(2)]
    public void NAwareCutoff_AtOrUnderReferenceWindow_IsByteIdenticalToNullWindow_EveryTrial(double hours)
    {
        const int trials = 20_000;
        const int n = 48; // any N; the reference-window short-circuit does not depend on it
        var rng = new Random(4058);
        var window = TimeSpan.FromHours(hours);
        var mismatches = 0;

        for (var i = 0; i < trials; i++)
        {
            var peak = double.NegativeInfinity;
            for (var s = 0; s < n; s++)
            {
                var z = NextStandardNormal(rng);
                if (z > peak) peak = z;
            }

            var withWindow = AnomalyGate.EvaluateZScore(
                mean: 0.0, effectiveStdDev: 1.0, isTrustworthy: true, peak, windowMean: SafeWindowMean,
                deviationThreshold: Threshold, magnitudeFloor: MagnitudeFloor,
                absoluteFallbackBar: AbsoluteFallbackBar, sigmaCap: SigmaCap, window: window);
            var withoutWindow = AnomalyGate.EvaluateZScore(
                mean: 0.0, effectiveStdDev: 1.0, isTrustworthy: true, peak, windowMean: SafeWindowMean,
                deviationThreshold: Threshold, magnitudeFloor: MagnitudeFloor,
                absoluteFallbackBar: AbsoluteFallbackBar, sigmaCap: SigmaCap, window: null);

            if (withWindow.Fire != withoutWindow.Fire || withWindow.ThresholdUsed != withoutWindow.ThresholdUsed)
                mismatches++;
        }

        Assert.Equal(0, mismatches);
    }

    private static double MeasureFireRate(int n, TimeSpan window, int seed)
    {
        const int trials = 100_000;
        var rng = new Random(seed);
        var fires = 0;
        for (var i = 0; i < trials; i++)
        {
            var peak = double.NegativeInfinity;
            for (var s = 0; s < n; s++)
            {
                var z = NextStandardNormal(rng);
                if (z > peak) peak = z;
            }

            var decision = AnomalyGate.EvaluateZScore(
                mean: 0.0, effectiveStdDev: 1.0, isTrustworthy: true, peak, windowMean: SafeWindowMean,
                deviationThreshold: Threshold, magnitudeFloor: MagnitudeFloor,
                absoluteFallbackBar: AbsoluteFallbackBar, sigmaCap: SigmaCap, window: window);
            if (decision.Fire) fires++;
        }

        return fires / (double)trials;
    }

    /// <summary>
    /// #3653 A8 option B (lane L1a): the TILE arm of the same validation, for <c>AnomalyGate.EvaluateTiles</c>'s
    /// union rule (the window fires when at least 1 of H tiles fires). It covers the tile counts the design runs: 4 tiles at 4 h
    /// and 24 tiles at 24 h (design §1's table). Each tile draws n = 12 samples (the 5-minute family) and is scored
    /// against a fixed trustworthy bucket (mean 0, stddev 1).
    /// <para>INDEPENDENT samples (rho = 0): the pair gate needs the tile MEAN past the cutoff as well as the peak, and
    /// the mean of 12 independent draws almost never is. So the union rate is near zero at both window lengths. That
    /// is what the first test pins, and it is why it cannot test the correction's calibration.</para>
    /// <para>HOUR-CORRELATED samples (rho = 0.95, a shared per-hour level plus noise: the worst case design §5 names):
    /// the tile mean moves with the hour, so fires are frequent enough to measure. Only there does the per-tile cutoff
    /// k_W = <c>NAwarePeakCutoff(k, W)</c> have to hold the 4 h and 24 h union rates within 20 %. A mutation guard
    /// scores the 24 tiles WITHOUT the correction (window = 4 h, so the cutoff stays k) and requires a clearly higher
    /// rate, so the equality check cannot pass for a reason other than the correction.</para>
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(24)]
    public void EvaluateTiles_IndependentNullTiles_AlmostNeverFire(int tileCount)
    {
        var rate = MeasureTileUnionFireRate(tileCount, samplesPerTile: 12, rho: 0.0,
            window: TimeSpan.FromHours(tileCount), seed: 4058, trials: 20_000);
        output.WriteLine($"rho=0 tiles={tileCount} window={tileCount}h fire_rate={rate:0.#####}");

        Assert.True(rate < 1e-3, $"independent null tiles fired at {rate:0.#####} (tiles={tileCount}); the pair gate should keep this under 1e-3.");
    }

    [Fact]
    public void EvaluateTiles_HourCorrelatedNull_UnionFireRate_4hVs24h_WithinTwentyPercent_AndTheCorrectionIsWhatHoldsIt()
    {
        const int trials = 20_000;
        const double rho = 0.95;
        var rate4h = MeasureTileUnionFireRate(4, 12, rho, TimeSpan.FromHours(4), seed: 4058, trials: trials);
        var rate24h = MeasureTileUnionFireRate(24, 12, rho, TimeSpan.FromHours(24), seed: 4059, trials: trials);
        var uncorrected24h = MeasureTileUnionFireRate(24, 12, rho, TimeSpan.FromHours(4), seed: 4059, trials: trials);
        output.WriteLine($"rho={rho} tiles=4@4h rate={rate4h:0.#####} | tiles=24@24h rate={rate24h:0.#####} | 24 tiles uncorrected={uncorrected24h:0.#####}");

        // Large enough to measure (derived about 0.08 at 4 h for the classical k = 2.0 this bucket falls to), so the gap
        // below is not two zeros compared.
        Assert.True(rate4h > 0.02 && rate24h > 0.02, $"rates too small to compare: 4h={rate4h:0.#####}, 24h={rate24h:0.#####}");

        var relativeGap = Math.Abs(rate4h - rate24h) / Math.Max(rate4h, rate24h);
        Assert.True(relativeGap <= 0.20,
            $"tiles=4@4h rate={rate4h:0.#####} vs tiles=24@24h rate={rate24h:0.#####}: relative gap {relativeGap:0.###} exceeds 20% ({trials} trials each, seeds 4058/4059).");

        // Mutation guard: the same 24 tiles at the uncorrected cutoff fire far more often (derived about 0.39).
        Assert.True(uncorrected24h > 2 * rate24h,
            $"24 tiles uncorrected={uncorrected24h:0.#####} vs corrected={rate24h:0.#####}: the correction is not what holds the rates equal.");
    }

    private static double MeasureTileUnionFireRate(int tileCount, int samplesPerTile, double rho, TimeSpan window, int seed, int trials)
    {
        var rng = new Random(seed);
        var map = TrustworthySingleBucketMap();
        var fires = 0;
        for (var i = 0; i < trials; i++)
        {
            var tiles = BuildNullTiles(rng, tileCount, samplesPerTile, rho);
            var verdict = AnomalyGate.EvaluateTiles(
                tiles, map, Threshold, AnomalyThresholds.ModifiedZThreshold, MagnitudeFloor,
                AbsoluteFallbackBar, SigmaCap, window);
            if (verdict is { Decision.Fire: true }) fires++;
        }

        return fires / (double)trials;
    }

    /// <summary>One null bucket (mean 0, stddev 1) that answers EVERY (hour, dow) key the same way. The question
    /// here is the union rule across tiles, not bucket selection, so every tile in a trial is scored against the
    /// same trustworthy baseline.</summary>
    private static BaselineBucketMap TrustworthySingleBucketMap()
    {
        var dict = new Dictionary<(int, int), BaselineBucket>();
        for (var h = 0; h < 24; h++)
        for (var d = 0; d < 7; d++)
            dict[(h, d)] = new BaselineBucket
            {
                HourOfDay = h, DayOfWeek = d, Tier = BaselineTier.Full,
                Mean = 0, StdDev = 1, SampleCount = 100, DistinctDays = 10, AbsStdDevFloor = 0,
            };
        return new BaselineBucketMap(dict, LocalClockWindow.Utc(new DateTime(2026, 9, 24)));
    }

    /// <summary>Null tiles: each sample is sqrt(rho)·u + sqrt(1 − rho)·e, where u is the tile's shared per-hour level
    /// and e is per-sample noise, both standard normal. So every sample is N(0, 1) whatever rho is, and rho is
    /// the within-hour correlation.</summary>
    private static WindowTile[] BuildNullTiles(Random rng, int tileCount, int samplesPerTile, double rho)
    {
        var tiles = new WindowTile[tileCount];
        var shared = Math.Sqrt(rho);
        var own = Math.Sqrt(1 - rho);
        for (var t = 0; t < tileCount; t++)
        {
            var hourLevel = NextStandardNormal(rng);
            var peak = double.NegativeInfinity;
            var sum = 0.0;
            for (var s = 0; s < samplesPerTile; s++)
            {
                var z = shared * hourLevel + own * NextStandardNormal(rng);
                if (z > peak) peak = z;
                sum += z;
            }

            var localHour = new DateTime(2026, 9, 24, t % 24, 0, 0, DateTimeKind.Unspecified);
            tiles[t] = new WindowTile(localHour, peak, sum / samplesPerTile, samplesPerTile);
        }

        return tiles;
    }

    /// <summary>Box-Muller, one sample per call (discards the paired second sample — trial counts here
    /// are cheap enough that the 2x draw cost does not matter and a single-return helper is easier to
    /// call per-sample in the loops above).</summary>
    private static double NextStandardNormal(Random rng)
    {
        var u1 = 1.0 - rng.NextDouble(); // (0,1] — avoids log(0)
        var u2 = rng.NextDouble();
        return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
    }
}

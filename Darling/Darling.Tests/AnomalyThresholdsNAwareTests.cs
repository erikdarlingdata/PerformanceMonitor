/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A8 slice 1: pins the Šidák-corrected N-aware peak cutoff — <c>AnomalyThresholds.InverseNormalCdf</c>
/// (Wichura AS241), <c>AnomalyThresholds.NAwarePeakCutoff</c> itself, and the <c>AnomalyGate</c> pair overloads'
/// <c>window</c> parameter that feeds it — before the call sites that wire it are trusted. Both math
/// primitives are <c>internal</c>; <c>PerformanceMonitor.Analysis.csproj</c> carries
/// <c>InternalsVisibleTo Include="Darling.Tests"</c> for exactly this file.
/// </summary>
public class AnomalyThresholdsNAwareTests
{
    // ── InverseNormalCdf (Φ⁻¹, Wichura AS241) ──────────────────────────────────────────────────

    [Fact]
    public void InverseNormalCdf_at_0_975_matches_the_textbook_two_sided_95pct_quantile()
    {
        // The two-sided 95% normal quantile — the standard reference value everyone checks AS241 against.
        var result = AnomalyThresholds.InverseNormalCdf(0.975);
        Assert.Equal(1.959963984540054, result, precision: 12);
    }

    [Fact]
    public void InverseNormalCdf_at_1e_minus_10_is_accurate_to_1e_minus_9_relative()
    {
        // A far-tail probability, the regime NAwarePeakCutoff actually exercises (short windows against a
        // long reference ask for a tiny tail probability). Reference value from a bisection on
        // 0.5*erfc(-x/sqrt(2)) at double precision (independent of the AS241 rational approximation under
        // test): -6.3613409024040575.
        const double expected = -6.3613409024040575;
        var result = AnomalyThresholds.InverseNormalCdf(1e-10);
        var relativeError = Math.Abs((result - expected) / expected);
        Assert.True(relativeError < 1e-9, $"relative error {relativeError} for result {result}");
    }

    // ── NAwarePeakCutoff (the Šidák correction itself) ─────────────────────────────────────────

    [Theory]
    [InlineData(3.5, 4)]   // exactly the reference window
    [InlineData(3.5, 1)]   // shorter than the reference window — still returns k unchanged
    public void NAwarePeakCutoff_at_or_under_the_4h_reference_returns_k_exactly(double k, double hours)
    {
        var window = TimeSpan.FromHours(hours);
        var result = AnomalyThresholds.NAwarePeakCutoff(k, window);
        Assert.Equal(k, result); // exact — no Φ⁻¹(Φ(k)) round trip, per the method's remarks
    }

    [Fact]
    public void NAwarePeakCutoff_at_24h_is_approximately_3_95()
    {
        var result = AnomalyThresholds.NAwarePeakCutoff(3.5, TimeSpan.FromHours(24));
        Assert.InRange(result, 3.95 - 0.01, 3.95 + 0.01);
    }

    [Theory]
    [InlineData(4.0, 8.0, 24.0)]
    [InlineData(8.0, 24.0, 48.0)]
    [InlineData(4.0, 24.0, 48.0)]
    public void NAwarePeakCutoff_is_monotone_increasing_in_window_length(double shortHours, double midHours, double longHours)
    {
        const double k = 3.5;
        var shortCutoff = AnomalyThresholds.NAwarePeakCutoff(k, TimeSpan.FromHours(shortHours));
        var midCutoff = AnomalyThresholds.NAwarePeakCutoff(k, TimeSpan.FromHours(midHours));
        var longCutoff = AnomalyThresholds.NAwarePeakCutoff(k, TimeSpan.FromHours(longHours));

        Assert.True(shortCutoff <= midCutoff, $"{shortCutoff} should be <= {midCutoff}");
        Assert.True(midCutoff <= longCutoff, $"{midCutoff} should be <= {longCutoff}");
    }

    [Theory]
    [InlineData(2.0)]  // classical
    [InlineData(3.5)]  // robust
    [InlineData(5.0)]  // heavy-tail
    public void NAwarePeakCutoff_maps_every_family_cutoff_monotonically_over_4_8_24_48h(double k)
    {
        var at4h = AnomalyThresholds.NAwarePeakCutoff(k, TimeSpan.FromHours(4));
        var at8h = AnomalyThresholds.NAwarePeakCutoff(k, TimeSpan.FromHours(8));
        var at24h = AnomalyThresholds.NAwarePeakCutoff(k, TimeSpan.FromHours(24));
        var at48h = AnomalyThresholds.NAwarePeakCutoff(k, TimeSpan.FromHours(48));

        Assert.Equal(k, at4h); // reference window: byte-identical
        Assert.True(at4h <= at8h);
        Assert.True(at8h <= at24h);
        Assert.True(at24h <= at48h);
    }

    // ── AnomalyGate pair overload: the window parameter, end to end ───────────────────────────

    /// <summary>
    /// A peak whose classical z sits strictly between the robust 3.5 cutoff and its own 24h-corrected
    /// value (≈3.95) — the whole point of the slice: it must fire at the 4-hour reference window and
    /// must NOT fire at 24 hours, with the mean clause passing on both.
    /// </summary>
    private static (double Mean, double StdDev, double Peak, double WindowMean) BuildBetween35And395Case()
    {
        // mean 100, stddev 10 → peak at 3.8σ = 138 (between 3.5 and the 24h-corrected ~3.95). The mean
        // clause judges against the SAME un-corrected threshold (3.5) on every window length (class
        // remarks: only the peak clause is Šidák-corrected), so the window mean is set at 4.5σ = 145 —
        // comfortably clear of 3.5 regardless of which window is under test.
        return (Mean: 100.0, StdDev: 10.0, Peak: 138.0, WindowMean: 145.0);
    }

    [Fact]
    public void Gate_fires_at_4h_window_for_a_peak_between_3_5_and_3_95()
    {
        var (mean, stdDev, peak, windowMean) = BuildBetween35And395Case();

        var decision = AnomalyGate.EvaluateZScore(
            mean, stdDev, isTrustworthy: true, peak, windowMean,
            deviationThreshold: 3.5, magnitudeFloor: 0.0, absoluteFallbackBar: 1_000_000.0, sigmaCap: 25.0,
            window: TimeSpan.FromHours(4));

        Assert.True(decision.Fire);
        Assert.Equal(3.5, decision.ThresholdUsed);
    }

    [Fact]
    public void Gate_does_not_fire_at_24h_window_for_the_same_peak()
    {
        var (mean, stdDev, peak, windowMean) = BuildBetween35And395Case();

        var decision = AnomalyGate.EvaluateZScore(
            mean, stdDev, isTrustworthy: true, peak, windowMean,
            deviationThreshold: 3.5, magnitudeFloor: 0.0, absoluteFallbackBar: 1_000_000.0, sigmaCap: 25.0,
            window: TimeSpan.FromHours(24));

        Assert.False(decision.Fire);
        // ThresholdUsed still reports the raised bar the peak was actually judged against.
        var expectedThreshold = AnomalyThresholds.NAwarePeakCutoff(3.5, TimeSpan.FromHours(24));
        Assert.Equal(expectedThreshold, decision.ThresholdUsed);
        Assert.InRange(decision.ThresholdUsed, 3.95 - 0.01, 3.95 + 0.01);
    }

    [Fact]
    public void Gate_with_window_null_gives_exactly_todays_decision()
    {
        var (mean, stdDev, peak, windowMean) = BuildBetween35And395Case();

        var withNullWindow = AnomalyGate.EvaluateZScore(
            mean, stdDev, isTrustworthy: true, peak, windowMean,
            deviationThreshold: 3.5, magnitudeFloor: 0.0, absoluteFallbackBar: 1_000_000.0, sigmaCap: 25.0,
            window: null);

        var withoutWindowParam = AnomalyGate.EvaluateZScore(
            mean, stdDev, isTrustworthy: true, peak, windowMean,
            deviationThreshold: 3.5, magnitudeFloor: 0.0, absoluteFallbackBar: 1_000_000.0, sigmaCap: 25.0);

        Assert.Equal(withoutWindowParam.Fire, withNullWindow.Fire);
        Assert.Equal(withoutWindowParam.Sigma, withNullWindow.Sigma);
        Assert.Equal(withoutWindowParam.ThresholdUsed, withNullWindow.ThresholdUsed);
        Assert.Equal(withoutWindowParam.MeanSigma, withNullWindow.MeanSigma);

        // And a peak that clears 3.5 at the 4-hour reference window fires under a null window too —
        // null keeps the pre-#3653 verdict, which for this peak (3.8σ >= 3.5) is a fire.
        Assert.True(withNullWindow.Fire);
        Assert.Equal(3.5, withNullWindow.ThresholdUsed);
    }

    [Fact]
    public void ThresholdUsed_reports_k_sub_w_the_actually_applied_peak_cutoff()
    {
        var (mean, stdDev, peak, windowMean) = BuildBetween35And395Case();
        var window = TimeSpan.FromHours(48);

        var decision = AnomalyGate.EvaluateZScore(
            mean, stdDev, isTrustworthy: true, peak, windowMean,
            deviationThreshold: 3.5, magnitudeFloor: 0.0, absoluteFallbackBar: 1_000_000.0, sigmaCap: 25.0,
            window: window);

        var expected = AnomalyThresholds.NAwarePeakCutoff(3.5, window);
        Assert.Equal(expected, decision.ThresholdUsed);
        Assert.NotEqual(3.5, decision.ThresholdUsed); // confirms the raised bar, not the raw family cutoff
    }
}

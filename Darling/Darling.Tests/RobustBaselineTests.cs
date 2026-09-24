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
/// #1743 phase 1: the robust-statistics upgrade, pinned to the MEASUREMENTS that justified it.
/// Two real datasets carry the calibration: DARLING01's live store after weeks of HammerDB bursts
/// (the self-poisoned-baseline pathology — mean 17x the median, stddev 11,994 against a MAD of 11,
/// where the engine was fully blind to a 26x workload surge), and the production fleet's apex
/// replica (the realistic form — a busy tenant's OWN history inflating its stddev enough to mask a
/// genuine sustained 2-3x evening surge at every classical threshold). The numbers in these tests
/// are those measurements verbatim, not invented fixtures — if a refactor moves any of these
/// verdicts, it has undone the finding, not broken a style preference.
/// </summary>
public sealed class RobustBaselineTests
{
    /* ── the DARLING01 calibration set (issue #1743 comment, measured 2026-07-31) ── */

    private static BaselineBucket Darling01BatchBaseline() => new()
    {
        HourOfDay = -1,
        DayOfWeek = -1,
        Tier = BaselineTier.Flat,
        Mean = 1107,
        StdDev = 11994,
        Median = 64,
        Mad = 11,
        SampleCount = 29408,
        DistinctDays = 21,
        AbsStdDevFloor = 0, // batch requests: server-relative, no absolute floor
    };

    [Fact]
    public void Darling01_TwentySixTimesSurge_ClassicalZBlind_ModifiedZUnmissable()
    {
        var baseline = Darling01BatchBaseline();
        const double loadAverage = 1682; // the HammerDB window's real average, 26x idle

        /* The shipped-blindness pin: z against the burst-poisoned mean/stddev registers under a
           tenth of a sigma, so the classical gate at ANY sane threshold cannot fire. */
        var classical = AnomalyGate.EvaluateZScore(
            baseline.Mean, baseline.EffectiveStdDev, baseline.IsTrustworthy, loadAverage,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(classical.Fire);
        Assert.True(classical.Sigma < 0.1, $"measured 0.0σ; got {classical.Sigma}");

        /* The same value against median/MAD is 99σ — unmissable (display-capped at 25). */
        var modifiedZ = BaselineMath.ModifiedZScore(baseline, loadAverage);
        Assert.True(modifiedZ > 90 && modifiedZ < 110, $"measured 99.2; got {modifiedZ:F1}");

        var robust = AnomalyGate.EvaluateZScore(
            baseline, loadAverage,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(robust.Fire);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, robust.Sigma); // capped for display
        Assert.False(robust.LowQualityBaseline);
    }

    [Fact]
    public void Darling01_IdleNoise_StaysUnderTheMagnitudeFloor()
    {
        /* The floors compose with the robust statistic instead of competing: from a median of 64
           with MAD 11, the modified-z fires from ~97/sec — which the 500/sec magnitude floor
           correctly clamps, so quiet-metric noise stays out exactly as the issue comment argued. */
        var baseline = Darling01BatchBaseline();
        const double noisyIdle = 160; // ~5.9σ modified — well over threshold, under the floor

        var decision = AnomalyGate.EvaluateZScore(
            baseline, noisyIdle,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(decision.Fire);
    }

    /* ── the apex fleet measurement (52 production replicas, 2026-08-02) ── */

    private static BaselineBucket ApexBatchBaseline() => new()
    {
        HourOfDay = -1,
        DayOfWeek = -1,
        Tier = BaselineTier.Flat,
        Mean = 869,
        StdDev = 466,
        Median = 764,
        Mad = 148,
        SampleCount = 16298,
        DistinctDays = 17,
        AbsStdDevFloor = 0,
    };

    [Fact]
    public void ApexFridaySurge_MaskedFromClassicalZ_CaughtByModifiedZ()
    {
        /* The realistic production form of the pathology: apex's Friday-evening sustained surge
           (samples 1,533-1,799/sec against a median of 764) read 1.4-2.0 CLASSICAL sigmas — the
           busy tenant's own history inflates its stddev into self-masking — while the modified-z
           read 3.5-4.7 and fired. Values verbatim from the prod store's 24h backtest. */
        var baseline = ApexBatchBaseline();
        const double surgeSample = 1547; // the 19:25 UTC sample

        var classical = AnomalyGate.EvaluateZScore(
            baseline.Mean, baseline.EffectiveStdDev, baseline.IsTrustworthy, surgeSample,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(classical.Fire, "the classical gate measured 1.5σ here — under 2.0");

        var robust = AnomalyGate.EvaluateZScore(
            baseline, surgeSample,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(robust.Fire, "the modified-z measured 3.6 here — over 3.5, over the 500 floor");
        Assert.InRange(robust.Sigma, 3.5, 3.7);
    }

    /* ── degradation, floors, and trust interplay ── */

    [Fact]
    public void RobustlessBucket_DegradesToTheClassicalGate_NeverMisfires()
    {
        /* A bucket without robust statistics (a rollup-bound metric, a pre-#1743 cached map, or a
           pooled-synthesis tier) reports EffectiveRobustSigma 0 — the bucket overload must return
           EXACTLY the classical verdict, not silence and not a judgment against zeroed fields. */
        var bucket = new BaselineBucket
        {
            Tier = BaselineTier.Full, HourOfDay = 12, DayOfWeek = 3,
            Mean = 100, StdDev = 10, Median = 0, Mad = 0,
            SampleCount = 50, DistinctDays = 5, AbsStdDevFloor = 0,
        };
        Assert.Equal(0, bucket.EffectiveRobustSigma);

        var viaBucket = AnomalyGate.EvaluateZScore(
            bucket, 700,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        var classical = AnomalyGate.EvaluateZScore(
            bucket.Mean, bucket.EffectiveStdDev, bucket.IsTrustworthy, 700,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);

        Assert.Equal(classical, viaBucket);
        Assert.True(viaBucket.Fire); // 60σ over a real baseline, floor cleared — both paths agree
    }

    [Fact]
    public void MadCollapse_OnABoundedMetric_TheAbsoluteFloorKeepsSigmaSane()
    {
        /* Fleet-measured, MAD hit zero only on idle-box CPU — exactly where the bounded-metric
           absolute dispersion floor applies. A 3-point CPU wobble over a MAD-collapsed baseline
           must not manufacture a fire: sigma is judged against the 5.0-point floor, not zero. */
        var bucket = new BaselineBucket
        {
            Tier = BaselineTier.Flat, HourOfDay = -1, DayOfWeek = -1,
            Mean = 2.1, StdDev = 0.4, Median = 2.0, Mad = 0.0,
            SampleCount = 7000, DistinctDays = 4,
            AbsStdDevFloor = 5.0, // BaselineMath.AbsStdDevFloorFor(MetricNames.Cpu)
        };
        Assert.Equal(5.0, bucket.EffectiveRobustSigma);

        var decision = AnomalyGate.EvaluateZScore(
            bucket, 5.0,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.CpuFloorPct, AnomalyThresholds.CpuFallbackPct,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(decision.Fire);
    }

    [Fact]
    public void MemoryFallbackBar_DoesNotFireOnHealthyTotalEqualsTarget()
    {
        /* #1996, found dogfooding on the production fleet: memory's healthy steady state is
           total ≈ target = 100%, and its fallback bar sat at 95 — so every untrustworthy bucket
           (403 of 406 firing buckets had exactly 2 distinct days, one under the Full-tier floor)
           fired on NORMAL behavior, rendered to operators as "spiked to 100% — 0σ above its 100%
           baseline". The bar now sits above 100: healthy stays silent on thin baselines, genuine
           over-target pressure still fires. */
        var thinBucket = new BaselineBucket
        {
            Tier = BaselineTier.Full, HourOfDay = 3, DayOfWeek = 0,
            Mean = 100.0, StdDev = 0.05, Median = 100.0, Mad = 0.02,
            SampleCount = 82, DistinctDays = 2, // plenty of samples, under the day floor
            AbsStdDevFloor = 4.0,
        };
        Assert.False(thinBucket.IsTrustworthy);

        var healthy = AnomalyGate.EvaluateZScore(
            thinBucket, 100.1,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.MemoryPressureFloorPct, AnomalyThresholds.MemoryPressureFallbackPct,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(healthy.Fire, "total = target is the goal state, not pressure");

        var overTarget = AnomalyGate.EvaluateZScore(
            thinBucket, 103.0,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.MemoryPressureFloorPct, AnomalyThresholds.MemoryPressureFallbackPct,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(overTarget.Fire, "total exceeding target is genuine pressure and must still fire");
        Assert.True(overTarget.LowQualityBaseline);
    }

    [Fact]
    public void UntrustworthyBaseline_RobustPath_FiresOnlyOnTheAbsoluteBar()
    {
        /* The trust/fallback complementarity is unchanged by the robust statistic: a thin baseline
           does not trust ANY deviation score and fires only on the higher absolute bar. */
        var bucket = new BaselineBucket
        {
            Tier = BaselineTier.Full, HourOfDay = 9, DayOfWeek = 2,
            Mean = 100, StdDev = 10, Median = 95, Mad = 8,
            SampleCount = 4, DistinctDays = 1, AbsStdDevFloor = 0, // below Full trust floors
        };
        Assert.False(bucket.IsTrustworthy);

        var under = AnomalyGate.EvaluateZScore(
            bucket, 2000,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(under.Fire); // enormous modified-z, but under the 5000 fallback bar
        Assert.True(under.LowQualityBaseline);

        var over = AnomalyGate.EvaluateZScore(
            bucket, 6000,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(over.Fire);
        Assert.True(over.FallbackExceedance >= 1.0);
    }

    /* ── #3653 (A8, first slice): the gate judges the window PEAK AND the window MEAN ──

       The pre-#3653 gate tested the window MAX against a per-SAMPLE hour×dow distribution, so its null
       expectation rose with the number of samples in the window and one hot sample read as a fired
       anomaly. The pair gate fires only when both statistics clear the EXISTING cutoffs: z on both when
       the baseline is trustworthy (floor on the peak, as today); the absolute-fallback bar on the peak
       AND the magnitude floor on the mean when it is not. The reported Sigma stays the peak's. */

    /// <summary>A trustworthy classical bucket (no robust stats) — mean 10, stddev 2 — in CPU units so the
    /// 50% floor and 90% fallback bar carry their shipped meaning.</summary>
    private static BaselineBucket TrustedCpuBaseline() => new()
    {
        Tier = BaselineTier.Full, HourOfDay = 14, DayOfWeek = 2,
        Mean = 10, StdDev = 2, Median = 0, Mad = 0,
        SampleCount = 60, DistinctDays = 5, AbsStdDevFloor = 0,
    };

    private static AnomalyGate.ZDecision CpuPair(BaselineBucket bucket, double peak, double windowMean) =>
        AnomalyGate.EvaluateZScore(
            bucket, peak, windowMean,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.CpuFloorPct, AnomalyThresholds.CpuFallbackPct,
            AnomalyThresholds.SigmaDisplayCap);

    private static AnomalyGate.ZDecision CpuPeakOnly(BaselineBucket bucket, double peak) =>
        AnomalyGate.EvaluateZScore(
            bucket, peak,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.CpuFloorPct, AnomalyThresholds.CpuFallbackPct,
            AnomalyThresholds.SigmaDisplayCap);

    [Fact]
    public void PairGate_Trustworthy_OneHotSample_PeakClearsMeanDoesNot_NoFire_WherePeakAloneFired()
    {
        /* THE A8 shape: a 90% sample in a window that otherwise idled at the 10% baseline. The peak is
           40σ out and over the 50% floor — the peak-only gate fires (that is the bias being fixed) — but
           the window mean of 12% is 1σ, under the 2.0 cutoff, so the pair gate stays quiet. */
        var bucket = TrustedCpuBaseline();
        Assert.True(bucket.IsTrustworthy);
        Assert.Equal(0, bucket.EffectiveRobustSigma); // classical frame

        Assert.True(CpuPeakOnly(bucket, 90).Fire, "red-first: the peak-only verdict is the pre-#3653 fire");

        var pair = CpuPair(bucket, peak: 90, windowMean: 12);
        Assert.False(pair.Fire);
        Assert.False(pair.LowQualityBaseline);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, pair.Sigma); // the peak's, capped — still reported
        Assert.Equal(1.0, pair.MeanSigma!.Value, precision: 6);      // (12 - 10) / 2
    }

    [Fact]
    public void PairGate_Trustworthy_MeanClearsButPeakUnderTheFloor_NoFire()
    {
        /* The peak side of the AND: for a consistent pair (mean <= peak) the mean's z clearing implies the
           peak's z clears too, so the only way the PEAK fails on this path is the #1486 magnitude floor —
           a window running 10σ sustained (mean 30%) with a 45% peak is a real shift on a trivial value,
           and the floor keeps it out exactly as it did before the pair. */
        var bucket = TrustedCpuBaseline();
        var pair = CpuPair(bucket, peak: 45, windowMean: 30);
        Assert.False(pair.Fire);
        Assert.Equal(10.0, pair.MeanSigma!.Value, precision: 6); // the mean cleared; the peak did not
    }

    [Fact]
    public void PairGate_Trustworthy_BothClear_Fires_SigmaIsThePeaks_MeanSigmaRidesBeside()
    {
        /* A sustained shift: peak 90% (40σ, capped to 25 for display), window mean 30% (10σ). Both clear
           the 2.0 cutoff, the peak clears the floor — fire, and the two deviations are carried separately
           so the story can say "peak 25σ, mean 10σ" instead of letting one number stand for the window. */
        var bucket = TrustedCpuBaseline();
        var pair = CpuPair(bucket, peak: 90, windowMean: 30);
        Assert.True(pair.Fire);
        Assert.False(pair.LowQualityBaseline);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, pair.Sigma);
        Assert.Equal(10.0, pair.MeanSigma!.Value, precision: 6);
        Assert.Equal(0.0, pair.FallbackExceedance);
        Assert.Equal(AnomalyThresholds.DefaultDeviationThreshold, pair.ThresholdUsed);
    }

    [Fact]
    public void PairGate_Trustworthy_MeanExactlyAtTheCutoff_Fires_TheCutoffIsInclusiveLikeThePeaks()
    {
        /* The existing peak rule is >= threshold; the mean's must be the SAME rule, not a stricter one. */
        var bucket = TrustedCpuBaseline();
        Assert.True(CpuPair(bucket, peak: 90, windowMean: 14).Fire);   // (14 - 10) / 2 = 2.0 exactly
        Assert.False(CpuPair(bucket, peak: 90, windowMean: 13.9).Fire);
    }

    [Fact]
    public void PairGate_Untrustworthy_TwoBars_PeakOverTheFallbackBar_AndMeanOverTheFloor()
    {
        /* The untrustworthy path trusts no z on either statistic, so the mean gets the one absolute
           instrument the path has — the magnitude FLOOR (50%), deliberately not the 90% fallback bar,
           which is sized for a peak on a young store and would demand a window pegged the whole way
           through. Three windows over the same thin baseline: */
        var thin = new BaselineBucket
        {
            Tier = BaselineTier.Full, HourOfDay = 14, DayOfWeek = 2,
            Mean = 10, StdDev = 2, Median = 0, Mad = 0,
            SampleCount = 16, DistinctDays = 2, AbsStdDevFloor = 0, // under the Full-tier 3-day floor
        };
        Assert.False(thin.IsTrustworthy);

        /* Peak 95 over the 90 bar, mean 60 over the 50 floor → fire; the exceedance stays the PEAK's. */
        var both = CpuPair(thin, peak: 95, windowMean: 60);
        Assert.True(both.Fire);
        Assert.True(both.LowQualityBaseline);
        Assert.Equal(95.0 / 90.0, both.FallbackExceedance, precision: 6);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, both.Sigma);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, both.MeanSigma!.Value); // (60 - 10) / 2 = 25, at the cap

        /* Peak 95 over the bar, mean 12 UNDER the floor → one hot sample on a young store: no fire. The
           peak-only gate fired here — the pre-#3653 verdict. */
        Assert.True(CpuPeakOnly(thin, 95).Fire);
        var hotSample = CpuPair(thin, peak: 95, windowMean: 12);
        Assert.False(hotSample.Fire);
        Assert.True(hotSample.LowQualityBaseline);
        Assert.Equal(95.0 / 90.0, hotSample.FallbackExceedance, precision: 6); // still stamped; the scorer never sees a non-fire

        /* Peak 80 UNDER the bar, mean 60 over the floor → the mean clears but the peak does not: no fire
           (the fallback bar on the peak is unchanged by the pair). */
        Assert.False(CpuPair(thin, peak: 80, windowMean: 60).Fire);

        /* Mean exactly AT the floor clears it — inclusive, like every other bar in the gate. */
        Assert.True(CpuPair(thin, peak: 95, windowMean: 50).Fire);
    }

    [Fact]
    public void PairGate_RobustFrame_JudgesTheMeanAsAModifiedZ_AgainstMedianAndMad()
    {
        /* The Darling01 calibration bucket (median 64, MAD 11 → robust sigma 16.3): the HammerDB window's
           real 1682/sec average as BOTH statistics is 99σ modified on each — fire, MeanSigma capped like
           Sigma. The same 1682 peak over a window that averaged 70/sec (0.37σ modified) is one burst in an
           idle window — no fire, and the peak-only verdict fired. */
        var baseline = Darling01BatchBaseline();
        Assert.True(baseline.EffectiveRobustSigma > 0);

        var sustained = AnomalyGate.EvaluateZScore(
            baseline, 1682, 1682,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(sustained.Fire);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, sustained.Sigma);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, sustained.MeanSigma!.Value);
        Assert.Equal(AnomalyThresholds.ModifiedZThreshold, sustained.ThresholdUsed);

        var burst = AnomalyGate.EvaluateZScore(
            baseline, 1682, 70,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(burst.Fire);
        Assert.InRange(burst.MeanSigma!.Value, 0.3, 0.45); // (70 - 64) / (11 / 0.6745) = 0.37
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, burst.Sigma); // the peak's 99σ, capped, still reported

        var peakOnly = AnomalyGate.EvaluateZScore(
            baseline, 1682,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.BatchRequestFloor, AnomalyThresholds.BatchRequestFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(peakOnly.Fire, "red-first: the peak-only robust verdict is the pre-#3653 fire");
    }

    [Fact]
    public void PairGate_RobustlessBucket_DegradesToTheClassicalPairGate_Exactly()
    {
        /* Mirror of RobustlessBucket_DegradesToTheClassicalGate_NeverMisfires for the pair: a bucket with
           zeroed robust fields must hand the pair to the classical pair gate, verdict for verdict. */
        var bucket = TrustedCpuBaseline();
        var viaBucket = CpuPair(bucket, peak: 90, windowMean: 30);
        var classical = AnomalyGate.EvaluateZScore(
            bucket.Mean, bucket.EffectiveStdDev, bucket.IsTrustworthy, 90, 30,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.CpuFloorPct, AnomalyThresholds.CpuFallbackPct,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.Equal(classical, viaBucket);
        Assert.True(viaBucket.Fire);
    }

    [Fact]
    public void PeakOnlyOverloads_ReturnTodaysVerdict_WithNoMeanSigma_TheTransitionalContract()
    {
        /* The PostgreSQL-target detectors reach the gate through the peak-only overloads until their
           content lanes pass a mean: those overloads must be BYTE-FOR-BYTE the pre-#3653 verdict — which
           is the pair verdict with the mean clauses vacuous — and must say so with a null MeanSigma
           rather than a 0 that would read as "the mean sat at baseline". */
        var bucket = TrustedCpuBaseline();
        var peakOnly = CpuPeakOnly(bucket, 90);
        Assert.Null(peakOnly.MeanSigma);
        Assert.True(peakOnly.Fire);

        /* A pair whose mean equals its peak has nothing for the mean clause to add: identical verdict. */
        var degeneratePair = CpuPair(bucket, peak: 90, windowMean: 90);
        Assert.Equal(peakOnly with { MeanSigma = degeneratePair.MeanSigma }, degeneratePair);

        var thin = new BaselineBucket
        {
            Tier = BaselineTier.Full, HourOfDay = 14, DayOfWeek = 2,
            Mean = 10, StdDev = 2, SampleCount = 16, DistinctDays = 2, AbsStdDevFloor = 0,
        };
        var thinPeakOnly = CpuPeakOnly(thin, 95);
        Assert.Null(thinPeakOnly.MeanSigma);
        Assert.True(thinPeakOnly.Fire);
        Assert.Equal(thinPeakOnly with { MeanSigma = null }, CpuPair(thin, 95, 95) with { MeanSigma = null });
    }

    [Fact]
    public void PairGate_TheStoryCarriesBothDeviations_AndTheScorerStillGradesThePeaks()
    {
        /* The composed story says "peak Nσ … the window's mean Mσ" when the fact carries the pair; a fact
           from before the pair (no mean sigma) keeps the single-deviation sentence verbatim. And the
           scorer — FactScorer.ScoreAnomalyFact, unchanged by #3653 — grades off deviation_sigma exactly as
           before: the mean's sigma is display, not severity. */
        static Fact Cpu(bool withMean) => new()
        {
            Source = "anomaly",
            Key = "ANOMALY_CPU_SPIKE",
            Value = 90,
            Metadata = withMean
                ? new Dictionary<string, double>
                {
                    ["peak_cpu"] = 90, ["avg_cpu_in_window"] = 30, ["baseline_mean"] = 10, ["baseline_samples"] = 60,
                    ["deviation_sigma"] = 16.0, ["mean_deviation_sigma"] = 4.0, ["fire_threshold"] = 2.0, ["confidence"] = 1.0,
                }
                : new Dictionary<string, double>
                {
                    ["peak_cpu"] = 90, ["avg_cpu_in_window"] = 30, ["baseline_mean"] = 10, ["baseline_samples"] = 60,
                    ["deviation_sigma"] = 16.0, ["fire_threshold"] = 2.0, ["confidence"] = 1.0,
                },
        };

        var paired = Cpu(withMean: true);
        var composed = FactAdvice.Compose("ANOMALY_CPU_SPIKE", new Dictionary<string, Fact> { ["ANOMALY_CPU_SPIKE"] = paired })!;
        Assert.Contains("16σ above its 10% baseline", composed.Investigation, StringComparison.Ordinal);
        Assert.Contains("the window's mean of 30% sat 4σ above it", composed.Investigation, StringComparison.Ordinal);
        Assert.Contains("16σ above its baseline", composed.Headline, StringComparison.Ordinal); // the headline stays the peak's

        var legacy = Cpu(withMean: false);
        var legacyComposed = FactAdvice.Compose("ANOMALY_CPU_SPIKE", new Dictionary<string, Fact> { ["ANOMALY_CPU_SPIKE"] = legacy })!;
        Assert.DoesNotContain("window's mean", legacyComposed.Investigation, StringComparison.Ordinal);
        Assert.Contains("16σ above its 10% baseline", legacyComposed.Investigation, StringComparison.Ordinal);

        Assert.Equal(Score(legacy), Score(paired));
        Assert.Equal(1.0, Score(paired), precision: 3); // 16σ against a 2.0 anchor saturates — the peak's grade, as before
    }

    /* ── tier selection over exact (GROUPING SETS) sentinel buckets ── */

    [Fact]
    public void SelectBucket_PrefersTheExactSentinelTier_OverPooledSynthesis()
    {
        /* Medians cannot be pooled, so when the provider supplies exact hour-only/flat tiers under
           sentinel keys, SelectBucket must hand back THOSE (robust fields intact) instead of
           synthesizing a pooled bucket whose Median/Mad would be zero. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            // Sparse full bucket: below CollapseThreshold, so selection must move down a tier.
            [(14, 2)] = new BaselineBucket { HourOfDay = 14, DayOfWeek = 2, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, Median = 99, Mad = 4, SampleCount = 4, DistinctDays = 2 },
            // A sibling full bucket the POOLED path would have folded in.
            [(14, 3)] = new BaselineBucket { HourOfDay = 14, DayOfWeek = 3, Tier = BaselineTier.Full, Mean = 300, StdDev = 5, Median = 299, Mad = 4, SampleCount = 40, DistinctDays = 5 },
            // The provider's EXACT hour-only tier for hour 14.
            [(14, -1)] = new BaselineBucket { HourOfDay = 14, DayOfWeek = -1, Tier = BaselineTier.HourOnly, Mean = 250, StdDev = 80, Median = 240, Mad = 60, SampleCount = 44, DistinctDays = 12 },
            // The provider's EXACT flat tier.
            [(-1, -1)] = new BaselineBucket { HourOfDay = -1, DayOfWeek = -1, Tier = BaselineTier.Flat, Mean = 180, StdDev = 90, Median = 170, Mad = 70, SampleCount = 900, DistinctDays = 17 },
        };

        var selected = BaselineMath.SelectBucket(map, 14, 2);
        Assert.Equal(BaselineTier.HourOnly, selected.Tier);
        Assert.Equal(240, selected.Median); // the exact tier's robust center, not a zeroed synthesis
        Assert.Equal(60, selected.Mad);

        /* An hour with no data at all falls through to the exact flat sentinel. */
        var flat = BaselineMath.SelectBucket(map, 3, 6);
        Assert.Equal(BaselineTier.Flat, flat.Tier);
        Assert.Equal(170, flat.Median);
    }

    [Fact]
    public void SelectBucket_WithoutSentinels_PooledFallback_ExcludesNothingAndZeroesRobustFields()
    {
        /* A pre-#1743 map (no sentinel keys): pooling still works exactly as before, and the
           synthesized bucket's zeroed robust fields are what routes the gate down its classical
           path — degradation, not misfire. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            [(14, 2)] = new BaselineBucket { HourOfDay = 14, DayOfWeek = 2, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, SampleCount = 6, DistinctDays = 2 },
            [(14, 3)] = new BaselineBucket { HourOfDay = 14, DayOfWeek = 3, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, SampleCount = 6, DistinctDays = 2 },
        };

        var selected = BaselineMath.SelectBucket(map, 14, 2);
        Assert.Equal(BaselineTier.HourOnly, selected.Tier);
        Assert.Equal(12, selected.SampleCount);
        Assert.Equal(0, selected.Median);
        Assert.Equal(0, selected.EffectiveRobustSigma);
    }

    /* ── #3653 A8 option B: fall back to the first coarser TRUSTWORTHY tier ── */

    [Fact]
    public void SelectBucket_ChosenFullUntrustworthy_ExactHourOnlyTrustworthy_ReturnsHourOnly()
    {
        /* Full clears its sample count (RestoreThreshold) but has only 2 distinct days — a young
           target, weeks from Full's 3-distinct-day floor. The exact hour-only sentinel already
           clears its own (higher) floors, so the fallback should hand it back instead of the
           absolute-fallback Full bucket. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            [(5, 1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = 1, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, SampleCount = 20, DistinctDays = 2 },
            [(5, -1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = -1, Tier = BaselineTier.HourOnly, Mean = 90, StdDev = 20, SampleCount = 10, DistinctDays = 10 },
        };

        var selected = BaselineMath.SelectBucket(map, 5, 1);
        Assert.Equal(BaselineTier.HourOnly, selected.Tier);
        Assert.Equal(90, selected.Mean);
    }

    [Fact]
    public void SelectBucket_FullAndHourOnlyUntrustworthy_ExactFlatTrustworthy_ReturnsFlat()
    {
        /* Full is untrustworthy (2 distinct days) AND the hour-only tier is also untrustworthy (5
           distinct days, short of its 10-day floor) — the walk must not stop at HourOnly, it must
           keep going to Flat. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            [(5, 1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = 1, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, SampleCount = 20, DistinctDays = 2 },
            [(5, -1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = -1, Tier = BaselineTier.HourOnly, Mean = 90, StdDev = 10, SampleCount = 15, DistinctDays = 5 },
            [(-1, -1)] = new BaselineBucket { HourOfDay = -1, DayOfWeek = -1, Tier = BaselineTier.Flat, Mean = 95, StdDev = 8, SampleCount = 50, DistinctDays = 10 },
        };

        var selected = BaselineMath.SelectBucket(map, 5, 1);
        Assert.Equal(BaselineTier.Flat, selected.Tier);
        Assert.Equal(95, selected.Mean);
    }

    [Fact]
    public void SelectBucket_EveryTierUntrustworthy_ReturnsChosenFull_TheAbsoluteFallbackAsToday()
    {
        /* Full, hour-only AND flat are all untrustworthy — the walk finds nothing and returns
           today's chosen bucket unchanged: the absolute-fallback path, exactly as before this
           lane. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            [(5, 1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = 1, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, SampleCount = 20, DistinctDays = 2 },
            [(5, -1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = -1, Tier = BaselineTier.HourOnly, Mean = 90, StdDev = 10, SampleCount = 15, DistinctDays = 5 },
            [(-1, -1)] = new BaselineBucket { HourOfDay = -1, DayOfWeek = -1, Tier = BaselineTier.Flat, Mean = 95, StdDev = 8, SampleCount = 50, DistinctDays = 2 },
        };

        var selected = BaselineMath.SelectBucket(map, 5, 1);
        Assert.Equal(BaselineTier.Full, selected.Tier);
        Assert.Equal(100, selected.Mean);
        Assert.Equal(2, selected.DistinctDays);
    }

    [Fact]
    public void SelectBucket_ChosenFullIsZeroHistory_NeverReplacedByACoarserTier()
    {
        /* A Full bucket that clears its floors with every statistic at zero is IsZeroHistory —
           the strongest statement a bounded metric's baseline can make — and must never be
           overridden by a coarser tier even when one is trustworthy. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            [(5, 1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = 1, Tier = BaselineTier.Full, Mean = 0, StdDev = 0, Median = 0, Mad = 0, SampleCount = 20, DistinctDays = 5 },
            [(5, -1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = -1, Tier = BaselineTier.HourOnly, Mean = 90, StdDev = 10, SampleCount = 15, DistinctDays = 12 },
            [(-1, -1)] = new BaselineBucket { HourOfDay = -1, DayOfWeek = -1, Tier = BaselineTier.Flat, Mean = 95, StdDev = 8, SampleCount = 50, DistinctDays = 10 },
        };

        var selected = BaselineMath.SelectBucket(map, 5, 1);
        Assert.Equal(BaselineTier.Full, selected.Tier);
        Assert.True(selected.IsZeroHistory);
        Assert.Equal(0, selected.Mean);
    }

    [Fact]
    public void SelectBucket_TrustworthyFull_ReturnedUnchanged_MatureTargetIsByteIdentical()
    {
        /* A mature target's Full bucket is already trustworthy — the fallback walk must not run
           at all, and the bucket handed back is the exact same instance selected by sample count
           today. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            [(5, 1)] = new BaselineBucket { HourOfDay = 5, DayOfWeek = 1, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, SampleCount = 20, DistinctDays = 5 },
        };

        var selected = BaselineMath.SelectBucket(map, 5, 1);
        Assert.Equal(BaselineTier.Full, selected.Tier);
        Assert.True(selected.IsTrustworthy);
        Assert.Same(map[(5, 1)], selected);
    }

    [Fact]
    public void SelectBucket_NoSentinels_PooledFullUntrustworthy_PooledHourOnlyTrustworthy_ReturnsPooledHourOnly()
    {
        /* No provider sentinels at all (pre-#1743 map): the fallback tier must be built by the
           SAME pooled synthesis as today's selection, not skipped just because there's no exact
           bucket to hand back. Five hour-8 dow buckets pool to 40 samples across 10 distinct days
           with differing means, which is enough to clear HourOnly's (10 sample, 10 day) floor with
           real pooled dispersion. */
        var map = new Dictionary<(int, int), BaselineBucket>
        {
            [(8, 1)] = new BaselineBucket { HourOfDay = 8, DayOfWeek = 1, Tier = BaselineTier.Full, Mean = 100, StdDev = 5, SampleCount = 20, DistinctDays = 2 },
            [(8, 2)] = new BaselineBucket { HourOfDay = 8, DayOfWeek = 2, Tier = BaselineTier.Full, Mean = 110, StdDev = 5, SampleCount = 5, DistinctDays = 2 },
            [(8, 3)] = new BaselineBucket { HourOfDay = 8, DayOfWeek = 3, Tier = BaselineTier.Full, Mean = 90, StdDev = 5, SampleCount = 5, DistinctDays = 2 },
            [(8, 4)] = new BaselineBucket { HourOfDay = 8, DayOfWeek = 4, Tier = BaselineTier.Full, Mean = 105, StdDev = 5, SampleCount = 5, DistinctDays = 2 },
            [(8, 5)] = new BaselineBucket { HourOfDay = 8, DayOfWeek = 5, Tier = BaselineTier.Full, Mean = 95, StdDev = 5, SampleCount = 5, DistinctDays = 2 },
        };

        var selected = BaselineMath.SelectBucket(map, 8, 1);
        Assert.Equal(BaselineTier.HourOnly, selected.Tier);
        Assert.Equal(40, selected.SampleCount);
        Assert.Equal(10, selected.DistinctDays);
        Assert.True(selected.IsTrustworthy);
    }

    /* ── honest confidence ── */

    [Fact]
    public void Confidence_TierAndDensity_UntrustworthyIsZero()
    {
        var fullDense = new BaselineBucket { Tier = BaselineTier.Full, Mean = 100, StdDev = 10, SampleCount = 20, DistinctDays = 5 };
        Assert.Equal(1.0, fullDense.Confidence, precision: 3);

        var fullThin = new BaselineBucket { Tier = BaselineTier.Full, Mean = 100, StdDev = 10, SampleCount = 10, DistinctDays = 3 };
        Assert.Equal(0.5, fullThin.Confidence, precision: 3);

        var flat = new BaselineBucket { Tier = BaselineTier.Flat, Mean = 100, StdDev = 10, SampleCount = 600, DistinctDays = 17 };
        Assert.Equal(0.7, flat.Confidence, precision: 3);

        var untrustworthy = new BaselineBucket { Tier = BaselineTier.Full, Mean = 100, StdDev = 10, SampleCount = 4, DistinctDays = 1 };
        Assert.Equal(0.0, untrustworthy.Confidence);
    }

    /* ── the scorer keeps the catches the detector makes ── */

    private static double Score(Fact fact)
    {
        new FactScorer().ScoreAll(new List<Fact> { fact });
        return fact.BaseSeverity;
    }

    [Fact]
    public void WaitProfileScoring_GradesOffModifiedZ_SoTheMaskedSurgeClassSurvives()
    {
        /* Without this arm, a wait-profile fact fired at modified-z 6 with a ratio of 1.5 would be
           zeroed by the ratio floor immediately after being caught — the fleet backtest's entire
           modz-only class (1,281 samples the ratio never saw) silently dropped at scoring. */
        var caught = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_WAIT_PROFILE",
            Value = 1,
            Metadata = new Dictionary<string, double> { ["modified_z"] = 6.0, ["ratio"] = 1.5, ["is_new"] = 0 },
        };
        Assert.True(Score(caught) >= 0.5);

        var underCutoff = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_WAIT_PROFILE",
            Value = 1,
            Metadata = new Dictionary<string, double> { ["modified_z"] = 4.0, ["ratio"] = 1.5, ["is_new"] = 0 },
        };
        Assert.Equal(0.0, Score(underCutoff));

        /* Pre-#1743 facts (no modified_z) keep the ratio ramp; the is_new sentinel ratio must keep
           scoring through the ratio path even though its fact now carries a modified_z. */
        var legacy = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_WAIT_PROFILE",
            Value = 1,
            Metadata = new Dictionary<string, double> { ["ratio"] = 6.0 },
        };
        Assert.True(Score(legacy) > 0.5);

        var isNew = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_WAIT_PROFILE",
            Value = 1,
            Metadata = new Dictionary<string, double> { ["modified_z"] = 2.0, ["ratio"] = AnomalyThresholds.NoBaselineRatio, ["is_new"] = 1 },
        };
        Assert.True(Score(isNew) > 0.5);
    }

    [Fact]
    public void WaitProfilePair_TheStoryNamesTheMeanBesideThePeak_AndTheScorerStillGradesThePeaks()
    {
        /* #3741: the wait-profile detector now fires its trusted arms only when the window MEAN clears its
           bar beside the peak, and stamps avg_ms_per_sec / mean_modified_z beside current_ms_per_sec /
           modified_z. The composed story says so — with the mean's robust deviation on the gate arm, its
           multiple of the baseline on the ratio arm (modified z 0 by construction there), and as plain
           context on is_new, where the bar is on the peak ALONE and the sentence must not claim a gate the
           detector did not apply. A pre-#3741 fact (no mean) keeps its sentence verbatim. And the scorer
           — FactScorer's wait-profile arm, unchanged — grades off modified_z / ratio exactly as before. */
        static Fact Profile(bool withMean, bool robust, bool isNew)
        {
            var metadata = new Dictionary<string, double>
            {
                ["current_ms_per_sec"] = 3200, ["baseline_mean"] = 200, ["total_wait_ms"] = 7_710_000,
                ["ratio"] = isNew ? AnomalyThresholds.NoBaselineRatio : 16.0,
                ["modified_z"] = robust ? 20.235 : 0, ["is_new"] = isNew ? 1 : 0,
                ["contrib_SOS_SCHEDULER_YIELD"] = 7_710_000,
            };
            if (withMean)
            {
                metadata["avg_ms_per_sec"] = 1606.25;
                metadata["mean_modified_z"] = robust ? 9.485 : 0;
            }
            return new Fact { Source = "anomaly", Key = "ANOMALY_WAIT_PROFILE", Value = 7_710_000, Metadata = metadata };
        }

        static string Story(Fact f) =>
            FactAdvice.Compose("ANOMALY_WAIT_PROFILE", new Dictionary<string, Fact> { ["ANOMALY_WAIT_PROFILE"] = f })!.Investigation;

        var gateArm = Profile(withMean: true, robust: true, isNew: false);
        var story = Story(gateArm);
        Assert.Contains("peaked at about 3200 ms/sec", story, StringComparison.Ordinal);
        Assert.Contains("the window's mean of 1606.3 ms/sec sat 9.5 robust σ above the baseline median", story, StringComparison.Ordinal);
        Assert.Contains("fires only when the mean clears its bar as well", story, StringComparison.Ordinal);

        var ratioArm = Profile(withMean: true, robust: false, isNew: false);
        var ratioStory = Story(ratioArm);
        Assert.Contains("the window's mean of 1606.3 ms/sec was roughly 8× that baseline", ratioStory, StringComparison.Ordinal);
        Assert.Contains("fires only when the mean clears its bar as well", ratioStory, StringComparison.Ordinal);
        Assert.DoesNotContain("robust σ", ratioStory, StringComparison.Ordinal);

        var isNewArm = Profile(withMean: true, robust: false, isNew: true);
        var isNewStory = Story(isNewArm);
        Assert.Contains("(the window's mean was about 1606.3 ms/sec)", isNewStory, StringComparison.Ordinal);
        Assert.DoesNotContain("fires only when", isNewStory, StringComparison.Ordinal); // the bar is on the peak alone here

        var legacy = Profile(withMean: false, robust: true, isNew: false);
        var legacyStory = Story(legacy);
        Assert.DoesNotContain("window's mean", legacyStory, StringComparison.Ordinal);
        Assert.Contains("peaked at about 3200 ms/sec this window — roughly 16× the 200 ms/sec normal", legacyStory, StringComparison.Ordinal);

        /* The grade is the peak's, before and after the pair. */
        Assert.Equal(Score(legacy), Score(gateArm));
        Assert.Equal(1.0, Score(gateArm), precision: 3); // 20.2σ against the 5.0 cutoff saturates at 15σ
        Assert.Equal(Score(Profile(withMean: false, robust: false, isNew: false)), Score(ratioArm));
    }

    [Fact]
    public void GenericAnomalyScoring_AnchorsTheRampOnTheFiringThreshold()
    {
        /* Review-caught on this PR: the generic deviation ramp was still 2σ→4σ while robust fires
           start at 3.5σ (or 5.0σ for query duration — past the old saturation point, so every fire
           scored a flat 1.0 with zero differentiation). The ramp now anchors on fire_threshold and
           saturates at 2x the anchor — byte-identical to the old shape for classical and pre-#1743
           facts, proportional for robust fires. */
        var atCutoff = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_QUERY_DURATION",
            Value = 1,
            Metadata = new Dictionary<string, double>
                { ["deviation_sigma"] = 5.0, ["fire_threshold"] = 5.0, ["confidence"] = 1.0 },
        };
        Assert.Equal(0.5, Score(atCutoff), precision: 3);

        var saturated = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_QUERY_DURATION",
            Value = 1,
            Metadata = new Dictionary<string, double>
                { ["deviation_sigma"] = 10.0, ["fire_threshold"] = 5.0, ["confidence"] = 1.0 },
        };
        Assert.Equal(1.0, Score(saturated), precision: 3);

        /* A pre-#1743 fact carries no fire_threshold: the old 2σ→4σ ramp verbatim. */
        var legacy = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_CPU_SPIKE",
            Value = 1,
            Metadata = new Dictionary<string, double> { ["deviation_sigma"] = 3.0, ["confidence"] = 1.0 },
        };
        Assert.Equal(0.75, Score(legacy), precision: 3);
    }

    /* ── the heavy-tail threshold routing ── */

    [Fact]
    public void ModifiedZThresholds_HeavyTailFamiliesAtFive_EverythingElseAtThreePointFive()
    {
        Assert.Equal(5.0, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.WaitStats));
        Assert.Equal(5.0, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.WaitMsPerSec));
        Assert.Equal(5.0, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.QueryDuration));
        Assert.Equal(3.5, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.BatchRequests));
        Assert.Equal(3.5, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.Cpu));
        Assert.Equal(3.5, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.SessionCount));
    }

    /* ── #3691 lane 41: a zero-history baseline is the STRONGEST evidence, not none ──

       An all-zero bucket has no dispersion, so EffectiveStdDev is 0 and IsTrustworthy is false for it
       however many samples it holds — which routed a month of measured quiet to the absolute-fallback bar
       sized for a young store. The measured face: ANOMALY_PG_BLOCKING, 92 blocked sessions against a clean
       month, severity 0.5, worded "first occurrence, no baseline yet". These pins hold the new arm AND the
       byte-identity of every verdict where isZeroHistory is false (the calibration pins above are the rest
       of that proof and are deliberately untouched). */

    /// <summary>A month of one-minute blocked-session captures for one hour-of-week, none of them non-zero:
    /// the shape PgTargetBaselineProvider's blocking arm returns for a server that never blocks (every logged
    /// capture a measured zero). Full tier, 257 samples over 30 distinct days — well past the Full floors
    /// (10 samples, 3 days).</summary>
    private static BaselineBucket ZeroHistoryBlockingBucket(
        long samples = 257, long days = 30, double mean = 0, double stdDev = 0, double median = 0, double mad = 0,
        BaselineTier tier = BaselineTier.Full) => new()
    {
        Tier = tier, HourOfDay = 14, DayOfWeek = 2,
        Mean = mean, StdDev = stdDev, Median = median, Mad = mad,
        SampleCount = samples, DistinctDays = days, AbsStdDevFloor = 0,
    };

    /// <summary>The zero-history shape above with a BOUNDED metric's absolute dispersion floor on it — the
    /// combination #3859's exemption is about, which <see cref="ZeroHistoryBlockingBucket"/> cannot make
    /// (blocking is server-relative and carries no floor, so its <c>AbsStdDevFloor</c> is fixed at 0).</summary>
    private static BaselineBucket FlooredBucket(
        double floor, double mean = 0, double stdDev = 0, double median = 0, double mad = 0) => new()
    {
        Tier = BaselineTier.Full, HourOfDay = 14, DayOfWeek = 2,
        Mean = mean, StdDev = stdDev, Median = median, Mad = mad,
        SampleCount = 257, DistinctDays = 30, AbsStdDevFloor = floor,
    };

    [Fact]
    public void ZeroHistory_TheMatrix_TrustworthyAndZeroHistoryAreMutuallyExclusive_AndFloorsStillBind()
    {
        /* Enough samples + all zero → zero-history, and NOT trustworthy (the existing contract every caller
           reads is unchanged: there is still no dispersion to divide by). */
        var quiet = ZeroHistoryBlockingBucket();
        Assert.True(quiet.IsZeroHistory);
        Assert.False(quiet.IsTrustworthy);
        Assert.Equal(0, quiet.EffectiveStdDev);
        Assert.Equal(0, quiet.EffectiveRobustSigma);

        /* Enough samples + non-zero → trustworthy, never zero-history. */
        var busy = TrustedCpuBaseline();
        Assert.True(busy.IsTrustworthy);
        Assert.False(busy.IsZeroHistory);

        /* Too few samples + all zero → NEITHER: a young store that has not looked long enough to claim
           "never happens", so it keeps the absolute-fallback path it has always had. */
        var young = ZeroHistoryBlockingBucket(samples: 9);
        Assert.False(young.IsZeroHistory);
        Assert.False(young.IsTrustworthy);

        /* Enough samples but too few DAYS + all zero → also neither: 257 samples from two afternoons is not
           a month of quiet, and the day floor is the gate that says so (the SAME floors IsTrustworthy uses,
           read through the one shared helper so the two cannot drift). */
        var twoDays = ZeroHistoryBlockingBucket(days: 2);
        Assert.False(twoDays.IsZeroHistory);
        Assert.False(twoDays.IsTrustworthy);

        /* A bucket whose CLASSICAL frame collapsed but whose robust frame did not is a rollup artefact, not a
           quiet hour — all four statistics must be zero. */
        Assert.False(ZeroHistoryBlockingBucket(median: 3, mad: 1).IsZeroHistory);
        Assert.False(ZeroHistoryBlockingBucket(mean: 0.2, stdDev: 0.45).IsZeroHistory);

        /* Confidence is the tier/density quality it earns — it IS quality — where an untrustworthy,
           non-zero-history bucket still reads 0. Full tier, 257 samples → saturated density → 1.0. */
        Assert.Equal(1.0, quiet.Confidence, precision: 3);
        Assert.Equal(0.0, young.Confidence);
        Assert.Equal(0.0, twoDays.Confidence);
        Assert.Equal(0.7, ZeroHistoryBlockingBucket(tier: BaselineTier.Flat).Confidence, precision: 3);
    }

    [Fact]
    public void ZeroActivity_OnAFlooredMetric_ReportsZeroDispersion_NotTheFloor()
    {
        /* #3859 item 1, RULING recorded as a pin rather than a fix: the zero-activity arm in
           EffectiveStdDev/EffectiveRobustSigma runs BEFORE AbsStdDevFloor is consulted, so a bounded metric
           that carries a floor and sat at exactly zero reports 0 and not its floor. Both doc comments now say
           so in words; this is what stops the words from being the only thing holding it.

           THE PIN EXISTS SO NOBODY 'FIXES' THE EXEMPTION SILENTLY. The comments read for a long time as if the
           floor were unconditional, and making them true — moving the Math.Max above the zero check — is a
           one-line change that looks like a correction and reds nothing else. It would hand the gate a
           dispersion no sample ever showed, IsZeroHistory would still be true but the extremity arm's inputs
           would no longer describe a dead metric, and #3849's arm would be re-gated on exactly the bounded
           metrics (memory 4.0, CPU 5.0) it was written to reach. The floor spreads a LIVE metric's dispersion;
           it does not hide a dead one. If this test is in your way, that is the decision you are making.

           Memory's real floor, read from the provider rather than retyped, on a bucket that clears the Full
           floors with every statistic at zero — the shape a never-pressured hour actually returns. */
        var floor = BaselineMath.AbsStdDevFloorFor(MetricNames.Memory);
        Assert.True(floor > 0, "the pin is vacuous on a metric with no floor to apply");

        var floored = FlooredBucket(floor);
        Assert.Equal(0, floored.EffectiveStdDev);
        Assert.Equal(0, floored.EffectiveRobustSigma);

        /* And the arm the exemption exists for is still reachable THROUGH the floor — the consequence, not
           just the arithmetic. The bucket reads zero-history and the gate is left to AnomalyGate (#3849's
           arm, which this pin only READS). */
        Assert.True(floored.IsZeroHistory);

        /* The live half of the contract is untouched: the same floor on a bucket with real movement under it
           still binds, so this is an exemption for zero activity and not the floor being abandoned. */
        var live = FlooredBucket(floor, mean: 2.1, stdDev: 0.4, median: 2.0, mad: 0.1);
        Assert.Equal(floor, live.EffectiveStdDev);
        Assert.Equal(floor, live.EffectiveRobustSigma);
        Assert.False(live.IsZeroHistory);
    }

    [Fact]
    public void ZeroHistory_PeakOverTheFloor_FiresAsAnExtremity_BothFrames_SigmaIsTheCap()
    {
        var quiet = ZeroHistoryBlockingBucket();

        /* The robust-first frame (the bucket overloads every PostgreSQL-target family calls): 92 blocked
           sessions over the 3-session floor against a month of zeros → fire, as an extremity. */
        var pair = AnomalyGate.EvaluateZScore(
            quiet, peak: 92, windowMean: 40,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.PgBlockedSessionsFloor, AnomalyThresholds.PgBlockedSessionsFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(pair.Fire);
        Assert.True(pair.ZeroHistory);
        Assert.False(pair.LowQualityBaseline);              // NOT low quality — the whole point
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, pair.Sigma);
        Assert.Equal(AnomalyThresholds.SigmaDisplayCap, pair.MeanSigma!.Value);
        Assert.Equal(0.0, pair.FallbackExceedance);         // no absolute bar was used; the scorer must not grade one

        /* The classical frame, primitives in: the same verdict, so neither entry point can drift. */
        var classical = AnomalyGate.EvaluateZScore(
            quiet.Mean, quiet.EffectiveStdDev, quiet.IsTrustworthy, 92, 40,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.PgBlockedSessionsFloor, AnomalyThresholds.PgBlockedSessionsFallback,
            AnomalyThresholds.SigmaDisplayCap, isZeroHistory: quiet.IsZeroHistory);
        Assert.Equal(pair, classical);

        /* The peak-only overload keeps its contract: null MeanSigma, never a 0 that reads as "at baseline". */
        var peakOnly = AnomalyGate.EvaluateZScore(
            quiet, 92,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.PgBlockedSessionsFloor, AnomalyThresholds.PgBlockedSessionsFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(peakOnly.Fire);
        Assert.True(peakOnly.ZeroHistory);
        Assert.Null(peakOnly.MeanSigma);

        /* THE FACE OF THE LANE: this is what used to happen — the untrustworthy path, 92 under the 10-session
           fallback bar... which it clears, so it fired, but as a low-quality "first occurrence" whose severity
           came off the fallback ramp's 0.5 floor. Pinned as the pre-state by passing isZeroHistory: false. */
        var asItWas = AnomalyGate.EvaluateZScore(
            quiet.Mean, quiet.EffectiveStdDev, quiet.IsTrustworthy, 92, 40,
            AnomalyThresholds.DefaultDeviationThreshold,
            AnomalyThresholds.PgBlockedSessionsFloor, AnomalyThresholds.PgBlockedSessionsFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(asItWas.LowQualityBaseline);
        Assert.False(asItWas.ZeroHistory);
        Assert.Equal(0.0, asItWas.Sigma);                   // no dispersion → no sigma at all
    }

    [Fact]
    public void ZeroHistory_PeakUnderTheMagnitudeFloor_StaysQuiet_TheFloorIsTheOnlyNoiseGuard()
    {
        /* One lock handoff caught mid-flight is a 1 against a month of zeros: infinitely far out in sigmas,
           and exactly the noise the magnitude floor exists to keep out. The floor is the ONLY guard on this
           arm (the pair gate cannot discriminate against a zero centre), so this pin is what stands between
           the arm and a fact per quiet minute. */
        var quiet = ZeroHistoryBlockingBucket();

        var one = AnomalyGate.EvaluateZScore(
            quiet, peak: 1, windowMean: 0.004,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.PgBlockedSessionsFloor, AnomalyThresholds.PgBlockedSessionsFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.False(one.Fire);
        Assert.True(one.ZeroHistory);

        /* AT the floor it fires — inclusive, like every other bar in the gate. */
        var atFloor = AnomalyGate.EvaluateZScore(
            quiet, peak: AnomalyThresholds.PgBlockedSessionsFloor, windowMean: 0.02,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.PgBlockedSessionsFloor, AnomalyThresholds.PgBlockedSessionsFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(atFloor.Fire);

        /* And a mean of zero does not veto: against a zero history the mean carries no information the peak
           has not already given, which is stated in the arm's remarks and pinned here so nobody "fixes" it
           into a second bar this lane has no measurement for. */
        var quietWindow = AnomalyGate.EvaluateZScore(
            quiet, peak: 92, windowMean: 0,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold,
            AnomalyThresholds.PgBlockedSessionsFloor, AnomalyThresholds.PgBlockedSessionsFallback,
            AnomalyThresholds.SigmaDisplayCap);
        Assert.True(quietWindow.Fire);
    }

    [Fact]
    public void ZeroHistory_TheScorerGradesTheExtremityPastTheFaceLine_AndTheThirdAdviceShapePrintsNoSigma()
    {
        /* Severity: the arm invents no scale — it stamps the capped sigma and the shared deviation ramp does
           the rest. 25σ against a 2.0 anchor is saturated (0.5 + 0.5 x min((25-2)/2, 1) = 1.0), times the
           bucket's honest confidence (1.0 for a Full 257-sample bucket) → 1.0. The face line — "sat at 0.5
           with 92 blocked sessions against a clean month" — is answered by this number. */
        var extremity = new Fact
        {
            Source = "anomaly",
            Key = PgTargetFactKeys.AnomalyBlocking,
            Value = 92,
            Metadata = new Dictionary<string, double>
            {
                ["peak_blocked_sessions"] = 92,
                ["avg_blocked_sessions"] = 40,
                ["deviation_sigma"] = AnomalyThresholds.SigmaDisplayCap,
                ["fire_threshold"] = AnomalyThresholds.DefaultDeviationThreshold,
                ["baseline_low_quality"] = 0,
                ["baseline_zero_history"] = 1,
                ["fallback_exceedance"] = 0,
                ["baseline_samples"] = 257,
                ["baseline_distinct_days"] = 30,
                ["confidence"] = 1.0,
                ["threshold_lineage"] = 0,
            },
        };
        Assert.Equal(1.0, Score(extremity), precision: 3);
        Assert.True(Score(extremity) > 0.5, "the face line: 92 blocked sessions against a clean month must not read 0.5");

        /* The third advice shape: the quantity, that this hour saw NONE of it, the samples AND days the claim
           rests on — and no σ figure, because the stored one is a cap rather than a measurement. */
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyBlocking, new Dictionary<string, Fact> { [PgTargetFactKeys.AnomalyBlocking] = extremity })!;
        Assert.Equal("Blocked sessions per capture reached 92 sessions — against a month in which this hour saw none", block.Headline);
        Assert.Contains("is not thin — it is a measured ZERO: 257 baseline samples across 30 distinct days, not one of them above zero", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Beyond any σ", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("σ above", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first occurrence", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("too thin", block.Investigation, StringComparison.Ordinal);
        /* The source sentence survives: the composed prose still names the table and the zero rule. */
        Assert.Contains("pg_blocking_edges", block.Investigation, StringComparison.Ordinal);

        /* The other two shapes are byte-identical to what they were — the stamp is absent or 0 on every fact
           that is not a zero-history fire, and the arm is one `if` above them. */
        var trusted = new Fact
        {
            Source = "anomaly",
            Key = PgTargetFactKeys.AnomalyBlocking,
            Value = 12,
            Metadata = new Dictionary<string, double>
            {
                ["peak_blocked_sessions"] = 12, ["baseline_mean"] = 0.2, ["baseline_median"] = 0,
                ["deviation_sigma"] = 6.2, ["fire_threshold"] = 2.0, ["baseline_low_quality"] = 0,
                ["baseline_zero_history"] = 0, ["baseline_samples"] = 257, ["confidence"] = 1.0,
            },
        };
        var trustedBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyBlocking, new Dictionary<string, Fact> { [PgTargetFactKeys.AnomalyBlocking] = trusted })!;
        Assert.Equal("Blocked sessions per capture spiked to 12 sessions — 6.2σ above its baseline for this time of week", trustedBlock.Headline);

        var unstamped = new Fact { Source = trusted.Source, Key = trusted.Key, Value = trusted.Value, Metadata = new Dictionary<string, double>(trusted.Metadata) };
        unstamped.Metadata.Remove("baseline_zero_history");
        var unstampedBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyBlocking, new Dictionary<string, Fact> { [PgTargetFactKeys.AnomalyBlocking] = unstamped })!;
        Assert.Equal(trustedBlock.Headline, unstampedBlock.Headline);
        Assert.Equal(trustedBlock.Investigation, unstampedBlock.Investigation);
        Assert.Equal(trustedBlock.Remediation, unstampedBlock.Remediation);
    }

    [Fact]
    public void ZeroHistory_TheSqlServerHalf_SameThirdShape_AndTheTrustedSentenceUnmoved()
    {
        /* One engine, one truth: the SQL Server composer gets the same third shape from the same stamp (the
           gate is shared, so a SQL Server target with a month of zero blocked sessions reaches the arm too).
           FactAdvice's trusted sentence is byte-identical either side of the change. */
        static Fact Sessions(bool zeroHistory) => new()
        {
            Source = "anomaly",
            Key = "ANOMALY_SESSION_SPIKE",
            Value = 300,
            Metadata = new Dictionary<string, double>
            {
                ["peak_connections"] = 300, ["baseline_mean"] = zeroHistory ? 0 : 20, ["baseline_samples"] = 257,
                ["baseline_distinct_days"] = 30, ["deviation_sigma"] = zeroHistory ? AnomalyThresholds.SigmaDisplayCap : 8.0,
                ["fire_threshold"] = 2.0, ["baseline_low_quality"] = 0,
                ["baseline_zero_history"] = zeroHistory ? 1 : 0, ["confidence"] = 1.0,
            },
        };

        var extremity = FactAdvice.Compose("ANOMALY_SESSION_SPIKE", new Dictionary<string, Fact> { ["ANOMALY_SESSION_SPIKE"] = Sessions(zeroHistory: true) })!;
        Assert.Contains("against a month in which this hour saw none", extremity.Headline, StringComparison.Ordinal);
        Assert.Contains("257 baseline samples across 30 distinct days, not one of them above zero", extremity.Investigation, StringComparison.Ordinal);
        Assert.Contains("Beyond any σ", extremity.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("σ above its", extremity.Investigation, StringComparison.Ordinal);

        var trusted = FactAdvice.Compose("ANOMALY_SESSION_SPIKE", new Dictionary<string, Fact> { ["ANOMALY_SESSION_SPIKE"] = Sessions(zeroHistory: false) })!;
        Assert.Contains("8σ above its", trusted.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Beyond any", trusted.Investigation, StringComparison.Ordinal);

        var unstamped = Sessions(zeroHistory: false);
        unstamped.Metadata.Remove("baseline_zero_history");
        unstamped.Metadata.Remove("baseline_distinct_days");
        var unstampedBlock = FactAdvice.Compose("ANOMALY_SESSION_SPIKE", new Dictionary<string, Fact> { ["ANOMALY_SESSION_SPIKE"] = unstamped })!;
        Assert.Equal(trusted.Headline, unstampedBlock.Headline);
        Assert.Equal(trusted.Investigation, unstampedBlock.Investigation);
        Assert.Equal(trusted.Remediation, unstampedBlock.Remediation);
    }
}

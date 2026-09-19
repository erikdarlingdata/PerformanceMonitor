/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The shared z-score anomaly gate — the one place the "interaction trap" decision lives so the
/// three triplicated detectors (Lite <c>AnomalyDetector</c>, Darling <c>PgAnomalyDetector</c>,
/// Dashboard <c>SqlServerAnomalyDetector</c>) and the PostgreSQL-target detector
/// (<c>PgTargetAnomalyDetector</c>) cannot drift.
///
/// <para>
/// The #1486 absolute-magnitude floors and the baseline-quality gate (BaselineBucket.IsTrustworthy)
/// are COMPLEMENTARY, not both-mandatory — if they were AND-ed, a young store with a thin baseline
/// would go blind (the z-path is untrustworthy AND a trivial value clears no floor). Instead:
/// </para>
/// <list type="bullet">
///   <item>Trustworthy baseline → trust the z-score, with the magnitude floor as a sanity ceiling
///     (a huge z on a trivial value still doesn't fire).</item>
///   <item>Untrustworthy baseline → do NOT trust z (a z-score against a non-existent baseline is
///     meaningless); fire only on the HIGHER absolute-fallback bar. Not silence — absolute rules
///     preserve new-deployment coverage.</item>
/// </list>
/// The displayed sigma is always computed from whatever dispersion the baseline has (capped), so a
/// fallback-fired anomaly still shows how far above baseline it landed.
///
/// <para>
/// <b>#3653 (A8, first slice): the gate judges the window PEAK AND the window MEAN, both under the
/// existing cutoffs.</b> Every detector's statistic was the window MAX tested against a per-SAMPLE
/// hour×dow distribution, so the null expectation of the test rose with the number of samples in the
/// window: a 24-hour anchored pass mechanically reported more anomalies than the 4-hour scheduled pass
/// on identical behaviour (the maximum of N draws from the same distribution is further out the more
/// draws there are), and a single hot sample was indistinguishable from a sustained shift. The window
/// mean is the sample-count-neutral statistic the store already computes beside the peak — the mean of
/// N draws from the baseline distribution sits AT the baseline mean whatever N is — so requiring the
/// mean to clear the same bar removes the N-bias without new storage or a new number. The ruling for
/// this slice keeps every cutoff, floor and bar exactly as it is; the per-window-peak-per-bucket
/// baseline family (a peak distribution to test a peak against) waits until the anomaly rate is
/// re-read after the dented legacy buckets age out. The honest per-path rule, chosen from the
/// complementarity above — each path tests the mean with the instrument it already trusts:
/// </para>
/// <list type="bullet">
///   <item>Trustworthy → z on BOTH: the peak's deviation AND the mean's deviation clear the cutoff,
///     and the peak clears the magnitude floor as today. The floor stays on the peak ONLY: it is the
///     "trivial value" sanity ceiling for the value the finding reports, sized for a peak, and a
///     window whose mean sits N sigmas above baseline is a sustained shift whether or not that mean
///     also reaches a peak-sized floor (a 5σ sustained 45-connection window against a 20-connection
///     baseline is not trivial because the SESSION floor is 50).</item>
///   <item>Untrustworthy → no z is trusted on either statistic, so the mean gets the one absolute
///     instrument this path has: the peak clears the absolute-fallback bar as today AND the mean
///     clears the MAGNITUDE FLOOR. The floor, not the fallback bar: the bar is deliberately sized
///     higher, for a peak on a young store; asking a window's average to sit at that bar would demand
///     a window pegged the whole way through and give up the new-deployment coverage the fallback
///     exists to preserve. The floor is the lowest bar that means "not trivial", which is exactly the
///     question a mean answers on this path.</item>
/// </list>
/// <para>
/// The reported <c>Sigma</c> stays the PEAK's — that is the deviation the finding shows and the scorer
/// grades (<c>FactScorer.ScoreAnomalyFact</c> is unchanged). The mean's sigma rides beside it as
/// <c>ZDecision.MeanSigma</c> so the story can say "peak 6.1σ, mean 2.4σ". TRANSITIONAL: the peak-only
/// overloads keep their signatures and return exactly today's verdict (no mean clause, <c>MeanSigma</c>
/// null) so a caller that has not yet read its window mean — the PostgreSQL-target detector's families,
/// whose content lanes will pass their means, and the frozen Dashboard twin, which is bug-fix-only —
/// inherits the gate through this root without a source change and closes the gap by switching to the
/// pair overload beside it.
/// </para>
/// </summary>
public static class AnomalyGate
{
    /// <summary>Outcome of a z-vs-absolute-fallback decision.</summary>
    /// <param name="Fire">Whether the anomaly should be emitted.</param>
    /// <param name="Sigma">Capped deviation-in-sigmas of the window PEAK for display (0 when the baseline
    /// has no dispersion). The peak's, always — #3653 added the mean to the DECISION, not to the reported
    /// deviation.</param>
    /// <param name="LowQualityBaseline">True when the decision took the absolute-fallback path (thin baseline).</param>
    /// <param name="FallbackExceedance">Peak ÷ the absolute-fallback bar — set only on the low-quality
    /// path (0 otherwise). On that path the stored <c>Sigma</c> is the real (small) z, which the scorer's
    /// 2σ gate would zero out; the scorer grades the fire off THIS exceedance instead so the finding
    /// still surfaces. See <c>FactScorer.ScoreAnomalyFact</c>.</param>
    /// <param name="ThresholdUsed">#1743: the firing cutoff this decision was judged against —
    /// the (knob-scaled) classical threshold on the classical path, the (knob-scaled) modified-z
    /// cutoff on the robust path. The scorer anchors its severity ramp here so a family that fires
    /// at 5σ doesn't score saturated-flat against a ramp built for 2σ fires.</param>
    /// <param name="MeanSigma">#3653: the window MEAN's capped deviation-in-sigmas in the same frame as
    /// <paramref name="Sigma"/> (classical mean/stddev or robust median/MAD), for the story's second
    /// clause. <c>null</c> when the caller supplied no window mean (the transitional peak-only overloads)
    /// — never 0, which would read as "the mean sat at baseline".</param>
    public readonly record struct ZDecision(bool Fire, double Sigma, bool LowQualityBaseline, double FallbackExceedance, double ThresholdUsed = 0, double? MeanSigma = null);

    /// <summary>
    /// Decides whether a z-score anomaly fires on the window PEAK alone — the pre-#3653 verdict,
    /// kept source-compatible for callers that have not yet read their window mean (see the class
    /// remarks: transitional; prefer the pair overload). Callers pass the baseline's
    /// <paramref name="mean"/>, its <paramref name="effectiveStdDev"/> (already floored), and its
    /// <paramref name="isTrustworthy"/> flag (BaselineBucket carries all three; it is triplicated,
    /// so this helper takes primitives).
    /// </summary>
    /// <param name="magnitudeFloor">#1486 sanity ceiling — in the z-path the observed peak must also
    /// clear this, so a giant z on a trivial value can't surface.</param>
    /// <param name="absoluteFallbackBar">The higher bar the observed peak must clear when the baseline
    /// is untrustworthy. Should be strictly above <paramref name="magnitudeFloor"/>.</param>
    /// <param name="sigmaCap">Display cap for the reported sigma (#1486's 25σ cap).</param>
    public static ZDecision EvaluateZScore(
        double mean,
        double effectiveStdDev,
        bool isTrustworthy,
        double peak,
        double deviationThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap)
        => Decide(mean, effectiveStdDev, isTrustworthy, peak, windowMean: null,
            deviationThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap);

    /// <summary>
    /// #3653: the PAIR gate on the classical frame — fires only when the window <paramref name="peak"/>
    /// AND the window <paramref name="windowMean"/> both clear the existing rules (class remarks: z on
    /// both when trustworthy, with the magnitude floor on the peak; the absolute-fallback bar on the peak
    /// AND the magnitude floor on the mean when untrustworthy). <paramref name="mean"/> is the
    /// BASELINE's mean; <paramref name="windowMean"/> is the analysis window's — the two are different
    /// quantities and the parameter order (baseline frame first, then the two window statistics) keeps
    /// them apart at every call site. <c>Sigma</c> is the peak's; <c>MeanSigma</c> the window mean's.
    /// </summary>
    public static ZDecision EvaluateZScore(
        double mean,
        double effectiveStdDev,
        bool isTrustworthy,
        double peak,
        double windowMean,
        double deviationThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap)
        => Decide(mean, effectiveStdDev, isTrustworthy, peak, windowMean,
            deviationThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap);

    /// <summary>
    /// #1743: the robust-first gate on the window PEAK alone — the pre-#3653 verdict, kept
    /// source-compatible for callers that have not yet read their window mean (transitional; prefer the
    /// pair overload beside it). When the bucket carries robust statistics (a provider that
    /// computes exact per-tier median/MAD), the deviation is the MODIFIED z-score against
    /// median/EffectiveRobustSigma at <paramref name="modifiedZThreshold"/> — fleet-measured to
    /// catch sustained deviations a burst-inflated stddev masks (a busy tenant's real 2-3x evening
    /// surge read 1.4-2.0 classical sigmas and 3.5-4.7 robust ones), while the SAME magnitude
    /// floors, trust gate, and absolute-fallback interplay apply unchanged — the floors are what
    /// keep a MAD-collapsed quiet metric sane, and the trust/fallback complementarity must never
    /// AND into blindness (see class remarks). A bucket WITHOUT robust statistics (the rollup-bound
    /// metrics, a pre-#1743 cached map, the pooled-synthesis fallback tiers) reports
    /// EffectiveRobustSigma 0 and degrades to the classical gate at
    /// <paramref name="classicalDeviationThreshold"/> — never silence, never a misfire against
    /// zeroed robust fields.
    /// </summary>
    public static ZDecision EvaluateZScore(
        BaselineBucket baseline,
        double peak,
        double classicalDeviationThreshold,
        double modifiedZThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap)
        => DecideRobustFirst(baseline, peak, windowMean: null,
            classicalDeviationThreshold, modifiedZThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap);

    /// <summary>
    /// #3653: the PAIR gate on the robust-first frame — the #1743 bucket overload with the window
    /// <paramref name="windowMean"/> judged beside the window <paramref name="peak"/>. On the robust path
    /// both statistics are modified z-scores against median/EffectiveRobustSigma at
    /// <paramref name="modifiedZThreshold"/>; a bucket without robust statistics degrades BOTH to the
    /// classical pair gate at <paramref name="classicalDeviationThreshold"/>. The trust/fallback rule
    /// per statistic is the class remarks'. <c>Sigma</c> is the peak's; <c>MeanSigma</c> the mean's.
    /// </summary>
    public static ZDecision EvaluateZScore(
        BaselineBucket baseline,
        double peak,
        double windowMean,
        double classicalDeviationThreshold,
        double modifiedZThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap)
        => DecideRobustFirst(baseline, peak, windowMean,
            classicalDeviationThreshold, modifiedZThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap);

    private static ZDecision DecideRobustFirst(
        BaselineBucket baseline,
        double peak,
        double? windowMean,
        double classicalDeviationThreshold,
        double modifiedZThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap)
    {
        var robustSigma = baseline.EffectiveRobustSigma;
        if (robustSigma <= 0)
        {
            return Decide(
                baseline.Mean, baseline.EffectiveStdDev, baseline.IsTrustworthy, peak, windowMean,
                classicalDeviationThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap);
        }

        return Decide(
            baseline.Median, robustSigma, baseline.IsTrustworthy, peak, windowMean,
            modifiedZThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap);
    }

    /// <summary>
    /// The ONE decision body both frames share — classical (<paramref name="center"/> = baseline mean,
    /// <paramref name="dispersion"/> = floored stddev) and robust (median, MAD-derived sigma) differ only
    /// in what they hand in. Four public overloads over one body so the peak-only and pair verdicts
    /// cannot drift from each other: a null <paramref name="windowMean"/> is the transitional
    /// peak-only call and every mean clause below is vacuously true for it, which is BYTE-FOR-BYTE the
    /// pre-#3653 verdict (the <c>RobustBaselineTests</c> calibration pins hold unchanged).
    /// </summary>
    private static ZDecision Decide(
        double center,
        double dispersion,
        bool isTrustworthy,
        double peak,
        double? windowMean,
        double threshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap)
    {
        var peakSigma = dispersion > 0
            ? Math.Min((peak - center) / dispersion, sigmaCap)
            : 0.0;
        double? meanSigma = windowMean is null
            ? null
            : dispersion > 0 ? Math.Min((windowMean.Value - center) / dispersion, sigmaCap) : 0.0;

        if (isTrustworthy)
        {
            // dispersion > 0 is guaranteed when trustworthy (IsTrustworthy requires EffectiveStdDev > 0;
            // the robust frame arrives here only when EffectiveRobustSigma > 0).
            var peakDeviation = (peak - center) / dispersion;
            var peakClears = peakDeviation >= threshold && peak >= magnitudeFloor;
            // #3653: the mean's deviation clears the SAME cutoff; the floor stays on the peak (class remarks).
            var meanClears = windowMean is null || (windowMean.Value - center) / dispersion >= threshold;
            return new ZDecision(peakClears && meanClears, Math.Min(peakDeviation, sigmaCap), LowQualityBaseline: false, FallbackExceedance: 0.0, ThresholdUsed: threshold, MeanSigma: meanSigma);
        }

        // Untrustworthy baseline → absolute-threshold fallback (NOT silence). The exceedance (>= 1.0 on a
        // fire) is carried so the scorer can grade it off the absolute bar instead of the untrustworthy z.
        // #3653: the mean clears the magnitude FLOOR (the lower, "not trivial" bar — class remarks); the
        // exceedance stays the peak's, since that is what the scorer grades and the finding reports.
        var peakClearsBar = peak >= absoluteFallbackBar;
        var meanClearsFloor = windowMean is null || windowMean.Value >= magnitudeFloor;
        return new ZDecision(
            peakClearsBar && meanClearsFloor,
            peakSigma,
            LowQualityBaseline: true,
            FallbackExceedance: absoluteFallbackBar > 0 ? peak / absoluteFallbackBar : 0.0,
            ThresholdUsed: threshold,
            MeanSigma: meanSigma);
    }
}

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
///   <item><b>ZERO-HISTORY baseline (#3691 lane 41) → the STRONGEST evidence there is, not the
///     weakest.</b> A bucket that cleared its tier's sample AND distinct-day floors and was never once
///     non-zero (<c>BaselineBucket.IsZeroHistory</c>) is not "no baseline" — for a metric bounded below
///     by zero it is the most confident statement a baseline can make about this hour, and it had been
///     routed to the absolute fallback because an all-zero bucket has no dispersion and so cannot be
///     trustworthy. Any peak clearing the MAGNITUDE FLOOR against a month of zeros is an extremity;
///     see the arm's own remarks in <c>Decide</c> for why the floor is the only noise guard there.</item>
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
    /// <param name="ZeroHistory">#3691 lane 41: the decision took the ZERO-HISTORY extremity arm — the
    /// bucket cleared its tier's floors and held nothing but zeros, so the peak is an extremity against the
    /// strongest baseline statement there is rather than a deviation measured in sigmas. Mutually exclusive
    /// with <paramref name="LowQualityBaseline"/>: on this arm the baseline is NOT low quality and the
    /// detectors stamp it <c>baseline_low_quality = 0</c>, <c>baseline_zero_history = 1</c>. <c>Sigma</c> and
    /// <c>MeanSigma</c> are the display CAP on this arm, not measurements — the advice says "beyond any σ"
    /// and prints no figure.</param>
    public readonly record struct ZDecision(bool Fire, double Sigma, bool LowQualityBaseline, double FallbackExceedance, double ThresholdUsed = 0, double? MeanSigma = null, bool ZeroHistory = false);

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
    /// <param name="isZeroHistory">#3691 lane 41: the bucket's <c>IsZeroHistory</c>. Defaulted false because
    /// this primitive frame has no bucket to ask — the frozen Dashboard twin passes its OWN bucket type's
    /// three primitives here (bug-fix support, its baseline model is a separate copy) and a caller that does
    /// not name it gets exactly today's verdict, byte for byte.</param>
    public static ZDecision EvaluateZScore(
        double mean,
        double effectiveStdDev,
        bool isTrustworthy,
        double peak,
        double deviationThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap,
        bool isZeroHistory = false)
        => Decide(mean, effectiveStdDev, isTrustworthy, peak, windowMean: null,
            deviationThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap, isZeroHistory);

    /// <summary>
    /// #3653: the PAIR gate on the classical frame — fires only when the window <paramref name="peak"/>
    /// AND the window <paramref name="windowMean"/> both clear the existing rules (class remarks: z on
    /// both when trustworthy, with the magnitude floor on the peak; the absolute-fallback bar on the peak
    /// AND the magnitude floor on the mean when untrustworthy). <paramref name="mean"/> is the
    /// BASELINE's mean; <paramref name="windowMean"/> is the analysis window's — the two are different
    /// quantities and the parameter order (baseline frame first, then the two window statistics) keeps
    /// them apart at every call site. <c>Sigma</c> is the peak's; <c>MeanSigma</c> the window mean's.
    /// </summary>
    /// <param name="window">#3653 A8 slice 1: the analysis window's length (<c>context.TimeRangeEnd -
    /// context.TimeRangeStart</c>). <c>null</c> (the default) keeps today's behaviour exactly — the peak clause
    /// judges against <paramref name="deviationThreshold"/> unchanged. When supplied, the PEAK clause alone is
    /// judged against the Šidák-corrected <c>AnomalyThresholds.NAwarePeakCutoff(deviationThreshold, window)</c>
    /// instead — a no-op at the 4-hour reference window or shorter (class remarks). The mean clause and the
    /// magnitude floor are unaffected either way.</param>
    public static ZDecision EvaluateZScore(
        double mean,
        double effectiveStdDev,
        bool isTrustworthy,
        double peak,
        double windowMean,
        double deviationThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap,
        bool isZeroHistory = false,
        TimeSpan? window = null)
        => Decide(mean, effectiveStdDev, isTrustworthy, peak, windowMean,
            deviationThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap, isZeroHistory, window);

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
    /// <para>#3691 lane 41: the two bucket overloads read <c>baseline.IsZeroHistory</c> themselves, so every
    /// caller that hands in a bucket inherits the zero-history extremity arm with no source change — which is
    /// how both SKUs and every PostgreSQL-target family get it at once.</para>
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
    /// <param name="window">#3653 A8 slice 1: see the classical pair overload's remarks — <c>null</c> keeps
    /// today's behaviour; supplied, it Šidák-corrects the peak clause only, on whichever frame
    /// (<paramref name="modifiedZThreshold"/> or <paramref name="classicalDeviationThreshold"/>) the bucket
    /// actually degrades to.</param>
    public static ZDecision EvaluateZScore(
        BaselineBucket baseline,
        double peak,
        double windowMean,
        double classicalDeviationThreshold,
        double modifiedZThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap,
        TimeSpan? window = null)
        => DecideRobustFirst(baseline, peak, windowMean,
            classicalDeviationThreshold, modifiedZThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap, window);

    /// <summary>
    /// #3653 A8 option B (lane L1a): one tile's verdict from <see cref="EvaluateTiles"/>, plus enough of the tile
    /// and the bucket it was scored against for the caller to build a finding (design §1's <c>tile_local_hour</c>,
    /// <c>tile_day_of_week</c>, <c>tile_start_ticks</c>, and the worst tile's peak/mean/baseline).
    /// </summary>
    /// <param name="Decision">The worst tile's own <see cref="ZDecision"/> — <c>Sigma</c> is that tile's peak
    /// deviation, <c>ThresholdUsed</c> is the k_W actually applied to it.</param>
    /// <param name="Tile">The worst tile itself — its <c>LocalHour</c> is <c>tile_local_hour</c>/<c>tile_day_of_week</c>'s
    /// source and its <c>Peak</c>/<c>Mean</c> are what the finding reports in place of the whole-window statistics.</param>
    /// <param name="Bucket">The baseline bucket the worst tile was scored against — its tier and quality feed
    /// <c>AddBaselineContext</c> exactly as the whole-window bucket does today.</param>
    /// <param name="TilesScored">How many tiles cleared <c>minTileSamples</c> and had a non-empty bucket —
    /// <c>tiles_scored</c>.</param>
    /// <param name="TilesFired">How many of the scored tiles had <c>Decision.Fire</c> true — <c>tiles_fired</c>. 0
    /// when the verdict's own <c>Decision.Fire</c> is false.</param>
    public readonly record struct TileVerdict(ZDecision Decision, WindowTile Tile, BaselineBucket Bucket, int TilesScored, int TilesFired);

    /// <summary>
    /// #3653 A8 option B (lane L1a): scores every tile in <paramref name="tiles"/> against its own (hour, dow)
    /// bucket in <paramref name="map"/> and returns the window's verdict — the per-hour-tile gate the design's §1
    /// and §3 describe. Fires when ≥ 1 tile fires (the worst firing tile, largest <c>Sigma</c>, ties to the later
    /// <c>LocalHour</c>); when every scored tile is judged and none fires, still returns a verdict with
    /// <c>Decision.Fire = false</c>, the highest-<c>Sigma</c> scored tile, and <c>TilesFired = 0</c> — the caller
    /// gets a display verdict even on a quiet pass. Returns <c>null</c> only when NO tile could be scored at all
    /// (every tile under <paramref name="minTileSamples"/>, or every selected bucket empty): the design's
    /// never-blind rule — the caller falls back to the whole-window <see cref="EvaluateZScore(BaselineBucket,
    /// double,double,double,double,double,double,TimeSpan?)"/>/<c>EvaluateZScore</c> pair against the start bucket
    /// rather than going silent.
    /// <para>Each tile is decided through <see cref="DecideRobustFirst"/> with <c>correctMean: true</c> (design §3:
    /// "through a private flag on the existing internal decision path; don't copy <c>Decide</c>"), so the tile's
    /// mean clause is judged against the SAME Šidák-raised k_W as its peak clause — the trust, untrustworthy and
    /// zero-history paths inside <c>Decide</c> are otherwise untouched per tile.</para>
    /// </summary>
    /// <param name="tiles">One entry per target-local hour the window's SQL grouped by (design §1); a tile below
    /// <paramref name="minTileSamples"/> is skipped before its bucket is even looked up.</param>
    /// <param name="map">The precomputed (hour, dow) baseline map for this window (lane L1b's provider accessor) —
    /// each tile is looked up independently through <see cref="BaselineBucketMap.For"/>, so each can land on a
    /// different tier with its own trust/zero-history state.</param>
    /// <param name="window">The analysis window's length, fed to <c>AnomalyThresholds.NAwarePeakCutoff</c> exactly
    /// as the whole-window pair overload's own <c>window</c> parameter is — the same k_W applies to every tile's
    /// peak AND mean clause (design §1: the tile cutoff is anchored at the 4-hour reference, not at 1 tile-hour).</param>
    /// <param name="minTileSamples">The design §1 minimum-samples edge rule; defaults to
    /// <see cref="AnomalyThresholds.MinTileSamples"/>.</param>
    public static TileVerdict? EvaluateTiles(
        IReadOnlyList<WindowTile> tiles,
        BaselineBucketMap map,
        double classicalDeviationThreshold,
        double modifiedZThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap,
        TimeSpan window,
        int minTileSamples = AnomalyThresholds.MinTileSamples)
    {
        var scored = new List<(ZDecision Decision, WindowTile Tile, BaselineBucket Bucket)>(tiles.Count);

        foreach (var tile in tiles)
        {
            // Design §1's minimum-samples edge rule: a tile under the floor is a partial first/last hour or a
            // restart gap, not scored at all — skipped before its bucket is even looked up.
            if (tile.Samples < minTileSamples)
                continue;

            var bucket = map.For(tile.LocalHour.Hour, (int)tile.LocalHour.DayOfWeek);
            // Design §1's missing-bucket rule: SelectBucket's own empty sentinel (SampleCount == 0) means this
            // tile's hour has no history at all yet — skipped, not scored against nothing.
            if (bucket.SampleCount == 0)
                continue;

            var decision = DecideRobustFirst(
                bucket, tile.Peak, tile.Mean,
                classicalDeviationThreshold, modifiedZThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap,
                window, correctMean: true);

            scored.Add((decision, tile, bucket));
        }

        // Design §1's never-blind rule: no tile scored at all → null, caller falls back to EvaluateZScore.
        if (scored.Count == 0)
            return null;

        var firing = scored.Where(s => s.Decision.Fire).ToList();
        var tilesFired = firing.Count;
        // ≥ 1 tile fires → the worst tile is the firing one with the largest Sigma, ties to the later LocalHour;
        // nothing fires → the highest-Sigma tile among every tile that was scored (still reported, Fire stays
        // false because that tile's own Decision.Fire is false).
        var candidates = tilesFired > 0 ? firing : scored;
        var worst = candidates
            .OrderByDescending(s => s.Decision.Sigma)
            .ThenByDescending(s => s.Tile.LocalHour)
            .First();

        return new TileVerdict(worst.Decision, worst.Tile, worst.Bucket, scored.Count, tilesFired);
    }

    /// <param name="correctMean">#3653 A8 option B (lane L1a): when true, the TRUSTWORTHY path's mean clause
    /// judges against the SAME Šidák-raised <c>peakThreshold</c> as the peak clause, instead of the uncorrected
    /// <paramref name="classicalDeviationThreshold"/>/<paramref name="modifiedZThreshold"/> the whole-window pair
    /// gate uses (design §1: "tile mode corrects the mean too" — H tile means are H chances under the null in the
    /// fully autocorrelated worst case, exactly the peak's own N-aware argument). Default false keeps every
    /// existing caller (the whole-window pair overloads) byte-identical. The untrustworthy and zero-history paths
    /// are unaffected either way — neither compares the mean against a Šidák-corrected threshold at all (see
    /// <see cref="Decide"/>'s own remarks), which is what "the trust, untrustworthy and zero-history paths stay
    /// exactly as Decide has them" (design §3) means in practice.</param>
    private static ZDecision DecideRobustFirst(
        BaselineBucket baseline,
        double peak,
        double? windowMean,
        double classicalDeviationThreshold,
        double modifiedZThreshold,
        double magnitudeFloor,
        double absoluteFallbackBar,
        double sigmaCap,
        TimeSpan? window = null,
        bool correctMean = false)
    {
        var robustSigma = baseline.EffectiveRobustSigma;
        if (robustSigma <= 0)
        {
            return Decide(
                baseline.Mean, baseline.EffectiveStdDev, baseline.IsTrustworthy, peak, windowMean,
                classicalDeviationThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap, baseline.IsZeroHistory, window, correctMean);
        }

        /* A zero-history bucket cannot reach here: IsZeroHistory requires Mad <= 0, which is EffectiveRobustSigma
           0 (absent an absolute floor, which only bounded metrics carry and which is itself dispersion the bucket
           does have). The flag is threaded anyway so the two calls read the same and neither can silently drop it. */
        return Decide(
            baseline.Median, robustSigma, baseline.IsTrustworthy, peak, windowMean,
            modifiedZThreshold, magnitudeFloor, absoluteFallbackBar, sigmaCap, baseline.IsZeroHistory, window, correctMean);
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
        double sigmaCap,
        bool isZeroHistory = false,
        TimeSpan? window = null,
        bool correctMean = false)
    {
        // #3653 A8 slice 1: the Sidák-corrected peak cutoff, applied to the PEAK clause only (the mean clause
        // below keeps judging against `threshold` unchanged — it is already sample-count-neutral, see the
        // AnomalyThresholds.NAwarePeakCutoff and class remarks). `window` null, or at/under the 4-hour reference
        // window, returns `threshold` itself — byte-identical to the pre-#3653/pre-A8 verdict.
        var peakThreshold = window is { } w ? AnomalyThresholds.NAwarePeakCutoff(threshold, w) : threshold;

        /* #3691 lane 41: the ZERO-HISTORY arm, ONE `if` at the very top, so every verdict for every other
           baseline shape is structurally untouched — nothing runs before this test, nothing below it changed.

           A bucket that cleared its tier's sample AND distinct-day floors and held nothing but zeros (the
           BaselineBucket.IsZeroHistory contract) was reaching the untrustworthy path below, because an
           all-zero bucket has no dispersion and so can never be trustworthy. That routed a month of MEASURED
           quiet to the absolute-fallback bar — a bar deliberately sized for a peak on a young store, where the
           honest answer really is "we do not know your normal yet". Here we do know it, exactly: for a metric
           bounded below by zero, thirty days of zeros across enough distinct days is the strongest statement a
           baseline can make about this hour. Measured consequence, the face of this lane: ANOMALY_PG_BLOCKING
           with 92 blocked sessions against a clean month read 0.5, "first occurrence, no baseline yet".

           So the peak is judged as an EXTREMITY, not as a deviation: any peak clearing the magnitude floor is
           categorically outside a history with no non-zero sample in it. peakSigma is pinned at sigmaCap — a
           CAP, not a measurement (the true quantity is unbounded: (peak - 0) / 0), which is why the advice for
           this arm prints no σ figure and says "beyond any σ" instead.

           THE MAGNITUDE FLOOR IS THE ONLY NOISE GUARD HERE, DELIBERATELY. The #3653 pair gate cannot
           discriminate on this arm: against a zero centre with zero dispersion, ANY non-zero window mean is
           infinitely far out, so a mean clause would either be vacuous (every non-zero mean passes) or an
           arbitrary new bar this lane has no measurement for. MeanSigma therefore carries the same cap when
           the caller supplied a mean and stays null when it did not — never 0, which would read as "the mean
           sat at baseline". The floor is what keeps the single lock handoff caught mid-flight (1 blocked
           session against PgBlockedSessionsFloor = 3) out, and it is the bar these families' floors were
           chosen as: the lowest value that is not trivial. A peak UNDER the floor does not fire, exactly as
           today.

           LowQualityBaseline stays FALSE here and ZeroHistory says which arm ran, so the detectors stamp
           baseline_low_quality = 0 / baseline_zero_history = 1 and the scorer grades the capped sigma on its
           normal deviation ramp (FactScorer.ScoreAnomalyFact) rather than the young-store fallback ramp that
           floors at 0.5. FallbackExceedance is 0: no absolute bar was used, and the scorer must not grade one. */
        if (isZeroHistory)
        {
            var zeroHistorySigma = sigmaCap;
            return new ZDecision(
                peak >= magnitudeFloor,
                zeroHistorySigma,
                LowQualityBaseline: false,
                FallbackExceedance: 0.0,
                ThresholdUsed: threshold,
                MeanSigma: windowMean is null ? null : zeroHistorySigma,
                ZeroHistory: true);
        }

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
            // #3653 A8 slice 1: the peak clears the (possibly N-aware-raised) peakThreshold; the floor is
            // unchanged. ThresholdUsed carries peakThreshold, not threshold, so fire_threshold reports what was
            // actually applied to the peak (class remarks; the scorer anchors its severity ramp here).
            var peakClears = peakDeviation >= peakThreshold && peak >= magnitudeFloor;
            // #3653: the mean's deviation clears the SAME (un-corrected) cutoff; the floor stays on the peak.
            // #3653 A8 option B (lane L1a): `correctMean` raises that bar to the SAME peakThreshold the peak
            // clause was just judged against — tile mode's own correction (design §1); every whole-window caller
            // leaves it false and keeps `threshold` exactly as before.
            var meanThreshold = correctMean ? peakThreshold : threshold;
            var meanClears = windowMean is null || (windowMean.Value - center) / dispersion >= meanThreshold;
            return new ZDecision(peakClears && meanClears, Math.Min(peakDeviation, sigmaCap), LowQualityBaseline: false, FallbackExceedance: 0.0, ThresholdUsed: peakThreshold, MeanSigma: meanSigma);
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

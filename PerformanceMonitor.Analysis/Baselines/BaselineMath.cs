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

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>
/// The store-agnostic baseline math shared by the two active providers (Lite <c>BaselineProvider</c>,
/// Darling <c>PgBaselineProvider</c>): the rolling-window / tier-selection thresholds, the tier
/// selection with hysteresis (<see cref="SelectBucket"/>), the pooled-statistics collapse
/// (hour-only / flat), and the bounded-metric dispersion floor. Each provider keeps only the
/// genuinely dialect-specific part — the per-metric baseline SQL and its parameter binding. Extracted
/// verbatim so the two apps' baseline behavior cannot drift.
/// </summary>
public static class BaselineMath
{
    /// <summary>Rolling window for baseline computation.</summary>
    public const int BaselineWindowDays = 30;

    /// <summary>Collapse to hour-only when full bucket has fewer than this many samples.</summary>
    public const int CollapseThreshold = 10;

    /// <summary>Restore to full bucket when sample count reaches this level (hysteresis).</summary>
    public const int RestoreThreshold = 15;

    /// <summary>
    /// Selects the most specific baseline bucket available for the (hour, day-of-week) coordinate,
    /// collapsing to less-specific tiers as sample density falls: Full (hour+dow) -> Hour-only ->
    /// Flat. Hysteresis keeps a Full bucket in the 10-14 sample band (RestoreThreshold vs
    /// CollapseThreshold) instead of flapping to hour-only. Pure over the precomputed bucket map,
    /// so it lives here rather than in either store provider.
    /// <para>#1743: providers that compute exact per-tier robust statistics (GROUPING SETS) store
    /// the hour-only tier under the sentinel key (hour, -1) and the flat tier under (-1, -1) in the
    /// SAME map. When a sentinel entry exists it is AUTHORITATIVE over the pooled synthesis below —
    /// medians cannot be pooled from per-bucket medians, so a synthesized tier would carry
    /// zeroed robust fields and silently drop the robust path. The pooled fallback remains for
    /// maps without sentinels (a provider not yet migrated, or a cached pre-#1743 map): its
    /// buckets read Median=0/Mad=0, which EffectiveRobustSigma reports as zero-activity, and the
    /// detector's robust path degrades to the classical one rather than misfiring.</para>
    /// </summary>
    public static BaselineBucket SelectBucket(
        IReadOnlyDictionary<(int HourOfDay, int DayOfWeek), BaselineBucket> baselines,
        int hourOfDay,
        int dayOfWeek)
    {
        // Try full bucket (hour + day-of-week)
        var fullKey = (hourOfDay, dayOfWeek);
        if (baselines.TryGetValue(fullKey, out var fullBucket) && fullBucket.SampleCount >= RestoreThreshold)
            return fullBucket;

        // If full bucket exists but below restore threshold, check if it's above collapse threshold
        // (hysteresis: don't collapse if we're between 10-14 samples and were previously using full)
        if (fullBucket != null && fullBucket.SampleCount >= CollapseThreshold)
            return fullBucket;

        // Hour-only tier: the provider's exact (GROUPING SETS) bucket when present, else pooled.
        if (baselines.TryGetValue((hourOfDay, -1), out var exactHourOnly))
        {
            if (exactHourOnly.SampleCount >= CollapseThreshold)
                return exactHourOnly;
        }
        else
        {
            /* Pooled synthesis over the REAL full buckets only — sentinel entries must not
               contribute or the tier would double-count itself. */
            var hourBuckets = baselines
                .Where(kvp => kvp.Key.HourOfDay == hourOfDay && kvp.Key.DayOfWeek >= 0)
                .Select(kvp => kvp.Value)
                .ToList();

            if (hourBuckets.Count > 0)
            {
                var collapsed = CollapseToHourOnly(hourBuckets);
                if (collapsed.SampleCount >= CollapseThreshold)
                    return collapsed;
            }
        }

        // Flat tier: same rule.
        if (baselines.TryGetValue((-1, -1), out var exactFlat))
        {
            if (exactFlat.SampleCount >= 3) // Minimum viable baseline
                return exactFlat;
        }
        else
        {
            var allBuckets = baselines
                .Where(kvp => kvp.Key.HourOfDay >= 0 && kvp.Key.DayOfWeek >= 0)
                .Select(kvp => kvp.Value)
                .ToList();
            if (allBuckets.Count > 0)
            {
                var flat = CollapseToFlat(allBuckets);
                if (flat.SampleCount >= 3) // Minimum viable baseline
                    return flat;
            }
        }

        return BaselineBucket.Empty;
    }

    /// <summary>
    /// #1743: the modified z-score — 0.6745 * (value − median) / (MAD/0.6745-derived sigma), which
    /// algebraically is (value − Median) / EffectiveRobustSigma with the consistency constant
    /// already folded into EffectiveRobustSigma's scaling. Returns 0 when the bucket reports
    /// zero-activity robust dispersion, so callers can gate on <c>&gt; 0</c> exactly as they gate
    /// the classical z on EffectiveStdDev.
    /// </summary>
    public static double ModifiedZScore(BaselineBucket bucket, double value)
    {
        var sigma = bucket.EffectiveRobustSigma;
        if (sigma <= 0) return 0;
        return (value - bucket.Median) / sigma;
    }

    /// <summary>
    /// Bounded-metric absolute dispersion floor (see BaselineBucket.AbsStdDevFloor). Server-relative
    /// metrics have no universal floor and return 0. Tunable — calibrate on the SQL2025/HammerDB box.
    ///
    /// <para>#3691: <see cref="MetricNames.PgCpu"/> takes the SQL Server CPU floor by UNIT PARITY, not by
    /// reuse-by-value. The metric is <c>pg_cpu_utilization.acu_utilization_percent</c> — percent of the
    /// configured capacity ceiling on Aurora Serverless (never a raw <c>cpu_percent</c>; the target provider's
    /// arm says so) — and the floor's unit is percentage points of a 0–100 bounded scale, the same unit
    /// <see cref="MetricNames.Cpu"/> carries. The 5-point floor exists because a bounded percentage that sat
    /// still for a month has a variance the arithmetic collapses to nothing, and one ordinary point of
    /// movement then reads as a 25σ event (the <c>SigmaDisplayCap</c> the PostgreSQL buckets leaned on before
    /// this arm); the floor says "five points is the least a percentage can move and mean something", and
    /// that statement is about the scale, not the engine. What the floor is NOT: a fleet measurement of
    /// PostgreSQL CPU dispersion — none exists yet; it is the same engine-defined argument that set 5.0 for
    /// SQL Server, applied to the same unit. <see cref="MetricNames.PgWaitMsPerSec"/> stays at 0 with the
    /// SQL Server wait-profile metric: ms of wait per second of wall clock is server-relative (a store class
    /// idling at 20 ms/s and one saturating at 2,000 ms/s are both healthy), so no universal floor is
    /// honest; the detector's magnitude floors and the quality gate carry that arm, as they do for SQL Server.
    /// The other PostgreSQL names (tps, session count, deadlock rate) are server-relative by the same
    /// argument and fall through.</para>
    /// </summary>
    public static double AbsStdDevFloorFor(string metricName) => metricName switch
    {
        MetricNames.Cpu => 5.0,        // CPU utilization %
        MetricNames.PgCpu => 5.0,      // percent of the capacity ceiling — same unit as Cpu, same floor (#3691, see the summary)
        MetricNames.Memory => 4.0,     // memory pressure %
        MetricNames.IoLatency => 2.5,  // I/O latency ms
        _ => 0.0,                       // batch/query-duration/sessions/waits/blocking/deadlock — server-relative (PG tps/sessions/deadlock/wait too)
    };

    /// <summary>
    /// Collapses multiple day-of-week buckets for the same hour into a single
    /// hour-only bucket using pooled statistics.
    /// </summary>
    private static BaselineBucket CollapseToHourOnly(List<BaselineBucket> hourBuckets)
    {
        var totalSamples = hourBuckets.Sum(b => b.SampleCount);
        if (totalSamples == 0)
            return BaselineBucket.Empty;

        // Weighted mean across all day-of-week buckets for this hour
        var weightedMean = hourBuckets.Sum(b => b.Mean * b.SampleCount) / totalSamples;

        // Pooled standard deviation
        var pooledVariance = PoolVariance(hourBuckets, weightedMean);

        return new BaselineBucket
        {
            HourOfDay = hourBuckets[0].HourOfDay,
            DayOfWeek = -1, // Indicates hour-only
            Mean = weightedMean,
            StdDev = Math.Sqrt(pooledVariance),
            SampleCount = totalSamples,
            // Each calendar day lands in exactly one day-of-week bucket, so summing distinct-days
            // across the dow buckets for this hour is exact (no double-count).
            DistinctDays = hourBuckets.Sum(b => b.DistinctDays),
            AbsStdDevFloor = hourBuckets[0].AbsStdDevFloor,
            Tier = BaselineTier.HourOnly
        };
    }

    /// <summary>
    /// Collapses all buckets into a single flat baseline (equivalent to old 24h behavior).
    /// </summary>
    private static BaselineBucket CollapseToFlat(List<BaselineBucket> allBuckets)
    {
        var totalSamples = allBuckets.Sum(b => b.SampleCount);
        if (totalSamples == 0)
            return BaselineBucket.Empty;

        var weightedMean = allBuckets.Sum(b => b.Mean * b.SampleCount) / totalSamples;
        var pooledVariance = PoolVariance(allBuckets, weightedMean);

        return new BaselineBucket
        {
            HourOfDay = -1,
            DayOfWeek = -1,
            Mean = weightedMean,
            StdDev = Math.Sqrt(pooledVariance),
            SampleCount = totalSamples,
            // A calendar day recurs across the 24 hour buckets, so summing would double-count;
            // MAX is a ~5 ceiling (each (hour, dow) bucket holds at most ~5 same-weekday dates in a
            // 30-day window) — a cheap proxy that avoids an extra global DISTINCT-days query.
            //
            // #3859: THIS admission is the one that matters downstream, and a comment cannot travel.
            // The number is stamped as baseline_distinct_days on every z-family fact and surfaced
            // through get_analysis_facts, where an operator reading a Flat-tier fact sees a ceiling
            // and has no way to tell it from a count. So the three AddBaselineContext bodies stamp
            // baseline_distinct_days_is_proxy = 1 beside it on the Flat tier — keyed on this Tier,
            // which is why the stamp is conditional there rather than unconditional. If the proxy is
            // ever replaced by a real global DISTINCT-days read, that stamp goes with it; the three
            // are held in agreement by the delegation-equality census in SharedBaselineModelPinTests.
            DistinctDays = allBuckets.Max(b => b.DistinctDays),
            AbsStdDevFloor = allBuckets[0].AbsStdDevFloor,
            Tier = BaselineTier.Flat
        };
    }

    /// <summary>
    /// Computes pooled variance from multiple buckets, accounting for both
    /// within-bucket variance and between-bucket mean differences.
    /// </summary>
    private static double PoolVariance(List<BaselineBucket> buckets, double grandMean)
    {
        var totalSamples = buckets.Sum(b => b.SampleCount);
        if (totalSamples <= 1) return 0;

        double totalSumSq = 0;
        foreach (var b in buckets)
        {
            if (b.SampleCount <= 0) continue;
            // Within-bucket variance contribution
            totalSumSq += (b.StdDev * b.StdDev) * (b.SampleCount - 1);
            // Between-bucket mean difference contribution
            totalSumSq += b.SampleCount * (b.Mean - grandMean) * (b.Mean - grandMean);
        }

        return totalSumSq / (totalSamples - 1);
    }
}

/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis.Baselines;

/// <summary>
/// #4731: the metadata of the two SQL Server COUNT families, <c>ANOMALY_BLOCKING_SPIKE</c> and
/// <c>ANOMALY_DEADLOCK_SPIKE</c>, assembled once for both products. Lite's <c>AnomalyDetector</c> and Darling's
/// <c>PgAnomalyDetector</c> each used to spell the same four keys twice (four copies in all), and the copies
/// had already drifted from the gate families on one point: the count families never asked whether their
/// bucket was a measured zero.
/// <para>
/// <b>Why the count families need their own stamp.</b> They skip <see cref="AnomalyGate"/> — the rule is a
/// count floor plus a rate multiple, not a z-score — so the gate's zero-history handling never reached them. A
/// bucket that <see cref="BaselineBucket.IsZeroHistory"/> (tier floors cleared, all four statistics zero) is
/// never <see cref="BaselineBucket.IsTrustworthy"/>, so such a fact fires on the count alone with
/// <c>is_new = 1</c> and reads "first occurrence, no baseline yet" — the words for a baseline the engine has
/// not built, said about a baseline it has measured as empty. <c>baseline_zero_history</c> is the same stamp
/// the gate families carry, and <c>FactAdvice.ComposeAnomalyRatio</c> reads it BEFORE <c>is_new</c>.
/// </para>
/// <para>
/// <b>What stays exactly as it was.</b> The firing rule stays in the detectors (it decides whether a fact
/// exists at all), and <c>ratio</c> and <c>is_new</c> are computed here the way they always were: an
/// untrustworthy bucket is <c>is_new = 1</c> at the <see cref="AnomalyThresholds.NoBaselineRatio"/> sentinel,
/// a trustworthy one carries the real per-hour multiple. The scorer reads only <c>ratio</c> for these keys, so
/// a zero-history fact scores exactly as the same fact did before the stamp existed.
/// </para>
/// <para>
/// <b>Reach.</b> The stamp is a property of the bucket, not of the supply. Both products' event baselines
/// (<c>blocked_process_baseline</c> / <c>deadlock_baseline</c> in PostgreSQL, <c>v_blocked_process_reports</c> /
/// <c>v_deadlocks</c> in DuckDB) group the event rows that exist, so a bucket they hand back has a positive
/// mean and the stamp is 0 until a supply records a measured zero. The wording is ready for that supply; no
/// detector edit is needed when it arrives.
/// </para>
/// </summary>
public static class CountFamilyMetadata
{
    /// <summary>
    /// The count-family keys for one fired fact: <c>current_count</c>, <c>baseline_rate</c>, <c>ratio</c>,
    /// <c>is_new</c>, <c>baseline_zero_history</c> and <c>baseline_samples</c>. The caller adds the shared
    /// baseline context (hour, day of week, tier, robust frame, confidence, distinct days) with its own
    /// <c>AddBaselineContext</c>, exactly as before.
    /// </summary>
    /// <param name="currentCount">The raw event count over the whole analysis window.</param>
    /// <param name="currentPerHour"><paramref name="currentCount"/> divided by the window's length in hours.</param>
    /// <param name="baselineRate">The bucket's mean events per hour, or 0 when the bucket holds no samples.</param>
    /// <param name="baseline">The bucket the fact was judged against.</param>
    public static Dictionary<string, double> Build(long currentCount, double currentPerHour, double baselineRate, BaselineBucket baseline)
    {
        var isNew = !baseline.IsTrustworthy;
        return new Dictionary<string, double>
        {
            ["current_count"] = currentCount,
            ["baseline_rate"] = baselineRate,
            ["ratio"] = isNew ? AnomalyThresholds.NoBaselineRatio : currentPerHour / baselineRate,
            ["is_new"] = isNew ? 1 : 0,
            /* #4731: the gate families' stamp, beside the sample count the zero-history wording rests on. The
               distinct-day count rides in with the caller's AddBaselineContext, as it does for every family. */
            ["baseline_zero_history"] = baseline.IsZeroHistory ? 1 : 0,
            ["baseline_samples"] = baseline.SampleCount
        };
    }
}

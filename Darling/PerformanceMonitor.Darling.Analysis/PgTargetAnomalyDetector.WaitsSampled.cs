/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetAnomalyDetector
{
    /// <summary>
    /// The window's stock SAMPLED all-types wait rate per collection — <c>Δsample_count × profile_period_ms</c>
    /// summed over the collection's series (the reader's reset rule: a count that went backwards is taken whole;
    /// a series' first in-window sighting contributes nothing), CPU/Running excluded, over the milliseconds that
    /// collection's sampler was WATCHING (<c>sampled_ms</c>, V133; NULL = the whole <c>LAG</c> interval) — then
    /// PEAK, MEAN, total, the count of rated collections, the observed and wall seconds, and how many rated
    /// collections had to be read as their whole interval. The same rows, differencing and denominator as the
    /// <c>pg_sampled_wait_ms_per_sec</c> baseline arm (<c>PgTargetBaselineProvider.WaitsSampled.cs</c>) and the
    /// wait partial's stock read, so peak, mean and bucket are in one unit. <c>$1</c> server_id, <c>$2</c>/<c>$3</c>
    /// window (naive UTC, the fact reads' closed shape).
    ///
    /// <para><b><c>exact_collections</c> is the other source's count over the same window</b> — <c>pg_wait_stats</c>'s
    /// distinct collections. When it is above zero the engine's measured deltas exist for this window and the
    /// sampled profile is a second, coarser reading of the same seconds: the detector sits out and the exact
    /// profile (<c>ANOMALY_PG_WAIT_PROFILE</c>) is the one that fires, exactly as the wait partial withholds the
    /// sampled FACTS and stamps <c>sampled_suppressed_by_exact</c> on the exact ones. One index probe.</para>
    /// </summary>
    public const string SampledWaitRateWindowSql = @"
WITH series AS (
    SELECT collection_time, event_type, sample_count, profile_period_ms, sampled_ms,
           LAG(sample_count) OVER (PARTITION BY event_type, event, query_id ORDER BY collection_time) AS prev_count
    FROM pg_wait_sampling
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
),
per_collection AS (
    SELECT collection_time,
           MAX(sampled_ms) AS sampled_ms,
           CAST(coalesce(SUM(
               CASE WHEN prev_count IS NULL THEN NULL
                    WHEN sample_count < prev_count THEN sample_count
                    ELSE sample_count - prev_count
               END * profile_period_ms) FILTER (WHERE lower(event_type) IS DISTINCT FROM 'cpu'), 0) AS DOUBLE PRECISION) AS total_wait_ms,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
    FROM series
    GROUP BY collection_time
),
rated AS (
    SELECT collection_time,
           total_wait_ms,
           CAST(coalesce(sampled_ms / 1000.0, interval_sec) AS DOUBLE PRECISION) AS observed_sec,
           interval_sec,
           (sampled_ms IS NULL) AS sampled_ms_unknown
    FROM per_collection
    WHERE interval_sec > 0
    AND   coalesce(sampled_ms / 1000.0, interval_sec) > 0
)
SELECT MAX(total_wait_ms / observed_sec)                       AS peak_ms_per_sec,
       AVG(total_wait_ms / observed_sec)                       AS mean_ms_per_sec,
       SUM(total_wait_ms)                                      AS total_wait_ms,
       COUNT(*)                                                AS sample_count,
       (SELECT COUNT(*) FROM per_collection)                   AS collection_count,
       coalesce(SUM(observed_sec), 0)                          AS observed_sec,
       coalesce(SUM(interval_sec), 0)                          AS interval_sec,
       COUNT(*) FILTER (WHERE sampled_ms_unknown)              AS unknown_sampled_collections,
       (SELECT COUNT(DISTINCT w.collection_time)
        FROM pg_wait_stats AS w
        WHERE w.server_id = $1 AND w.collection_time >= $2 AND w.collection_time <= $3) AS exact_collections
FROM rated";

    /// <summary>The window's six largest (event_type, event) sampled contributors by estimated time — the same
    /// per-series reset-aware difference, CPU/Running excluded — named in the anomaly's <c>contrib_Type:event</c>
    /// metadata keys (the value is estimated milliseconds) so the advice leads with them and the reconciler folds
    /// the story onto the dominant wait's own card (<c>PgTargetFactKeys.WaitProfileFamilies</c>).</summary>
    public const string SampledWaitContribWindowSql = @"
WITH series AS (
    SELECT event_type, event, sample_count, profile_period_ms,
           LAG(sample_count) OVER (PARTITION BY event_type, event, query_id ORDER BY collection_time) AS prev_count
    FROM pg_wait_sampling
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   event_type IS NOT NULL
    AND   lower(event_type) IS DISTINCT FROM 'cpu'
)
SELECT event_type, event,
       CAST(SUM(CASE WHEN prev_count IS NULL THEN 0
                     WHEN sample_count < prev_count THEN sample_count
                     ELSE sample_count - prev_count
                END * profile_period_ms) AS bigint) AS total_ms
FROM series
GROUP BY event_type, event
HAVING SUM(CASE WHEN prev_count IS NULL THEN 0
                WHEN sample_count < prev_count THEN sample_count
                ELSE sample_count - prev_count
           END) > 0
ORDER BY total_ms DESC
LIMIT 6";

    /// <summary>
    /// <see cref="PgTargetFactKeys.AnomalySampledWaitProfile"/> (lane 24 of #3691; stock PostgreSQL only): the
    /// window's stock SAMPLED all-types wait rate — per second the sampler was WATCHING, CPU/Running excluded —
    /// against the <c>pg_sampled_wait_ms_per_sec</c> bucket. Lane 9's Aurora wait-profile detector
    /// (<c>DetectWaitProfileAnomalies</c>) with the instrument swapped, and with the #3653 peak-AND-mean gate
    /// (#3724) the ratio family did not yet have: every clause below is asked of the window PEAK and of the window
    /// MEAN alike, so one hot five-minute collection is not a profile shift and a 24-hour anchored pass does not
    /// mechanically fire more often than the 4-hour one. Robust bucket: modified z at the heavy-tail cutoff
    /// (<see cref="AnomalyThresholds.HeavyTailModifiedZThreshold"/>, the SQL Server fleet's calibrated robust
    /// statistic used by reference) on both statistics AND the magnitude bar on the peak; classical bucket: the ratio
    /// at <see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/> on both AND the bar on the peak; untrustworthy:
    /// the bar alone on the peak AND the mean, <c>is_new</c>, no sentinel ratio. The bar is
    /// <see cref="AnomalyThresholds.PgSampledWaitProfileFallbackMsPerSec"/> — lane 9 left it deliberately undeclared
    /// while nothing read it; it is the Aurora figure restated on its own (unmeasured) lineage and NOT an alias, so
    /// the two instruments calibrate apart. Every fact carries <c>threshold_lineage = 0</c>.
    ///
    /// <para><b>Why its own key, metric and detector.</b> The sampled series is a per-backend-sample count quantised at
    /// <c>profile_period_ms</c> — it cannot see a wait shorter than the period, rounds a continuous wait to ±1
    /// period per backend-sample, and on the service-sampler arm watches a tenth of the clock — so its noise
    /// distribution is not the measured microsecond sum's; a bucket that pooled the two, or a bar tuned on one
    /// applied to the other, is the unit error #3689 §5 refused. The two profiles share a SHAPE (ratio / modified-z
    /// metadata, <c>contrib_Type:event</c>, the same extremity escape and co-fires) through
    /// <c>PgTargetFactKeys.IsWaitProfileAnomaly</c>, and nothing else.</para>
    ///
    /// <para><b>The honest denominator, and what a straddling window reads.</b> <c>sampled_ms</c> arrived in V133;
    /// every earlier row is NULL and is read as its whole interval (lane 5's arithmetic, a ~10× understatement on
    /// the sampler arm), never a guessed 30 s. A window or a bucket that straddles the install therefore mixes the
    /// two readings; the fact states <c>sampled_ms_known</c> (0 when any rated collection was read the old way) and
    /// the advice says the estimate is from sampling. The bucket is built the same way, so on a fleet that has
    /// just installed V133 the first passes compare an honest window against a mostly-blind baseline and can fire
    /// on the arithmetic change alone — a first-month reading to expect, stated here, not a defect to hide.</para>
    ///
    /// <para><b>Sits out</b> when <c>pg_wait_sampling</c> has no rows in the window (an Aurora target), when
    /// <c>pg_wait_stats</c> ALSO has rows (both sources — the exact profile is the one that fires; the wait partial
    /// withholds the sampled facts the same way), when no collection was rated, or when the bucket is empty.</para>
    /// </summary>
    private async partial Task DetectSampledWaitProfileAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgSampledWaitMsPerSec, context.TimeRangeStart, context.CancellationToken);

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            double peakRate, meanRate, totalWaitMs, observedSec, intervalSec;
            long sampleCount, collectionCount, unknownSampled, exactCollections;
            using (var rateCmd = WindowCommand(SampledWaitRateWindowSql, connection, context))
            {
                using var rateReader = await rateCmd.ExecuteReaderAsync(context.CancellationToken);
                if (!await rateReader.ReadAsync(context.CancellationToken)) return;
                peakRate = rateReader.IsDBNull(0) ? 0.0 : Convert.ToDouble(rateReader.GetValue(0));
                meanRate = rateReader.IsDBNull(1) ? 0.0 : Convert.ToDouble(rateReader.GetValue(1));
                totalWaitMs = rateReader.IsDBNull(2) ? 0.0 : Convert.ToDouble(rateReader.GetValue(2));
                sampleCount = rateReader.IsDBNull(3) ? 0L : Convert.ToInt64(rateReader.GetValue(3));
                collectionCount = rateReader.IsDBNull(4) ? 0L : Convert.ToInt64(rateReader.GetValue(4));
                observedSec = rateReader.IsDBNull(5) ? 0.0 : Convert.ToDouble(rateReader.GetValue(5));
                intervalSec = rateReader.IsDBNull(6) ? 0.0 : Convert.ToDouble(rateReader.GetValue(6));
                unknownSampled = rateReader.IsDBNull(7) ? 0L : Convert.ToInt64(rateReader.GetValue(7));
                exactCollections = rateReader.IsDBNull(8) ? 0L : Convert.ToInt64(rateReader.GetValue(8));
            }

            /* No rows: this flavour does not write pg_wait_sampling (Aurora) — sit out. Both sources: the exact
               profile is the one that fires (summary). No rated collection: nothing to judge. */
            if (collectionCount == 0 || exactCollections > 0 || sampleCount == 0) return;
            if (baseline.SampleCount == 0) return;

            bool isNew;
            double ratio, meanRatio, fallbackExceedance;
            var modifiedZ = BaselineMath.ModifiedZScore(baseline, peakRate);
            var meanModifiedZ = BaselineMath.ModifiedZScore(baseline, meanRate);
            if (baseline.IsTrustworthy && baseline.EffectiveRobustSigma > 0)
            {
                isNew = false;
                ratio = baseline.Mean > 0 ? peakRate / baseline.Mean : 0;
                meanRatio = baseline.Mean > 0 ? meanRate / baseline.Mean : 0;
                fallbackExceedance = 0;
                /* The heavy-tail cutoff by reference on BOTH statistics; the (unmeasured) magnitude bar on the peak. */
                if (modifiedZ < HeavyTailModifiedZThreshold || meanModifiedZ < HeavyTailModifiedZThreshold || peakRate < PgSampledWaitProfileFallbackMsPerSec) return;
            }
            else if (baseline.IsTrustworthy && baseline.Mean > 0)
            {
                isNew = false;
                ratio = peakRate / baseline.Mean;
                meanRatio = meanRate / baseline.Mean;
                fallbackExceedance = 0;
                /* unmeasured: PgRatioAnomalyThreshold on both statistics, PgSampledWaitProfileFallbackMsPerSec on the peak. */
                if (ratio < PgRatioAnomalyThreshold || meanRatio < PgRatioAnomalyThreshold || peakRate < PgSampledWaitProfileFallbackMsPerSec) return;
            }
            else
            {
                isNew = true;
                ratio = 0;
                meanRatio = 0;
                fallbackExceedance = peakRate / PgSampledWaitProfileFallbackMsPerSec;
                /* unmeasured: the bar alone, on the peak AND the mean (#3724's untrustworthy-path rule: the one
                   absolute instrument this path has, asked of both statistics). */
                if (fallbackExceedance < 1.0 || meanRate < PgSampledWaitProfileFallbackMsPerSec) return;
            }

            var metadata = new Dictionary<string, double>
            {
                ["current_ms_per_sec"] = peakRate,
                ["mean_ms_per_sec"] = meanRate,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_samples"] = baseline.SampleCount,
                ["total_wait_ms"] = totalWaitMs,
                ["window_samples"] = sampleCount,
                ["ratio"] = ratio,
                ["mean_ratio"] = meanRatio,
                ["modified_z"] = modifiedZ,
                ["mean_modified_z"] = meanModifiedZ,
                ["is_new"] = isNew ? 1 : 0,
                ["fallback_exceedance"] = fallbackExceedance,
                ["fire_threshold"] = isNew ? 0 : (baseline.EffectiveRobustSigma > 0 ? HeavyTailModifiedZThreshold : PgRatioAnomalyThreshold),
                /* The instrument, on the fact: every figure here is estimated from sampling, over the time the
                   sampler watched (observed) beside the wall time between the same collections (interval), and
                   whether every rated collection disclosed its sampled_ms (V133) or some were read as their
                   whole interval. The wait facts carry the same three keys (PgTargetScorer.Waits.cs). */
                [PgTargetScorer.WaitIsSampledKey] = 1,
                [PgTargetScorer.WaitSourceObservedMsKey] = observedSec * 1000.0,
                [PgTargetScorer.WaitSourceIntervalMsKey] = intervalSec * 1000.0,
                [PgTargetScorer.WaitSampledMsKnownKey] = unknownSampled == 0 ? 1 : 0,
                /* unmeasured: the bar is chosen, the heavy-tail cutoff is the SQL Server fleet's and the ratio multiple
                   is chosen — see AnomalyThresholds.PgSampledWaitProfileFallbackMsPerSec. */
                ["threshold_lineage"] = 0,
            };
            AddBaselineContext(metadata, baseline);

            using (var contribCmd = WindowCommand(SampledWaitContribWindowSql, connection, context))
            {
                using var contribReader = await contribCmd.ExecuteReaderAsync(context.CancellationToken);
                while (await contribReader.ReadAsync(context.CancellationToken))
                {
                    var waitType = contribReader.GetString(0);
                    var waitEvent = contribReader.IsDBNull(1) ? null : contribReader.GetString(1);
                    var name = string.IsNullOrEmpty(waitEvent) ? waitType : waitType + ":" + waitEvent;
                    metadata[PgTargetFactKeys.WaitContributorMetadataPrefix + name] = Convert.ToDouble(contribReader.GetValue(2));
                }
            }

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalySampledWaitProfile,
                Value = totalWaitMs,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Sampled wait-profile anomaly detection failed: {Message}", ex.Message);
        }
    }
}

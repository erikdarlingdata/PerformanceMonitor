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
    /// The window's SERVER-WIDE mean execution ms per statement call, per <c>pg_statement_stats</c> collection — peak,
    /// mean and count — at the SAME grain and under the SAME rule as the <c>pg_statement_mean_ms</c> baseline arm
    /// (<c>PgTargetBaselineProvider.Plans.cs</c>): Σ <c>delta_total_exec_time_ms</c> over Σ <c>delta_calls</c> across
    /// every statement row of one <c>collection_time</c>, the collector's STORED deltas; a collection with no calls
    /// is not a sample (a mean over no calls is undefined, not zero), so the mean here and the bucket's both exclude
    /// idle minutes and the #3653 pair gate compares like with like. The peak's collection time rides along for the
    /// advice's age clause. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    /// </summary>
    public const string StatementMeanWindowSql = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(delta_calls) AS mean_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    GROUP BY collection_time
    HAVING SUM(delta_calls) > 0
)
SELECT MAX(mean_ms) AS peak_mean_ms,
       AVG(mean_ms) AS avg_mean_ms,
       COUNT(*)     AS sample_count,
       (SELECT collection_time FROM per_collection ORDER BY mean_ms DESC, collection_time DESC LIMIT 1) AS peak_time
FROM per_collection";

    /// <summary>
    /// <c>ANOMALY_PG_PLAN_REGRESSION</c>: the window's peak server-wide per-call statement mean (ms) against the
    /// <c>pg_statement_mean_ms</c> bucket — the z-score shape, graded by the shared deviation ramp (registered in
    /// <c>PgTargetScorer.IsDeviationScoredAnomalyKey</c> by the v3 plumbing). Folds onto <c>PG_PLAN_REGRESSION</c>.
    ///
    /// <para><b>Server-wide, by the coordinator's v3 ruling — and named as such.</b> The baseline seam keys one series
    /// per (server, metric) with no per-statement dimension (the arm's doc in <c>PgTargetBaselineProvider.Plans.cs</c>
    /// records the decision), so this anomaly says the SERVER's statements got slower per call than this hour usually
    /// sees, not that one statement did. It is the statistical corroborator of the per-statement flip
    /// <c>PG_PLAN_REGRESSION</c> names, and the fact family's amplifier reads its verdict; on its own it is as likely
    /// a heavier parameter mix, a colder cache or one new expensive statement as a plan change, and the advice says so.
    /// The metadata carries <c>server_wide = 1</c> so a reader of <c>get_analysis_facts</c> sees the grain.</para>
    ///
    /// <para><b>The PAIR gate (#3653), peak AND window mean.</b> A per-collection mean over a one-minute delta is
    /// spiky by nature — one minute in which the only calls were a report's is a mean of seconds against a baseline
    /// of milliseconds — and the peak alone would fire on every such minute. Requiring the window's MEAN to clear the
    /// same cutoff (trusted path) or the magnitude floor (fallback path) makes the finding "calls were slower for much
    /// of this window", which is what a plan regression on a hot statement looks like from the server's mean. The
    /// consequence is stated: a short regression inside a long anchored pass may not fire here — the regular fact
    /// grades the flip regardless, and this anomaly folds into that story when both fire.</para>
    ///
    /// <para><b>The robust cutoff is the heavy-tail one, by reference.</b> A per-call mean is a query-duration
    /// quantity — the SQL Server fleet measured that family at 3.13× mean-over-median, which is why
    /// <c>MetricNames.QueryDuration</c> takes <see cref="AnomalyThresholds.HeavyTailModifiedZThreshold"/> — and the
    /// PostgreSQL population is unmeasured, so this detector passes the heavy-tail cutoff explicitly (lane 9's
    /// wait-profile shape) rather than registering the metric in <c>ModifiedZThresholdFor</c>'s switch (a shared
    /// file). The magnitude floor and fallback are <see cref="AnomalyThresholds.PgStatementMeanMsFloor"/> /
    /// <see cref="AnomalyThresholds.PgStatementMeanMsFallback"/>, both unmeasured; the fact carries
    /// <c>threshold_lineage = 0</c>.</para>
    ///
    /// <para><b>Silent by design</b> when the bucket has no samples (no <c>pg_statement_stats</c> rows in the 30 days
    /// before the window — no <c>pg_stat_statements</c> on the target, or a young store) or the window has none.</para>
    ///
    /// <para>/* filled by lane 27 — the marker stays, as v1's did. The body copies the five v1 detectors in the root
    /// file: its own <c>try</c> / <c>catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> fence, the bucket through <see cref="Baselines"/>, the window read
    /// <see cref="StatementMeanWindowSql"/> over the collector table (the detector census reflects it),
    /// <c>CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c> (through <c>WindowCommand</c>), the
    /// token on every call, the #3653 PAIR gate, and <c>AnomalyGate</c>'s metadata plus <c>threshold_lineage = 0</c>.
    /// The bars are in the <c>AnomalyThresholds</c> PostgreSQL block WITH their lineage markers (the lane 27 block). */</para>
    /// </summary>
    private async partial Task DetectPlanRegressionAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgStatementMeanMs, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = WindowCommand(StatementMeanWindowSql, connection, context);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            if (windowSamples == 0) return;

            var peakMeanMs = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgMeanMs = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var peakTime = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);

            /* unmeasured: PgStatementMeanMsFloor / PgStatementMeanMsFallback (see AnomalyThresholds); the heavy-tail
               robust cutoff is used by reference as the shared query-duration cutoff, not re-declared. */
            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakMeanMs, avgMeanMs,
                DefaultDeviationThreshold, HeavyTailModifiedZThreshold, PgStatementMeanMsFloor, PgStatementMeanMsFallback, SigmaDisplayCap);
            if (!decision.Fire) return;

            /* unmeasured: both bars — the helper's default stamp, threshold_lineage = 0. */
            var metadata = ZScoreMetadata(baseline, decision, windowSamples);
            metadata["peak_mean_ms"] = peakMeanMs;
            metadata["avg_mean_ms"] = avgMeanMs;
            metadata["mean_sigma"] = decision.MeanSigma ?? 0.0;
            metadata["server_wide"] = 1;
            if (peakTime is { } at)
                metadata["peak_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(at)).TotalSeconds);

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyPlanRegression,
                Value = peakMeanMs,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Plan-regression anomaly detection failed: {Message}", ex.Message);
        }
    }
}

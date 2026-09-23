/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetAnomalyDetector
{
    /// <summary>
    /// The window's per-call mean execution ms PER FLIPPED STATEMENT, per <c>pg_statement_stats</c> collection — the
    /// candidate set of <c>PG_PLAN_REGRESSION</c> itself (a statement captured under two or more distinct
    /// <c>plan_hash</c> values inside the window: <c>PgTargetFactCollector.PgTargetPlanFlipSql</c>'s
    /// <c>hash_count &gt;= 2</c>, the same orphan exclusion — <c>query_id &lt;&gt; 0</c>, no <c>%Q</c>), bounded to
    /// <c>$4</c> statements by execution time so the keyed baseline population this detector asks for is bounded
    /// exactly as lane 33's cardinality note asks. Per candidate: the peak and mean of its per-collection per-call
    /// mean, the sample count, and the collection the peak fell in (for the advice's age clause).
    ///
    /// <para><b>The same quantity and the same sample rule as the KEYED bucket arm</b>
    /// (<c>PgTargetBaselineProvider.Statements.cs</c>' <c>pg_statement_mean_ms</c> keyed arm): Σ
    /// <c>delta_total_exec_time_ms</c> over Σ <c>delta_calls</c> across that <c>queryid</c>'s rows stamped with one
    /// <c>collection_time</c> — the collector's STORED deltas, pooled to the <c>queryid</c> grain the fact family,
    /// the plan captures and the text store share — and a collection in which the statement made no calls is NOT a
    /// sample (<c>HAVING SUM(delta_calls) &gt; 0</c>): a mean over no calls is undefined, not zero. The window side
    /// and the bucket side therefore exclude the statement's idle minutes the same way, which is what lets the #3653
    /// pair gate compare like with like.</para>
    ///
    /// <para><b>Why the flip set and not the top statements.</b> The anomaly folds onto <c>PG_PLAN_REGRESSION</c>, and
    /// the reconciler's fold is by KEY, not by statement (<c>PgTargetFactKeys.AnomalyToFamilies</c>) — so the honest
    /// way to keep the two facts about one statement is to draw both from the same population: the statements whose
    /// plan actually changed in this window. A statement that got slower without a captured flip is the regular
    /// family's <c>PG_BAD_ACTOR</c> / its own-normal anomaly (lane 34), not a plan regression.</para>
    ///
    /// <para><c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC, the closed PostgreSQL fact-read shape),
    /// <c>$4</c> the candidate count.</para>
    /// </summary>
    public const string StatementMeanKeyedWindowSql = @"
WITH flipped AS (
    SELECT query_id
    FROM pg_plan_capture
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_id IS NOT NULL
    AND   query_id <> 0
    AND   plan_hash IS NOT NULL
    GROUP BY query_id
    HAVING COUNT(DISTINCT plan_hash) >= 2
),
per_collection AS (
    SELECT s.queryid,
           s.collection_time,
           SUM(s.delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(s.delta_calls) AS mean_ms,
           SUM(s.delta_total_exec_time_ms)::DOUBLE PRECISION                      AS stmt_ms
    FROM pg_statement_stats AS s
    JOIN flipped AS f
      ON f.query_id = s.queryid
    WHERE s.server_id = $1
    AND   s.collection_time >= $2
    AND   s.collection_time <= $3
    GROUP BY s.queryid, s.collection_time
    HAVING SUM(s.delta_calls) > 0
)
SELECT queryid,
       MAX(mean_ms) AS peak_mean_ms,
       AVG(mean_ms) AS avg_mean_ms,
       COUNT(*)     AS sample_count,
       (ARRAY_AGG(collection_time ORDER BY mean_ms DESC, collection_time DESC))[1] AS peak_time
FROM per_collection
GROUP BY queryid
ORDER BY SUM(stmt_ms) DESC
LIMIT $4";

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
    /// <c>ANOMALY_PG_PLAN_REGRESSION</c>: the window's peak per-call statement mean (ms) against the
    /// <c>pg_statement_mean_ms</c> bucket — the z-score shape, graded by the shared deviation ramp (registered in
    /// <c>PgTargetScorer.IsDeviationScoredAnomalyKey</c> by the v3 plumbing). Folds onto <c>PG_PLAN_REGRESSION</c>.
    ///
    /// <para><b>TWO SERIES, and the keyed one is the instrument (#3691 lane 39, on calibration D's evidence).</b>
    /// Lane 27 wrote this detector on the SERVER-WIDE mean because the baseline seam keyed one series per
    /// (server, metric); the keyed seam exists since #3810 and the third fleet calibration read both series over the
    /// same 50 Aurora PostgreSQL clusters (28 days, fenced at 2026-09-19 16:40Z — measurements-3691.md §D5) and
    /// settled which one to grade on. The server-wide per-call mean's hour-of-week ratio reached a maximum of 4.4 in
    /// a week across the whole fleet and NEVER reached the 3.0 multiple (p99 2.05; its level is p50 1.0 ms, p99
    /// 12 ms): a per-statement 90× step is diluted to nothing at the server's mean, which is what a weak instrument
    /// looks like when you finally measure it. The KEYED per-<c>queryid</c> series has the tail the finding needs
    /// (ratio p99 2.0, p99.9 57, maximum 90; level p50 8 ms, p99 1,092 ms). So the detector now reads the keyed
    /// series for every FLIPPED statement (<see cref="StatementMeanKeyedWindowSql"/>) and names ONE statement; the
    /// server-wide arm is retained ONLY as the cold fallback for a server with no trustworthy keyed bucket yet.
    /// <c>series = 1</c> keyed / <c>0</c> server-wide is stamped on every fact, beside the shipped
    /// <c>server_wide</c> flag (the complement, kept because <c>server_wide = 1</c> is what the v3 fact has said
    /// since #3798 and a saved query may ask for it).</para>
    ///
    /// <para><b>The keyed arm.</b> For each candidate — the flip set, bounded to
    /// <c>PgTargetFactCollector.TopStatementCount</c> by execution time — the statement's window peak and mean
    /// per-call cost are graded against ITS OWN hour-of-week bucket, every candidate's read in one keyed
    /// <c>GetBaselinesAsync(serverId, pg_statement_mean_ms, keys: the queryids, …)</c> (#3901). Lane 34's shape, file for file: the
    /// trust check PRECEDES the gate (an untrustworthy own-normal is recorded as <c>own_normal_&lt;queryid&gt; = 0</c>
    /// and never graded, so no statement is silently handed the server's grade), the absolute-fallback bar is
    /// <c>double.PositiveInfinity</c> — unreachable by construction on this arm — and the #3653 pair gate asks the
    /// peak AND the window mean, because a per-collection per-call mean over a one-minute delta is spiky by nature
    /// (one minute whose only call was a report's is a mean of seconds against a baseline of milliseconds). ONE fact
    /// is emitted, for the statement furthest beyond its normal (largest peak sigma, ties to the larger peak), with
    /// its <c>queryid</c> on <see cref="Fact.ObjectName"/>; every candidate's verdict rides in metadata under
    /// <see cref="OwnNormalMetadataPrefix"/>. A story needs one leaf.</para>
    ///
    /// <para><b>The fold, and its honest limit.</b> <c>PgTargetFactKeys.AnomalyToFamilies</c> folds this key onto
    /// <c>PG_PLAN_REGRESSION</c> and the reconciler matches on (family key, database) — it cannot see a statement,
    /// and the reconciler root is not this lane's to edit. What makes the fold land on the SAME statement is the
    /// population: both facts are drawn from the flip set and both are one per pass, so the anomaly names a
    /// statement whose plan changed in this window, as the regular fact does. When the two nonetheless name
    /// different statements (two flips, one worst by ratio and another worst by sigma) the incident carries both
    /// cards and each names its own <c>queryid</c> — and the graph edge, which DOES compare the ids
    /// (<c>PgTargetRelationshipGraph.Plans.cs</c>), stays shut, so no story claims the deviation belongs to the
    /// statement the regression named. A reconciler arm that resolves the family per story from the anomaly's
    /// <c>queryid</c> is the follow-up filed with lane 34's.</para>
    ///
    /// <para><b>The cold fallback.</b> When NO candidate has a trustworthy keyed bucket — a young store, a server
    /// whose flipped statements are all first-seen, or no captured flip at all — the server-wide arm runs exactly as
    /// #3798 shipped it, stamped <c>series = 0</c> / <c>server_wide = 1</c>, and the advice says it cannot name a
    /// statement. When at least one candidate HAS a trustworthy bucket the keyed arm decides, fire or silence: a
    /// series measured to be blind must not get a second vote after the sighted one said no.</para>
    ///
    /// <para><b>The PAIR gate (#3653), peak AND window mean.</b> A per-collection mean over a one-minute delta is
    /// spiky by nature — one minute in which the only calls were a report's is a mean of seconds against a baseline
    /// of milliseconds — and the peak alone would fire on every such minute. Requiring the window's MEAN to clear the
    /// same cutoff (trusted path) or the magnitude floor (fallback path) makes the finding "calls were slower for much
    /// of this window", which is what a plan regression on a hot statement looks like from the server's mean. The
    /// consequence is stated: a short regression inside a long anchored pass may not fire here — the regular fact
    /// grades the flip regardless, and this anomaly folds into that story when both fire.</para>
    ///
    /// <para><b>The robust cutoff is the heavy-tail one, by reference — on both arms.</b> A per-call mean is a
    /// query-duration quantity — the SQL Server fleet measured that family at 3.13× mean-over-median, which is why
    /// <c>MetricNames.QueryDuration</c> takes <see cref="AnomalyThresholds.HeavyTailModifiedZThreshold"/> — and the
    /// PostgreSQL population is unmeasured, so this detector passes the heavy-tail cutoff explicitly (lane 9's
    /// wait-profile shape) rather than registering the metric in <c>ModifiedZThresholdFor</c>'s switch (a shared
    /// file). The magnitude floor and fallback are <see cref="AnomalyThresholds.PgStatementMeanMsFloor"/> /
    /// <see cref="AnomalyThresholds.PgStatementMeanMsFallback"/>.</para>
    ///
    /// <para><b>The lineage stamp: <c>threshold_lineage = 0</c> on both arms, and exactly why.</b> Calibration D
    /// measured the hour-of-week RATIO distributions of both series and the LEVEL distribution of each (§D5, 28 days
    /// × 50 Aurora PostgreSQL clusters, fenced at 2026-09-19 16:40Z) — which is what places the CHOICE of series and
    /// retires the server-wide one. It did not place the sigma cutoffs this arm grades on: those are the shared
    /// classical default and the SQL Server fleet's heavy-tail robust cutoff, reused BY NAME, and
    /// <c>PgStatementMeanMsFloor</c> / <c>Fallback</c> are still chosen numbers (the read now gives the level
    /// distribution a future calibration would place them on — stated on the constants). A measured ratio
    /// distribution does not make a sigma bar measured, so the flag stays 0 on both arms and the advice keeps
    /// saying the bars are chosen.</para>
    ///
    /// <para><b>Silent by design</b> when no statement flipped and the server-wide bucket has no samples (no
    /// <c>pg_statement_stats</c> rows in the 30 days before the window — no <c>pg_stat_statements</c> on the target,
    /// or a young store), or the window has none.</para>
    ///
    /// <para>/* filled by lane 27, switched to the keyed series by lane 39 — the marker stays, as v1's did. The body
    /// copies the five v1 detectors in the root file: its own <c>try</c> / <c>catch (Exception ex) when
    /// (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))</c> fence — ONE fence over both arms, the
    /// fallback being an arm of this detector and not a detector of its own — the buckets through
    /// <see cref="Baselines"/>, the window reads over collector tables (the detector census reflects both),
    /// <c>CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c> (through <c>WindowCommand</c>), the
    /// token on every call, the #3653 PAIR gate, and <c>AnomalyGate</c>'s metadata plus <c>threshold_lineage = 0</c>.
    /// The bars are in the <c>AnomalyThresholds</c> PostgreSQL block WITH their lineage markers (the lane 27 block). */</para>
    /// </summary>
    private async partial Task DetectPlanRegressionAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var candidates = new List<(long QueryId, double PeakMeanMs, double AvgMeanMs, long Samples, DateTime? PeakTime)>();
            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = WindowCommand(StatementMeanKeyedWindowSql, connection, context);
                cmd.Parameters.AddWithValue(PgTargetFactCollector.TopStatementCount);
                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (reader.IsDBNull(0)) continue;
                    candidates.Add((
                        QueryId: Convert.ToInt64(reader.GetValue(0)),
                        PeakMeanMs: reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                        AvgMeanMs: reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                        Samples: reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                        PeakTime: reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
                }
            }

            /* #3901: the flip set's own-normals in ONE keyed read (an empty set reads nothing) — asking per candidate
               re-read the 30-day slice of pg_statement_stats once per statement. */
            var keys = candidates.ConvertAll(candidate => candidate.QueryId.ToString(CultureInfo.InvariantCulture));
            var keyedBaselines = await _baselineProvider.GetBaselinesAsync(
                context.ServerId, MetricNames.PgStatementMeanMs, keys, context.TimeRangeStart, context.CancellationToken);

            var verdicts = new Dictionary<string, double>(StringComparer.Ordinal);
            var withoutBaseline = 0;
            var trustworthy = 0;
            var fired = 0;
            (long QueryId, double PeakMeanMs, double AvgMeanMs, long Samples, DateTime? PeakTime, BaselineBucket Baseline, AnomalyGate.ZDecision Decision)? worst = null;

            foreach (var candidate in candidates)
            {
                var key = candidate.QueryId.ToString(CultureInfo.InvariantCulture);
                var keyed = keyedBaselines[key];

                /* No trustworthy own-normal for this statement: recorded, never graded (lane 34's rule) — the
                   server-wide series is not a stand-in for one statement's history, it is the fallback for a server
                   that has none at all. */
                if (keyed.SampleCount == 0 || !keyed.IsTrustworthy || candidate.Samples == 0)
                {
                    verdicts[OwnNormalMetadataPrefix + key] = 0;
                    withoutBaseline++;
                    continue;
                }

                trustworthy++;

                /* unmeasured: PgStatementMeanMsFloor (see AnomalyThresholds) and the two cutoffs reused by name; the
                   absolute-fallback bar is unreachable by construction on this arm — the trust check above precedes
                   the gate, so a statement without an own-normal is never handed an absolute grade silently. */
                var keyedDecision = AnomalyGate.EvaluateZScore(
                    keyed, candidate.PeakMeanMs, candidate.AvgMeanMs,
                    DefaultDeviationThreshold, HeavyTailModifiedZThreshold, PgStatementMeanMsFloor, double.PositiveInfinity, SigmaDisplayCap);

                if (!keyedDecision.Fire)
                {
                    verdicts[OwnNormalMetadataPrefix + key] = 1;
                    continue;
                }

                verdicts[OwnNormalMetadataPrefix + key] = 2;
                fired++;
                if (worst is null || keyedDecision.Sigma > worst.Value.Decision.Sigma
                    || (keyedDecision.Sigma == worst.Value.Decision.Sigma && candidate.PeakMeanMs > worst.Value.PeakMeanMs))
                {
                    worst = (candidate.QueryId, candidate.PeakMeanMs, candidate.AvgMeanMs, candidate.Samples, candidate.PeakTime, keyed, keyedDecision);
                }
            }

            /* The sighted instrument decides whenever it can see: one trustworthy keyed bucket is enough, and its
                silence is a verdict (§D5 — the server-wide ratio never reached the multiple in a measured week, so
                letting it speak after the keyed series said no would be the weak instrument overruling the strong). */
            if (trustworthy > 0)
            {
                if (worst is null) return;
                EmitKeyedPlanRegressionAnomaly(context, anomalies, worst.Value, verdicts, candidates.Count, fired, withoutBaseline);
                return;
            }

            await DetectServerWidePlanRegressionAnomaly(context, anomalies, candidates.Count, withoutBaseline);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Plan-regression anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// The keyed arm's fact: the statement furthest beyond its OWN per-call normal, <c>queryid</c> on
    /// <see cref="Fact.ObjectName"/> (the string seam — a 64-bit id is not exact in a double, lane 7's rule) and
    /// <c>series = 1</c>. <c>ratio</c> is the peak over the bucket mean, the "200 ms against its routine 20 ms" the
    /// advice states; <c>candidates_*</c> count the flip set, how many of them were beyond their normal and how many
    /// had no trustworthy bucket to be judged against, so a reader can tell "one statement of six" from "the only
    /// one we could judge".
    /// </summary>
    private void EmitKeyedPlanRegressionAnomaly(
        AnalysisContext context,
        List<Fact> anomalies,
        (long QueryId, double PeakMeanMs, double AvgMeanMs, long Samples, DateTime? PeakTime, BaselineBucket Baseline, AnomalyGate.ZDecision Decision) worst,
        Dictionary<string, double> verdicts,
        int candidateCount,
        int fired,
        int withoutBaseline)
    {
        /* unmeasured: the cutoffs and the floor above — the helper's default stamp, threshold_lineage = 0. */
        var metadata = ZScoreMetadata(worst.Baseline, worst.Decision, worst.Samples);
        metadata["peak_mean_ms"] = worst.PeakMeanMs;
        metadata["avg_mean_ms"] = worst.AvgMeanMs;
        metadata["mean_sigma"] = worst.Decision.MeanSigma ?? 0.0;
        metadata["ratio"] = worst.Baseline.Mean > 0 ? worst.PeakMeanMs / worst.Baseline.Mean : 0.0;
        metadata[PgTargetScorer.PlanAnomalySeriesKey] = 1;
        metadata["server_wide"] = 0;
        metadata["candidates_evaluated"] = candidateCount;
        metadata["candidates_fired"] = fired;
        metadata["candidates_without_baseline"] = withoutBaseline;
        foreach (var (name, value) in verdicts)
            metadata[name] = value;
        if (worst.PeakTime is { } at)
            metadata["peak_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(at)).TotalSeconds);

        anomalies.Add(new Fact
        {
            Source = AnomalySource,
            Key = PgTargetFactKeys.AnomalyPlanRegression,
            Value = worst.PeakMeanMs,
            ServerId = context.ServerId,
            ObjectName = worst.QueryId.ToString(CultureInfo.InvariantCulture),
            Metadata = metadata,
        });
    }

    /// <summary>
    /// The COLD FALLBACK arm, #3798's detector unchanged in arithmetic and stamped <c>series = 0</c>: the window's
    /// peak server-wide per-call statement mean against the unkeyed <c>pg_statement_mean_ms</c> bucket. It runs only
    /// when no flipped statement had a trustworthy keyed bucket, and it says the SERVER's statements got slower per
    /// call than this hour usually sees — not that one statement did. §D5 measured it to be a weak instrument (its
    /// hour-of-week ratio never reached 3.0 across 50 clusters in a week; a per-statement 90× step dilutes to
    /// nothing in the server's mean), which is exactly why it is retained here and nowhere else: on a young server
    /// a blunt reading is better than no reading, and the fact says which it is. No own fence — it runs inside the
    /// caller's, being an arm of one detector. <c>candidates_without_baseline</c> rides along so a reader sees WHY
    /// the blunt arm answered.
    /// </summary>
    private async Task DetectServerWidePlanRegressionAnomaly(AnalysisContext context, List<Fact> anomalies, int candidateCount, int withoutBaseline)
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
        metadata[PgTargetScorer.PlanAnomalySeriesKey] = 0;
        metadata["candidates_evaluated"] = candidateCount;
        metadata["candidates_without_baseline"] = withoutBaseline;
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
}

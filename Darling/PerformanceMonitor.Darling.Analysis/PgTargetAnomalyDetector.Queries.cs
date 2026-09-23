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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetAnomalyDetector
{
    /// <summary>
    /// The window's candidate statements — its top <c>$4</c> by execution time, lane 7's candidate set
    /// (<c>PgTargetFactCollector.TopStatementCount</c>, so the keyed baseline population this detector asks for is
    /// bounded exactly as lane 33's cardinality note asks) — each with its share of the window's total
    /// (<c>window_share</c>: the number the <c>PG_BAD_ACTOR</c> card carries as <c>share_of_window_time</c>, the
    /// SAME Σ / Σ over the same rows) and, per collection, the statistic the bucket is made of: the statement's share
    /// of THAT collection's total (<c>peak_share</c>, <c>mean_share</c>, <c>sample_count</c>).
    ///
    /// <para><b>The same sample rule as the bucket arm</b> (<c>PgTargetBaselineProvider.Statements.cs</c>): a collection
    /// where nothing ran is NO sample (<c>HAVING SUM(…) &gt; 0</c> in <c>collections</c>); a collection where OTHER
    /// statements ran and this one did not IS a sample at 0 (the <c>CROSS JOIN</c> to every busy collection and the
    /// <c>LEFT JOIN</c> to the statement's own rows, <c>coalesce(…, 0)</c>) — the statement's habit includes the
    /// minutes it sat out, on both sides of the comparison. Stored deltas only (lane 7's discipline; a <c>LAG</c> here
    /// would span the gap in the stored rows). <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC, the closed
    /// PostgreSQL fact-read shape), <c>$4</c> the candidate count.</para>
    /// </summary>
    public const string StatementShareWindowSql = @"
WITH per_collection AS (
    SELECT queryid,
           collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION AS stmt_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    GROUP BY queryid, collection_time
),
collections AS (
    SELECT collection_time, SUM(stmt_ms) AS collection_ms
    FROM per_collection
    GROUP BY collection_time
    HAVING SUM(stmt_ms) > 0
),
candidates AS (
    SELECT queryid,
           SUM(stmt_ms)             AS stmt_ms,
           SUM(SUM(stmt_ms)) OVER () AS window_ms
    FROM per_collection
    GROUP BY queryid
    HAVING SUM(stmt_ms) > 0
    ORDER BY SUM(stmt_ms) DESC
    LIMIT $4
)
SELECT w.queryid,
       w.stmt_ms / w.window_ms                              AS window_share,
       MAX(coalesce(p.stmt_ms, 0) / c.collection_ms)        AS peak_share,
       AVG(coalesce(p.stmt_ms, 0) / c.collection_ms)        AS mean_share,
       COUNT(c.collection_time)                             AS sample_count
FROM candidates AS w
CROSS JOIN collections AS c
LEFT JOIN per_collection AS p
  ON p.queryid = w.queryid
 AND p.collection_time = c.collection_time
GROUP BY w.queryid, w.stmt_ms, w.window_ms
ORDER BY w.stmt_ms DESC";

    /// <summary>The metadata key prefix under which the anomaly records EVERY candidate's verdict against its own
    /// normal — <c>own_normal_&lt;queryid&gt;</c>: 0 no trustworthy own-normal yet (young store, first seen, or a
    /// share too steady to have a dispersion), 1 within its own normal, 2 beyond it (fired). The card's advice reads
    /// its own statement's entry so a context card can say WHICH of the three it is; the <c>queryid</c> rides in the
    /// key's text, never as a double (lane 7's rule).</summary>
    internal const string OwnNormalMetadataPrefix = "own_normal_";

    /// <summary>
    /// <see cref="PgTargetFactKeys.AnomalyBadActorShare"/> (lane 34, ruled 2026-09-20): ONE statement's share of the
    /// window's execution time against that statement's OWN hour-of-week share baseline — lane 33's keyed
    /// <c>pg_statement_share</c> arm, read through the keyed <c>GetBaselinesAsync</c> with the <c>queryid</c>s as
    /// the keys — the z-score shape on the #3653 PAIR gate, graded by the shared deviation ramp
    /// (<c>PgTargetScorer.IsDeviationScoredAnomalyKey</c>). The ruling this detector implements: <i>the bad actor is
    /// graded as deviation from the statement's own hour-of-week share baseline; the absolute share is context</i> —
    /// because the 2026-09-20 read (§C4) found the absolute bars ROUTINE (given a busy hour, top-1 share ≥ 0.25 on 85 %
    /// of hours, ≥ 0.60 on 44 %): a statement at its usual 60 % is the workload; a statement at 55 % that usually
    /// takes 10 % is the story, and only its own series can tell the two apart.
    ///
    /// <para><b>Candidates, and why one anomaly per pass.</b> The window's top <c>TopStatementCount</c> statements by
    /// execution time — lane 7's set, the same rows that become <c>PG_BAD_ACTOR_*</c> cards — each read against its
    /// own bucket. Every candidate's verdict is recorded on the fact (<see cref="OwnNormalMetadataPrefix"/>), and the
    /// fact is emitted ONCE, for the candidate furthest beyond its normal (the largest peak sigma among those that
    /// fired), with that statement's <c>queryid</c> on <see cref="Fact.ObjectName"/> — the string seam lane 27 uses,
    /// because the key is one const (the shared switches route it by name) and a 64-bit id is not exact in a double.
    /// A story needs one leaf; a second deviant statement in the same pass is stated on the fact and on its own
    /// context card, not raised as a second anomaly.</para>
    ///
    /// <para><b>The gate.</b> The trusted path only: a candidate whose own bucket is not
    /// <see cref="BaselineBucket.IsTrustworthy"/> (too few samples or days, or a share so steady it has no
    /// dispersion) is recorded as <c>own_normal = 0</c> and NOT graded — the ruling forbids an absolute grade
    /// standing in silently for a missing own-normal, so the absolute-fallback arm of the shared gate is unreachable
    /// here by construction (<c>double.PositiveInfinity</c> is passed as its bar, and the trust check precedes the
    /// call). On the trusted path the window's PEAK per-collection share AND its MEAN must both clear the cutoff
    /// (#3653's pair, the shape lanes 17 / 27 / 28 took: a per-collection share is spiky — one minute in which this
    /// statement was the only thing running is a share of 1.0 against a baseline of 0.1 — and the mean clause makes
    /// the finding "this statement held more of the window than it usually holds of this hour"). The magnitude
    /// floor on the peak is <c>PgTargetScorer.BadActorShareConcerning</c> BY REFERENCE — the measured-routine line:
    /// a statement must at some point in the window have held at least the share that is routine for a TOP statement
    /// before its deviation is the story; a 10× jump from 0.5 % to 5 % is a statement nobody would call a bad actor.
    /// The cutoffs are the shared defaults reused by name — <see cref="AnomalyThresholds.DefaultDeviationThreshold"/>
    /// classical, <see cref="AnomalyThresholds.ModifiedZThresholdFor"/>'s default robust cutoff — unmeasured for
    /// statement share; the fact carries <c>threshold_lineage = 0</c>.</para>
    ///
    /// <para><b>What the third calibration read (2026-09-21) settled, and what it did not.</b> §D3A read the very
    /// distribution this detector is about — a top-5 statement's share of the collection against its own
    /// hour-of-week mean, 1,487 statement-hours over 50 Aurora PostgreSQL clusters, 28 days fenced at 2026-09-19
    /// 16:40Z (<c>pg_statement_stats</c> died fleet-wide at 16:44Z on a schema regression, so every window ends
    /// before it). The finding: a ratio of 3× or more happens on 0.94 % of statement-hours, and 0.40 % once the peak
    /// magnitude floor is required as well — the instrument fires about four times in a thousand statement-hours,
    /// which is the rate a story-level finding should have, and the absolute share in the same week is routine
    /// (p50 0.077, p99 0.56), re-confirming why the grade is the deviation. That read places the MULTIPLE (recorded
    /// on <see cref="AnomalyThresholds.PgRatioAnomalyThreshold"/> with the other families' verdicts) and the peak
    /// floor's cut. It does NOT place this detector's bars: the gate is a Z-test and its cutoffs are sigmas, and no
    /// percentile of a ratio distribution places a sigma cutoff — a 3× share and a 3σ share are different claims
    /// about different distributions. The fact therefore still carries <c>threshold_lineage = 0</c>, and it will
    /// until someone reads the per-statement SIGMA distribution (share minus its bucket mean over the bucket's
    /// effective dispersion, per statement-hour) on a fleet with a live statement collector.</para>
    ///
    /// <para><b>Metadata.</b> The shared z-family set (<c>ZScoreMetadata</c>: <c>deviation_sigma</c>,
    /// <c>fire_threshold</c>, <c>baseline_*</c>, <c>confidence</c> …) plus <c>window_share</c> (the card's number),
    /// <c>peak_share</c>, <c>mean_share</c>, <c>mean_sigma</c>, <c>ratio</c> (window share over the bucket mean — the
    /// "61 % against its routine 22 %" the advice states), <c>candidates_evaluated</c> / <c>candidates_fired</c> /
    /// <c>candidates_without_baseline</c>, and one <c>own_normal_&lt;queryid&gt;</c> per candidate.</para>
    ///
    /// <para><b>Silent by design</b> when the window has no statement rows (no <c>pg_stat_statements</c>, a young
    /// store), when no candidate has a trustworthy own-normal, and when none is beyond it. Fenced like every
    /// detector; the keyed reads are lane 33's cached series, asked for as ONE set (#3901: one 30-day read of
    /// <c>pg_statement_stats</c> per cache period for all <c>TopStatementCount</c> candidates, not one per
    /// statement).</para>
    /// </summary>
    private async partial Task DetectBadActorShareAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var candidates = new List<(long QueryId, double WindowShare, double PeakShare, double MeanShare, long Samples)>();
            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = WindowCommand(StatementShareWindowSql, connection, context);
                cmd.Parameters.AddWithValue(PgTargetFactCollector.TopStatementCount);
                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (reader.IsDBNull(0)) continue;
                    candidates.Add((
                        QueryId: Convert.ToInt64(reader.GetValue(0)),
                        WindowShare: reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                        PeakShare: reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                        MeanShare: reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                        Samples: reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4))));
                }
            }
            if (candidates.Count == 0) return;

            /* #3901: every candidate's own-normal in ONE keyed read — the arm reads the 30-day slice once for the set,
               where asking per candidate re-read it (and recomputed every collection's total) once per statement. */
            var keys = candidates.ConvertAll(candidate => candidate.QueryId.ToString(CultureInfo.InvariantCulture));
            var baselines = await _baselineProvider.GetBaselinesAsync(
                context.ServerId, MetricNames.PgStatementShare, keys, context.TimeRangeStart, context.CancellationToken);

            var verdicts = new Dictionary<string, double>(StringComparer.Ordinal);
            var withoutBaseline = 0;
            var fired = 0;
            (long QueryId, double WindowShare, double PeakShare, double MeanShare, long Samples, BaselineBucket Baseline, AnomalyGate.ZDecision Decision)? worst = null;

            foreach (var candidate in candidates)
            {
                var key = candidate.QueryId.ToString(CultureInfo.InvariantCulture);
                var baseline = baselines[key];

                /* No trustworthy own-normal: recorded, never graded — the ruling's "first seen; no own-normal yet". */
                if (baseline.SampleCount == 0 || !baseline.IsTrustworthy || candidate.Samples == 0)
                {
                    verdicts[OwnNormalMetadataPrefix + key] = 0;
                    withoutBaseline++;
                    continue;
                }

                /* unmeasured for statement share: the shared classical and robust cutoffs, reused by name;
                   BadActorShareConcerning as the peak's magnitude floor is the measured-ROUTINE line (context, not the
                   grade — see the summary); the fallback bar is unreachable by construction (trust checked above). */
                var decision = AnomalyGate.EvaluateZScore(
                    baseline, candidate.PeakShare, candidate.MeanShare,
                    DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgStatementShare),
                    PgTargetScorer.BadActorShareConcerning, double.PositiveInfinity, SigmaDisplayCap);

                if (!decision.Fire)
                {
                    verdicts[OwnNormalMetadataPrefix + key] = 1;
                    continue;
                }

                verdicts[OwnNormalMetadataPrefix + key] = 2;
                fired++;
                if (worst is null || decision.Sigma > worst.Value.Decision.Sigma
                    || (decision.Sigma == worst.Value.Decision.Sigma && candidate.WindowShare > worst.Value.WindowShare))
                {
                    worst = (candidate.QueryId, candidate.WindowShare, candidate.PeakShare, candidate.MeanShare, candidate.Samples, baseline, decision);
                }
            }

            if (worst is null) return;
            var (queryId, windowShare, peakShare, meanShare, samples, bucket, verdict) = worst.Value;

            /* unmeasured: the cutoffs above — the helper's default stamp, threshold_lineage = 0. */
            var metadata = ZScoreMetadata(bucket, verdict, samples);
            metadata["window_share"] = windowShare;
            metadata["peak_share"] = peakShare;
            metadata["mean_share"] = meanShare;
            metadata["mean_sigma"] = verdict.MeanSigma ?? 0.0;
            metadata["ratio"] = bucket.Mean > 0 ? windowShare / bucket.Mean : 0.0;
            metadata["candidates_evaluated"] = candidates.Count;
            metadata["candidates_fired"] = fired;
            metadata["candidates_without_baseline"] = withoutBaseline;
            foreach (var (name, value) in verdicts)
                metadata[name] = value;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyBadActorShare,
                Value = windowShare,
                ServerId = context.ServerId,
                ObjectName = queryId.ToString(CultureInfo.InvariantCulture),
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Bad-actor share anomaly detection failed: {Message}", ex.Message);
        }
    }
}

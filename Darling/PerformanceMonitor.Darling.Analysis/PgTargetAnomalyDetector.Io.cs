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
    /// The window's ms per data-file read per FIFTEEN-MINUTE sample: the per-identity reset-aware difference of
    /// <c>pg_io_stats</c> (<c>object_type = 'relation'</c>, every backend and context) at the quarter-hour grain —
    /// the last counter value of each identity in each sample, differenced against the previous sample's, clamped
    /// at 0, summed over identities — then peak, average and count over the samples that clear the reads floor,
    /// plus the peak sample and the reads behind it. The same quantity, at the same grain and under the same floor,
    /// as the <c>pg_io_read_latency</c> baseline arm (<c>PgTargetBaselineProvider.Io.cs</c>), so the number a
    /// sample is judged against is the number the buckets were built from; the calibration read (§B1) distributed
    /// the same quotient at the hour grain, and a ratio of two sums is the same ratio over any partition of the
    /// same rows.
    ///
    /// <para><b>Why the quarter-hour (#3691 between waves).</b> Lane 11 shipped the hour: one sample per hour-of-week
    /// bucket per week, four or five in 30 days, under <c>BaselineMath.RestoreThreshold</c> — so every I/O anomaly
    /// was judged against the hour-of-day collapse. Four samples an hour restores the hour-of-week tier, and the
    /// detector reads the grain the baseline is built at or the comparison is apples to oranges. A one-minute
    /// ms-per-read on a server doing a few hundred reads a minute is a quotient over a handful of operations; at
    /// the quarter-hour the quotient stabilises and the reads floor (<c>PgTargetScorer.IoBaselineBucketMinimumReads</c>,
    /// the literal 250 below — <c>PgTargetIoTests</c> pins them equal) has a population to apply to. A sample under
    /// the floor is not rated, on either side of the comparison.</para>
    ///
    /// <para><c>MAX(counter)</c> per sample is the per-identity last value — a cumulative counter is monotone within
    /// one <c>stats_reset</c> epoch, and a reset inside the sample makes the next difference negative, where
    /// <c>GREATEST(raw, 0)</c> drops it — the shape the calibration read validated. With timing off every
    /// <c>read_time_ms</c> delta is 0, the quotient is 0 and the peak sits under the floor: the detector is silent
    /// where the regular fact says <c>unavailable</c>, never "0 ms, 20σ below normal".</para>
    /// </summary>
    public const string IoLatencyWindowSql = @"
WITH sampled AS (
    SELECT backend_type, context,
           date_bin('15 minutes', collection_time, TIMESTAMP '2000-01-01') AS sample_start,
           MAX(reads)        AS reads,
           MAX(read_time_ms) AS read_ms
    FROM pg_io_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   object_type = 'relation'
    GROUP BY backend_type, context, date_bin('15 minutes', collection_time, TIMESTAMP '2000-01-01')
),
deltas AS (
    SELECT sample_start,
           reads   - LAG(reads)   OVER series AS raw_reads,
           read_ms - LAG(read_ms) OVER series AS raw_read_ms
    FROM sampled
    WINDOW series AS (PARTITION BY backend_type, context ORDER BY sample_start)
),
per_sample AS (
    SELECT sample_start,
           SUM(GREATEST(raw_reads, 0))::DOUBLE PRECISION   AS reads,
           SUM(GREATEST(raw_read_ms, 0))::DOUBLE PRECISION AS read_ms
    FROM deltas
    WHERE raw_reads IS NOT NULL
    GROUP BY sample_start
),
rated AS (
    SELECT sample_start, reads, read_ms / reads AS ms_per_read
    FROM per_sample
    WHERE reads >= 250 /* PgTargetScorer.IoBaselineBucketMinimumReads — the per-sample reads floor */
)
SELECT MAX(ms_per_read) AS peak_ms_per_read,
       AVG(ms_per_read) AS avg_ms_per_read,
       COUNT(*)         AS rated_samples,
       (SELECT sample_start FROM rated ORDER BY ms_per_read DESC, sample_start DESC LIMIT 1) AS peak_sample,
       (SELECT reads        FROM rated ORDER BY ms_per_read DESC, sample_start DESC LIMIT 1) AS peak_sample_reads
FROM rated";

    /// <summary>
    /// <c>ANOMALY_PG_IO_LATENCY</c>: the window's peak quarter-hour read latency (ms per read, from <c>pg_io_stats</c>) against the <c>pg_io_read_latency</c> bucket — the z-score shape <c>DetectSessionAnomalies</c> takes, graded by the shared deviation ramp (registered in <c>PgTargetScorer.IsDeviationScoredAnomalyKey</c>).
    /// <para>/* filled by lane 11 of #3691 (design §2a, the I/O analogue). The body copies the five v1 detectors in
    /// the root file: its own <c>try</c> / <c>catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> fence, the bucket through <see cref="Baselines"/>, the window read
    /// <see cref="IoLatencyWindowSql"/> over the collector table, <c>CommandTimeout =
    /// DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c> (through <c>WindowCommand</c>), the token on every
    /// call, and <c>AnomalyGate</c>'s metadata. */</para>
    ///
    /// <para><b>The gate's two bars, both measured.</b> <see cref="AnomalyThresholds.PgIoLatencyFloorMs"/> (2 ms) is
    /// the magnitude ceiling on the trusted path — under it a deviation is fast storage's jitter — and
    /// <see cref="AnomalyThresholds.PgIoLatencyFallbackMs"/> (20 ms) is the bar on an untrustworthy bucket, sitting
    /// between the regular fact's WARNING and CRITICAL so a young store's anomaly is never a finding the fact did
    /// not already make. The fact therefore carries <c>threshold_lineage = 1</c>, overriding the lane-9 helper's
    /// unmeasured stamp: these bars were read off the fleet (measurements-3691.md §B1, 2026-09-19) and the
    /// population is named in <c>PgTargetScorer.Io.cs</c>.</para>
    ///
    /// <para><b>Folds into <c>PG_IO_READ_LATENCY_MS</c></b> (<c>PgTargetFactKeys.AnomalyToFamilies</c>) and is that
    /// fact's first corroborator (<c>PgTargetScorer.Io.cs</c> amplifiers); it has no amplifier arm of its own — the
    /// impact lives in the parent, which is never capped.</para>
    /// </summary>
    private async partial Task DetectIoAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgIoReadLatency, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = WindowCommand(IoLatencyWindowSql, connection, context);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var ratedSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            if (ratedSamples == 0) return;

            var peakMsPerRead = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgMsPerRead = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var peakSample = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
            var peakSampleReads = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakMsPerRead,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgIoReadLatency), PgIoLatencyFloorMs, PgIoLatencyFallbackMs, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = ZScoreMetadata(baseline, decision, ratedSamples);
            /* measured (§B1): the two bars above were read off the fleet — see the summary. */
            metadata["threshold_lineage"] = 1;
            metadata["peak_ms_per_read"] = peakMsPerRead;
            metadata["avg_ms_per_read"] = avgMsPerRead;
            /* The quarter-hour sample the peak came from, the reads behind it, and the admission floor those reads
               cleared (PgTargetScorer.IoBaselineBucketMinimumReads — measured as an admission floor on 2026-09-20,
               §C5, stated on the constant with its consequence for the baseline tier; a sample-admission floor, not a
               grading bar, so the lineage stamp above is 1 for the measured fire bars and now for the floor too). */
            metadata["peak_sample_reads"] = peakSampleReads;
            metadata["peak_sample_ticks"] = peakSample?.Ticks ?? 0;
            metadata["bucket_reads_floor"] = PgTargetScorer.IoBaselineBucketMinimumReads;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyIoLatency,
                Value = peakMsPerRead,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] I/O latency anomaly detection failed: {Message}", ex.Message);
        }
    }
}

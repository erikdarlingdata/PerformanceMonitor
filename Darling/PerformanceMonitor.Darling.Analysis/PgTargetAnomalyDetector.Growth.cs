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
    /// The window's instance-growth rate per <c>pg_database_size_stats</c> collection — peak, mean and count of the
    /// per-day-rated <c>total_bytes</c> differences — at the SAME grain and under the SAME rules as the
    /// <c>pg_database_growth_bytes_per_day</c> baseline arm (<c>PgTargetBaselineProvider.Growth.cs</c>): one total per
    /// collection time, NULL totals excluded, shrinks clamped to zero, the difference over the samples' own gap
    /// scaled to a day. The window read reaches ONE sample before the window start (<c>LAG</c> over a set that begins
    /// an hour early) so the first in-window sample has a predecessor to difference against — otherwise a one-hour
    /// window over an hourly series would hold zero deltas and the detector would be silent on every default pass.
    /// The peak sample's time rides along for the advice's age clause.
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    /// </summary>
    public const string DatabaseGrowthWindowSql = @"
WITH totals AS (
    SELECT DISTINCT ON (collection_time) collection_time, total_bytes
    FROM pg_database_size_stats
    WHERE server_id = $1 AND collection_time >= $2 - INTERVAL '2 hours' AND collection_time <= $3
    AND   total_bytes IS NOT NULL
    ORDER BY collection_time
),
sampled AS (
    SELECT collection_time,
           total_bytes - LAG(total_bytes) OVER series AS raw_growth_bytes,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER series))) AS interval_sec
    FROM totals
    WINDOW series AS (ORDER BY collection_time)
),
rated AS (
    SELECT collection_time, GREATEST(raw_growth_bytes, 0)::DOUBLE PRECISION * 86400.0 / interval_sec AS bytes_per_day
    FROM sampled
    WHERE raw_growth_bytes IS NOT NULL
    AND   interval_sec > 0
    AND   collection_time >= $2
)
SELECT MAX(bytes_per_day) AS peak_bytes_per_day,
       AVG(bytes_per_day) AS mean_bytes_per_day,
       COUNT(*)           AS sample_count,
       (SELECT collection_time FROM rated ORDER BY bytes_per_day DESC, collection_time DESC LIMIT 1) AS peak_at
FROM rated";

    /// <summary>
    /// <c>ANOMALY_PG_DATABASE_GROWTH</c>: the window's peak instance-growth rate (bytes per day per collection, from
    /// <c>pg_database_size_stats</c>' <c>total_bytes</c>) against the <c>pg_database_growth_bytes_per_day</c> bucket —
    /// the z-score shape, graded by the shared deviation ramp (registered in <c>PgTargetScorer.IsDeviationScoredAnomalyKey</c>
    /// by this lane). Folds onto <c>PG_DATABASE_GROWTH</c>.
    /// <para>/* filled by lane 38 of #3691 — declared and filled by the content lane (no stub existed). The body copies
    /// the filled detectors: its own <c>try</c> / <c>catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> fence, the bucket through <see cref="Baselines"/>, the window read
    /// <see cref="DatabaseGrowthWindowSql"/> over the collector table, <c>CommandTimeout =
    /// DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c> (through <c>WindowCommand</c>), the token on every call,
    /// and <c>AnomalyGate</c>'s metadata plus <c>threshold_lineage = 0</c> on the fact (both bars unmeasured —
    /// <see cref="AnomalyThresholds.PgDatabaseGrowthFloorBytesPerDay"/> / <see cref="AnomalyThresholds.PgDatabaseGrowthFallbackBytesPerDay"/>). */</para>
    ///
    /// <para><b>The PAIR gate</b>, like the blocking and CPU-burn detectors: an hourly growth series is spiky by nature
    /// — one COPY, one index build, one <c>VACUUM</c> that could not truncate — and a window of two or three hourly
    /// deltas would fire on any one of them from the peak alone. Requiring the window's MEAN to clear the same cutoff
    /// (trusted path) or the magnitude floor (fallback path) makes the finding "the instance was growing fast for
    /// this whole window". The consequence is stated honestly: a one-hour load inside a 24-hour anchored pass may not
    /// fire here — the regular <c>PG_DATABASE_GROWTH</c> fact grades the fortnight regardless, and this anomaly folds
    /// into that story when both fire.</para>
    ///
    /// <para><b>Silent by design</b> where the instance total is NULL (a database the role may not size — on a managed
    /// service, typically the vendor's own; the trend fact still speaks for the databases it can see and says why the
    /// total is missing), when the bucket has no samples (a store younger than the collector's first hour), and when the
    /// window holds no delta. A zero-activity bucket (thirty days of an instance that never grew — the routine shape of
    /// a read-mostly server) is untrustworthy by <c>EffectiveStdDev</c>'s contract and takes the absolute-fallback path,
    /// so such a server's first real load still fires at the fallback bar. The bucket is THIN by construction (hourly
    /// → about four samples per hour-of-week bucket over 30 days) and will grade at the hour-only or flat tier for
    /// months; the advice states the tier.</para>
    /// </summary>
    private async partial Task DetectDatabaseGrowthAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgDatabaseGrowthBytesPerDay, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = WindowCommand(DatabaseGrowthWindowSql, connection, context);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            if (windowSamples == 0) return;

            var peakPerDay = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var meanPerDay = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var peakAt = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
            if (peakPerDay <= 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakPerDay, meanPerDay,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgDatabaseGrowthBytesPerDay), PgDatabaseGrowthFloorBytesPerDay, PgDatabaseGrowthFallbackBytesPerDay, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            /* unmeasured: both bars (see AnomalyThresholds) — the helper's default stamp, threshold_lineage = 0. */
            var metadata = ZScoreMetadata(baseline, decision, windowSamples);
            metadata["peak_bytes_per_day"] = peakPerDay;
            metadata["mean_bytes_per_day"] = meanPerDay;
            metadata["mean_sigma"] = decision.MeanSigma ?? 0.0;
            if (peakAt is { } at)
                metadata["peak_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(at)).TotalSeconds);

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyDatabaseGrowth,
                Value = peakPerDay,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Database-growth anomaly detection failed: {Message}", ex.Message);
        }
    }
}

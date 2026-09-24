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
    /// The window's distinct blocked sessions per <c>pg_blocking</c> capture — peak, mean and count — at the SAME
    /// grain and under the SAME zero rule as the <c>pg_blocked_sessions</c> baseline arm
    /// (<c>PgTargetBaselineProvider.Blocking.cs</c>): every SUCCESS run of the collector in <c>collection_log</c> is a
    /// sample, binned to the minute; a minute with edge rows carries <c>COUNT(DISTINCT blocked_pid)</c>, a logged
    /// minute with none is a ZERO. The mean therefore includes the quiet captures, exactly as the buckets do, so the
    /// #3653 pair gate compares like with like. The peak minute rides along for the advice's age clause.
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    /// </summary>
    public const string BlockedSessionsWindowSql = @"
WITH looked AS (
    SELECT DISTINCT date_bin('1 minute', collection_time, TIMESTAMP '2000-01-01') AS minute
    FROM collection_log
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   collector_name = 'pg_blocking'
    AND   status = 'SUCCESS'
),
blocked AS (
    SELECT date_bin('1 minute', collection_time, TIMESTAMP '2000-01-01') AS minute,
           COUNT(DISTINCT blocked_pid)::DOUBLE PRECISION AS blocked_sessions
    FROM pg_blocking_edges
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   blocked_pid IS NOT NULL
    GROUP BY date_bin('1 minute', collection_time, TIMESTAMP '2000-01-01')
),
per_capture AS (
    SELECT coalesce(b.minute, l.minute)     AS minute,
           coalesce(b.blocked_sessions, 0)  AS blocked_sessions
    FROM looked AS l
    FULL OUTER JOIN blocked AS b ON b.minute = l.minute
)
SELECT MAX(blocked_sessions) AS peak_blocked_sessions,
       AVG(blocked_sessions) AS avg_blocked_sessions,
       COUNT(*)              AS sample_count,
       (SELECT minute FROM per_capture ORDER BY blocked_sessions DESC, minute DESC LIMIT 1) AS peak_minute
FROM per_capture";

    /// <summary>
    /// <c>ANOMALY_PG_BLOCKING</c>: the window's peak blocked-session count per capture (<c>pg_blocking_edges</c> waiters per <c>collection_time</c>, zero for a logged capture with none) against the <c>pg_blocked_sessions</c> bucket — the z-score shape, graded by the shared deviation ramp (registered in <c>PgTargetScorer.IsDeviationScoredAnomalyKey</c> by the between-waves batch). Folds onto <c>PG_BLOCKING_CHAIN</c>.
    /// <para>/* filled by lane 17 of #3691 — the marker stays, as v1's did. The body copies the five v1 detectors in
    /// the root file: its own <c>try</c> / <c>catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex,
    /// context.CancellationToken))</c> fence, the bucket through <see cref="Baselines"/>, the window read
    /// <see cref="BlockedSessionsWindowSql"/> over the collector table and the log (the detector census whitelists
    /// <c>collection_log</c> by name, with this file's argument), <c>CommandTimeout =
    /// DarlingAnalysisService.AnalysisCommandTimeoutSeconds</c> (through <c>WindowCommand</c>), the token on every
    /// call, and <c>AnomalyGate</c>'s metadata plus <c>threshold_lineage = 0</c> on the fact (both bars unmeasured —
    /// <see cref="AnomalyThresholds.PgBlockedSessionsFloor"/> / <see cref="AnomalyThresholds.PgBlockedSessionsFallback"/>). */</para>
    ///
    /// <para><b>The PAIR gate, not the peak-only one.</b> This is the first PostgreSQL detector to pass its window
    /// mean (<c>AnomalyGate</c>'s #3653 remarks name the PostgreSQL content lanes as the callers that would): a
    /// blocked-session series is spiky by nature — one lock handoff caught mid-flight is a 1 against a baseline of
    /// zeros — and the peak alone would fire on every such minute in a long window. Requiring the window's MEAN to
    /// clear the same cutoff (on the trusted path) or the magnitude floor (on the fallback path) makes the finding
    /// "sessions were queued for much of this window", which is what an operator would call a blocking storm. The
    /// consequence is stated honestly: a 4-hour storm inside a 24-hour anchored pass may not fire here — the
    /// regular <c>PG_BLOCKING_CHAIN</c> fact grades it on duration regardless, and this anomaly folds into that
    /// story when both fire.</para>
    ///
    /// <para><b>Silent by design</b> when the bucket has no samples (no <c>pg_blocking</c> run logged in the last 30
    /// days — a server the collector never visited) or the window has none; a zero-activity bucket (30 days of
    /// logged captures, never an edge) is untrustworthy by <c>EffectiveStdDev</c>'s contract and takes the
    /// absolute-fallback path, so a never-blocking server's first storm still fires at the fallback bar.</para>
    /// </summary>
    private async partial Task DetectBlockingAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgBlockedSessions, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = WindowCommand(BlockedSessionsWindowSql, connection, context);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            if (windowSamples == 0) return;

            var peakBlocked = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgBlocked = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var peakMinute = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakBlocked, avgBlocked,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgBlockedSessions), PgBlockedSessionsFloor, PgBlockedSessionsFallback, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            /* unmeasured: both bars (see AnomalyThresholds) — the helper's default stamp, threshold_lineage = 0. */
            var metadata = ZScoreMetadata(baseline, decision, windowSamples);
            metadata["peak_blocked_sessions"] = peakBlocked;
            metadata["avg_blocked_sessions"] = avgBlocked;
            metadata["mean_sigma"] = decision.MeanSigma ?? 0.0;
            if (peakMinute is { } at)
                metadata["peak_age_s"] = Math.Max(0, (AsNaive(context.TimeRangeEnd) - AsNaive(at)).TotalSeconds);

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyBlocking,
                Value = peakBlocked,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Blocking anomaly detection failed: {Message}", ex.Message);
        }
    }
}

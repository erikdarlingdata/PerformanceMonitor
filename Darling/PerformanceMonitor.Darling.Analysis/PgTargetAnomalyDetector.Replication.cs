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
    /// The window's per-collection worst-standby replay gap (<c>MAX(replay_bytes_behind)</c> per <c>collection_time</c>
    /// — the PICK the PG_REPLICATION_LAG read ranks on, so the anomaly and the fact it folds into measure the same
    /// thing), then peak / average / count. Rows with a NULL gap (a standby in <c>startup</c>) are not samples.
    /// The baseline arm (<c>PgTargetBaselineProvider.Replication.cs</c>) takes the same per-collection pick, so the
    /// comparison is like for like — with the #3538 A8 peak-versus-per-sample residue the root file states.
    /// </summary>
    public const string ReplayLagWindowSql = @"
WITH per_collection AS (
    SELECT collection_time, MAX(replay_bytes_behind)::DOUBLE PRECISION AS replay_bytes
    FROM pg_replication_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   replay_bytes_behind IS NOT NULL
    GROUP BY collection_time
)
SELECT MAX(replay_bytes) AS peak_replay_bytes,
       AVG(replay_bytes) AS avg_replay_bytes,
       COUNT(*)          AS sample_count
FROM per_collection";

    /// <summary>
    /// <c>ANOMALY_PG_REPLICATION_LAG</c>: the window's peak worst-standby replay gap (bytes, from
    /// <c>pg_replication_stats</c>) against the <c>pg_replay_lag_bytes</c> bucket — the z-score shape through the
    /// shared <see cref="AnomalyGate"/>, graded by the shared deviation ramp (<c>PgTargetScorer.IsDeviationScoredAnomalyKey</c>
    /// registered it in the plumbing).
    /// <para>/* filled by lane 12 of #3691 — the body copies the five v1 detectors: its own fence, the bucket
    /// through the provider, the window read a <c>public const string …Sql</c> in this file, the pass deadline on
    /// the command, the token on every call, and the gate's metadata plus <c>threshold_lineage = 0</c>. */</para>
    ///
    /// <para><b>The floors are the fact's own</b> (<c>PgTargetScorer.Replication.cs</c>): the magnitude floor on the
    /// trusted path is one default WAL segment (<see cref="PgTargetScorer.PgReplayLagBytesFloor"/> — below it a
    /// deviation is inside the unit the quantity is measured in, however many sigmas it reads), and the
    /// absolute-fallback bar on an untrustworthy baseline is the slot alert's byte bar by reference
    /// (<see cref="PgTargetScorer.PgReplayLagBytesFallback"/>) — a young store's lag anomaly fires where the alert
    /// would page a slot retaining the same WAL. Declared beside the scorer rather than in <c>AnomalyThresholds</c>
    /// for the reason stated there. Folds into <c>PG_REPLICATION_LAG</c> (<c>PgTargetFactKeys.AnomalyToFamilies</c>).</para>
    ///
    /// <para>A server with no standby has no rows, no baseline, and the detector contributes nothing — absence is
    /// normal, not <c>unavailable</c>. The five-minute cadence: the fact states its own <c>window_samples</c>.</para>
    /// </summary>
    private async partial Task DetectReplicationAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.PgReplayLagBytes, context.TimeRangeStart, context.CancellationToken);
            if (baseline.SampleCount == 0) return;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = WindowCommand(ReplayLagWindowSql, connection, context);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var peakBytes = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgBytes = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakBytes, avgBytes,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgReplayLagBytes),
                PgTargetScorer.PgReplayLagBytesFloor, PgTargetScorer.PgReplayLagBytesFallback, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            var metadata = ZScoreMetadata(baseline, decision, windowSamples);
            metadata["peak_replay_bytes"] = peakBytes;
            metadata["avg_replay_bytes"] = avgBytes;

            anomalies.Add(new Fact
            {
                Source = AnomalySource,
                Key = PgTargetFactKeys.AnomalyReplicationLag,
                Value = peakBytes,
                ServerId = context.ServerId,
                Metadata = metadata,
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgTargetAnomalyDetector] Replication-lag anomaly detection failed: {Message}", ex.Message);
        }
    }
}

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
SELECT " + WindowTiles.LocalHourSql + @" AS local_hour,
       MAX(replay_bytes) AS peak_replay_bytes,
       AVG(replay_bytes) AS avg_replay_bytes,
       COUNT(*)          AS sample_count
FROM per_collection
GROUP BY " + WindowTiles.LocalHourSql + @"
ORDER BY " + WindowTiles.LocalHourSql;

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
            /* #3653 A8 option B: per-hour tiles against the tile's own (hour, dow) bucket, never-blind fallback to
               today's window-peak path when no tile scores. */
            var map = await _baselineProvider.GetBucketMapAsync(
                context.ServerId, MetricNames.PgReplayLagBytes, context.TimeRangeStart, context.TimeRangeEnd, context.CancellationToken);

            var tiles = new List<WindowTile>();
            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            using (var cmd = WindowCommand(ReplayLagWindowSql, connection, context))
            {
                /* #3653 A8 option B: the tiled read's GROUP BY key (WindowTiles.LocalHourSql) binds $4..$6 from the
                   ANALYSIS window's clock — WindowCommand only binds $1..$3. */
                cmd.Parameters.AddWithValue(AsNaive(map.WindowClock.TransitionAtUtc));
                cmd.Parameters.AddWithValue(map.WindowClock.OffsetBeforeMinutes);
                cmd.Parameters.AddWithValue(map.WindowClock.OffsetAfterMinutes);

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                    tiles.Add(WindowTiles.ReadTile(reader, localHourOrdinal: 0, peakOrdinal: 1, meanOrdinal: 2, samplesOrdinal: 3));
            }

            var whole = WindowTiles.WholeWindow(tiles);
            if (whole.Samples == 0) return;

            var window = context.TimeRangeEnd - context.TimeRangeStart;
            var tv = AnomalyGate.EvaluateTiles(
                tiles, map,
                DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgReplayLagBytes),
                PgTargetScorer.PgReplayLagBytesFloor, PgTargetScorer.PgReplayLagBytesFallback, SigmaDisplayCap,
                window);

            AnomalyGate.ZDecision decision;
            BaselineBucket baseline;
            double peakBytes;
            double avgBytes;
            long windowSamples;

            if (tv is null)
            {
                baseline = await _baselineProvider.GetBaselineAsync(
                    context.ServerId, MetricNames.PgReplayLagBytes, context.TimeRangeStart, context.CancellationToken);
                if (baseline.SampleCount == 0) return;

                decision = AnomalyGate.EvaluateZScore(
                    baseline, whole.Peak, whole.Mean,
                    DefaultDeviationThreshold, ModifiedZThresholdFor(MetricNames.PgReplayLagBytes),
                    PgTargetScorer.PgReplayLagBytesFloor, PgTargetScorer.PgReplayLagBytesFallback, SigmaDisplayCap,
                    window: window);
                if (!decision.Fire) return;

                peakBytes = whole.Peak;
                avgBytes = whole.Mean;
                windowSamples = whole.Samples;
            }
            else
            {
                decision = tv.Value.Decision;
                baseline = tv.Value.Bucket;
                peakBytes = tv.Value.Tile.Peak;
                avgBytes = tv.Value.Tile.Mean;
                windowSamples = tv.Value.Tile.Samples;
                if (!decision.Fire) return;
            }

            var metadata = ZScoreMetadata(baseline, decision, windowSamples);
            metadata["peak_replay_bytes"] = peakBytes;
            metadata["avg_replay_bytes"] = avgBytes;
            if (tv is { } verdict)
                WindowTiles.AddTileMetadata(metadata, verdict, tiles, map.WindowClock);

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
